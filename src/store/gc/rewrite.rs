use crc32c::Crc32cHasher;
use std::{
    cmp::Ordering, collections::HashMap, hash::Hasher, ops::Deref, sync::Arc,
    sync::atomic::Ordering::AcqRel,
};

#[cfg(feature = "metrics")]
use crate::utils::observe::{CounterMetric, EventKind, HistogramMetric, ObserveEvent};
use crate::{
    OpCode, Options,
    io::GatherIO,
    map::data::{FileFooter, FileReader, FileVersion, MetaReader, new_reader},
    meta::{
        DelInterval, Delete, FileKind, IntervalPair, JunkRegistry, MemStat, MetaKind,
        RewriteRegistration, StatInner,
    },
    must_exist, must_ok, must_true,
    types::{refbox::BoxRef, traits::IAsSlice},
    utils::{
        bitmap::BitMap,
        block::Block,
        compress::{
            COMPRESS_MIN_LEN, CompressorGuard, CompressorPool, DecompressorPool, RecordCompressor,
            RecordDecompressor,
        },
        data::{AddrPair, GatherWriter, Interval, LenSeq},
    },
};
#[cfg(feature = "metrics")]
use std::time::Instant;

use super::GarbageCollector;
use crate::utils::diag;

#[derive(Clone, Copy, Debug)]
pub(super) struct Score {
    pub(super) id: u64,
    pub(super) size: usize,
    pub(super) rate: f64,
    pub(super) up2: u64,
    pub(super) bucket_id: u64,
}

impl Score {
    pub(super) fn from(stat: StatInner, now: u64) -> Self {
        Self {
            id: stat.file_id,
            size: stat.active_size,
            rate: Self::calc_decline_rate(&stat, now),
            up2: stat.up2,
            bucket_id: stat.bucket_id,
        }
    }

    fn calc_decline_rate(stat: &StatInner, now: u64) -> f64 {
        let free = stat.total_size.saturating_sub(stat.active_size);
        let live = stat.active_elems.max(1);
        // no junk has been applied yet, or
        // it's possible gc and flush thread get same tick
        if free == 0 || stat.up2 == now {
            return f64::INFINITY;
        }

        (stat.active_size as f64 / free as f64).powi(2) / (live as f64 * (now - stat.up2) as f64)
    }

    fn cmp_priority(&self, other: &Self) -> Ordering {
        self.rate
            .total_cmp(&other.rate)
            .then_with(|| self.up2.cmp(&other.up2))
            .then_with(|| self.size.cmp(&other.size))
            .then_with(|| self.id.cmp(&other.id))
    }
}

struct RewritePlan {
    victims: Vec<u64>,
    obsoleted: Vec<u64>,
    del_intervals: DelInterval,
}

struct RewriteOutputs {
    fstats: Vec<MemStat>,
    output_ids: Vec<u64>,
    remap_intervals: Vec<IntervalPair>,
    reloc_targets: HashMap<u64, (u64, LenSeq)>,
}

impl GarbageCollector {
    fn interval_meta_kind(kind: FileKind) -> MetaKind {
        kind_select!(kind, data => MetaKind::DataInterval, blob => MetaKind::BlobInterval)
    }

    fn stat_meta_kind(kind: FileKind) -> MetaKind {
        kind_select!(kind, data => MetaKind::DataStat, blob => MetaKind::BlobStat)
    }

    #[cfg(feature = "metrics")]
    fn rewrite_counter(kind: FileKind) -> CounterMetric {
        kind_select!(kind, data => CounterMetric::GcDataRewrite, blob => CounterMetric::GcBlobRewrite)
    }

    #[cfg(feature = "metrics")]
    fn rewrite_micros(kind: FileKind) -> HistogramMetric {
        kind_select!(kind, data => HistogramMetric::GcDataRewriteMicros, blob => HistogramMetric::GcBlobRewriteMicros)
    }

    #[cfg(feature = "metrics")]
    fn rewrite_victim_hist(kind: FileKind) -> HistogramMetric {
        kind_select!(kind, data => HistogramMetric::GcDataRewriteVictimFiles, blob => HistogramMetric::GcBlobRewriteVictimFiles)
    }

    #[cfg(feature = "metrics")]
    fn rewrite_complete_event(kind: FileKind) -> EventKind {
        kind_select!(kind, data => EventKind::GcDataRewriteComplete, blob => EventKind::GcBlobRewriteComplete)
    }

    #[cfg(feature = "failpoints")]
    fn rewrite_stage_marker_failpoint(kind: FileKind) -> &'static str {
        kind_select!(kind, data => "mace_gc_data_rewrite_after_stage_marker", blob => "mace_gc_blob_rewrite_after_stage_marker")
    }

    #[cfg(feature = "failpoints")]
    fn rewrite_after_dir_sync_failpoint(kind: FileKind) -> &'static str {
        kind_select!(kind, data => "mace_gc_data_rewrite_after_data_dir_sync", blob => "mace_gc_blob_rewrite_after_data_dir_sync")
    }

    #[cfg(feature = "failpoints")]
    fn rewrite_before_meta_commit_failpoint(kind: FileKind) -> &'static str {
        kind_select!(kind, data => "mace_gc_data_rewrite_before_meta_commit", blob => "mace_gc_blob_rewrite_before_meta_commit")
    }

    #[cfg(feature = "failpoints")]
    fn rewrite_after_meta_commit_failpoint(kind: FileKind) -> &'static str {
        kind_select!(kind, data => "mace_gc_data_rewrite_after_meta_commit", blob => "mace_gc_blob_rewrite_after_meta_commit")
    }

    pub(super) fn collect_candidates(
        &self,
        kind: FileKind,
    ) -> (
        HashMap<u64, Vec<u64>>,
        HashMap<u64, (u64, u64)>,
        Vec<StatInner>,
    ) {
        let mut obsoleted = HashMap::<u64, Vec<u64>>::new();
        let mut bucket_usage = HashMap::<u64, (u64, u64)>::new();
        let mut candidates = Vec::new();
        let stats = &self.store.manifest.file_state(kind).stat;
        stats.for_each_bucket_file(|bucket_id, files| {
            for &fid in files {
                if self.store.manifest.is_unsynced_file(kind, fid) {
                    continue;
                }
                if let Some(stat) = stats.stat_inner(fid) {
                    if stat.active_elems == 0 {
                        obsoleted.entry(bucket_id).or_default().push(fid);
                    } else {
                        let e = bucket_usage.entry(bucket_id).or_insert((0, 0));
                        e.0 += stat.total_size as u64;
                        e.1 += stat.active_size as u64;
                        candidates.push(stat);
                    }
                }
            }
        });
        (obsoleted, bucket_usage, candidates)
    }

    pub(super) fn plan_rewrite_from_global(
        ranked: Vec<Score>,
        bucket_usage: &HashMap<u64, (u64, u64)>,
        tgt_ratio: u64,
        tgt_size: usize,
        eager: bool,
    ) -> Vec<(u64, Vec<Score>)> {
        let mut by_bucket: HashMap<u64, Vec<Score>> = HashMap::new();
        for score in ranked {
            by_bucket.entry(score.bucket_id).or_default().push(score);
        }

        let mut bucket_ids: Vec<u64> = by_bucket.keys().copied().collect();
        bucket_ids.sort_unstable();
        let mut plans = Vec::new();
        for bucket_id in bucket_ids {
            let ranked = by_bucket.remove(&bucket_id).unwrap_or_default();
            if !Self::should_run_rewrite_for_bucket(bucket_id, tgt_ratio, bucket_usage) {
                continue;
            }
            for p in Self::select_rewrite_batches_for_bucket(ranked, tgt_size, eager) {
                plans.push((bucket_id, p));
            }
        }
        plans
    }

    pub(super) fn select_rewrite_batches_for_bucket(
        ranked: Vec<Score>,
        tgt_size: usize,
        eager: bool,
    ) -> Vec<Vec<Score>> {
        let mut current = Vec::new();
        let mut current_size = 0usize;
        let mut batches = Vec::new();
        let target = tgt_size.max(1);

        for s in ranked {
            // isolate an oversized victim so record-level packing can split it independently
            if s.size >= target {
                if eager && current.len() > 1 {
                    batches.push(std::mem::take(&mut current));
                    current_size = 0;
                }
                batches.push(vec![s]);
                continue;
            }

            if eager && !current.is_empty() && current_size + s.size > target && current.len() > 1 {
                batches.push(std::mem::take(&mut current));
                current_size = 0;
            }
            current_size += s.size;
            current.push(s);
            if current_size >= target && current.len() > 1 {
                batches.push(std::mem::take(&mut current));
                current_size = 0;
            }
        }

        if eager && current.len() > 1 {
            batches.push(current);
        }
        batches
    }

    pub(super) fn rank_candidates<F>(candidates: Vec<StatInner>, tick_for_bucket: F) -> Vec<Score>
    where
        F: Fn(u64) -> u64,
    {
        let mut ranked: Vec<Score> = candidates
            .into_iter()
            .map(|s| Score::from(s, tick_for_bucket(s.bucket_id)))
            .filter(|s| s.rate.is_finite())
            .collect();
        ranked.sort_unstable_by(Score::cmp_priority);
        if ranked.len() > Self::MAX_ELEMS {
            ranked.truncate(Self::MAX_ELEMS);
        }
        ranked
    }

    fn should_run_rewrite(ratio: u64, tgt_ratio: u64) -> bool {
        ratio >= tgt_ratio
    }

    fn should_run_rewrite_for_bucket(
        bucket_id: u64,
        tgt_ratio: u64,
        bucket_usage: &HashMap<u64, (u64, u64)>,
    ) -> bool {
        let Some((total, active)) = bucket_usage.get(&bucket_id).copied() else {
            return false;
        };
        if total == 0 {
            return false;
        }
        let ratio = (total - active) * 100 / total;
        Self::should_run_rewrite(ratio, tgt_ratio)
    }

    pub(super) fn should_run_rewrite_live(
        &self,
        kind: FileKind,
        bucket_id: u64,
        tgt_ratio: u64,
    ) -> bool {
        let Some(ratio) = self.current_bucket_ratio(kind, bucket_id) else {
            return false;
        };
        Self::should_run_rewrite(ratio, tgt_ratio)
    }

    fn current_bucket_ratio(&self, kind: FileKind, bucket_id: u64) -> Option<u64> {
        self.store
            .manifest
            .file_state(kind)
            .stat
            .bucket_ratio(bucket_id, |fid| {
                self.store.manifest.is_unsynced_file(kind, fid)
            })
    }

    fn collect_rewrite_plan(
        &self,
        kind: FileKind,
        bucket_id: u64,
        candidate: &[Score],
    ) -> (RewritePlan, Vec<RewriteItem>) {
        let opt = &self.store.opt;
        let mut plan = RewritePlan {
            victims: Vec::new(),
            obsoleted: Vec::new(),
            del_intervals: DelInterval {
                lo: Vec::new(),
                bucket_id,
            },
        };
        let mut items = Vec::new();
        for x in candidate {
            let path = match kind {
                FileKind::Data => opt.data_file(x.id),
                FileKind::Blob => opt.blob_file(x.id),
            };
            let mut loader = MetaReader::new(opt.fs.as_ref(), path);
            let relocs = loader.get_reloc();
            let ivls: Vec<Interval> = loader
                .get_interval()
                .iter()
                .copied()
                .filter(|ivl| ivl.lo <= ivl.hi)
                .collect();
            let mut im = InactiveMap::new(&ivls);
            let persisted = match self
                .store
                .manifest
                .file_state(kind)
                .stat
                .load_persisted_stat_inner(x.id, &self.store.manifest.btree)
            {
                Ok(stat) => stat,
                Err(OpCode::NotFound)
                    if self.store.manifest.is_retired_stat(kind, bucket_id, x.id) =>
                {
                    continue;
                }
                Err(err) => must_ok!(Result::<StatInner, OpCode>::Err(err)),
            };
            let bitmap = self
                .store
                .manifest
                .file_state(kind)
                .stat
                .load_mask_from_btree(x.id, persisted.total_elems, &self.store.manifest.btree)
                .expect("must exist");
            let active: Vec<Entry> = relocs
                .iter()
                .filter(|m| !bitmap.test(m.val.seq))
                .map(|m| {
                    let interval_lo = im.test(m.key);
                    Entry {
                        key: m.key,
                        interval_lo,
                        raw_len: m.val.raw_len(),
                        compressed_len: m.val.compressed_len(),
                    }
                })
                .collect();
            if active.is_empty() {
                plan.obsoleted.push(x.id);
                continue;
            }
            im.collect(|_, ivl| {
                // every old interval is replaced with exact record intervals below
                plan.del_intervals.push(ivl.lo);
            });
            items.push(RewriteItem::new(x.id, x.up2, active));
            plan.victims.push(x.id);
        }
        (plan, items)
    }

    fn build_rewrite_outputs(
        &self,
        kind: FileKind,
        bucket_id: u64,
        tick: u64,
        permit: &crate::map::buffer::BucketRewritePermit,
        items: Vec<RewriteItem>,
    ) -> Option<RewriteOutputs> {
        let opt = &self.store.opt;
        let target = self.target_file_size(kind).max(1);
        let chunks = split_rewrite_items(items, target, permit.enable_compression);
        if chunks.is_empty() {
            return None;
        }
        let mut outputs = RewriteOutputs {
            fstats: Vec::with_capacity(chunks.len()),
            output_ids: Vec::with_capacity(chunks.len()),
            remap_intervals: Vec::new(),
            reloc_targets: HashMap::new(),
        };
        for chunk in chunks {
            let file_id = self.alloc_file_id();
            // stage orphan intent before rewrite output is flushed
            self.store.manifest.stage_orphan_file(kind, file_id);
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash(Self::rewrite_stage_marker_failpoint(kind));

            let mut builder = RewriteBuilder::new(
                file_id,
                opt,
                chunk.len(),
                bucket_id,
                tick,
                permit.enable_compression,
                permit.compressors.clone(),
            );
            for item in chunk {
                let mut begin = 0;
                while begin < item.pos.len() {
                    let interval_lo = item.pos[begin].interval_lo;
                    let mut end = begin + 1;
                    while end < item.pos.len() && item.pos[end].interval_lo == interval_lo {
                        end += 1;
                    }
                    let lo = item.pos[begin].key;
                    let hi = item.pos[end - 1].key;
                    outputs
                        .remap_intervals
                        .push(IntervalPair::new(lo, hi, file_id, bucket_id));
                    builder.add_interval(lo, hi);
                    begin = end;
                }
                builder.add_item(item);
            }
            let (mut fstat, relocs) = must_ok!(builder.build(kind));
            fstat.inner.bucket_id = bucket_id;
            for (key, reloc) in relocs {
                must_true!(
                    outputs
                        .reloc_targets
                        .insert(key, (file_id, reloc))
                        .is_none()
                );
            }
            outputs.fstats.push(fstat);
            outputs.output_ids.push(file_id);
            #[cfg(feature = "extra_check")]
            if outputs.fstats.len() == 1 {
                crate::testing::fire_gc_rewrite_sync_point(
                    match kind {
                        FileKind::Data => crate::testing::GcRewriteSyncPoint::DuringDataBuild,
                        FileKind::Blob => crate::testing::GcRewriteSyncPoint::DuringBlobBuild,
                    },
                    bucket_id,
                    &opt.db_root,
                );
            }
        }

        self.store.opt.sync_data_dir();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::rewrite_after_dir_sync_failpoint(kind));
        #[cfg(feature = "extra_check")]
        crate::testing::fire_gc_rewrite_sync_point(
            match kind {
                FileKind::Data => crate::testing::GcRewriteSyncPoint::BeforeDataPublish,
                FileKind::Blob => crate::testing::GcRewriteSyncPoint::BeforeBlobPublish,
            },
            bucket_id,
            &opt.db_root,
        );
        Some(outputs)
    }

    #[cfg_attr(not(feature = "extra_check"), allow(unused_variables))]
    fn publish_rewrite_outputs(
        &self,
        kind: FileKind,
        bucket_id: u64,
        plan: RewritePlan,
        outputs: RewriteOutputs,
        registration: RewriteRegistration<'_>,
    ) -> Delete {
        let junk = self.store.manifest.junk();
        let gate = junk.close_gate(kind);
        #[cfg(feature = "extra_check")]
        crate::testing::fire_gc_stat_sync_point(
            match kind {
                FileKind::Data => crate::testing::GcStatSyncPoint::DataRewriteGateClosed,
                FileKind::Blob => crate::testing::GcStatSyncPoint::BlobRewriteGateClosed,
            },
            bucket_id,
            &self.store.opt.db_root,
        );
        gate.drain();
        let cut = junk.begin_publish(kind, registration, gate);
        let mut txn = self.store.manifest.begin();
        txn.record(MetaKind::Sequences, self.store.manifest.sequences.deref());

        let (fstats, stats) = self.store.manifest.prepare_stat_intervals(
            kind,
            outputs.fstats,
            outputs.reloc_targets,
            &cut,
        );
        #[cfg(feature = "extra_check")]
        crate::testing::fire_gc_stat_sync_point(
            match kind {
                FileKind::Data => crate::testing::GcStatSyncPoint::DataRewritePublishing {
                    output_count: fstats.len(),
                },
                FileKind::Blob => crate::testing::GcStatSyncPoint::BlobRewritePublishing {
                    output_count: fstats.len(),
                },
            },
            bucket_id,
            &self.store.opt.db_root,
        );

        for stat in &stats {
            txn.record(Self::stat_meta_kind(kind), stat);
        }
        if !plan.del_intervals.is_empty() {
            txn.record(Self::delete_interval_meta_kind(kind), &plan.del_intervals);
        }
        for i in &outputs.remap_intervals {
            txn.record(Self::interval_meta_kind(kind), i);
        }
        let tmp: Delete = plan.victims.into();
        txn.record(Self::delete_meta_kind(kind), &tmp);
        for &file_id in &outputs.output_ids {
            self.store
                .manifest
                .clear_orphan_file(kind, &mut txn, file_id);
        }
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::rewrite_before_meta_commit_failpoint(kind));
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::rewrite_after_meta_commit_failpoint(kind));

        self.store.manifest.publish_stat_intervals(
            kind,
            fstats,
            &tmp,
            &plan.del_intervals,
            &outputs.remap_intervals,
        );
        // restoring Idle and reopening the gate must complete before physical deletion
        JunkRegistry::finish_publish(cut);
        #[cfg(feature = "extra_check")]
        crate::testing::fire_gc_stat_sync_point(
            match kind {
                FileKind::Data => crate::testing::GcStatSyncPoint::DataRewriteAfterPublish,
                FileKind::Blob => crate::testing::GcStatSyncPoint::BlobRewriteAfterPublish,
            },
            bucket_id,
            &self.store.opt.db_root,
        );
        tmp
    }

    fn retire_rewrite(&self, kind: FileKind, delete: Delete) {
        self.store.manifest.save_obsolete_files(kind, &delete);
        must_ok!(self.store.manifest.delete_files());
        self.file_runs(kind).fetch_add(1, AcqRel);
    }

    pub(super) fn rewrite_files(&self, kind: FileKind, candidate: &[Score], bucket_id: u64) {
        #[cfg(feature = "metrics")]
        let started = Instant::now();
        let Some(permit) = self.store.manifest.try_acquire_rewrite(bucket_id) else {
            return;
        };
        let tick = self
            .store
            .manifest
            .buckets
            .get_runtime(bucket_id)
            .next_update_epoch(kind);
        let collect_ids: Vec<u64> = candidate.iter().map(|x| x.id).collect();
        let junk = self.store.manifest.junk();
        let registration = {
            let gate = junk.close_gate(kind);
            gate.drain();
            // register while the initial gate is closed; start_rewrite reopens it
            junk.start_rewrite(kind, gate, &collect_ids)
        };
        #[cfg(feature = "extra_check")]
        crate::testing::fire_gc_stat_sync_point(
            match kind {
                FileKind::Data => crate::testing::GcStatSyncPoint::DataRewriteCollecting,
                FileKind::Blob => crate::testing::GcStatSyncPoint::BlobRewriteCollecting,
            },
            bucket_id,
            &self.store.opt.db_root,
        );

        let (plan, items) = self.collect_rewrite_plan(kind, bucket_id, candidate);
        #[cfg(feature = "metrics")]
        let victim_count = plan.victims.len() as u64;

        // it's possible that another thread deactivated all live items while we were processing
        junk.handoff(kind, &plan.obsoleted);
        self.process_obsoleted_files(kind, &plan.obsoleted, bucket_id);
        junk.clear_ordinary(kind, &plan.obsoleted);

        let Some(outputs) = self.build_rewrite_outputs(kind, bucket_id, tick, &permit, items)
        else {
            // the armed registration cancels the collection on early return
            return;
        };
        #[cfg(feature = "metrics")]
        let first_output = outputs.output_ids[0];
        let delete = self.publish_rewrite_outputs(kind, bucket_id, plan, outputs, registration);
        self.retire_rewrite(kind, delete);

        #[cfg(feature = "metrics")]
        {
            self.store
                .opt
                .observer
                .counter(Self::rewrite_counter(kind), 1);
            self.store.opt.observer.histogram(
                Self::rewrite_micros(kind),
                started.elapsed().as_micros() as u64,
            );
            self.store
                .opt
                .observer
                .histogram(Self::rewrite_victim_hist(kind), victim_count);
            self.store.opt.observer.event(ObserveEvent {
                kind: Self::rewrite_complete_event(kind),
                bucket_id,
                txid: 0,
                file_id: first_output,
                value: victim_count,
            });
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct PendingReloc {
    pub(super) key: u64,
    off: usize,
    pub(super) raw_len: u32,
    pub(super) compressed_len: u32,
    crc: u32,
}

impl PendingReloc {
    pub(super) const fn new(
        key: u64,
        off: usize,
        raw_len: u32,
        compressed_len: u32,
        crc: u32,
    ) -> Self {
        Self {
            key,
            off,
            raw_len,
            compressed_len,
            crc,
        }
    }
}

pub(super) fn build_sorted_relocs(
    pending: &mut [PendingReloc],
) -> (Vec<AddrPair>, HashMap<u64, LenSeq>) {
    pending.sort_unstable_by_key(|x| x.key);
    let mut relocs = Vec::with_capacity(pending.len());
    let mut reloc_map = HashMap::with_capacity(pending.len());
    for (seq, entry) in pending.iter().enumerate() {
        let seq = seq as u32;
        relocs.push(AddrPair::new(
            entry.key,
            entry.off,
            entry.raw_len,
            entry.compressed_len,
            seq,
            entry.crc,
        ));
        let old = reloc_map.insert(
            entry.key,
            LenSeq::new(entry.raw_len, entry.compressed_len, seq),
        );
        must_true!(old.is_none(), "rewritten reloc key must be unique");
    }
    (relocs, reloc_map)
}

pub(super) struct RewriteBuilder<'a> {
    file_id: u64,
    items: Vec<RewriteItem>,
    intervals: Vec<Interval>,
    nr_interval: u32,
    weighted_up2_sum: u128,
    weighted_up2_size: u128,
    tick: u64,
    opt: &'a Options,
    enable_compression: bool,
    compressors: Arc<CompressorPool>,
    pub(super) bucket_id: u64,
}

impl<'a> RewriteBuilder<'a> {
    pub(super) fn new(
        file_id: u64,
        opt: &'a Options,
        cap: usize,
        bucket_id: u64,
        tick: u64,
        enable_compression: bool,
        compressors: Arc<CompressorPool>,
    ) -> Self {
        Self {
            file_id,
            items: Vec::with_capacity(cap),
            intervals: Vec::with_capacity(cap),
            nr_interval: 0,
            weighted_up2_sum: 0,
            weighted_up2_size: 0,
            tick,
            opt,
            enable_compression,
            compressors,
            bucket_id,
        }
    }

    pub(super) fn add_item(&mut self, item: RewriteItem) {
        let weight = item.live_bytes.max(1) as u128;
        self.weighted_up2_sum += weight * item.up2 as u128;
        self.weighted_up2_size += weight;
        self.items.push(item);
    }

    pub(super) fn add_interval(&mut self, lo: u64, hi: u64) {
        self.intervals.push(Interval::new(lo, hi));
        self.nr_interval += 1;
    }

    pub(super) fn build(
        &mut self,
        kind: FileKind,
    ) -> Result<(MemStat, HashMap<u64, LenSeq>), OpCode> {
        let up2 = self
            .weighted_up2_sum
            .checked_div(self.weighted_up2_size)
            .map(|x| x as u64)
            .unwrap_or(self.tick);
        let mut off = 0;
        let path = match kind {
            FileKind::Data => self.opt.data_file(self.file_id),
            FileKind::Blob => self.opt.blob_file(self.file_id),
        };
        let mut writer = GatherWriter::trunc(
            self.opt.fs.as_ref(),
            &path,
            match kind {
                FileKind::Data => 128,
                FileKind::Blob => 8,
            },
        );
        let mut reloc: Vec<u8> = Vec::new();
        let mut pending_relocs = Vec::new();
        let decoders = DecompressorPool::new();
        let mut buffers = RewriteBuffers::new(1 << 20);
        let mut decoder = RecordDecompressor::new()?;

        self.items.sort_unstable_by_key(|x| (x.up2, x.id));
        let mut beg = u64::MAX;
        let mut end = u64::MIN;
        let mut compression = RewriteCompression {
            enabled: self.enable_compression,
            compressors: self.compressors.as_ref(),
            compressor: None,
        };
        for item in &self.items {
            beg = beg.min(item.id);
            end = end.max(item.id);

            let reader_path = match kind {
                FileKind::Data => self.opt.data_file(item.id),
                FileKind::Blob => self.opt.blob_file(item.id),
            };
            let reader = new_reader(reader_path, decoders.clone(), self.opt.fs.clone());
            for e in &item.pos {
                let encoded = rewrite_record(
                    &reader,
                    &mut writer,
                    &mut buffers,
                    &mut decoder,
                    &mut compression,
                    e,
                )?;
                pending_relocs.push(PendingReloc::new(
                    e.key,
                    off,
                    encoded.raw_len,
                    encoded.compressed_len,
                    encoded.crc,
                ));
                off += if encoded.compressed_len == 0 {
                    encoded.raw_len as usize
                } else {
                    encoded.compressed_len as usize
                };
            }
        }

        let nr_reloc = pending_relocs.len() as u32;
        let (sorted_relocs, reloc_map) = build_sorted_relocs(&mut pending_relocs);
        reloc.reserve(sorted_relocs.len() * AddrPair::LEN);
        for entry in sorted_relocs {
            reloc.extend_from_slice(entry.as_slice());
        }

        self.intervals.sort_unstable_by_key(|ivl| ivl.lo);
        let mut intervals = Vec::with_capacity(self.intervals.len() * Interval::LEN);
        for interval in &self.intervals {
            intervals.extend_from_slice(interval.as_slice());
        }
        let mut interval_crc = Crc32cHasher::default();
        let is = intervals.as_slice();
        interval_crc.write(is);
        writer.queue(is);

        let mut reloc_crc = Crc32cHasher::default();
        let s = reloc.as_slice();
        reloc_crc.write(s);
        writer.queue(s);

        let footer = FileFooter::new(
            nr_reloc,
            self.nr_interval,
            reloc_crc.finish() as u32,
            interval_crc.finish() as u32,
        );

        writer.queue(footer.as_slice());
        writer.flush();
        writer.sync();
        match kind {
            FileKind::Data => {
                diag::info!("compacted to {path:?} {footer:?}");
            }
            FileKind::Blob => {
                diag::info!("compacted [{beg}, {end}] to {path:?} {footer:?}");
            }
        }

        let stat = MemStat::from_parts(
            StatInner {
                file_id: self.file_id,
                up1: up2,
                up2,
                active_elems: nr_reloc,
                total_elems: nr_reloc,
                active_size: off,
                total_size: off,
                bucket_id: self.bucket_id,
            },
            Some(BitMap::new(nr_reloc)),
        );
        Ok((stat, reloc_map))
    }
}

pub(super) struct RewriteItem {
    pub(super) id: u64,
    pub(super) up2: u64,
    pub(super) live_bytes: usize,
    pos: Vec<Entry>,
}

pub(super) fn split_rewrite_items(
    items: Vec<RewriteItem>,
    target: usize,
    enable_compression: bool,
) -> Vec<Vec<RewriteItem>> {
    let target = target.max(1);
    let mut chunks = Vec::new();
    let mut chunk = Vec::new();
    let mut chunk_size = 0usize;

    for item in items {
        for entry in item.pos {
            let record_size = if enable_compression && entry.compressed_len != 0 {
                entry.compressed_len as usize
            } else {
                entry.raw_len as usize
            };

            if record_size >= target {
                if !chunk.is_empty() {
                    chunks.push(std::mem::take(&mut chunk));
                    chunk_size = 0;
                }
                chunks.push(vec![RewriteItem::new(item.id, item.up2, vec![entry])]);
                continue;
            }

            if chunk_size != 0 && chunk_size + record_size > target {
                chunks.push(std::mem::take(&mut chunk));
                chunk_size = 0;
            }
            chunk_size += record_size;
            if let Some(last) = chunk.last_mut()
                && last.id == item.id
                && last.up2 == item.up2
            {
                last.live_bytes += entry.raw_len as usize;
                last.pos.push(entry);
            } else {
                chunk.push(RewriteItem::new(item.id, item.up2, vec![entry]));
            }
        }
    }

    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

impl RewriteItem {
    pub(super) fn new(id: u64, up2: u64, pos: Vec<Entry>) -> Self {
        let live_bytes = pos.iter().map(|e| e.raw_len as usize).sum();
        Self {
            id,
            up2,
            live_bytes,
            pos,
        }
    }
}

pub(super) struct Entry {
    /// logical address
    pub(super) key: u64,
    /// lower bound of the source interval containing this address
    pub(super) interval_lo: u64,
    /// decoded or stored bytes to read
    pub(super) raw_len: u32,
    /// stored compressed length, 0 means raw
    pub(super) compressed_len: u32,
}

struct InactiveMap {
    ivls: Vec<Interval>,
    map: Vec<bool>,
}

impl InactiveMap {
    pub(super) fn new(ivls: &[Interval]) -> Self {
        let mut tmp: Vec<Interval> = ivls.to_vec();
        tmp.sort_unstable_by(|x, y| { x.lo }.cmp(&{ y.lo }));

        Self {
            ivls: tmp,
            map: vec![false; ivls.len()],
        }
    }

    /// test if interval still has active addr, otherwise those interval will be collected and removed
    fn test(&mut self, addr: u64) -> u64 {
        let pos = match self.ivls.binary_search_by(|x| { x.lo }.cmp(&addr)) {
            Ok(pos) => pos,
            Err(pos) => {
                if pos == 0 {
                    unreachable!("active address must belong to an interval");
                }
                pos - 1
            }
        };
        must_true!(pos < self.ivls.len());
        must_true!(addr >= self.ivls[pos].lo);
        must_true!(addr <= self.ivls[pos].hi);
        self.map[pos] = true;
        self.ivls[pos].lo
    }

    fn collect<F>(&self, mut f: F)
    where
        F: FnMut(bool, Interval),
    {
        for (idx, ivl) in self.ivls.iter().enumerate() {
            // true when not referenced
            f(!self.map[idx], *ivl);
        }
    }
}

struct RewrittenRecord {
    pub(super) raw_len: u32,
    pub(super) compressed_len: u32,
    crc: u32,
}

struct RewriteBuffers {
    io: Block,
}

impl RewriteBuffers {
    pub(super) fn new(size: usize) -> Self {
        Self {
            io: Block::alloc(size),
        }
    }

    fn ensure_io(&mut self, need: usize) {
        if self.io.len() < need {
            self.io.realloc(need);
        }
    }
}

struct RewriteCompression<'a> {
    enabled: bool,
    compressors: &'a CompressorPool,
    compressor: Option<CompressorGuard<'a>>,
}

impl<'a> RewriteCompression<'a> {
    fn enabled(&self) -> bool {
        self.enabled
    }

    fn get_or_insert(&mut self) -> &mut RecordCompressor {
        if self.compressor.is_none() {
            self.compressor = Some(must_ok!(
                self.compressors.borrow(),
                "rewrite compressor must exist"
            ));
        }
        must_exist!(self.compressor.as_mut(), "rewrite compressor must exist")
            as &mut RecordCompressor
    }
}

#[inline]
fn copy_exact<R>(
    r: &R,
    w: &mut GatherWriter,
    buf: &mut [u8],
    len: usize,
    mut off: u64,
) -> Result<u32, OpCode>
where
    R: GatherIO,
{
    let mut crc = Crc32cHasher::default();
    let mut done = 0;
    while done < len {
        let cnt = buf.len().min(len - done);
        let s = &mut buf[..cnt];
        read_exact_at(r, s, off)?;
        crc.write(s);
        w.write(s);
        done += cnt;
        off += cnt as u64;
    }
    Ok(crc.finish() as u32)
}

fn read_exact_at<R>(r: &R, buf: &mut [u8], mut off: u64) -> Result<(), OpCode>
where
    R: GatherIO,
{
    let mut done = 0;
    while done < buf.len() {
        let got = r.read(&mut buf[done..], off).map_err(|_| OpCode::IoError)?;
        if got == 0 {
            return Err(OpCode::Corruption);
        }
        done += got;
        off += got as u64;
    }
    Ok(())
}

fn write_current_record(
    w: &mut GatherWriter,
    codec: Option<&mut RecordCompressor>,
    b: &BoxRef,
) -> Result<RewrittenRecord, OpCode> {
    if let Some(codec) = codec {
        let encoded = codec.encode_box(b)?;
        if let Some(bytes) = encoded.bytes.as_ref() {
            w.write(bytes);
        } else {
            b.with_persisted_parts(|head, tail| {
                w.write(head);
                if let Some(body) = tail {
                    w.write(body);
                }
            });
        }
        return Ok(RewrittenRecord {
            raw_len: encoded.raw_len,
            compressed_len: encoded.compressed_len,
            crc: encoded.crc,
        });
    }

    let mut crc = Crc32cHasher::default();
    b.with_persisted_parts(|head, tail| {
        crc.write(head);
        w.write(head);
        if let Some(body) = tail {
            crc.write(body);
            w.write(body);
        }
    });
    Ok(RewrittenRecord {
        raw_len: b.dump_len() as u32,
        compressed_len: 0,
        crc: crc.finish() as u32,
    })
}

fn rewrite_record<'a>(
    reader: &FileReader,
    writer: &mut GatherWriter,
    buffers: &mut RewriteBuffers,
    decoder: &mut RecordDecompressor,
    compression: &mut RewriteCompression<'a>,
    entry: &Entry,
) -> Result<RewrittenRecord, OpCode> {
    let target_version = FileVersion::CURRENT;
    let reloc = must_exist!(
        reader.find_reloc(entry.key),
        "reloc must exist for {}",
        entry.key
    );
    let reloc_crc = reloc.crc;
    let raw_len = entry.raw_len as usize;
    let stored_len = if entry.compressed_len == 0 {
        raw_len
    } else {
        entry.compressed_len as usize
    };

    if reader.version().can_reuse_to(target_version) {
        if entry.compressed_len == 0 && (!compression.enabled() || raw_len < COMPRESS_MIN_LEN) {
            buffers.ensure_io(stored_len.clamp(1, 4 << 20));
            let crc = copy_exact(
                reader.file(),
                writer,
                buffers.io.mut_slice(0, buffers.io.len()),
                stored_len,
                reloc.off as u64,
            )?;
            must_true!(eq crc, reloc_crc);
            return Ok(RewrittenRecord {
                raw_len: entry.raw_len,
                compressed_len: 0,
                crc,
            });
        }

        if compression.enabled() && entry.compressed_len == 0 {
            let codec = compression.get_or_insert();
            buffers.ensure_io(stored_len);
            let src = buffers.io.mut_slice::<u8>(0, stored_len);
            read_exact_at(reader.file(), src, reloc.off as u64)?;
            let crc = crc32c::crc32c(src);
            must_true!(eq crc, reloc_crc);

            if let Some(compressed) = codec.try_compress(src)? {
                let crc = crc32c::crc32c(&compressed);
                let compressed_len = compressed.len() as u32;
                writer.write(&compressed);
                return Ok(RewrittenRecord {
                    raw_len: entry.raw_len,
                    compressed_len,
                    crc,
                });
            }

            writer.write(src);
            return Ok(RewrittenRecord {
                raw_len: entry.raw_len,
                compressed_len: 0,
                crc,
            });
        }

        if compression.enabled() && entry.compressed_len > 0 {
            buffers.ensure_io(stored_len.clamp(1, 4 << 20));
            let crc = copy_exact(
                reader.file(),
                writer,
                buffers.io.mut_slice(0, buffers.io.len()),
                stored_len,
                reloc.off as u64,
            )?;
            must_true!(eq crc, reloc_crc);
            return Ok(RewrittenRecord {
                raw_len: entry.raw_len,
                compressed_len: entry.compressed_len,
                crc,
            });
        }

        must_true!(!compression.enabled());
        if entry.compressed_len == 0 {
            buffers.ensure_io(stored_len);
            let src = buffers.io.mut_slice::<u8>(0, stored_len);
            read_exact_at(reader.file(), src, reloc.off as u64)?;
            let crc = crc32c::crc32c(src);
            must_true!(eq crc, reloc_crc);
            writer.write(src);
            return Ok(RewrittenRecord {
                raw_len: entry.raw_len,
                compressed_len: 0,
                crc,
            });
        }

        let crc = decoder.decode_to_writer(
            reader.file(),
            reloc.off as u64,
            raw_len,
            stored_len,
            writer,
        )?;
        must_true!(eq crc.stored, reloc_crc);
        return Ok(RewrittenRecord {
            raw_len: entry.raw_len,
            compressed_len: 0,
            crc: crc.raw,
        });
    }

    let record = reader.read_at(entry.key);
    let codec = if compression.enabled() && record.dump_len() >= COMPRESS_MIN_LEN {
        Some(compression.get_or_insert())
    } else {
        None
    };
    write_current_record(writer, codec, &record)
}
