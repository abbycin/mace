use parking_lot::Mutex;

#[cfg(feature = "failpoints")]
use crate::OpCode;
use crate::cc::context::Context;
use crate::map::IDataReader;
use crate::map::flush::{CheckpointObserver, FlushDirective, FlushResult};
use crate::meta::{FileKind, IntervalPair, Manifest, MemStat, MetaKind, PersistStat, Txn};
use crate::must_ok;
use crate::types::refbox::{BoxRef, BoxView};
use crate::utils::Handle;
use crate::utils::data::init_group_pos;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Default)]
struct FlushStatDeltaKind {
    old_stats: Vec<PersistStat>,
    new_stats: Vec<PersistStat>,
}

struct FlushStatDelta {
    kinds: [FlushStatDeltaKind; 2],
}

impl FlushStatDelta {
    fn kind(&self, kind: FileKind) -> &FlushStatDeltaKind {
        &self.kinds[kind.slot()]
    }

    fn kind_mut(&mut self, kind: FileKind) -> &mut FlushStatDeltaKind {
        &mut self.kinds[kind.slot()]
    }
}

pub(crate) struct ManifestDataReader {
    meta: Handle<Manifest>,
}

impl ManifestDataReader {
    pub(crate) fn new(meta: Handle<Manifest>) -> Self {
        Self { meta }
    }
}

impl IDataReader for ManifestDataReader {
    fn load_data(&self, bucket_id: u64, addr: u64, cache: &dyn Fn(BoxRef)) -> BoxRef {
        self.meta.load_data(bucket_id, addr, cache)
    }

    fn load_blob(&self, bucket_id: u64, addr: u64, cache: &dyn Fn(BoxView)) -> BoxRef {
        self.meta.load_blob(bucket_id, addr, cache)
    }
}

pub(crate) struct ManifestCheckpointObserver {
    manifest: Handle<Manifest>,
    ctx: Handle<Context>,
    on_finish: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl ManifestCheckpointObserver {
    pub(crate) fn new(manifest: Handle<Manifest>, ctx: Handle<Context>) -> Self {
        Self {
            manifest,
            ctx,
            on_finish: Mutex::new(None),
        }
    }

    pub(crate) fn set_finish_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        self.on_finish.lock().replace(hook);
    }

    #[cfg(feature = "failpoints")]
    #[cold]
    fn abort_flush_publish(stage: &str, err: OpCode) -> ! {
        log::error!("flush publish {} failed: {:?}", stage, err);
        std::process::abort()
    }

    fn next_tick(&self, bucket_id: u64, kind: FileKind) -> u64 {
        self.manifest
            .get_bucket_runtime(bucket_id)
            .load_update_epoch(kind)
    }

    fn interval_meta_kind(kind: FileKind) -> MetaKind {
        match kind {
            FileKind::Data => MetaKind::DataInterval,
            FileKind::Blob => MetaKind::BlobInterval,
        }
    }

    fn stat_meta_kind(kind: FileKind) -> MetaKind {
        match kind {
            FileKind::Data => MetaKind::DataStat,
            FileKind::Blob => MetaKind::BlobStat,
        }
    }

    fn record_stat_update(txn: &mut Txn<'_>, kind: FileKind, stat: &PersistStat) {
        match kind {
            FileKind::Data => txn.record_data_stat_update(stat),
            FileKind::Blob => txn.record_blob_stat_update(stat),
        }
    }

    fn update_stat_interval(&self, txn: &mut Txn, result: &mut FlushResult) -> FlushStatDelta {
        let bucket_id = result.bucket_id;
        let mut delta = FlushStatDelta {
            kinds: [FlushStatDeltaKind::default(), FlushStatDeltaKind::default()],
        };

        for kind in FileKind::ALL {
            let tick = result
                .kind(kind)
                .ivls
                .first()
                .map(|x| x.file_id)
                .map(|_| {
                    self.manifest
                        .get_bucket_runtime(bucket_id)
                        .next_update_epoch(kind)
                })
                .unwrap_or_else(|| self.next_tick(bucket_id, kind));
            let mut by_file = BTreeMap::<u64, PersistStat>::new();
            for stat in self
                .manifest
                .apply_junks(kind, bucket_id, tick, &result.kind(kind).junk)
            {
                by_file.insert(stat.file_id, stat);
            }
            delta.kind_mut(kind).old_stats = by_file.into_values().collect();
        }

        #[cfg(feature = "failpoints")]
        if FileKind::ALL
            .iter()
            .any(|&kind| !delta.kind(kind).old_stats.is_empty())
        {
            crate::utils::failpoint::crash("mace_flush_after_old_stat_delta");
        }

        for kind in FileKind::ALL {
            #[cfg(feature = "extra_check")]
            assert_eq!(result.kind(kind).stats.len(), result.kind(kind).ivls.len());

            let ivls = result.kind(kind).ivls.clone();
            let slot = result.kind_mut(kind);
            let mut new_stats = Vec::with_capacity(slot.stats.len());
            for (mem_stat, ivl) in slot.stats.drain(..).zip(ivls) {
                new_stats.push(mem_stat);
                self.manifest.clear_orphan_file(kind, txn, ivl.file_id);
            }
            delta.kind_mut(kind).new_stats = new_stats;
        }
        delta
    }

    fn publish(&self, mut result: FlushResult) {
        let has_new_files = result.has_new_files();
        let bucket_id = result.bucket_id;
        let retired_stat_snapshot = self.manifest.snapshot_retired_stat_keys(bucket_id);
        let frontier_delta = *result.latest_chkpoint_lsn.deref();
        let previous_frontier = self
            .manifest
            .bucket_frontier
            .get(&bucket_id)
            .map(|x| *x.value())
            .unwrap_or_else(init_group_pos);
        let groups = self.ctx.groups();

        // page checkpoint can fold uncleaned txn versions into durable pages, recovery still needs
        // the corresponding WAL tail to rebuild tx outcomes before safe_txid can expose them
        for (i, g) in groups.iter().enumerate() {
            if i < frontier_delta.len() && frontier_delta[i] > previous_frontier[i] {
                let mut log = g.logging.lock();
                must_ok!(log.sync_checkpoint_barrier());
            }
        }

        result.sync();
        if has_new_files {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_flush_after_data_dir_sync");
        }
        let bucket_frontier = self
            .manifest
            .merge_bucket_frontier(bucket_id, &frontier_delta);
        let mut txn = self.manifest.begin();
        let stat_delta = self.update_stat_interval(&mut txn, &mut result);

        for kind in FileKind::ALL {
            for ivl in &result.kind(kind).ivls {
                txn.record(Self::interval_meta_kind(kind), ivl);
            }
            stat_delta
                .kind(kind)
                .old_stats
                .iter()
                .filter(|x| !self.manifest.is_retired_stat(kind, bucket_id, x.file_id))
                .for_each(|x| Self::record_stat_update(&mut txn, kind, x));
            stat_delta
                .kind(kind)
                .new_stats
                .iter()
                .for_each(|x| txn.record(Self::stat_meta_kind(kind), x));
        }

        txn.record(MetaKind::BucketFrontier, &bucket_frontier);
        txn.record(MetaKind::Map, &result.map_table);
        txn.record(MetaKind::Sequences, self.manifest.sequences.deref());

        #[cfg(feature = "failpoints")]
        if let Err(e) = crate::utils::failpoint::check("mace_flush_before_manifest_commit") {
            Self::abort_flush_publish("before manifest commit", e);
        }
        txn.commit();
        self.manifest.clear_retired_stat_keys(retired_stat_snapshot);
        for kind in FileKind::ALL {
            self.manifest.clear_synced_files(kind);
        }

        #[cfg(feature = "failpoints")]
        if let Err(e) = crate::utils::failpoint::check("mace_flush_after_manifest_commit") {
            Self::abort_flush_publish("after manifest commit", e);
        }

        let groups = self.ctx.groups();
        let sync = self.ctx.opt.sync_on_write;
        let global_frontier = self.manifest.global_frontier_lower_bound(groups.len());

        for (i, g) in groups.iter().enumerate() {
            let mut pos = global_frontier[i];
            let mut lk = g.logging.lock();
            if let Some(min) = g.min_active_lsn(&mut lk)
                && min < pos
            {
                pos = min;
            }
            if lk.update_checkpoint(pos) && sync {
                let mut f = lk.writer.clone();
                drop(lk);
                // checkpoint must be synced in durable mode
                f.sync();
            }
        }
    }
}

impl CheckpointObserver for ManifestCheckpointObserver {
    fn flush_directive(&self, bucket_id: u64) -> FlushDirective {
        match self.manifest.bucket_runtimes.get(&bucket_id) {
            Some(runtime) => {
                if runtime.state.is_deleting() {
                    return FlushDirective::Skip;
                }
                FlushDirective::Normal
            }
            None => FlushDirective::Skip,
        }
    }

    fn next_update_epoch(&self, bucket_id: u64, kind: FileKind) -> u64 {
        self.manifest
            .get_bucket_runtime(bucket_id)
            .next_update_epoch(kind)
    }

    fn stage_unsynced_file(&self, kind: FileKind, file_id: u64) {
        self.manifest.stage_unsynced_file(kind, file_id);
    }

    fn stage_orphan_file(&self, kind: FileKind, file_id: u64) {
        self.manifest.stage_orphan_file(kind, file_id);
    }

    fn update_mem_interval_stat(&self, kind: FileKind, ivl: IntervalPair, stat: MemStat) {
        self.manifest.add_stat(kind, stat, ivl);
    }

    fn on_checkpoint(&self, result: FlushResult) {
        self.publish(result)
    }

    fn finish_checkpoint(&self) {
        if let Some(hook) = self.on_finish.lock().as_ref().cloned() {
            hook();
        }
    }
}
