use crc32c::Crc32cHasher;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    hash::Hasher,
    ops::Deref,
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering::{AcqRel, Acquire, Relaxed},
        },
        mpsc::{Receiver, RecvTimeoutError, Sender, channel},
    },
    thread::JoinHandle,
    time::Duration,
};

use crate::{
    OpCode, Options, Store,
    cc::context::Context,
    index::tree::Tree,
    io::{File, GatherIO},
    map::{
        buffer::BucketContext,
        data::{BlobFooter, DataFooter, MetaReader},
        table::{BucketState, Swip},
    },
    meta::{
        BUCKET_PENDING_DEL, BlobStatInner, DataStatInner, DelInterval, Delete, IntervalPair,
        MemBlobStat, MemDataStat, MetaKind, MetaOp, Numerics, blob_interval_name,
        data_interval_name, page_table_name,
    },
    store::VacuumStats,
    types::traits::IAsSlice,
    utils::{
        Handle, MutRef, ROOT_PID,
        bitmap::BitMap,
        block::Block,
        countblock::Countblock,
        data::{AddrPair, GatherWriter, Interval, LenSeq},
    },
};

const GC_QUIT: i32 = -1;
const GC_PAUSE: i32 = 3;
const GC_RESUME: i32 = 5;
const GC_START: i32 = 7;

fn gc_thread(mut gc: GarbageCollector, rx: Receiver<i32>, sem: Arc<Countblock>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("garbage-collector".into())
        .spawn(move || {
            let timeout = Duration::from_millis(gc.store.opt.gc_timeout);
            let mut pause = false;

            loop {
                match rx.recv_timeout(timeout) {
                    Ok(x) => match x {
                        GC_PAUSE => {
                            pause = true;
                        }
                        GC_RESUME => {
                            pause = false;
                        }
                        GC_START => {
                            gc.run();
                            sem.post();
                        }
                        GC_QUIT => break,
                        _ => unreachable!("invalid instruction  {}", x),
                    },
                    Err(RecvTimeoutError::Timeout) => {
                        if !pause {
                            gc.run();
                        }
                    }
                    Err(e) => {
                        log::error!("gc receive error {e}");
                        break;
                    }
                }
            }

            sem.post();
            log::info!("garbage-collector thread exit");
        })
        .unwrap()
}

#[derive(Clone)]
pub(crate) struct GCHandle {
    tx: Arc<Sender<i32>>,
    sem: Arc<Countblock>,
    data_runs: Arc<AtomicU64>,
    blob_runs: Arc<AtomicU64>,
}

impl GCHandle {
    pub(crate) fn quit(&self) {
        self.tx.send(GC_QUIT).unwrap();
        self.sem.wait();
    }

    pub(crate) fn pause(&self) {
        self.tx.send(GC_PAUSE).unwrap();
    }

    pub(crate) fn resume(&self) {
        self.tx.send(GC_RESUME).unwrap();
    }

    pub(crate) fn start(&self) {
        self.tx.send(GC_START).unwrap();
        self.sem.wait();
    }

    pub(crate) fn data_gc_count(&self) -> u64 {
        self.data_runs.load(Acquire)
    }

    pub(crate) fn blob_gc_count(&self) -> u64 {
        self.blob_runs.load(Acquire)
    }
}

pub(crate) fn start_gc(store: MutRef<Store>, ctx: Handle<Context>) -> GCHandle {
    let (tx, rx) = channel();
    let sem = Arc::new(Countblock::new(0));
    let data_runs = Arc::new(AtomicU64::new(0));
    let blob_runs = Arc::new(AtomicU64::new(0));
    let gc = GarbageCollector {
        numerics: ctx.numerics.clone(),
        ctx,
        store,
        data_runs: data_runs.clone(),
        blob_runs: blob_runs.clone(),
    };
    gc_thread(gc, rx, sem.clone());
    GCHandle {
        tx: Arc::new(tx),
        sem,
        data_runs,
        blob_runs,
    }
}

#[derive(Clone, Copy, Debug)]
struct Score {
    id: u64,
    size: usize,
    rate: f64,
    up2: u64,
}

impl Score {
    fn from(stat: DataStatInner, now: u64) -> Self {
        Self {
            id: stat.file_id,
            size: stat.active_size,
            rate: Self::calc_decline_rate(stat, now),
            up2: stat.up2,
        }
    }

    fn calc_decline_rate(stat: DataStatInner, now: u64) -> f64 {
        let free = stat.total_size.saturating_sub(stat.active_size);
        // no junk has been applied yet, or
        // it's possible gc and flush thread get same tick
        if free == 0 || stat.up2 == now {
            return f64::MIN;
        }

        -(stat.active_size as f64 / free as f64).powi(2)
            / (stat.total_elems as f64 * (now - stat.up2) as f64)
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match other.rate.partial_cmp(&self.rate) {
            Some(Ordering::Equal) => self.id.cmp(&other.id),
            Some(o) => o,
            None => Ordering::Equal,
        }
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Score {}

struct GarbageCollector {
    numerics: Arc<Numerics>,
    ctx: Handle<Context>,
    store: MutRef<Store>,
    data_runs: Arc<AtomicU64>,
    blob_runs: Arc<AtomicU64>,
}

impl GarbageCollector {
    const MAX_ELEMS: usize = 1024;

    fn run(&mut self) {
        self.process_wal();
        self.process_data();
        self.process_blob();
        self.process_pending_buckets();
        self.scavenge();
        self.store.manifest.delete_files();
    }

    fn process_pending_buckets(&mut self) {
        let mut bucket_id = None;
        let _ = self.store.manifest.btree.view(BUCKET_PENDING_DEL, |txn| {
            let mut iter = txn.iter();
            let mut k = Vec::new();
            let mut v = Vec::new();
            if iter.next_ref(&mut k, &mut v) {
                bucket_id = Some(<u64>::from_be_bytes(k[..8].try_into().unwrap()));
            }
            Ok(())
        });

        if let Some(bucket_id) = bucket_id {
            self.clean_one_bucket(bucket_id);
        }
    }

    fn clean_one_bucket(&mut self, bucket_id: u64) {
        let bucket_table = page_table_name(bucket_id);
        let data_interval_table = data_interval_name(bucket_id);
        let blob_interval_table = blob_interval_name(bucket_id);
        const PID_PER_ROUND: usize = 100000;

        loop {
            let mut pids = Vec::with_capacity(PID_PER_ROUND);
            let _ = self.store.manifest.btree.view(&bucket_table, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) && pids.len() < PID_PER_ROUND {
                    let pid = <u64>::from_be_bytes(k[..8].try_into().unwrap());
                    let addr = <u64>::from_be_bytes(v[..8].try_into().unwrap());
                    pids.push((pid, addr));
                }
                Ok(())
            });

            if pids.is_empty() {
                break;
            }

            let mut txn = self.store.manifest.begin();
            let ops = txn.ops_mut().entry(bucket_table.clone()).or_default();
            for (pid, _) in &pids {
                ops.push(MetaOp::Del(pid.to_be_bytes().to_vec()));
            }
            txn.commit();
        }

        // table is now empty, destroy the btree bucket and remove pending record
        let _ = self.store.manifest.btree.del_bucket(&bucket_table);
        let _ = self.store.manifest.btree.del_bucket(&data_interval_table);
        let _ = self.store.manifest.btree.del_bucket(&blob_interval_table);
        let mut txn = self.store.manifest.begin();
        txn.ops_mut()
            .entry(BUCKET_PENDING_DEL.to_string())
            .or_default()
            .push(MetaOp::Del(bucket_id.to_be_bytes().to_vec()));
        self.store.manifest.nr_buckets.fetch_sub(1, Relaxed);
        txn.commit();
    }

    fn scavenge(&mut self) {
        let g = crossbeam_epoch::pin();

        let bucket_ctxs = self.store.manifest.buckets.active_contexts();

        for ctx in bucket_ctxs {
            let bucket_id = ctx.bucket_id;
            let state = ctx.state.clone();
            let table = ctx.table.clone();
            let max_pid = table.len();
            if max_pid == 0 {
                continue;
            }
            if state.is_vacuuming() {
                continue;
            }
            // strategy: scan the entire page table approximately every 500 ticks (e.g., ~8 hours if tick=1min)
            // but keep the batch size within a reasonable range [128, 10000]
            let batch_size = (max_pid / 500).clamp(128, 10000);
            let mut compact_count = 0;
            let max_compact_per_tick = 64; // limit I/O impact

            for _ in 0..batch_size {
                if state.is_deleting() || state.is_drop() {
                    break;
                }
                if state.is_vacuuming() {
                    break;
                }

                let mut cursor = table.scavenge_cursor.load(Relaxed);

                if cursor >= max_pid {
                    cursor = 0;
                }
                table.scavenge_cursor.store(cursor + 1, Relaxed);

                if !self
                    .store
                    .manifest
                    .bucket_metas_by_id
                    .contains_key(&bucket_id)
                {
                    break; // bucket removed
                }

                let swip = Swip::new(table.get(cursor));
                if swip.is_null() {
                    continue;
                }

                let tree = Tree::new(self.store.clone(), ROOT_PID, ctx.clone());

                match tree.try_scavenge(cursor, &g) {
                    Ok(true) => {
                        compact_count += 1;
                    }
                    _ => {
                        // page is locked or busy or something else, just skip it this time
                    }
                }

                // if we reached the I/O quota, stop this batch early
                if compact_count >= max_compact_per_tick {
                    break;
                }
            }
        }
    }

    fn process_obsoleted_blob(&self, obsoleted: &[u64], bucket_id: u64) {
        if !obsoleted.is_empty() {
            let mut unlinked = Delete::default();
            let mut del_intervals = DelInterval {
                lo: Vec::new(),
                bucket_id,
            };
            obsoleted.iter().for_each(|&x| {
                let mut loader = MetaReader::<BlobFooter>::new(self.store.opt.blob_file(x))
                    .expect("never happen");
                let ivls = loader.get_interval().unwrap();
                for i in ivls {
                    del_intervals.push(i.lo);
                }
                unlinked.push(x);
            });
            let mut txn = self.store.manifest.begin();
            txn.record(MetaKind::BlobDelete, &unlinked);
            txn.record(MetaKind::BlobDelInterval, &del_intervals);
            txn.commit();

            self.store
                .manifest
                .blob_stat
                .remove_stat_interval(obsoleted);
            self.store.manifest.save_obsolete_blob(obsoleted);
            self.store.manifest.delete_files();
        }
    }

    fn process_obsoleted_data(&self, obsoleted: Vec<u64>, bucket_id: u64) {
        if !obsoleted.is_empty() {
            let mut unlinked = Delete::default();
            let mut del_intervals = DelInterval {
                lo: Vec::new(),
                bucket_id,
            };
            obsoleted.iter().for_each(|&x| {
                let mut loader = MetaReader::<DataFooter>::new(self.store.opt.data_file(x))
                    .expect("never happen");
                let ivls = loader.get_interval().unwrap();
                for i in ivls {
                    del_intervals.push(i.lo);
                }
                unlinked.push(x);
            });
            let mut txn = self.store.manifest.begin();
            txn.record(MetaKind::DataDelete, &unlinked);
            txn.record(MetaKind::DataDelInterval, &del_intervals);
            txn.commit();

            self.store
                .manifest
                .data_stat
                .remove_stat_interval(&obsoleted);
            self.store.manifest.save_obsolete_data(&obsoleted);
            self.store.manifest.delete_files();
        }
    }

    fn process_blob(&mut self) {
        let (obsoleted, victims) = self.store.manifest.blob_stat.get_victims(
            self.store.opt.blob_gc_ratio,
            self.store.opt.blob_garbage_ratio,
        );

        // NOTE: obsoleted blobs may come from different buckets,
        // but get_victims returns file_ids. We need to find their bucket_ids.
        for fid in obsoleted {
            // drop read-lock here, avoid dead-lock
            let bucket_id = self
                .store
                .manifest
                .blob_stat
                .read()
                .get(&fid)
                .map(|s| s.bucket_id);
            if let Some(bid) = bucket_id {
                self.process_obsoleted_blob(&[fid], bid);
            }
        }

        let mut groups: HashMap<u64, Vec<(u64, usize)>> = HashMap::new();
        for (fid, sz, bid) in victims {
            groups.entry(bid).or_default().push((fid, sz));
        }

        for (bucket_id, list) in groups {
            let mut dst_size = 0;
            let mut dst = Vec::new();
            for (file_id, size) in list {
                dst_size += size;
                dst.push(file_id);
                if dst_size >= self.store.opt.blob_max_size && dst.len() >= 2 {
                    self.rewrite_blob(&dst, bucket_id);
                    dst.clear();
                    dst_size = 0;
                }
            }

            if self.store.opt.gc_eager && dst.len() >= 2 {
                self.rewrite_blob(&dst, bucket_id);
            }
        }
    }

    fn process_data(&mut self) {
        let tgt_ratio = self.store.opt.data_garbage_ratio as u64;
        let tgt_size = self.store.opt.gc_compacted_size;
        let eager = self.store.opt.gc_eager;

        let buckets: Vec<u64> = self
            .store
            .manifest
            .data_stat
            .bucket_files()
            .iter()
            .map(|x: dashmap::mapref::multiple::RefMulti<'_, u64, Vec<u64>>| *x.key())
            .collect();
        let tick = self.numerics.next_data_id.load(Relaxed);

        for bucket_id in buckets {
            let files = match self.store.manifest.data_stat.bucket_files().get(&bucket_id) {
                Some(f) => f,
                None => continue,
            };

            let mut total = 0;
            let mut active = 0;
            let mut obsoleted = Vec::new();
            let mut candidates = Vec::new();

            for &fid in files.value() {
                if let Some(s) = self.store.manifest.data_stat.get(&fid) {
                    total += s.total_size as u64;
                    active += s.active_size as u64;
                    if s.active_elems == 0 {
                        obsoleted.push(fid);
                    } else {
                        // copy the inner value (DataStatInner is Copy)
                        candidates.push(s.inner);
                    }
                }
            }

            // drop the lock on bucket_files before calling rewrite_data to avoid borrow conflicts
            drop(files);

            if total == 0 {
                continue;
            }
            let ratio = (total - active) * 100 / total;
            if ratio < tgt_ratio {
                continue;
            }

            let mut heap = BinaryHeap::new();
            for s in candidates {
                let score = Score::from(s, tick);
                if heap.len() < Self::MAX_ELEMS {
                    heap.push(score);
                } else {
                    let top = heap.peek().unwrap();
                    if top.cmp(&score).is_gt() {
                        heap.pop();
                        heap.push(score);
                    }
                }
            }

            self.process_obsoleted_data(obsoleted, bucket_id);

            let mut victims = vec![];
            let mut sum = 0;
            let mut tmp = Vec::from_iter(heap);
            let mut rewritten = false;
            while let Some(x) = tmp.pop() {
                sum += x.size;
                victims.push(x);

                if sum >= tgt_size && victims.len() > 1 {
                    self.rewrite_data(victims.clone(), bucket_id);
                    rewritten = true;
                    break;
                }
            }
            if !rewritten && eager && victims.len() > 1 {
                self.rewrite_data(victims, bucket_id);
            }
        }
    }

    fn process_wal(&mut self) {
        for g in self.store.context.groups().iter() {
            let mut checkpoint_id = g.active_txns.min_position_file_id();
            let (oldest_id, last_ckpt_file) = {
                let logging = g.logging.lock();
                (logging.oldest_wal_id(), logging.last_ckpt().file_id)
            };
            checkpoint_id = std::cmp::min(checkpoint_id, last_ckpt_file);

            if oldest_id >= checkpoint_id {
                continue;
            }

            // [oldest_id, checkpoint_id)
            Self::process_one_wal(&self.store.opt, g.id as u8, oldest_id, checkpoint_id);
            g.logging.lock().advance_oldest_wal_id(checkpoint_id);
        }
    }

    fn process_one_wal(opt: &Options, id: u8, beg: u64, end: u64) {
        // NOTE: not including `end`
        for seq in beg..end {
            let from = opt.wal_file(id, seq);
            if !from.exists() {
                continue;
            }
            let to = opt.wal_backup(id, seq);
            if opt.keep_stable_wal_file {
                log::info!("rename {from:?} to {to:?}");
                std::fs::rename(&from, &to)
                    .inspect_err(|e| {
                        log::error!("can't rename {from:?} to {to:?}, error {e:?}");
                    })
                    .unwrap();
            } else {
                log::info!("unlink {from:?}");
                std::fs::remove_file(&from)
                    .inspect_err(|e| log::error!("can't remove {from:?}, error {e:?}"))
                    .unwrap();
            }
        }
    }

    fn rewrite_data(&mut self, candidate: Vec<Score>, bucket_id: u64) {
        let opt = &self.store.opt;
        let file_id = self.numerics.next_data_id.fetch_add(1, Relaxed);
        // stage orphan intent before rewrite output is flushed
        // crash can happen after file sync but before manifest commit
        self.store.manifest.stage_orphan_data_file(file_id);
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_gc_data_rewrite_after_stage_marker");
        let mut builder = DataReWriter::new(file_id, opt, candidate.len(), bucket_id);
        let mut remap_intervals = Vec::with_capacity(candidate.len());
        let mut del_intervals = DelInterval {
            lo: Vec::new(),
            bucket_id,
        };
        let mut obsoleted = Vec::new();

        self.store.manifest.data_stat.start_collect_junks(); // stop in update_stat_interval
        let victims: Vec<u64> = candidate
            .iter()
            .filter_map(|x| {
                let mut loader =
                    MetaReader::<DataFooter>::new(opt.data_file(x.id)).expect("never happen");
                let relocs = loader.get_reloc().unwrap();
                let ivls = loader.get_interval().unwrap();
                let mut im = InactiveMap::new(ivls);
                let bitmap = self
                    .store
                    .manifest
                    .data_stat
                    .load_mask_clone(x.id, &self.store.manifest.btree)
                    .expect("must exist");

                // collect active frames
                let active: Vec<Entry> = relocs
                    .iter()
                    .filter(|m| !bitmap.test(m.val.seq))
                    .map(|m| {
                        // test here because bitmap maybe full of garbage, it must be ignore
                        im.test(m.key);
                        Entry {
                            key: m.key,
                            off: m.val.off,
                            len: m.val.len,
                            crc: m.val.crc,
                        }
                    })
                    .collect();

                if active.is_empty() {
                    obsoleted.push(x.id);
                    return None;
                }
                im.collect(|unref, ivl| {
                    let Interval { lo, hi } = ivl;
                    if unref {
                        del_intervals.push(lo);
                    } else {
                        remap_intervals.push(IntervalPair::new(lo, hi, file_id, bucket_id));
                        builder.add_interval(lo, hi);
                    }
                });
                builder.add_frame(Item::new(x.id, x.up2, active));
                Some(x.id)
            })
            .collect();

        // it's possible that other thread deactived all data in data file while we are procesing
        self.process_obsoleted_data(obsoleted, bucket_id);

        // 1. perform disk I/O (build data file)
        let (mut fstat, relocs) = builder
            .build()
            .inspect_err(|e| {
                log::error!("error {e}");
            })
            .unwrap();
        fstat.inner.bucket_id = bucket_id;

        // 2. commit metadata transaction
        let mut txn = self.store.manifest.begin();
        txn.record(MetaKind::Numerics, self.store.manifest.numerics.deref());

        // the only problem is junks collected by flush thread maybe too many
        let stat = self.store.manifest.update_data_stat_interval(
            fstat,
            relocs,
            &victims,
            &del_intervals,
            &remap_intervals,
        );

        txn.record(MetaKind::DataStat, &stat);

        // 1. record delete first
        if !del_intervals.is_empty() {
            txn.record(MetaKind::DataDelInterval, &del_intervals);
        }
        // 2. then record remapping, old intervals are point to new file_id
        for i in &remap_intervals {
            txn.record(MetaKind::DataInterval, i);
        }
        // in case crash happens before/during deleting files
        let tmp: Delete = victims.into();
        txn.record(MetaKind::DataDelete, &tmp);
        // clear intent in the same metadata txn that publishes the new file
        self.store
            .manifest
            .clear_orphan_data_file(&mut txn, file_id);
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_gc_data_rewrite_before_meta_commit");
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_gc_data_rewrite_after_meta_commit");

        // 3. it's safe to clean obsolete files, because they are not referenced
        self.store.manifest.save_obsolete_data(&tmp);
        self.store.manifest.delete_files();
        self.data_runs.fetch_add(1, AcqRel);
    }

    fn rewrite_blob(&mut self, candidate: &[u64], bucket_id: u64) {
        let opt = &self.ctx.opt;
        let mut remap_intervals = Vec::new();
        let mut del_intervals = DelInterval {
            lo: Vec::new(),
            bucket_id,
        };
        let mut builder = BlobRewriter::new(opt, bucket_id);
        let blob_id = self.numerics.next_blob_id.fetch_add(1, Relaxed);
        // stage orphan intent before rewrite output is flushed
        // crash can happen after file sync but before manifest commit
        self.store.manifest.stage_orphan_blob_file(blob_id);
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_gc_blob_rewrite_after_stage_marker");
        let mut obsoleted = Vec::new();

        self.store.manifest.blob_stat.start_collect_junks();
        let victims: Vec<u64> = candidate
            .iter()
            .filter_map(|&victim_id| {
                let mut loader =
                    MetaReader::<BlobFooter>::new(opt.blob_file(victim_id)).expect("never happen");
                let relocs = loader.get_reloc().unwrap();
                let bitmap = self
                    .store
                    .manifest
                    .blob_stat
                    .load_mask_clone(victim_id, &self.store.manifest.btree)
                    .expect("must exist");
                let ivls = loader.get_interval().unwrap();
                let mut im = InactiveMap::new(ivls);

                let active: Vec<Entry> = relocs
                    .iter()
                    .filter(|x| !bitmap.test(x.val.seq))
                    .map(|x| {
                        // test here because bitmap maybe full of garbage, it must be ignore
                        im.test(x.key);
                        Entry {
                            key: x.key,
                            off: x.val.off,
                            len: x.val.len,
                            crc: x.val.crc,
                        }
                    })
                    .collect();

                if active.is_empty() {
                    obsoleted.push(victim_id);
                    return None;
                }
                im.collect(|unref, ivl| {
                    let Interval { lo, hi } = ivl;
                    if unref {
                        del_intervals.push(lo);
                    } else {
                        remap_intervals.push(IntervalPair::new(lo, hi, blob_id, bucket_id));
                        builder.add_interval(lo, hi);
                    }
                });
                builder.add_item(BlobItem::new(victim_id, active));
                Some(victim_id)
            })
            .collect();

        // it's possible that other thread deactivated all data in blob file while we are processing
        self.process_obsoleted_blob(&obsoleted, bucket_id);

        // 1. perform disk I/O (build blob file)
        let (mut bstat, reloc) = builder
            .build(blob_id)
            .inspect_err(|e| {
                log::error!("error {e:?}");
            })
            .unwrap();
        bstat.inner.bucket_id = bucket_id;

        // 2. commit metadata transaction
        let mut txn = self.store.manifest.begin();
        txn.record(MetaKind::Numerics, self.store.manifest.numerics.deref());

        let stat = self.store.manifest.update_blob_stat_interval(
            bstat,
            reloc,
            &victims,
            &del_intervals,
            &remap_intervals,
        );
        txn.record(MetaKind::BlobStat, &stat);

        if !del_intervals.is_empty() {
            txn.record(MetaKind::BlobDelInterval, &del_intervals);
        }

        for i in &remap_intervals {
            txn.record(MetaKind::BlobInterval, i);
        }

        let tmp: Delete = victims.into();
        txn.record(MetaKind::BlobDelete, &tmp);
        // clear intent in the same metadata txn that publishes the new file
        self.store
            .manifest
            .clear_orphan_blob_file(&mut txn, blob_id);
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_gc_blob_rewrite_before_meta_commit");

        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_gc_blob_rewrite_after_meta_commit");

        self.store.manifest.save_obsolete_blob(&tmp);
        self.store.manifest.delete_files();
        self.blob_runs.fetch_add(1, AcqRel);
    }
}

pub(crate) fn vacuum_bucket(
    store: MutRef<Store>,
    bucket_ctx: Arc<BucketContext>,
) -> Result<VacuumStats, OpCode> {
    let state = bucket_ctx.state.clone();
    let start_epoch = state.vacuum_epoch();
    let mut stats = VacuumStats::default();

    loop {
        if state.is_deleting() || state.is_drop() {
            return Err(OpCode::Again);
        }
        if state.try_begin_vacuum() {
            break;
        }
        if state.vacuum_epoch() != start_epoch {
            return Ok(stats);
        }
        state.wait_vacuum(start_epoch);
        if state.vacuum_epoch() != start_epoch {
            return Ok(stats);
        }
    }

    let _guard = VacuumGuard::new(state.clone());
    if state.is_deleting() || state.is_drop() {
        return Err(OpCode::Again);
    }

    let g = crossbeam_epoch::pin();
    let bucket_id = bucket_ctx.bucket_id;
    let table = bucket_ctx.table.clone();
    let max_pid = table.len();
    if max_pid == 0 {
        return Ok(stats);
    }

    let tree = Tree::new(store.clone(), ROOT_PID, bucket_ctx);

    for pid in ROOT_PID..max_pid {
        if state.is_deleting() || state.is_drop() {
            break;
        }
        if store.manifest.bucket_metas_by_id.get(&bucket_id).is_none() {
            break;
        }

        let swip = Swip::new(table.get(pid));
        if swip.is_null() {
            continue;
        }

        stats.scanned += 1;
        if matches!(tree.try_scavenge(pid, &g), Ok(true)) {
            stats.compacted += 1;
        }
    }

    Ok(stats)
}

struct VacuumGuard {
    state: MutRef<BucketState>,
}

impl VacuumGuard {
    fn new(state: MutRef<BucketState>) -> Self {
        Self { state }
    }
}

impl Drop for VacuumGuard {
    fn drop(&mut self) {
        self.state.end_vacuum();
    }
}

struct DataReWriter<'a> {
    file_id: u64,
    items: Vec<Item>,
    intervals: Vec<u8>,
    nr_interval: u32,
    sum_up2: u64,
    total: u64,
    opt: &'a Options,
    bucket_id: u64,
}

impl<'a> DataReWriter<'a> {
    fn new(file_id: u64, opt: &'a Options, cap: usize, bucket_id: u64) -> Self {
        Self {
            file_id,
            items: Vec::with_capacity(cap),
            intervals: Vec::with_capacity(cap),
            nr_interval: 0,
            sum_up2: 0,
            total: cap as u64,
            opt,
            bucket_id,
        }
    }

    fn add_frame(&mut self, item: Item) {
        self.sum_up2 += item.up2;
        self.items.push(item);
    }

    fn add_interval(&mut self, lo: u64, hi: u64) {
        let ivl = Interval::new(lo, hi);
        self.intervals.extend_from_slice(ivl.as_slice());
        self.nr_interval += 1;
    }

    fn build(&mut self) -> Result<(MemDataStat, HashMap<u64, LenSeq>), OpCode> {
        let up2 = self.sum_up2 / self.total;
        let block = Block::alloc(1 << 20);
        let mut seq = 0;
        let mut off = 0;
        let path = self.opt.data_file(self.file_id);
        let mut writer = GatherWriter::append(&path, 128);
        let mut reloc: Vec<u8> = Vec::new();
        let mut reloc_map = HashMap::new();
        let buf = block.mut_slice(0, block.len());

        self.items.sort_unstable_by(|x, y| x.id.cmp(&y.id));

        for item in &self.items {
            let reader = File::options()
                .read(true)
                .open(self.opt.data_file(item.id))
                .unwrap();
            for e in &item.pos {
                let len = e.len as usize;
                let crc = copy(&reader, &mut writer, buf, len, e.off as u64)?;
                assert_eq!(crc, e.crc);
                let m = AddrPair::new(e.key, off, e.len, seq, crc);
                reloc.extend_from_slice(m.as_slice());
                reloc_map.insert(e.key, LenSeq::new(e.len, seq));
                off += len;
                seq += 1;
            }
        }

        let mut interval_crc = Crc32cHasher::default();
        let is = self.intervals.as_slice();
        interval_crc.write(is);
        writer.queue(is);

        let mut reloc_crc = Crc32cHasher::default();
        let s = reloc.as_slice();
        reloc_crc.write(s);
        writer.queue(s);

        let footer = DataFooter {
            up2,
            nr_reloc: seq,
            nr_intervals: self.nr_interval,
            reloc_crc: reloc_crc.finish() as u32,
            interval_crc: interval_crc.finish() as u32,
        };

        writer.queue(footer.as_slice());

        writer.flush();
        writer.sync();
        log::info!("compacted to {path:?} {footer:?}");

        let stat = MemDataStat {
            inner: DataStatInner {
                file_id: self.file_id,
                up1: up2,
                up2,
                active_elems: seq,
                total_elems: seq,
                active_size: off,
                total_size: off,
                bucket_id: self.bucket_id,
            },
            mask: Some(BitMap::new(seq)),
        };
        Ok((stat, reloc_map))
    }
}

struct BlobRewriter<'a> {
    opt: &'a Options,
    items: Vec<BlobItem>,
    intervals: Vec<u8>,
    nr_interval: u32,
    bucket_id: u64,
}

impl<'a> BlobRewriter<'a> {
    fn new(opt: &'a Options, bucket_id: u64) -> Self {
        Self {
            opt,
            items: Vec::new(),
            intervals: Vec::new(),
            nr_interval: 0,
            bucket_id,
        }
    }

    fn add_item(&mut self, item: BlobItem) {
        self.items.push(item);
    }

    fn add_interval(&mut self, lo: u64, hi: u64) {
        let ivl = Interval::new(lo, hi);
        self.intervals.extend_from_slice(ivl.as_slice());
        self.nr_interval += 1;
    }

    fn build(&mut self, file_id: u64) -> Result<(MemBlobStat, HashMap<u64, LenSeq>), OpCode> {
        let path = self.opt.blob_file(file_id);
        let mut w = GatherWriter::trunc(&path, 8);
        let mut off = 0;
        let mut seq = 0;
        let mut reloc = Vec::new();
        let mut map = HashMap::new();
        let block = Block::alloc(4 << 20);
        let buf = block.mut_slice(0, block.len());

        #[cfg(feature = "extra_check")]
        assert!(self.items.is_sorted_by_key(|x| x.id));

        let mut beg = u64::MAX;
        let mut end = u64::MIN;
        for item in &self.items {
            beg = beg.min(item.id);
            end = end.max(item.id);

            let reader = File::options()
                .read(true)
                .open(self.opt.blob_file(item.id))
                .unwrap();

            for e in &item.pos {
                let len = e.len as usize;
                let crc = copy(&reader, &mut w, buf, len, e.off as u64)?;
                assert_eq!(crc, e.crc);
                let m = AddrPair::new(e.key, off, e.len, seq, crc);
                reloc.extend_from_slice(m.as_slice());
                map.insert(e.key, LenSeq::new(e.len, seq));
                off += len;
                seq += 1;
            }
        }

        let mut interval_crc = Crc32cHasher::default();
        let is = self.intervals.as_slice();
        interval_crc.write(is);
        w.queue(is);

        let mut reloc_crc = Crc32cHasher::default();
        let rs = reloc.as_slice();
        reloc_crc.write(rs);
        w.queue(rs);

        let footer = BlobFooter {
            nr_reloc: seq,
            nr_intervals: self.nr_interval,
            reloc_crc: reloc_crc.finish() as u32,
            interval_crc: interval_crc.finish() as u32,
        };

        w.queue(footer.as_slice());
        w.flush();
        w.sync();
        log::info!("compacted [{beg}, {end}] to {path:?} {footer:?}");
        let stat = MemBlobStat {
            inner: BlobStatInner {
                file_id,
                active_size: off,
                nr_active: seq,
                nr_total: seq,
                bucket_id: self.bucket_id,
            },
            mask: Some(BitMap::new(seq)),
        };
        Ok((stat, map))
    }
}

struct Item {
    id: u64,
    up2: u64,
    pos: Vec<Entry>,
}

struct BlobItem {
    id: u64,
    pos: Vec<Entry>,
}

impl Item {
    const fn new(id: u64, up2: u64, pos: Vec<Entry>) -> Self {
        Self { id, up2, pos }
    }
}

impl BlobItem {
    const fn new(id: u64, pos: Vec<Entry>) -> Self {
        Self { id, pos }
    }
}

struct Entry {
    /// logical address
    key: u64,
    /// offset in data file
    off: usize,
    /// length of dumpped BoxRef
    len: u32,
    /// old checksum
    crc: u32,
}

struct InactiveMap {
    ivls: Vec<Interval>,
    map: Vec<bool>,
}

impl InactiveMap {
    fn new(ivls: &[Interval]) -> Self {
        let mut tmp: Vec<Interval> = ivls.to_vec();
        tmp.sort_unstable_by(|x, y| { x.lo }.cmp(&{ y.lo }));

        Self {
            ivls: tmp,
            map: vec![false; ivls.len()],
        }
    }

    /// test if interval still has active addr, otherwise those interval will be collected and removed
    fn test(&mut self, addr: u64) {
        let pos = match self.ivls.binary_search_by(|x| { x.lo }.cmp(&addr)) {
            Ok(pos) => pos,
            Err(pos) => {
                if pos == 0 {
                    return;
                }
                pos - 1
            }
        };
        assert!(pos < self.ivls.len());
        assert!(addr >= self.ivls[pos].lo);
        self.map[pos] = true;
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

fn copy<R>(
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
    let mut n = 0;
    let buf_sz = buf.len();

    while n < len {
        let cnt = buf_sz.min(len - n);
        let s = &mut buf[0..cnt];
        r.read(s, off).map_err(|e| {
            log::error!("can't read, {:?}", e);
            OpCode::IoError
        })?;
        crc.write(s);
        // the data will be reused next time, so we write data to file instead of queue it
        w.write(s);
        off += cnt as u64;
        n += cnt;
    }
    Ok(crc.finish() as u32)
}
