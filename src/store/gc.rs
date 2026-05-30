use crc32c::Crc32cHasher;
use crossbeam_epoch::Guard;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    hash::Hasher,
    ops::Deref,
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering::{AcqRel, Relaxed},
        },
        mpsc::{Receiver, RecvTimeoutError, Sender, channel},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crate::{
    OpCode, Options, Store,
    cc::{
        context::{AbortCleanState, AbortCleanTask, Context},
        wal::{EntryType, WalBegin, WalCommit, WalUpdate, ptr_to, wal_record_sz},
    },
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
        data::{AddrPair, GatherWriter, Interval, LenSeq, Position},
        lru::Lru,
        observe::{CounterMetric, EventKind, HistogramMetric, ObserveEvent},
    },
};

const GC_QUIT: i32 = -1;
const GC_PAUSE: i32 = 3;
const GC_RESUME: i32 = 5;
const GC_START: i32 = 7;
const GC_WAL: i32 = 11;

fn gc_thread(mut gc: GarbageCollector, rx: Receiver<i32>, sem: Arc<Countblock>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("garbage-collector".into())
        .spawn(move || {
            let timeout = Duration::from_millis(gc.store.opt.gc_timeout);
            let mut pause = false;
            let mut next_run_at = Instant::now() + timeout;

            loop {
                let wait_timeout = if pause {
                    timeout
                } else {
                    next_run_at.saturating_duration_since(Instant::now())
                };
                match rx.recv_timeout(wait_timeout) {
                    Ok(x) => match x {
                        GC_PAUSE => {
                            pause = true;
                            sem.post();
                        }
                        GC_RESUME => {
                            pause = false;
                            next_run_at = Instant::now() + timeout;
                            sem.post();
                        }
                        GC_START => {
                            gc.run();
                            if !pause {
                                next_run_at = Instant::now() + timeout;
                            }
                            sem.post();
                        }
                        GC_WAL => {
                            GarbageCollector::process_wal_clean(gc.ctx);
                        }
                        GC_QUIT => break,
                        _ => unreachable!("invalid instruction  {}", x),
                    },
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(e) => {
                        log::error!("gc receive error {e}");
                        break;
                    }
                }

                if !pause && Instant::now() >= next_run_at {
                    gc.run();
                    next_run_at = Instant::now() + timeout;
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
        self.sem.wait();
    }

    pub(crate) fn resume(&self) {
        self.tx.send(GC_RESUME).unwrap();
        self.sem.wait();
    }

    pub(crate) fn start(&self) {
        self.tx.send(GC_START).unwrap();
        self.sem.wait();
    }

    pub(crate) fn wal_clean(&self, ctx: Handle<Context>) {
        if self.tx.send(GC_WAL).is_err() {
            GarbageCollector::process_wal_clean(ctx);
        }
    }

    pub(crate) fn data_gc_count(&self) -> u64 {
        self.data_runs.load(Relaxed)
    }

    pub(crate) fn blob_gc_count(&self) -> u64 {
        self.blob_runs.load(Relaxed)
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
    bucket_id: u64,
}

impl Score {
    fn from(stat: DataStatInner, now: u64) -> Self {
        Self {
            id: stat.file_id,
            size: stat.active_size,
            rate: Self::calc_decline_rate(stat, now),
            up2: stat.up2,
            bucket_id: stat.bucket_id,
        }
    }

    fn calc_decline_rate(stat: DataStatInner, now: u64) -> f64 {
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
            .then_with(|| self.id.cmp(&other.id))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BlobVictim {
    file_id: u64,
    active_size: usize,
    bucket_id: u64,
    nr_active: u32,
    nr_total: u32,
}

impl BlobVictim {
    fn from(inner: BlobStatInner) -> Self {
        Self {
            file_id: inner.file_id,
            active_size: inner.active_size,
            bucket_id: inner.bucket_id,
            nr_active: inner.nr_active,
            nr_total: inner.nr_total.max(1),
        }
    }

    fn cmp_utilization(&self, other: &Self) -> Ordering {
        let lhs = self.nr_active as u128 * other.nr_total as u128;
        let rhs = other.nr_active as u128 * self.nr_total as u128;
        lhs.cmp(&rhs)
            .then_with(|| self.nr_active.cmp(&other.nr_active))
            .then_with(|| self.nr_total.cmp(&other.nr_total))
            .then_with(|| self.file_id.cmp(&other.file_id))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp_priority(other)
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

struct AbortCleanProgress {
    stabilize_buckets: HashSet<u64>,
}

impl GarbageCollector {
    const MAX_ELEMS: usize = 1024;
    const ABORT_CLEAN_TREE_CACHE_CAP: usize = 64;
    const ABORT_CLEAN_WAL_FILE_CACHE_CAP: usize = 16;

    fn run(&mut self) {
        let started = Instant::now();
        self.store.opt.observer.counter(CounterMetric::GcRun, 1);
        self.process_abort_clean();
        Self::process_wal_clean(self.ctx);
        self.process_data();
        self.process_blob();
        self.process_pending_buckets();
        self.scavenge();
        self.store.manifest.delete_files();
        self.store.opt.observer.histogram(
            HistogramMetric::GcRunMicros,
            started.elapsed().as_micros() as u64,
        );
    }

    fn process_wal_clean(ctx: Handle<Context>) {
        let mut log_dir_dirty = false;
        for g in ctx.groups().iter() {
            let mut checkpoint_id = g.active_txns.min_position_file_id();
            if let Some(min_pending_file) = ctx.min_file_id(g.id as u8) {
                checkpoint_id = checkpoint_id.min(min_pending_file);
            }
            let (oldest_id, last_ckpt_file) = {
                let mut logging = g.logging.lock();
                if ctx.opt.sync_on_write
                    && let Err(e) = logging.sync(false)
                {
                    log::error!("wal sync fail, group {}, error {:?}", g.id, e);
                    continue;
                }
                (logging.oldest_wal_id(), logging.last_ckpt().file_id)
            };
            checkpoint_id = checkpoint_id.min(last_ckpt_file);
            if oldest_id >= checkpoint_id {
                continue;
            }

            // [oldest_id, checkpoint_id)
            let recycled = Self::process_one_wal(ctx, g.id as u8, oldest_id, checkpoint_id);
            if recycled > 0 {
                log_dir_dirty = true;
                ctx.opt
                    .observer
                    .counter(CounterMetric::GcWalRecycleFile, recycled);
            }
            g.logging.lock().advance_oldest_wal_id(checkpoint_id);
        }
        if log_dir_dirty {
            ctx.opt.sync_log_dir();
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_after_dir_sync");
        }
    }

    fn process_one_wal(ctx: Handle<Context>, group: u8, beg: u64, end: u64) -> u64 {
        let mut recycled = 0;
        // NOTE: not including `end`
        for seq in beg..end {
            let from = ctx.opt.wal_file(group, seq);
            if !from.exists() {
                continue;
            }
            let to = ctx.opt.wal_backup(group, seq);
            if ctx.opt.keep_stable_wal_file {
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
            recycled += 1;
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_after_remove_before_dir_sync");
        }
        recycled
    }

    fn process_abort_clean(&mut self) {
        for txid in self.ctx.drain_abort_clean_events() {
            self.ctx.mark_abort_clean_quiesced(txid);
        }

        let tasks = self.ctx.abort_clean_tasks();
        if tasks.is_empty() {
            return;
        }

        let mut block = Block::alloc(1024);
        let trees = Lru::<u64, Option<Tree>>::new();
        let g = crossbeam_epoch::pin();
        let mut queued_quiesce = false;
        let mut round_stabilize_buckets = HashSet::new();
        let mut cleaned_txids = Vec::new();
        for task in tasks {
            match task.state {
                AbortCleanState::Pending => {
                    match self.clean_one_abort_task(&g, task, &trees, &mut block) {
                        Ok(progress) => {
                            round_stabilize_buckets.extend(progress.stabilize_buckets);
                            cleaned_txids.push(task.txid);
                        }
                        Err(OpCode::Again) => {}
                        Err(e) => {
                            log::error!("abort clean failed, txid={} error={:?}", task.txid, e);
                        }
                    }
                }
                AbortCleanState::WaitingQuiesce => {
                    if task.quiesced {
                        self.ctx.remove_abort_clean(task.txid);
                        self.ctx.del_aborted(task.txid);
                    }
                }
            }
        }

        let checkpoint_ok = if round_stabilize_buckets.is_empty() {
            true
        } else {
            match self.stabilize_cleaned_pages(&round_stabilize_buckets, &trees) {
                Ok(()) => true,
                Err(e) => {
                    log::error!(
                        "abort clean checkpoint batch failed, tasks={}, buckets={}, error={:?}",
                        cleaned_txids.len(),
                        round_stabilize_buckets.len(),
                        e
                    );
                    false
                }
            }
        };

        if checkpoint_ok {
            let sink = self.ctx.abort_clean_event_sink();
            for txid in cleaned_txids {
                self.ctx.mark_abort_clean_wait_quiesce(txid);
                let sink = sink.clone();
                g.defer(move || {
                    sink.lock().push(txid);
                });
                queued_quiesce = true;
            }
        }

        if queued_quiesce {
            g.flush();
        }
    }

    fn stabilize_cleaned_pages(
        &self,
        dirty_buckets: &HashSet<u64>,
        trees: &Lru<u64, Option<Tree>>,
    ) -> Result<(), OpCode> {
        for &bucket_id in dirty_buckets {
            let tree = match trees.get(&bucket_id) {
                Some(tree) => tree.clone(),
                None => self.get_tree(trees, bucket_id)?,
            };
            if let Some(tree) = tree {
                tree.bucket.checkpoint_and_wait();
                self.ctx
                    .opt
                    .observer
                    .counter(CounterMetric::GcAbortCleanCheckpointBucket, 1);
            }
        }
        Ok(())
    }

    fn get_tree(
        &self,
        cache: &Lru<u64, Option<Tree>>,
        bucket_id: u64,
    ) -> Result<Option<Tree>, OpCode> {
        if let Some(tree) = cache.get(&bucket_id) {
            return Ok(tree.clone());
        }

        let tree = if self
            .store
            .manifest
            .bucket_metas_by_id
            .get(&bucket_id)
            .is_none()
        {
            None
        } else {
            match self.store.manifest.load_bucket_context(bucket_id) {
                Ok(ctx) => Some(Tree::new(self.store.clone(), ROOT_PID, ctx)),
                Err(OpCode::NotFound) => None,
                Err(e) => return Err(e),
            }
        };
        cache.add(Self::ABORT_CLEAN_TREE_CACHE_CAP, bucket_id, tree.clone());
        Ok(tree)
    }

    fn clean_one_abort_task(
        &self,
        g: &Guard,
        task: AbortCleanTask,
        trees: &Lru<u64, Option<Tree>>,
        block: &mut Block,
    ) -> Result<AbortCleanProgress, OpCode> {
        let mut cursor = task.tail_lsn;
        let mut stabilize_buckets = HashSet::new();
        let wal_files = Lru::<u64, (File, u64)>::new();
        let mut seen_keys = HashSet::<(u64, Vec<u8>)>::new();

        loop {
            if wal_files.get(&cursor.file_id).is_none() {
                let path = self.ctx.opt.wal_file(task.group_id, cursor.file_id);
                if !path.exists() {
                    return Err(OpCode::Corruption);
                }
                let file = File::options().read(true).open(&path)?;
                let end = file.size()?;
                wal_files.add(
                    Self::ABORT_CLEAN_WAL_FILE_CACHE_CAP,
                    cursor.file_id,
                    (file, end),
                );
                self.ctx
                    .opt
                    .observer
                    .counter(CounterMetric::GcAbortCleanWalFileOpen, 1);
            }

            let cache_guard = wal_files.get(&cursor.file_id).ok_or(OpCode::Corruption)?;
            let (file, end) = (&cache_guard.0, cache_guard.1);
            if cursor.offset >= end {
                return Err(OpCode::Corruption);
            }

            let header = block.mut_slice(0, 1);
            file.read(header, cursor.offset)?;
            let et: EntryType = header[0].try_into()?;
            let sz = wal_record_sz(et)?;
            if cursor.offset + sz as u64 > end {
                return Err(OpCode::Corruption);
            }
            if block.len() < sz {
                block.realloc(sz);
            }
            file.read(block.mut_slice(0, sz), cursor.offset)?;

            match et {
                EntryType::Update => {
                    let u = ptr_to::<WalUpdate>(block.data());
                    let payload_len = u.payload_len();
                    let total = sz + payload_len;
                    if cursor.offset + total as u64 > end {
                        return Err(OpCode::Corruption);
                    }
                    if block.len() < total {
                        block.realloc(total);
                    }
                    file.read(block.mut_slice(sz, payload_len), cursor.offset + sz as u64)?;
                    let u = ptr_to::<WalUpdate>(block.data());
                    if !u.is_intact() {
                        return Err(OpCode::Corruption);
                    }

                    let txid = { u.txid };
                    if txid != task.txid {
                        return Err(OpCode::Corruption);
                    }

                    let bucket_id = { u.bucket_id };
                    if let Some(tree) = self.get_tree(trees, bucket_id)? {
                        // a foreground retry may have already removed the aborted head from memory
                        // without checkpointing it, so any live bucket touched by this abort chain
                        // still needs a durability barrier before we can retire the abort task
                        stabilize_buckets.insert(bucket_id);
                        let raw = u.key();
                        if !seen_keys.insert((bucket_id, raw.to_vec())) {
                            cursor = Position {
                                file_id: { u.prev_id },
                                offset: { u.prev_off },
                            };
                            continue;
                        }
                        loop {
                            match tree.remove_aborted(g, raw) {
                                Ok(_) => break,
                                Err(OpCode::Again) => g.flush(),
                                Err(e) => return Err(e),
                            }
                        }
                    }

                    cursor = Position {
                        file_id: { u.prev_id },
                        offset: { u.prev_off },
                    };
                }
                EntryType::Begin => {
                    let b = ptr_to::<WalBegin>(block.data());
                    if !b.is_intact() || { b.txid } != task.txid {
                        return Err(OpCode::Corruption);
                    }
                    return Ok(AbortCleanProgress { stabilize_buckets });
                }
                EntryType::Abort | EntryType::Commit => {
                    let c = ptr_to::<WalCommit>(block.data());
                    if !c.is_intact() || { c.txid } != task.txid {
                        return Err(OpCode::Corruption);
                    }
                    return Ok(AbortCleanProgress { stabilize_buckets });
                }
                _ => return Err(OpCode::Corruption),
            }
        }
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
            let removed_pages = self.clean_one_bucket(bucket_id);
            self.store
                .opt
                .observer
                .counter(CounterMetric::GcPendingBucketClean, 1);
            self.store.opt.observer.event(ObserveEvent {
                kind: EventKind::GcPendingBucketCleaned,
                bucket_id,
                txid: 0,
                file_id: 0,
                value: removed_pages,
            });
        }
    }

    fn clean_one_bucket(&mut self, bucket_id: u64) -> u64 {
        let bucket_table = page_table_name(bucket_id);
        let data_interval_table = data_interval_name(bucket_id);
        let blob_interval_table = blob_interval_name(bucket_id);
        const PID_PER_ROUND: usize = 100000;
        let mut removed_pages = 0;

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
            removed_pages += pids.len() as u64;
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
        removed_pages
    }

    fn scavenge(&mut self) {
        let started = Instant::now();
        let g = crossbeam_epoch::pin();
        let mut scanned_pages = 0;
        let mut compacted_pages = 0;

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
                scanned_pages += 1;

                let tree = Tree::new(self.store.clone(), ROOT_PID, ctx.clone());

                match tree.try_scavenge(cursor, &g) {
                    Ok(true) => {
                        compact_count += 1;
                        compacted_pages += 1;
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

        if scanned_pages > 0 {
            self.store
                .opt
                .observer
                .counter(CounterMetric::GcScavengePageScan, scanned_pages);
        }
        if compacted_pages > 0 {
            self.store
                .opt
                .observer
                .counter(CounterMetric::GcScavengePageCompact, compacted_pages);
        }
        self.store.opt.observer.histogram(
            HistogramMetric::GcScavengeMicros,
            started.elapsed().as_micros() as u64,
        );
    }

    fn process_obsoleted_blob(&self, obsoleted: &[u64], bucket_id: u64) {
        if !obsoleted.is_empty() {
            let mut unlinked = Delete::default();
            let mut del_intervals = DelInterval {
                lo: Vec::new(),
                bucket_id,
            };
            obsoleted
                .iter()
                .filter(|x| !self.store.manifest.is_unsynced_blob_file(**x))
                .for_each(|&x| {
                    let mut loader = MetaReader::<BlobFooter>::new(self.store.opt.blob_file(x))
                        .expect("never happen");
                    let ivls = loader.get_interval().unwrap();
                    for i in ivls {
                        if i.lo <= i.hi {
                            del_intervals.push(i.lo);
                        }
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
                .remove_stat_interval(&unlinked);
            self.store.manifest.save_obsolete_blob(&unlinked);
            self.store.manifest.delete_files();
            self.store
                .opt
                .observer
                .counter(CounterMetric::GcBlobObsoleteFile, unlinked.len() as u64);
            self.blob_runs.fetch_add(1, Relaxed);
        }
    }

    fn process_obsoleted_data(&self, obsoleted: &[u64], bucket_id: u64) {
        if !obsoleted.is_empty() {
            let mut unlinked = Delete::default();
            let mut del_intervals = DelInterval {
                lo: Vec::new(),
                bucket_id,
            };
            obsoleted
                .iter()
                .filter(|x| !self.store.manifest.is_unsynced_data_file(**x))
                .for_each(|&x| {
                    let mut loader = MetaReader::<DataFooter>::new(self.store.opt.data_file(x))
                        .expect("never happen");
                    let ivls = loader.get_interval().unwrap();
                    for i in ivls {
                        // keep deleting legacy sentinel keys from old empty data files
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
                .remove_stat_interval(&unlinked);
            self.store.manifest.save_obsolete_data(&unlinked);
            self.store.manifest.delete_files();
            self.store
                .opt
                .observer
                .counter(CounterMetric::GcDataObsoleteFile, unlinked.len() as u64);
            self.data_runs.fetch_add(1, Relaxed);
        }
    }

    fn process_blob(&mut self) {
        let (obsoleted, victims) = self.collect_blob_candidates(
            self.store.opt.blob_gc_ratio,
            self.store.opt.blob_garbage_ratio,
        );

        for (bucket_id, files) in obsoleted {
            self.process_obsoleted_blob(&files, bucket_id);
        }

        let groups = Self::group_blob_victims_by_bucket(victims);
        for (bucket_id, list) in groups {
            let mut dst_size = 0;
            let mut dst = Vec::new();
            for (file_id, size) in list {
                dst_size += size;
                dst.push(file_id);
                if dst_size >= self.store.opt.blob_file_size && dst.len() >= 2 {
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

    fn collect_blob_candidates(
        &self,
        file_ratio: usize,
        garbage_ratio: usize,
    ) -> (HashMap<u64, Vec<u64>>, Vec<BlobVictim>) {
        let lk = self.store.manifest.blob_stat.read();
        let mut obsoleted = HashMap::<u64, Vec<u64>>::new();
        let mut candidates = Vec::new();

        for (_, stat) in lk.iter() {
            if self.store.manifest.is_unsynced_blob_file(stat.file_id) {
                continue;
            }
            if stat.nr_active == 0 {
                obsoleted
                    .entry(stat.bucket_id)
                    .or_default()
                    .push(stat.file_id);
            } else {
                candidates.push(BlobVictim::from(stat.inner));
            }
        }
        drop(lk);

        if candidates.len() < 2 {
            return (obsoleted, Vec::new());
        }
        (
            obsoleted,
            Self::select_blob_victims(candidates, file_ratio, garbage_ratio),
        )
    }

    fn select_blob_victims(
        mut candidates: Vec<BlobVictim>,
        file_ratio: usize,
        garbage_ratio: usize,
    ) -> Vec<BlobVictim> {
        if candidates.len() < 2 {
            return Vec::new();
        }
        candidates.sort_unstable_by(BlobVictim::cmp_utilization);
        let selected = Self::pick_blob_candidate_count(candidates.len(), file_ratio);
        if selected < 2 {
            return Vec::new();
        }
        candidates.truncate(selected);
        if !Self::blob_ratio_gate_passed(&candidates, garbage_ratio) {
            return Vec::new();
        }
        candidates
    }

    fn blob_ratio_gate_passed(candidates: &[BlobVictim], garbage_ratio: usize) -> bool {
        let mut nr_total = 0u64;
        let mut nr_active = 0u64;
        for c in candidates {
            nr_total += c.nr_total as u64;
            nr_active += c.nr_active as u64;
        }
        if nr_total == 0 {
            return false;
        }
        let ratio = (nr_total - nr_active) * 100 / nr_total;
        (ratio as usize) >= garbage_ratio
    }

    fn pick_blob_candidate_count(total: usize, ratio: usize) -> usize {
        if total < 2 {
            return 0;
        }
        let ratio = ratio.min(100);
        if ratio == 0 {
            return 0;
        }
        total.saturating_mul(ratio) / 100
    }

    fn group_blob_victims_by_bucket(victims: Vec<BlobVictim>) -> Vec<(u64, Vec<(u64, usize)>)> {
        let mut groups: HashMap<u64, Vec<(u64, usize)>> = HashMap::new();
        for v in victims {
            groups
                .entry(v.bucket_id)
                .or_default()
                .push((v.file_id, v.active_size));
        }
        let mut out: Vec<(u64, Vec<(u64, usize)>)> = groups.into_iter().collect();
        out.sort_by_key(|x| x.0);
        out
    }

    fn process_data(&mut self) {
        let tgt_ratio = self.store.opt.data_garbage_ratio as u64;
        let tgt_size = self.store.opt.data_file_size;
        let eager = self.store.opt.gc_eager;
        let tick = self.numerics.next_data_id.load(Relaxed);
        let mut bucket_usage = HashMap::<u64, (u64, u64)>::new();
        let mut bucket_obsoleted: HashMap<u64, Vec<u64>> = HashMap::new();
        let mut candidates = Vec::new();

        for x in self.store.manifest.data_stat.bucket_files().iter() {
            let bucket_id = *x.key();
            for &fid in x.value().iter() {
                if self.store.manifest.is_unsynced_data_file(fid) {
                    continue;
                }
                if let Some(s) = self.store.manifest.data_stat.get(&fid) {
                    if s.active_elems == 0 {
                        bucket_obsoleted.entry(bucket_id).or_default().push(fid);
                    } else {
                        let e = bucket_usage.entry(bucket_id).or_insert((0, 0));
                        e.0 += s.total_size as u64;
                        e.1 += s.active_size as u64;
                        // copy the inner value (DataStatInner is Copy)
                        candidates.push(s.inner);
                    }
                }
            }
        }

        // fully obsolete files should be reclaimed immediately, independent of ratio gate
        for (bucket_id, files) in bucket_obsoleted {
            self.process_obsoleted_data(&files, bucket_id);
        }

        let ranked = Self::rank_data_candidates(candidates, tick);
        if ranked.is_empty() {
            return;
        }

        let plans =
            Self::plan_data_rewrite_from_global(ranked, &bucket_usage, tgt_ratio, tgt_size, eager);
        for (bucket_id, victim) in plans {
            if !self.should_run_data_rewrite_live(bucket_id, tgt_ratio) {
                continue;
            }
            self.rewrite_data(victim, bucket_id);
        }
    }

    fn plan_data_rewrite_from_global(
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
            if !Self::should_run_data_rewrite_for_bucket(bucket_id, tgt_ratio, bucket_usage) {
                continue;
            }
            if let Some(p) = Self::select_data_rewrite_batch_for_bucket(ranked, tgt_size, eager) {
                plans.push((bucket_id, p));
            }
        }
        plans
    }

    fn select_data_rewrite_batch_for_bucket(
        ranked: Vec<Score>,
        tgt_size: usize,
        eager: bool,
    ) -> Option<Vec<Score>> {
        let mut current = Vec::new();
        let mut current_size = 0usize;
        for s in ranked {
            current_size += s.size;
            current.push(s);
            if current_size >= tgt_size && current.len() > 1 {
                return Some(current);
            }
        }
        if eager && current.len() > 1 {
            return Some(current);
        }
        None
    }

    fn rank_data_candidates(candidates: Vec<DataStatInner>, tick: u64) -> Vec<Score> {
        let mut ranked: Vec<Score> = candidates
            .into_iter()
            .map(|s| Score::from(s, tick))
            .filter(|s| s.rate.is_finite())
            .collect();
        ranked.sort_unstable_by(Score::cmp_priority);
        if ranked.len() > Self::MAX_ELEMS {
            ranked.truncate(Self::MAX_ELEMS);
        }
        ranked
    }

    fn should_run_data_rewrite(ratio: u64, tgt_ratio: u64) -> bool {
        ratio >= tgt_ratio
    }

    fn should_run_data_rewrite_for_bucket(
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
        Self::should_run_data_rewrite(ratio, tgt_ratio)
    }

    fn should_run_data_rewrite_live(&self, bucket_id: u64, tgt_ratio: u64) -> bool {
        let Some(ratio) = self.current_bucket_data_ratio(bucket_id) else {
            return false;
        };
        Self::should_run_data_rewrite(ratio, tgt_ratio)
    }

    fn current_bucket_data_ratio(&self, bucket_id: u64) -> Option<u64> {
        let files = self
            .store
            .manifest
            .data_stat
            .bucket_files()
            .get(&bucket_id)?;
        let mut total = 0u64;
        let mut active = 0u64;
        for &fid in files.value().iter() {
            if self.store.manifest.is_unsynced_data_file(fid) {
                continue;
            }
            if let Some(s) = self.store.manifest.data_stat.get(&fid) {
                if s.active_elems == 0 {
                    continue;
                }
                total += s.total_size as u64;
                active += s.active_size as u64;
            }
        }
        if total == 0 {
            return None;
        }
        Some((total - active) * 100 / total)
    }

    fn rewrite_data(&mut self, candidate: Vec<Score>, bucket_id: u64) {
        let started = Instant::now();
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
                let ivls: Vec<Interval> = loader
                    .get_interval()
                    .unwrap()
                    .iter()
                    .copied()
                    .filter(|ivl| ivl.lo <= ivl.hi)
                    .collect();
                let mut im = InactiveMap::new(&ivls);
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
        let victim_count = victims.len() as u64;

        // it's possible that other thread deactived all data in data file while we are procesing
        self.process_obsoleted_data(&obsoleted, bucket_id);

        // 1. perform disk I/O (build data file)
        let (mut fstat, relocs) = builder
            .build()
            .inspect_err(|e| {
                log::error!("error {e}");
            })
            .unwrap();
        fstat.inner.bucket_id = bucket_id;
        self.store.opt.sync_data_dir();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_gc_data_rewrite_after_data_dir_sync");

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
        self.store
            .opt
            .observer
            .counter(CounterMetric::GcDataRewrite, 1);
        self.store.opt.observer.histogram(
            HistogramMetric::GcDataRewriteMicros,
            started.elapsed().as_micros() as u64,
        );
        self.store
            .opt
            .observer
            .histogram(HistogramMetric::GcDataRewriteVictimFiles, victim_count);
        self.store.opt.observer.event(ObserveEvent {
            kind: EventKind::GcDataRewriteComplete,
            bucket_id,
            txid: 0,
            file_id,
            value: victim_count,
        });
    }

    fn rewrite_blob(&mut self, candidate: &[u64], bucket_id: u64) {
        let started = Instant::now();
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
                let ivls: Vec<Interval> = loader
                    .get_interval()
                    .unwrap()
                    .iter()
                    .copied()
                    .filter(|ivl| ivl.lo <= ivl.hi)
                    .collect();
                let mut im = InactiveMap::new(&ivls);

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
        let victim_count = victims.len() as u64;

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
        self.store.opt.sync_data_dir();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_gc_blob_rewrite_after_data_dir_sync");

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
        self.store
            .opt
            .observer
            .counter(CounterMetric::GcBlobRewrite, 1);
        self.store.opt.observer.histogram(
            HistogramMetric::GcBlobRewriteMicros,
            started.elapsed().as_micros() as u64,
        );
        self.store
            .opt
            .observer
            .histogram(HistogramMetric::GcBlobRewriteVictimFiles, victim_count);
        self.store.opt.observer.event(ObserveEvent {
            kind: EventKind::GcBlobRewriteComplete,
            bucket_id,
            txid: 0,
            file_id: blob_id,
            value: victim_count,
        });
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
        let mut writer = GatherWriter::trunc(&path, 128);
        let mut reloc: Vec<u8> = Vec::new();
        let mut reloc_map = HashMap::new();
        let buf = block.mut_slice(0, block.len());

        self.items.sort_unstable_by_key(|x| x.id);

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

        self.items.sort_unstable_by_key(|x| x.id);

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
