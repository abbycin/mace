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
    map::data::{FileFooter, FileVersion, MetaReader},
    meta::{
        BUCKET_PENDING_DEL, DelInterval, Delete, FileKind, FileReader, IntervalPair, Manifest,
        MemStat, MetaKind, Sequences, blob_interval_name, data_interval_name, new_reader,
        page_table_name,
    },
    must_exist, must_true,
    types::{refbox::BoxRef, traits::IAsSlice},
    utils::{
        Handle, MutRef, ROOT_PID,
        bitmap::BitMap,
        block::Block,
        compress::{
            COMPRESS_MIN_LEN, CompressorGuard, CompressorPool, DecompressorPool, RecordCompressor,
            RecordDecompressor,
        },
        countblock::Countblock,
        data::{AddrPair, GatherWriter, Interval, LenSeq, Position},
        lru::Lru,
        observe::{CounterMetric, EventKind, HistogramMetric, ObserveEvent},
    },
};
use crate::{
    meta::{MetaOp, StatInner},
    must_ok,
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
                        GC_WAL => gc.process_wal_clean(),
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
        .expect("can't start garbage-collector thread")
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
        must_ok!(self.tx.send(GC_QUIT));
        self.sem.wait();
    }

    pub(crate) fn pause(&self) {
        must_ok!(self.tx.send(GC_PAUSE));
        self.sem.wait();
    }

    pub(crate) fn resume(&self) {
        must_ok!(self.tx.send(GC_RESUME));
        self.sem.wait();
    }

    pub(crate) fn start(&self) {
        must_ok!(self.tx.send(GC_START));
        self.sem.wait();
    }

    pub(crate) fn wal_clean(&self, manifest: Handle<crate::meta::Manifest>, ctx: Handle<Context>) {
        if self.tx.send(GC_WAL).is_err() {
            let mut gc = GarbageCollector {
                sequences: ctx.sequences.clone(),
                ctx,
                store: MutRef::default(),
                data_runs: Arc::new(AtomicU64::new(0)),
                blob_runs: Arc::new(AtomicU64::new(0)),
            };
            gc.process_wal_clean_with_manifest(manifest);
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
        sequences: ctx.sequences.clone(),
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

pub(crate) fn drain_abort_clean_during_recovery(
    store: MutRef<Store>,
    ctx: Handle<Context>,
) -> Result<(), OpCode> {
    let mut gc = GarbageCollector {
        sequences: ctx.sequences.clone(),
        ctx,
        store,
        data_runs: Arc::new(AtomicU64::new(0)),
        blob_runs: Arc::new(AtomicU64::new(0)),
    };
    gc.run_abort_clean_recovery()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AbortCleanLoadMode {
    SteadyState,
    Recovery,
}

#[derive(Clone, Copy, Debug)]
struct Score {
    id: u64,
    size: usize,
    rate: f64,
    up2: u64,
    bucket_id: u64,
}

trait GcStat {
    fn file_id(&self) -> u64;
    fn active_size(&self) -> usize;
    fn total_size(&self) -> usize;
    fn active_elems(&self) -> u32;
    fn up2(&self) -> u64;
    fn bucket_id(&self) -> u64;
}

impl GcStat for StatInner {
    fn file_id(&self) -> u64 {
        self.file_id
    }

    fn active_size(&self) -> usize {
        self.active_size
    }

    fn total_size(&self) -> usize {
        self.total_size
    }

    fn active_elems(&self) -> u32 {
        self.active_elems
    }

    fn up2(&self) -> u64 {
        self.up2
    }

    fn bucket_id(&self) -> u64 {
        self.bucket_id
    }
}

impl Score {
    fn from<S>(stat: S, now: u64) -> Self
    where
        S: GcStat,
    {
        Self {
            id: stat.file_id(),
            size: stat.active_size(),
            rate: Self::calc_decline_rate(&stat, now),
            up2: stat.up2(),
            bucket_id: stat.bucket_id(),
        }
    }

    fn calc_decline_rate<S>(stat: &S, now: u64) -> f64
    where
        S: GcStat,
    {
        let free = stat.total_size().saturating_sub(stat.active_size());
        let live = stat.active_elems().max(1);
        // no junk has been applied yet, or
        // it's possible gc and flush thread get same tick
        if free == 0 || stat.up2() == now {
            return f64::INFINITY;
        }

        (stat.active_size() as f64 / free as f64).powi(2)
            / (live as f64 * (now - stat.up2()) as f64)
    }

    fn cmp_priority(&self, other: &Self) -> Ordering {
        self.rate
            .total_cmp(&other.rate)
            .then_with(|| self.up2.cmp(&other.up2))
            .then_with(|| self.size.cmp(&other.size))
            .then_with(|| self.id.cmp(&other.id))
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
    sequences: Arc<Sequences>,
    ctx: Handle<Context>,
    store: MutRef<Store>,
    data_runs: Arc<AtomicU64>,
    blob_runs: Arc<AtomicU64>,
}

struct AbortCleanProgress {
    stabilize_buckets: HashSet<u64>,
}

impl GarbageCollector {
    const MAX_ELEMS: usize = 1024 * 100;
    const ABORT_CLEAN_TREE_CACHE_CAP: usize = 64;
    const ABORT_CLEAN_WAL_FILE_CACHE_CAP: usize = 16;

    fn run(&mut self) {
        let started = Instant::now();
        self.store.opt.observer.counter(CounterMetric::GcRun, 1);
        self.process_abort_clean();
        self.process_wal_clean();
        for kind in FileKind::ALL {
            self.process_files(kind);
        }
        self.process_pending_buckets();
        self.store.manifest.delete_files();
        self.store.opt.observer.histogram(
            HistogramMetric::GcRunMicros,
            started.elapsed().as_micros() as u64,
        );
    }

    fn process_wal_clean(&mut self) {
        self.process_wal_clean_with_manifest(self.store.manifest);
    }

    fn process_wal_clean_with_manifest(&mut self, manifest: Handle<Manifest>) {
        let ctx = self.ctx;
        for g in ctx.groups().iter() {
            let (oldest_id, last_ckpt_file, mut checkpoint_id) = {
                let mut logging = g.logging.lock();
                if ctx.opt.sync_on_write
                    && let Err(e) = logging.sync(false)
                {
                    log::error!("wal sync fail, group {}, error {:?}", g.id, e);
                    continue;
                }
                (
                    logging.oldest_wal_id(),
                    logging.last_ckpt().file_id,
                    g.min_active_wal_file_id(&mut logging),
                )
            };
            if let Some(min_pending_file) = ctx.min_abort_clean_file_id(g.id as u8) {
                checkpoint_id = checkpoint_id.min(min_pending_file);
            }
            checkpoint_id = checkpoint_id.min(last_ckpt_file);
            if oldest_id >= checkpoint_id {
                continue;
            }

            let intent = crate::meta::WalRecycleIntent {
                group_id: g.id as u8,
                from_file_id: oldest_id,
                to_file_id: checkpoint_id,
            };
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_before_intent_commit");
            manifest.commit_wal_recycle_intent(intent);

            // [oldest_id, checkpoint_id)
            let recycled = Self::process_one_wal(ctx, intent);
            if recycled == 0 {
                manifest.commit_wal_recycle_done(intent);
                g.logging.lock().advance_oldest_wal_id(checkpoint_id);
                continue;
            }
            ctx.opt.sync_log_dir();
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_after_dir_sync_before_done_commit");
            manifest.commit_wal_recycle_done(intent);
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_after_done_commit_before_publish");
            g.logging.lock().advance_oldest_wal_id(checkpoint_id);
            ctx.opt
                .observer
                .counter(CounterMetric::GcWalRecycleFile, recycled);
        }
    }

    fn process_one_wal(ctx: Handle<Context>, intent: crate::meta::WalRecycleIntent) -> u64 {
        let mut recycled = 0;
        // NOTE: not including `end`
        for seq in intent.from_file_id..intent.to_file_id {
            let from = ctx.opt.wal_file(intent.group_id, seq);
            if !must_ok!(ctx.opt.fs.try_exists(&from), "can't stat {:?}", from) {
                continue;
            }
            let to = ctx.opt.wal_backup(intent.group_id, seq);
            if ctx.opt.keep_stable_wal_file {
                log::info!("rename {from:?} to {to:?}");
                must_ok!(
                    ctx.opt.fs.rename(&from, &to),
                    "can't rename {from:?} to {to:?}"
                );
            } else {
                log::info!("unlink {from:?}");
                must_ok!(
                    ctx.opt.fs.remove_file_if_exists(&from),
                    "can't remove {from:?}"
                );
            }
            recycled += 1;
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_after_remove_before_dir_sync");
        }
        recycled
    }

    fn run_abort_clean_recovery(&mut self) -> Result<(), OpCode> {
        loop {
            let tasks = self.ctx.abort_clean_tasks();
            if tasks.is_empty() {
                return Ok(());
            }

            let mut block = Block::alloc(1024);
            let trees = Lru::<u64, Option<Tree>>::new();
            let g = crossbeam_epoch::pin();
            let mut round_stabilize_buckets = HashSet::new();
            let mut cleaned_txids = Vec::new();

            for task in tasks {
                match task.state {
                    AbortCleanState::Pending => match self.clean_one_abort_task(
                        &g,
                        task,
                        &trees,
                        &mut block,
                        AbortCleanLoadMode::Recovery,
                    ) {
                        Ok(progress) => {
                            round_stabilize_buckets.extend(progress.stabilize_buckets);
                            cleaned_txids.push(task.txid);
                        }
                        Err(OpCode::Again) => {}
                        Err(e) => return Err(e),
                    },
                    AbortCleanState::WaitingQuiesce => {
                        self.ctx.remove_abort_clean(task.txid);
                    }
                }
            }

            if !round_stabilize_buckets.is_empty() {
                self.stabilize_cleaned_pages(
                    &round_stabilize_buckets,
                    &trees,
                    AbortCleanLoadMode::Recovery,
                )?;
            }

            for txid in cleaned_txids {
                self.ctx.remove_abort_clean(txid);
            }
        }
    }

    fn process_abort_clean(&mut self) {
        let drained_events = self.ctx.drain_abort_clean_events();
        for &txid in &drained_events {
            self.ctx.mark_abort_clean_quiesced(txid);
        }

        let tasks = self.ctx.abort_clean_tasks();
        if tasks.is_empty() {
            for txid in drained_events {
                self.ctx.remove_abort_clean(txid);
            }
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
                    match self.clean_one_abort_task(
                        &g,
                        task,
                        &trees,
                        &mut block,
                        AbortCleanLoadMode::SteadyState,
                    ) {
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
                    }
                }
            }
        }

        let checkpoint_ok = if round_stabilize_buckets.is_empty() {
            true
        } else {
            match self.stabilize_cleaned_pages(
                &round_stabilize_buckets,
                &trees,
                AbortCleanLoadMode::SteadyState,
            ) {
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

        for txid in drained_events {
            self.ctx.remove_abort_clean(txid);
        }
    }

    fn stabilize_cleaned_pages(
        &self,
        dirty_buckets: &HashSet<u64>,
        trees: &Lru<u64, Option<Tree>>,
        mode: AbortCleanLoadMode,
    ) -> Result<(), OpCode> {
        for &bucket_id in dirty_buckets {
            let tree = match trees.get(&bucket_id) {
                Some(tree) => tree.clone(),
                None => self.get_tree(trees, bucket_id, mode)?,
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
        mode: AbortCleanLoadMode,
    ) -> Result<Option<Tree>, OpCode> {
        if let Some(tree) = cache.get(&bucket_id) {
            return Ok(tree.clone());
        }

        let bucket_missing = self
            .store
            .manifest
            .bucket_metas_by_id
            .get(&bucket_id)
            .is_none();
        let unloaded_in_steady_state = mode == AbortCleanLoadMode::SteadyState
            && !self.store.manifest.buckets.buckets.contains_key(&bucket_id);
        let tree = if bucket_missing || unloaded_in_steady_state {
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
        mode: AbortCleanLoadMode,
    ) -> Result<AbortCleanProgress, OpCode> {
        let mut cursor = task.tail_lsn;
        let mut stabilize_buckets = HashSet::new();
        let wal_files = Lru::<u64, (File, u64)>::new();
        let mut seen_keys = HashSet::<(u64, Vec<u8>)>::new();

        loop {
            if wal_files.get(&cursor.file_id).is_none() {
                let path = self.ctx.opt.wal_file(task.group_id, cursor.file_id);
                if !self.ctx.opt.fs.try_exists(&path)? {
                    return Err(OpCode::Corruption);
                }
                let file = File::options()
                    .read(true)
                    .open(self.ctx.opt.fs.as_ref(), &path)?;
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
                    if let Some(tree) = self.get_tree(trees, bucket_id, mode)? {
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
            let mut iter = txn.iter_uncached();
            let mut k = Vec::new();
            let mut v = Vec::new();
            if iter.next_ref(&mut k, &mut v) {
                bucket_id = Some(<u64>::from_le_bytes(must_ok!(k[..8].try_into())));
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

        let removed_pages = self.delete_bucket_batch(&bucket_table);
        if removed_pages != 0 {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_pending_bucket_reap_after_batch_before_finalize");
            return removed_pages as u64;
        }
        let removed_data_intervals = self.delete_bucket_batch(&data_interval_table);
        if removed_data_intervals != 0 {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_pending_bucket_reap_after_batch_before_finalize");
            return removed_data_intervals as u64;
        }
        let removed_blob_intervals = self.delete_bucket_batch(&blob_interval_table);
        if removed_blob_intervals != 0 {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_pending_bucket_reap_after_batch_before_finalize");
            return removed_blob_intervals as u64;
        }

        // table is now empty, destroy the btree bucket and remove pending record
        let _ = self.store.manifest.btree.del_bucket(&bucket_table);
        let _ = self.store.manifest.btree.del_bucket(&data_interval_table);
        let _ = self.store.manifest.btree.del_bucket(&blob_interval_table);
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(
            "mace_pending_bucket_reap_after_finalize_before_meta_commit",
        );
        let mut txn = self.store.manifest.begin();
        txn.ops_mut()
            .entry(BUCKET_PENDING_DEL.to_string())
            .or_default()
            .push(MetaOp::Del(bucket_id.to_le_bytes().to_vec()));
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_pending_bucket_reap_after_manifest_commit");
        self.store.manifest.nr_buckets.fetch_sub(1, Relaxed);
        self.store.manifest.bucket_runtimes.remove(&bucket_id);
        0
    }

    fn delete_bucket_batch(&self, bucket: &str) -> usize {
        let mut keys = Vec::with_capacity(Self::MAX_ELEMS);
        let _ = self.store.manifest.btree.view(bucket, |txn| {
            let mut iter = txn.iter_uncached();
            let mut k = Vec::new();
            let mut v = Vec::new();
            while iter.next_ref(&mut k, &mut v) && keys.len() < Self::MAX_ELEMS {
                keys.push(k.clone());
            }
            Ok(())
        });

        if keys.is_empty() {
            return 0;
        }

        let mut txn = self.store.manifest.begin();
        let ops = txn.ops_mut().entry(bucket.to_string()).or_default();
        for key in &keys {
            ops.push(MetaOp::Del(key.clone()));
        }
        txn.commit();
        keys.len()
    }

    fn delete_meta_kind(kind: FileKind) -> MetaKind {
        match kind {
            FileKind::Data => MetaKind::DataDelete,
            FileKind::Blob => MetaKind::BlobDelete,
        }
    }

    fn delete_interval_meta_kind(kind: FileKind) -> MetaKind {
        match kind {
            FileKind::Data => MetaKind::DataDelInterval,
            FileKind::Blob => MetaKind::BlobDelInterval,
        }
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

    fn obsolete_counter(kind: FileKind) -> CounterMetric {
        match kind {
            FileKind::Data => CounterMetric::GcDataObsoleteFile,
            FileKind::Blob => CounterMetric::GcBlobObsoleteFile,
        }
    }

    fn rewrite_counter(kind: FileKind) -> CounterMetric {
        match kind {
            FileKind::Data => CounterMetric::GcDataRewrite,
            FileKind::Blob => CounterMetric::GcBlobRewrite,
        }
    }

    fn rewrite_micros(kind: FileKind) -> HistogramMetric {
        match kind {
            FileKind::Data => HistogramMetric::GcDataRewriteMicros,
            FileKind::Blob => HistogramMetric::GcBlobRewriteMicros,
        }
    }

    fn rewrite_victim_hist(kind: FileKind) -> HistogramMetric {
        match kind {
            FileKind::Data => HistogramMetric::GcDataRewriteVictimFiles,
            FileKind::Blob => HistogramMetric::GcBlobRewriteVictimFiles,
        }
    }

    fn rewrite_complete_event(kind: FileKind) -> EventKind {
        match kind {
            FileKind::Data => EventKind::GcDataRewriteComplete,
            FileKind::Blob => EventKind::GcBlobRewriteComplete,
        }
    }

    #[cfg(feature = "failpoints")]
    fn obsolete_after_meta_commit_failpoint(kind: FileKind) -> &'static str {
        match kind {
            FileKind::Data => "mace_gc_data_obsolete_after_meta_commit",
            FileKind::Blob => "mace_gc_blob_obsolete_after_meta_commit",
        }
    }

    #[cfg(feature = "failpoints")]
    fn obsolete_after_retired_mark_failpoint(kind: FileKind) -> &'static str {
        match kind {
            FileKind::Data => "mace_gc_data_obsolete_after_retired_mark",
            FileKind::Blob => "mace_gc_blob_obsolete_after_retired_mark",
        }
    }

    #[cfg(feature = "failpoints")]
    fn obsolete_after_remove_stat_failpoint(kind: FileKind) -> &'static str {
        match kind {
            FileKind::Data => "mace_gc_data_obsolete_after_remove_stat",
            FileKind::Blob => "mace_gc_blob_obsolete_after_remove_stat",
        }
    }

    #[cfg(feature = "failpoints")]
    fn rewrite_stage_marker_failpoint(kind: FileKind) -> &'static str {
        match kind {
            FileKind::Data => "mace_gc_data_rewrite_after_stage_marker",
            FileKind::Blob => "mace_gc_blob_rewrite_after_stage_marker",
        }
    }

    #[cfg(feature = "failpoints")]
    fn rewrite_after_dir_sync_failpoint(kind: FileKind) -> &'static str {
        match kind {
            FileKind::Data => "mace_gc_data_rewrite_after_data_dir_sync",
            FileKind::Blob => "mace_gc_blob_rewrite_after_data_dir_sync",
        }
    }

    #[cfg(feature = "failpoints")]
    fn rewrite_before_meta_commit_failpoint(kind: FileKind) -> &'static str {
        match kind {
            FileKind::Data => "mace_gc_data_rewrite_before_meta_commit",
            FileKind::Blob => "mace_gc_blob_rewrite_before_meta_commit",
        }
    }

    #[cfg(feature = "failpoints")]
    fn rewrite_after_meta_commit_failpoint(kind: FileKind) -> &'static str {
        match kind {
            FileKind::Data => "mace_gc_data_rewrite_after_meta_commit",
            FileKind::Blob => "mace_gc_blob_rewrite_after_meta_commit",
        }
    }

    fn next_tick(&self, bucket_id: u64, kind: FileKind) -> u64 {
        self.store
            .manifest
            .get_bucket_runtime(bucket_id)
            .load_update_epoch(kind)
    }

    fn alloc_file_id(&self) -> u64 {
        self.sequences.next_file_id.fetch_add(1, Relaxed)
    }

    fn target_ratio(&self, kind: FileKind) -> u64 {
        match kind {
            FileKind::Data => self.store.opt.data_garbage_ratio as u64,
            FileKind::Blob => self.store.opt.blob_garbage_ratio as u64,
        }
    }

    fn target_file_size(&self, kind: FileKind) -> usize {
        match kind {
            FileKind::Data => self.store.opt.data_file_size,
            FileKind::Blob => self.store.opt.blob_file_size,
        }
    }

    fn file_runs(&self, kind: FileKind) -> &AtomicU64 {
        match kind {
            FileKind::Data => self.data_runs.as_ref(),
            FileKind::Blob => self.blob_runs.as_ref(),
        }
    }

    fn process_obsoleted_files(&self, kind: FileKind, obsoleted: &[u64], bucket_id: u64) {
        if obsoleted.is_empty() {
            return;
        }

        let mut unlinked = Delete::default();
        let mut del_intervals = DelInterval {
            lo: Vec::new(),
            bucket_id,
        };
        obsoleted
            .iter()
            .filter(|x| !self.store.manifest.is_unsynced_file(kind, **x))
            .for_each(|&x| {
                let path = match kind {
                    FileKind::Data => self.store.opt.data_file(x),
                    FileKind::Blob => self.store.opt.blob_file(x),
                };
                let mut loader = MetaReader::new(self.store.opt.fs.as_ref(), path);
                let ivls = loader.get_interval();
                for i in ivls {
                    if kind == FileKind::Data || i.lo <= i.hi {
                        // keep deleting old sentinel keys from earlier empty data files
                        del_intervals.push(i.lo);
                    }
                }
                unlinked.push(x);
            });

        let mut txn = self.store.manifest.begin();
        txn.record(Self::delete_meta_kind(kind), &unlinked);
        txn.record(Self::delete_interval_meta_kind(kind), &del_intervals);
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::obsolete_after_meta_commit_failpoint(kind));

        // only ordinary obsolete reclaim publishes retired keys for flush races
        self.store
            .manifest
            .mark_retired_stats(kind, bucket_id, &unlinked);
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::obsolete_after_retired_mark_failpoint(kind));
        self.store
            .manifest
            .stat_ctx(kind)
            .remove_stat_interval(&unlinked);
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::obsolete_after_remove_stat_failpoint(kind));
        self.store.manifest.save_obsolete_files(kind, &unlinked);
        self.store.manifest.delete_files();
        self.store
            .opt
            .observer
            .counter(Self::obsolete_counter(kind), unlinked.len() as u64);
        self.file_runs(kind).fetch_add(1, Relaxed);
    }

    fn process_files(&mut self, kind: FileKind) {
        let tgt_ratio = self.target_ratio(kind);
        let tgt_size = self.target_file_size(kind);
        let eager = self.store.opt.gc_eager;
        let (obsoleted, bucket_usage, candidates) = self.collect_candidates(kind);

        for (bucket_id, files) in obsoleted {
            self.process_obsoleted_files(kind, &files, bucket_id);
        }

        let ranked = Self::rank_candidates(candidates, |bucket_id| self.next_tick(bucket_id, kind));
        if ranked.is_empty() {
            return;
        }

        let plans =
            Self::plan_rewrite_from_global(ranked, &bucket_usage, tgt_ratio, tgt_size, eager);
        for (bucket_id, victim) in plans {
            if !self.should_run_rewrite_live(kind, bucket_id, tgt_ratio) {
                continue;
            }
            self.rewrite_files(kind, &victim, bucket_id);
        }
    }

    fn collect_candidates(
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
        for x in self.store.manifest.stat_ctx(kind).bucket_files().iter() {
            let bucket_id = *x.key();
            for &fid in x.value().iter() {
                if self.store.manifest.is_unsynced_file(kind, fid) {
                    continue;
                }
                if let Some(stat) = self.store.manifest.stat_ctx(kind).get(&fid) {
                    if stat.active_elems == 0 {
                        obsoleted.entry(bucket_id).or_default().push(fid);
                    } else {
                        let e = bucket_usage.entry(bucket_id).or_insert((0, 0));
                        e.0 += stat.total_size as u64;
                        e.1 += stat.active_size as u64;
                        candidates.push(stat.inner);
                    }
                }
            }
        }
        (obsoleted, bucket_usage, candidates)
    }

    fn plan_rewrite_from_global(
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
            if let Some(p) = Self::select_rewrite_batch_for_bucket(ranked, tgt_size, eager) {
                plans.push((bucket_id, p));
            }
        }
        plans
    }

    fn select_rewrite_batch_for_bucket(
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

    fn rank_candidates<T, F>(candidates: Vec<T>, tick_for_bucket: F) -> Vec<Score>
    where
        T: GcStat + Copy,
        F: Fn(u64) -> u64,
    {
        let mut ranked: Vec<Score> = candidates
            .into_iter()
            .map(|s| Score::from(s, tick_for_bucket(s.bucket_id())))
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

    fn should_run_rewrite_live(&self, kind: FileKind, bucket_id: u64, tgt_ratio: u64) -> bool {
        let Some(ratio) = self.current_bucket_ratio(kind, bucket_id) else {
            return false;
        };
        Self::should_run_rewrite(ratio, tgt_ratio)
    }

    fn current_bucket_ratio(&self, kind: FileKind, bucket_id: u64) -> Option<u64> {
        self.store
            .manifest
            .stat_ctx(kind)
            .bucket_ratio(bucket_id, |fid| {
                self.store.manifest.is_unsynced_file(kind, fid)
            })
    }

    fn rewrite_files(&mut self, kind: FileKind, candidate: &[Score], bucket_id: u64) {
        let started = Instant::now();
        let opt = &self.store.opt;
        let Some(permit) = self.store.manifest.try_acquire_rewrite(bucket_id) else {
            return;
        };
        let file_id = self.alloc_file_id();
        let tick = self
            .store
            .manifest
            .get_bucket_runtime(bucket_id)
            .next_update_epoch(kind);
        // stage orphan intent before rewrite output is flushed
        // crash can happen after file sync but before manifest commit
        self.store.manifest.stage_orphan_file(kind, file_id);
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::rewrite_stage_marker_failpoint(kind));
        let mut builder = RewriteBuilder::new(
            file_id,
            opt,
            candidate.len(),
            bucket_id,
            tick,
            permit.enable_compression,
            permit.compressors.clone(),
        );
        let mut remap_intervals = Vec::with_capacity(candidate.len());
        let mut del_intervals = DelInterval {
            lo: Vec::new(),
            bucket_id,
        };
        let mut obsoleted = Vec::new();

        self.store.manifest.stat_ctx(kind).start_collect_junks(); // stop in update_stat_interval
        let victims: Vec<u64> = candidate
            .iter()
            .filter_map(|x| {
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
                let bitmap = self
                    .store
                    .manifest
                    .stat_ctx(kind)
                    .load_mask_clone(x.id, &self.store.manifest.btree)
                    .expect("must exist");

                let active: Vec<Entry> = relocs
                    .iter()
                    .filter(|m| !bitmap.test(m.val.seq))
                    .map(|m| {
                        im.test(m.key);
                        Entry {
                            key: m.key,
                            raw_len: m.val.raw_len(),
                            compressed_len: m.val.compressed_len(),
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
                builder.add_item(RewriteItem::new(x.id, x.up2, active));
                Some(x.id)
            })
            .collect();
        let victim_count = victims.len() as u64;

        // it's possible that another thread deactivated all live items while we were processing
        self.process_obsoleted_files(kind, &obsoleted, bucket_id);

        let (mut fstat, relocs) = must_ok!(builder.build(kind));
        fstat.inner.bucket_id = bucket_id;
        self.store.opt.sync_data_dir();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::rewrite_after_dir_sync_failpoint(kind));

        let mut txn = self.store.manifest.begin();
        txn.record(MetaKind::Sequences, self.store.manifest.sequences.deref());

        let stat = self.store.manifest.update_stat_interval(
            kind,
            fstat,
            relocs,
            &victims,
            &del_intervals,
            &remap_intervals,
        );

        txn.record(Self::stat_meta_kind(kind), &stat);

        if !del_intervals.is_empty() {
            txn.record(Self::delete_interval_meta_kind(kind), &del_intervals);
        }
        for i in &remap_intervals {
            txn.record(Self::interval_meta_kind(kind), i);
        }
        let tmp: Delete = victims.into();
        txn.record(Self::delete_meta_kind(kind), &tmp);
        self.store
            .manifest
            .clear_orphan_file(kind, &mut txn, file_id);
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::rewrite_before_meta_commit_failpoint(kind));
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::rewrite_after_meta_commit_failpoint(kind));

        self.store.manifest.save_obsolete_files(kind, &tmp);
        self.store.manifest.delete_files();
        self.file_runs(kind).fetch_add(1, AcqRel);
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
            file_id,
            value: victim_count,
        });
    }
}

#[derive(Clone, Copy, Debug)]
struct PendingReloc {
    key: u64,
    off: usize,
    raw_len: u32,
    compressed_len: u32,
    crc: u32,
}

impl PendingReloc {
    const fn new(key: u64, off: usize, raw_len: u32, compressed_len: u32, crc: u32) -> Self {
        Self {
            key,
            off,
            raw_len,
            compressed_len,
            crc,
        }
    }
}

fn build_sorted_relocs(pending: &mut [PendingReloc]) -> (Vec<AddrPair>, HashMap<u64, LenSeq>) {
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

struct RewriteBuilder<'a> {
    file_id: u64,
    items: Vec<RewriteItem>,
    intervals: Vec<u8>,
    nr_interval: u32,
    weighted_up2_sum: u128,
    weighted_up2_size: u128,
    tick: u64,
    opt: &'a Options,
    enable_compression: bool,
    compressors: Arc<CompressorPool>,
    bucket_id: u64,
}

impl<'a> RewriteBuilder<'a> {
    fn new(
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

    fn add_item(&mut self, item: RewriteItem) {
        let weight = item.live_bytes.max(1) as u128;
        self.weighted_up2_sum += weight * item.up2 as u128;
        self.weighted_up2_size += weight;
        self.items.push(item);
    }

    fn add_interval(&mut self, lo: u64, hi: u64) {
        let ivl = Interval::new(lo, hi);
        self.intervals.extend_from_slice(ivl.as_slice());
        self.nr_interval += 1;
    }

    fn build(&mut self, kind: FileKind) -> Result<(MemStat, HashMap<u64, LenSeq>), OpCode> {
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

        let mut interval_crc = Crc32cHasher::default();
        let is = self.intervals.as_slice();
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
                log::info!("compacted to {path:?} {footer:?}");
            }
            FileKind::Blob => {
                log::info!("compacted [{beg}, {end}] to {path:?} {footer:?}");
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

struct RewriteItem {
    id: u64,
    up2: u64,
    live_bytes: usize,
    pos: Vec<Entry>,
}

impl RewriteItem {
    fn new(id: u64, up2: u64, pos: Vec<Entry>) -> Self {
        let live_bytes = pos.iter().map(|e| e.raw_len as usize).sum();
        Self {
            id,
            up2,
            live_bytes,
            pos,
        }
    }
}

struct Entry {
    /// logical address
    key: u64,
    /// decoded or stored bytes to read
    raw_len: u32,
    /// stored compressed length, 0 means raw
    compressed_len: u32,
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
        must_true!(pos < self.ivls.len());
        must_true!(addr >= self.ivls[pos].lo);
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

struct RewrittenRecord {
    raw_len: u32,
    compressed_len: u32,
    crc: u32,
}

struct RewriteBuffers {
    io: Block,
}

impl RewriteBuffers {
    fn new(size: usize) -> Self {
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

#[cfg(test)]
mod tests {
    use super::{Entry, PendingReloc, RewriteBuilder, RewriteItem, build_sorted_relocs};
    use crate::{
        Options, RandomPath,
        map::data::{FileVersion, MetaReader},
        meta::{FileKind, StatInner},
        types::{
            header::{NodeType, TagKind},
            refbox::BoxRef,
            traits::IHeader,
        },
        utils::{INIT_ID, compress::CompressorPool},
    };

    fn sample_pages() -> [BoxRef; 2] {
        let (pid, addr) = (114514, 1919810);
        let mut p = BoxRef::alloc(233, addr);
        p.header_mut().pid = pid;
        p.header_mut().kind = TagKind::Delta;
        p.header_mut().node_type = NodeType::Leaf;

        let (pid1, addr1) = (192, 68);
        let mut p1 = BoxRef::alloc(666, addr1);
        p1.header_mut().pid = pid1;
        p1.header_mut().kind = TagKind::Delta;
        p1.header_mut().node_type = NodeType::Leaf;
        [p, p1]
    }

    #[test]
    fn build_sorted_relocs_orders_by_addr_and_reassigns_seq() {
        let mut pending = vec![
            PendingReloc::new(30, 300, 30, 0, 3),
            PendingReloc::new(10, 100, 10, 4, 1),
            PendingReloc::new(20, 200, 20, 0, 2),
        ];

        let (relocs, map) = build_sorted_relocs(&mut pending);

        assert_eq!(relocs.len(), 3);
        let key0 = relocs[0].key;
        let seq0 = relocs[0].val.seq;
        let key1 = relocs[1].key;
        let seq1 = relocs[1].val.seq;
        let key2 = relocs[2].key;
        let seq2 = relocs[2].val.seq;
        assert_eq!(key0, 10);
        assert_eq!(seq0, 0);
        assert_eq!(key1, 20);
        assert_eq!(seq1, 1);
        assert_eq!(key2, 30);
        assert_eq!(seq2, 2);

        assert_eq!(map.get(&10).unwrap().seq, 0);
        assert_eq!(map.get(&10).unwrap().compressed_len, 4);
        assert_eq!(map.get(&20).unwrap().seq, 1);
        assert_eq!(map.get(&30).unwrap().seq, 2);
    }

    #[test]
    fn score_uses_shared_data_and_blob_formula() {
        let now = 100;
        let data = StatInner {
            file_id: 7,
            up1: 90,
            up2: 80,
            active_elems: 4,
            total_elems: 8,
            active_size: 40,
            total_size: 100,
            bucket_id: 3,
        };
        let blob = StatInner {
            file_id: 7,
            up1: 90,
            up2: 80,
            active_elems: 4,
            total_elems: 8,
            active_size: 40,
            total_size: 100,
            bucket_id: 3,
        };

        let data_score = super::Score::from(data, now);
        let blob_score = super::Score::from(blob, now);

        assert_eq!(data_score.rate, blob_score.rate);
        assert_eq!(data_score.size, blob_score.size);
        assert_eq!(data_score.up2, blob_score.up2);
    }

    #[test]
    fn rewrite_builder_emits_v1() {
        let path = RandomPath::new();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        let opt = opt.validate().unwrap();
        let _ = opt.create_dir();

        let [p, p1] = sample_pages();
        let mut file_id = INIT_ID;
        let mut writer =
            crate::map::data::FileBuilder::new(0, false, CompressorPool::new(), opt.fs.clone());
        writer.add(p);
        writer.add(p1);
        let files = writer.flush_files(
            FileKind::Data,
            opt.data_file_size,
            7,
            || {
                let id = file_id;
                file_id += 1;
                (id, opt.data_file(id))
            },
            |_| {},
            |_| {},
        );
        assert_eq!(files.len(), 1);

        let mut loader = MetaReader::new(opt.fs.as_ref(), opt.data_file(INIT_ID));
        assert_eq!(loader.version(), FileVersion::V1);
        let relocs = loader.get_reloc();
        let intervals = loader.get_interval();

        let mut builder =
            RewriteBuilder::new(INIT_ID + 1, &opt, 1, 0, 9, false, CompressorPool::new());
        for ivl in intervals.iter().copied() {
            builder.add_interval(ivl.lo, ivl.hi);
        }
        let active = relocs
            .iter()
            .map(|m| Entry {
                key: m.key,
                raw_len: m.val.raw_len(),
                compressed_len: m.val.compressed_len(),
            })
            .collect();
        builder.add_item(RewriteItem::new(INIT_ID, 7, active));

        let (_stat, _reloc_map) = builder.build(FileKind::Data).expect("rewrite must succeed");
        let reopened = MetaReader::new(opt.fs.as_ref(), opt.data_file(INIT_ID + 1));
        assert_eq!(reopened.version(), FileVersion::V1);
    }

    #[test]
    fn rewrite_builder_keeps_compressed_v1_records_compressed() {
        let path = RandomPath::new();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        let opt = opt.validate().unwrap();
        let _ = opt.create_dir();

        let mut page = BoxRef::alloc(4096, 114514);
        page.header_mut().pid = 1919810;
        page.header_mut().kind = TagKind::Delta;
        page.header_mut().node_type = NodeType::Leaf;
        page.data_slice_mut::<u8>()[size_of::<crate::types::header::DeltaHeader>()..].fill(b'x');

        let mut file_id = INIT_ID;
        let mut writer =
            crate::map::data::FileBuilder::new(0, true, CompressorPool::new(), opt.fs.clone());
        writer.add(page);
        let files = writer.flush_files(
            FileKind::Data,
            opt.data_file_size,
            7,
            || {
                let id = file_id;
                file_id += 1;
                (id, opt.data_file(id))
            },
            |_| {},
            |_| {},
        );
        assert_eq!(files.len(), 1);

        let mut loader = MetaReader::new(opt.fs.as_ref(), opt.data_file(INIT_ID));
        let relocs = loader.get_reloc();
        assert_eq!(relocs.len(), 1);
        assert!(relocs[0].val.compressed_len() > 0);
        let intervals = loader.get_interval();

        let mut builder =
            RewriteBuilder::new(INIT_ID + 1, &opt, 1, 0, 9, true, CompressorPool::new());
        for ivl in intervals.iter().copied() {
            builder.add_interval(ivl.lo, ivl.hi);
        }
        let active = relocs
            .iter()
            .map(|m| Entry {
                key: m.key,
                raw_len: m.val.raw_len(),
                compressed_len: m.val.compressed_len(),
            })
            .collect();
        builder.add_item(RewriteItem::new(INIT_ID, 7, active));

        let (_stat, _reloc_map) = builder.build(FileKind::Data).expect("rewrite must succeed");
        let mut rewritten = MetaReader::new(opt.fs.as_ref(), opt.data_file(INIT_ID + 1));
        let rewritten_relocs = rewritten.get_reloc();
        assert_eq!(rewritten_relocs.len(), 1);
        assert!(rewritten_relocs[0].val.compressed_len() > 0);
    }
}
