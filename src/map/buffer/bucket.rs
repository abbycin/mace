//! bucket runtime contexts, lifecycle state, leases and the bucket manager

use std::ptr::null_mut;
use std::sync::{
    Arc,
    atomic::{
        AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst},
    },
    mpsc::{Receiver, Sender},
};

use dashmap::{DashMap, DashSet};
use parking_lot::RwLock;

use crate::map::{
    Loader, Page, SharedState,
    cache::{CANDIDATE_RING_SIZE, CANDIDATE_SAMPLE_RATE, CacheState, CandidateRing, NodeCache},
    table::{PageMap, Swip},
};
use crate::{
    OpCode,
    cc::context::Context,
    meta::Manifest,
    must_exist,
    types::refbox::BoxRef,
    utils::{
        Handle, MutRef, ROOT_PID,
        compress::CompressorPool,
        interval::IntervalMap,
        lru::ShardPriorityLru,
        merge_operator::MergeOperator,
        options::{BucketOptions, ParsedOptions},
    },
};

use super::pool::Pool;
use crate::map::adapter::ManifestCheckpointObserver;
use crate::map::flow::{FlowController, ForegroundWritePermit};
use crate::map::flush::Checkpoint;

/// what a bucket-lifecycle transition does with the bucket's dirty state
///
/// the choice is made at the reclaim site, never inferred by the checkpoint thread from the
/// runtime lifecycle state: a manual unload must publish its sealed batch, a logical delete must
/// drop it, and recovery/shutdown must publish it with a full file synchronization
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReclaimMode {
    /// publish the sealed batch before the runtime is released
    ///
    /// `force_fsync` is set when the WAL that would otherwise guard the published closure is about
    /// to be removed (recovery rebuild) or when the process is closing
    FinalCheckpoint { force_fsync: bool },
    /// a logical delete removed the bucket: the sealed batch must not be published
    Discard,
}

impl ReclaimMode {
    const NONE: u8 = 0;
    const FINAL: u8 = 1;
    const FINAL_FSYNC: u8 = 2;
    const DISCARD: u8 = 3;

    fn encode(self) -> u8 {
        match self {
            ReclaimMode::FinalCheckpoint { force_fsync: false } => Self::FINAL,
            ReclaimMode::FinalCheckpoint { force_fsync: true } => Self::FINAL_FSYNC,
            ReclaimMode::Discard => Self::DISCARD,
        }
    }

    fn decode(raw: u8) -> Option<Self> {
        match raw {
            Self::NONE => None,
            Self::FINAL => Some(ReclaimMode::FinalCheckpoint { force_fsync: false }),
            Self::FINAL_FSYNC => Some(ReclaimMode::FinalCheckpoint { force_fsync: true }),
            Self::DISCARD => Some(ReclaimMode::Discard),
            _ => unreachable!("reclaim mode is written only through `ReclaimMode::encode`"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Lifecycle {
    Active = 0,
    Dropping = 1,
    Deleting = 2,
}

impl Lifecycle {
    fn from_raw(raw: u8) -> Self {
        match raw {
            Self::ACTIVE => Lifecycle::Active,
            Self::DROPPING => Lifecycle::Dropping,
            Self::DELETING => Lifecycle::Deleting,
            _ => unreachable!("bucket lifecycle is written only through its setters"),
        }
    }

    const ACTIVE: u8 = 0;
    const DROPPING: u8 = 1;
    const DELETING: u8 = 2;
}

/// lifecycle plus the address allocator of one bucket
///
/// a bucket is `Active` while new leases may be acquired. `Dropping` marks a manual unload in
/// progress and `Deleting` a logical delete; both refuse new leases and contexts at the same
/// lifecycle boundary.
///
/// the lifecycle and the nudge reference form a Dekker pair: a nudge increments `nudge_ref` and
/// then reads the lifecycle, while a reclaim writes the lifecycle and then reads `nudge_ref`. All
/// four accesses are sequentially consistent, so seeing `nudge_ref == 0` proves that no nudge
/// observed the old lifecycle and is about to enqueue a task. Acquire/Release only would allow
/// both sides to read the old value and let a task be queued after the runtime was retired.
pub(crate) struct BucketState {
    lifecycle: AtomicU8,
    pub(crate) next_addr: AtomicU64,
    txn_ref: AtomicUsize,
    nudge_ref: AtomicUsize,
}

/// one in-flight checkpoint-nudge attempt, taken only while the bucket is active
///
/// the reclaim path waits for this count to drain before it retires a runtime, so a nudge that
/// already passed the lifecycle check cannot enqueue a checkpoint task afterwards
pub(crate) struct NudgeSlot<'a> {
    state: &'a BucketState,
}

impl Drop for NudgeSlot<'_> {
    fn drop(&mut self) {
        self.state.nudge_ref.fetch_sub(1, SeqCst);
    }
}

impl BucketState {
    pub(crate) fn new() -> Self {
        Self {
            lifecycle: AtomicU8::new(Lifecycle::ACTIVE),
            next_addr: AtomicU64::new(crate::utils::INIT_ADDR),
            txn_ref: AtomicUsize::new(0),
            nudge_ref: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub(crate) fn inc_txn_ref(&self) {
        self.txn_ref.fetch_add(1, AcqRel);
    }

    #[inline]
    pub(crate) fn dec_txn_ref(&self) {
        self.txn_ref.fetch_sub(1, AcqRel);
    }

    #[inline]
    pub(crate) fn set_dropping(&self) {
        self.lifecycle.store(Lifecycle::DROPPING, SeqCst);
    }

    #[inline]
    pub(crate) fn set_deleting(&self) {
        self.lifecycle.store(Lifecycle::DELETING, SeqCst);
    }

    /// return a successfully unloaded bucket to `Active`
    pub(crate) fn set_active(&self) {
        self.lifecycle.store(Lifecycle::ACTIVE, SeqCst);
    }

    /// sequentially consistent on purpose: see the Dekker note on [`BucketState`]
    #[inline]
    pub(crate) fn is_active(&self) -> bool {
        Lifecycle::from_raw(self.lifecycle.load(SeqCst)) == Lifecycle::Active
    }

    #[inline]
    pub(crate) fn is_busy(&self) -> bool {
        self.txn_ref.load(Acquire) > 0
    }

    /// take a checkpoint-nudge slot, or `None` when a reclaim already began
    ///
    /// the increment and the lifecycle load are sequentially consistent: together with the
    /// reclaim's lifecycle store and `wait_nudge_quiescent` they form one total order
    pub(crate) fn try_nudge(&self) -> Option<NudgeSlot<'_>> {
        self.nudge_ref.fetch_add(1, SeqCst);
        if !self.is_active() {
            self.nudge_ref.fetch_sub(1, SeqCst);
            return None;
        }
        Some(NudgeSlot { state: self })
    }

    /// wait until every nudge that observed the previous lifecycle finished enqueueing
    pub(crate) fn wait_nudge_quiescent(&self) {
        while self.nudge_ref.load(SeqCst) != 0 {
            std::hint::spin_loop();
        }
    }
}

/// per-bucket runtime that outlives the loaded context
///
/// it carries the lifecycle state, compression contexts, the update epochs and the reclaim mode,
/// so background GC and the checkpoint thread can read lifecycle without a bucket context
pub(crate) struct BucketRuntime {
    pub(crate) state: MutRef<BucketState>,
    pub(crate) compressors: Arc<CompressorPool>,
    rewrite_inflight: AtomicU32,
    update_epochs: [AtomicU64; 2],
    leases: AtomicUsize,
    reclaim: AtomicU8,
}

impl BucketRuntime {
    pub(crate) fn new() -> Self {
        Self {
            state: MutRef::new(BucketState::new()),
            compressors: CompressorPool::new(),
            rewrite_inflight: AtomicU32::new(0),
            update_epochs: [AtomicU64::new(1), AtomicU64::new(1)],
            leases: AtomicUsize::new(0),
            reclaim: AtomicU8::new(ReclaimMode::NONE),
        }
    }

    /// take a lease on a bucket that the catalog already validated as active
    ///
    /// only `BucketCatalog::load_for_open`/`begin_create` may raise the count from zero, and both
    /// run inside the catalog's structural lock; every other holder arrives through `Clone`
    pub(crate) fn acquire_lease(self: &Arc<Self>) -> BucketLease {
        self.leases.fetch_add(1, AcqRel);
        BucketLease {
            runtime: self.clone(),
        }
    }

    fn release_lease(&self) {
        self.leases.fetch_sub(1, AcqRel);
    }

    pub(crate) fn lease_count(&self) -> usize {
        self.leases.load(Acquire)
    }

    pub(crate) fn begin_rewrite(self: &Arc<Self>) -> BucketRewriteGuard {
        self.rewrite_inflight.fetch_add(1, AcqRel);
        BucketRewriteGuard {
            runtime: self.clone(),
        }
    }

    pub(crate) fn has_rewrite(&self) -> bool {
        self.rewrite_inflight.load(Acquire) != 0
    }

    pub(crate) fn observe_stat_epoch(&self, kind: crate::meta::FileKind, up1: u64, up2: u64) {
        let epoch = up1.max(up2).saturating_add(1).max(1);
        self.update_epochs[kind.slot()].fetch_max(epoch, Relaxed);
    }

    pub(crate) fn load_update_epoch(&self, kind: crate::meta::FileKind) -> u64 {
        self.update_epochs[kind.slot()].load(Relaxed)
    }

    pub(crate) fn next_update_epoch(&self, kind: crate::meta::FileKind) -> u64 {
        self.update_epochs[kind.slot()].fetch_add(1, Relaxed)
    }

    /// record what the reclaim in progress does with the sealed batch
    pub(crate) fn begin_reclaim(&self, mode: ReclaimMode) {
        self.reclaim.store(mode.encode(), Release);
    }

    pub(crate) fn clear_reclaim(&self) {
        self.reclaim.store(ReclaimMode::NONE, Release);
    }

    /// `None` means no reclaim is in progress, so a checkpoint publishes normally
    pub(crate) fn reclaim_mode(&self) -> Option<ReclaimMode> {
        ReclaimMode::decode(self.reclaim.load(Acquire))
    }
}

/// explicit lease on a bucket's runtime, replacing the `Arc<BucketMeta>` strong-count check
///
/// cloning a `Bucket` takes one more lease; dropping it releases one. Unload and delete refuse
/// while any lease is held, which is the only busy signal they consult for open handles
pub(crate) struct BucketLease {
    runtime: Arc<BucketRuntime>,
}

impl Clone for BucketLease {
    fn clone(&self) -> Self {
        self.runtime.leases.fetch_add(1, AcqRel);
        Self {
            runtime: self.runtime.clone(),
        }
    }
}

impl Drop for BucketLease {
    fn drop(&mut self) {
        self.runtime.release_lease();
    }
}

pub(crate) struct BucketRewriteGuard {
    runtime: Arc<BucketRuntime>,
}

impl Drop for BucketRewriteGuard {
    fn drop(&mut self) {
        self.runtime.rewrite_inflight.fetch_sub(1, AcqRel);
    }
}

pub(crate) struct BucketRewritePermit {
    pub(crate) enable_compression: bool,
    pub(crate) compressors: Arc<CompressorPool>,
    pub(crate) _guard: BucketRewriteGuard,
}

pub(crate) struct BucketContext {
    pub(crate) pool: Handle<Pool>,
    pub(crate) table: MutRef<PageMap>,
    pub(crate) state: MutRef<BucketState>,
    pub(crate) opt: Arc<BucketOptions>,
    pub(crate) has_merge: AtomicBool,
    pub(crate) data_intervals: RwLock<IntervalMap>,
    pub(crate) blob_intervals: RwLock<IntervalMap>,
    pub(crate) lru: Handle<ShardPriorityLru<BoxRef>>,
    pub(crate) bucket_id: u64,
    pub(crate) reader: Handle<Manifest>,
    /// exact keys whose merge chain produced a contract violation
    /// runtime-only and cleared when the bucket is unloaded
    pub(crate) merge_blocked_keys: DashSet<Vec<u8>>,
    ctx: Handle<Context>,
    cache: NodeCache,
    candidates: CandidateRing,
    tx: Sender<SharedState>,
    candidate_tick: AtomicU64,
    evict_pending: AtomicBool,
    final_checkpointed: AtomicBool,
    reclaimed: AtomicBool,
    /// a context built for creation but not yet published must not checkpoint on drop: its
    /// bucket has no runtime in the manager yet, and a queued task would abort the flusher
    unpublished: AtomicBool,
}

impl BucketContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        ctx: Handle<Context>,
        global: &ParsedOptions,
        opt: Arc<BucketOptions>,
        state: MutRef<BucketState>,
        bucket_id: u64,
        table: MutRef<PageMap>,
        flush: Checkpoint,
        lru: Handle<ShardPriorityLru<BoxRef>>,
        reader: Handle<Manifest>,
        tx: Sender<SharedState>,
        compressors: Arc<CompressorPool>,
    ) -> Self {
        let flow = Arc::new(FlowController::new(global, opt.as_ref()));
        let pool = Handle::new(Pool::new(
            opt.as_ref(),
            compressors.clone(),
            table.clone(),
            state.clone(),
            flow,
            flush,
            bucket_id,
        ));

        Self {
            pool,
            table,
            state,
            has_merge: AtomicBool::new(opt.merge_operator.is_some()),
            opt,
            data_intervals: RwLock::new(IntervalMap::new()),
            blob_intervals: RwLock::new(IntervalMap::new()),
            lru,
            bucket_id,
            reader,
            merge_blocked_keys: DashSet::new(),
            ctx,
            cache: NodeCache::new(),
            candidates: CandidateRing::new(CANDIDATE_RING_SIZE),
            tx,
            candidate_tick: AtomicU64::new(0),
            evict_pending: AtomicBool::new(false),
            final_checkpointed: AtomicBool::new(false),
            reclaimed: AtomicBool::new(false),
            unpublished: AtomicBool::new(true),
        }
    }

    /// the context is now visible through the bucket manager and may checkpoint on drop
    pub(crate) fn publish(&self) {
        self.unpublished.store(false, Release);
    }

    /// validates a runtime operator against the one fixed when this context was loaded
    pub(crate) fn validate_merge_operator(
        &self,
        op: Option<&Arc<dyn MergeOperator>>,
    ) -> Result<(), OpCode> {
        let Some(op) = op else {
            return Ok(());
        };
        if let Some(existing) = self.opt.merge_operator.as_ref()
            && Arc::ptr_eq(existing, op)
        {
            return Ok(());
        }
        Err(OpCode::Invalid)
    }

    #[inline(always)]
    pub(crate) fn merge_operator(&self) -> Option<&dyn MergeOperator> {
        self.opt.merge_operator.as_deref()
    }

    #[inline]
    pub(crate) fn mark_merge(&self) {
        self.has_merge.store(true, Release);
    }

    pub(crate) fn context(&self) -> Handle<Context> {
        self.ctx
    }

    pub(crate) fn before_foreground_write(&self, bytes: u64) -> ForegroundWritePermit {
        self.pool.before_foreground_write(bytes)
    }

    pub(crate) fn has_pending_flush_data(&self) -> bool {
        self.pool.has_pending_flush_data()
    }

    /// periodic checkpoint nudge from the evictor
    ///
    /// the nudge slot is taken before the lifecycle check so a reclaim that already began can
    /// wait for this attempt; a nudge refused by the lifecycle never enqueues a checkpoint task
    pub(crate) fn nudge_checkpoint(&self, min_interval_ms: u64) {
        let Some(_slot) = self.state.try_nudge() else {
            return;
        };
        self.pool.nudge_checkpoint(min_interval_ms)
    }

    pub(crate) fn checkpoint(&self) {
        self.pool.checkpoint(false);
    }

    pub(crate) fn loader(&self, ctx: Handle<Context>) -> Loader {
        Loader {
            pool: self.pool,
            ctx,
            lru: self.lru,
            node_pins: MutRef::new(Loader::new_node_pins(Loader::PIN_CAP)),
            bucket_id: self.bucket_id,
            reader: self.reader,
        }
    }

    pub(crate) fn cache(&self, p: Page) {
        let size = p.size() as isize;
        let state = if p.is_intl() {
            CacheState::Hot
        } else {
            CacheState::Warm
        };
        self.cache.insert(p.pid(), state, size);
        self.candidates.push(p.pid());
        self.maybe_evict();
    }

    pub(crate) fn load(&self, pid: u64) -> Option<Page> {
        loop {
            let swip = Swip::new(self.table.get(pid));
            if swip.is_null() {
                return None;
            }
            if !swip.is_tagged() {
                let page = Page::from_swip(swip.raw());
                page.mark_recent();
                return Some(page);
            }
            let new = Page::load(self.loader(self.ctx), swip.untagged());
            if self.table.cas(pid, swip.raw(), new.swip()).is_ok() {
                self.cache(new);
                return Some(new);
            } else {
                new.reclaim();
            }
        }
    }

    pub(crate) fn warm(&self, pid: u64, size: usize) {
        self.cache.touch(pid, size as isize);
        // avoid sampling when cache is far from pressure to reduce atomic contention
        let used = self.cache.used();
        if used >= (self.opt.cache_capacity as isize >> 2) {
            self.maybe_push_candidate(pid);
        }
        self.maybe_evict();
    }

    pub(crate) fn warm_state(&self, pid: u64) {
        self.cache.warm(pid)
    }

    pub(crate) fn cool(&self, pid: u64) -> Option<CacheState> {
        self.cache.cool(pid)
    }

    pub(crate) fn cache_state(&self, pid: u64) -> Option<CacheState> {
        self.cache.state(pid)
    }

    pub(crate) fn evict_cache(&self, pid: u64) -> bool {
        self.cache.evict(pid)
    }

    pub(crate) fn candidate_snapshot(&self) -> Vec<u64> {
        self.candidates.snapshot()
    }

    pub(crate) fn almost_full(&self) -> bool {
        let threshold = self.opt.cache_capacity as isize * 80 / 100;
        self.cache.used() >= threshold
    }

    pub(crate) fn evict_sample_target(&self, candidates: usize) -> usize {
        if candidates == 0 {
            return 0;
        }
        let pct = self.opt.cache_evict_pct.min(100);
        if pct == 0 {
            return 0;
        }
        (candidates * pct / 100).max(1).min(candidates)
    }

    pub(crate) fn begin_eviction(&self) -> bool {
        self.evict_pending.swap(false, AcqRel)
    }

    fn maybe_push_candidate(&self, pid: u64) {
        let tick = self.candidate_tick.fetch_add(1, Relaxed) + 1;
        if tick.is_multiple_of(CANDIDATE_SAMPLE_RATE) {
            self.candidates.push(pid);
        }
    }

    fn maybe_evict(&self) {
        if !self.almost_full() {
            return;
        }
        if self.evict_pending.swap(true, AcqRel) {
            return;
        }
        if self.tx.send(SharedState::Evict(self.bucket_id)).is_err() {
            self.evict_pending.store(false, Release);
        }
    }

    fn reclaim_pages(&self) {
        let end = self.table.len();
        for pid in ROOT_PID..end {
            let swip = Swip::new(self.table.get(pid));
            if swip.is_null() || swip.is_tagged() {
                continue;
            }
            let _ = self.cache.evict(pid);
            let page = Page::from_swip(swip.untagged());
            page.reclaim();
        }
    }

    pub(crate) fn checkpoint_and_wait(&self, force_fsync: bool) {
        if self.reclaimed.load(Acquire) {
            return;
        }
        self.pool.checkpoint_and_wait_fresh(force_fsync);
    }

    fn flush_and_wait(&self) {
        self.pool.checkpoint(false);
        self.pool.wait_checkpoint();
    }

    pub(crate) fn checkpoint_before_reclaim(&self, force_fsync: bool) {
        // a reclaim cycle needs at most one final checkpoint
        if self.reclaimed.load(Acquire) || self.final_checkpointed.load(Acquire) {
            return;
        }
        self.pool.checkpoint_and_wait_fresh(force_fsync);
        self.final_checkpointed.store(true, Release);
    }

    /// drop the sealed batch of a logically deleted bucket
    ///
    /// the forced round drains whatever batch was already queued through the discard directive -
    /// those generations are released with the context, not published - and `final_checkpointed`
    /// keeps the context drop from publishing a fresh one
    pub(crate) fn discard(&self) {
        if self.reclaimed.load(Acquire) || self.final_checkpointed.load(Acquire) {
            return;
        }
        self.pool.checkpoint_and_wait_fresh(false);
        self.final_checkpointed.store(true, Release);
    }

    pub(crate) fn reclaim(&self) {
        if self
            .reclaimed
            .compare_exchange(false, true, AcqRel, Acquire)
            .is_err()
        {
            return;
        }
        // buckets may still be reclaimed without an explicit final checkpoint path; a bucket that
        // is unloading or deleting already had its dirty state published or discarded by the
        // reclaim site, so a context that outlived it must not queue a fresh batch
        if !self.unpublished.load(Acquire)
            && !self.final_checkpointed.swap(false, AcqRel)
            && self.state.is_active()
        {
            self.flush_and_wait();
        }
        self.reclaim_pages();
        Handle::reclaim(&self.pool);
    }
}

impl Drop for BucketContext {
    fn drop(&mut self) {
        self.reclaim();
    }
}

pub(crate) struct BucketManager {
    buckets: DashMap<u64, Arc<BucketContext>>,
    runtimes: DashMap<u64, Arc<BucketRuntime>>,
    pub(crate) lru: Handle<ShardPriorityLru<BoxRef>>,
    pub(crate) flush: Option<Checkpoint>,
    pub(crate) tx: Sender<SharedState>,
    pub(crate) rx: Receiver<()>,
    pub(crate) ctx: Handle<Context>,
    pub(crate) reader: Option<Handle<Manifest>>,
}

impl BucketManager {
    pub(crate) fn new(
        opt: Arc<ParsedOptions>,
        ctx: Handle<Context>,
        tx: Sender<SharedState>,
        rx: Receiver<()>,
    ) -> Self {
        Self {
            buckets: DashMap::new(),
            runtimes: DashMap::new(),
            lru: Handle::new(ShardPriorityLru::new(opt.lru_capacity)),
            flush: None,
            tx,
            rx,
            ctx,
            reader: None,
        }
    }

    pub(crate) fn set_context(&mut self, ctx: Handle<Context>, reader: Handle<Manifest>) {
        self.ctx = ctx;
        self.reader = Some(reader);
    }

    pub(crate) fn start(
        &mut self,
        ctx: Handle<Context>,
        reader: Handle<Manifest>,
        observer: Arc<ManifestCheckpointObserver>,
    ) -> Result<(), OpCode> {
        // this is the only site that replaces the flusher, so the rule that a live one must
        // be stopped first belongs here: an overwritten `Checkpoint` has no `Drop`, so its
        // thread and its tasks would outlive the rebind
        if self.flush.is_some() {
            log::error!("refusing to start a checkpoint flusher while one is running");
            return Err(OpCode::IoError);
        }
        // build the flusher before rebinding: a failed spawn must leave the manager on its
        // previous context instead of pointing at a context the caller is about to drop
        let flush = Checkpoint::new(ctx, observer)?;
        self.set_context(ctx, reader);
        self.flush = Some(flush);
        Ok(())
    }

    fn reclaim_lru(&mut self) {
        if !self.lru.inner().is_null() {
            self.lru.reclaim();
            self.lru = Handle::from(null_mut());
        }
    }

    pub(crate) fn abort(&mut self) {
        // loaded buckets may still hold recovery-built dirty state; keep the flusher alive
        // until BucketContext::drop finishes its own checkpoint closure
        self.buckets.clear();

        if let Some(f) = self.flush.take() {
            f.quit();
        }

        self.reclaim_lru();
    }

    pub(crate) fn quit(&mut self) {
        // 1) stop evictor to avoid new eviction work while draining flushes
        let _ = self.tx.send(SharedState::Quit);
        let _ = self.rx.recv();

        // 2) final-checkpoint every loaded bucket with a full file synchronization: the exiting
        // process no longer has a WAL that guards the pages it publishes
        let loaded: Vec<(u64, Arc<BucketContext>)> = self.contexts();
        for (bucket_id, ctx) in &loaded {
            self.get_runtime(*bucket_id)
                .begin_reclaim(ReclaimMode::FinalCheckpoint { force_fsync: true });
            ctx.checkpoint_before_reclaim(true);
        }

        // 3) stop flusher after outstanding flush tasks are drained
        if let Some(f) = self.flush.take() {
            f.quit();
        }

        // 4) release bucket contexts after the final checkpoint is already done
        self.buckets.clear();
        drop(loaded);

        // 5) release shared lru
        self.reclaim_lru();
    }

    pub(crate) fn context(&self, bucket_id: u64) -> Option<Arc<BucketContext>> {
        self.buckets.get(&bucket_id).map(|x| x.value().clone())
    }

    pub(crate) fn contains_context(&self, bucket_id: u64) -> bool {
        self.buckets.contains_key(&bucket_id)
    }

    pub(crate) fn contexts(&self) -> Vec<(u64, Arc<BucketContext>)> {
        self.buckets
            .iter()
            .map(|x| (*x.key(), x.value().clone()))
            .collect()
    }

    pub(crate) fn contexts_is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    pub(crate) fn insert_context(&self, bucket_id: u64, ctx: Arc<BucketContext>) {
        self.buckets.insert(bucket_id, ctx);
    }

    pub(crate) fn runtime(&self, bucket_id: u64) -> Option<Arc<BucketRuntime>> {
        self.runtimes.get(&bucket_id).map(|x| x.value().clone())
    }

    pub(crate) fn get_runtime(&self, bucket_id: u64) -> Arc<BucketRuntime> {
        must_exist!(self.runtime(bucket_id), "bucket runtime must exist")
    }

    // bucket runtime remains alive across load/unload so background GC can read lifecycle state
    // and reuse compression contexts without recreating the full bucket context
    pub(crate) fn ensure_runtime(&self, bucket_id: u64) -> Arc<BucketRuntime> {
        self.runtimes
            .entry(bucket_id)
            .or_insert_with(|| Arc::new(BucketRuntime::new()))
            .value()
            .clone()
    }

    /// publish the runtime a create guard has already bound to its context and lease
    ///
    /// creation must insert this exact `Arc`; allocating another runtime here would split the
    /// lease/state identity from the one the loaded context uses
    pub(crate) fn insert_runtime(&self, bucket_id: u64, runtime: Arc<BucketRuntime>) {
        debug_assert!(
            self.runtime(bucket_id).is_none(),
            "bucket {bucket_id} already had a runtime"
        );
        self.runtimes.insert(bucket_id, runtime);
    }

    pub(crate) fn remove_runtime(&self, bucket_id: u64) {
        let _ = self.runtimes.remove(&bucket_id);
    }

    /// take a loaded bucket out of service with an explicit reclaim mode
    ///
    /// the mode outlives the context removal for `Discard`, so a sealed batch enqueued before the
    /// delete still resolves to `Skip` after the runtime was retired by physical cleanup
    pub(crate) fn reclaim_bucket(&self, bucket_id: u64, mode: ReclaimMode) {
        let runtime = self.get_runtime(bucket_id);
        runtime.begin_reclaim(mode);
        if let Some(ctx) = self.context(bucket_id) {
            match mode {
                ReclaimMode::FinalCheckpoint { force_fsync } => {
                    ctx.checkpoint_before_reclaim(force_fsync)
                }
                ReclaimMode::Discard => ctx.discard(),
            }
        }
        let _ = self.buckets.remove(&bucket_id);
        if matches!(mode, ReclaimMode::FinalCheckpoint { .. }) {
            // a finalized bucket returns to service as soon as its context is gone; a discard
            // keeps its mode until physical cleanup retires the runtime
            runtime.clear_reclaim();
        }
    }

    pub(crate) fn unload_all(&self, mode: ReclaimMode) {
        let bucket_ids: Vec<u64> = self.buckets.iter().map(|x| *x.key()).collect();
        for bucket_id in bucket_ids {
            self.reclaim_bucket(bucket_id, mode);
        }
    }

    pub(crate) fn active_contexts(&self) -> Vec<Arc<BucketContext>> {
        self.buckets
            .iter()
            .filter_map(|x| {
                let ctx = x.value().clone();
                if ctx.state.is_active() {
                    Some(ctx)
                } else {
                    None
                }
            })
            .collect()
    }

    /// the loaded context of an active bucket: a bucket that is unloading or deleting is never
    /// handed back to a new reader or to the evictor
    pub(crate) fn context_if_active(&self, bucket_id: u64) -> Option<Arc<BucketContext>> {
        let ctx = self.context(bucket_id)?;
        ctx.state.is_active().then_some(ctx)
    }
}
