use super::*;

impl Context {
    pub(crate) fn alloc_view_pin(&self) -> Handle<CCNode> {
        let pin = self.pool.alloc();
        pin.begin_reg();
        #[cfg(feature = "extra_check")]
        crate::testing::fire_view_sync_point(
            crate::testing::ViewSyncPoint::AfterCcnodeRegisteringBeforeTimestampSample,
        );
        let start_ts = self.sample_view_oracle();
        pin.activate(start_ts);
        pin
    }

    pub fn free_view_pin(&self, pin: Handle<CCNode>) {
        pin.clear_idle();
        self.pool.free(pin);
    }

    /// test-determinism wakeup (extra_check): nudges the collector so a test
    /// can drive safe_exclusive advancement synchronously instead of waiting
    /// for the background duty-cycle. production keeps the pure polling
    /// cadence — delayed reclamation is a designed tradeoff, not a leak.
    #[cfg(feature = "extra_check")]
    pub(crate) fn request_collect(&self) {
        let _ = self.tx.send(CollectorSignal::Wake);
    }

    #[inline]
    pub(crate) fn safe_exclusive(&self) -> u64 {
        self.safe_exclusive.load(Acquire)
    }

    #[inline(always)]
    pub(crate) fn recovering(&self) -> bool {
        self.recovering.load(Acquire)
    }

    pub(crate) fn init_safe_exclusive(&self, recovered_oracle: u64) {
        // recovery must not regress a collector-published boundary
        self.safe_exclusive.fetch_max(recovered_oracle, AcqRel);
        self.recovering.store(false, Release);
    }

    #[inline]
    fn sample_view_oracle(&self) -> u64 {
        self.sequences.oracle.load(SeqCst) // must use seqcst
    }

    pub(crate) fn alloc_oracle(&self) -> u64 {
        self.sequences.oracle.fetch_add(1, AcqRel)
    }

    #[inline]
    pub(crate) fn alloc_begin_oracle(&self) -> u64 {
        self.sequences.oracle.fetch_add(1, SeqCst) // must use seqcst
    }

    #[inline]
    pub(crate) fn compact_safe_txid(&self) -> u64 {
        let mut safe = self.safe_exclusive().saturating_sub(1);
        let pending_floor = self
            .pending_abort_clean_seqlock
            .read(|| self.pending_abort_clean_floor.load(Relaxed));
        if pending_floor != u64::MAX {
            safe = safe.min(pending_floor.saturating_sub(1));
        }
        safe
    }
}

#[cfg_attr(not(feature = "extra_check"), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CollectorSignal {
    Wake,
    Quit,
}

#[inline]
pub(super) fn two_choices(ticket: usize, nr: usize) -> (usize, usize) {
    debug_assert!(nr > 0);
    let home = ticket % nr;
    if nr == 1 {
        return (home, home);
    }

    // advance the offset after each full home round so every home visits every other choice
    let round = ticket / nr;
    let offset = 1 + round % (nr - 1);

    let second = home + offset;
    (home, if second >= nr { second - nr } else { second })
}

pub(super) fn collect_thread(
    reader: Receiver<CollectorSignal>,
    sequences: Arc<Sequences>,
    groups: Arc<Vec<WriterGroup>>,
    safe_exclusive: Arc<AtomicU64>,
    pool: Arc<CCPool>,
) -> JoinHandle<()> {
    // one collector per mace instance; the token distinguishes this
    // instance's collector from every other instance's in the process
    #[cfg(feature = "extra_check")]
    let collector_token = Arc::as_ptr(&sequences) as usize;
    std::thread::Builder::new()
        .name("collector".into())
        .spawn(move || {
            let mut committed = Vec::new();
            let mut registry_nodes = Vec::with_capacity(CCPOOL_SHARD);
            let mut idle_delay = collector_idle_delay(Duration::ZERO);
            while let Err(RecvTimeoutError::Timeout) | Ok(CollectorSignal::Wake) =
                reader.recv_timeout(idle_delay)
            {
                let cost = run_collect_cycle(
                    &sequences,
                    groups.as_ref(),
                    safe_exclusive.as_ref(),
                    &pool,
                    &mut committed,
                    &mut registry_nodes,
                    #[cfg(feature = "extra_check")]
                    collector_token,
                );
                idle_delay = collector_idle_delay(cost);
                if drain_pending_wakes(&reader) {
                    break;
                }
            }
        })
        .expect("can't start collector thread")
}

const MAINTENANCE_BUDGET: Duration = Duration::from_micros(200);
pub(super) const COLLECTOR_MIN_SLEEP: Duration = Duration::from_millis(10);
const PRUNE_LOCK_BATCH: usize = 256;
pub(super) const DUTY_CYCLE_SLEEP_MULTIPLIER: u32 = 19;

fn run_collect_cycle(
    sequences: &Sequences,
    groups: &[WriterGroup],
    safe_exclusive: &AtomicU64,
    pool: &CCPool,
    committed: &mut Vec<(usize, u64, u64)>,
    registry_nodes: &mut Vec<*mut CCNode>,
    #[cfg(feature = "extra_check")] collector_token: usize,
) -> Duration {
    let proof_scan_started = Instant::now();
    // `SeqCst` linearizes this cut with writer/view registration and timestamp sampling (`oracle`,
    // `WriterGroup::txn_seq`, and `CCNode::state`)
    //
    // forbidden execution: the collector misses a live writer or view registration while it
    // samples start_ts < cut
    //
    // if the scan does not cover a registration that stays live, its state load must precede the
    // `Registering` store in the `SeqCst` order; because this cut precedes the scan and registration
    // precedes the oracle allocation/sample, the resulting cut < scan < registration < oracle order
    // proves start_ts >= cut
    let cut = sequences.oracle.load(SeqCst);
    #[cfg(feature = "extra_check")]
    crate::testing::fire_collector_sync_point(
        crate::testing::CollectorSyncPoint::AfterCollectorCutBeforeFactScan,
        cut,
    );

    let mut floor = cut;
    let mut can_publish = true;
    committed.clear();

    for group in groups.iter() {
        match group.stable_ts() {
            RegistrationTs::None => {}
            RegistrationTs::Published(start_ts) => {
                floor = floor.min(start_ts);
            }
            RegistrationTs::Pending => {
                can_publish = false;
            }
        }
        for fact in group.facts.iter() {
            let start_ts = *fact.key();
            match *fact.value() {
                TxnFact::Active(_) => {
                    floor = floor.min(start_ts);
                }
                TxnFact::Committed(commit_ts) => {
                    committed.push((group.id, start_ts, commit_ts));
                }
                TxnFact::Aborted => {}
            }
        }
    }

    let _guard = crossbeam_epoch::pin();
    snapshot_registry_nodes(pool, registry_nodes);
    for &node in registry_nodes.iter() {
        let cc = unsafe { &*node };
        match cc.collect() {
            ViewState::Idle => {}
            ViewState::Registering => {
                can_publish = false;
            }
            ViewState::Active(start_ts) => {
                floor = floor.min(start_ts);
            }
        }
    }

    let mut published_safe = safe_exclusive.load(Acquire);
    if can_publish {
        let mut committed_floor = u64::MAX;
        for &(_, start_ts, commit_ts) in committed.iter() {
            if commit_ts >= floor {
                committed_floor = committed_floor.min(start_ts);
            }
        }
        let candidate = floor.min(committed_floor);
        debug_assert!(candidate <= cut);
        debug_assert!(candidate >= published_safe);
        if candidate > published_safe {
            safe_exclusive.store(candidate, Release);
            published_safe = candidate;
        }
    }
    let proof_scan_cost = proof_scan_started.elapsed();

    let deadline = Instant::now() + MAINTENANCE_BUDGET;
    if published_safe != 0 {
        #[cfg(feature = "extra_check")]
        crate::testing::fire_collector_sync_point(
            crate::testing::CollectorSyncPoint::AfterSafePublishBeforeCommittedFactPrune,
            published_safe,
        );
        prune_committed_facts(groups, committed, published_safe, deadline);
    }
    #[cfg(feature = "extra_check")]
    crate::testing::fire_collector_completed(collector_token);
    if Instant::now() < deadline {
        pool.maybe_shrink_until(deadline);
    }
    proof_scan_cost
}

fn prune_committed_facts(
    groups: &[WriterGroup],
    committed: &[(usize, u64, u64)],
    safe_exclusive: u64,
    deadline: Instant,
) {
    for (idx, &(group_id, start_ts, _)) in committed.iter().enumerate() {
        if start_ts >= safe_exclusive {
            continue;
        }
        if idx > 0 && idx.is_multiple_of(PRUNE_LOCK_BATCH) && Instant::now() >= deadline {
            break;
        }
        groups[group_id].facts.remove(&start_ts);
    }
}

pub(super) fn collector_idle_delay(proof_scan_cost: Duration) -> Duration {
    proof_scan_cost
        .checked_mul(DUTY_CYCLE_SLEEP_MULTIPLIER)
        .map_or(Duration::MAX, |delay| delay.max(COLLECTOR_MIN_SLEEP))
}

pub(super) fn drain_pending_wakes(reader: &Receiver<CollectorSignal>) -> bool {
    loop {
        match reader.try_recv() {
            Ok(CollectorSignal::Wake) => {}
            Ok(CollectorSignal::Quit) | Err(TryRecvError::Disconnected) => return true,
            Err(TryRecvError::Empty) => return false,
        }
    }
}

fn snapshot_registry_nodes(pool: &CCPool, registry_nodes: &mut Vec<*mut CCNode>) {
    let registry = pool.registry.read();
    registry_nodes.clear();
    registry_nodes.extend(registry.iter().map(|node| node.inner()));
}

pub(super) const CCPOOL_SHARD: usize = 32;

const CCPOOL_SHARD_MASK: usize = CCPOOL_SHARD - 1;

const VIEW_STATE_IDLE: u64 = 0;
const VIEW_STATE_REGISTERING: u64 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ViewState {
    Idle,
    Registering,
    Active(u64),
}

#[inline]
fn encode_view_state(state: ViewState) -> u64 {
    match state {
        ViewState::Idle => VIEW_STATE_IDLE,
        ViewState::Registering => VIEW_STATE_REGISTERING,
        ViewState::Active(start_ts) => {
            must_true!(start_ts <= u64::MAX - 2);
            start_ts + 2
        }
    }
}

#[inline]
fn decode_view_state(raw: u64) -> ViewState {
    match raw {
        VIEW_STATE_IDLE => ViewState::Idle,
        VIEW_STATE_REGISTERING => ViewState::Registering,
        x => ViewState::Active(x - 2),
    }
}

pub(crate) struct CCNode {
    state: AtomicU64,
    pub(super) shard_index: usize,
    registry_index: usize,
}

impl CCNode {
    pub(super) fn new() -> Self {
        Self {
            state: AtomicU64::new(encode_view_state(ViewState::Idle)),
            shard_index: 0,
            registry_index: 0,
        }
    }

    pub(super) fn begin_reg(&self) {
        must_true!(matches!(
            decode_view_state(self.state.load(Relaxed)),
            ViewState::Idle
        ));
        self.state
            .store(encode_view_state(ViewState::Registering), SeqCst);
    }

    pub(super) fn activate(&self, start_ts: u64) {
        must_true!(matches!(
            decode_view_state(self.state.load(Relaxed)),
            ViewState::Registering
        ));
        self.state
            .store(encode_view_state(ViewState::Active(start_ts)), Release);
    }

    pub(super) fn clear_idle(&self) {
        self.state
            .store(encode_view_state(ViewState::Idle), Release);
    }

    pub(crate) fn start_ts(&self) -> u64 {
        match decode_view_state(self.state.load(Acquire)) {
            ViewState::Active(start_ts) => start_ts,
            ViewState::Idle | ViewState::Registering => NULL_ORACLE,
        }
    }

    pub(super) fn collect(&self) -> ViewState {
        decode_view_state(self.state.load(SeqCst))
    }
}

/// bound on idle pins a thread keeps out of the shared shards: enough to
/// absorb short-view churn reuse, small enough that thread exits strand at
/// most this many nodes for the pool-drop registry reclaim
const LOCAL_PINS_MAX: usize = 2;

/// per-thread idle-pin cache for one CCPool. holds only non-owning Handle
/// copies of nodes that are Idle and still owned by the core registry. the
/// Weak back-reference lets the drop return them to their shard while the
/// pool is still alive.
struct LocalPins {
    nodes: Vec<Handle<CCNode>>,
    core: Weak<CCPool>,
}

impl LocalPins {
    fn new(core: Weak<CCPool>) -> Self {
        Self {
            nodes: Vec::with_capacity(LOCAL_PINS_MAX),
            core,
        }
    }
}

impl Drop for LocalPins {
    fn drop(&mut self) {
        // runs OUTSIDE the registry threads lock on both teardown paths
        // (ThreadTable::drop drops entries after releasing the lock;
        // LocalSlot::drain drops retired values outside its lock scope), so
        // dropping an upgraded Arc here cannot re-enter the threads lock
        let Some(core) = self.core.upgrade() else {
            // pool core is already dropping: nodes stay registered and
            // CCPool::drop's registry walk reclaims them exactly once
            return;
        };
        for node in self.nodes.drain(..) {
            core.push_shard(node.shard_index, node);
        }
        // dropping `core` may run CCPool::drop if this was the last strong
        // reference; safe because we hold no registry lock here
    }
}

pub(super) struct CCPool {
    pub(super) shards: [Mutex<Vec<Handle<CCNode>>>; CCPOOL_SHARD],
    shard_index: CachePad<AtomicUsize>,
    // TODO: maybe change to seqlock ?
    registry: RwLock<Vec<Handle<CCNode>>>,
    pub(super) registry_len: CachePad<AtomicUsize>,
    /// per-thread idle-pin cache: each thread keeps up to LOCAL_PINS_MAX
    /// idle nodes out of the shard mutexes
    pins: LocalSlot<LocalPins>,
    /// weak handle to this pool, so a LocalPins drop on thread exit can reach
    /// the shards only while the pool is alive (armed once at construction)
    self_weak: OnceLock<Weak<CCPool>>,
}

impl CCPool {
    /// builds the pool behind an Arc: the self weak back-reference used by the
    /// thread-exit idle-pin return is armed right after allocation
    pub(super) fn new() -> Arc<Self> {
        let pool = Arc::new(Self {
            shards: std::array::from_fn(|_| Mutex::new(Vec::new())),
            shard_index: CachePad::default(),
            registry: RwLock::new(Vec::new()),
            registry_len: CachePad::from(AtomicUsize::new(0)),
            pins: LocalSlot::new(),
            self_weak: OnceLock::new(),
        });
        // prime the initial shard population (one node per shard)
        {
            let mut registry = pool.registry.write();
            for shard in pool.shards.iter() {
                let mut h = Handle::new(CCNode::new());
                h.registry_index = registry.len();
                registry.push(h);
                shard.lock().push(h);
            }
            pool.registry_len.store(registry.len(), Release);
        }
        pool.self_weak
            .set(Arc::downgrade(&pool))
            .expect("self weak armed once");
        pool
    }

    fn next_ticket(&self) -> usize {
        self.shard_index.fetch_add(1, Relaxed)
    }

    fn try_pop_shard(&self, index: usize) -> Option<Handle<CCNode>> {
        let mut h = self.shards[index].lock().pop()?;
        h.shard_index = index;
        Some(h)
    }

    fn push_shard(&self, index: usize, cc: Handle<CCNode>) {
        self.shards[index].lock().push(cc);
    }

    pub(super) fn alloc(&self) -> Handle<CCNode> {
        if let Some(pin) = self.pop_local_pin() {
            return pin;
        }
        let ticket = self.next_ticket();
        let (shard, second) = two_choices(ticket, CCPOOL_SHARD);

        if let Some(x) = self.try_pop_shard(shard) {
            x
        } else if let Some(x) = self.try_pop_shard(second) {
            x
        } else {
            let mut cc = Handle::new(CCNode::new());
            cc.shard_index = shard;
            let mut r = self.registry.write();
            cc.registry_index = r.len();
            r.push(cc);
            self.registry_len.store(r.len(), Release);
            cc
        }
    }

    pub(super) fn free(&self, cc: Handle<CCNode>) {
        if self.cache_local_pin(cc) {
            return;
        }
        self.push_shard(cc.shard_index, cc);
    }

    /// pop one idle node from this thread's local cache, if it has one
    fn pop_local_pin(&self) -> Option<Handle<CCNode>> {
        self.pins.with(
            || LocalPins::new(self.self_weak.get().cloned().expect("pool self weak armed")),
            |local| local.nodes.pop(),
        )
    }

    /// cache an idle node on this thread (up to LOCAL_PINS_MAX); returns true
    /// when cached, false when the local list is full and the caller must
    /// spill it to the shared shard
    fn cache_local_pin(&self, cc: Handle<CCNode>) -> bool {
        let spill = self.pins.with(
            // init must NOT push: with() runs f on the init-created value too,
            // so a push here would insert the node twice into one list
            || LocalPins::new(self.self_weak.get().cloned().expect("pool self weak armed")),
            |local| {
                if local.nodes.len() < LOCAL_PINS_MAX {
                    local.nodes.push(cc);
                    None
                } else {
                    Some(cc)
                }
            },
        );
        spill.is_none()
    }

    fn maybe_shrink_one(&self, start: usize, guard: &Guard) -> bool {
        let mut victim = None;
        for i in 0..CCPOOL_SHARD {
            let idx = (start + i) & CCPOOL_SHARD_MASK;
            if let Some(cc) = self.try_pop_shard(idx) {
                victim = Some((idx, cc));
                break;
            }
        }
        let Some((idx, cc)) = victim else {
            return false;
        };
        let Some(mut r) = self.registry.try_write() else {
            self.push_shard(idx, cc);
            return false;
        };
        if r.len() <= CCPOOL_SHARD {
            drop(r);
            self.push_shard(idx, cc);
            return false;
        }

        let last = r.swap_remove(cc.registry_index);
        must_true!(eq last.inner(), cc.inner());
        if cc.registry_index < r.len() {
            r[cc.registry_index].registry_index = cc.registry_index;
        }
        self.registry_len.store(r.len(), Release);
        drop(r);
        guard.defer(move || cc.reclaim());
        true
    }

    pub(super) fn maybe_shrink_until(&self, deadline: Instant) {
        let len = self.registry_len.load(Acquire);
        if len <= CCPOOL_SHARD {
            return;
        }
        let backlog = len - CCPOOL_SHARD;
        let mut batch = backlog / 8;
        if !backlog.is_multiple_of(8) {
            batch += 1;
        }
        batch = batch.clamp(1, 16);

        let guard = crossbeam_epoch::pin();
        let mut start = self.next_ticket() & CCPOOL_SHARD_MASK;
        let mut done = 0;
        while done < batch
            && self.registry_len.load(Acquire) > CCPOOL_SHARD
            && Instant::now() < deadline
        {
            if !self.maybe_shrink_one(start, &guard) {
                break;
            }
            done += 1;
            start = (start + 1) & CCPOOL_SHARD_MASK;
        }
    }
}

impl Drop for CCPool {
    fn drop(&mut self) {
        // last Arc drop is the quiescent point: remove every thread's LocalPins
        // cell before reclaiming registry allocations, so no thread-exit drop
        // can observe a half-torn pool (idempotent: slot Drop after this no-ops)
        self.pins.drain();
        let mut r = self.registry.write();
        while let Some(x) = r.pop() {
            x.reclaim();
        }
    }
}
