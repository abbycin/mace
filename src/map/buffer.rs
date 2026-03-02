use parking_lot::RwLock;
use std::sync::{
    Arc,
    atomic::{
        AtomicBool, AtomicIsize, AtomicPtr, AtomicU64,
        Ordering::{AcqRel, Acquire, Relaxed},
    },
    mpsc::{Receiver, Sender},
};
use std::time::Duration;

use crate::{
    cc::context::Context,
    map::{
        SharedState,
        cache::{CANDIDATE_RING_SIZE, CANDIDATE_SAMPLE_RATE, CacheState, CandidateRing, NodeCache},
        data::Arena,
        table::Swip,
    },
    meta::Numerics,
    types::{
        page::Page,
        refbox::BoxView,
        traits::{IHeader, ILoader},
    },
    utils::{
        Backoff, Handle, MutRef, OpCode, ROOT_PID,
        countblock::Countblock,
        data::Position,
        interval::IntervalMap,
        lru::{CachePriority, ShardPriorityLru},
        observe::{CounterMetric, EventKind, GaugeMetric, ObserveEvent, Observer},
        options::ParsedOptions,
        queue::Queue,
    },
};
use dashmap::{DashMap, Entry};

use super::data::FlushData;
use super::flush::{Flush, FlushObserver};
use crate::map::table::{BucketState, PageMap};
use crate::types::refbox::BoxRef;

pub trait DataReader: Send + Sync {
    fn load_data(
        &self,
        bucket_id: u64,
        addr: u64,
        cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode>;
    fn load_blob(
        &self,
        bucket_id: u64,
        addr: u64,
        cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode>;
    fn load_blob_uncached(&self, bucket_id: u64, addr: u64) -> Result<BoxRef, OpCode>;
}

struct DummyDataReader;

impl DataReader for DummyDataReader {
    fn load_data(
        &self,
        _bucket_id: u64,
        _addr: u64,
        _cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode> {
        Err(OpCode::NotFound)
    }

    fn load_blob(
        &self,
        _bucket_id: u64,
        _addr: u64,
        _cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode> {
        Err(OpCode::NotFound)
    }

    fn load_blob_uncached(&self, _bucket_id: u64, _addr: u64) -> Result<BoxRef, OpCode> {
        Err(OpCode::NotFound)
    }
}

struct Ids {
    addr: u64,
    arena: u64,
}

impl Ids {
    const fn new(addr: u64, arena: u64) -> Self {
        Self { addr, arena }
    }
}

struct Iim {
    map: RwLock<(Vec<Ids>, usize)>,
}

impl Iim {
    fn new(addr: u64, arena: u64, init_arena: usize) -> Self {
        let mut ids = Vec::with_capacity(init_arena);
        ids.push(Ids::new(addr, arena));
        Self {
            map: RwLock::new((ids, 0)),
        }
    }

    fn find(&self, addr: u64) -> Option<u64> {
        let lk = self.map.read();
        let (ids, head) = &*lk;
        if *head >= ids.len() {
            return None;
        }
        let active = &ids[*head..];
        let pos = match active.binary_search_by(|x| x.addr.cmp(&addr)) {
            Ok(pos) => pos,
            Err(pos) => {
                if pos == 0 {
                    return None;
                }
                pos - 1
            }
        };
        Some(active[pos].arena)
    }

    fn push(&self, addr: u64, arena: u64) {
        let mut lk = self.map.write();
        lk.0.push(Ids::new(addr, arena));
    }

    fn pop(&self) {
        let mut lk = self.map.write();
        let (ids, head) = &mut *lk;
        if *head >= ids.len() {
            return;
        }
        *head += 1;
        // compact prefix occasionally to keep binary-search cache friendly
        if *head >= 64 && *head * 2 >= ids.len() {
            ids.drain(..*head);
            *head = 0;
        }
    }
}

pub(crate) struct FlowGlobal {
    burst_in_use: AtomicU64,
    loaded_buckets: AtomicU64,
    pending_flush: AtomicU64,
    gate_buckets: AtomicU64,
    burst_cap_cfg: u64,
    burst_per_bucket: u64,
    cache_used: Arc<AtomicIsize>,
    cache_cap: usize,
    observer: Arc<dyn Observer>,
}

impl FlowGlobal {
    pub(crate) fn new(opt: &ParsedOptions, cache_used: Arc<AtomicIsize>) -> Self {
        let burst_per_bucket = (opt.default_arenas / 2).max(1) as u64;
        let burst_cap_cfg = if burst_per_bucket == 0 {
            0
        } else {
            (opt.cache_capacity / (4 * opt.data_file_size)).max(1) as u64
        };
        Self {
            burst_in_use: AtomicU64::new(0),
            loaded_buckets: AtomicU64::new(0),
            pending_flush: AtomicU64::new(0),
            gate_buckets: AtomicU64::new(0),
            burst_cap_cfg,
            burst_per_bucket,
            cache_used,
            cache_cap: opt.cache_capacity,
            observer: opt.observer.clone(),
        }
    }

    pub(crate) fn on_bucket_load(&self) {
        self.loaded_buckets.fetch_add(1, AcqRel);
    }

    pub(crate) fn on_bucket_unload(&self) {
        let old = self.loaded_buckets.fetch_sub(1, AcqRel);
        debug_assert!(old > 0);
    }

    pub(crate) fn on_bucket_unload_n(&self, n: usize) {
        if n == 0 {
            return;
        }
        let old = self.loaded_buckets.fetch_sub(n as u64, AcqRel);
        debug_assert!(old >= n as u64);
    }

    fn cap(&self) -> u64 {
        let used = self.cache_used.load(Acquire).max(0) as usize;
        if self.cache_cap > 0 && used >= self.cache_cap * 90 / 100 {
            return 0;
        }
        let by_bucket = self.loaded_buckets.load(Acquire) * self.burst_per_bucket;
        self.burst_cap_cfg.min(by_bucket)
    }

    pub(crate) fn try_acquire_burst(&self) -> bool {
        if self.burst_cap_cfg == 0 || self.burst_per_bucket == 0 {
            return false;
        }
        let mut cur = self.burst_in_use.load(Acquire);
        loop {
            let cap = self.cap();
            if cur >= cap {
                return false;
            }
            match self
                .burst_in_use
                .compare_exchange_weak(cur, cur + 1, AcqRel, Acquire)
            {
                Ok(_) => {
                    self.observer
                        .gauge(GaugeMetric::FlowBurstInUseGlobal, (cur + 1) as i64);
                    return true;
                }
                Err(x) => cur = x,
            }
        }
    }

    pub(crate) fn release_burst(&self) {
        let old = self.burst_in_use.fetch_sub(1, AcqRel);
        debug_assert!(old > 0);
        self.observer
            .gauge(GaugeMetric::FlowBurstInUseGlobal, (old - 1) as i64);
    }

    pub(crate) fn burst_budget_for_bucket(&self, local_max: u32) -> u64 {
        if local_max == 0 {
            return 0;
        }
        let used = self.cache_used.load(Acquire).max(0) as usize;
        if self.cache_cap > 0 && used >= self.cache_cap * 90 / 100 {
            return 0;
        }
        local_max as u64
    }

    pub(crate) fn on_flush_enqueue(&self) {
        let cur = self.pending_flush.fetch_add(1, AcqRel) + 1;
        self.observer
            .gauge(GaugeMetric::FlowPendingFlushGlobal, cur as i64);
    }

    pub(crate) fn on_flush_done(&self) {
        let old = self.pending_flush.fetch_sub(1, AcqRel);
        debug_assert!(old > 0);
        self.observer
            .gauge(GaugeMetric::FlowPendingFlushGlobal, (old - 1) as i64);
    }

    pub(crate) fn gate_enter(&self) {
        let cur = self.gate_buckets.fetch_add(1, AcqRel) + 1;
        self.observer
            .gauge(GaugeMetric::FlowGateBucketCount, cur as i64);
    }

    pub(crate) fn gate_exit(&self) {
        let old = self.gate_buckets.fetch_sub(1, AcqRel);
        debug_assert!(old > 0);
        self.observer
            .gauge(GaugeMetric::FlowGateBucketCount, (old - 1) as i64);
    }
}

pub(crate) struct Pool {
    flush: Flush,
    map: Arc<Iim>,
    free: Arc<Queue<Handle<Arena>>>,
    wait: Arc<DashMap<u64, Handle<Arena>>>,
    cur: AtomicPtr<Arena>,
    pub(crate) ctx: Handle<Context>,
    state: MutRef<BucketState>,
    flow: Arc<FlowGlobal>,
    arena_cap: usize,
    arena_groups: u8,
    init_arena: u32,
    burst_max: u32,
    burst_in_use: Arc<AtomicU64>,
    max_log_size: usize,
    pub(crate) bucket_id: u64,
    flush_in: AtomicU64,
    flush_out: Arc<AtomicU64>,
    free_wait: Arc<Countblock>,
    flush_wait: Arc<Countblock>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AllocClass {
    Normal,
    Structural,
}

impl Pool {
    const WAIT_WARN_SECS: u64 = 5;

    pub(crate) fn new(
        opt: Arc<ParsedOptions>,
        ctx: Handle<Context>,
        state: MutRef<BucketState>,
        flow: Arc<FlowGlobal>,
        flush: Flush,
        bucket_id: u64,
        next_addr: u64,
    ) -> Self {
        let groups = ctx.groups().len() as u8;
        let init_arena = opt.default_arenas;
        let id = Self::get_id(&ctx.numerics);
        let q = Queue::new(init_arena as usize);
        for _ in 0..init_arena {
            let h = Handle::new(Arena::new(opt.data_file_size, groups));
            q.push(h).unwrap();
        }

        let h = q.pop().unwrap();
        let max_log_size = opt.max_log_size * opt.concurrent_write as usize;
        let burst_max = (init_arena / 2).max(1);

        let this = Self {
            flush,
            map: Arc::new(Iim::new(next_addr, id, init_arena as usize)),
            free: Arc::new(q),
            wait: Arc::new(DashMap::new()),
            cur: AtomicPtr::new(h.inner()),
            ctx,
            state,
            flow,
            arena_cap: opt.data_file_size,
            arena_groups: groups,
            init_arena,
            burst_max,
            burst_in_use: Arc::new(AtomicU64::new(0)),
            max_log_size,
            bucket_id,
            flush_in: AtomicU64::new(0),
            flush_out: Arc::new(AtomicU64::new(0)),
            free_wait: Arc::new(Countblock::new(0)),
            flush_wait: Arc::new(Countblock::new(0)),
        };

        h.reset(id);
        this
    }

    fn get_id(numerics: &Numerics) -> u64 {
        numerics.next_data_id.fetch_add(1, Relaxed)
    }

    pub(crate) fn current(&self) -> Handle<Arena> {
        self.cur.load(Relaxed).into()
    }

    pub(crate) fn load(&self, addr: u64) -> Option<BoxRef> {
        let arena_id = self.map.find(addr)?;
        let cur = self.current();
        if cur.id() == arena_id {
            return cur.load(addr);
        }
        if let Some(h) = self.wait.get(&arena_id)
            && matches!(h.state(), Arena::WARM | Arena::COLD)
            && h.id() == arena_id
        {
            return h.load(addr);
        }
        None
    }

    fn try_acquire_burst(&self) -> bool {
        if self.burst_max == 0 {
            return false;
        }
        let mut cur = self.burst_in_use.load(Acquire);
        loop {
            if cur >= self.burst_max as u64 {
                return false;
            }
            match self
                .burst_in_use
                .compare_exchange_weak(cur, cur + 1, AcqRel, Acquire)
            {
                Ok(_) => {
                    if self.flow.try_acquire_burst() {
                        return true;
                    }
                    let old = self.burst_in_use.fetch_sub(1, AcqRel);
                    debug_assert!(old > 0);
                    return false;
                }
                Err(x) => cur = x,
            }
        }
    }

    fn flush_slot_cap(&self) -> u64 {
        self.init_arena as u64 + self.flow.burst_budget_for_bucket(self.burst_max)
    }

    fn pending_flush(&self) -> u64 {
        self.flush_in
            .load(Acquire)
            .saturating_sub(self.flush_out.load(Acquire))
    }

    fn wait_free_signal(&self) {
        let ok = self
            .free_wait
            .wait_timeout(Duration::from_secs(Self::WAIT_WARN_SECS));
        if !ok {
            log::warn!(
                "bucket {} alloc wait timeout, pending_flush {}, burst_in_use {}",
                self.bucket_id,
                self.pending_flush(),
                self.burst_in_use.load(Acquire),
            );
        }
    }

    fn wait_flush_signal(&self) {
        let ok = self
            .flush_wait
            .wait_timeout(Duration::from_secs(Self::WAIT_WARN_SECS));
        if !ok {
            log::warn!(
                "bucket {} flush wait timeout, pending_flush {}",
                self.bucket_id,
                self.pending_flush(),
            );
        }
    }

    fn wait_alloc_drain(&self, arena: Handle<Arena>) {
        let mut sched = Backoff::new();
        while arena.alloc_inflight() != 0 {
            sched.shape();
        }
    }

    fn maybe_shape(&self, pending: u64, sched: &mut Backoff) {
        let shape_floor = (self.init_arena / 2) as u64;
        let gate_ceil = self.flush_slot_cap().max(1);
        if pending >= shape_floor && pending < gate_ceil {
            sched.shape();
        } else {
            sched.reset();
        }
    }

    fn enter_gate(&self, in_gate: &mut bool, pending: u64) {
        if *in_gate {
            return;
        }
        *in_gate = true;
        self.ctx
            .opt
            .observer
            .counter(CounterMetric::FlowGateEnterCount, 1);
        self.ctx.opt.observer.event(ObserveEvent {
            kind: EventKind::FlowGateEnter,
            bucket_id: self.bucket_id,
            txid: 0,
            file_id: 0,
            value: pending,
        });
        self.flow.gate_enter();
    }

    fn exit_gate(&self, in_gate: &mut bool, pending: u64) {
        if !*in_gate {
            return;
        }
        *in_gate = false;
        self.ctx
            .opt
            .observer
            .counter(CounterMetric::FlowGateExitCount, 1);
        self.ctx.opt.observer.event(ObserveEvent {
            kind: EventKind::FlowGateExit,
            bucket_id: self.bucket_id,
            txid: 0,
            file_id: 0,
            value: pending,
        });
        self.flow.gate_exit();
    }

    fn wait_install_handoff(&self, observed: Handle<Arena>) {
        let mut sched = Backoff::new();
        loop {
            let cur = self.current();
            if cur.inner() != observed.inner() {
                break;
            }
            if cur.state() == Arena::HOT {
                break;
            }
            sched.shape();
        }
    }

    fn handle_log_limit(&self, observed: Handle<Arena>, cur_log: usize, class: AllocClass) {
        if self.install_new(class) {
            let _ = self
                .ctx
                .numerics
                .log_size
                .fetch_update(Relaxed, Relaxed, |x| Some(x.saturating_sub(cur_log)));
        } else {
            self.wait_install_handoff(observed);
        }
    }

    fn handle_no_space(&self, observed: Handle<Arena>, class: AllocClass) {
        if !self.install_new(class) {
            self.wait_install_handoff(observed);
        }
    }

    fn try_take_next_arena(
        &self,
        in_gate: &mut bool,
        pending: u64,
        class: AllocClass,
    ) -> Option<Handle<Arena>> {
        if let Some(p) = self.free.pop() {
            self.exit_gate(in_gate, pending);
            return Some(p);
        }
        if self.try_acquire_burst() {
            self.ctx
                .opt
                .observer
                .counter(CounterMetric::FlowAllocBurstGrant, 1);
            self.exit_gate(in_gate, pending);
            let p = Handle::new(Arena::new(self.arena_cap, self.arena_groups));
            p.set_origin(Arena::ORIGIN_BURST);
            return Some(p);
        }
        self.ctx
            .opt
            .observer
            .counter(CounterMetric::FlowAllocBurstDeny, 1);
        if class == AllocClass::Structural {
            self.ctx
                .opt
                .observer
                .counter(CounterMetric::FlowAllocStructGrant, 1);
            self.exit_gate(in_gate, pending);
            let p = Handle::new(Arena::new(self.arena_cap, self.arena_groups));
            p.set_origin(Arena::ORIGIN_STRUCT);
            return Some(p);
        }
        if class == AllocClass::Structural {
            self.ctx
                .opt
                .observer
                .counter(CounterMetric::FlowAllocStructDeny, 1);
        }
        None
    }

    fn acquire_next_arena(&self, class: AllocClass) -> Handle<Arena> {
        let mut in_gate = false;
        let mut sched = Backoff::new();
        loop {
            let pending = self.pending_flush();
            if let Some(p) = self.try_take_next_arena(&mut in_gate, pending, class) {
                sched.reset();
                return p;
            }
            self.enter_gate(&mut in_gate, pending);
            self.maybe_shape(pending, &mut sched);
            self.wait_free_signal();
        }
    }

    // NOTE: currently, Structural alloc has no restriction
    fn install_new(&self, class: AllocClass) -> bool {
        let cur = self.current();
        if !cur.mark_warm() {
            return false;
        }
        self.wait_alloc_drain(cur);
        let p = self.acquire_next_arena(class);

        let id = Self::get_id(&self.ctx.numerics);
        self.wait.insert(cur.id(), cur);
        self.flush(cur);

        let next_addr = self.state.next_addr.load(Relaxed);
        self.map.push(next_addr, id);

        let was_burst = p.is_burst();
        let was_struct = p.is_struct();
        p.reset(id);
        if was_burst {
            p.set_origin(Arena::ORIGIN_BURST);
        } else if was_struct {
            p.set_origin(Arena::ORIGIN_STRUCT);
        }

        self.cur
            .compare_exchange(cur.inner(), p.inner(), Relaxed, Relaxed)
            .expect("never happen");
        true
    }

    fn flush(&self, h: Handle<Arena>) {
        let wait = self.wait.clone();
        let free = self.free.clone();
        let map = self.map.clone();
        let out = self.flush_out.clone();
        let free_wait = self.free_wait.clone();
        let flush_wait = self.flush_wait.clone();
        let burst_in_use = self.burst_in_use.clone();
        let flow_release = self.flow.clone();
        let flow_done = self.flow.clone();

        let release = move || {
            map.pop();
            wait.remove(&h.id());

            if h.is_struct() {
                h.reclaim();
            } else if h.is_burst() {
                let old = burst_in_use.fetch_sub(1, AcqRel);
                debug_assert!(old > 0);
                flow_release.release_burst();
                h.reclaim();
            } else {
                h.clear();
                h.set_state(Arena::COLD, Arena::FLUSH);
                free.push(h).expect("can't fail");
            }
            free_wait.post();
        };
        let done = move || {
            out.fetch_add(1, Relaxed);
            flow_done.on_flush_done();
            flush_wait.post();
        };

        let data = FlushData::new(h, self.bucket_id, Box::new(release), Box::new(done));
        self.flush_in.fetch_add(1, AcqRel);
        self.flow.on_flush_enqueue();
        self.flush
            .tx
            .send(data)
            .expect("flusher channel disconnected before flush publish");
    }

    pub(crate) fn flush_all(&self) {
        let ptr = self.cur.swap(std::ptr::null_mut(), Relaxed);
        if !ptr.is_null() {
            let h = Handle::from(ptr);
            self.wait.insert(h.id(), h);
            if h.state() == Arena::HOT {
                let old = h.set_state(Arena::HOT, Arena::WARM);
                debug_assert_eq!(old, Arena::HOT);
            }
            self.wait_alloc_drain(h);
            self.flush(h);
        }
    }

    pub(crate) fn wait_flush(&self) {
        while self.flush_in.load(Acquire) != self.flush_out.load(Acquire) {
            self.wait_flush_signal();
        }
    }

    fn alloc_inner(&self, size: u32, class: AllocClass) -> Result<(Handle<Arena>, BoxRef), OpCode> {
        loop {
            let a = self.current();
            let cur = self.ctx.numerics.log_size.load(Relaxed);
            if cur >= self.max_log_size {
                self.handle_log_limit(a, cur, class);
                continue;
            }
            if !a.try_enter_hot_alloc() {
                self.wait_install_handoff(a);
                continue;
            }
            let ret = a.alloc(&self.state.next_addr, size);
            a.leave_hot_alloc();

            match ret {
                Ok(x) => return Ok((a, x)),
                Err(OpCode::Again) => {
                    self.wait_install_handoff(a);
                    continue;
                }
                Err(OpCode::NoSpace) => self.handle_no_space(a, class),
                _ => unreachable!(),
            }
        }
    }

    pub(crate) fn alloc(&self, size: u32) -> Result<(Handle<Arena>, BoxRef), OpCode> {
        self.alloc_inner(size, AllocClass::Normal)
    }

    pub(crate) fn alloc_struct(&self, size: u32) -> Result<(Handle<Arena>, BoxRef), OpCode> {
        self.alloc_inner(size, AllocClass::Structural)
    }

    pub(crate) fn alloc_pair(
        &self,
        size1: u32,
        size2: u32,
    ) -> Result<((Handle<Arena>, BoxRef), (Handle<Arena>, BoxRef)), OpCode> {
        let total_real_size = BoxRef::real_size(size1) + BoxRef::real_size(size2);
        loop {
            let a = self.current();
            let cur = self.ctx.numerics.log_size.load(Relaxed);
            if cur >= self.max_log_size {
                self.handle_log_limit(a, cur, AllocClass::Normal);
                continue;
            }
            if !a.try_enter_hot_alloc() {
                self.wait_install_handoff(a);
                continue;
            }
            let ret = (|| {
                a.reserve_batch(total_real_size, 2)?;
                let start = self.state.reserve_addr_span(2);
                let b1 = a.alloc_at_addr(start, BoxRef::real_size(size1));
                let b2 = a.alloc_at_addr(start + 1, BoxRef::real_size(size2));
                Ok(((a, b1), (a, b2)))
            })();
            a.leave_hot_alloc();
            match ret {
                Ok(v) => return Ok(v),
                Err(OpCode::Again) => {
                    self.wait_install_handoff(a);
                    continue;
                }
                Err(OpCode::NoSpace) => {
                    self.handle_no_space(a, AllocClass::Normal);
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub(crate) fn recycle<F>(&self, addr: &[u64], mut gc: F)
    where
        F: FnMut(u64),
    {
        let a = self.current();
        a.inc_ref();
        for &i in addr {
            if !a.recycle(i) {
                gc(i);
            }
        }
        a.dec_ref();
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        // wait for pending flushes
        while self.flush_in.load(Acquire) != self.flush_out.load(Acquire) {
            self.wait_flush_signal();
        }
        // take all waiting arenas
        for entry in self.wait.iter() {
            let h = *entry.value();
            h.reclaim();
        }
        // free arenas in queue
        while let Some(h) = self.free.pop() {
            h.reclaim();
        }
        // reclaim cur
        let ptr = self.cur.swap(std::ptr::null_mut(), Relaxed);
        if !ptr.is_null() {
            Handle::from(ptr).reclaim();
        }
    }
}

pub(crate) struct BucketContext {
    pub(crate) pool: Handle<Pool>,
    pub(crate) table: MutRef<PageMap>,
    pub(crate) state: MutRef<BucketState>,
    pub(crate) data_intervals: RwLock<IntervalMap>,
    pub(crate) blob_intervals: RwLock<IntervalMap>,
    pub(crate) lru: Handle<ShardPriorityLru<BoxRef>>,
    pub(crate) bucket_id: u64,
    pub(crate) reader: Arc<dyn DataReader>,
    cache: NodeCache,
    candidates: CandidateRing,
    tx: Sender<SharedState>,
    candidate_tick: AtomicU64,
    reclaimed: AtomicBool,
}

impl BucketContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        ctx: Handle<Context>,
        state: MutRef<BucketState>,
        flow: Arc<FlowGlobal>,
        bucket_id: u64,
        next_addr: u64,
        table: MutRef<PageMap>,
        flush: Flush,
        lru: Handle<ShardPriorityLru<BoxRef>>,
        reader: Arc<dyn DataReader>,
        used: Arc<AtomicIsize>,
        tx: Sender<SharedState>,
    ) -> Self {
        let pool = Handle::new(Pool::new(
            ctx.opt.clone(),
            ctx,
            state.clone(),
            flow,
            flush,
            bucket_id,
            next_addr,
        ));

        Self {
            pool,
            table,
            state,
            data_intervals: RwLock::new(IntervalMap::new()),
            blob_intervals: RwLock::new(IntervalMap::new()),
            lru,
            bucket_id,
            reader,
            cache: NodeCache::new(used),
            candidates: CandidateRing::new(CANDIDATE_RING_SIZE),
            tx,
            candidate_tick: AtomicU64::new(0),
            reclaimed: AtomicBool::new(false),
        }
    }

    pub(crate) fn alloc(&self, size: u32) -> Result<(Handle<Arena>, BoxRef), OpCode> {
        self.pool.alloc(size)
    }

    pub(crate) fn alloc_struct(&self, size: u32) -> Result<(Handle<Arena>, BoxRef), OpCode> {
        self.pool.alloc_struct(size)
    }

    pub(crate) fn alloc_pair(
        &self,
        size1: u32,
        size2: u32,
    ) -> Result<((Handle<Arena>, BoxRef), (Handle<Arena>, BoxRef)), OpCode> {
        self.pool.alloc_pair(size1, size2)
    }

    pub(crate) fn record_lsn(&self, group_id: usize, pos: Position) {
        self.pool.current().record_lsn(group_id, pos);
    }

    pub(crate) fn recycle<F>(&self, addr: &[u64], gc: F)
    where
        F: FnMut(u64),
    {
        self.pool.recycle(addr, gc);
    }

    pub(crate) fn loader(&self, ctx: Handle<Context>) -> Loader {
        Loader {
            pool: self.pool,
            ctx,
            lru: self.lru,
            pinned: MutRef::new(DashMap::with_capacity(Loader::PIN_CAP)),
            bucket_id: self.bucket_id,
            reader: self.reader.clone(),
        }
    }

    pub(crate) fn cache(&self, p: Page<Loader>) {
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

    pub(crate) fn load(&self, pid: u64) -> Result<Option<Page<Loader>>, OpCode> {
        loop {
            let swip = Swip::new(self.table.get(pid));
            if swip.is_null() {
                return Ok(None);
            }
            if !swip.is_tagged() {
                // we delay cache warm up when the value of page map entry was updated
                return Ok(Some(Page::<Loader>::from_swip(swip.raw())));
            }
            let new = Page::load(self.loader(self.pool.ctx), swip.untagged())?;
            if self.table.cas(pid, swip.raw(), new.swip()).is_ok() {
                self.cache(new);
                return Ok(Some(new));
            } else {
                new.reclaim();
            }
        }
    }

    pub(crate) fn warm(&self, pid: u64, size: usize) {
        self.cache.touch(pid, size as isize);
        // avoid sampling when cache is far from pressure to reduce atomic contention
        let used = self.cache.used();
        if used >= (self.pool.ctx.opt.cache_capacity as isize >> 2) {
            self.maybe_push_candidate(pid);
        }
        self.maybe_evict();
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

    pub(crate) fn update_cache_size(&self, pid: u64, size: usize) {
        let _ = self.cache.update_size(pid, size as isize);
    }

    pub(crate) fn candidate_snapshot(&self) -> Vec<u64> {
        self.candidates.snapshot()
    }

    fn maybe_push_candidate(&self, pid: u64) {
        let tick = self.candidate_tick.fetch_add(1, Relaxed) + 1;
        if tick.is_multiple_of(CANDIDATE_SAMPLE_RATE) {
            self.candidates.push(pid);
        }
    }

    fn maybe_evict(&self) {
        let threshold = self.pool.ctx.opt.cache_capacity as isize * 80 / 100;
        if self.cache.used() >= threshold {
            let _ = self.tx.send(SharedState::Evict);
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
            let page = Page::<Loader>::from_swip(swip.untagged());
            page.reclaim();
        }
    }

    pub(crate) fn flush_and_wait(&self) {
        self.pool.flush_all();
        self.pool.wait_flush();
    }

    pub(crate) fn reclaim(&self) {
        if self
            .reclaimed
            .compare_exchange(false, true, AcqRel, Acquire)
            .is_err()
        {
            return;
        }
        // this is necessary, because the bucket maybe dropped/deleted without exit the process
        self.flush_and_wait();
        self.reclaim_pages();
        Handle::reclaim(&self.pool);
    }
}

impl Drop for BucketContext {
    fn drop(&mut self) {
        self.reclaim();
    }
}

pub(crate) struct BucketMgr {
    pub(crate) buckets: DashMap<u64, Arc<BucketContext>>,
    pub(crate) lru: Handle<ShardPriorityLru<BoxRef>>,
    pub(crate) used: Arc<AtomicIsize>,
    pub(crate) flow: Arc<FlowGlobal>,
    pub(crate) flush: Option<Flush>,
    pub(crate) tx: Sender<SharedState>,
    pub(crate) rx: Receiver<()>,
    pub(crate) ctx: Handle<Context>,
    pub(crate) reader: Arc<dyn DataReader>,
}

impl BucketMgr {
    pub(crate) fn new(
        opt: Arc<ParsedOptions>,
        ctx: Handle<Context>,
        tx: Sender<SharedState>,
        rx: Receiver<()>,
    ) -> Self {
        let reader = Arc::new(DummyDataReader);
        let used = Arc::new(AtomicIsize::new(0));
        let flow = Arc::new(FlowGlobal::new(opt.as_ref(), used.clone()));
        Self {
            buckets: DashMap::new(),
            lru: Handle::new(ShardPriorityLru::new(
                opt.cache_count,
                opt.high_priority_ratio,
            )),
            used,
            flow,
            flush: None,
            tx,
            rx,
            ctx,
            reader,
        }
    }

    pub(crate) fn set_context(&mut self, ctx: Handle<Context>, reader: Arc<dyn DataReader>) {
        self.ctx = ctx;
        self.reader = reader;
    }

    pub(crate) fn start(
        &mut self,
        ctx: Handle<Context>,
        reader: Arc<dyn DataReader>,
        observer: Arc<dyn FlushObserver>,
    ) {
        self.set_context(ctx, reader);
        self.flush = Some(Flush::new(ctx, observer));
    }

    pub(crate) fn quit(&self) {
        // 1) stop evictor to avoid new eviction/compaction work while draining flushes
        let _ = self.tx.send(SharedState::Quit);
        let _ = self.rx.recv();

        // 2) flush all buckets while flusher thread is still alive
        for ctx in self.buckets.iter() {
            ctx.flush_and_wait();
        }

        // 3) stop flusher after outstanding flush tasks are drained
        if let Some(f) = self.flush.as_ref() {
            f.quit();
        }

        // 4) release bucket contexts after flushers are gone to avoid races
        // reclaim is deferred to BucketContext::drop when the last Arc goes away
        self.flow.on_bucket_unload_n(self.buckets.len());
        self.buckets.clear();

        // 5) release shared lru
        self.lru.reclaim();
    }

    pub(crate) fn del_bucket(&self, bucket_id: u64) {
        if let Some(ctx) = self.buckets.get(&bucket_id).map(|x| x.value().clone()) {
            ctx.flush_and_wait();
        }
        if self.buckets.remove(&bucket_id).is_some() {
            self.flow.on_bucket_unload();
        }
    }

    pub(crate) fn active_contexts(&self) -> Vec<Arc<BucketContext>> {
        self.buckets
            .iter()
            .filter_map(|x| {
                let ctx = x.value().clone();
                if ctx.state.is_deleting() || ctx.state.is_drop() {
                    None
                } else {
                    Some(ctx)
                }
            })
            .collect()
    }
}

pub struct Loader {
    pub(crate) pool: Handle<Pool>,
    ctx: Handle<Context>,
    lru: Handle<ShardPriorityLru<BoxRef>>,
    pinned: MutRef<DashMap<u64, BoxRef>>,
    bucket_id: u64,
    reader: Arc<dyn DataReader>,
}

impl Drop for Loader {
    fn drop(&mut self) {}
}

impl Loader {
    const PIN_CAP: usize = 64;

    pub fn find(&self, addr: u64) -> Result<BoxRef, OpCode> {
        if let Some(x) = self.lru.get(addr) {
            return Ok(x.clone());
        }
        if let Some(x) = self.pool.load(addr) {
            self.lru.add(CachePriority::High, addr, x.clone());
            return Ok(x);
        }
        self.reader.load_data(self.bucket_id, addr, &|b| {
            self.lru.add(CachePriority::High, addr, b);
        })
    }
}

impl ILoader for Loader {
    fn deep_copy(&self) -> Self {
        Self {
            pool: self.pool,
            ctx: self.ctx,
            lru: self.lru,
            pinned: MutRef::new(DashMap::new()),
            bucket_id: self.bucket_id,
            reader: self.reader.clone(),
        }
    }

    fn shallow_copy(&self) -> Self {
        Self {
            pool: self.pool,
            ctx: self.ctx,
            lru: self.lru,
            pinned: self.pinned.clone(),
            bucket_id: self.bucket_id,
            reader: self.reader.clone(),
        }
    }

    fn pin(&self, data: BoxRef) {
        self.pinned.insert(data.header().addr, data);
    }

    fn load(&self, addr: u64) -> Result<BoxView, OpCode> {
        if let Some(p) = self.pinned.get(&addr) {
            return Ok(p.view());
        }
        let x = self.find(addr)?;
        let e = self.pinned.entry(addr);
        match e {
            Entry::Occupied(o) => Ok(o.get().view()),
            Entry::Vacant(v) => {
                let r = x.view();
                v.insert(x);
                Ok(r)
            }
        }
    }

    fn load_remote(&self, addr: u64) -> Result<BoxRef, OpCode> {
        if let Some(x) = self.lru.get(addr) {
            return Ok(x.clone());
        }
        if let Some(x) = self.pool.load(addr) {
            self.lru.add(CachePriority::Low, addr, x.clone());
            return Ok(x.clone());
        }
        self.reader.load_blob(self.bucket_id, addr, &|b| {
            self.lru.add(CachePriority::Low, addr, b);
        })
    }

    fn load_remote_uncached(&self, addr: u64) -> BoxRef {
        if let Some(x) = self.pool.load(addr) {
            return x;
        }
        self.reader
            .load_blob_uncached(self.bucket_id, addr)
            .expect("must exist")
    }
}

#[cfg(test)]
mod tests {
    use super::FlowGlobal;
    use crate::utils::observe::NoopObserver;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicIsize, AtomicU64, Ordering::Acquire};

    #[test]
    fn flow_global_should_be_bounded_by_loaded_buckets() {
        let flow = FlowGlobal {
            burst_in_use: AtomicU64::new(0),
            loaded_buckets: AtomicU64::new(0),
            pending_flush: AtomicU64::new(0),
            gate_buckets: AtomicU64::new(0),
            burst_cap_cfg: 10,
            burst_per_bucket: 2,
            cache_used: Arc::new(AtomicIsize::new(0)),
            cache_cap: 100,
            observer: Arc::new(NoopObserver),
        };

        assert!(!flow.try_acquire_burst());

        flow.on_bucket_load();
        assert!(flow.try_acquire_burst());
        assert!(flow.try_acquire_burst());
        assert!(!flow.try_acquire_burst());

        flow.on_bucket_load();
        assert!(flow.try_acquire_burst());
        assert!(flow.try_acquire_burst());
        assert!(!flow.try_acquire_burst());
    }

    #[test]
    fn flow_global_release_should_return_capacity() {
        let flow = FlowGlobal {
            burst_in_use: AtomicU64::new(0),
            loaded_buckets: AtomicU64::new(2),
            pending_flush: AtomicU64::new(0),
            gate_buckets: AtomicU64::new(0),
            burst_cap_cfg: 3,
            burst_per_bucket: 8,
            cache_used: Arc::new(AtomicIsize::new(0)),
            cache_cap: 100,
            observer: Arc::new(NoopObserver),
        };

        assert!(flow.try_acquire_burst());
        assert!(flow.try_acquire_burst());
        assert!(flow.try_acquire_burst());
        assert!(!flow.try_acquire_burst());

        flow.release_burst();
        assert!(flow.try_acquire_burst());
    }

    #[test]
    fn flow_global_should_disable_burst_under_high_cache_pressure() {
        let cache_used = Arc::new(AtomicIsize::new(95));
        let flow = FlowGlobal {
            burst_in_use: AtomicU64::new(0),
            loaded_buckets: AtomicU64::new(4),
            pending_flush: AtomicU64::new(0),
            gate_buckets: AtomicU64::new(0),
            burst_cap_cfg: 32,
            burst_per_bucket: 8,
            cache_used: cache_used.clone(),
            cache_cap: 100,
            observer: Arc::new(NoopObserver),
        };

        assert!(!flow.try_acquire_burst());

        cache_used.store(70, std::sync::atomic::Ordering::Relaxed);
        assert!(flow.try_acquire_burst());
    }

    #[test]
    fn flow_global_pending_flush_should_track_enqueue_and_done() {
        let flow = FlowGlobal {
            burst_in_use: AtomicU64::new(0),
            loaded_buckets: AtomicU64::new(2),
            pending_flush: AtomicU64::new(0),
            gate_buckets: AtomicU64::new(0),
            burst_cap_cfg: 32,
            burst_per_bucket: 8,
            cache_used: Arc::new(AtomicIsize::new(0)),
            cache_cap: 100,
            observer: Arc::new(NoopObserver),
        };

        for _ in 0..40 {
            flow.on_flush_enqueue();
        }
        assert_eq!(flow.pending_flush.load(Acquire), 40);

        flow.on_flush_done();
        assert_eq!(flow.pending_flush.load(Acquire), 39);
    }
}
