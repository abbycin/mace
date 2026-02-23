use parking_lot::RwLock;
use std::sync::{
    Arc,
    atomic::{
        AtomicBool, AtomicIsize, AtomicPtr, AtomicU64, AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
    mpsc::{Receiver, Sender},
};

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
        Handle, MutRef, OpCode, ROOT_PID,
        data::Position,
        interval::IntervalMap,
        lru::{CachePriority, ShardPriorityLru},
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
    map: RwLock<Vec<Ids>>,
}

impl Iim {
    fn new(addr: u64, arena: u64) -> Self {
        Self {
            map: RwLock::new(vec![Ids::new(addr, arena)]),
        }
    }

    fn find(&self, addr: u64) -> Option<u64> {
        let lk = self.map.read();
        let pos = match lk.binary_search_by(|x| x.addr.cmp(&addr)) {
            Ok(pos) => pos,
            Err(pos) => {
                if pos == 0 {
                    return None;
                }
                pos - 1
            }
        };
        Some(lk[pos].arena)
    }

    fn push(&self, addr: u64, arena: u64) {
        let mut lk = self.map.write();
        lk.push(Ids::new(addr, arena));
    }

    fn pop(&self) {
        let mut lk = self.map.write();
        lk.remove(0);
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
    allow_over_provision: bool,
    max_log_size: usize,
    pub(crate) bucket_id: u64,
    flush_in: AtomicU64,
    flush_out: Arc<AtomicU64>,
    alloc_inflight: AtomicUsize,
    packed_multi: AtomicBool,
}

pub(crate) struct PackedAlloc {
    pool: Handle<Pool>,
    dep_group_id: u64,
    total_real_size: u32,
    remain_real_size: u32,
    nr_frames: u32,
    remain_frames: u32,
    detached: Vec<PackedSegment>,
    current: PackedSegment,
    done: bool,
}

struct PackedSegment {
    arena: Handle<Arena>,
    reserved_real_size: u32,
    reserved_frames: u32,
    allocated_addrs: Vec<u64>,
}

struct AllocGate<'a> {
    pool: &'a Pool,
}

impl Drop for AllocGate<'_> {
    fn drop(&mut self) {
        self.pool.alloc_inflight.fetch_sub(1, AcqRel);
    }
}

impl PackedSegment {
    fn new(arena: Handle<Arena>) -> Self {
        Self {
            arena,
            reserved_real_size: 0,
            reserved_frames: 0,
            allocated_addrs: Vec::new(),
        }
    }
}

impl PackedAlloc {
    fn should_rotate_before_reserve(seg: &PackedSegment, real_size: u32) -> bool {
        let cur = seg.arena.real_size.load(Relaxed);
        let cap = seg.arena.cap() as u64;
        let next = cur.saturating_add(real_size as u64);
        if next <= cap {
            return false;
        }
        // allow one oversize frame on an empty arena, otherwise rotate to next arena
        seg.reserved_frames != 0 || cur != 0
    }

    fn rotate_current_non_group(&mut self) {
        let old = self.current.arena.set_state(Arena::HOT, Arena::WARM);
        if old != Arena::HOT {
            log::error!(
                "packed rotate non-group failed, arena {}, state {}",
                self.current.arena.id(),
                old
            );
            panic!("packed rotate non-group failed");
        }
        let next = self.pool.install_new(self.current.arena);
        self.current = PackedSegment::new(next);
    }

    fn rotate_current_for_group(&mut self) {
        let old = self.current.arena.set_state(Arena::HOT, Arena::WARM);
        if old != Arena::HOT {
            log::error!(
                "packed rotate group failed, arena {}, state {}",
                self.current.arena.id(),
                old
            );
            panic!("packed rotate group failed");
        }

        let mut seg = PackedSegment::new(self.current.arena);
        std::mem::swap(&mut seg, &mut self.current);
        let next = self.pool.install_new_deferred(seg.arena);
        self.detached.push(seg);
        self.current = PackedSegment::new(next);
    }

    pub(crate) fn alloc(&mut self, size: u32) -> (Handle<Arena>, BoxRef) {
        let real_size = BoxRef::real_size(size);
        if self.remain_frames == 0 || self.remain_real_size < real_size {
            log::error!(
                "packed multi overflow, group {}, frames {}, remain_frames {}, remain_real {}, need {}",
                self.dep_group_id,
                self.nr_frames,
                self.remain_frames,
                self.remain_real_size,
                real_size
            );
            panic!("packed multi overflow");
        }

        loop {
            if Self::should_rotate_before_reserve(&self.current, real_size) {
                if self.current.reserved_frames == 0 {
                    self.rotate_current_non_group();
                } else {
                    self.rotate_current_for_group();
                }
                continue;
            }

            let arena = self.current.arena;
            match arena.reserve_batch(real_size, 1) {
                Ok(()) => {
                    let addr = self.pool.state.reserve_addr_span(1);
                    let b = arena.alloc_at_addr(addr, real_size);
                    self.current.reserved_real_size += real_size;
                    self.current.reserved_frames += 1;
                    self.current.allocated_addrs.push(addr);
                    self.remain_real_size -= real_size;
                    self.remain_frames -= 1;
                    return (arena, b);
                }
                Err(OpCode::Again) => continue,
                Err(OpCode::NoSpace) => {
                    if self.current.reserved_frames == 0 {
                        self.rotate_current_non_group();
                    } else {
                        self.rotate_current_for_group();
                    }
                }
                Err(e) => {
                    log::error!(
                        "packed multi reserve failed, group {}, arena {}, err {:?}",
                        self.dep_group_id,
                        arena.id(),
                        e
                    );
                    panic!("packed multi reserve failed");
                }
            }
        }
    }

    pub(crate) fn finish(mut self) {
        if self.remain_frames != 0 || self.remain_real_size != 0 {
            log::error!(
                "packed multi mismatch, group {}, frames {}, remain_frames {}, total_real {}, remain_real {}",
                self.dep_group_id,
                self.nr_frames,
                self.remain_frames,
                self.total_real_size,
                self.remain_real_size
            );
            panic!("packed multi mismatch");
        }

        if self.detached.is_empty() {
            self.done = true;
            self.pool.leave_packed_multi();
            return;
        }

        if self.current.reserved_frames != 0 {
            self.rotate_current_for_group();
        }

        let expected = self.detached.len() as u32;
        if expected == 0 {
            log::error!(
                "packed multi has no detached arenas, group {}",
                self.dep_group_id
            );
            panic!("packed multi has no detached arenas");
        }

        for seg in &self.detached {
            self.pool
                .flush_with_dep_group(seg.arena, self.dep_group_id, expected);
        }

        self.done = true;
        self.pool.leave_packed_multi();
    }
}

impl Drop for PackedAlloc {
    fn drop(&mut self) {
        if !self.done {
            // no cancel path: packed allocation is only created by BaseView::new_leaf with strict begin/end pairing
            // node update paths are serialized by page/node locks, so rollback never depends on partial packed cleanup
            // dropping without finish means packed closure invariant is broken and must fail fast
            self.pool.leave_packed_multi();
            let used_frames = self.nr_frames.saturating_sub(self.remain_frames);
            let used_real_size = self.total_real_size.saturating_sub(self.remain_real_size);
            log::error!(
                "packed allocation dropped without finish, group {}, used_frames {}, used_size {}, detached {}",
                self.dep_group_id,
                used_frames,
                used_real_size,
                self.detached.len()
            );
            panic!("packed allocation dropped without finish");
        }
    }
}

impl Pool {
    const INIT_ARENA: u32 = 16;
    const MULTI_DEP_GROUP_TAG: u64 = 1 << 63;

    pub(crate) fn new(
        opt: Arc<ParsedOptions>,
        ctx: Handle<Context>,
        state: MutRef<BucketState>,
        flush: Flush,
        bucket_id: u64,
        next_addr: u64,
    ) -> Self {
        let groups = ctx.groups().len() as u8;
        let id = Self::get_id(&ctx.numerics);
        let q = Queue::new(Self::INIT_ARENA as usize);
        for _ in 0..Self::INIT_ARENA {
            let h = Handle::new(Arena::new(opt.data_file_size, groups));
            q.push(h).unwrap();
        }

        let h = q.pop().unwrap();
        let max_log_size = opt.max_log_size * opt.concurrent_write as usize;

        let this = Self {
            flush,
            map: Arc::new(Iim::new(next_addr, id)),
            free: Arc::new(q),
            wait: Arc::new(DashMap::new()),
            cur: AtomicPtr::new(h.inner()),
            ctx,
            state,
            allow_over_provision: opt.over_provision,
            max_log_size,
            bucket_id,
            flush_in: AtomicU64::new(0),
            flush_out: Arc::new(AtomicU64::new(0)),
            alloc_inflight: AtomicUsize::new(0),
            packed_multi: AtomicBool::new(false),
        };

        h.reset(id);
        this
    }

    fn get_id(numerics: &Numerics) -> u64 {
        numerics.next_data_id.fetch_add(1, Relaxed)
    }

    fn enter_alloc_gate(&self) -> AllocGate<'_> {
        loop {
            while self.packed_multi.load(Acquire) {
                std::thread::yield_now();
            }
            self.alloc_inflight.fetch_add(1, AcqRel);
            if !self.packed_multi.load(Acquire) {
                return AllocGate { pool: self };
            }
            self.alloc_inflight.fetch_sub(1, AcqRel);
        }
    }

    fn enter_packed_multi(&self) {
        while self
            .packed_multi
            .compare_exchange(false, true, AcqRel, Acquire)
            .is_err()
        {
            std::thread::yield_now();
        }
        while self.alloc_inflight.load(Acquire) != 0 {
            std::thread::yield_now();
        }
    }

    fn leave_packed_multi(&self) {
        self.packed_multi.store(false, Release);
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

    fn install_new_inner(&self, cur: Handle<Arena>, defer_flush: bool) -> Handle<Arena> {
        let id = Self::get_id(&self.ctx.numerics);
        self.wait.insert(cur.id(), cur);
        if !defer_flush {
            self.flush(cur);
        }

        let next_addr = self.state.next_addr.load(Relaxed);
        self.map.push(next_addr, id);

        let p = loop {
            if let Some(p) = self.free.pop() {
                break p;
            }
            if self.allow_over_provision {
                break Handle::new(Arena::new(cur.cap(), cur.groups()));
            }
            std::hint::spin_loop();
        };
        p.reset(id);

        self.cur
            .compare_exchange(cur.inner(), p.inner(), Relaxed, Relaxed)
            .expect("never happen");
        p
    }

    fn install_new(&self, cur: Handle<Arena>) -> Handle<Arena> {
        self.install_new_inner(cur, false)
    }

    fn install_new_deferred(&self, cur: Handle<Arena>) -> Handle<Arena> {
        self.install_new_inner(cur, true)
    }

    fn flush(&self, h: Handle<Arena>) {
        self.flush_with_dep_group(h, h.id(), 1);
    }

    fn flush_with_dep_group(&self, h: Handle<Arena>, dep_group_id: u64, dep_group_items: u32) {
        let wait = self.wait.clone();
        let free = self.free.clone();
        let map = self.map.clone();
        let out = self.flush_out.clone();

        let release = move || {
            h.set_state(Arena::COLD, Arena::FLUSH);
            h.clear();
            map.pop();
            wait.remove(&h.id());
            if free.push(h).is_err() {
                h.reclaim();
            }
        };
        let done = move || {
            out.fetch_add(1, Relaxed);
        };

        let data = if dep_group_items == 1 && dep_group_id == h.id() {
            FlushData::new(h, self.bucket_id, Box::new(release), Box::new(done))
        } else {
            FlushData::with_dep_group(
                h,
                self.bucket_id,
                dep_group_id,
                dep_group_items,
                Box::new(release),
                Box::new(done),
            )
        };
        let _ = self.flush.tx.send(data);
        self.flush_in.fetch_add(1, Relaxed);
    }

    pub(crate) fn flush_all(&self) {
        let _gate = self.enter_alloc_gate();
        let ptr = self.cur.swap(std::ptr::null_mut(), Relaxed);
        if !ptr.is_null() {
            let h = Handle::from(ptr);
            self.wait.insert(h.id(), h);
            h.set_state(Arena::HOT, Arena::WARM);
            self.flush(h);
        }
    }

    pub(crate) fn wait_flush(&self) {
        while self.flush_in.load(Acquire) != self.flush_out.load(Acquire) {
            std::thread::yield_now();
        }
    }

    pub(crate) fn alloc(&self, size: u32) -> Result<(Handle<Arena>, BoxRef), OpCode> {
        let _gate = self.enter_alloc_gate();
        loop {
            let a = self.current();
            let cur = self.ctx.numerics.log_size.load(Relaxed);
            if cur >= self.max_log_size && a.set_state(Arena::HOT, Arena::WARM) == Arena::HOT {
                let old = self.ctx.numerics.log_size.fetch_sub(cur, Relaxed);
                assert!(old >= cur);
                self.install_new(a);
                continue;
            }

            match a.alloc(&self.state.next_addr, size) {
                Ok(x) => return Ok((a, x)),
                Err(OpCode::Again) => continue,
                Err(OpCode::NoSpace) => {
                    if a.set_state(Arena::HOT, Arena::WARM) == Arena::HOT {
                        self.install_new(a);
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    pub(crate) fn begin_packed_alloc(
        &self,
        total_real_size: u32,
        nr_frames: u32,
    ) -> Result<PackedAlloc, OpCode> {
        if nr_frames == 0 {
            log::error!("invalid packed allocation frame count 0");
            panic!("invalid packed allocation frame count");
        }

        self.enter_packed_multi();
        let pool = Handle::from(self as *const Pool as *mut Pool);
        let current = self.current();
        if (current.id() & Self::MULTI_DEP_GROUP_TAG) != 0 {
            log::error!("arena id overflow for multi dep group, id {}", current.id());
            panic!("arena id overflow for multi dep group");
        }
        Ok(PackedAlloc {
            pool,
            dep_group_id: current.id() | Self::MULTI_DEP_GROUP_TAG,
            total_real_size,
            remain_real_size: total_real_size,
            nr_frames,
            remain_frames: nr_frames,
            detached: Vec::new(),
            current: PackedSegment::new(current),
            done: false,
        })
    }

    pub(crate) fn alloc_pair(
        &self,
        size1: u32,
        size2: u32,
    ) -> Result<((Handle<Arena>, BoxRef), (Handle<Arena>, BoxRef)), OpCode> {
        let _gate = self.enter_alloc_gate();
        let total_real_size = BoxRef::real_size(size1) + BoxRef::real_size(size2);
        loop {
            let a = self.current();
            let cur = self.ctx.numerics.log_size.load(Relaxed);
            if cur >= self.max_log_size && a.set_state(Arena::HOT, Arena::WARM) == Arena::HOT {
                let old = self.ctx.numerics.log_size.fetch_sub(cur, Relaxed);
                assert!(old >= cur);
                self.install_new(a);
                continue;
            }

            match a.reserve_batch(total_real_size, 2) {
                Ok(()) => {
                    let start = self.state.reserve_addr_span(2);
                    let b1 = a.alloc_at_addr(start, BoxRef::real_size(size1));
                    let b2 = a.alloc_at_addr(start + 1, BoxRef::real_size(size2));
                    return Ok(((a, b1), (a, b2)));
                }
                Err(OpCode::Again) => continue,
                Err(OpCode::NoSpace) => {
                    if a.set_state(Arena::HOT, Arena::WARM) == Arena::HOT {
                        self.install_new(a);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub(crate) fn recycle<F>(&self, addr: &[u64], mut gc: F)
    where
        F: FnMut(u64),
    {
        let _gate = self.enter_alloc_gate();
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
            std::thread::yield_now();
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

    pub(crate) fn alloc_pair(
        &self,
        size1: u32,
        size2: u32,
    ) -> Result<((Handle<Arena>, BoxRef), (Handle<Arena>, BoxRef)), OpCode> {
        self.pool.alloc_pair(size1, size2)
    }

    pub(crate) fn begin_packed_alloc(
        &self,
        total_real_size: u32,
        nr_frames: u32,
    ) -> Result<PackedAlloc, OpCode> {
        self.pool.begin_packed_alloc(total_real_size, nr_frames)
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
        Self {
            buckets: DashMap::new(),
            lru: Handle::new(ShardPriorityLru::new(
                opt.cache_count,
                opt.high_priority_ratio,
            )),
            used: Arc::new(AtomicIsize::new(0)),
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
        self.buckets.clear();

        // 5) release shared lru
        self.lru.reclaim();
    }

    pub(crate) fn del_bucket(&self, bucket_id: u64) {
        if let Some(ctx) = self.buckets.get(&bucket_id).map(|x| x.value().clone()) {
            ctx.flush_and_wait();
        }
        let _ = self.buckets.remove(&bucket_id);
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
mod test {
    use super::Pool;
    use crate::Options;
    use crate::cc::context::Context;
    use crate::map::flush::{Flush, FlushDirective, FlushObserver, FlushResult};
    use crate::map::table::BucketState;
    use crate::meta::Numerics;
    use crate::types::header::{NodeType, TagFlag, TagKind};
    use crate::types::traits::IHeader;
    use crate::utils::{Handle, INIT_ADDR, MutRef, OpCode, RandomPath};
    use parking_lot::Mutex;
    use std::sync::Arc;

    #[derive(Default)]
    struct CaptureObserver {
        groups: Mutex<Vec<(u64, u32)>>,
    }

    impl CaptureObserver {
        fn snapshot(&self) -> Vec<(u64, u32)> {
            self.groups.lock().clone()
        }
    }

    impl FlushObserver for CaptureObserver {
        fn flush_directive(&self, _bucket_id: u64) -> FlushDirective {
            FlushDirective::Normal
        }

        fn stage_orphan_data_file(&self, _file_id: u64) {}

        fn stage_orphan_blob_file(&self, _file_id: u64) {}

        fn on_flush(&self, result: FlushResult) -> Result<(), OpCode> {
            self.groups
                .lock()
                .push((result.dep_group_id, result.dep_group_items));
            result.done.mark_done();
            Ok(())
        }
    }

    #[test]
    fn packed_multi_alloc_uses_one_dep_group() {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        opt.concurrent_write = 1;
        opt.data_file_size = 8 << 10;
        opt.max_log_size = 64 << 10;
        opt.wal_file_size = 8 << 10;
        let opt = Arc::new(opt.validate().expect("validate options failed"));

        let numerics = Arc::new(Numerics::default());
        let ctx = Handle::new(Context::new(opt.clone(), numerics, &[]));
        let observer = Arc::new(CaptureObserver::default());
        let flush = Flush::new(ctx, observer.clone());

        {
            let state = MutRef::new(BucketState::new());
            let pool = Pool::new(opt.clone(), ctx, state, flush.clone(), 7, INIT_ADDR);

            let frame_size = 5_000u32;
            let frame_real = crate::types::refbox::BoxRef::real_size(frame_size);
            let total_real = frame_real * 3;
            assert!(total_real as usize > opt.data_file_size);

            let mut packed = pool
                .begin_packed_alloc(total_real, 3)
                .expect("begin packed allocation failed");
            for seq in 0..3 {
                let (arena, mut b) = packed.alloc(frame_size);
                let h = b.header_mut();
                h.kind = TagKind::Base;
                h.node_type = NodeType::Leaf;
                h.flag = TagFlag::Normal;
                h.txid = seq + 1;
                arena.dec_ref();
            }
            packed.finish();
            pool.wait_flush();

            let groups = observer.snapshot();
            assert!(
                !groups.is_empty(),
                "must flush at least one arena in packed group"
            );
            let group_items = groups[0].1;
            assert!(
                group_items > 1,
                "expected multi-arena packed flush, got dep_group_items={group_items}"
            );
            assert_eq!(
                groups.len(),
                group_items as usize,
                "flush count should match dep_group_items"
            );

            let group_id = groups[0].0;
            assert_ne!(
                group_id & Pool::MULTI_DEP_GROUP_TAG,
                0,
                "multi-arena dep group should carry multi tag"
            );
            for (gid, items) in groups {
                assert_eq!(gid, group_id);
                assert_eq!(items, group_items);
            }
        }

        flush.quit();
        ctx.quit();
        ctx.reclaim();
    }
}
