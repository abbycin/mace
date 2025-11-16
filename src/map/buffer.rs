use std::sync::{
    Arc, RwLock,
    atomic::AtomicPtr,
    mpsc::{Receiver, Sender},
};

use crate::{
    OpCode,
    cc::context::Context,
    map::{SharedState, data::Arena, table::Swip},
    meta::Numerics,
    types::{
        page::Page,
        refbox::BoxView,
        traits::{IHeader, ILoader},
    },
    utils::{
        Handle, MutRef,
        lru::{CachePriority, ShardPriorityLru},
        options::ParsedOptions,
        queue::Queue,
    },
};
use dashmap::{DashMap, Entry};

use super::{cache::CacheState, data::FlushData, flush::Flush};
use crate::map::cache::NodeCache;
use crate::map::table::PageMap;
use crate::types::refbox::BoxRef;
use std::sync::atomic::Ordering::Relaxed;

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
        let lk = self.map.read().expect("can't lock read");
        let pos = match lk.binary_search_by(|x| x.addr.cmp(&addr)) {
            Ok(pos) => pos,
            Err(pos) => {
                // a recovered Pool don't have previous arena addr
                if pos == 0 {
                    return None;
                }
                pos - 1
            }
        };
        #[cfg(feature = "extra_check")]
        {
            debug_assert!(pos < lk.len());
            debug_assert!(addr >= lk[pos].addr);
        }
        Some(lk[pos].arena)
    }

    /// the addr is monotonically increasing, so we can perform binary search on
    /// map
    fn push(&self, addr: u64, arena: u64) {
        let mut lk = self.map.write().expect("can't lock write");
        lk.push(Ids::new(addr, arena));
    }

    /// the data file flush is FIFO, so we can simply remove the first element and
    /// left-shift all rest elements
    fn pop(&self) {
        let mut lk = self.map.write().expect("can't lock write");
        lk.remove(0);
    }
}

#[cfg(feature = "metric")]
#[derive(Debug)]
pub struct PoolStat {
    nr_spin: AtomicUsize,
    nr_alloc: AtomicUsize,
    nr_free: AtomicUsize,
}

#[cfg(feature = "metric")]
static G_POOL_STAT: PoolStat = PoolStat {
    nr_spin: AtomicUsize::new(0),
    nr_alloc: AtomicUsize::new(0),
    nr_free: AtomicUsize::new(0),
};

#[cfg(feature = "metric")]
macro_rules! inc_count {
    ($field: ident) => {
        G_POOL_STAT.$field.fetch_add(1, Relaxed)
    };
}

#[cfg(not(feature = "metric"))]
macro_rules! inc_count {
    ($field: ident) => {};
}

#[cfg(feature = "metric")]
pub fn g_pool_status() -> &'static PoolStat {
    &G_POOL_STAT
}

struct Pool {
    flush: Flush,
    map: Arc<Iim>,
    /// contains all flushed arena
    free: Arc<Queue<Handle<Arena>>>,
    /// contains all non-HOT arena
    wait: Arc<DashMap<u64, Handle<Arena>>>,
    /// currently HOT arena
    cur: AtomicPtr<Arena>,
    numerics: Arc<Numerics>,
    allow_over_provision: bool,
}

impl Pool {
    const INIT_ARENA: u32 = 16; // must be power of 2

    fn new(opt: Arc<ParsedOptions>, ctx: Handle<Context>, numerics: Arc<Numerics>) -> Self {
        let workers = opt.workers;
        let id = Self::get_id(&numerics);
        let q = Queue::new(Self::INIT_ARENA as usize);
        for _ in 0..Self::INIT_ARENA {
            let h = Handle::new(Arena::new(opt.data_file_size, workers));
            q.push(h).unwrap();
        }

        let h = q.pop().unwrap();

        let this = Self {
            flush: Flush::new(ctx),
            map: Arc::new(Iim::new(numerics.address.fetch_add(1, Relaxed), id)),
            free: Arc::new(q),
            wait: Arc::new(DashMap::new()),
            cur: AtomicPtr::new(h.inner()),
            numerics,
            allow_over_provision: opt.over_provision,
        };

        inc_count!(nr_alloc);
        h.reset(id);
        this
    }

    fn get_id(numerics: &Numerics) -> u64 {
        numerics.next_data_id.fetch_add(1, Relaxed)
    }

    fn current(&self) -> Handle<Arena> {
        self.cur.load(Relaxed).into()
    }

    fn load(&self, addr: u64) -> Option<BoxRef> {
        let arena_id = self.map.find(addr)?;
        let cur = self.current();
        let cur_id = cur.id();

        if cur_id == arena_id {
            return Some(cur.load(addr));
        }

        if let Some(h) = self.wait.get(&arena_id) {
            if !matches!(h.state(), Arena::WARM | Arena::COLD) {
                return None;
            }
            if h.id() == arena_id {
                return Some(h.load(addr));
            }
        }
        None
    }

    /// only one thread can process `install_new` at same time
    fn install_new(&self, cur: Handle<Arena>) {
        let id = Self::get_id(&self.numerics);
        self.wait.insert(cur.id(), cur);
        self.flush(cur);

        // it's ok, since all threads are wait for install new arena so that address
        // will not be changed before new arena has been installed
        self.map.push(self.numerics.address.load(Relaxed), id);

        let mut is_spin = false;
        inc_count!(nr_alloc);
        let p = loop {
            if let Some(p) = self.free.pop() {
                break p;
            }
            if self.allow_over_provision {
                break Handle::new(Arena::new(cur.cap(), cur.workers()));
            } else {
                if !is_spin {
                    is_spin = true;
                    inc_count!(nr_spin);
                }
                std::hint::spin_loop();
            }
        };
        p.reset(id);

        self.cur
            .compare_exchange(cur.inner(), p.inner(), Relaxed, Relaxed)
            .expect("never happen");
    }

    fn flush(&self, h: Handle<Arena>) {
        let wait = self.wait.clone();
        let free = self.free.clone();
        let map = self.map.clone();

        let cb = move || {
            let x = h.set_state(Arena::COLD, Arena::FLUSH);
            assert_eq!(x, Arena::COLD);
            h.clear();
            wait.remove(&h.id());
            if free.push(h).is_err() {
                h.reclaim();
            }
            map.pop();
            inc_count!(nr_free);
        };

        let _ = self
            .flush
            .tx
            .send(FlushData::new(h, Box::new(cb)))
            .inspect_err(|x| {
                log::error!("can't send flush data {x:?}");
                std::process::abort();
            });
    }

    fn quit(&self) {
        let cur = self.current();
        self.wait.insert(cur.id(), cur);
        // cur may still hot
        cur.set_state(Arena::HOT, Arena::WARM);
        self.flush(cur);
        self.flush.quit();
        // arena in `free` has invalid id, so clean them individually
        while let Some(h) = self.free.pop() {
            h.reclaim();
        }
        self.wait.iter().for_each(|h| {
            h.reclaim();
        });
    }
}

pub(crate) struct Buffers {
    ctx: Handle<Context>,
    max_log_size: usize,
    /// used for restrict in memory node count
    cache: Arc<NodeCache>,
    table: Arc<PageMap>,
    /// second level cache, provide data for NodeCache
    lru: Handle<ShardPriorityLru<BoxRef>>,
    pool: Handle<Pool>,
    tx: Sender<SharedState>,
    rx: Receiver<()>,
}

impl Buffers {
    pub(crate) fn new(
        pagemap: Arc<PageMap>,
        ctx: Handle<Context>,
        cache: Arc<NodeCache>,
        tx: Sender<SharedState>,
        rx: Receiver<()>,
    ) -> Self {
        let opt = ctx.opt.clone();
        let numerics = ctx.numerics.clone();
        Self {
            ctx,
            max_log_size: ctx.opt.max_log_size * ctx.opt.workers as usize,
            cache,
            table: pagemap,
            lru: Handle::new(ShardPriorityLru::new(
                opt.cache_count,
                opt.high_priority_ratio,
            )),
            pool: Handle::new(Pool::new(opt.clone(), ctx, numerics)),
            tx,
            rx,
        }
    }

    pub(crate) fn alloc(&self, size: u32) -> Result<(Handle<Arena>, BoxRef), OpCode> {
        loop {
            let a = self.pool.current();
            let cur = self.pool.numerics.log_size.load(Relaxed);
            if cur >= self.max_log_size && a.set_state(Arena::HOT, Arena::WARM) == Arena::HOT {
                let old = self.pool.numerics.log_size.fetch_sub(cur, Relaxed);
                assert!(old >= cur);
                self.pool.install_new(a);
                continue;
            }

            match a.alloc(&self.pool.numerics, size) {
                Ok(x) => return Ok((a, x)),
                Err(e @ OpCode::TooLarge) => return Err(e),
                Err(OpCode::Again) => continue,
                Err(OpCode::NeedMore) => {
                    if a.set_state(Arena::HOT, Arena::WARM) == Arena::HOT {
                        self.pool.install_new(a);
                    }
                }
                _ => unreachable!("invalid opcode"),
            }
        }
    }

    pub(crate) fn record_lsn(&self, worker_id: usize, seq: u64) {
        self.pool.current().record_lsn(worker_id, seq);
    }

    pub(crate) fn recycle<F>(&self, addr: &[u64], mut gc: F)
    where
        F: FnMut(u64),
    {
        let a = self.pool.current();
        a.inc_ref();
        for &i in addr {
            if !a.recycle(i) {
                gc(i);
            }
        }
        a.dec_ref();
    }

    pub(crate) fn quit(&self) {
        let _ = self.tx.send(SharedState::Quit);
        let _ = self.rx.recv();
        // make sure flush thread quit before we reclaim mapping
        self.pool.quit();
        self.lru.reclaim();
        self.pool.reclaim();
    }

    pub(crate) fn loader(&self) -> Loader {
        let split_elems = self.ctx.opt.split_elems as u32;
        Loader {
            split_elems,
            pool: self.pool,
            ctx: self.ctx,
            cache: self.lru,
            pinned: MutRef::new(DashMap::with_capacity(split_elems as usize)),
        }
    }

    pub(crate) fn cache(&self, p: Page<Loader>) {
        if p.size() > self.cache.cap() {
            log::warn!("cache_capacity too small: {}", self.cache.cap());
        }
        let state = CacheState::Warm as u32 + p.is_intl() as u32;
        self.cache.put(p.pid(), state, p.size() as isize);

        if self.cache.full() {
            self.tx.send(SharedState::Evict).expect("never happen");
        }
    }

    pub(crate) fn evict(&self, pid: u64) {
        self.cache.evict_one(pid);
    }

    pub(crate) fn warm(&self, pid: u64, size: usize) {
        self.cache.warm(pid, size);
    }

    pub(crate) fn load(&self, pid: u64) -> Result<Option<Page<Loader>>, OpCode> {
        loop {
            let swip = Swip::new(self.table.get(pid));
            // never mapped or unmapped
            if swip.is_null() {
                return Ok(None);
            }

            if !swip.is_tagged() {
                // we delay cache warm up when the value of page map entry was updated
                return Ok(Some(Page::<Loader>::from_swip(swip.raw())));
            }
            let new = Page::load(self.loader(), swip.untagged()).ok_or(OpCode::NotFound)?;
            if self.table.cas(pid, swip.raw(), new.swip()).is_ok() {
                self.cache(new);
                return Ok(Some(new));
            } else {
                new.reclaim();
            }
        }
    }
}

pub struct Loader {
    split_elems: u32,
    pool: Handle<Pool>,
    ctx: Handle<Context>,
    cache: Handle<ShardPriorityLru<BoxRef>>,
    /// it's per-node context
    pinned: MutRef<DashMap<u64, BoxRef>>,
}

impl Loader {
    fn find(&self, addr: u64) -> Option<BoxRef> {
        if let Some(x) = self.cache.get(addr) {
            return Some(x.clone());
        }
        if let Some(x) = self.pool.load(addr) {
            self.cache.add(CachePriority::High, addr, x.clone());
            return Some(x);
        }
        let x = self.ctx.manifest.load_data(addr)?;
        self.cache.add(CachePriority::High, addr, x.clone());
        Some(x)
    }
}

// clone happens where node is going to be replace, be careful this may hurt cold fetch performance
impl Clone for Loader {
    fn clone(&self) -> Self {
        Self {
            split_elems: self.split_elems,
            pool: self.pool,
            ctx: self.ctx,
            cache: self.cache,
            pinned: MutRef::new(DashMap::with_capacity(self.split_elems as usize)),
        }
    }
}

impl ILoader for Loader {
    fn shallow_copy(&self) -> Self {
        Self {
            split_elems: self.split_elems,
            pool: self.pool,
            ctx: self.ctx,
            cache: self.cache,
            pinned: self.pinned.clone(),
        }
    }

    /// save data come from arena
    fn pin(&self, data: BoxRef) {
        let r = self.pinned.insert(data.header().addr, data);
        debug_assert!(r.is_none());
    }

    fn load(&self, addr: u64) -> Option<BoxView> {
        if let Some(p) = self.pinned.get(&addr) {
            let v = p.view();
            return Some(v);
        }
        let x = self.find(addr)?;

        // concurrent load may happen, we have to make sure that all the load get the same view
        let e = self.pinned.entry(addr);
        match e {
            Entry::Occupied(o) => Some(o.get().view()),
            Entry::Vacant(v) => {
                let r = x.view();
                v.insert(x);
                Some(r)
            }
        }
    }

    fn load_remote(&self, addr: u64) -> Option<BoxView> {
        if let Some(x) = self.cache.get(addr) {
            return Some(x.view());
        }
        if let Some(x) = self.pool.load(addr) {
            self.cache.add(CachePriority::Low, addr, x.clone());
            return Some(x.view());
        }

        let b = self.ctx.manifest.load_blob(addr)?;
        let v = b.view();
        self.cache.add(CachePriority::Low, addr, b);
        Some(v)
    }
}
