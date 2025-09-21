use std::sync::{
    Arc,
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
        traits::{IHeader, IInlineSize, ILoader},
    },
    utils::{Handle, MutRef, options::ParsedOptions, queue::Queue},
};
use dashmap::{DashMap, Entry};

use super::{cache::CacheState, data::FlushData, flush::Flush};
use crate::map::cache::NodeCache;
use crate::map::table::PageMap;
use crate::types::refbox::BoxRef;
use crate::utils::lru::Lru;
use crate::utils::unpack_id;
use std::sync::atomic::Ordering::{Acquire, Relaxed};

struct Pool {
    workers: usize,
    flush: Flush,
    /// contains all flushed arena
    free: Arc<Queue<Handle<Arena>>>,
    /// contains all non-HOT arena
    wait: Arc<DashMap<u32, Handle<Arena>>>,
    /// currently HOT arena
    cur: AtomicPtr<Arena>,
    numerics: Arc<Numerics>,
}

impl Pool {
    const INIT_ARENA: u32 = 16; // must be power of 2

    fn new(
        opt: Arc<ParsedOptions>,
        ctx: Handle<Context>,
        numerics: Arc<Numerics>,
    ) -> Result<Pool, OpCode> {
        let workers = opt.workers;
        let id = Self::get_id(&numerics)?;
        let q = Queue::new(Self::INIT_ARENA as usize);
        for _ in 0..Self::INIT_ARENA {
            let h = Handle::new(Arena::new(opt.data_file_size, workers));
            q.push(h).unwrap();
        }

        let h = q.pop().unwrap();

        let this = Self {
            workers,
            flush: Flush::new(ctx),
            free: Arc::new(q),
            wait: Arc::new(DashMap::new()),
            cur: AtomicPtr::new(h.inner()),
            numerics,
        };

        h.reset(id);
        Ok(this)
    }

    fn get_id(numerics: &Numerics) -> Result<u32, OpCode> {
        let id = numerics.next_file_id.fetch_add(1, Relaxed);

        // next id is equal to the oldest, which means there's no space left
        if id + 1 == numerics.oldest_file_id.load(Acquire) {
            return Err(OpCode::DbFull);
        }
        Ok(id)
    }

    fn current(&self) -> Handle<Arena> {
        self.cur.load(Relaxed).into()
    }

    fn load(&self, addr: u64) -> Option<BoxRef> {
        let (id, off) = unpack_id(addr);
        let cur = self.current();
        let cur_id = cur.id();

        if cur_id == id {
            return Some(cur.load(off));
        }

        if let Some(h) = self.wait.get(&id) {
            if !matches!(h.state(), Arena::WARM | Arena::COLD) {
                return None;
            }
            if h.id() == id {
                return Some(h.load(off));
            }
        }
        None
    }

    /// only one thread can process `install_new` at same time
    fn install_new(&self, cur: Handle<Arena>) -> Result<(), OpCode> {
        let id = Self::get_id(&self.numerics)?;
        self.wait.insert(cur.id(), cur);
        self.flush(cur);

        let p = if let Some(p) = self.free.pop() {
            p
        } else {
            Handle::new(Arena::new(cur.cap(), self.workers))
        };
        p.reset(id);

        self.cur
            .compare_exchange(cur.inner(), p.inner(), Relaxed, Relaxed)
            .expect("never happen");
        Ok(())
    }

    fn flush(&self, h: Handle<Arena>) {
        let wait = self.wait.clone();
        let free = self.free.clone();

        let cb = move || {
            let x = h.set_state(Arena::COLD, Arena::FLUSH);
            assert_eq!(x, Arena::COLD);
            wait.remove(&h.id());
            if free.push(h).is_err() {
                h.reclaim();
            }
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
    /// used for restrict in memory node count
    cache: Arc<NodeCache>,
    table: Arc<PageMap>,
    /// second level cache, provide data for NodeCache
    remote: Handle<Lru<BoxRef>>,
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
    ) -> Result<Self, OpCode> {
        let opt = ctx.opt.clone();
        let numerics = ctx.numerics.clone();
        Ok(Self {
            ctx,
            cache,
            table: pagemap,
            remote: Handle::new(Lru::new(opt.cache_count)),
            pool: Handle::new(Pool::new(opt.clone(), ctx, numerics)?),
            tx,
            rx,
        })
    }

    pub(crate) fn alloc(&self, size: u32) -> Result<(Handle<Arena>, BoxRef), OpCode> {
        loop {
            let a = self.pool.current();
            match a.alloc(&self.pool.numerics, size) {
                Ok(x) => return Ok((a, x)),
                Err(e @ OpCode::TooLarge) => return Err(e),
                Err(OpCode::Again) => continue,
                Err(OpCode::NeedMore) => {
                    if a.set_state(Arena::HOT, Arena::WARM) == Arena::HOT {
                        self.pool.install_new(a)?;
                    }
                }
                _ => unreachable!("invalid opcode"),
            }
        }
    }

    pub(crate) fn record_lsn(&self, worker_id: usize, seq: u64) {
        self.pool.current().record_lsn(worker_id, seq);
    }

    #[cfg(not(feature = "disable_recycle"))]
    pub(crate) fn tombstone_active<F>(&self, addr: &[u64], mut gc: F)
    where
        F: FnMut(u64),
    {
        let a = self.pool.current();
        a.inc_ref();
        for i in addr {
            if !a.recycle(i) {
                gc(*i);
            }
        }
        a.dec_ref();
    }

    #[cfg(feature = "disable_recycle")]
    pub(crate) fn tombstone_active<F>(&self, addr: &[u64], mut gc: F)
    where
        F: FnMut(u64),
    {
        for i in addr {
            gc(*i);
        }
    }

    pub(crate) fn quit(&self) {
        let _ = self.tx.send(SharedState::Quit);
        let _ = self.rx.recv();
        // make sure flush thread quit before we reclaim mapping
        self.pool.quit();
        self.remote.reclaim();
        self.pool.reclaim();
    }

    pub(crate) fn loader(&self) -> Loader {
        let split_elems = self.ctx.opt.split_elems as u32;
        Loader {
            max_inline_size: self.ctx.opt.max_inline_size,
            split_elems,
            pool: self.pool,
            ctx: self.ctx,
            cache: self.remote,
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
    max_inline_size: u32,
    split_elems: u32,
    pool: Handle<Pool>,
    ctx: Handle<Context>,
    cache: Handle<Lru<BoxRef>>,
    /// it's per-node context
    pinned: MutRef<DashMap<u64, BoxRef>>,
}

impl Loader {
    fn load(&self, addr: u64) -> Option<BoxRef> {
        if let Some(x) = self.cache.get(addr) {
            return Some(x.clone());
        }
        if let Some(x) = self.pool.load(addr) {
            self.cache.add(addr, x.clone());
            return Some(x);
        }
        let x = self.ctx.manifest.load_impl(addr)?;
        self.cache.add(addr, x.clone());
        Some(x)
    }
}

// clone happens where node is going to be replace, be careful this may hurt cold fetch performance
impl Clone for Loader {
    fn clone(&self) -> Self {
        Self {
            max_inline_size: self.max_inline_size,
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
            max_inline_size: self.max_inline_size,
            split_elems: self.split_elems,
            pool: self.pool,
            ctx: self.ctx,
            cache: self.cache,
            pinned: self.pinned.clone(),
        }
    }

    fn pin(&self, data: BoxRef) {
        let r = self.pinned.insert(data.header().addr, data);
        debug_assert!(r.is_none());
    }

    fn pin_load(&self, addr: u64) -> Option<BoxView> {
        if let Some(p) = self.pinned.get(&addr) {
            let v = p.view();
            return Some(v);
        }
        let x = self.load(addr)?;

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
}

impl IInlineSize for Loader {
    fn inline_size(&self) -> u32 {
        self.max_inline_size
    }
}
