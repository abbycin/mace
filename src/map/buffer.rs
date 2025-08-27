use std::sync::{
    Arc,
    atomic::AtomicPtr,
    mpsc::{Receiver, Sender},
};

use crate::{
    OpCode,
    map::{SharedState, data::Arena, table::Swip},
    types::{
        page::Page,
        refbox::BoxView,
        traits::{IHeader, IInlineSize, ILoader},
    },
    utils::{
        Handle, INVALID_ID, MutRef, NULL_ID, data::Meta, options::ParsedOptions, queue::Queue,
    },
};
use dashmap::{DashMap, Entry};

use super::{cache::CacheState, data::FlushData, flush::Flush, load::Mapping};
use crate::map::cache::NodeCache;
use crate::map::table::PageMap;
use crate::types::refbox::BoxRef;
use crate::utils::lru::Lru;
use crate::utils::unpack_id;
use std::sync::atomic::Ordering::Relaxed;

struct Pool {
    flush: Flush,
    /// contains all flushed arena
    free: Arc<Queue<Handle<Arena>>>,
    /// contains all non-HOT arena
    wait: Arc<DashMap<u32, Handle<Arena>>>,
    /// currently HOT arena
    cur: AtomicPtr<Arena>,
    meta: Arc<Meta>,
}

impl Pool {
    const INIT_ARENA: u32 = 16; // must be power of 2

    fn new(
        opt: Arc<ParsedOptions>,
        mapping: Handle<Mapping>,
        meta: Arc<Meta>,
    ) -> Result<Pool, OpCode> {
        let id = Self::get_id(&meta)?;
        let q = Queue::new(Self::INIT_ARENA as usize);
        for _ in 0..Self::INIT_ARENA {
            let h = Handle::new(Arena::new(opt.data_file_size));
            q.push(h).unwrap();
        }

        let h = q.pop().unwrap();

        let this = Self {
            flush: Flush::new(mapping, meta.clone()),
            free: Arc::new(q),
            wait: Arc::new(DashMap::new()),
            cur: AtomicPtr::new(h.inner()),
            meta,
        };

        h.reset(id);
        Ok(this)
    }

    fn get_id(meta: &Meta) -> Result<u32, OpCode> {
        let id = meta.next_data.fetch_add(1, Relaxed);

        // TODO: deal with id exhaustion
        if id == NULL_ID {
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
        let id = Self::get_id(&self.meta)?;
        self.wait.insert(cur.id(), cur);
        self.flush(cur);

        let p = if let Some(p) = self.free.pop() {
            p
        } else {
            Handle::new(Arena::new(cur.cap()))
        };
        p.reset(id);

        self.cur
            .compare_exchange(cur.inner(), p.inner(), Relaxed, Relaxed)
            .expect("never happen");
        Ok(())
    }

    fn flush(&self, h: Handle<Arena>) {
        assert!(h.id() > INVALID_ID);
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
    max_inline_size: u32,
    split_elems: u32,
    /// used for restrict in memory node count
    cache: Arc<NodeCache>,
    table: Arc<PageMap>,
    /// second level cache, provide data for NodeCache
    remote: Handle<Lru<BoxRef>>,
    pool: Handle<Pool>,
    pub mapping: Handle<Mapping>,
    tx: Sender<SharedState>,
    rx: Receiver<()>,
}

impl Buffers {
    pub(crate) fn new(
        pagemap: Arc<PageMap>,
        opt: Arc<ParsedOptions>,
        meta: Arc<Meta>,
        cache: Arc<NodeCache>,
        mapping: Mapping,
        tx: Sender<SharedState>,
        rx: Receiver<()>,
    ) -> Result<Self, OpCode> {
        let mapping = Handle::new(mapping);
        Ok(Self {
            max_inline_size: opt.max_inline_size,
            split_elems: opt.split_elems as u32,
            cache,
            table: pagemap,
            remote: Handle::new(Lru::new(opt.cache_count)),
            pool: Handle::new(Pool::new(opt.clone(), mapping, meta)?),
            mapping,
            tx,
            rx,
        })
    }

    pub(crate) fn alloc(&self, size: u32) -> Result<(Handle<Arena>, BoxRef), OpCode> {
        loop {
            let a = self.pool.current();
            match a.alloc(size) {
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

    #[cfg(not(feature = "test_disable_recycle"))]
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

    #[cfg(feature = "test_disable_recycle")]
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
        self.mapping.reclaim();
        self.remote.reclaim();
        self.pool.reclaim();
    }

    pub(crate) fn loader(&self) -> Loader {
        Loader {
            max_inline_size: self.max_inline_size,
            split_elems: self.split_elems,
            pool: self.pool,
            mapping: self.mapping,
            cache: self.remote,
            pinned: MutRef::new(DashMap::with_capacity(self.split_elems as usize)),
        }
    }

    pub(crate) fn cache(&self, p: Page<Loader>) {
        let state = CacheState::Warm as u32 + p.is_intl() as u32;
        self.cache.put(p.pid(), state, p.size() as isize);

        if self.cache.full() {
            self.tx.send(SharedState::Evict).expect("never happen");
        }
    }

    pub(crate) fn evict(&self, pid: u64) {
        self.cache.evict_one(pid);
    }

    pub(crate) fn load(&self, pid: u64) -> Option<Page<Loader>> {
        loop {
            let swip = Swip::new(self.table.get(pid));
            // never mapped or unmapped
            if swip.is_null() {
                return None;
            }

            if !swip.is_tagged() {
                self.cache.warm(pid);
                return Some(Page::<Loader>::from_swip(swip.raw()));
            }
            let new = Page::load(self.loader(), swip.untagged());
            if self.table.cas(pid, swip.raw(), new.swip()).is_ok() {
                self.cache(new);
                return Some(new);
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
    mapping: Handle<Mapping>,
    cache: Handle<Lru<BoxRef>>,
    /// it's per-node context
    pinned: MutRef<DashMap<u64, BoxRef>>,
}

impl Loader {
    fn load(&self, addr: u64) -> BoxRef {
        if let Some(x) = self.cache.get(addr) {
            return x.clone();
        }
        if let Some(x) = self.pool.load(addr) {
            self.cache.add(addr, x.clone());
            return x;
        }
        let x = self.mapping.load_impl(addr);
        self.cache.add(addr, x.clone());
        x
    }
}

// clone happens where node is going to be replace, be careful this may hurt cold fetch performance
impl Clone for Loader {
    fn clone(&self) -> Self {
        Self {
            max_inline_size: self.max_inline_size,
            split_elems: self.split_elems,
            pool: self.pool,
            mapping: self.mapping,
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
            mapping: self.mapping,
            cache: self.cache,
            pinned: self.pinned.clone(),
        }
    }

    fn pin(&self, data: BoxRef) {
        let r = self.pinned.insert(data.header().addr, data);
        debug_assert!(r.is_none());
    }

    fn pin_load(&self, addr: u64) -> BoxView {
        if let Some(p) = self.pinned.get(&addr) {
            let v = p.view();
            return v;
        }
        let x = self.load(addr);

        // concurrent load may happen, we have to make sure that all the load get the same view
        let e = self.pinned.entry(addr);
        match e {
            Entry::Occupied(o) => o.get().view(),
            Entry::Vacant(v) => {
                let r = x.view();
                v.insert(x);
                r
            }
        }
    }
}

impl IInlineSize for Loader {
    fn inline_size(&self) -> u32 {
        self.max_inline_size
    }
}
