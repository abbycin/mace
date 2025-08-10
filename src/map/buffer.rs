use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicPtr, AtomicU32},
    },
};

use crate::{
    OpCode,
    map::{data::Arena, table::Swip},
    types::{
        page::Page,
        refbox::BoxView,
        traits::{IHeader, IInlineSize, ILoader},
    },
    utils::{
        Handle, INIT_ORACLE, INVALID_ID, MutRef, NULL_ID, data::Meta, options::ParsedOptions,
        queue::Queue,
    },
};
use crossbeam_epoch::Guard;
use dashmap::{DashMap, Entry};

use super::{cache::CacheState, data::FlushData, flush::Flush, load::Mapping};
use crate::map::cache::NodeCache;
use crate::map::table::PageMap;
use crate::types::refbox::BoxRef;
use crate::utils::lru::Lru;
use crate::utils::unpack_id;
use std::sync::atomic::Ordering::{Relaxed, Release};

struct Pool {
    flush: Flush,
    /// contains all flushed arena
    free: Arc<Queue<Handle<Arena>>>,
    /// contains all non-HOT arena
    wait: Arc<DashMap<u32, Handle<Arena>>>,
    /// queue of arena ordered by id, for flush only
    que: Mutex<VecDeque<Handle<Arena>>>,
    /// currently HOT arena
    cur: AtomicPtr<Arena>,
    meta: Arc<Meta>,
    flsn: AtomicU32,
}

impl Pool {
    const NR_MAX_ARENA: u32 = 32; // must be power of 2

    fn new(
        opt: Arc<ParsedOptions>,
        mapping: Handle<Mapping>,
        meta: Arc<Meta>,
    ) -> Result<Pool, OpCode> {
        let id = Self::get_id(&meta)?;
        let q = Queue::new(Self::NR_MAX_ARENA as usize);
        for _ in 0..Self::NR_MAX_ARENA {
            let h = Handle::new(Arena::new(opt.data_file_size));
            q.push(h).unwrap();
        }

        let h = q.pop().unwrap();

        let this = Self {
            flush: Flush::new(mapping),
            free: Arc::new(q),
            wait: Arc::new(DashMap::new()),
            que: Mutex::new(VecDeque::new()),
            cur: AtomicPtr::new(h.inner()),
            meta,
            flsn: AtomicU32::new(INIT_ORACLE as u32),
        };

        h.set_state(Arena::FLUSH, Arena::HOT);
        h.reset(id, this.flsn.load(Relaxed));
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

    fn update_flsn(&self) {
        self.flsn.fetch_add(1, Release);
        self.try_flush();
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
        {
            let mut lk = self.que.lock().unwrap();
            lk.push_back(cur); // keep the allocattion order
            drop(lk);
            self.try_flush();
        }

        let p = if let Some(p) = self.free.pop() {
            p
        } else {
            Handle::new(Arena::new(cur.cap()))
        };
        p.set_state(Arena::FLUSH, Arena::HOT);
        p.reset(id, self.flsn.load(Relaxed));

        self.cur
            .compare_exchange(cur.inner(), p.inner(), Relaxed, Relaxed)
            .expect("never happen");
        Ok(())
    }

    fn try_flush(&self) -> bool {
        let Ok(mut que) = self.que.try_lock() else {
            return false;
        };
        let lsn = self.flsn.load(Relaxed);

        while let Some(h) = que.pop_front() {
            let ready = if h.is_dirty() {
                // there're write txns, we must wait until log flushed, namely pool's flsn >
                // arena's
                lsn.wrapping_sub(h.flsn.load(Relaxed)) as i32 > 0
            } else {
                // no write happens
                h.state() == Arena::WARM
            };

            if ready && h.unref() && h.set_state(Arena::WARM, Arena::COLD) == Arena::WARM {
                self.flush(h);
            } else {
                que.push_front(h);
                break;
            }
        }
        true
    }

    fn flush(&self, h: Handle<Arena>) {
        assert_eq!(h.state(), Arena::COLD);

        let id = h.id();
        assert!(id > INVALID_ID);
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
            .send(FlushData::new(id, h.used(), Box::new(cb)))
            .inspect_err(|x| {
                log::error!("can't send flush data {x:?}");
                std::process::abort();
            });
    }

    fn flush_all(&self) {
        let mut cnt = 0;
        let cur = self.current();
        self.wait.insert(cur.id(), cur);
        let mut q: Vec<Handle<Arena>> = Vec::new();
        self.wait.iter().for_each(|h| {
            if matches!(h.state(), Arena::HOT | Arena::WARM) {
                h.state.store(Arena::COLD, Relaxed);
                q.push(*h);
            }
        });

        q.sort_unstable_by_key(|x| x.id());
        for h in q.iter() {
            self.flush(*h);
        }

        let n = q.len();
        while cnt != n {
            cnt = 0;
            for h in q.iter() {
                if matches!(h.state(), Arena::FLUSH) {
                    cnt += 1;
                }
            }
        }
    }

    fn quit(&self) {
        self.flush_all();
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

pub struct Buffers {
    max_inline_size: u32,
    split_elems: u32,
    /// used for restrict in memory node count
    cache: NodeCache,
    table: Arc<PageMap>,
    /// second level cache, provide data for NodeCache
    remote: Handle<Lru<BoxRef>>,
    pool: Handle<Pool>,
    pub mapping: Handle<Mapping>,
}

impl Buffers {
    pub fn new(
        pagemap: Arc<PageMap>,
        opt: Arc<ParsedOptions>,
        meta: Arc<Meta>,
        mapping: Mapping,
    ) -> Result<Self, OpCode> {
        let mapping = Handle::new(mapping);
        Ok(Self {
            max_inline_size: opt.max_inline_size,
            split_elems: opt.split_elem as u32,
            cache: NodeCache::new(opt.cache_capacity, opt.cache_evict_pct),
            table: pagemap,
            remote: Handle::new(Lru::new(opt.cache_count)),
            pool: Handle::new(Pool::new(opt.clone(), mapping, meta)?),
            mapping,
        })
    }

    pub fn alloc(&self, size: u32) -> Result<(Handle<Arena>, BoxRef), OpCode> {
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
    pub fn tombstone_active<F>(&self, addr: &[u64], mut gc: F)
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
    pub fn tombstone_active<F>(&self, addr: &[u64], mut gc: F)
    where
        F: FnMut(u64),
    {
        for i in addr {
            gc(*i);
        }
    }

    pub fn quit(&self) {
        // make sure flush thread quit before we reclaim mapping
        self.pool.quit();
        self.mapping.reclaim();
        self.remote.reclaim();
        self.pool.reclaim();
        self.cache.reclaim(|pid| {
            let swip = Swip::new(self.table.get(pid));
            if !swip.is_null() && !swip.is_tagged() {
                let p = Page::<Loader>::from_swip(swip.raw());
                p.reclaim();
            }
        });
    }

    #[inline]
    pub fn flush_arena(&self) {
        self.pool.try_flush();
    }

    #[inline]
    pub fn update_flsn(&self) {
        self.pool.update_flsn();
    }

    #[inline]
    pub fn mark_dirty(&self) {
        let a = self.pool.current();
        a.mark_dirty();
    }

    pub fn loader(&self) -> Loader {
        Loader {
            max_inline_size: self.max_inline_size,
            split_elems: self.split_elems,
            pool: self.pool,
            mapping: self.mapping,
            cache: self.remote,
            pinned: MutRef::new(DashMap::with_capacity(self.split_elems as usize)),
        }
    }

    pub fn cache(&self, g: &Guard, p: Page<Loader>) {
        if self.cache.full() {
            let pids = self.cache.evict();
            for pid in pids {
                loop {
                    let swip = Swip::new(self.table.get(pid));
                    if swip.is_null() || swip.is_tagged() {
                        continue;
                    }
                    let old = Page::<Loader>::from_swip(swip.untagged());
                    if self
                        .table
                        .cas(pid, swip.raw(), Swip::tagged(old.latest_addr()))
                        .is_ok()
                    {
                        g.defer(move || old.reclaim());
                        break;
                    }
                }
            }
        }
        let state = CacheState::Warm as u32 + p.is_intl() as u32;
        self.cache.put(p.pid(), state, p.size() as isize);
    }

    pub fn evict(&self, pid: u64) {
        self.cache.evict_one(pid);
    }

    pub fn load(&self, g: &Guard, pid: u64) -> Option<Page<Loader>> {
        loop {
            let swip = Swip::new(self.table.get(pid));
            // never mapped or unmapped
            if swip.is_null() {
                return None;
            }

            if !swip.is_tagged() {
                return Some(Page::<Loader>::from_swip(swip.raw()));
            }
            let new = Page::load(self.loader(), swip.untagged());
            if self.table.cas(pid, swip.raw(), new.swip()).is_ok() {
                self.cache(g, new);
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
