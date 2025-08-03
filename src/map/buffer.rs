use std::sync::{
    Arc,
    atomic::{AtomicPtr, AtomicU32, AtomicUsize},
};

use crate::{
    OpCode,
    map::{data::Arena, table::Swip},
    types::{page::Page, traits::IInlineSize},
    utils::{
        Handle, INIT_ORACLE, INVALID_ID, NULL_ID, countblock::Countblock, data::Meta,
        options::ParsedOptions,
    },
};
use crossbeam_epoch::Guard;

use super::{cache::CacheState, data::FlushData, flush::Flush, load::Mapping};
use crate::map::cache::NodeCache;
use crate::map::table::PageMap;
use crate::types::refbox::{BoxRef, ILoader};
use crate::utils::lru::Lru;
use crate::utils::unpack_id;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

struct Pool {
    flush: Flush,
    sem: Arc<Countblock>,
    buf: Vec<Handle<Arena>>,
    free: AtomicUsize,
    seal: AtomicUsize,
    cur: AtomicPtr<Arena>,
    meta: Arc<Meta>,
    flsn: AtomicU32,
}

unsafe impl Sync for Pool {}
unsafe impl Send for Pool {}

impl Pool {
    fn new_arena(cap: u32, idx: usize) -> *mut Arena {
        let x = Box::new(Arena::new(cap, idx));
        Box::into_raw(x)
    }

    fn new(
        opt: Arc<ParsedOptions>,
        sem: Arc<Countblock>,
        mapping: Handle<Mapping>,
        meta: Arc<Meta>,
    ) -> Result<Self, OpCode> {
        let cap = opt.arena_count as usize;
        let mut buf: Vec<Handle<Arena>> = Vec::with_capacity(cap);

        for i in 0..cap {
            let a = Self::new_arena(opt.arena_size, i);
            buf.push(a.into());
        }
        let h = buf[0];
        let free = AtomicUsize::new(1);

        let this = Self {
            flush: Flush::new(mapping),
            sem,
            buf,
            free,
            seal: AtomicUsize::new(0),
            flsn: AtomicU32::new(INIT_ORACLE as u32),
            cur: AtomicPtr::new(h.inner()),
            meta,
        };

        let id = this.gen_id()?;
        h.set_state(Arena::FLUSH, Arena::HOT);
        h.reset(id, this.flsn.load(Relaxed));
        Ok(this)
    }

    fn load(&self, addr: u64) -> Option<BoxRef> {
        let (id, off) = unpack_id(addr);
        let cur = self.current();
        let cur_id = cur.id();

        if cur_id == id {
            return Some(cur.load(off));
        }

        let idx = cur.idx as usize;
        let n = self.buf.len();
        let mut i = (n + idx - 1) % n;
        // NOTE: the logical id of arena may not be contiguous (because of GC)
        while i != idx {
            let h = self.buf[i];
            h.acquire();
            if !matches!(h.state(), Arena::WARM | Arena::COLD) {
                h.release();
                break;
            }
            if h.id() == id {
                h.release();
                return Some(h.load(off));
            }
            h.release();
            i = (n + i - 1) % n;
        }
        None
    }

    fn try_flush(&self) {
        let idx = self.seal.load(Relaxed);
        let h = self.buf[idx];

        let ready = if h.is_dirty() {
            // there're write txns, we must wait until log flushed, namely pool's flsn > arena's
            let r = self.flsn.load(Relaxed);
            let l = h.flsn.load(Relaxed);
            r.wrapping_sub(l) as i32 > 0
        } else {
            // no write txns
            h.state() == Arena::WARM
        };

        if ready && h.unref() && h.set_state(Arena::WARM, Arena::COLD) == Arena::WARM {
            let next = (idx + 1) % self.buf.len();
            self.seal.store(next, Relaxed);
            self.flush(h);
        }
    }

    fn update_flsn(&self) {
        self.flsn.fetch_add(1, Release);
        self.try_flush();
    }

    fn flush(&self, h: Handle<Arena>) {
        assert_eq!(h.state(), Arena::COLD);

        let id = h.id();
        assert!(id > INVALID_ID);
        let sem = self.sem.clone();

        let cb = move || {
            let x = h.set_state(Arena::COLD, Arena::FLUSH);
            assert_eq!(x, Arena::COLD);
            sem.post();
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

    fn current(&self) -> Handle<Arena> {
        self.cur.load(Relaxed).into()
    }

    // actually, concunrrent install will not happen, since only one thread can successfully change
    // state to WARM
    fn install_new(&self, cur: Handle<Arena>) -> Result<(), OpCode> {
        let idx = self.free.load(Acquire);
        let p = self.buf[idx];
        let next = (idx + 1) % self.buf.len();
        let id = self.gen_id()?;

        log::trace!("swap arena {idx} => {next}");
        while !p.balanced() || p.set_state(Arena::FLUSH, Arena::HOT) != Arena::FLUSH {
            self.try_flush();
        }
        p.reset(id, self.flsn.load(Relaxed));
        self.free.store(next, Release); // release ordering is required

        self.cur
            .compare_exchange(cur.inner(), p.inner(), Relaxed, Relaxed)
            .expect("never happen");
        Ok(())
    }

    fn gen_id(&self) -> Result<u32, OpCode> {
        let id = self.meta.next_data.fetch_add(1, Relaxed);

        // TODO: deal with id exhaustion
        if id == NULL_ID {
            return Err(OpCode::DbFull);
        }
        Ok(id)
    }

    fn flush_all(&self) {
        let mut cnt = 0;
        let mut idx = self.seal.load(Relaxed);
        let n = self.buf.len();
        let end = (n + idx - 1) % n;

        while idx != end {
            let h = self.buf[idx];
            if matches!(h.state(), Arena::HOT | Arena::WARM) {
                h.state.store(Arena::COLD, Relaxed);
                self.flush(h);
            }
            idx = (idx + 1) % n;
        }

        while cnt != n {
            cnt = 0;
            for h in &self.buf {
                if matches!(h.state(), Arena::FLUSH) {
                    cnt += 1;
                }
            }
        }
    }

    fn quit(&self) {
        self.flush_all();
        self.flush.quit();
        self.buf
            .iter()
            .map(|h| {
                h.clear();
                unsafe { drop(Box::from_raw(h.inner())) };
            })
            .count();
    }
}

pub struct Buffers {
    max_inline_size: u32,
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
        sem: Arc<Countblock>,
        meta: Arc<Meta>,
        mapping: Mapping,
    ) -> Result<Self, OpCode> {
        let mapping = Handle::new(mapping);
        Ok(Self {
            max_inline_size: opt.max_inline_size,
            cache: NodeCache::new(opt.cache_capacity, opt.cache_evict_pct),
            table: pagemap,
            remote: Handle::new(Lru::new(opt.cache_count)),
            pool: Handle::new(Pool::new(opt.clone(), sem, mapping, meta)?),
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
            pool: self.pool,
            mapping: self.mapping,
            cache: self.remote,
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

#[derive(Clone)]
pub struct Loader {
    max_inline_size: u32,
    pool: Handle<Pool>,
    mapping: Handle<Mapping>,
    cache: Handle<Lru<BoxRef>>,
}

impl ILoader for Loader {
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

impl IInlineSize for Loader {
    fn inline_size(&self) -> u32 {
        self.max_inline_size
    }
}
