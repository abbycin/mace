use std::{
    cell::Cell,
    fmt::Debug,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicU8, AtomicU32, AtomicU64, AtomicUsize},
    },
};

use crate::{
    OpCode,
    map::data::FrameFlag,
    static_assert,
    utils::{INIT_ORACLE, INVALID_ID, NULL_ID, countblock::Countblock, data::Meta, raw_ptr_to_ref},
};

use super::{
    data::{ArenaIter, FlushData, Frame, FrameOwner, FrameRef},
    flush::Flush,
    load::Mapping,
};
use crate::map::cache::Cache;
use crate::utils::bytes::ByteArray;
use crate::utils::options::Options;
use crate::utils::{pack_id, unpack_id};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

// use C repr to fix the layout
#[repr(C)]
pub(crate) struct Arena {
    raw: ByteArray,
    idx: u16,
    state: AtomicU8,
    dirty: AtomicU8,
    refcnt: AtomicU32,
    offset: AtomicU32,
    flsn: AtomicU64,
    id: Cell<u32>,
    borrow_cnt: AtomicU32,
    padding: [u8; 16],
}

static_assert!(size_of::<Arena>() == 64);

impl Arena {
    /// memory can be allocated
    const HOT: u8 = 4;
    /// memory no longer available for allocating
    const WARM: u8 = 3;
    /// waiting for flush
    const COLD: u8 = 2;
    /// flushed to disk
    const FLUSH: u8 = 1;

    const FRESH: u8 = 0;
    const STALE: u8 = 1;
    const DIRTY: u8 = 2;

    fn new(cap: u32, idx: usize) -> Self {
        Self {
            raw: ByteArray::alloc(cap as usize),
            state: AtomicU8::new(Self::FLUSH),
            dirty: AtomicU8::new(Self::FRESH),
            refcnt: AtomicU32::new(0),
            offset: AtomicU32::new(0),
            flsn: AtomicU64::new(0),
            idx: idx as u16,
            id: Cell::new(INVALID_ID),
            borrow_cnt: AtomicU32::new(0),
            padding: const { [0u8; 16] },
        }
    }

    fn reset(&self, id: u32, flsn: u64) {
        self.id.set(id);
        self.flsn.store(flsn, Relaxed);
        self.dirty.store(Self::FRESH, Relaxed);
        assert_eq!(self.state(), Self::HOT);
        self.offset.store(0, Relaxed);
    }

    fn alloc_size(&self, size: u32) -> Result<u32, OpCode> {
        let cap = self.raw.len() as u32;
        if size > cap {
            return Err(OpCode::TooLarge);
        }

        let mut cur = self.offset.load(Relaxed);
        loop {
            // it's possible that other thread change the state to WARM
            if self.state() != Self::HOT {
                return Err(OpCode::Again);
            }

            let new = cur + size;
            if new > cap {
                return Err(OpCode::NeedMore);
            }

            match self.offset.compare_exchange(cur, new, AcqRel, Acquire) {
                Ok(_) => return Ok(cur),
                Err(e) => cur = e,
            }
        }
    }

    fn alloc_at(&self, off: u32, size: u32) -> (usize, FrameRef) {
        let frame = unsafe { &mut *self.load(off) };
        let addr = pack_id(self.id.get(), off);

        frame.init(&self.refcnt, addr, FrameFlag::Unknown);
        frame.set_size(size);
        (self.idx as usize, FrameRef::new(frame as *mut Frame))
    }

    // NOTE: it's possible that concurrent allocation may paritally fail, such as: a thread request
    // a big chunk while the rest space is not enough but enough for othet threads, the failed one
    // will request to switch arena while other threads are remain using it
    // we increase the ref count before allocating, so that current arena won't be flushed
    pub fn alloc(&self, size: u32) -> Result<(usize, FrameRef), OpCode> {
        self.inc_ref(); // use release ordering
        let real_size = Frame::alloc_size(size); // avoid UB in pointer type cast
        let offset = self.alloc_size(real_size).inspect_err(|_| {
            self.dec_ref();
        })?;
        Ok(self.alloc_at(offset, size))
    }

    fn used(&self) -> ArenaIter {
        ArenaIter::new(self.raw, self.offset.load(Relaxed))
    }

    #[inline]
    fn load(&self, off: u32) -> *mut Frame {
        unsafe { self.raw.data().add(off as usize).cast::<Frame>() }
    }

    fn set_state(&self, cur: u8, new: u8) -> u8 {
        // we don't care if it's success, what we want is the `dirty` flag either be STALE or DIRTY
        self.set_dirty(Self::FRESH, Self::STALE);
        self.state
            .compare_exchange(cur, new, AcqRel, Acquire)
            .unwrap_or_else(|x| x)
    }

    #[inline]
    fn state(&self) -> u8 {
        self.state.load(Relaxed)
    }

    #[inline(always)]
    fn set_dirty(&self, cur: u8, new: u8) {
        let _ = self.dirty.compare_exchange(cur, new, Relaxed, Relaxed);
    }

    fn mark_dirty(&self) {
        self.set_dirty(Self::FRESH, Self::DIRTY);
    }

    #[inline]
    fn is_dirty(&self) -> bool {
        self.dirty.load(Relaxed) == Self::DIRTY
    }

    fn refs(&self) -> u32 {
        self.refcnt.load(Relaxed)
    }

    #[inline]
    fn inc_ref(&self) {
        self.refcnt.fetch_add(1, Release);
    }

    #[inline]
    fn dec_ref(&self) {
        let x = self.refcnt.fetch_sub(1, Relaxed);
        assert!(x > 0);
    }

    fn inc_borrow(&self) {
        self.borrow_cnt.fetch_add(1, Relaxed);
    }

    fn dec_borrow(&self) {
        self.borrow_cnt.fetch_sub(1, Relaxed);
    }

    #[inline]
    fn balanced(&self) -> bool {
        self.borrow_cnt.load(Relaxed) == 0
    }

    #[inline(always)]
    fn id(&self) -> u32 {
        self.id.get()
    }
}

fn free_arena(a: *mut Arena) {
    unsafe {
        ByteArray::free((*a).raw);
        drop(Box::from_raw(a));
    }
}

impl Debug for Arena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Arena")
            .field("raw ", &self.raw.data())
            .field("idx", &self.idx)
            .field("state", &self.state)
            .field("refcnt", &self.refcnt)
            .field("offset", &self.offset)
            .field("flsn", &unpack_id(self.flsn.load(Relaxed)))
            .field("id", &self.id.get())
            .field("borrow_cnt", &self.borrow_cnt)
            .finish()
    }
}

struct Pool {
    flush: Flush,
    sem: Arc<Countblock>,
    buf: Vec<Handle>,
    free: AtomicUsize,
    seal: AtomicUsize,
    flsn: AtomicU64,
    cur: AtomicPtr<Arena>,
    meta: Arc<Meta>,
}

unsafe impl Sync for Pool {}
unsafe impl Send for Pool {}

#[derive(Clone, Copy)]
struct Handle {
    raw: *mut Arena,
}

unsafe impl Sync for Handle {}
unsafe impl Send for Handle {}

impl Deref for Handle {
    type Target = Arena;

    fn deref(&self) -> &Self::Target {
        raw_ptr_to_ref(self.raw)
    }
}

impl From<*mut Arena> for Handle {
    fn from(value: *mut Arena) -> Self {
        Self { raw: value }
    }
}

impl Pool {
    fn new_arena(cap: u32, idx: usize) -> *mut Arena {
        let x = Box::new(Arena::new(cap, idx));
        Box::into_raw(x)
    }

    fn new(
        opt: Arc<Options>,
        sem: Arc<Countblock>,
        mapping: Arc<Mapping>,
        meta: Arc<Meta>,
    ) -> Result<Self, OpCode> {
        let cap = opt.buffer_count as usize;
        let mut buf = Vec::with_capacity(cap);

        for i in 0..cap {
            let a = Self::new_arena(opt.buffer_size, i);
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
            flsn: AtomicU64::new(INIT_ORACLE),
            cur: AtomicPtr::new(h.raw),
            meta,
        };

        let id = this.gen_id()?;
        h.set_state(Arena::FLUSH, Arena::HOT);
        h.reset(id, this.flsn.load(Relaxed));
        Ok(this)
    }

    fn release(&self, idx: usize) {
        let h = self.buf.get(idx).expect("index out of range");
        assert!(matches!(h.state(), Arena::HOT | Arena::WARM));
        h.dec_ref();
        self.try_flush();
    }

    fn load_f<F>(&self, id: u32, off: u32, mut cb: F) -> Option<FrameOwner>
    where
        F: FnMut(FrameOwner) -> FrameOwner,
    {
        let cur = self.current();
        let cur_id = cur.id();

        if cur_id == id {
            return Some(FrameOwner::from(cur.load(off)));
        }

        let idx = cur.idx as usize;
        let n = self.buf.len();
        let mut i = (n + idx - 1) % n;
        // NOTE: the logical id of arena may not be contiguous (because of GC)
        while i != idx {
            let h = self.buf[i];
            h.inc_borrow();
            if !matches!(h.state(), Arena::WARM | Arena::COLD) {
                h.dec_borrow();
                break;
            }
            if h.id() == id {
                let r = cb(FrameOwner::from(h.load(off)));
                h.dec_borrow();
                return Some(r);
            }
            h.dec_borrow();
            i = (n + i - 1) % n;
        }
        None
    }

    fn try_flush(&self) {
        let idx = self.seal.load(Relaxed);
        let h = self.buf[idx];

        let ready = if h.is_dirty() {
            let r = self.flsn.load(Relaxed);
            let l = h.flsn.load(Relaxed);
            r > l
        } else {
            h.state() == Arena::WARM
        };

        if ready && h.refs() == 0 && h.set_state(Arena::WARM, Arena::COLD) == Arena::WARM {
            let next = (idx + 1) % self.buf.len();
            self.seal.store(next, Relaxed);
            self.flush(h);
        }
    }

    fn update_flsn(&self) {
        self.flsn.fetch_add(1, Release);
        self.try_flush();
    }

    fn flush(&self, h: Handle) {
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
                log::error!("can't send flush data {:?}", x);
                std::process::abort();
            });
    }

    fn current(&self) -> Handle {
        self.cur.load(Relaxed).into()
    }

    // actually, concunrrent install will not happen, since only one thread can successfully change
    // state to WARM
    fn install_new(&self, cur: Handle) -> Result<(), OpCode> {
        let idx = self.free.load(Acquire);
        let p = self.buf[idx];
        let next = (idx + 1) % self.buf.len();
        let id = self.gen_id()?;

        log::debug!("swap arena {} => {}", idx, next);
        while !p.balanced() || p.set_state(Arena::FLUSH, Arena::HOT) != Arena::FLUSH {
            self.try_flush();
        }
        p.reset(id, self.flsn.load(Relaxed));
        self.free.store(next, Release); // release ordering is required

        self.cur
            .compare_exchange(cur.raw, p.raw, Relaxed, Relaxed)
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
                h.refcnt.store(0, Relaxed);
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
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.buf.iter().map(|x| free_arena(x.raw)).count();
    }
}

pub struct Buffers {
    cache: Cache<FrameOwner>,
    pool: Pool,
    pub mapping: Arc<Mapping>,
}

impl Buffers {
    pub fn new(
        opt: Arc<Options>,
        sem: Arc<Countblock>,
        meta: Arc<Meta>,
        mapping: Mapping,
    ) -> Result<Self, OpCode> {
        let mapping = Arc::new(mapping);
        Ok(Self {
            cache: Cache::new(&opt),
            pool: Pool::new(opt, sem, mapping.clone(), meta)?,
            mapping,
        })
    }

    pub fn alloc(&self, size: u32) -> Result<(usize, FrameRef), OpCode> {
        loop {
            let a = self.pool.current();
            match a.alloc(size) {
                Ok(x) => return Ok(x),
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

    pub fn quit(&self) {
        self.pool.quit()
    }

    #[inline]
    pub fn release_buffer(&self, buffer_id: usize) {
        self.pool.release(buffer_id);
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

    #[inline]
    fn cache(&self, src: FrameOwner) -> FrameOwner {
        let addr = src.addr();
        self.cache.put(addr, src).expect("can't is full")
    }

    pub fn load(&self, addr: u64) -> FrameOwner {
        let (id, off) = unpack_id(addr);

        assert!(id > 0);

        // lookup in current or sealed arena
        if let Some(f) = self.pool.load_f(id, off, |f| self.cache(f)) {
            return f;
        }

        if let Some(x) = self.cache.get(addr) {
            return x;
        }

        loop {
            if let Some(f) = self.mapping.load(addr) {
                if let Err(e) = self.cache.put(addr, f.clone()) {
                    log::warn!("cache is full, {:?}", e);
                }
                return f;
            }
        }
    }
}
