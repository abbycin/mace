use std::{
    cmp::max,
    fmt::Debug,
    ops::Deref,
    sync::{
        atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicUsize},
        Arc,
    },
};

use crate::{
    map::data::FrameFlag,
    utils::{data::Meta, lru::Lru, raw_ptr_to_ref, NEXT_ID, NULL_ID, NULL_ORACLE},
    OpCode,
};

use super::{
    data::{ArenaIter, FlushData, Frame, FrameOwner, FrameRef},
    flush::Flush,
    load::FileReader,
};
use crate::map::cache::Cache;
use crate::utils::bytes::ByteArray;
use crate::utils::options::Options;
use crate::utils::{pack_id, unpack_id};
use std::sync::atomic::{
    AtomicU16,
    Ordering::{AcqRel, Acquire, Relaxed, Release},
};
use std::sync::RwLock;

pub(crate) struct Arena {
    raw: ByteArray,
    idx: u16,
    state: AtomicU16,
    refcnt: AtomicU32,
    stable: AtomicU32,
    offset: AtomicU32,
    /// indicate that whether the arena is in current round
    tick: Arc<AtomicU64>,
    flsn: AtomicU64,
    file_id: AtomicU64,
    borrow_cnt: AtomicU64,
}

impl Arena {
    /// memory can be allocated
    const HOT: u16 = 4;
    /// memory no longer available for allocating
    const WARM: u16 = 3;
    /// waiting for flush
    const COLD: u16 = 2;
    /// flushed to disk
    const FLUSH: u16 = 1;

    fn new(cap: u32, idx: usize) -> Self {
        Self {
            raw: ByteArray::alloc(cap as usize),
            state: AtomicU16::new(Self::FLUSH),
            refcnt: AtomicU32::new(0),
            stable: AtomicU32::new(0),
            offset: AtomicU32::new(0),
            tick: Arc::new(AtomicU64::new(0)),
            flsn: AtomicU64::new(0),
            idx: idx as u16,
            file_id: AtomicU64::new(0),
            borrow_cnt: AtomicU64::new(0),
        }
    }

    fn reset(&self, id: u16, off: u64) {
        self.tick.fetch_add(1, Relaxed);
        self.set_file_id(pack_id(id, off));
        self.flsn.store(0, Relaxed);
        assert_eq!(self.state(), Self::HOT);
        self.stable.store(0, Relaxed);
        self.offset.store(0, Relaxed);
    }

    fn iter(&self) -> ArenaIter {
        ArenaIter::new(self.raw, self.stable.load(Relaxed))
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
        let frame = unsafe { &mut *self.load_impl(off) };
        let (id, beg) = unpack_id(self.file_id());
        let addr = pack_id(id, beg + off as u64);

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

    fn update_stable(&self, off: u64) {
        let arena_off = off - unpack_id(self.file_id()).1;
        self.stable.store(arena_off as u32, Relaxed);
    }

    /// CAS is only necessary when it was in `release_buffer`
    fn update_flsn(&self, flsn: u64) {
        let mut old = self.flsn.load(Relaxed);
        let new = max(old, flsn);
        loop {
            match self.flsn.compare_exchange(old, new, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(e) if e < new => {
                    old = e;
                }
                Err(_) => break,
            }
        }
    }

    fn used(&self) -> ArenaIter {
        ArenaIter::new(self.raw, self.offset.load(Relaxed))
    }

    fn load_impl(&self, off: u32) -> *mut Frame {
        unsafe { self.raw.data().add(off as usize).cast::<Frame>() }
    }

    fn load(&self, off: u64) -> *mut Frame {
        let beg = unpack_id(self.file_id()).1;
        self.load_impl((off - beg) as u32)
    }

    fn set_state(&self, cur: u16, new: u16) -> u16 {
        self.state
            .compare_exchange(cur, new, AcqRel, Acquire)
            .unwrap_or_else(|x| x)
    }

    fn state(&self) -> u16 {
        self.state.load(Relaxed)
    }

    fn refs(&self) -> u32 {
        self.refcnt.load(Relaxed)
    }

    fn inc_ref(&self) {
        self.refcnt.fetch_add(1, Release);
    }

    fn dec_ref(&self) {
        let x = self.refcnt.fetch_sub(1, Relaxed);
        assert!(x > 0);
    }

    fn borrowed(&self) {
        self.borrow_cnt.fetch_add(1, Relaxed);
    }

    fn returned(&self) {
        self.borrow_cnt.fetch_sub(1, Relaxed);
    }

    fn balanced(&self) -> bool {
        self.borrow_cnt.load(Relaxed) == 0
    }

    fn tick(&self) -> Arc<AtomicU64> {
        self.tick.clone()
    }

    fn file_id(&self) -> u64 {
        self.file_id.load(Relaxed)
    }

    fn set_file_id(&self, x: u64) {
        self.file_id.store(x, Relaxed);
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
            .field("stable", &self.stable)
            .field("offset", &self.offset)
            .field("tick", &self.tick)
            .field("flsn", &unpack_id(self.flsn.load(Relaxed)))
            .field("fild_id", &unpack_id(self.file_id.load(Relaxed)))
            .field("borrow_cnt", &self.borrow_cnt)
            .finish()
    }
}

struct Pool {
    files: FileMap,
    flush: Flush,
    buf: Vec<Handle>,
    free: AtomicUsize,
    seal: AtomicUsize,
    flsn: AtomicU64,
    cur: AtomicPtr<Arena>,
    buf_size: u64,
    max_file_size: u64,
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

    fn new(opt: Arc<Options>, files: FileMap, meta: Arc<Meta>) -> Result<Self, OpCode> {
        let cap = opt.buffer_count as usize;
        let mut buf = Vec::with_capacity(cap);

        for i in 0..cap {
            let a = Self::new_arena(opt.buffer_size, i);
            buf.push(a.into());
        }
        let h = buf[0];
        let free = AtomicUsize::new(1);

        let this = Self {
            files,
            flush: Flush::new(opt.clone()),
            buf,
            free,
            seal: AtomicUsize::new(0),
            flsn: AtomicU64::new(NULL_ORACLE),
            cur: AtomicPtr::new(h.raw),
            buf_size: opt.buffer_size as u64,
            max_file_size: opt.data_file_size as u64,
            meta,
        };

        let (id, off) = this.next()?;
        this.update_next(id, off);
        h.set_state(Arena::FLUSH, Arena::HOT);
        h.reset(id, off);
        Ok(this)
    }

    // we can update flsn either here or where arena was cooled to WARM
    fn release(&self, idx: usize, off: u64) {
        let h = self.buf.get(idx).expect("index out of range");
        assert!(matches!(h.state(), Arena::HOT | Arena::WARM));
        h.dec_ref();
        h.update_stable(off);
        self.try_flush();
    }

    fn stabilize(&self, f: Box<dyn Fn(u64)>) {
        let cur = self.current();
        let tick = cur.tick();
        let iter = if matches!(cur.state(), Arena::HOT | Arena::WARM) {
            cur.iter()
        } else {
            ArenaIter::default()
        };

        let data = FlushData::new(true, unpack_id(cur.file_id()).0, tick, iter, f);
        self.flush
            .tx
            .send(data)
            .inspect_err(|e| {
                log::error!("can't stabilize arena {}", e);
            })
            .unwrap();
    }

    fn should_stabilize(&self) -> bool {
        self.try_flush();
        let seal = self.seal.load(Relaxed);

        // the previous arena is COLD or FLUSH
        seal == self.current().idx as usize
    }

    fn load_f<F>(&self, id: u16, off: u64, mut cb: F) -> Option<FrameOwner>
    where
        F: FnMut(FrameOwner) -> FrameOwner,
    {
        let file_id = pack_id(id, off / self.buf_size * self.buf_size);
        let cur = self.current();
        if cur.file_id() == file_id {
            return Some(FrameOwner::from(cur.load(off)));
        }

        let idx = cur.idx as usize;
        let n = self.buf.len();
        let mut i = (n + idx - 1) % n;
        while i != idx {
            let h = self.buf[i];
            h.borrowed();
            if !matches!(h.state(), Arena::WARM | Arena::COLD) {
                h.returned();
                break;
            }
            if h.file_id() == file_id {
                let r = cb(FrameOwner::from(h.load(off)));
                h.returned();
                return Some(r);
            }
            h.returned();
            i = (n + i - 1) % n;
        }
        None
    }

    fn try_flush(&self) {
        let idx = self.seal.load(Relaxed);
        let h = self.buf[idx];
        let (oid, opos) = unpack_id(h.flsn.load(Relaxed));
        let flsn = self.flsn.load(Relaxed);
        let (cid, cpos) = unpack_id(flsn);

        if cid < NEXT_ID || oid < NEXT_ID {
            return;
        }

        let ready = if flsn == NULL_ORACLE {
            true
        } else {
            // NOTE: we can limit the length of the TXN so that it won't span all wal files
            if cid < oid {
                true // log was stabilized and wrapped
            } else {
                (cid > oid) || cpos > opos // log was stabilized
            }
        };

        if ready && h.refs() == 0 && h.set_state(Arena::WARM, Arena::COLD) == Arena::WARM {
            let next = (idx + 1) % self.buf.len();
            self.seal.store(next, Relaxed);
            self.flush(h);
        }
    }

    fn update_flsn(&self, flsn: u64) {
        self.flsn.store(flsn, Release);
        self.try_flush();
    }

    fn flush(&self, h: Handle) {
        assert_eq!(h.state(), Arena::COLD);

        let file_id = h.file_id();
        let (id, _) = unpack_id(file_id);
        let meta = self.meta.clone();
        assert!(id > 0);
        let files = self.files.clone();
        let cb = move |pos| {
            let x = h.set_state(Arena::COLD, Arena::FLUSH);
            assert_eq!(x, Arena::COLD);
            let lk = files.write().expect("can't lock write");
            if let Some(f) = lk.get_mut(&id) {
                f.load();
            }
            log::trace!(
                "flushed {:?} current {:?}",
                (id, pos),
                unpack_id(meta.next_data.load(Relaxed))
            );
            meta.flushed.store(pack_id(id, pos), Relaxed);
        };

        let _ = self
            .flush
            .tx
            .send(FlushData::new(false, id, h.tick(), h.used(), Box::new(cb)))
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
        let (id, off) = self.next()?;

        log::debug!("swap arena {} => {}", idx, next);
        while !p.balanced() || p.set_state(Arena::FLUSH, Arena::HOT) != Arena::FLUSH {
            std::hint::spin_loop();
        }
        p.reset(id, off);
        self.free.store(next, Release); // release ordering is required

        match self.cur.compare_exchange(cur.raw, p.raw, Relaxed, Relaxed) {
            Ok(_) => self.update_next(id, off),
            Err(_) => unreachable!("never happen"),
        }
        Ok(())
    }

    fn next(&self) -> Result<(u16, u64), OpCode> {
        let (mut id, mut off) = unpack_id(self.meta.next_data.load(Relaxed));

        if off >= self.max_file_size {
            off = 0;
            if id == NULL_ID {
                id = NEXT_ID;
            } else {
                id += 1;
            }
            if id == self.meta.oldest_file() {
                return Err(OpCode::DbFull);
            }
        }
        Ok((id, off))
    }

    fn update_next(&self, id: u16, off: u64) {
        self.meta.update_file(id, off + self.buf_size);
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

    fn start(&self) {
        self.flsn.store(0, Relaxed);
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

type FileMap = Arc<RwLock<Lru<u16, FileReader>>>;

pub struct Buffers {
    opt: Arc<Options>,
    pool: Pool,
    cache: Cache,
    files: FileMap,
}

impl Buffers {
    const MAX_CACHED_META: usize = 16;
    pub fn new(opt: Arc<Options>, meta: Arc<Meta>) -> Result<Self, OpCode> {
        let files = Arc::new(RwLock::new(Lru::new(Self::MAX_CACHED_META)));
        Ok(Self {
            opt: opt.clone(),
            pool: Pool::new(opt.clone(), files.clone(), meta)?,
            cache: Cache::new(&opt),
            files,
        })
    }

    /// NOTE: mutable buffer will never be cached, the result is the raw data with frame header
    pub fn alloc(&self, size: u32) -> Result<(usize, FrameRef), OpCode> {
        loop {
            let a = self.pool.current();
            match a.alloc(size) {
                Ok(x) => return Ok(x),
                Err(e @ OpCode::TooLarge) => return Err(e),
                Err(OpCode::Again) => continue,
                Err(OpCode::NeedMore) => {
                    if a.set_state(Arena::HOT, Arena::WARM) == Arena::HOT {
                        a.update_flsn(self.pool.flsn.load(Relaxed));
                        self.pool.install_new(a)?;
                    }
                }
                _ => unreachable!("invalid opcode"),
            }
        }
    }

    pub fn start(&self) {
        self.pool.start();
    }

    pub fn quit(&self) {
        self.pool.quit()
    }

    pub fn release_buffer(&self, buffer_id: usize, off: u64) {
        self.pool.release(buffer_id, off);
    }

    pub fn should_stabilize(&self) -> bool {
        self.pool.should_stabilize()
    }

    pub fn stabilize(&self, f: Box<dyn Fn(u64)>) {
        self.pool.stabilize(f);
    }

    pub fn update_flsn(&self, flsn: u64) {
        debug_assert_ne!(flsn, 0);
        self.pool.update_flsn(flsn);
    }

    fn cache(&self, src: FrameOwner) -> FrameOwner {
        let addr = src.addr();
        let dst = FrameOwner::alloc(src.payload_size() as usize);
        debug_assert_eq!(src.size(), dst.size());
        src.copy_to(&dst);
        self.cache.put(addr, dst.clone()).expect("can't is full");
        dst
    }

    pub fn load(&self, addr: u64) -> FrameOwner {
        if let Some(x) = self.cache.get(addr) {
            return x;
        }

        let (id, off) = unpack_id(addr);

        assert!(id > 0);

        // lookup in current or sealed arena
        if let Some(f) = self.pool.load_f(id, off, |f| self.cache(f)) {
            return f;
        }

        loop {
            if let Some(f) = self.load_from_file(id, off) {
                if let Err(e) = self.cache.put(addr, f.clone()) {
                    log::warn!("cache is full, {:?}", e);
                }
                return f;
            }
        }
    }

    fn load_from_file(&self, id: u16, off: u64) -> Option<FrameOwner> {
        let lk = self.files.read().expect("can't lock read");
        if let Some(r) = lk.get(&id) {
            return r.read_at(off);
        }
        drop(lk);

        let mut lk = self.files.try_write().ok()?;
        let path = self.opt.data_file(id);
        let r = FileReader::new(path)?;
        let f = r.read_at(off);
        lk.add(id, r);
        f
    }
}
