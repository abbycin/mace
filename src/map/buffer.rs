use std::{
    alloc::alloc_zeroed,
    cell::Cell,
    ptr::null_mut,
    sync::{
        atomic::{AtomicPtr, AtomicU32},
        Arc, Mutex,
    },
};

use crate::{
    map::data::FrameFlag,
    static_assert,
    utils::{next_power_of_2, queue::Queue, raw_ptr_to_ref, NEXT_ID},
    OpCode,
};

use super::{
    data::{FlushData, Frame, FrameOwner},
    flush::Flush,
    load::FileReader,
};
use crate::map::cache::Cache;
use crate::utils::byte_array::ByteArray;
use crate::utils::options::Options;
use crate::utils::{decode_u64, encode_u64};
use std::alloc::{dealloc, Layout};
use std::collections::HashMap;
use std::sync::atomic::{
    AtomicU16,
    Ordering::{AcqRel, Acquire, Relaxed},
};
use std::sync::RwLock;

struct Arena {
    raw: ByteArray,
    /// new, sealed, free, flushed
    state: AtomicU16,
    refcnt: AtomicU16,
    offset: AtomicU32,
    /// page id
    id: Cell<u32>,
}

static_assert!(size_of::<Arena>() == 32);

impl Arena {
    const STATE_FREE: u16 = 1;
    const STATE_SEALED: u16 = 2;
    const STATE_FLUSHED: u16 = 3;

    fn new(id: u32, cap: u32) -> Self {
        Self {
            raw: ByteArray::alloc(cap as usize),
            state: AtomicU16::new(Self::STATE_FREE),
            refcnt: AtomicU16::new(0),
            offset: AtomicU32::new(0),
            id: Cell::new(id),
        }
    }

    fn reset(&self, id: u32) {
        self.id.set(id);
        self.state.store(Self::STATE_FREE, Relaxed);
        self.refcnt.store(0, Relaxed);
        self.offset.store(0, Relaxed);
    }

    fn alloc_size(&self, size: u32, is_new: bool) -> Result<u32, OpCode> {
        let cap = self.raw.len() as u32;
        let mut cur = self.offset.load(Relaxed);

        loop {
            if self.is_sealed() {
                return Err(OpCode::Again);
            }

            if is_new {
                self.inc_ref();
            }

            if size > cap || cur + size > cap {
                return Err(OpCode::NeedMore);
            }
            let new = cur + size;

            match self.offset.compare_exchange(cur, new, AcqRel, Acquire) {
                Ok(_) => return Ok(cur), // return old offset which is start address
                Err(e) => cur = e,
            }
        }
    }

    fn to(&self, off: u32) -> &mut Frame {
        unsafe { &mut *self.load(off) }
    }

    fn alloc_at(&self, off: u32, size: u32) -> Result<(u64, u32, FrameOwner), OpCode> {
        let frame = self.to(off);
        let addr = encode_u64(self.id.get(), off);

        frame.init(addr, FrameFlag::Normal);
        frame.set_size(size);
        Ok((addr, self.id.get(), FrameOwner::from(frame as *mut Frame)))
    }

    /// return in buffer offset and memeory address, `is_new` indicate if the allocation is in the
    /// same system transaction
    pub fn alloc(&self, size: u32, is_new: bool) -> Result<(u64, u32, FrameOwner), OpCode> {
        // avoiding UB in pointer type cast
        let real_size = Frame::alloc_size(size);
        let offset = self.alloc_size(real_size, is_new)?;
        self.alloc_at(offset, size)
    }

    fn used(&self) -> ByteArray {
        self.raw.sub_array(0..self.offset.load(Relaxed) as usize)
    }

    fn load(&self, off: u32) -> *mut Frame {
        unsafe { self.raw.data().add(off as usize).cast::<Frame>() }
    }

    fn is_sealed(&self) -> bool {
        self.state.load(Relaxed) == Self::STATE_SEALED
    }

    fn mark_sealed(&self) {
        self.state.store(Self::STATE_SEALED, Relaxed);
    }

    fn is_flushed(&self) -> bool {
        self.state.load(Relaxed) == Self::STATE_FLUSHED
    }

    fn mark_flushed(&self) {
        self.state.store(Self::STATE_FLUSHED, Relaxed);
    }

    fn is_flushable(&self) -> bool {
        self.is_sealed() && self.refs() == 0
    }

    fn mark_flushable(&self) {
        self.refcnt.store(0, Relaxed);
        self.mark_sealed();
    }

    fn refs(&self) -> u16 {
        self.refcnt.load(Relaxed)
    }

    #[inline(always)]
    fn inc_ref(&self) {
        self.refcnt.fetch_add(1, Relaxed);
    }

    fn dec_ref(&self) {
        let x = self.refcnt.fetch_sub(1, Relaxed);
        assert!(x > 0);
    }

    fn destroy(&self) {
        ByteArray::free(self.raw);
    }
}

unsafe impl Send for Arena {}

type FileMap = Arc<RwLock<HashMap<u32, FileReader>>>;

struct Pool {
    bufs: RwLock<HashMap<u32, *mut Arena>>,
    flush: Flush,
    freelist: Queue<*mut Arena>,
    junks: Queue<*mut Arena>,
    cur: AtomicPtr<Arena>,
    cap: u32,
    cnt: Mutex<u32>,
    buf_size: u32,
    next_id: AtomicU32,
    files: FileMap,
    opt: Arc<Options>,
}

fn free_arena(a: *mut Arena) {
    unsafe {
        (*a).destroy();
        dealloc(a as *mut u8, Layout::new::<Arena>());
    }
}

impl Pool {
    fn find_next_id(opt: &Arc<Options>) -> Result<u32, OpCode> {
        let mut next_id = 0;
        let dir = std::fs::read_dir(&opt.db_path).map_err(|_| OpCode::IoError)?;

        for i in dir {
            let f = i.map_err(|_| OpCode::IoError)?.file_name();
            let name = f.to_str().expect("invalid encode");
            if !name.starts_with(Options::PAGE_PREFIX) {
                continue;
            }
            let v: Vec<&str> = name.split(Options::PAGE_PREFIX).collect();
            let tmp: u32 = v[1].parse().expect("invalid number");
            if next_id < tmp {
                next_id = tmp;
            }
        }
        Ok(std::cmp::max(next_id + 1, NEXT_ID))
    }

    fn new(opt: Arc<Options>, files: FileMap) -> Result<Self, OpCode> {
        let cap = opt.buffer_count;
        let q_cap = next_power_of_2(cap as usize) as u32;
        let next_id = Self::find_next_id(&opt)?;

        Ok(Self {
            bufs: RwLock::new(HashMap::new()),
            flush: Flush::new(opt.clone()),
            freelist: Queue::new(q_cap, Some(Box::new(|a| free_arena(a)))),
            junks: Queue::new(q_cap, Some(Box::new(|a| free_arena(a)))),
            cur: AtomicPtr::new(null_mut()),
            cap,
            cnt: Mutex::new(0),
            buf_size: opt.buffer_size,
            next_id: AtomicU32::new(next_id),
            files,
            opt,
        })
    }

    fn current(&self) -> &Arena {
        loop {
            let cur = self.cur.load(Relaxed);
            if !cur.is_null() {
                return raw_ptr_to_ref(cur);
            }

            self.install_new();
        }
    }

    fn flush_arena(&self, a: &Arena, id: u32) {
        if a.is_flushable() {
            let mut bufs = self.bufs.write().expect("can't lock write");
            let a = bufs.remove(&id).expect("bad id");
            self.junks.push(a).expect("no space");
            let a = raw_ptr_to_ref(a);
            let map = self.files.clone();
            let opt = self.opt.clone();
            let r = self.flush.tx.send(FlushData::new(
                id,
                a.used(),
                Box::new(move || {
                    let mut files = map.write().expect("can't lock write");
                    files.insert(id, FileReader::new(&opt, id));
                    a.mark_flushed();
                }),
            ));
            // flusher was existed
            if r.is_err() {
                unimplemented!()
            }
        }
    }

    fn release(&self, id: u32) {
        let a = self.get(id).expect("bad id");
        a.dec_ref();
        self.flush_arena(a, id);
    }

    fn get(&self, id: u32) -> Option<&Arena> {
        let lk = self.bufs.read().expect("can't lock read");
        if let Some(x) = lk.get(&id) {
            Some(raw_ptr_to_ref(*x))
        } else {
            None
        }
    }

    fn get_or_alloc(&self) -> Option<*mut Arena> {
        loop {
            if let Ok(ptr) = self.freelist.pop() {
                let id = self.next_id.fetch_add(1, Relaxed);
                let p = raw_ptr_to_ref(ptr);
                p.reset(id);
                return Some(ptr);
            } else {
                let mut lk = self.cnt.lock().expect("lock failed");

                if *lk > self.cap {
                    return None;
                }
                *lk += 1;

                let a = unsafe {
                    let a = alloc_zeroed(Layout::new::<Arena>()) as *mut Arena;
                    std::ptr::write(a, Arena::new(0, self.buf_size));
                    a
                };
                self.freelist.push(a).expect("no space");
            }
        }
    }

    /// FIXME: it will cause busy-wait when write speed > flush speed
    fn install_new(&self) {
        let cnt = self.junks.count();
        for _ in 0..cnt {
            if let Ok(x) = self.junks.pop() {
                let a = raw_ptr_to_ref(x);
                if a.is_flushed() {
                    self.freelist.push(x).expect("no space");
                } else {
                    self.junks.push(x).expect("no space");
                }
            }
        }

        if let Some(p) = self.get_or_alloc() {
            let cur = self.cur.load(Relaxed);
            if self.cur.compare_exchange(cur, p, AcqRel, Acquire).is_ok() {
                if !cur.is_null() {
                    self.junks.push(cur).expect("no space");
                }

                let mut bufs = self.bufs.write().expect("can't lock write");
                let a = raw_ptr_to_ref(p);
                bufs.insert(a.id.get(), p);
            }
            // else
            // new arena was already installed by other thread
        }
    }

    fn flush_all(&self) {
        let buf = self.bufs.read().expect("can't lock read");
        let mut tmp: Vec<(u32, &Arena)> =
            buf.iter().map(|(k, v)| (*k, raw_ptr_to_ref(*v))).collect();
        drop(buf);

        for (id, a) in &tmp {
            a.mark_flushable();
            assert!(a.is_flushable());
            self.flush_arena(a, *id);
        }

        let mut cnt = 0;
        let expect = tmp.len();
        while cnt != expect {
            if let Some((id, a)) = tmp.pop() {
                if a.is_flushed() {
                    cnt += 1;
                } else {
                    tmp.push((id, a));
                }
            }
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.flush_all();
        self.flush.quit();
    }
}

pub struct Buffers {
    opt: Arc<Options>,
    pool: Pool,
    cache: Cache,
    files: Arc<RwLock<HashMap<u32, FileReader>>>,
}

impl Buffers {
    pub fn new(opt: Arc<Options>) -> Result<Self, OpCode> {
        let files = Arc::new(RwLock::new(HashMap::new()));
        Ok(Self {
            opt: opt.clone(),
            pool: Pool::new(opt.clone(), files.clone())?,
            cache: Cache::new(&opt),
            files,
        })
    }

    /// NOTE: mutable buffer will never be cached, the result is the raw data with frame header
    pub fn alloc(&self, size: u32, is_new: bool) -> Result<(u64, u32, FrameOwner), OpCode> {
        loop {
            let a = self.pool.current();
            match a.alloc(size, is_new) {
                Ok(x) => return Ok(x),
                Err(e @ OpCode::TooLarge) => return Err(e),
                Err(OpCode::Again) => continue,
                Err(OpCode::NeedMore) => {
                    a.mark_sealed();
                    self.pool.install_new();
                }
                _ => unreachable!("invalid opcode"),
            }
        }
    }

    fn copy_to_cache(&self, addr: u64, src: FrameOwner) -> Arc<FrameOwner> {
        let dst = Arc::new(FrameOwner::alloc(src.payload_size() as usize));
        debug_assert_eq!(src.size(), dst.size());
        src.copy_to(&dst);
        self.cache.put(addr, dst.clone());
        dst
    }

    pub fn cache(&self, addr: u64) {
        let (id, off) = decode_u64(addr);
        if let Some(a) = self.pool.get(id) {
            self.copy_to_cache(addr, FrameOwner::from(a.load(off)));
        }
    }

    pub fn release_buffer(&self, buffer_id: u32) {
        self.pool.release(buffer_id);
    }

    /// NOTE: the loaded buffer is with a frame, and the return value is offset after the frame
    pub fn load(&self, addr: u64) -> Result<Arc<FrameOwner>, OpCode> {
        if let Some(x) = self.cache.get(addr) {
            return Ok(x);
        }

        let (id, off) = decode_u64(addr);

        if let Some(a) = self.pool.get(id) {
            let x = FrameOwner::from(a.load(off));
            debug_assert_eq!(x.addr(), addr);
            // NOTE: this may conflict with SysTxn::commit, we'd better remove the cache in systxn
            return Ok(self.copy_to_cache(addr, x));
        }

        // NOTE: multiple threads may load data from the same addr, we allow this to happen, since
        // it's uncommon and the data loaded is read-only it will not cause errors
        let lk = self.files.read().expect("can't lock");
        if let Some(r) = lk.get(&id) {
            self.load_frame(r, addr, off)
        } else {
            drop(lk);
            let mut lk = self.files.write().expect("can't lock write");
            let r = FileReader::new(&self.opt, id);
            lk.insert(id, r.clone());
            drop(lk);
            self.load_frame(&r, addr, off)
        }
    }

    fn load_frame(&self, r: &FileReader, addr: u64, off: u32) -> Result<Arc<FrameOwner>, OpCode> {
        let f = r.read_addr(off);
        let a = Arc::new(f);
        self.cache.put(addr, a.clone());
        Ok(a)
    }
}
