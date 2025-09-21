use crc32c::Crc32cHasher;
use dashmap::DashMap;
use io::{File, GatherIO};

use crate::meta::{FileStat, Numerics, PageTable, StatInner};
use crate::types::header::TagFlag;
use crate::types::refbox::BoxRef;
use crate::types::traits::IHeader;
use crate::utils::NULL_ADDR;
use crate::utils::bitmap::BitMap;
use crate::utils::block::Block;
use crate::utils::data::{AddrPair, GatherWriter, JUNK_LEN, Position};
use crate::utils::{CachePad, Handle, INIT_ID, NULL_PID, pack_id, raw_ptr_to_ref};
use crate::{OpCode, static_assert};
use std::alloc::{Layout, alloc_zeroed};
use std::cell::Cell;
use std::cmp::min;
use std::fmt::Debug;
use std::hash::Hasher;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU8, AtomicU16, AtomicU64};

pub(crate) struct FlushData {
    arena: Handle<Arena>,
    cb: Box<dyn FnOnce()>,
}

unsafe impl Send for FlushData {}

impl FlushData {
    pub fn new(arena: Handle<Arena>, cb: Box<dyn FnOnce()>) -> Self {
        Self { arena, cb }
    }

    pub fn id(&self) -> u32 {
        self.arena.id()
    }

    pub fn mark_done(self) {
        (self.cb)()
    }
}

impl Deref for FlushData {
    type Target = Arena;

    fn deref(&self) -> &Self::Target {
        &self.arena
    }
}

// use C repr to fix the layout
#[repr(C)]
pub(crate) struct Arena {
    items: DashMap<u64, BoxRef>,
    id: Cell<u32>,
    /// flush LSN
    pub(crate) flsn: Box<[CachePad<AtomicU64>]>,
    real_size: AtomicU64,
    // pack file_id and seq
    offset: AtomicU64,
    cap: u32,
    refs: AtomicU16,
    pub(crate) state: AtomicU8,
}

impl Deref for Arena {
    type Target = DashMap<u64, BoxRef>;

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

impl Arena {
    /// memory can be allocated
    pub(crate) const HOT: u8 = 4;
    /// memory no longer available for allocating
    pub(crate) const WARM: u8 = 3;
    /// waiting for flush
    pub(crate) const COLD: u8 = 2;
    /// flushed to disk
    pub(crate) const FLUSH: u8 = 1;

    fn alloc_flsn(n: usize) -> Box<[CachePad<AtomicU64>]> {
        static_assert!(size_of::<CachePad<Position>>() == 64);
        static_assert!(align_of::<CachePad<Position>>() == 64);
        let layout = Layout::from_size_align(64 * n, 64).unwrap();
        unsafe {
            let p = alloc_zeroed(layout);
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                p.cast::<CachePad<AtomicU64>>(),
                n,
            ))
        }
    }

    pub(crate) fn new(cap: u32, workers: usize) -> Self {
        Self {
            items: DashMap::with_capacity(16 << 10),
            flsn: Self::alloc_flsn(workers),
            id: Cell::new(INIT_ID),
            refs: AtomicU16::new(0),
            offset: AtomicU64::new(0),
            real_size: AtomicU64::new(0),
            cap,
            state: AtomicU8::new(Self::FLUSH),
        }
    }

    pub(crate) fn reset(&self, id: u32) {
        self.id.set(id);
        assert_eq!(self.state(), Self::FLUSH);
        self.set_state(Arena::FLUSH, Arena::HOT);
        self.offset.store(0, Relaxed);
        self.items.clear();
        self.real_size.store(0, Relaxed);
        assert!(self.unref());
    }

    pub(crate) fn cap(&self) -> u32 {
        self.cap
    }

    pub(crate) fn sizes(&self) -> (u64, u64) {
        let x = self.real_size.load(Relaxed);
        let y = self.offset.load(Relaxed);
        (x, y)
    }

    fn alloc_size(&self, size: u32) -> Result<u32, OpCode> {
        if size > self.cap {
            return Err(OpCode::TooLarge);
        }

        let mut cur = self.real_size.load(Relaxed);
        loop {
            // it's possible that other thread change the state to WARM
            if self.state() != Self::HOT {
                return Err(OpCode::Again);
            }

            let new = cur + size as u64;
            if new > self.cap as u64 {
                return Err(OpCode::NeedMore);
            }

            match self.real_size.compare_exchange(cur, new, AcqRel, Acquire) {
                Ok(_) => {
                    let off = self.offset.fetch_add(1_u64, Relaxed);
                    if off >= u32::MAX as u64 {
                        return Err(OpCode::NeedMore);
                    }
                    return Ok(off as u32);
                }
                Err(e) => cur = e,
            }
        }
    }

    fn alloc_at(&self, numerics: &Numerics, off: u32, size: u32) -> BoxRef {
        let addr = pack_id(self.id.get(), off);
        let p = BoxRef::alloc(size, addr, numerics.epoch.fetch_add(1, Relaxed));
        self.items.insert(addr, p.clone());
        p
    }

    pub fn alloc(&self, numerics: &Numerics, size: u32) -> Result<BoxRef, OpCode> {
        let real_size = BoxRef::real_size(size);
        self.inc_ref();
        let offset = self.alloc_size(real_size).inspect_err(|_| self.dec_ref())?;
        Ok(self.alloc_at(numerics, offset, size))
    }

    pub(crate) fn dealloc(&self, addr: u64, len: usize) {
        if self.items.remove(&addr).is_some() {
            self.real_size.fetch_sub(len as u64, AcqRel);
        }
    }

    #[inline]
    pub(crate) fn load(&self, off: u32) -> BoxRef {
        let addr = pack_id(self.id(), off);
        self.items.get(&addr).unwrap().value().clone()
    }

    pub(crate) fn set_state(&self, cur: u8, new: u8) -> u8 {
        self.state
            .compare_exchange(cur, new, AcqRel, Acquire)
            .unwrap_or_else(|x| x)
    }

    pub(crate) fn state(&self) -> u8 {
        self.state.load(Relaxed)
    }

    /// we can't remove entry, although it has performance boost, but it may cause further lookup
    /// fail (because we allow load from WARM and COLD arena which is not flushed)
    ///
    /// if we back up the removed entry, the previous boost will be lost, and it will slow down front
    /// thread
    #[allow(unused)]
    pub(crate) fn recycle(&self, addr: &u64) -> bool {
        if let Some(mut x) = self.items.get_mut(addr) {
            let h = x.value_mut().header_mut();
            h.flag = TagFlag::TombStone;
            let old = self.real_size.fetch_sub(h.total_size as u64, AcqRel);
            #[cfg(feature = "extra_check")]
            assert!(old >= h.total_size as u64);
            true
        } else {
            false
        }
    }

    pub(crate) fn record_lsn(&mut self, worker_id: usize, seq: u64) {
        self.flsn[worker_id].store(seq, Relaxed);
    }

    pub(crate) fn inc_ref(&self) {
        self.refs.fetch_add(1, Relaxed);
    }

    pub(crate) fn dec_ref(&self) {
        self.refs.fetch_sub(1, Release);
    }

    fn unref(&self) -> bool {
        self.refs.load(Acquire) == 0
    }

    pub(crate) fn id(&self) -> u32 {
        self.id.get()
    }
}

impl Debug for Arena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Arena")
            .field("items ", &self.items.len())
            .field("state", &self.state)
            .field("offset", &self.offset)
            .field("id", &self.id.get())
            .finish()
    }
}

/// the layout of a flushed arena is:
/// ```text
/// high                                      low
/// +--------+-------------+------------------+
/// | footer | relocations |  frames (normal) |
/// +--------+-------------+-------+----------+
///
/// ```
/// write file from frames to footer while read file from footer to relocations
#[repr(C, packed(1))]
#[derive(Default, Debug)]
pub(crate) struct DataFooter {
    /// monotonically increasing
    pub(crate) up2: u64,
    /// item's relocation table
    pub(crate) nr_reloc: u32,
    /// active frames
    pub(crate) nr_active: u32,
    /// active frame size, also the initial total size
    pub(crate) active_size: usize,
    pub(crate) padding: u32,
    pub(crate) crc: u32,
}

impl DataFooter {
    pub(crate) const LEN: usize = size_of::<Self>();

    pub(crate) fn as_slice(&self) -> &[u8] {
        unsafe {
            let p = self as *const Self;
            std::slice::from_raw_parts(p.cast::<u8>(), size_of::<Self>())
        }
    }

    fn reloc_pos(&self) -> usize {
        Self::LEN
    }

    fn meta_len(&self) -> usize {
        self.reloc_len()
    }

    fn get<T>(&self, off: usize, n: usize) -> &[T] {
        let p = self as *const Self;
        unsafe {
            let p = p.cast::<u8>().add(off).cast::<T>();
            std::slice::from_raw_parts(p, n)
        }
    }

    fn reloc_slice(&self) -> &[u8] {
        self.get(self.reloc_pos(), self.reloc_len())
    }

    fn reloc_len(&self) -> usize {
        self.nr_reloc as usize * AddrPair::LEN
    }

    pub(crate) fn relocs(&self) -> &[AddrPair] {
        self.get(self.reloc_pos(), self.nr_reloc as usize)
    }
}

/// keeps only relocate and junk and data
pub(crate) struct DataBuilder {
    nr_rel: u32,
    nr_junk: u32,
    nr_active: u32,
    active_size: usize,
    /// never flush to file
    pub(crate) junks: Vec<BoxRef>,
    reloc: Vec<u8>,
    frames: Vec<BoxRef>,
}

impl DataBuilder {
    pub(crate) fn new() -> Self {
        Self {
            nr_rel: 0,
            nr_junk: 0,
            nr_active: 0,
            active_size: 0,
            junks: Vec::new(),
            reloc: Vec::new(),
            frames: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, f: BoxRef) {
        let h = f.header();
        match h.flag {
            TagFlag::Normal | TagFlag::Sibling => {
                self.nr_active += 1;
                self.active_size += f.total_size() as usize;
                self.nr_rel += 1;
                self.frames.push(f);
            }
            TagFlag::Junk => {
                self.nr_junk += h.payload_size / JUNK_LEN as u32;
                self.junks.push(f);
            }
            TagFlag::TombStone | TagFlag::Unmap => {}
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.frames.is_empty() && self.junks.is_empty()
    }

    pub(crate) fn active_frames(&self) -> usize {
        self.frames.len()
    }

    pub(crate) fn stat(&self, id: u32, tick: u64) -> FileStat {
        FileStat {
            inner: StatInner {
                file_id: id,
                _padding: 0,
                up1: tick,
                up2: tick,
                active_elems: self.nr_active,
                total_elems: self.nr_active,
                active_size: self.active_size,
                total_size: self.active_size,
            },
            deleted_elems: BitMap::new(self.nr_active),
        }
    }

    pub(crate) fn build(&mut self, tick: u64, path: PathBuf) {
        let mut pos: usize = 0;
        let mut crc = Crc32cHasher::default();
        let mut w = GatherWriter::trunc(&path, 64);

        for (seq, f) in self.frames.iter().enumerate() {
            let h = f.header();
            // we dump the whole BoxRef into file, so use total_size, when load we must convert it
            // back to the original size when it was allocated
            let reloc = AddrPair::new(h.addr, pos, h.total_size, seq as u32);
            self.reloc.extend_from_slice(reloc.as_slice());
            let s = f.dump_slice();
            pos += s.len();

            crc.write(s);
            w.queue(s);
        }

        let s = self.reloc.as_slice();
        crc.write(s);
        w.queue(s);

        let hdr = DataFooter {
            up2: tick,
            nr_reloc: self.nr_rel,
            nr_active: self.nr_active,
            active_size: self.active_size,
            padding: 0,
            crc: crc.finish() as u32,
        };

        w.queue(hdr.as_slice());
        w.flush();
        w.sync();
    }
}

pub(crate) struct DataMetaReader {
    file: File,
    /// position of buffer, from begin to end
    pos: usize,
    /// offset of file, from end to begin
    off: u64,
    buf: Block,
    validate: bool,
}

impl DataMetaReader {
    fn open<T: AsRef<Path>>(path: T, validate: bool) -> Result<Self, OpCode> {
        let file = File::options()
            .read(true)
            .trunc(false)
            .open(&path.as_ref().to_path_buf())
            .map_err(|_| {
                // log::warn!("can't open {:?} {}", path.as_ref(), x);
                OpCode::IoError
            })?;
        let off = file.size().expect("can't get file size");
        if off < DataFooter::LEN as u64 {
            return Err(OpCode::NeedMore);
        }

        Ok(Self {
            file,
            pos: 0,
            off,
            buf: Block::alloc(4096),
            validate,
        })
    }

    pub(crate) fn new<T: AsRef<Path>>(path: T, validate_data: bool) -> Result<Self, OpCode> {
        Self::open(path, validate_data)
    }

    pub(crate) fn get_meta(&mut self) -> Result<&DataFooter, OpCode> {
        let crc = self.read_meta().map_err(|e| {
            log::error!("io error {e}");
            OpCode::IoError
        })?;
        let f = raw_ptr_to_ref(self.buf.data().cast::<DataFooter>());

        if crc != f.crc {
            log::error!("bad checksum, expect {} get {}", { f.crc }, crc);
            Err(OpCode::BadData)
        } else {
            Ok(f)
        }
    }

    fn read_meta(&mut self) -> Result<u32, std::io::Error> {
        Self::get_footer(self)?;
        let f = raw_ptr_to_ref(self.buf.data().cast::<DataFooter>());
        self.get_reloc(f)?;
        if self.validate {
            self.calc_crc(f)
        } else {
            Ok(f.crc)
        }
    }

    fn get_footer(&mut self) -> Result<(), std::io::Error> {
        let flen = DataFooter::LEN;
        let s = self.buf.mut_slice(0, flen);
        self.off -= flen as u64;

        self.file.read(s, self.off)?;
        self.pos += flen;

        let footer = raw_ptr_to_ref(s.as_mut_ptr().cast::<DataFooter>());
        if flen + footer.meta_len() > self.buf.len() {
            self.buf.realloc(footer.meta_len() + flen);
        }

        Ok(())
    }

    fn get_reloc(&mut self, f: &DataFooter) -> Result<(), std::io::Error> {
        if f.reloc_len() > 0 {
            self.off -= f.reloc_len() as u64;
            let s = self.buf.mut_slice(self.pos, f.reloc_len());

            self.file.read(s, self.off)?;
            self.pos += f.reloc_len();
        }
        Ok(())
    }

    fn calc_crc(&self, f: &DataFooter) -> Result<u32, std::io::Error> {
        let mut buf = [0u8; 4096];
        let mut h = Crc32cHasher::default();
        let end = self.off;
        let mut off = 0;

        while off < end {
            let len = min((end - off) as usize, buf.len());
            let s = &mut buf[0..len];
            self.file.read(s, off)?;
            off += len as u64;
            h.write(s);
        }

        h.write(f.reloc_slice());

        Ok(h.finish() as u32)
    }

    pub(crate) fn take(self) -> File {
        self.file
    }
}

pub(crate) struct MapBuilder {
    table: PageTable,
}

impl MapBuilder {
    pub(crate) fn new() -> Self {
        Self {
            table: PageTable::default(),
        }
    }

    fn add_impl(&mut self, pid: u64, addr: u64, epoch: u64, is_unmap: bool) {
        self.table
            .add(pid, if is_unmap { NULL_ADDR } else { addr }, epoch);
    }

    pub(crate) fn add(&mut self, f: &BoxRef) {
        let h = f.header();
        match h.flag {
            TagFlag::Normal => {
                self.add_impl(h.pid, h.addr, h.epoch, false);
            }
            TagFlag::Unmap => {
                self.add_impl(h.pid, h.addr, h.epoch, true);
            }
            TagFlag::Sibling => {
                assert_eq!(h.pid, NULL_PID);
            }
            _ => {}
        }
    }

    pub(crate) fn table(self) -> PageTable {
        self.table
    }
}

#[cfg(test)]
mod test {
    use crate::{
        Options, RandomPath,
        types::{refbox::BoxRef, traits::IHeader},
        utils::INIT_ID,
    };

    use super::{DataBuilder, DataMetaReader};

    #[test]
    fn data_dump_load() {
        let path = RandomPath::new();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;

        opt.create_dir();

        let (pid, addr) = (114514, 1919810);
        let mut p = BoxRef::alloc(233, addr, 0);
        p.header_mut().pid = pid;

        let mut builder = DataBuilder::new();

        builder.add(p.clone());

        let path = opt.data_file(INIT_ID);
        builder.build(0, path);

        let mut loader = DataMetaReader::new(opt.data_file(INIT_ID), true).unwrap();

        let d = loader.get_meta().unwrap();
        let reloc = d.relocs();

        assert_eq!(reloc.len(), 1);

        let h = p.header();
        let r = &reloc[0];
        assert_eq!({ r.key }, addr);
        assert_eq!({ r.val.off }, 0);
        assert_eq!({ r.val.len }, h.total_size);
    }
}
