use crc32c::Crc32cHasher;
use dashmap::DashMap;
use io::{File, GatherIO};

use crate::types::header::TagFlag;
use crate::types::refbox::BoxRef;
use crate::types::traits::{IAsSlice, IHeader};
use crate::utils::bitmap::BitMap;
use crate::utils::block::Block;
use crate::utils::data::{
    AddrPair, GatherWriter, ID_LEN, JUNK_LEN, MapEntry, Meta, PageTable, Position, Reloc,
};
use crate::utils::lru::LruInner;
use crate::utils::{CachePad, Handle, INIT_ID, NULL_PID, pack_id, raw_ptr_to_ref};
use crate::utils::{MutRef, NULL_ADDR, rand_range};
use crate::{OpCode, Options, static_assert};
use std::alloc::{Layout, alloc_zeroed};
use std::cell::Cell;
use std::cmp::min;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::hash::Hasher;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU8, AtomicU16, AtomicU32, AtomicU64};

/// NOTE: the fields except `file_id` will be changed, up1 and up2 change on new write, nr_active
/// and active_size change when flush have new deallocate frame point to current file_id
pub(crate) struct FileStat {
    pub(crate) file_id: u32,
    /// up1 and up2, see [Efficiently Reclaiming Space in a Log Structured Store](https://ieeexplore.ieee.org/document/9458684)
    pub(crate) up1: u64,
    pub(crate) up2: u64,
    pub(crate) nr_active: u32,
    pub(crate) active_size: usize,
    // the following two field will never change
    pub(crate) total: u32,
    pub(crate) total_size: usize,
    /// when an active frame was deallocated, mask it in the bitmap
    pub(crate) dealloc: BitMap,
    pub(crate) refcnt: AtomicU32,
}

impl FileStat {
    pub(crate) fn update(&mut self, reloc: Reloc, tick: u64) {
        self.nr_active -= 1;
        self.active_size -= reloc.len as usize;
        self.dealloc.set(reloc.seq);
        // make sure monotonically increasing
        if self.up1 < tick {
            self.up1 = self.up2;
            self.up2 = tick;
        }
    }
}

/// it must be protected by a lock
pub(crate) struct StatHandle {
    raw: *mut FileStat,
}

unsafe impl Send for StatHandle {}
unsafe impl Sync for StatHandle {}

impl From<*mut FileStat> for StatHandle {
    fn from(raw: *mut FileStat) -> Self {
        Self { raw }
    }
}

impl Deref for StatHandle {
    type Target = FileStat;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.raw }
    }
}

impl DerefMut for StatHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.raw }
    }
}

impl Clone for StatHandle {
    fn clone(&self) -> Self {
        self.refcnt.fetch_add(1, Release);
        Self { raw: self.raw }
    }
}

impl Drop for StatHandle {
    fn drop(&mut self) {
        if self.refcnt.fetch_sub(1, Release) == 1 {
            unsafe {
                drop(Box::from_raw(self.raw));
            }
        }
    }
}

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

    fn alloc_at(&self, meta: &Meta, off: u32, size: u32) -> BoxRef {
        let addr = pack_id(self.id.get(), off);
        let p = BoxRef::alloc(size, addr, meta.epoch.fetch_add(1, Relaxed));
        self.items.insert(addr, p.clone());
        p
    }

    pub fn alloc(&self, meta: &Meta, size: u32) -> Result<BoxRef, OpCode> {
        let real_size = BoxRef::real_size(size);
        self.inc_ref();
        let offset = self.alloc_size(real_size).inspect_err(|_| self.dec_ref())?;
        Ok(self.alloc_at(meta, offset, size))
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
/// high                                                     low
/// +--------+------+---------------------+-------+----------+
/// | footer | lids | relocations | junks |  frames (normal) |
/// +--------+------+---------------------+-------+----------+
///
/// ```
/// write file from frames to footer while read file from footer to relocations
#[repr(C, packed(1))]
#[derive(Default, Debug)]
pub(crate) struct DataFooter {
    /// monotonically increasing
    pub(crate) up2: u64,
    /// logical id entries
    pub(crate) nr_lid: u32,
    /// item's relocation table
    pub(crate) nr_reloc: u32,
    /// count of junks
    pub(crate) nr_junk: u32,
    /// active frames
    pub(crate) nr_active: u32,
    pub(crate) padding: u32,
    /// active frame size, also the initial total size
    pub(crate) active_size: usize,
    pub(crate) padding2: u32,
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

    fn lid_pos(&self) -> usize {
        Self::LEN
    }

    fn reloc_pos(&self) -> usize {
        Self::LEN + self.lid_len()
    }

    fn meta_len(&self) -> usize {
        self.lid_len() + self.reloc_len()
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

    fn lid_slice(&self) -> &[u8] {
        self.get(self.lid_pos(), self.lid_len())
    }

    fn lid_len(&self) -> usize {
        self.nr_lid as usize * ID_LEN
    }

    fn reloc_len(&self) -> usize {
        self.nr_reloc as usize * AddrPair::LEN
    }

    fn junk_len(&self) -> usize {
        self.nr_junk as usize * JUNK_LEN
    }

    pub(crate) fn lids(&self) -> &[u32] {
        self.get(self.lid_pos(), self.nr_lid as usize)
    }

    pub(crate) fn relocs(&self) -> &[AddrPair] {
        self.get(self.reloc_pos(), self.nr_reloc as usize)
    }
}

pub(crate) struct DataBuilder {
    tick: u64,
    file_id: u32,
    nr_rel: u32,
    nr_junk: u32,
    nr_active: u32,
    active_size: usize,
    pub(crate) junks: Vec<BoxRef>,
    reloc: Vec<u8>,
    frames: Vec<BoxRef>,
}

impl DataBuilder {
    pub(crate) fn new(tick: u64, file_id: u32) -> Self {
        Self {
            tick,
            file_id,
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

    pub(crate) fn build<T>(&mut self, opt: &Options, state: &mut T)
    where
        T: GatherIO,
    {
        let mut pos: usize = 0;
        let mut crc = Crc32cHasher::default();
        let path = opt.data_file(self.file_id);
        let ctx = DataState {
            kind: StateType::Data,
            file_id: self.file_id,
        };

        state.write(ctx.as_slice()).unwrap();
        state.sync().unwrap();

        // must create after state is stabilized
        let mut w = GatherWriter::new(&path, 64);

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

        #[cfg(feature = "extra_check")]
        {
            let mut cnt = 0;
            for f in &self.junks {
                let s = f.data_slice::<u8>();
                cnt += s.len();
                crc.write(s);
                w.queue(s);
            }
            assert_eq!(self.nr_junk as usize, cnt / JUNK_LEN);
        }

        #[cfg(not(feature = "extra_check"))]
        for f in &self.junks {
            let s = f.data_slice::<u8>();
            crc.write(s);
            w.queue(s);
        }

        let s = self.reloc.as_slice();
        crc.write(s);
        w.queue(s);

        let hdr = DataFooter {
            up2: self.tick,
            nr_lid: 0,
            nr_reloc: self.nr_rel,
            nr_junk: self.nr_junk,
            nr_active: self.nr_active,
            padding: 0,
            active_size: self.active_size,
            padding2: 0,
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
    const BUF_LEN: usize = DataFooter::LEN;

    fn open<T: AsRef<Path>>(path: T, validate: bool) -> Result<Self, OpCode> {
        let file = File::options()
            .read(true)
            .trunc(false)
            .open(&path.as_ref().to_path_buf())
            .map_err(|x| {
                log::warn!("can't open {:?} {}", path.as_ref(), x);
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
            buf: Block::alloc(Self::BUF_LEN),
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

    pub(crate) fn get_junk(&mut self) -> Result<Vec<u64>, OpCode> {
        let f = raw_ptr_to_ref(self.buf.data().cast::<DataFooter>());
        if f.junk_len() == 0 {
            return Ok(Vec::new());
        }
        let mut v: Vec<u64> = vec![0; f.junk_len() / JUNK_LEN];
        let off = self.off - f.junk_len() as u64;
        let s = unsafe {
            let ptr = v.as_mut_ptr().cast::<u8>();
            std::slice::from_raw_parts_mut(ptr, f.junk_len())
        };
        self.file.read(s, off).map_err(|e| {
            log::error!("read error {e}");
            OpCode::IoError
        })?;

        Ok(v)
    }

    fn read_meta(&mut self) -> Result<u32, std::io::Error> {
        Self::get_footer(self)?;
        let f = raw_ptr_to_ref(self.buf.data().cast::<DataFooter>());
        self.get_lid(f)?;
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

    fn get_lid(&mut self, f: &DataFooter) -> Result<(), std::io::Error> {
        if f.lid_len() > 0 {
            self.off -= f.lid_len() as u64;
            let s = self.buf.mut_slice(self.pos, f.lid_len());

            self.file.read(s, self.off)?;
            self.pos += f.lid_len();
        }
        Ok(())
    }

    fn get_reloc(&mut self, f: &DataFooter) -> Result<(), std::io::Error> {
        if f.reloc_len() > 0 {
            self.off -= f.reloc_len() as u64;
            let s = self.buf.mut_slice(self.pos, f.reloc_len());

            self.file.read(s, self.off)?;
            self.pos += f.reloc_len();
            assert_eq!(self.pos, self.buf.len());
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
        h.write(f.lid_slice());

        Ok(h.finish() as u32)
    }

    pub(crate) fn take(self) -> File {
        self.file
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) enum StateType {
    #[default]
    Data,
    Map,
}

impl From<u8> for StateType {
    fn from(value: u8) -> Self {
        assert!(value < 2);
        unsafe { std::mem::transmute(value) }
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C, packed(1))]
pub(crate) struct DataState {
    kind: StateType,
    pub(crate) file_id: u32,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C, packed(1))]
pub(crate) struct MapSate {
    kind: StateType,
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

impl IAsSlice for DataState {}
impl IAsSlice for MapSate {}

#[derive(Default, Debug)]
#[repr(C, packed(1))]
pub(crate) struct MapHeader {
    len: u32,
    crc32: u32,
}

impl IAsSlice for MapHeader {}

pub(crate) struct MapBuilder {
    tables: HashMap<u32, PageTable>,
    size_map: HashMap<u32, usize>,
    files: LruInner<u32, MutRef<io::File>>,
}

pub const MAX_MAP_FILE_SIZE: usize = 100 << 20;
pub const PID_PER_MAP_FILE: usize = MAX_MAP_FILE_SIZE / PageTable::ITEM_LEN;
pub const MAP_FILE_GC_SIZE: usize = MAX_MAP_FILE_SIZE + MAX_MAP_FILE_SIZE / 2;

pub fn pid_to_fid(pid: u64) -> u32 {
    (pid / PID_PER_MAP_FILE as u64) as u32
}

impl MapBuilder {
    const CAP: usize = 32;
    pub(crate) fn new() -> Self {
        Self {
            tables: HashMap::with_capacity(1),
            size_map: HashMap::new(),
            files: LruInner::new(),
        }
    }

    fn add_impl(&mut self, pid: u64, addr: u64, epoch: u64, is_unmap: bool) {
        let id = pid_to_fid(pid);
        match self.tables.entry(id) {
            Entry::Occupied(ref mut x) => x.get_mut().add(pid, addr, epoch),
            Entry::Vacant(x) => {
                let mut table = PageTable::default();
                table.add(pid, if is_unmap { NULL_ADDR } else { addr }, epoch);
                let _ = x.insert(table);
            }
        }
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

    fn record(state: &mut io::File, id: u32, offset: u64) {
        let ctx = MapSate {
            kind: StateType::Map,
            file_id: id,
            offset,
        };
        state.write(ctx.as_slice()).unwrap();
        state.sync().unwrap();
    }

    fn get_file(&self, opt: &Options, id: u32) -> MutRef<io::File> {
        if let Some(x) = self.files.get(&id) {
            x.clone()
        } else {
            let f = MutRef::new(
                io::File::options()
                    .append(true)
                    .write(true)
                    .create(true)
                    .open(&opt.map_file(id))
                    .unwrap(),
            );
            self.files.add(Self::CAP, id, f.clone());
            f
        }
    }

    pub(crate) fn build(&mut self, opt: &Options, state: &mut io::File) {
        for (id, table) in self.tables.iter() {
            let f = self.get_file(opt, *id);
            Self::record(state, *id, f.size().unwrap());

            let v = table.collect();
            let mut h = Crc32cHasher::default();
            h.write(&v);
            let hdr = MapHeader {
                len: v.len() as u32,
                crc32: h.finish() as u32,
            };

            let raw = f.raw();
            raw.write(hdr.as_slice()).unwrap();
            f.raw().write(&v).unwrap();
            f.raw().sync().unwrap();
            self.size_map.insert(*id, f.size().unwrap() as usize);
        }
    }

    pub(crate) fn compact(&mut self, opt: &Options) {
        let mut v: Vec<(u32, usize)> = self
            .size_map
            .iter()
            .filter(|x| {
                if cfg!(debug_assertions) {
                    x.1.cmp(&MAP_FILE_GC_SIZE).is_le()
                } else {
                    x.1.cmp(&MAP_FILE_GC_SIZE).is_ge()
                }
            })
            .map(|(x, y)| (*x, *y))
            .collect();

        if !v.is_empty() {
            v.sort_by(|x, y| x.1.cmp(&y.1));
            let n = rand_range(0..v.len()).max(1); // at least choose one
            let mut block = Block::alloc(MapReader::DEFAULT_BLOCK_SIZE);

            for (id, _) in &v[..n] {
                let id = *id;
                if Self::do_compact(id, &mut block, opt).is_err() {
                    log::error!("can't compact {:?}", opt.map_file(id));
                    let _ = std::fs::remove_file(opt.map_file_tmp(id));
                }
            }
        }
    }

    fn do_compact(id: u32, block: &mut Block, opt: &Options) -> Result<(), std::io::Error> {
        let from = opt.map_file_tmp(id);
        let to = opt.map_file(id);

        {
            let mut r = MapReader::new(&to);
            let mut map = PageTable::default();

            loop {
                let x = r.next(block).unwrap();
                if let Some(es) = x {
                    es.iter().for_each(|e| {
                        map.add(e.page_id, e.page_addr, e.epoch);
                    });
                } else {
                    break;
                }
            }

            let mut f = File::options()
                .write(true)
                .trunc(true)
                .create(true)
                .open(&from)?;
            let mut h = Crc32cHasher::default();
            let v = map.collect();
            h.write(&v);
            let hdr = MapHeader {
                len: v.len() as u32,
                crc32: h.finish() as u32,
            };

            f.write(hdr.as_slice())?;
            f.write(&v)?;
            f.sync()?;
        }

        std::fs::rename(from, to)
    }
}

pub(crate) struct MapReader {
    path: PathBuf,
    file: File,
    offset: u64,
    end: u64,
}

impl MapReader {
    pub(crate) const DEFAULT_BLOCK_SIZE: usize = 4096;

    pub(crate) fn new<P: AsRef<Path>>(path: P) -> Self {
        let p = path.as_ref().to_path_buf();
        let file = File::options().read(true).open(&p).unwrap();
        let end = file.size().unwrap();
        Self {
            path: p,
            file,
            offset: 0,
            end,
        }
    }

    pub(crate) fn next(&mut self, block: &mut Block) -> Result<Option<&[MapEntry]>, OpCode> {
        if self.offset >= self.end {
            return Ok(None);
        }

        let mut hdr_buf = [0u8; size_of::<MapHeader>()];
        self.file.read(&mut hdr_buf[..], self.offset).map_err(|x| {
            log::error!("read file, error: {x:?}");
            OpCode::IoError
        })?;
        let hdr = MapHeader::from_slice(&hdr_buf[..]);
        self.offset += hdr_buf.len() as u64;

        // the size is limited to MAP_FILE_GC_SIZE
        if block.len() < hdr.len as usize {
            block.realloc(hdr.len as usize);
        }

        let mut hash = Crc32cHasher::default();
        let data = block.mut_slice(0, hdr.len as usize);
        self.file.read(data, self.offset).unwrap();

        hash.write(data);
        if hdr.crc32 != hash.finish() as u32 {
            log::error!("corrupt map file: {:?}", self.path);
            return Err(OpCode::BadData);
        }
        self.offset += hdr.len as u64;
        let count = hdr.len as usize / PageTable::ITEM_LEN;

        Ok(Some(unsafe {
            std::slice::from_raw_parts(block.data().cast::<MapEntry>(), count)
        }))
    }
}

#[cfg(test)]
mod test {
    use crc32c::Crc32cHasher;
    use io::GatherIO;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::Relaxed;
    use std::{cmp::Ordering, hash::Hasher};

    use crate::{
        Options, RandomPath,
        types::{
            refbox::BoxRef,
            traits::{IAsSlice, IHeader},
        },
        utils::{INIT_ID, NULL_ADDR, block::Block, data::PageTable},
    };

    use super::{DataBuilder, DataMetaReader, MapHeader, MapReader};

    struct DummyState;

    impl GatherIO for DummyState {
        fn read(&self, _data: &mut [u8], _pos: u64) -> Result<usize, std::io::Error> {
            unimplemented!()
        }

        fn sync(&mut self) -> Result<(), std::io::Error> {
            Ok(())
        }

        fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
            Ok(data.len())
        }

        fn size(&self) -> Result<u64, std::io::Error> {
            unimplemented!()
        }

        fn truncate(&self, _to: u64) -> Result<(), std::io::Error> {
            unimplemented!()
        }

        fn writev(
            &mut self,
            _data: &mut [io::IoVec],
            _total_len: usize,
        ) -> Result<(), std::io::Error> {
            unimplemented!()
        }
    }

    #[test]
    fn data_dump_load() {
        let path = RandomPath::new();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;

        let (pid, addr) = (114514, 1919810);
        let mut p = BoxRef::alloc(233, addr, 0);
        p.header_mut().pid = pid;

        let mut builder = DataBuilder::new(INIT_ID as u64, INIT_ID);

        builder.add(p.clone());

        std::fs::create_dir_all(&opt.db_root).unwrap();
        let mut state = DummyState;
        builder.build(&opt, &mut state);

        let mut loader = DataMetaReader::new(opt.data_file(builder.file_id), true).unwrap();

        let d = loader.get_meta().unwrap();
        let reloc = d.relocs();

        assert_eq!(reloc.len(), 1);

        let h = p.header();
        let r = &reloc[0];
        assert_eq!({ r.key }, addr);
        assert_eq!({ r.val.off }, 0);
        assert_eq!({ r.val.len }, h.total_size);
    }

    #[test]
    fn map_dump_load() {
        let path = RandomPath::new();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        let map_id = 1;

        std::fs::create_dir_all(&opt.db_root).unwrap();
        let mut w = io::File::options()
            .append(true)
            .write(true)
            .create(true)
            .open(&opt.map_file(map_id))
            .unwrap();
        let mut input = Vec::new();
        let mut epoch = 0;
        for _ in 0..10 {
            let mut table = PageTable::default();

            for i in 1..10 {
                table.add(i, i * i, epoch);
                input.push((i, i * i));
                epoch += 1;
            }

            let v = table.collect();
            let mut hash = Crc32cHasher::default();
            hash.write(&v);
            let h = MapHeader {
                len: v.len() as u32,
                crc32: hash.finish() as u32,
            };

            w.write(h.as_slice()).unwrap();
            w.write(&v).unwrap();
            w.sync().unwrap();
        }

        drop(w);

        let mut block = Block::alloc(MapReader::DEFAULT_BLOCK_SIZE);
        let mut r = MapReader::new(opt.map_file(map_id));
        let mut output = Vec::new();

        loop {
            let x = r.next(&mut block).unwrap();
            if let Some(es) = x {
                for e in es {
                    output.push((e.page_id, e.page_addr));
                }
            } else {
                break;
            }
        }

        input.sort_by(|x, y| match x.0.cmp(&y.0) {
            Ordering::Equal => x.1.cmp(&y.1),
            o => o,
        });

        output.sort_by(|x, y| match x.0.cmp(&y.0) {
            Ordering::Equal => x.1.cmp(&y.1),
            o => o,
        });
        assert_eq!(input, output);
    }

    #[test]
    fn map_dump_load2() {
        let path = RandomPath::new();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        let map_id = 1;

        std::fs::create_dir_all(&opt.db_root).unwrap();
        let mut w = io::File::options()
            .append(true)
            .write(true)
            .create(true)
            .open(&opt.map_file(map_id))
            .unwrap();
        let mut table = PageTable::default();
        let epoch = AtomicU64::new(0);

        table.add(1, 1, epoch.fetch_add(1, Relaxed));
        table.add(1, NULL_ADDR, epoch.fetch_add(1, Relaxed));
        table.add(2, 2, epoch.fetch_add(1, Relaxed));
        table.add(2, NULL_ADDR, epoch.fetch_add(1, Relaxed));
        table.add(2, 3, epoch.fetch_add(1, Relaxed));

        let v = table.collect();
        let mut hash = Crc32cHasher::default();
        hash.write(&v);
        let h = MapHeader {
            len: v.len() as u32,
            crc32: hash.finish() as u32,
        };

        w.write(h.as_slice()).unwrap();
        w.write(&v).unwrap();
        w.sync().unwrap();

        drop(w);

        let mut block = Block::alloc(MapReader::DEFAULT_BLOCK_SIZE);
        let mut r = MapReader::new(opt.map_file(map_id));
        let mut m = Vec::new();

        loop {
            let x = r.next(&mut block).unwrap();
            if let Some(es) = x {
                for e in es {
                    m.push((e.page_id, e.page_addr, e.epoch));
                }
            } else {
                break;
            }
        }

        assert_eq!(m.len(), 2);
        assert_eq!((m[0].0, m[0].1), (1, NULL_ADDR));
        assert_eq!((m[1].0, m[1].1), (2, 3));
    }
}
