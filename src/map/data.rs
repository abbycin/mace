use crc32c::Crc32cHasher;
use io::{File, GatherIO};

use crate::utils::bitmap::BitMap;
use crate::utils::block::Block;
use crate::utils::data::{AddrMap, GatherWriter, ID_LEN, JUNK_LEN, MapEntry, PageTable, Reloc};
use crate::utils::lru::LruInner;
use crate::utils::traits::{IAsSlice, IInfer};
use crate::utils::{MutRef, align_up, rand_range, unpack_id};
use crate::utils::{bytes::ByteArray, raw_ptr_to_ref, raw_ptr_to_ref_mut};
use crate::{OpCode, Options};
use std::alloc::alloc_zeroed;
use std::cmp::min;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use std::ptr::null;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::{
    alloc::{Layout, dealloc},
    ops::{Deref, DerefMut},
};

use super::cache::DeepCopy;

/// NOTE: the field except `file_id` will be changed, up1 and up2 change on new write, nr_active
/// and active_size change when flush have new deallocate frame point to current file_id
pub(crate) struct FileStat {
    pub(crate) file_id: u32,
    pub(crate) up1: u32,
    pub(crate) up2: u32,
    pub(crate) nr_active: u32,
    pub(crate) active_size: u32,
    // the following two field will never change
    pub(crate) total: u32,
    pub(crate) total_size: u32,
    /// when an active frame was deallocated, mask it in the bitmap
    pub(crate) dealloc: BitMap,
    pub(crate) refcnt: AtomicU32,
}

impl FileStat {
    pub(crate) fn update(&mut self, reloc: Reloc, now: u32) {
        self.nr_active -= 1;
        self.active_size -= reloc.len;
        self.dealloc.set(reloc.seq);
        // make sure monotonically increasing
        if self.up1 < now {
            self.up1 = self.up2;
            self.up2 = now;
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum FrameFlag {
    Normal = 1,
    TombStone = 3,
    /// a frame contains addrs point to other arena
    Junk = 11,
    Unknown = 13,
}

#[derive(Debug)]
pub struct Frame {
    /// the pid and addr is runtime info used to build mapping table during flush, it's useless when
    /// load from file
    pid: u64,
    /// pack_id(file_id, offset)
    addr: u64,
    /// borrowed
    borrowed: *const AtomicU32,
    /// payload size
    size: u32,
    flag: FrameFlag,
    owned: u8,
    refcnt: AtomicU32,
}

impl Frame {
    pub(crate) const FRAME_LEN: usize = size_of::<Self>();

    pub fn alloc_size(request: u32) -> u32 {
        align_up(Self::FRAME_LEN + request as usize, size_of::<usize>()) as u32
    }

    /// NOTE: the `size` is payload size
    pub fn init(&mut self, cnt: *const AtomicU32, addr: u64, flag: FrameFlag) {
        self.addr = addr;
        self.flag = flag;
        self.pid = 0;
        self.owned = 0;
        self.borrowed = cnt;
        self.refcnt.store(0, Relaxed);
    }

    // only can be use when traverse arena and deallocate frame itself
    pub fn size(&self) -> u32 {
        Self::alloc_size(self.size)
    }

    pub fn addr(&self) -> u64 {
        self.addr
    }

    pub fn payload_size(&self) -> u32 {
        self.size
    }

    pub fn set_size(&mut self, size: u32) {
        self.size = size;
    }

    pub fn set_addr(&mut self, addr: u64) {
        self.addr = addr;
    }

    pub fn set_pid(&mut self, pid: u64) {
        debug_assert_eq!(self.pid, 0);
        debug_assert!(self.flag == FrameFlag::Unknown || self.flag == FrameFlag::TombStone);
        self.pid = pid;
        self.flag = FrameFlag::Normal;
    }

    pub fn page_id(&self) -> u64 {
        self.pid
    }

    pub fn flag(&self) -> FrameFlag {
        self.flag
    }

    pub fn set_tombstone(&mut self) {
        self.flag = FrameFlag::TombStone;
    }

    pub fn set_normal(&mut self) {
        debug_assert_eq!(self.flag, FrameFlag::Unknown);
        self.flag = FrameFlag::Normal;
    }

    pub fn fill_junk(&mut self, junks: &[u64]) {
        self.flag = FrameFlag::Junk;
        let ptr = self as *mut Self;
        let dst = unsafe {
            std::slice::from_raw_parts_mut(
                ptr.add(1).cast::<u64>(),
                self.payload_size() as usize / JUNK_LEN,
            )
        };
        dst.copy_from_slice(junks);
    }
}

impl DeepCopy for FrameOwner {
    // NOTE: excluding `borrowed`
    fn deep_copy(self) -> Self {
        if self.owned == 1 {
            return self;
        }
        let mut other = FrameOwner::alloc(self.payload_size() as usize);
        debug_assert_eq!(self.size(), other.size());
        let src = self.payload();
        let dst = other.payload();
        assert_eq!(src.len(), dst.len());
        unsafe {
            std::ptr::copy(src.data(), dst.data(), dst.len());
        }
        debug_assert!(other.borrowed.is_null());
        // used in gc
        other.set_addr(self.addr);
        other
    }
}

pub struct FrameOwner {
    raw: *mut Frame,
}

unsafe impl Send for FrameOwner {}
unsafe impl Sync for FrameOwner {}

impl IInfer for FrameOwner {
    fn infer(&self) -> ByteArray {
        self.payload()
    }
}

impl Deref for FrameOwner {
    type Target = Frame;
    fn deref(&self) -> &Self::Target {
        raw_ptr_to_ref(self.raw)
    }
}

impl DerefMut for FrameOwner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        raw_ptr_to_ref_mut(self.raw)
    }
}

impl FrameOwner {
    pub(crate) fn alloc(size: usize) -> Self {
        let real_size = Frame::alloc_size(size as u32);
        let raw = unsafe {
            let f = alloc_zeroed(Layout::array::<*const ()>(real_size as usize).unwrap())
                .cast::<Frame>();
            &mut *f
        };
        raw.size = size as u32;
        raw.flag = FrameFlag::Unknown;
        raw.owned = 1;
        raw.borrowed = null();
        raw.refcnt.store(1, Relaxed);
        Self { raw }
    }

    pub(crate) fn from(raw: *mut Frame) -> Self {
        unsafe {
            let b = (*raw).borrowed;
            if !b.is_null() {
                (*b).fetch_add(1, Relaxed);
            }
        }
        Self { raw }
    }

    #[cfg(test)]
    pub(crate) fn as_ref(&self) -> FrameRef {
        FrameRef::new(self.raw)
    }

    #[cfg(test)]
    pub(crate) fn data(&self) -> ByteArray {
        ByteArray::new(self.raw as *mut u8, self.size() as usize)
    }

    pub fn payload(&self) -> ByteArray {
        let ptr = unsafe { self.raw.add(1).cast::<u8>() };
        ByteArray::new(ptr, self.payload_size() as usize)
    }
}

impl Drop for FrameOwner {
    fn drop(&mut self) {
        if !self.borrowed.is_null() {
            unsafe {
                (*self.borrowed).fetch_sub(1, Relaxed);
            }
        }
        if self.owned == 1 && self.refcnt.fetch_sub(1, Relaxed) == 1 {
            unsafe {
                dealloc(
                    self.raw as *mut u8,
                    Layout::array::<*const ()>(self.size() as usize).unwrap(),
                );
            }
        }
    }
}

impl Clone for FrameOwner {
    fn clone(&self) -> Self {
        if !self.borrowed.is_null() {
            unsafe {
                (*self.borrowed).fetch_add(1, Relaxed);
            }
        }
        if self.owned == 1 {
            self.refcnt.fetch_add(1, Relaxed);
        }
        Self { raw: self.raw }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct FrameRef {
    raw: *mut Frame,
}

unsafe impl Send for FrameRef {}
unsafe impl Sync for FrameRef {}

impl Deref for FrameRef {
    type Target = Frame;
    fn deref(&self) -> &Self::Target {
        raw_ptr_to_ref(self.raw)
    }
}

impl DerefMut for FrameRef {
    fn deref_mut(&mut self) -> &mut Self::Target {
        raw_ptr_to_ref_mut(self.raw)
    }
}

impl FrameRef {
    pub(crate) fn new(ptr: *mut Frame) -> Self {
        Self { raw: ptr }
    }

    pub(crate) fn payload(&self) -> ByteArray {
        let ptr = unsafe { self.raw.add(1).cast::<u8>() };
        ByteArray::new(ptr, self.payload_size() as usize)
    }

    fn payload_slice(&self) -> &[u8] {
        self.payload().as_slice(0, self.payload_size() as usize)
    }
}

pub(crate) struct FlushData {
    id: u32,
    pub(crate) iter: ArenaIter,
    cb: Box<dyn FnOnce()>,
}

unsafe impl Send for FlushData {}

impl FlushData {
    pub fn new(id: u32, iter: ArenaIter, cb: Box<dyn FnOnce()>) -> Self {
        Self { id, iter, cb }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn mark_done(self) {
        (self.cb)()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ArenaIter {
    raw: ByteArray,
    pos: u32,
    end: u32,
}

impl ArenaIter {
    pub(crate) fn new(raw: ByteArray, end: u32) -> Self {
        Self { raw, pos: 0, end }
    }
}

impl Iterator for ArenaIter {
    type Item = FrameRef;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.end {
            return None;
        }
        let f = unsafe {
            let data = self.raw.data().add(self.pos as usize).cast::<Frame>();
            FrameRef::new(data)
        };

        self.pos += f.size();

        Some(f)
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
    pub(crate) up2: u32,
    /// logical id entries
    pub(crate) nr_lid: u32,
    /// item's relocation table
    pub(crate) nr_reloc: u32,
    /// count of junks
    pub(crate) nr_junk: u32,
    /// active frames, only calculate the Normal and Slotted
    pub(crate) nr_active: u32,
    /// active frame size, also the initial total size
    pub(crate) active_size: u32,
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
        self.nr_reloc as usize * AddrMap::LEN
    }

    fn junk_len(&self) -> usize {
        self.nr_junk as usize * JUNK_LEN
    }

    pub(crate) fn lids(&self) -> &[u32] {
        self.get(self.lid_pos(), self.nr_lid as usize)
    }

    pub(crate) fn relocs(&self) -> &[AddrMap] {
        self.get(self.reloc_pos(), self.nr_reloc as usize)
    }
}

pub(crate) struct DataBuilder {
    id: u32,
    nr_rel: u32,
    nr_junk: u32,
    nr_active: u32,
    active_size: u32,
    pub(crate) junks: Vec<FrameRef>,
    reloc: Vec<u8>,
    frames: Vec<FrameRef>,
}

impl DataBuilder {
    pub(crate) fn new(id: u32) -> Self {
        Self {
            id,
            nr_rel: 0,
            nr_junk: 0,
            nr_active: 0,
            active_size: 0,
            junks: Vec::new(),
            reloc: Vec::new(),
            frames: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, f: FrameRef) {
        match f.flag() {
            FrameFlag::Normal => {
                self.nr_active += 1;
                self.active_size += f.payload_size();
                self.nr_rel += 1;
                self.frames.push(f);
            }
            FrameFlag::Junk => {
                self.nr_junk += f.payload_size() / JUNK_LEN as u32;
                self.junks.push(f);
            }
            FrameFlag::TombStone => {}
            FrameFlag::Unknown => unreachable!("invalid frame {:?}", unpack_id(f.addr())),
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
        let mut pos = 0;
        let mut crc = Crc32cHasher::default();
        let path = opt.data_file(self.id);
        let mut w = GatherWriter::new(&path);

        let ctx = DataState {
            kind: StateType::Data,
            file_id: self.id,
        };
        state.write(ctx.as_slice()).unwrap();
        state.sync().unwrap();

        for (seq, f) in self.frames.iter().enumerate() {
            let reloc = AddrMap::new(f.addr(), pos, f.payload_size(), seq as u32);
            pos += f.payload_size();
            self.reloc.extend_from_slice(reloc.as_slice());
            let s = f.payload_slice();
            crc.write(s);
            w.queue(s);
        }

        for f in &self.junks {
            let s = f.payload_slice();
            crc.write(s);
            w.queue(s);
        }

        let s = self.reloc.as_slice();
        crc.write(s);
        w.queue(s);

        let hdr = DataFooter {
            up2: self.id,
            nr_lid: 0,
            nr_reloc: self.nr_rel,
            nr_junk: self.nr_junk,
            nr_active: self.nr_active,
            active_size: self.active_size,
            crc: crc.finish() as u32,
        };

        w.queue(hdr.as_slice());
        w.flush();
        w.sync();
    }
}

pub(crate) struct DataMetaReader {
    file: File,
    pos: usize,
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
            log::error!("io error {}", e);
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
        let mut v: Vec<u64> = Vec::with_capacity(f.junk_len() / JUNK_LEN);
        let off = self.off - f.junk_len() as u64;
        let s = unsafe {
            let ptr = v.as_mut_ptr().cast::<u8>();
            std::slice::from_raw_parts_mut(ptr, f.junk_len())
        };
        self.file.read(s, off).map_err(|e| {
            log::error!("read error {}", e);
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

    pub(crate) fn add(&mut self, f: FrameRef) {
        if f.flag() == FrameFlag::Normal {
            let id = pid_to_fid(f.page_id());
            match self.tables.entry(id) {
                Entry::Occupied(ref mut x) => x.get_mut().add(f.page_id(), f.addr()),
                Entry::Vacant(x) => {
                    let mut table = PageTable::default();
                    table.add(f.page_id(), f.addr());
                    let _ = x.insert(table);
                }
            }
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
                        map.add(e.page_id(), e.page_addr());
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
            log::error!("read file, error: {:?}", x);
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
    use std::{cmp::Ordering, hash::Hasher};

    use crc32c::Crc32cHasher;
    use io::GatherIO;

    use crate::{
        Options, RandomPath,
        utils::{NEXT_ID, block::Block, data::PageTable, traits::IAsSlice},
    };

    use super::{DataBuilder, DataMetaReader, FrameOwner, MapHeader, MapReader};

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

        fn writev(&mut self, _data: &mut [io::IoVec]) -> Result<(), std::io::Error> {
            unimplemented!()
        }
    }

    #[test]
    fn data_dump_load() {
        let path = RandomPath::new();
        let f = FrameOwner::alloc(233);
        let (pid, addr) = (114514, 1919810);
        let mut view = f.as_ref();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;

        view.set_addr(addr);
        view.set_pid(pid);

        let mut builder = DataBuilder::new(NEXT_ID);

        builder.add(view);

        std::fs::create_dir_all(&opt.db_root).unwrap();
        let mut state = DummyState;
        builder.build(&opt, &mut state);

        let mut loader = DataMetaReader::new(opt.data_file(builder.id), true).unwrap();

        let d = loader.get_meta().unwrap();
        let reloc = d.relocs();

        assert_eq!(reloc.len(), 1);

        let r = &reloc[0];
        assert_eq!({ r.key }, addr);
        assert_eq!({ r.val.off }, 0);
        assert_eq!({ r.val.len }, f.payload_size());
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
        for _ in 0..10 {
            let mut table = PageTable::default();

            for i in 1..10 {
                table.add(i, i * i);
                input.push((i, i * i));
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
                    output.push((e.page_id(), e.page_addr()));
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
}
