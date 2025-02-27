use crc32c::Crc32cHasher;
use io::{File, GatherIO};

use crate::utils::bitmap::BitMap;
use crate::utils::block::Block;
use crate::utils::data::{AddrMap, MapEntry, PageTable, Reloc, ID_LEN, JUNK_LEN};
use crate::utils::{align_up, unpack_id};
use crate::utils::{bytes::ByteArray, raw_ptr_to_ref, raw_ptr_to_ref_mut};
use crate::OpCode;
use std::alloc::alloc_zeroed;
use std::cmp::min;
use std::hash::Hasher;
use std::io::Write;
use std::path::Path;
use std::ptr::{null, null_mut};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::{
    alloc::{dealloc, Layout},
    ops::{Deref, DerefMut},
};

/// NOTE: the field expect `file_id` will be changed, up1 and up2 change when new write, nr_active
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
        if !self.dealloc.full() {
            self.dealloc.set(reloc.seq);
        } else {
            log::error!("invalid function call, {:?}", reloc);
        }
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
    Slotted = 5,
    /// a frame contains addrs point to other arena
    Junk = 11,
    Unknown = 13,
}

#[derive(Debug)]
pub(crate) struct Frame {
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

    #[cfg(test)]
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

    pub fn set_slotted(&mut self) {
        debug_assert_eq!(self.flag, FrameFlag::Unknown);
        self.flag = FrameFlag::Slotted;
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

pub(crate) struct FrameOwner {
    raw: *mut Frame,
}

unsafe impl Send for FrameOwner {}
unsafe impl Sync for FrameOwner {}

impl Deref for FrameOwner {
    type Target = Frame;
    fn deref(&self) -> &Self::Target {
        raw_ptr_to_ref(self.raw)
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

    pub(crate) fn payload(&self) -> ByteArray {
        let ptr = unsafe { self.raw.add(1).cast::<u8>() };
        ByteArray::new(ptr, self.payload_size() as usize)
    }

    pub(crate) fn copy_to(&self, other: &FrameOwner) {
        let src = self.payload();
        let dst = other.payload();
        assert_eq!(src.len(), dst.len());
        unsafe {
            std::ptr::copy(src.data(), dst.data(), dst.len());
        }
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

    pub(crate) fn serialize<IO>(&self, file: &mut IO)
    where
        IO: Write,
    {
        let s = self.payload_slice();
        file.write_all(s).expect("can't write");
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

impl Default for ArenaIter {
    fn default() -> Self {
        Self {
            raw: ByteArray::new(null_mut(), 0),
            pos: 0,
            end: 0,
        }
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
/// high                                                                            low
/// +--------+-------+------+-------------+-------+---------------------------------+
/// | footer | lids | map entries | relocations | junks |  frames (normal, slotted) |
/// +--------+------+-------+-------------+-------+---------------------------------+
///
/// ```
#[repr(C, packed(1))]
#[derive(Default, Debug)]
pub(crate) struct DataFooter {
    pub(crate) up2: u32,
    /// logical id entries
    pub(crate) nr_lid: u32,
    /// page table
    pub(crate) nr_entry: u32,
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

    pub(crate) fn serialize<W: Write>(&self, w: &mut W) {
        let s = unsafe {
            let p = self as *const Self;
            std::slice::from_raw_parts(p.cast::<u8>(), size_of::<Self>())
        };
        w.write_all(s).expect("can't write");
    }

    fn lid_pos(&self) -> usize {
        Self::LEN
    }

    fn entry_pos(&self) -> usize {
        Self::LEN + self.lid_len()
    }

    fn reloc_pos(&self) -> usize {
        self.entry_pos() + self.nr_entry as usize * PageTable::ITEM_LEN
    }

    fn meta_len(&self) -> usize {
        self.lid_len() + self.entry_len() + self.reloc_len()
    }

    fn get<T>(&self, off: usize, n: usize) -> &[T] {
        let p = self as *const Self;
        unsafe {
            let p = p.cast::<u8>().add(off).cast::<T>();
            std::slice::from_raw_parts(p, n)
        }
    }

    fn entry_slice(&self) -> &[u8] {
        self.get(self.entry_pos(), self.entry_len())
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

    fn entry_len(&self) -> usize {
        self.nr_entry as usize * PageTable::ITEM_LEN
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

    pub(crate) fn entries(&self) -> &[MapEntry] {
        self.get(self.entry_pos(), self.nr_entry as usize)
    }

    pub(crate) fn relocs(&self) -> &[AddrMap] {
        self.get(self.reloc_pos(), self.nr_reloc as usize)
    }
}

pub(crate) struct DataBuilder {
    nr_rel: u32,
    nr_junk: u32,
    nr_active: u32,
    active_size: u32,
    pub(crate) junks: Vec<FrameRef>,
    entry: PageTable,
    reloc: Vec<u8>,
    frames: Vec<FrameRef>,
}

impl DataBuilder {
    fn add_impl(&mut self, f: FrameRef) {
        if f.flag() == FrameFlag::Normal {
            self.entry.add(f.page_id(), f.addr());
        }
        // including Slotted
        self.nr_active += 1;
        self.active_size += f.payload_size();
        self.nr_rel += 1;
        self.frames.push(f);
    }

    pub(crate) fn new() -> Self {
        Self {
            nr_rel: 0,
            nr_junk: 0,
            nr_active: 0,
            active_size: 0,
            junks: Vec::new(),
            entry: PageTable::default(),
            reloc: Vec::new(),
            frames: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, f: FrameRef) {
        match f.flag() {
            FrameFlag::Normal | FrameFlag::Slotted => {
                self.add_impl(f);
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

    pub(crate) fn build<W>(&mut self, w: &mut W, up2: u32)
    where
        W: Write,
    {
        let mut pos = 0;
        let mut crc = Crc32cHasher::default();

        for (seq, f) in self.frames.iter().enumerate() {
            let reloc = AddrMap::new(f.addr(), pos, f.payload_size(), seq as u32);
            pos += f.payload_size();
            self.reloc.extend_from_slice(reloc.as_slice());
            crc.write(f.payload_slice());
            f.serialize(w);
        }

        for f in &self.junks {
            crc.write(f.payload_slice());
            f.serialize(w);
        }

        crc.write(self.reloc.as_slice());
        w.write_all(self.reloc.as_slice()).unwrap();

        self.entry.hash(&mut crc);
        self.entry.serialize(w);

        let hdr = DataFooter {
            up2,
            nr_lid: 0,
            nr_entry: self.entry.len() as u32,
            nr_reloc: self.nr_rel,
            nr_junk: self.nr_junk,
            nr_active: self.nr_active,
            active_size: self.active_size,
            crc: crc.finish() as u32,
        };

        hdr.serialize(w);
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
        self.get_entry(f)?;
        self.get_reloc(f)?;
        if self.validate {
            self.calc_crc(f)
        } else {
            Ok(f.crc)
        }
    }

    fn get_footer(&mut self) -> Result<(), std::io::Error> {
        let flen = DataFooter::LEN;
        let s = self.buf.get_mut_slice(0, flen);
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
            let s = self.buf.get_mut_slice(self.pos, f.lid_len());

            self.file.read(s, self.off)?;
            self.pos += f.lid_len();
        }
        Ok(())
    }

    fn get_entry(&mut self, f: &DataFooter) -> Result<(), std::io::Error> {
        if f.entry_len() > 0 {
            self.off -= f.entry_len() as u64;
            let s = self.buf.get_mut_slice(self.pos, f.entry_len());

            self.file.read(s, self.off)?;
            self.pos += f.entry_len();
        }
        Ok(())
    }

    fn get_reloc(&mut self, f: &DataFooter) -> Result<(), std::io::Error> {
        if f.reloc_len() > 0 {
            self.off -= f.reloc_len() as u64;
            let s = self.buf.get_mut_slice(self.pos, f.reloc_len());

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
        h.write(f.entry_slice());
        h.write(f.lid_slice());

        Ok(h.finish() as u32)
    }

    pub(crate) fn take(self) -> (File, Block) {
        (self.file, self.buf)
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use crate::{utils::NEXT_ID, RandomPath};

    use super::{DataBuilder, DataMetaReader, FrameOwner};

    #[test]
    fn dump_load() {
        let path = RandomPath::tmp();
        let f = FrameOwner::alloc(233);
        let (pid, addr) = (114514, 1919810);
        let mut view = f.as_ref();

        view.set_addr(addr);
        view.set_pid(pid);

        let mut builder = DataBuilder::new();

        builder.add(view);

        let mut file = File::options()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&*path)
            .unwrap();
        builder.build(&mut file, NEXT_ID);

        let mut loader = DataMetaReader::new(&*path, true).unwrap();

        let d = loader.get_meta().unwrap();
        let map = d.entries();
        let reloc = d.relocs();

        assert_eq!(map.len(), 1);
        assert_eq!(reloc.len(), 1);

        let m = &map[0];
        assert_eq!(m.page_id(), pid);
        assert_eq!(m.page_addr(), addr);

        let r = &reloc[0];
        assert_eq!({ r.key }, addr);
        assert_eq!({ r.val.off }, 0);
        assert_eq!({ r.val.len }, f.payload_size());
    }
}
