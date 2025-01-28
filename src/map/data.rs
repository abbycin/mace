use crc32c::{crc32c_combine, Crc32cHasher};
use io::{File, SeekableGatherIO};

use crate::utils::block::Block;
use crate::utils::data::{AddrMap, MapEntry, PageTable};
use crate::utils::{align_up, unpack_id};
use crate::utils::{byte_array::ByteArray, raw_ptr_to_ref, raw_ptr_to_ref_mut};
use crate::OpCode;
use std::hash::Hasher;
use std::io::Write;
use std::path::Path;
use std::ptr::null_mut;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU16, AtomicU64};
use std::sync::Arc;
use std::{
    alloc::{alloc, dealloc, Layout},
    ops::{Deref, DerefMut},
};

/// a normal frame contains data, a deleted frame is table of addresses (file id + offset) point to
/// deleted data, a tombstone is a single deleted data in current page file
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u16)]
pub enum FrameFlag {
    Normal = 1,
    TombStone = 3,
    Slotted = 5,
    Unknown = 7,
}

#[repr(C)]
pub(crate) struct Frame {
    pid: u64,
    /// encode_u64(file_id, offset)
    addr: u64,
    /// the size excluding [`Frame`] itself (i.e., it's payload size)
    size: u32,
    flag: FrameFlag,
    state: AtomicU16,
}

impl Frame {
    pub(crate) const FRAME_LEN: usize = size_of::<Self>();
    pub(crate) const STATE_ACTIVE: u16 = 13;
    pub(crate) const STATE_INACTIVE: u16 = 17;
    pub(crate) const STATE_DEAD: u16 = 23;

    pub fn alloc_size(request: u32) -> u32 {
        align_up(Self::FRAME_LEN + request as usize, size_of::<usize>()) as u32
    }

    /// NOTE: the `size` is payload size
    pub fn init(&mut self, addr: u64, flag: FrameFlag) {
        self.addr = addr;
        self.flag = flag;
        self.pid = 0;
        self.state.store(Self::STATE_ACTIVE, Relaxed);
    }

    pub fn set_state(&self, current: u16, new: u16) -> u16 {
        self.state
            .compare_exchange(current, new, Relaxed, Relaxed)
            .unwrap_or_else(|x| x)
    }

    pub fn state(&self) -> u16 {
        self.state.load(Relaxed)
    }

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
            let f = alloc(Layout::array::<*const ()>(real_size as usize).unwrap()).cast::<Frame>();
            (*f).size = size as u32;
            (*f).flag = FrameFlag::Unknown;
            (*f).pid = 0;
            f
        };
        Self { raw }
    }

    #[cfg(test)]
    pub(crate) fn view(&self) -> FrameView {
        FrameView::new(self.raw)
    }

    pub(crate) fn data(&self) -> ByteArray {
        ByteArray::new(self.raw as *mut u8, self.size() as usize)
    }

    pub(crate) fn payload(&self) -> ByteArray {
        let ptr = unsafe { self.raw.add(1).cast::<u8>() };
        ByteArray::new(ptr, self.payload_size() as usize)
    }
}

impl Drop for FrameOwner {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.raw as *mut u8,
                Layout::array::<*const ()>(self.size() as usize).unwrap(),
            );
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct FrameView {
    raw: *mut Frame,
}

unsafe impl Send for FrameView {}
unsafe impl Sync for FrameView {}

impl Deref for FrameView {
    type Target = Frame;
    fn deref(&self) -> &Self::Target {
        raw_ptr_to_ref(self.raw)
    }
}

impl DerefMut for FrameView {
    fn deref_mut(&mut self) -> &mut Self::Target {
        raw_ptr_to_ref_mut(self.raw)
    }
}

impl FrameView {
    pub(crate) fn new(ptr: *mut Frame) -> Self {
        Self { raw: ptr }
    }

    pub(crate) fn payload(&self) -> ByteArray {
        let ptr = unsafe { self.raw.add(1).cast::<u8>() };
        ByteArray::new(ptr, self.payload_size() as usize)
    }

    pub(crate) fn copy_to(&self, other: &FrameOwner) {
        unsafe {
            std::ptr::copy(
                self.raw as *mut u8,
                other.raw as *mut u8,
                self.size() as usize,
            );
        }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.raw as *mut u8, self.size() as usize) }
    }

    pub(crate) fn serialize<IO>(&self, file: &mut IO)
    where
        IO: Write,
    {
        let s = self.as_slice();
        file.write_all(s).expect("can't write");
    }
}

pub(crate) struct FlushData {
    id: u16,
    now: u64,
    tick: Arc<AtomicU64>,
    pub(crate) iter: ArenaIter,
    cb: Box<dyn Fn()>,
}

unsafe impl Send for FlushData {}

impl FlushData {
    pub fn new(id: u16, tick: Arc<AtomicU64>, iter: ArenaIter, cb: Box<dyn Fn()>) -> Self {
        Self {
            id,
            now: tick.load(Relaxed),
            tick,
            iter,
            cb,
        }
    }

    pub fn still(&self) -> bool {
        self.tick.load(Relaxed) == self.now
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn mark_done(&self) {
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
    type Item = FrameView;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.end {
            return None;
        }
        let f = unsafe {
            let data = self.raw.data().add(self.pos as usize).cast::<Frame>();
            FrameView::new(data)
        };

        self.pos += f.size();

        Some(f)
    }
}

#[repr(C, packed(1))]
#[derive(Default, Debug)]
pub(crate) struct DataHeader {
    nr_map: u32,
    nr_reloc: u32,
    crc: u64,
    len: u64,
}

impl DataHeader {
    pub(crate) const LEN: usize = size_of::<Self>();

    fn serialize<W: Write>(&self, w: &mut W) {
        let s = unsafe {
            let p = self as *const Self;
            std::slice::from_raw_parts(p.cast::<u8>(), size_of::<Self>())
        };
        w.write_all(s).expect("can't write");
    }

    fn _size(nr_map: u32, nr_reloc: u32) -> usize {
        nr_map as usize * PageTable::ITEM_LEN + nr_reloc as usize * AddrMap::LEN + Self::LEN
    }

    fn get<T>(&self, off: usize, n: usize) -> &[T] {
        let p = self as *const Self;
        unsafe {
            let p = p.cast::<u8>().add(off).cast::<T>();
            std::slice::from_raw_parts(p, n)
        }
    }

    pub(crate) fn meta_size(&self) -> usize {
        Self::_size(self.nr_map, self.nr_reloc)
    }

    /// total size including header itself
    pub(crate) fn size(&self) -> usize {
        self.meta_size() + self.len as usize
    }

    pub(crate) fn maps(&self) -> &[MapEntry] {
        let off = Self::LEN;
        self.get(off, self.nr_map as usize)
    }

    pub(crate) fn relocs(&self) -> &[AddrMap] {
        let off = Self::LEN + self.nr_map as usize * PageTable::ITEM_LEN;
        self.get(off, self.nr_reloc as usize)
    }

    pub(crate) fn is_intact(&self) -> bool {
        let payload = self.get(Self::LEN, self.size() - Self::LEN);
        let mut h = Crc32cHasher::default();
        h.write(payload);
        if h.finish() != self.crc {
            log::error!(
                "invalid checksum, expect {} get {}",
                { self.crc },
                h.finish()
            );
            false
        } else {
            true
        }
    }
}

pub(crate) struct DataBuilder {
    nr_rel: u32,
    table: PageTable,
    reloc: Vec<u8>,
    frames: Vec<FrameView>,
}

impl Deref for DataBuilder {
    type Target = Vec<FrameView>;
    fn deref(&self) -> &Self::Target {
        &self.frames
    }
}

impl DerefMut for DataBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.frames
    }
}

impl DataBuilder {
    fn add_impl(&mut self, f: FrameView) {
        if f.flag() == FrameFlag::Normal {
            self.table.add(f.page_id(), f.addr());
        }
        self.nr_rel += 1;
        self.frames.push(f);
    }

    pub(crate) fn new() -> Self {
        Self {
            nr_rel: 0,
            table: PageTable::default(),
            reloc: Vec::new(),
            frames: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, f: FrameView) {
        match f.flag() {
            FrameFlag::Normal | FrameFlag::Slotted => {
                debug_assert_eq!(f.state(), Frame::STATE_DEAD);
                self.add_impl(f);
            }
            FrameFlag::Unknown => unreachable!("invalid frame {:?}", unpack_id(f.addr())),
            _ => {}
        }
    }

    pub(crate) fn build<W>(&mut self, off: u64, w: &mut W) -> u64
    where
        W: Write,
    {
        let hdr_sz = DataHeader::_size(self.table.len() as u32, self.nr_rel) as u64;
        let mut pos = off + hdr_sz;
        let mut body = Crc32cHasher::default();

        for f in &self.frames {
            let reloc = AddrMap::new(unpack_id(f.addr()).1, pos, f.payload_size());
            pos += f.size() as u64;
            self.reloc.extend_from_slice(reloc.as_slice());
            body.write(f.as_slice());
        }
        let len = pos - off - hdr_sz;
        let mut meta = Crc32cHasher::default();

        self.table.hash(&mut meta);
        meta.write(self.reloc.as_slice());

        let hdr = DataHeader {
            nr_map: self.table.len() as u32,
            nr_reloc: self.nr_rel,
            crc: crc32c_combine(meta.finish() as u32, body.finish() as u32, len as usize) as u64,
            len,
        };

        hdr.serialize(w);

        // meta
        self.table.serialize(w);
        w.write_all(self.reloc.as_slice()).expect("cant' write");

        // body
        for f in &self.frames {
            f.serialize(w);
        }

        len + hdr_sz
    }
}

pub(crate) struct DataLoader {
    file: File,
    off: u64,
    end: u64,
    block: Block,
}

impl DataLoader {
    const BUF_LEN: usize = 4096;

    pub(crate) fn new<T: AsRef<Path>>(path: T) -> Result<Self, OpCode> {
        let file = File::options()
            .read(true)
            .write(true)
            .trunc(false)
            .open(&path.as_ref().to_path_buf())
            .map_err(|x| {
                log::error!("can't open {:?} {}", path.as_ref(), x);
                OpCode::IoError
            })?;

        let end = file.size().expect("can't get file size");

        Ok(Self {
            file,
            off: 0,
            end,
            block: Block::alloc(Self::BUF_LEN),
        })
    }

    pub(crate) fn read_only(file: File, off: u64) -> Self {
        let end = file.size().expect("can't get file size");
        Self {
            file,
            off,
            end,
            block: Block::alloc(Self::BUF_LEN),
        }
    }

    pub(crate) fn get_meta(&mut self) -> Option<&DataHeader> {
        let off = size_of::<DataHeader>();
        let size = {
            let s = self.block.get_mut_slice(0, off);
            self.file.read(s, self.off).ok()?;
            let hdr = raw_ptr_to_ref(s.as_mut_ptr().cast::<DataHeader>());
            hdr.size()
        };

        if size > self.block.len() {
            self.block.realloc(size);
        }

        let s = self.block.get_mut_slice(off, size - off);
        self.file.read(s, self.off + off as u64).ok()?;
        self.off += size as u64;
        Some(raw_ptr_to_ref(self.block.data().cast::<DataHeader>()))
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.off == self.end
    }

    pub(crate) fn offset(&self) -> u64 {
        self.off
    }

    pub(crate) fn truncate(&self) {
        self.file.truncate(self.off).expect("can't truncate file");
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::sync::atomic::Ordering::Relaxed;

    use crate::{map::data::Frame, utils::unpack_id, RandomPath};

    use super::{DataBuilder, DataLoader, FrameOwner};

    #[test]
    fn dump_load() {
        let path = RandomPath::tmp();
        let f = FrameOwner::alloc(233);
        let (pid, addr) = (114514, 1919810);
        let mut view = f.view();

        view.init(addr, super::FrameFlag::Unknown);
        view.set_pid(pid);

        let mut builder = DataBuilder::new();

        f.state.store(Frame::STATE_DEAD, Relaxed);
        builder.add(view);

        let mut file = File::options()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&*path)
            .unwrap();
        builder.build(0, &mut file);

        let mut loader = DataLoader::new(&*path).unwrap();

        let d = loader.get_meta().unwrap();
        let map = d.maps();
        let reloc = d.relocs();

        assert!(d.is_intact());

        assert_eq!(map.len(), 1);
        assert_eq!(reloc.len(), 1);

        let m = &map[0];
        assert_eq!(m.page_id(), pid);
        assert_eq!(m.page_addr(), addr);

        let r = &reloc[0];
        assert_eq!({ r.key }, unpack_id(addr).1);
        assert_eq!({ r.val.off }, d.meta_size() as u64);
        assert_eq!({ r.val.len }, f.payload_size());
    }
}
