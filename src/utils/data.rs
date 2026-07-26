use crate::types::traits::IAsSlice;
use crate::{Options, must_ok};

use crate::io::{self, FileSystem, GatherIO, IoVec};
use std::fmt::Debug;
use std::path::PathBuf;

#[derive(Clone, Copy)]
pub struct LenSeq {
    pub raw_len: u32,
    pub compressed_len: u32,
    pub seq: u32,
}

impl LenSeq {
    pub const fn new(raw_len: u32, compressed_len: u32, seq: u32) -> Self {
        Self {
            raw_len,
            compressed_len,
            seq,
        }
    }

    pub const fn active_len(self) -> u32 {
        if self.compressed_len == 0 {
            self.raw_len
        } else {
            self.compressed_len
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C, packed(1))]
pub struct Reloc {
    /// frame offset in page file
    pub(crate) off: usize,
    /// decoded record length
    pub(crate) raw_len: u32,
    /// stored compressed length, 0 means raw
    pub(crate) compressed_len: u32,
    /// index in relocation table
    pub(crate) seq: u32,
    /// checksum of page
    pub(crate) crc: u32,
}

impl Reloc {
    pub const fn active_len(self) -> u32 {
        if self.compressed_len == 0 {
            self.raw_len
        } else {
            self.compressed_len
        }
    }

    #[inline]
    pub const fn raw_len(self) -> u32 {
        self.raw_len
    }

    #[inline]
    pub const fn compressed_len(self) -> u32 {
        self.compressed_len
    }

    #[inline]
    pub const fn is_compressed(&self) -> bool {
        self.compressed_len != 0
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct AddrPair {
    /// logical address
    pub(crate) key: u64,
    /// relocated address
    pub(crate) val: Reloc,
}

impl AddrPair {
    pub const LEN: usize = size_of::<Self>();
    pub fn new(
        key: u64,
        off: usize,
        raw_len: u32,
        compressed_len: u32,
        seq: u32,
        crc: u32,
    ) -> Self {
        Self {
            key,
            val: Reloc {
                off,
                raw_len,
                compressed_len,
                seq,
                crc,
            },
        }
    }
}

impl IAsSlice for AddrPair {}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct MapEntry {
    pub page_id: u64,
    // NULL_ADDR for delete mark
    pub page_addr: u64,
}

impl IAsSlice for MapEntry {}

#[derive(Clone, Copy)]
#[repr(C, packed(1))]
pub struct Interval {
    pub lo: u64,
    pub hi: u64,
}

impl Debug for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[{}, {}]", { self.lo }, { self.hi }))
    }
}

impl Interval {
    pub const LEN: usize = size_of::<Self>();

    pub const fn new(lo: u64, hi: u64) -> Self {
        Self { lo, hi }
    }
}

impl IAsSlice for Interval {}

pub struct GatherWriter {
    path: PathBuf,
    file: io::File,
    queue: Vec<IoVec>,
    owned: Vec<Vec<u8>>,
    queued_len: usize,
    max_iovcnt: usize,
}

unsafe impl Send for GatherWriter {}

impl GatherWriter {
    /// max iovec count is limited to 1024 in Linux
    pub(crate) const MAX_IOVCNT: usize = 1024;
    pub(crate) const DEFAULT_IOVCNT: usize = 64;

    fn open(fs: &dyn FileSystem, path: &PathBuf, trunc: bool) -> io::File {
        must_ok!(
            io::File::options()
                .write(true)
                .append(true)
                .trunc(trunc)
                .create(true)
                .open(fs, path),
            "can't open {:?}",
            path
        )
    }

    fn create(fs: &dyn FileSystem, path: &PathBuf, max_iovcnt: usize, trunc: bool) -> Self {
        Self {
            path: path.clone(),
            file: Self::open(fs, path, trunc),
            queue: Vec::with_capacity(max_iovcnt),
            owned: Vec::new(),
            queued_len: 0,
            max_iovcnt: if max_iovcnt >= Self::MAX_IOVCNT {
                Self::DEFAULT_IOVCNT
            } else {
                max_iovcnt
            },
        }
    }

    pub fn trunc(fs: &dyn FileSystem, path: &PathBuf, max_iovcnt: usize) -> Self {
        Self::create(fs, path, max_iovcnt, true)
    }

    pub fn append(fs: &dyn FileSystem, path: &PathBuf, max_iovcnt: usize) -> Self {
        Self::create(fs, path, max_iovcnt, false)
    }

    pub fn queue(&mut self, data: &[u8]) {
        if self.queue.len() >= self.max_iovcnt {
            self.flush();
        }
        self.queue.push(data.into());
        self.queued_len += data.len();
    }

    pub fn queue_owned(&mut self, data: Vec<u8>) {
        if self.queue.len() >= self.max_iovcnt {
            self.flush();
        }
        self.queued_len += data.len();
        self.owned.push(data);
        let slice = self
            .owned
            .last()
            .expect("owned payload must exist")
            .as_slice();
        self.queue.push(slice.into());
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn pos(&self) -> u64 {
        must_ok!(self.file.size(), "path: {:?}", self.path)
    }

    pub fn write(&mut self, data: &[u8]) {
        must_ok!(self.file.write(data), "path: {:?}", self.path);
    }

    pub fn flush(&mut self) {
        let iov = self.queue.as_mut_slice();
        must_ok!(
            self.file.writev(iov, self.queued_len),
            "path: {:?}",
            self.path
        );
        self.queued_len = 0;
        self.queue.clear();
        self.owned.clear();
    }

    pub fn sync(&mut self) {
        must_ok!(self.file.sync(), "path {:?}", self.path);
    }

    pub fn sync_data(&mut self) {
        must_ok!(self.file.sync_data(), "path: {:?}", self.path);
    }
}

impl Drop for GatherWriter {
    fn drop(&mut self) {
        self.flush();
    }
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct Position {
    pub file_id: u64,
    pub offset: u64,
}

pub type GroupPositions = [Position; Options::MAX_CONCURRENT_WRITE as usize];
pub const fn init_group_pos() -> GroupPositions {
    [Position::MIN; Options::MAX_CONCURRENT_WRITE as usize]
}

impl Position {
    pub const MIN: Self = Position::new(u64::MIN, u64::MIN);

    pub const fn new(id: u64, off: u64) -> Self {
        Self {
            file_id: id,
            offset: off,
        }
    }
}

impl Ord for Position {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.file_id.cmp(&other.file_id) {
            std::cmp::Ordering::Equal => self.offset.cmp(&other.offset),
            o => o,
        }
    }
}

impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Position {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Position {}
