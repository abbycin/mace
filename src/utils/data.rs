use crate::types::traits::IAsSlice;

use super::{INIT_ID, MutRef, rand_range};
use crc32c::Crc32cHasher;
use io::{GatherIO, IoVec};
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hasher;
use std::io::Write;
use std::path::{Path, PathBuf};

/// packed logical id and offset
pub(crate) const JUNK_LEN: usize = size_of::<u64>();

#[derive(Clone, Copy, Debug)]
#[repr(C, packed(1))]
pub struct Reloc {
    /// frame offset in page file
    pub(crate) off: usize,
    /// frame's length including header
    pub(crate) len: u32,
    /// index in reclocation table
    pub(crate) seq: u32,
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
    pub fn new(key: u64, off: usize, len: u32, seq: u32) -> Self {
        Self {
            key,
            val: Reloc { off, len, seq },
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            let p = self as *const Self as *const u8;
            std::slice::from_raw_parts(p, Self::LEN)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AddrMap {
    pub epoch: u64,
    /// pack_id(file_id, offset)
    pub addr: u64,
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct MapEntry {
    pub page_id: u64,
    // NULL_ADDR for delete mark
    pub page_addr: u64,
    pub epoch: u64,
}

impl IAsSlice for MapEntry {}

pub struct GatherWriter {
    path: PathBuf,
    file: io::File,
    queue: Vec<IoVec>,
    queued_len: usize,
    max_iovcnt: usize,
    trunc: bool,
}

unsafe impl Send for GatherWriter {}

impl GatherWriter {
    /// max iovec count is limited to 1024 in Linux
    pub(crate) const MAX_IOVCNT: usize = 1024;
    pub(crate) const DEFAULT_IOVCNT: usize = 64;

    fn open(path: &PathBuf, trunc: bool) -> io::File {
        io::File::options()
            .write(true)
            .append(true)
            .trunc(trunc)
            .create(true)
            .open(path)
            .inspect_err(|x| {
                log::error!("can't open {path:?}, {x}");
            })
            .unwrap()
    }

    fn create(path: &PathBuf, max_iovcnt: usize, trunc: bool) -> Self {
        Self {
            path: path.clone(),
            file: Self::open(path, trunc),
            queue: Vec::with_capacity(max_iovcnt),
            queued_len: 0,
            max_iovcnt: if max_iovcnt >= Self::MAX_IOVCNT {
                Self::DEFAULT_IOVCNT
            } else {
                max_iovcnt
            },
            trunc,
        }
    }

    pub fn trunc(path: &PathBuf, max_iovcnt: usize) -> Self {
        Self::create(path, max_iovcnt, true)
    }

    pub fn append(path: &PathBuf, max_iovcnt: usize) -> Self {
        Self::create(path, max_iovcnt, false)
    }

    pub fn reset(&mut self, path: &PathBuf) {
        self.path = path.clone();
        self.flush();
        self.file = Self::open(path, self.trunc);
    }

    pub fn queue(&mut self, data: &[u8]) {
        if self.queue.len() >= self.max_iovcnt {
            self.flush();
        }
        self.queue.push(data.into());
        self.queued_len += data.len();
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn pos(&self) -> u64 {
        self.file
            .size()
            .inspect_err(|x| {
                log::error!("can't get metadata of {:?}, {}", self.path, x);
            })
            .unwrap()
    }

    pub fn write(&mut self, data: &[u8]) {
        self.file.write(data).unwrap();
    }

    pub fn flush(&mut self) {
        let iov = self.queue.as_mut_slice();
        self.file
            .writev(iov, self.queued_len)
            .inspect_err(|x| log::error!("can't write iov, {x}"))
            .unwrap();
        self.queued_len = 0;
        self.queue.clear();
    }

    pub fn sync(&mut self) {
        self.file
            .sync()
            .inspect_err(|x| log::error!("can't sync {:?}, {}", self.path, x))
            .unwrap();
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

#[derive(Debug)]
#[repr(C)]
pub(crate) struct WalDesc {
    pub checkpoint: Position,
    pub wal_id: u64,
    pub worker: u16,
    padding: u16,
    pub checksum: u32,
}

impl WalDesc {
    pub(crate) fn new(wid: u16) -> Self {
        Self {
            checkpoint: Position::default(),
            wal_id: INIT_ID as u64,
            worker: wid,
            padding: 0,
            checksum: 0,
        }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe {
            let p = self as *const Self as *const u8;
            std::slice::from_raw_parts(p, size_of::<WalDesc>())
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe {
            let p = self as *mut Self as *mut u8;
            std::slice::from_raw_parts_mut(p, size_of::<WalDesc>())
        }
    }

    pub fn crc32(&self) -> u32 {
        let s = self.as_slice();
        let src = &s[0..s.len() - size_of::<u32>()];
        let mut h = Crc32cHasher::default();
        h.write(src);
        h.finish() as u32
    }

    pub fn is_valid(&self) -> bool {
        self.crc32() == self.checksum
    }

    pub fn sync<P>(&mut self, path: P)
    where
        P: AsRef<Path>,
    {
        let s = format!(
            "{}_{}",
            path.as_ref().as_os_str().to_str().unwrap(),
            rand_range(114..514)
        );
        let tmp = Path::new(&s);
        let mut f = File::options()
            .write(true)
            .truncate(true)
            .create(true)
            .open(tmp)
            .expect("can't open desc file");

        self.checksum = self.crc32();
        f.write_all(self.as_slice()).expect("can't write desc file");
        f.sync_all().expect("can't sync desc file");
        drop(f);

        std::fs::rename(tmp, path).expect("can't fail");
    }
}

pub(crate) type WalDescHandle = MutRef<WalDesc>;
