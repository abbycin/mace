use crate::OpCode;
use crate::types::traits::IAsSlice;

use super::{INIT_ID, MutRef, rand_range};
use crate::io::{self, GatherIO, IoVec};
use crc32c::Crc32cHasher;
use parking_lot::Mutex;
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hasher;
use std::io::Write;
use std::path::{Path, PathBuf};

/// packed logical id and offset
pub(crate) const JUNK_LEN: usize = size_of::<u64>();

#[derive(Clone, Copy)]
pub struct LenSeq {
    pub len: u32,
    pub seq: u32,
}

impl LenSeq {
    pub const fn new(len: u32, seq: u32) -> Self {
        Self { len, seq }
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C, packed(1))]
pub struct Reloc {
    /// frame offset in page file
    pub(crate) off: usize,
    /// frame length including header (excluding refcnt)
    pub(crate) len: u32,
    /// index in reclocation table
    pub(crate) seq: u32,
    /// checksum of page
    pub(crate) crc: u32,
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
    pub fn new(key: u64, off: usize, len: u32, seq: u32, crc: u32) -> Self {
        Self {
            key,
            val: Reloc { off, len, seq, crc },
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
        self.file
            .write(data)
            .inspect_err(|e| {
                log::error!("can't write: {:?}", e);
            })
            .expect("can't fail");
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
            .inspect_err(|x| {
                log::error!("can't sync {:?}, {}", self.path, x);
            })
            .expect("can't fail");
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
    pub oldest_id: u64,
    pub latest_id: u64,
    pub group: u8,
    padding1: u8,
    padding2: u16,
    pub checksum: u32,
}

impl WalDesc {
    pub(crate) fn new(group_id: u8) -> Self {
        Self {
            checkpoint: Position::default(),
            oldest_id: INIT_ID,
            latest_id: INIT_ID,
            group: group_id,
            padding1: 0,
            padding2: 0,
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

    pub fn update_oldest<P: AsRef<Path>>(&mut self, path: P, oldest_id: u64) -> Result<(), OpCode> {
        self.oldest_id = oldest_id;
        self.sync(path)
    }

    pub fn update_ckpt<P: AsRef<Path>>(
        &mut self,
        path: P,
        ckpt: Position,
        latest_id: u64,
    ) -> Result<(), OpCode> {
        self.checkpoint = ckpt;
        self.latest_id = latest_id;
        self.sync(path)
    }

    fn sync<P>(&mut self, path: P) -> Result<(), OpCode>
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
            .inspect_err(|e| log::error!("can't open, {e:?}"))
            .map_err(|_| OpCode::IoError)?;

        self.checksum = self.crc32();
        f.write_all(self.as_slice()).expect("can't write desc file");
        f.sync_all().expect("can't sync desc file");
        drop(f);

        let p = path.as_ref();
        std::fs::rename(tmp, p).map_err(|_| {
            log::error!("can't rename from {:?} to {:?}", tmp, p);
            OpCode::IoError
        })
    }
}

pub(crate) type WalDescHandle = MutRef<Mutex<WalDesc>>;
