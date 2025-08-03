use super::{INIT_ORACLE, MutRef, NEXT_ID, NULL_ADDR, OpCode, pack_id, rand_range};
use crate::utils::bitmap::{BITMAP_ELEM_LEN, BitMap, BitmapElemType};
use crc32c::Crc32cHasher;
use io::{GatherIO, IoVec};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hasher;
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64};
use std::sync::{Mutex, RwLock};

/// logical id and physical id are same length
pub(crate) const ID_LEN: usize = size_of::<u32>();
/// packed logical id and offset
pub(crate) const JUNK_LEN: usize = size_of::<u64>();

#[derive(Clone, Copy, Debug)]
#[repr(C, packed(1))]
pub struct Reloc {
    /// frame offset in page file
    pub(crate) off: usize,
    /// frame's payload length
    pub(crate) len: u32,
    /// index in reclocation table
    pub(crate) seq: u32,
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct AddrMap {
    /// pack_id(lid, off)
    pub(crate) key: u64,
    pub(crate) val: Reloc,
}

impl AddrMap {
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
#[repr(C, packed(1))]
pub struct MapEntry {
    page_id: u64,
    /// (file_id << 32) | arena_offset
    page_addr: u64,
}

impl MapEntry {
    fn as_slice(&self) -> &[u8] {
        unsafe {
            let p = self as *const MapEntry as *const u8;
            std::slice::from_raw_parts(p, size_of::<Self>())
        }
    }

    pub fn page_id(&self) -> u64 {
        self.page_id
    }

    pub fn page_addr(&self) -> u64 {
        self.page_addr
    }
}

#[derive(Default)]
pub struct PageTable {
    // pid, addr, len(offset + len)
    // NULL_ADDR for delete mark
    data: BTreeMap<u64, u64>,
}

impl PageTable {
    pub const ITEM_LEN: usize = size_of::<MapEntry>();

    pub fn collect(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.data.iter().for_each(|x| {
            buf.extend_from_slice(
                MapEntry {
                    page_id: *x.0,
                    page_addr: *x.1,
                }
                .as_slice(),
            );
        });
        buf
    }

    pub fn add(&mut self, pid: u64, addr: u64) {
        self.data
            .entry(pid)
            .and_modify(|x| {
                // when current is normal addr, or it's a tombstone and reused
                if *x < addr || *x == NULL_ADDR {
                    *x = addr;
                }
            })
            .or_insert(addr);
    }
}

impl Deref for PageTable {
    type Target = BTreeMap<u64, u64>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for PageTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

pub struct GatherWriter {
    path: PathBuf,
    file: io::File,
    queue: Vec<IoVec>,
    queued_len: usize,
    max_iovcnt: usize,
}

unsafe impl Send for GatherWriter {}

impl GatherWriter {
    /// max iovec count is limited to 1024 in Linux
    pub(crate) const MAX_IOVCNT: usize = 1024;
    pub(crate) const DEFAULT_IOVCNT: usize = 64;

    fn open(path: &PathBuf) -> io::File {
        io::File::options()
            .write(true)
            .append(true)
            .create(true)
            .open(path)
            .inspect_err(|x| {
                log::error!("can't open {path:?}, {x}");
            })
            .unwrap()
    }

    pub fn new(path: &PathBuf, max_iovcnt: usize) -> Self {
        Self {
            path: path.clone(),
            file: Self::open(path),
            queue: Vec::new(),
            queued_len: 0,
            max_iovcnt: if max_iovcnt >= Self::MAX_IOVCNT {
                Self::DEFAULT_IOVCNT
            } else {
                max_iovcnt
            },
        }
    }

    pub fn reset(&mut self, path: &PathBuf) {
        self.path = path.clone();
        self.flush();
        self.file = Self::open(path);
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

const META_COMPLETE: u16 = 114;
const META_IMCOMPLETE: u16 = 514;

#[derive(Debug)]
#[repr(C)]
pub(crate) struct WalDesc {
    // TODO: change to u128
    pub checkpoint: u64,
    // TODO: change to u64
    pub wal_id: u32,
    pub worker: u16,
    padding: u16,
    pub checksum: u32,
}

impl WalDesc {
    pub(crate) fn new(wid: u16) -> Self {
        Self {
            checkpoint: pack_id(NEXT_ID, 0),
            wal_id: NEXT_ID,
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

#[derive(Debug)]
#[repr(C)]
pub struct MetaInner {
    pub oracle: AtomicU64,
    /// oldest flushed txid
    pub wmk_oldest: AtomicU64,
    /// the latest data file id
    pub next_data: AtomicU32,
    state: AtomicU32,
    nr_worker: AtomicU16,
    padding: u16,
    checksum: AtomicU32,
}

impl MetaInner {
    const LEN: usize = size_of::<Self>();

    fn new(workers: usize) -> Self {
        Self {
            oracle: AtomicU64::new(INIT_ORACLE),
            wmk_oldest: AtomicU64::new(0),
            next_data: AtomicU32::new(NEXT_ID),
            state: AtomicU32::new(META_IMCOMPLETE as u32),
            nr_worker: AtomicU16::new(workers as u16),
            padding: 0,
            checksum: AtomicU32::new(0),
        }
    }

    fn slice(&self) -> &[u8] {
        unsafe {
            let p = self as *const Self as *const u8;
            std::slice::from_raw_parts(p, size_of::<Self>())
        }
    }

    pub fn safe_tixd(&self) -> u64 {
        self.wmk_oldest.load(Relaxed)
    }

    pub fn checksum(&self) -> u32 {
        self.checksum.load(Relaxed)
    }
}

#[repr(C)]
pub struct Meta {
    pub inner: MetaInner,
    pub mask: RwLock<BitMap>,
    pub mtx: Mutex<()>,
}

impl Deref for Meta {
    type Target = MetaInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Meta {
    pub fn new(workers: usize) -> Self {
        Self {
            inner: MetaInner::new(workers),
            mask: RwLock::new(BitMap::new(workers as u32)),
            mtx: Mutex::new(()),
        }
    }

    fn crc32_inner(&self, m: &BitMap) -> u32 {
        let mut h = Crc32cHasher::new(0);
        for i in m.slice() {
            h.write_u64(*i);
        }

        // ------------ footer -----------------

        h.write_u64(self.inner.oracle.load(Relaxed));
        // not calc wmk_oldest on purpose
        h.write_u32(self.inner.next_data.load(Relaxed));
        h.write_u32(self.inner.state.load(Relaxed));
        h.write_u16(self.inner.nr_worker.load(Relaxed));
        h.write_u16(m.len() as u16);
        h.finish() as u32
    }

    pub fn calc_crc32(&self) -> u32 {
        let lk = self.mask.read().unwrap();
        self.crc32_inner(&lk)
    }

    pub fn deserialize(data: &[u8], workers: usize) -> Result<Self, OpCode> {
        let mut inner = MetaInner::new(0);
        let ptr = &mut inner as *mut MetaInner as *mut u8;
        let s = unsafe {
            std::ptr::copy(
                data.as_ptr().add(data.len() - MetaInner::LEN),
                ptr,
                MetaInner::LEN,
            );
            let mask_len = data.len() - MetaInner::LEN;
            if mask_len % BITMAP_ELEM_LEN != 0 {
                return Err(OpCode::BadData);
            }
            let p = data.as_ptr().cast::<BitmapElemType>();
            let n = mask_len / BITMAP_ELEM_LEN;
            std::slice::from_raw_parts(p, n)
        };

        if inner.nr_worker.load(Relaxed) != workers as u16 {
            log::error!(
                "incorrect number of workers, expect {} real {}",
                inner.nr_worker.load(Relaxed),
                workers
            );
            return Err(OpCode::Invalid);
        }

        let tmp = Self {
            inner,
            mask: RwLock::new(BitMap::from(s)),
            mtx: Mutex::new(()),
        };
        if tmp.calc_crc32() != tmp.checksum() {
            return Err(OpCode::BadData);
        }
        Ok(tmp)
    }

    pub fn is_complete(&self) -> bool {
        self.inner.state.load(Relaxed) == META_COMPLETE as u32
    }

    fn serialize(&self, m: &BitMap) -> Vec<u8> {
        let crc = self.crc32_inner(m);
        self.inner.checksum.store(crc, Relaxed);
        let mask = unsafe {
            let s = m.slice();
            let p = s.as_ptr().cast::<u8>();
            let n = s.len() * BITMAP_ELEM_LEN;
            std::slice::from_raw_parts(p, n)
        };

        let mut s = Vec::with_capacity(mask.len() + size_of::<MetaInner>());

        s.extend_from_slice(mask);
        s.extend_from_slice(self.inner.slice());
        s
    }

    pub fn sync<P: AsRef<Path>>(&self, path: P, complete: bool) {
        let _lk = self.mtx.lock().unwrap();
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
            .expect("can't open meta file");

        let state = if complete {
            META_COMPLETE
        } else {
            META_IMCOMPLETE
        };
        self.inner.state.store(state as u32, Relaxed);
        let lk = self.mask.read().unwrap();
        f.write_all(self.serialize(&lk).as_slice())
            .expect("can't write meta file");
        f.sync_all().expect("can't sync meta file");
        drop(f);

        std::fs::rename(tmp, path).expect("can't fail");
    }
}

#[cfg(test)]
mod test {
    use io::{File, GatherIO};

    use crate::{
        RandomPath,
        utils::{block::Block, data::META_COMPLETE},
    };
    use std::{ops::Deref, sync::atomic::Ordering::Relaxed};

    use super::Meta;

    #[test]
    fn meta_s11n() {
        let meta = Meta::new(2);
        let root = RandomPath::new().deref().clone();
        let meta_file = root.join("meta");

        std::fs::create_dir_all(&root).unwrap();

        meta.state.store(META_COMPLETE as u32, Relaxed);
        meta.next_data.store(1, Relaxed);
        meta.oracle.store(5, Relaxed);
        meta.mask.write().unwrap().set(0);

        let chksum = meta.calc_crc32();

        meta.sync(&meta_file, true);
        assert!(meta_file.exists());

        let f = File::options().read(true).open(&meta_file).unwrap();
        let b = Block::alloc(256);
        let n = f.read(b.mut_slice(0, b.len()), 0).unwrap();

        let tmp = Meta::deserialize(b.slice(0, n), 2).unwrap();

        assert_eq!(tmp.checksum.load(Relaxed), chksum);
        let tmp_slice = tmp.inner.slice();
        let old_slice = meta.inner.slice();
        assert_eq!(old_slice, tmp_slice);

        std::fs::remove_dir_all(&root).unwrap();
    }
}
