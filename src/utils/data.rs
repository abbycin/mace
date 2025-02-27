use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hasher;
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU32, AtomicU64};

use crc32c::Crc32cHasher;

use super::{pack_id, rand_range, INIT_ORACLE, NEXT_ID};

/// logical id and physical id are same length
pub(crate) const ID_LEN: usize = size_of::<u32>();
/// packed logical id and offset
pub(crate) const JUNK_LEN: usize = size_of::<u64>();

#[derive(Clone, Copy, Debug)]
#[repr(C, packed(1))]
pub struct Reloc {
    /// frame offset in page file
    pub(crate) off: u32,
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
    pub fn new(key: u64, off: u32, len: u32, seq: u32) -> Self {
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
    data: HashMap<u64, MapEntry>,
}

impl PageTable {
    pub const ITEM_LEN: usize = size_of::<MapEntry>();

    pub fn serialize<F>(&self, file: &mut F) -> usize
    where
        F: Write,
    {
        let mut buf = Vec::new();

        self.data
            .values()
            .map(|e| {
                buf.extend_from_slice(e.as_slice());
            })
            .count();
        file.write_all(buf.as_slice())
            .expect("can't write page table");
        buf.len()
    }

    pub fn hash<H>(&self, h: &mut H)
    where
        H: Hasher,
    {
        self.data.values().map(|e| h.write(e.as_slice())).count();
    }

    pub fn add(&mut self, pid: u64, addr: u64) {
        if let Some(e) = self.get_mut(&pid) {
            // a delta chain in same arana, we use the latest mapping
            // NOTE: it's incorrect when addr was wrapped, but it's almost never happen in our spec
            if e.page_addr < addr {
                e.page_addr = addr;
            }
        } else {
            self.insert(
                pid,
                MapEntry {
                    page_id: pid,
                    page_addr: addr,
                },
            );
        }
    }
}

impl Deref for PageTable {
    type Target = HashMap<u64, MapEntry>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for PageTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

const META_COMPLETE: u32 = 114;
const META_IMCOMPLETE: u32 = 514;

#[derive(Debug)]
#[repr(C)]
pub struct Meta {
    pub oracle: AtomicU64,
    /// oldest flushed txid
    pub wmk_oldest: AtomicU64,
    /// the latest checkpoint
    pub ckpt: AtomicU64,
    /// the latest data file id
    pub next_data: AtomicU32,
    pub next_wal: AtomicU32,
    pub state: AtomicU32,
    pub checksum: AtomicU32,
}

impl Meta {
    pub const LEN: usize = size_of::<Self>();

    pub fn new() -> Self {
        let id = pack_id(NEXT_ID, 0);
        Self {
            oracle: AtomicU64::new(INIT_ORACLE),
            wmk_oldest: AtomicU64::new(0),
            next_data: AtomicU32::new(NEXT_ID),
            ckpt: AtomicU64::new(id),
            next_wal: AtomicU32::new(NEXT_ID),
            state: AtomicU32::new(META_IMCOMPLETE),
            checksum: AtomicU32::new(0),
        }
    }

    pub fn reset(&self) {
        let id = pack_id(NEXT_ID, 0);
        self.oracle.store(INIT_ORACLE, Relaxed);
        self.wmk_oldest.store(0, Relaxed);
        self.next_data.store(NEXT_ID, Relaxed);
        self.ckpt.store(id, Relaxed);
        self.next_wal.store(NEXT_ID, Relaxed);
        self.state.store(META_IMCOMPLETE, Relaxed);
        self.checksum.store(0, Relaxed);
    }

    pub fn crc32(&self) -> u32 {
        let mut h = Crc32cHasher::new(0);
        h.write_u64(self.oracle.load(Relaxed));
        // not calc wmk_oldest on purpose
        h.write_u64(self.ckpt.load(Relaxed));
        h.write_u32(self.next_data.load(Relaxed));
        h.write_u32(self.next_wal.load(Relaxed));
        h.write_u32(self.state.load(Relaxed));
        h.finish() as u32
    }

    pub fn deserialize(data: &[u8]) -> Self {
        let mut tmp = Self::new();
        let ptr = &mut tmp as *mut Self as *mut u8;
        unsafe {
            let dst = std::slice::from_raw_parts_mut(ptr, Self::LEN);
            dst.copy_from_slice(data);
        }
        tmp
    }

    pub fn is_complete(&self) -> bool {
        self.state.load(Relaxed) == META_COMPLETE
    }

    pub fn update_chkpt(&self, id: u32, off: u32) {
        assert_ne!(id, 0);
        self.ckpt.store(pack_id(id, off), Relaxed);
    }

    fn serialize(&self) -> &[u8] {
        self.checksum.store(self.crc32(), Relaxed);
        let ptr = self as *const Self as *const u8;
        unsafe { std::slice::from_raw_parts(ptr, Self::LEN) }
    }

    pub fn sync(&self, path: PathBuf, complete: bool) {
        let s = format!(
            "{}_{}",
            path.as_os_str().to_str().unwrap(),
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
        self.state.store(state, Relaxed);
        f.write_all(self.serialize())
            .expect("can't write meta file");
        f.sync_all().expect("can't sync meta file");
        drop(f);

        std::fs::rename(tmp, path).expect("can't fail");
    }
}

#[cfg(test)]
mod test {
    use crate::utils::data::META_COMPLETE;
    use std::sync::atomic::Ordering::Relaxed;

    use super::Meta;

    #[test]
    fn meta_s11n() {
        let meta = Meta::new();

        meta.state.store(META_COMPLETE, Relaxed);
        meta.next_data.store(1, Relaxed);
        meta.oracle.store(5, Relaxed);

        let mut buf = vec![0u8; Meta::LEN];
        let s = buf.as_mut_slice();
        s.copy_from_slice(meta.serialize());

        let tmp = Meta::deserialize(s);

        assert_eq!(tmp.crc32(), tmp.checksum.load(Relaxed));

        let path = std::env::temp_dir().join("meta");
        tmp.sync(path.clone(), true);
        assert!(path.exists());
        std::fs::remove_file(path).unwrap();
    }
}
