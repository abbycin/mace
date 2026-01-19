use std::{
    collections::BTreeMap,
    fmt::Debug,
    ops::{Deref, DerefMut},
    ptr::addr_of_mut,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
};

use crate::{
    OpCode,
    meta::IMetaCodec,
    types::traits::IAsSlice,
    utils::{
        INIT_ADDR, INIT_ID, INIT_ORACLE, INIT_WMK,
        bitmap::BitMap,
        data::{MapEntry, Reloc},
    },
};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub enum MetaKind {
    FileId,
    Numerics,
    DataInterval,
    BlobInterval,
    DataStat,
    BlobStat,
    Map,
    DataDelete,
    BlobDelete,
    DataDeleteDone,
    BlobDeleteDone,
    DataDelInterval,
    BlobDelInterval,
    KindEnd,
}

impl IAsSlice for MetaKind {}

impl TryFrom<u8> for MetaKind {
    type Error = OpCode;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value > Self::KindEnd as u8 {
            Err(OpCode::BadData)
        } else {
            Ok(unsafe { std::mem::transmute::<u8, MetaKind>(value) })
        }
    }
}

#[repr(C, packed(1))]
pub struct FileId {
    pub is_blob: bool,
    pub file_id: u64,
}

impl FileId {
    pub fn blob(file_id: u64) -> Self {
        Self {
            is_blob: true,
            file_id,
        }
    }

    pub fn data(file_id: u64) -> Self {
        Self {
            is_blob: false,
            file_id,
        }
    }
}

impl IAsSlice for FileId {}

impl IMetaCodec for FileId {
    fn packed_size(&self) -> usize {
        size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        to.copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        FileId::from_slice(src)
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct DataStatInner {
    pub file_id: u64,
    /// up1 and up2, see [Efficiently Reclaiming Space in a Log Structured Store](https://ieeexplore.ieee.org/document/9458684)
    pub up1: u64,
    pub up2: u64,
    pub active_elems: u32,
    pub total_elems: u32,
    pub active_size: usize,
    pub total_size: usize,
}

impl IAsSlice for DataStatInner {}

#[derive(Clone)]
pub struct DataStat {
    pub inner: DataStatInner,
    pub inactive_elems: Vec<u32>,
}

impl Deref for DataStat {
    type Target = DataStatInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct MemDataStat {
    pub inner: DataStatInner,
    pub mask: BitMap,
}

impl DataStat {
    fn len(&self) -> usize {
        size_of::<DataStatInner>() + self.inactive_elems.len() * size_of::<u32>()
    }
}

impl Deref for MemDataStat {
    type Target = DataStatInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for MemDataStat {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl MemDataStat {
    pub(super) fn update(&mut self, tick: u64, reloc: &Reloc) {
        self.active_elems -= 1;
        self.active_size -= reloc.len as usize;
        self.mask.set(reloc.seq);

        if self.up1 < tick {
            self.up2 = self.up1;
            self.up1 = tick;
        }
    }

    pub(crate) fn copy(&self) -> DataStat {
        DataStat {
            inner: self.inner,
            inactive_elems: Vec::new(),
        }
    }
}

#[derive(Debug)]
#[repr(C, packed(1))]
pub(crate) struct StatHdr {
    elems: u32,
    pub(crate) size: u32,
}

impl IAsSlice for StatHdr {}

impl IMetaCodec for DataStat {
    fn packed_size(&self) -> usize {
        size_of::<StatHdr>() + self.len()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let hdr = StatHdr {
            elems: self.inactive_elems.len() as u32,
            size: self.len() as u32,
        };
        to[0..hdr.len()].copy_from_slice(hdr.as_slice());
        let dst = &mut to[hdr.len()..];
        let inner_s = self.inner.as_slice();
        dst[..inner_s.len()].copy_from_slice(inner_s);
        let src = unsafe {
            let p = self.inactive_elems.as_ptr().cast::<u8>();
            std::slice::from_raw_parts(p, self.len() - size_of::<DataStatInner>())
        };
        dst[inner_s.len()..].copy_from_slice(src);
    }

    fn decode(src: &[u8]) -> Self {
        let hdr = StatHdr::from_slice(src);
        let inner = DataStatInner::from_slice(&src[hdr.len()..]);
        let mut stat = DataStat {
            inner,
            inactive_elems: Vec::with_capacity(hdr.elems as usize),
        };
        let seq = unsafe {
            src.as_ptr()
                .add(hdr.len() + size_of::<DataStatInner>())
                .cast::<u32>()
        };

        for i in 0..hdr.elems as usize {
            unsafe {
                let x = seq.add(i).read_unaligned();
                stat.inactive_elems.push(x);
            }
        }
        stat
    }
}

#[derive(Clone, Copy)]
pub struct BlobStatInner {
    pub file_id: u64,
    pub active_size: usize,
    pub nr_active: u32,
    pub nr_total: u32,
}

impl IAsSlice for BlobStatInner {}

#[derive(Clone)]
pub struct MemBlobStat {
    pub inner: BlobStatInner,
    pub mask: BitMap,
}

impl MemBlobStat {
    pub(super) fn update(&mut self, reloc: &Reloc) {
        self.nr_active -= 1;
        self.active_size -= reloc.len as usize;
        self.mask.set(reloc.seq);
    }

    #[allow(dead_code)]
    pub fn copy(&self) -> BlobStat {
        BlobStat {
            inner: self.inner,
            inactive_elems: Vec::new(),
        }
    }
}

pub struct BlobStat {
    pub inner: BlobStatInner,
    pub inactive_elems: Vec<u32>,
}

impl BlobStat {
    fn len(&self) -> usize {
        size_of::<BlobStatInner>() + self.inactive_elems.len() * size_of::<u32>()
    }
}

impl Deref for BlobStat {
    type Target = BlobStatInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for BlobStat {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Deref for MemBlobStat {
    type Target = BlobStatInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for MemBlobStat {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl IMetaCodec for BlobStat {
    fn packed_size(&self) -> usize {
        size_of::<StatHdr>() + self.len()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let hdr = StatHdr {
            elems: self.inactive_elems.len() as u32,
            size: self.len() as u32,
        };
        to[0..hdr.len()].copy_from_slice(hdr.as_slice());
        let dst = &mut to[hdr.len()..];
        let inner = self.inner.as_slice();
        dst[..inner.len()].copy_from_slice(inner);
        let junks = unsafe {
            let p = self.inactive_elems.as_ptr().cast::<u8>();
            std::slice::from_raw_parts(p, self.len() - size_of::<BlobStatInner>())
        };
        dst[inner.len()..].copy_from_slice(junks);
    }

    fn decode(src: &[u8]) -> Self {
        let hdr = StatHdr::from_slice(src);
        let inner = BlobStatInner::from_slice(&src[hdr.len()..]);
        let mut stat = BlobStat {
            inner,
            inactive_elems: Vec::with_capacity(hdr.elems as usize),
        };
        let seq = unsafe {
            src.as_ptr()
                .add(hdr.len() + size_of::<BlobStatInner>())
                .cast::<u32>()
        };

        for i in 0..hdr.elems as usize {
            unsafe {
                let x = seq.add(i).read_unaligned();
                stat.inactive_elems.push(x);
            }
        }
        stat
    }
}

#[derive(Default)]
pub struct PageTable {
    // pid, addr, len(offset + len)
    data: BTreeMap<u64, u64>,
}

impl PageTable {
    pub fn len(&self) -> usize {
        self.data.len() * size_of::<MapEntry>()
    }

    pub fn collect(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.data.iter().for_each(|(&pid, &addr)| {
            buf.extend_from_slice(
                MapEntry {
                    page_id: pid,
                    page_addr: addr,
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
                if *x < addr {
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

#[repr(C, packed(1))]
pub(crate) struct PageTableHdr {
    elems: u32,
    pub(crate) size: usize,
}

impl IAsSlice for PageTableHdr {}

impl IMetaCodec for PageTable {
    fn packed_size(&self) -> usize {
        size_of::<PageTableHdr>() + self.len()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let hdr = PageTableHdr {
            elems: self.data.len() as u32,
            size: self.len(),
        };
        to[0..hdr.len()].copy_from_slice(hdr.as_slice());
        to[hdr.len()..].copy_from_slice(&self.collect());
    }

    fn decode(src: &[u8]) -> Self {
        let hdr = PageTableHdr::from_slice(src);
        let mut table = PageTable::default();
        let p = unsafe { src.as_ptr().add(hdr.len()).cast::<MapEntry>() };

        for i in 0..hdr.elems as usize {
            let m = unsafe { p.add(i).read_unaligned() };
            table.add(m.page_id, m.page_addr);
        }

        table
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct Numerics {
    /// notify log data has been flushed
    pub signal: AtomicU64,
    pub next_data_id: AtomicU64,
    pub next_blob_id: AtomicU64,
    pub next_manifest_id: AtomicU64,
    pub oracle: AtomicU64,
    /// it's the logical address to a frame
    pub address: AtomicU64,
    pub wmk_oldest: AtomicU64,
    pub log_size: AtomicUsize,
    pub scavenge_cursor: AtomicU64,
}

impl Numerics {
    #[allow(dead_code)]
    pub(crate) fn safe_tixd(&self) -> u64 {
        self.wmk_oldest.load(Relaxed)
    }
}

impl Default for Numerics {
    fn default() -> Self {
        Self {
            signal: AtomicU64::new(INIT_ID),
            next_data_id: AtomicU64::new(INIT_ID),
            next_blob_id: AtomicU64::new(INIT_ID),
            next_manifest_id: AtomicU64::new(INIT_ID),
            oracle: AtomicU64::new(INIT_ORACLE),
            address: AtomicU64::new(INIT_ADDR),
            wmk_oldest: AtomicU64::new(INIT_WMK),
            log_size: AtomicUsize::new(0),
            scavenge_cursor: AtomicU64::new(0),
        }
    }
}

impl Clone for Numerics {
    // a snapshot of atomic values, that's ok for data sync
    fn clone(&self) -> Self {
        let mut tmp = Numerics::default();
        let dst = addr_of_mut!(tmp);
        unsafe { std::ptr::copy_nonoverlapping(self, dst, 1) };
        tmp
    }
}

impl IAsSlice for Numerics {}

impl IMetaCodec for Numerics {
    fn packed_size(&self) -> usize {
        size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        to.copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        Numerics::from_slice(src)
    }
}

#[derive(Default)]
pub struct Delete {
    pub id: Vec<u64>,
}

impl From<Vec<u64>> for Delete {
    fn from(value: Vec<u64>) -> Self {
        Self { id: value }
    }
}

impl Deref for Delete {
    type Target = Vec<u64>;
    fn deref(&self) -> &Self::Target {
        &self.id
    }
}

impl DerefMut for Delete {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.id
    }
}

#[repr(C, packed(1))]
pub(crate) struct DeleteHdr {
    nr_id: u32,
}

impl IAsSlice for DeleteHdr {}

impl IMetaCodec for Delete {
    fn packed_size(&self) -> usize {
        self.id.len() * size_of::<u64>() + size_of::<DeleteHdr>()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let hdr = DeleteHdr {
            nr_id: self.id.len() as u32,
        };
        to[0..hdr.len()].copy_from_slice(hdr.as_slice());
        let src = unsafe {
            let p = self.id.as_ptr().cast::<u8>();
            std::slice::from_raw_parts(p, self.len() * size_of::<u64>())
        };
        to[hdr.len()..].copy_from_slice(src);
    }

    fn decode(src: &[u8]) -> Self {
        let hdr = DeleteHdr::from_slice(src);
        let p = unsafe { src.as_ptr().add(hdr.len()).cast::<u64>() };
        let mut r = Delete::default();
        for i in 0..hdr.nr_id as usize {
            let id = unsafe { p.add(i).read_unaligned() };
            r.push(id);
        }
        r
    }
}

#[derive(Clone, Copy)]
pub struct IntervalPair {
    pub lo_addr: u64,
    pub hi_addr: u64,
    pub file_id: u64,
}

impl IntervalPair {
    pub const fn new(lo: u64, hi: u64, file_id: u64) -> Self {
        Self {
            lo_addr: lo,
            hi_addr: hi,
            file_id,
        }
    }
}

impl Debug for IntervalPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "[{}, {}] => {}",
            self.lo_addr, self.hi_addr, self.file_id
        ))
    }
}

impl IAsSlice for IntervalPair {}

impl IMetaCodec for IntervalPair {
    fn packed_size(&self) -> usize {
        size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        to.copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        Self::from_slice(src)
    }
}

#[derive(Default)]
pub struct DelInterval {
    pub lo: Vec<u64>,
}

impl Deref for DelInterval {
    type Target = Vec<u64>;

    fn deref(&self) -> &Self::Target {
        &self.lo
    }
}

impl DerefMut for DelInterval {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.lo
    }
}

#[derive(Clone, Copy)]
#[repr(C, packed(1))]
pub(crate) struct DelIntervalStartHdr {
    nr_lo: u16,
}

impl IAsSlice for DelIntervalStartHdr {}

impl IMetaCodec for DelInterval {
    fn packed_size(&self) -> usize {
        self.lo.len() * size_of::<u64>() + size_of::<DelIntervalStartHdr>()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let hdr = DelIntervalStartHdr {
            nr_lo: self.lo.len() as u16,
        };
        to[0..hdr.len()].copy_from_slice(hdr.as_slice());
        let src = unsafe {
            let p = self.lo.as_ptr().cast::<u8>();
            std::slice::from_raw_parts(p, self.lo.len() * size_of::<u64>())
        };
        to[hdr.len()..].copy_from_slice(src);
    }

    fn decode(src: &[u8]) -> Self {
        let hdr = DelIntervalStartHdr::from_slice(src);
        let p = unsafe { src.as_ptr().add(hdr.len()).cast::<u64>() };
        let mut r = DelInterval::default();
        for i in 0..hdr.nr_lo as usize {
            let id = unsafe { p.add(i).read_unaligned() };
            r.lo.push(id);
        }
        r
    }
}
