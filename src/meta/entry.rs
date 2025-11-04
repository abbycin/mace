use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    ptr::addr_of_mut,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
};

use crate::{
    OpCode,
    meta::IMetaCodec,
    types::traits::IAsSlice,
    utils::{
        INIT_ADDR, INIT_ID, INIT_ORACLE,
        bitmap::BitMap,
        data::{MapEntry, Reloc},
    },
};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub enum MetaKind {
    Begin,
    FileId,
    Commit,
    Numerics,
    Interval,
    Stat,
    Map,
    Delete,
    DelInterval,
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

#[derive(Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum TxnKind {
    FLush = 3,
    GC,
    Dump,
}

#[repr(C, packed(1))]
pub(crate) struct GenericHdr {
    pub(crate) kind: MetaKind,
    pub(crate) txid: u64,
}

impl GenericHdr {
    pub(crate) const SIZE: usize = size_of::<Self>();

    pub(crate) const fn new(kind: MetaKind, txid: u64) -> Self {
        Self { kind, txid }
    }

    pub(crate) fn decode(src: &[u8]) -> Result<Self, OpCode> {
        debug_assert!(src.len() >= Self::SIZE);
        let _kind: MetaKind = src[0].try_into()?;
        Ok(GenericHdr::from_slice(src))
    }
}

impl IAsSlice for GenericHdr {}

#[derive(Clone, Copy)]
pub struct Begin(pub TxnKind);

impl IMetaCodec for Begin {
    fn packed_size(&self) -> usize {
        size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        to[0] = self.0 as u8;
    }

    fn decode(src: &[u8]) -> Self {
        assert!(src[0] <= TxnKind::Dump as u8);
        assert!(src[0] >= TxnKind::FLush as u8);
        Begin(unsafe { std::mem::transmute::<u8, TxnKind>(src[0]) })
    }
}

#[repr(C, packed(1))]
pub struct FileId {
    pub file_id: u64,
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

#[derive(Debug)]
#[repr(C, packed(1))]
pub(super) struct Commit {
    pub(crate) txid: u64,
    pub(crate) checksum: u32,
}

impl IAsSlice for Commit {}

impl IMetaCodec for Commit {
    fn packed_size(&self) -> usize {
        size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        to.copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        Commit::from_slice(src)
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct StatInner {
    pub file_id: u64,
    /// up1 and up2, see [Efficiently Reclaiming Space in a Log Structured Store](https://ieeexplore.ieee.org/document/9458684)
    pub up1: u64,
    pub up2: u64,
    pub active_elems: u32,
    pub total_elems: u32,
    pub active_size: usize,
    pub total_size: usize,
}

impl IAsSlice for StatInner {}

#[derive(Clone)]
pub struct Stat {
    pub inner: StatInner,
    pub deleted_elems: Vec<u32>,
}

impl Deref for Stat {
    type Target = StatInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct FileStat {
    pub inner: StatInner,
    pub deleted_elems: BitMap,
}

impl Stat {
    fn len(&self) -> usize {
        size_of::<StatInner>() + self.deleted_elems.len() * size_of::<u32>()
    }
}

impl Deref for FileStat {
    type Target = StatInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for FileStat {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl FileStat {
    pub(super) fn update(&mut self, tick: u64, reloc: &Reloc) {
        self.active_elems -= 1;
        self.active_size -= reloc.len as usize;
        self.deleted_elems.set(reloc.seq);

        if self.up1 < tick {
            self.up2 = self.up1;
            self.up1 = tick;
        }
    }

    pub(crate) fn copy(&self) -> Stat {
        Stat {
            inner: self.inner,
            deleted_elems: vec![],
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

impl IMetaCodec for Stat {
    fn packed_size(&self) -> usize {
        size_of::<StatHdr>() + self.len()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let hdr = StatHdr {
            elems: self.deleted_elems.len() as u32,
            size: self.len() as u32,
        };
        to[0..hdr.len()].copy_from_slice(hdr.as_slice());
        let dst = &mut to[hdr.len()..];
        let inner_s = self.inner.as_slice();
        dst[..inner_s.len()].copy_from_slice(inner_s);
        let src = unsafe {
            let p = self.deleted_elems.as_ptr().cast::<u8>();
            std::slice::from_raw_parts(p, self.len() - size_of::<StatInner>())
        };
        dst[inner_s.len()..].copy_from_slice(src);
    }

    fn decode(src: &[u8]) -> Self {
        let hdr = StatHdr::from_slice(src);
        let inner = StatInner::from_slice(&src[hdr.len()..]);
        let mut stat = Stat {
            inner,
            deleted_elems: vec![],
        };
        let seq = unsafe {
            src.as_ptr()
                .add(hdr.len() + size_of::<StatInner>())
                .cast::<u32>()
        };

        for i in 0..hdr.elems as usize {
            unsafe {
                let x = seq.add(i).read_unaligned();
                stat.deleted_elems.push(x);
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
#[repr(C, align(64))]
pub struct Numerics {
    /// notify log data has been flushed
    pub signal: AtomicU64,
    pub next_file_id: AtomicU64,
    pub next_manifest_id: AtomicU64,
    pub oracle: AtomicU64,
    /// it's the logical address to a frame
    pub address: AtomicU64,
    pub wmk_oldest: AtomicU64,
    pub log_size: AtomicUsize,
}

impl Numerics {
    pub(crate) fn safe_tixd(&self) -> u64 {
        self.wmk_oldest.load(Relaxed)
    }
}

impl Default for Numerics {
    fn default() -> Self {
        Self {
            signal: AtomicU64::new(INIT_ID),
            next_file_id: AtomicU64::new(INIT_ID),
            next_manifest_id: AtomicU64::new(INIT_ID),
            oracle: AtomicU64::new(INIT_ORACLE),
            address: AtomicU64::new(INIT_ADDR),
            wmk_oldest: AtomicU64::new(0),
            log_size: AtomicUsize::new(0),
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
    id: Vec<u64>,
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

impl DeleteHdr {
    pub(crate) fn size(&self) -> usize {
        self.nr_id as usize * size_of::<u64>()
    }
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

impl DelIntervalStartHdr {
    pub(crate) fn size(&self) -> usize {
        self.nr_lo as usize * size_of::<u64>()
    }
}

impl IAsSlice for DelIntervalStartHdr {}

impl IMetaCodec for DelInterval {
    fn packed_size(&self) -> usize {
        self.lo.len() * size_of::<u64>() + size_of::<DelIntervalStartHdr>()
    }

    fn encode(&self, to: &mut [u8]) {
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

pub(crate) fn get_record_size(h: MetaKind, size: usize) -> Result<usize, OpCode> {
    let sz = match h {
        MetaKind::Begin => size_of::<Begin>(),
        MetaKind::Commit => size_of::<Commit>(),
        MetaKind::Numerics => size_of::<Numerics>(),
        MetaKind::Interval => size_of::<IntervalPair>(),
        MetaKind::Delete => size_of::<DeleteHdr>(),
        MetaKind::Map => size_of::<PageTableHdr>(),
        MetaKind::Stat => size_of::<StatHdr>(),
        MetaKind::DelInterval => size_of::<DelIntervalStartHdr>(),
        MetaKind::FileId => size_of::<FileId>(),
        _ => unreachable!("invalid kind {h:?}"),
    } + GenericHdr::SIZE;
    if size >= sz {
        Ok(sz)
    } else {
        Err(OpCode::NeedMore)
    }
}
