use crate::{must_exist, must_true};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    ops::{Deref, DerefMut},
    ptr::addr_of_mut,
    sync::atomic::AtomicU64,
};

use crate::{
    BucketOptions, OpCode,
    meta::{
        BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_FRONTIER, BUCKET_MISC, BUCKET_OBSOLETE_BLOB,
        BUCKET_OBSOLETE_DATA, SEQUENCES_KEY, blob_interval_name, data_interval_name,
        page_table_name,
    },
    observe::CounterMetric,
    types::traits::IAsSlice,
    utils::{
        INIT_ID, INIT_ORACLE, NULL_ADDR,
        bitmap::BitMap,
        data::{GroupPositions, MapEntry, Reloc},
    },
};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub enum MetaKind {
    Sequences,
    DataInterval,
    BlobInterval,
    DataStat,
    BlobStat,
    Map,
    BucketFrontier,
    DataDelete,
    BlobDelete,
    DataDeleteDone,
    BlobDeleteDone,
    DataDelInterval,
    BlobDelInterval,
    KindEnd,
}

impl IAsSlice for MetaKind {}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FileKind {
    Data,
    Blob,
}

impl FileKind {
    pub(crate) const ALL: [Self; 2] = [Self::Data, Self::Blob];

    pub(crate) const fn slot(self) -> usize {
        match self {
            Self::Data => 0,
            Self::Blob => 1,
        }
    }
}

impl TryFrom<u8> for MetaKind {
    type Error = OpCode;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value > Self::KindEnd as u8 {
            Err(OpCode::Corruption)
        } else {
            Ok(unsafe { std::mem::transmute::<u8, MetaKind>(value) })
        }
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
    pub bucket_id: u64,
}

impl IAsSlice for StatInner {}

#[derive(Clone)]
pub struct PersistStat {
    pub inner: StatInner,
    pub inactive_elems: Vec<u32>,
}

pub struct MemStat {
    pub inner: StatInner,
    pub mask: Option<BitMap>,
}

impl Deref for PersistStat {
    type Target = StatInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PersistStat {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl PersistStat {
    pub(crate) fn from_parts(inner: StatInner, inactive_elems: Vec<u32>) -> Self {
        Self {
            inner,
            inactive_elems,
        }
    }

    pub(crate) fn decode_inner_only(src: &[u8]) -> StatInner {
        let hdr = StatHdr::from_slice(src);
        StatInner::from_slice(&src[hdr.len()..])
    }

    pub(crate) fn decode_mask_only(src: &[u8], total_elems: u32) -> BitMap {
        let hdr = StatHdr::from_slice(src);
        let mut mask = BitMap::new(total_elems);
        let seq = unsafe {
            src.as_ptr()
                .add(hdr.len() + size_of::<StatInner>())
                .cast::<u32>()
        };
        for i in 0..hdr.elems as usize {
            unsafe {
                let x = seq.add(i).read_unaligned();
                mask.set(x);
            }
        }
        mask
    }

    fn len(&self) -> usize {
        size_of::<StatInner>() + self.inactive_elems.len() * size_of::<u32>()
    }
}

impl Deref for MemStat {
    type Target = StatInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for MemStat {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl MemStat {
    pub(crate) fn from_parts(inner: StatInner, mask: Option<BitMap>) -> Self {
        Self { inner, mask }
    }

    pub(super) fn update(&mut self, tick: u64, reloc: &Reloc) {
        self.active_elems -= 1;
        self.active_size -= reloc.active_len() as usize;
        must_exist!(self.mask.as_mut(), "mask loaded").set(reloc.seq);

        if self.up1 < tick {
            self.up2 = self.up1;
            self.up1 = tick;
        }
    }

    pub(crate) fn copy(&self) -> PersistStat {
        PersistStat::from_parts(self.inner, Vec::new())
    }

    pub(crate) fn clone_mem(&self) -> Self {
        Self::from_parts(self.inner, self.mask.clone())
    }
}

#[derive(Debug)]
#[repr(C, packed(1))]
pub(crate) struct StatHdr {
    elems: u32,
    pub(crate) size: u32,
}

impl IAsSlice for StatHdr {}

impl IMetaCodec for PersistStat {
    fn packed_size(&self) -> usize {
        size_of::<StatHdr>() + self.len()
    }

    fn encode(&self, to: &mut [u8]) {
        must_true!(eq to.len(), self.packed_size());
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
            std::slice::from_raw_parts(p, self.len() - size_of::<StatInner>())
        };
        dst[inner_s.len()..].copy_from_slice(src);
    }

    fn decode(src: &[u8]) -> Self {
        let hdr = StatHdr::from_slice(src);
        let inner = StatInner::from_slice(&src[hdr.len()..]);
        let mut stat = Self::from_parts(inner, Vec::with_capacity(hdr.elems as usize));
        let seq = unsafe {
            src.as_ptr()
                .add(hdr.len() + size_of::<StatInner>())
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
    pub bucket_id: u64,
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
                // unmap or update
                if *x < addr || addr == NULL_ADDR {
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
        must_true!(eq to.len(), self.packed_size());
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

#[derive(Clone, Copy)]
#[repr(C)]
pub struct BucketDurableFrontier {
    pub bucket_id: u64,
    pub lsn: GroupPositions,
}

impl BucketDurableFrontier {
    pub const fn new(bucket_id: u64, lsn: GroupPositions) -> Self {
        Self { bucket_id, lsn }
    }
}

impl IAsSlice for BucketDurableFrontier {}

impl IMetaCodec for BucketDurableFrontier {
    fn packed_size(&self) -> usize {
        size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        must_true!(eq to.len(), self.packed_size());
        to.copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        Self::from_slice(src)
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct Sequences {
    pub next_file_id: AtomicU64,
    pub next_bucket_id: AtomicU64,
    pub oracle: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct WalRecycleState {
    pub group_id: u8,
    pub stage: u8,
    pub _pad: [u8; 6],
    pub from_file_id: u64,
    pub to_file_id: u64,
}

impl WalRecycleState {
    pub const STAGE_NONE: u8 = 0;
    pub const STAGE_INTENT: u8 = 1;
    pub const STAGE_DONE: u8 = 2;

    pub const fn none(group_id: u8) -> Self {
        Self {
            group_id,
            stage: Self::STAGE_NONE,
            _pad: [0; 6],
            from_file_id: 0,
            to_file_id: 0,
        }
    }

    pub const fn intent(group_id: u8, from_file_id: u64, to_file_id: u64) -> Self {
        Self {
            group_id,
            stage: Self::STAGE_INTENT,
            _pad: [0; 6],
            from_file_id,
            to_file_id,
        }
    }

    pub const fn done(group_id: u8, from_file_id: u64, to_file_id: u64) -> Self {
        Self {
            group_id,
            stage: Self::STAGE_DONE,
            _pad: [0; 6],
            from_file_id,
            to_file_id,
        }
    }

    pub const fn is_none(self) -> bool {
        self.stage == Self::STAGE_NONE
    }

    pub const fn is_done(self) -> bool {
        self.stage == Self::STAGE_DONE
    }

    pub const fn oldest_id(self) -> u64 {
        match self.stage {
            Self::STAGE_DONE => self.to_file_id,
            _ => self.from_file_id,
        }
    }
}

impl IAsSlice for WalRecycleState {}

impl IMetaCodec for WalRecycleState {
    fn packed_size(&self) -> usize {
        size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        must_true!(eq to.len(), self.packed_size());
        to.copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        Self::from_slice(src)
    }
}

impl Default for Sequences {
    fn default() -> Self {
        Self {
            next_file_id: AtomicU64::new(INIT_ID),
            next_bucket_id: AtomicU64::new(INIT_ID),
            oracle: AtomicU64::new(INIT_ORACLE),
        }
    }
}

impl Clone for Sequences {
    // a snapshot of atomic values, that's ok for data sync
    fn clone(&self) -> Self {
        let mut tmp = Sequences::default();
        let dst = addr_of_mut!(tmp);
        unsafe { std::ptr::copy_nonoverlapping(self, dst, 1) };
        tmp
    }
}

impl IAsSlice for Sequences {}

impl IMetaCodec for Sequences {
    fn packed_size(&self) -> usize {
        size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        must_true!(eq to.len(), self.packed_size());
        to.copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        Sequences::from_slice(src)
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
        must_true!(eq to.len(), self.packed_size());
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
    pub bucket_id: u64,
}

impl IntervalPair {
    pub const fn new(lo: u64, hi: u64, file_id: u64, bucket_id: u64) -> Self {
        Self {
            lo_addr: lo,
            hi_addr: hi,
            file_id,
            bucket_id,
        }
    }
}

impl Debug for IntervalPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "bucket {} [{}, {}] => {}",
            self.bucket_id, self.lo_addr, self.hi_addr, self.file_id
        ))
    }
}

impl IAsSlice for IntervalPair {}

impl IMetaCodec for IntervalPair {
    fn packed_size(&self) -> usize {
        size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        must_true!(eq to.len(), self.packed_size());
        to.copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        Self::from_slice(src)
    }
}

#[derive(Default)]
pub struct DelInterval {
    pub lo: Vec<u64>,
    pub bucket_id: u64,
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
    bucket_id: u64,
}

impl IAsSlice for DelIntervalStartHdr {}

impl IMetaCodec for DelInterval {
    fn packed_size(&self) -> usize {
        self.lo.len() * size_of::<u64>() + size_of::<DelIntervalStartHdr>()
    }

    fn encode(&self, to: &mut [u8]) {
        must_true!(eq to.len(), self.packed_size());
        let hdr = DelIntervalStartHdr {
            nr_lo: self.lo.len() as u16,
            bucket_id: self.bucket_id,
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
        let mut r = DelInterval {
            lo: Vec::new(),
            bucket_id: hdr.bucket_id,
        };
        for i in 0..hdr.nr_lo as usize {
            let id = unsafe { p.add(i).read_unaligned() };
            r.push(id);
        }
        r
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BucketMeta {
    pub id: u64,
    pub options: BucketOptions,
}

impl IMetaCodec for BucketMeta {
    fn packed_size(&self) -> usize {
        size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        must_true!(eq to.len(), self.packed_size());
        to.copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        unsafe { std::ptr::read_unaligned(src.as_ptr().cast::<Self>()) }
    }
}

impl IAsSlice for BucketMeta {}

pub enum MetaOp {
    Put(Vec<u8>, Vec<u8>),
    Update(Vec<u8>, Vec<u8>, CounterMetric),
    Del(Vec<u8>),
}

pub(crate) trait IMetaCodec {
    fn packed_size(&self) -> usize;

    fn encode(&self, to: &mut [u8]);

    fn decode(src: &[u8]) -> Self;
}

pub(crate) trait MetaRecord: IMetaCodec {
    fn record(&self, kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>);
}

impl MetaRecord for Sequences {
    fn record(&self, _kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        ops.entry(BUCKET_MISC.to_string())
            .or_default()
            .push(MetaOp::Put(SEQUENCES_KEY.as_bytes().to_vec(), buf));
    }
}

impl MetaRecord for PageTable {
    fn record(&self, _kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let bucket_name = page_table_name(self.bucket_id);
        let bucket_ops = ops.entry(bucket_name).or_default();
        for (&pid, &addr) in self.iter() {
            bucket_ops.push(MetaOp::Put(
                pid.to_le_bytes().to_vec(),
                addr.to_le_bytes().to_vec(),
            ));
        }
    }
}

impl MetaRecord for BucketDurableFrontier {
    fn record(&self, _kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        ops.entry(BUCKET_FRONTIER.to_string())
            .or_default()
            .push(MetaOp::Put(self.bucket_id.to_le_bytes().to_vec(), buf));
    }
}

impl MetaRecord for PersistStat {
    fn record(&self, kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        let bucket = if kind == MetaKind::DataStat {
            BUCKET_DATA_STAT
        } else {
            BUCKET_BLOB_STAT
        };
        ops.entry(bucket.to_string())
            .or_default()
            .push(MetaOp::Put(self.file_id.to_le_bytes().to_vec(), buf));
    }
}

impl MetaRecord for IntervalPair {
    fn record(&self, kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        let bucket = if kind == MetaKind::DataInterval {
            data_interval_name(self.bucket_id)
        } else {
            blob_interval_name(self.bucket_id)
        };
        ops.entry(bucket)
            .or_default()
            .push(MetaOp::Put(self.lo_addr.to_le_bytes().to_vec(), buf));
    }
}

impl MetaRecord for Delete {
    fn record(&self, kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        match kind {
            MetaKind::DataDelete | MetaKind::BlobDelete => {
                let (obs_bucket, stat_bucket) = if kind == MetaKind::DataDelete {
                    (BUCKET_OBSOLETE_DATA, BUCKET_DATA_STAT)
                } else {
                    (BUCKET_OBSOLETE_BLOB, BUCKET_BLOB_STAT)
                };
                for &id in self.iter() {
                    let key = id.to_le_bytes().to_vec();
                    ops.entry(obs_bucket.to_string())
                        .or_default()
                        .push(MetaOp::Put(key.clone(), vec![]));
                    ops.entry(stat_bucket.to_string())
                        .or_default()
                        .push(MetaOp::Del(key));
                }
            }
            MetaKind::DataDeleteDone | MetaKind::BlobDeleteDone => {
                let bucket = if kind == MetaKind::DataDeleteDone {
                    BUCKET_OBSOLETE_DATA
                } else {
                    BUCKET_OBSOLETE_BLOB
                };
                let bucket_ops = ops.entry(bucket.to_string()).or_default();
                for &id in self.iter() {
                    bucket_ops.push(MetaOp::Del(id.to_le_bytes().to_vec()));
                }
            }
            _ => unreachable!(),
        }
    }
}

impl MetaRecord for DelInterval {
    fn record(&self, kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let bucket = if kind == MetaKind::DataDelInterval {
            data_interval_name(self.bucket_id)
        } else {
            blob_interval_name(self.bucket_id)
        };
        let bucket_ops = ops.entry(bucket).or_default();
        let mut dedup = BTreeSet::new();
        for &lo in self.iter() {
            if dedup.insert(lo) {
                bucket_ops.push(MetaOp::Del(lo.to_le_bytes().to_vec()));
            }
        }
    }
}
