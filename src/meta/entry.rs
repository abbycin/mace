use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    ptr::addr_of_mut,
    sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed},
};

use crate::{
    OpCode,
    meta::IMetaCodec,
    types::traits::IAsSlice,
    utils::{
        INIT_EPOCH, INIT_ID, INIT_ORACLE, MutRef,
        bitmap::BitMap,
        data::{AddrMap, MapEntry, Reloc},
    },
};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub(crate) enum EntryKind {
    Begin,
    Commit,
    Meta,
    Stat,
    Map,
    Lid,
    Delete,
}

impl IAsSlice for EntryKind {}

impl TryFrom<u8> for EntryKind {
    type Error = OpCode;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value > Self::Delete as u8 {
            Err(OpCode::BadData)
        } else {
            Ok(unsafe { std::mem::transmute::<u8, EntryKind>(value) })
        }
    }
}

pub(crate) const ENTRY_KIND_LEN: usize = size_of::<EntryKind>();

#[repr(C, packed(1))]
pub(crate) struct Begin {
    pub(crate) txid: u64,
}

impl IAsSlice for Begin {}

impl IMetaCodec for Begin {
    fn packed_size(&self) -> usize {
        ENTRY_KIND_LEN + size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let k = EntryKind::Begin;
        to[..ENTRY_KIND_LEN].copy_from_slice(k.as_slice());
        to[ENTRY_KIND_LEN..].copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        let k = EntryKind::from_slice(src);
        assert_eq!(k, EntryKind::Begin);
        Begin::from_slice(&src[ENTRY_KIND_LEN..])
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
        ENTRY_KIND_LEN + size_of::<Self>()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let k = EntryKind::Commit;
        to[..ENTRY_KIND_LEN].copy_from_slice(k.as_slice());
        to[ENTRY_KIND_LEN..].copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        let k = EntryKind::from_slice(src);
        assert_eq!(k, EntryKind::Commit);
        Commit::from_slice(&src[ENTRY_KIND_LEN..])
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct StatInner {
    pub file_id: u32,
    pub _padding: u32,
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
    pub deleted_elems: MutRef<Vec<u32>>,
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
    pub(super) fn update(&mut self, tick: u64, reloc: Reloc) {
        self.active_elems -= 1;
        self.active_size -= reloc.len as usize;
        self.deleted_elems.set(reloc.seq);

        if self.up1 < tick {
            self.up1 = self.up2;
            self.up2 = tick;
        }
    }

    pub(crate) fn copy(&self) -> Stat {
        Stat {
            inner: self.inner,
            deleted_elems: MutRef::new(vec![]),
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
        ENTRY_KIND_LEN + size_of::<StatHdr>() + self.len()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let k = EntryKind::Stat;
        to[..ENTRY_KIND_LEN].copy_from_slice(k.as_slice());
        let hdr = StatHdr {
            elems: self.deleted_elems.len() as u32,
            size: self.len() as u32,
        };
        to[ENTRY_KIND_LEN..ENTRY_KIND_LEN + size_of::<StatHdr>()].copy_from_slice(hdr.as_slice());
        let dst = &mut to[ENTRY_KIND_LEN + size_of::<StatHdr>()..];
        let inner_s = self.inner.as_slice();
        dst[..inner_s.len()].copy_from_slice(inner_s);
        let src = unsafe {
            let p = self.deleted_elems.as_ptr().cast::<u8>();
            std::slice::from_raw_parts(p, self.len() - size_of::<StatInner>())
        };
        dst[inner_s.len()..].copy_from_slice(src);
    }

    fn decode(src: &[u8]) -> Self {
        let k = EntryKind::from_slice(src);
        assert_eq!(k, EntryKind::Stat);
        let hdr = StatHdr::from_slice(&src[ENTRY_KIND_LEN..]);
        let inner = StatInner::from_slice(&src[ENTRY_KIND_LEN + size_of::<StatHdr>()..]);
        let mut stat = Stat {
            inner,
            deleted_elems: MutRef::new(vec![]),
        };
        let seq = unsafe {
            src.as_ptr()
                .add(ENTRY_KIND_LEN + size_of::<StatHdr>() + size_of::<StatInner>())
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
    data: BTreeMap<u64, AddrMap>,
}

impl PageTable {
    pub fn len(&self) -> usize {
        self.data.len() * size_of::<MapEntry>()
    }

    pub fn collect(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.data.iter().for_each(|(&pid, m)| {
            buf.extend_from_slice(
                MapEntry {
                    page_id: pid,
                    page_addr: m.addr,
                    epoch: m.epoch,
                }
                .as_slice(),
            );
        });
        buf
    }

    pub fn add(&mut self, pid: u64, addr: u64, epoch: u64) {
        self.data
            .entry(pid)
            .and_modify(|x| {
                if x.epoch < epoch {
                    x.epoch = epoch;
                    x.addr = addr;
                }
            })
            .or_insert(AddrMap { epoch, addr });
    }
}

impl Deref for PageTable {
    type Target = BTreeMap<u64, AddrMap>;
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
        ENTRY_KIND_LEN + size_of::<PageTableHdr>() + self.len()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let k = EntryKind::Map;
        to[0..ENTRY_KIND_LEN].copy_from_slice(k.as_slice());
        let hdr = PageTableHdr {
            elems: self.data.len() as u32,
            size: self.len(),
        };
        to[ENTRY_KIND_LEN..ENTRY_KIND_LEN + size_of::<PageTableHdr>()]
            .copy_from_slice(hdr.as_slice());
        to[ENTRY_KIND_LEN + size_of::<PageTableHdr>()..].copy_from_slice(&self.collect());
    }

    fn decode(src: &[u8]) -> Self {
        let k = EntryKind::from_slice(src);
        assert_eq!(k, EntryKind::Map);
        let hdr = PageTableHdr::from_slice(&src[ENTRY_KIND_LEN..]);
        let mut table = PageTable::default();
        let p = unsafe {
            src.as_ptr()
                .add(ENTRY_KIND_LEN + size_of::<PageTableHdr>())
                .cast::<MapEntry>()
        };

        for i in 0..hdr.elems as usize {
            let m = unsafe { p.add(i).read_unaligned() };
            table.add(m.page_id, m.page_addr, m.epoch);
        }

        table
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct Numerics {
    pub txid: u64,
    /// a snapshot of current flushed data file id
    pub flushed_id: u32,
    pub next_file_id: AtomicU32,
    pub oldest_file_id: AtomicU32,
    pub next_manifest_id: AtomicU64,
    pub oracle: AtomicU64,
    pub epoch: AtomicU64,
    pub tick: AtomicU64,
    pub wmk_oldest: AtomicU64,
}

impl Numerics {
    pub(crate) fn safe_tixd(&self) -> u64 {
        self.wmk_oldest.load(Relaxed)
    }
}

impl Default for Numerics {
    fn default() -> Self {
        Self {
            txid: 0,
            flushed_id: INIT_ID,
            next_file_id: AtomicU32::new(INIT_ID),
            oldest_file_id: AtomicU32::new(INIT_ID),
            next_manifest_id: AtomicU64::new(0),
            oracle: AtomicU64::new(INIT_ORACLE),
            epoch: AtomicU64::new(INIT_EPOCH),
            tick: AtomicU64::new(0),
            wmk_oldest: AtomicU64::new(0),
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
        size_of::<Self>() + ENTRY_KIND_LEN
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let k = EntryKind::Meta;
        to[..ENTRY_KIND_LEN].copy_from_slice(k.as_slice());
        to[ENTRY_KIND_LEN..].copy_from_slice(self.as_slice());
    }

    fn decode(src: &[u8]) -> Self {
        let k = EntryKind::from_slice(src);
        assert_eq!(k, EntryKind::Meta);
        Numerics::from_slice(&src[ENTRY_KIND_LEN..])
    }
}

#[derive(Default)]
pub struct Lid {
    pub(crate) physical_id: u32,
    /// unique id
    pub(crate) new_lids: Vec<u32>,
    pub(crate) del_lids: Vec<u32>,
}

impl Lid {
    fn new_slice(&self) -> &[u8] {
        let p = self.new_lids.as_ptr().cast::<u8>();
        let len = self.new_lids.len() * size_of::<u32>();
        unsafe { std::slice::from_raw_parts(p, len) }
    }

    fn del_slice(&self) -> &[u8] {
        let p = self.del_lids.as_ptr().cast::<u8>();
        let len = self.del_lids.len() * size_of::<u32>();
        unsafe { std::slice::from_raw_parts(p, len) }
    }

    fn len(&self) -> usize {
        (self.new_lids.len() + self.del_lids.len()) * size_of::<u32>()
    }

    pub(crate) fn new(id: u32) -> Self {
        Self {
            physical_id: id,
            new_lids: Vec::new(),
            del_lids: Vec::new(),
        }
    }

    pub(crate) fn empty() -> Self {
        Self::new(0)
    }

    pub(crate) fn reset(&mut self, id: u32) {
        self.physical_id = id;
        self.new_lids.clear();
        self.del_lids.clear();
    }

    pub(crate) fn add_multiple(&mut self, ids: &[u32]) {
        self.new_lids.extend_from_slice(ids);
    }

    pub(crate) fn add(&mut self, id: u32) {
        self.new_lids.push(id);
    }

    pub(crate) fn del(&mut self, id: u32) {
        self.del_lids.push(id);
    }
}

#[repr(C, packed(1))]
pub(crate) struct LidHdr {
    physical_id: u32,
    nr_new: u32,
    nr_del: u32,
    pub(crate) size: u32,
}

impl IAsSlice for LidHdr {}

impl IMetaCodec for Lid {
    fn packed_size(&self) -> usize {
        ENTRY_KIND_LEN + size_of::<LidHdr>() + self.len()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let k = EntryKind::Lid;
        to[..ENTRY_KIND_LEN].copy_from_slice(k.as_slice());
        let hdr = LidHdr {
            physical_id: self.physical_id,
            nr_new: self.new_lids.len() as u32,
            nr_del: self.del_lids.len() as u32,
            size: self.len() as u32,
        };
        to[ENTRY_KIND_LEN..ENTRY_KIND_LEN + size_of::<LidHdr>()].copy_from_slice(hdr.as_slice());
        let (snew, sdel) = (self.new_slice(), self.del_slice());
        let (new, del) = to[ENTRY_KIND_LEN + size_of::<LidHdr>()..].split_at_mut(snew.len());

        new.copy_from_slice(snew);
        del.copy_from_slice(sdel);
    }

    fn decode(src: &[u8]) -> Self {
        let k = EntryKind::from_slice(src);
        assert_eq!(k, EntryKind::Lid);
        let hdr = LidHdr::from_slice(&src[ENTRY_KIND_LEN..]);
        let mut lid = Lid::new(hdr.physical_id);
        let p = unsafe {
            src.as_ptr()
                .add(ENTRY_KIND_LEN + size_of::<LidHdr>())
                .cast::<u32>()
        };

        for i in 0..hdr.nr_new as usize {
            let x = unsafe { p.add(i).read_unaligned() };
            lid.new_lids.push(x);
        }

        for i in (hdr.nr_new as usize)..(hdr.nr_new + hdr.nr_del) as usize {
            let x = unsafe { p.add(i).read_unaligned() };
            lid.del_lids.push(x);
        }
        lid
    }
}

#[derive(Default)]
pub struct Delete {
    id: Vec<u32>,
}

impl Deref for Delete {
    type Target = Vec<u32>;
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
    pub(crate) size: u32,
}

impl IAsSlice for DeleteHdr {}

impl IMetaCodec for Delete {
    fn packed_size(&self) -> usize {
        self.id.len() * size_of::<u32>() + ENTRY_KIND_LEN + size_of::<DeleteHdr>()
    }

    fn encode(&self, to: &mut [u8]) {
        assert_eq!(to.len(), self.packed_size());
        let k = EntryKind::Delete;
        to[..ENTRY_KIND_LEN].copy_from_slice(k.as_slice());
        let hdr = DeleteHdr {
            nr_id: self.id.len() as u32,
            size: (self.id.len() * size_of::<u32>()) as u32,
        };
        to[ENTRY_KIND_LEN..ENTRY_KIND_LEN + size_of::<DeleteHdr>()].copy_from_slice(hdr.as_slice());
        let src = unsafe {
            let p = self.id.as_ptr().cast::<u8>();
            std::slice::from_raw_parts(p, self.len() * size_of::<u32>())
        };
        to[ENTRY_KIND_LEN + size_of::<DeleteHdr>()..].copy_from_slice(src);
    }

    fn decode(src: &[u8]) -> Self {
        let k = EntryKind::from_slice(src);
        assert_eq!(k, EntryKind::Delete);
        let hdr = DeleteHdr::from_slice(&src[ENTRY_KIND_LEN..]);
        let p = unsafe {
            src.as_ptr()
                .add(ENTRY_KIND_LEN + size_of::<DeleteHdr>())
                .cast::<u32>()
        };
        let mut r = Delete::default();
        for i in 0..hdr.nr_id as usize {
            let id = unsafe { p.add(i).read_unaligned() };
            r.push(id);
        }
        r
    }
}

pub(crate) fn get_record_size(h: EntryKind, size: usize) -> Result<usize, OpCode> {
    let sz = match h {
        EntryKind::Begin => size_of::<Begin>(),
        EntryKind::Commit => size_of::<Commit>(),
        EntryKind::Meta => size_of::<Numerics>(),
        EntryKind::Delete => size_of::<DeleteHdr>(),
        EntryKind::Lid => size_of::<LidHdr>(),
        EntryKind::Map => size_of::<PageTableHdr>(),
        EntryKind::Stat => size_of::<StatHdr>(),
    } + 1; // including EntryKind
    if size >= sz {
        Ok(sz)
    } else {
        Err(OpCode::NeedMore)
    }
}
