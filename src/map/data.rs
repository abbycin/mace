use crate::io::{File, GatherIO};
use crc32c::Crc32cHasher;
use dashmap::DashMap;

use crate::map::IFooter;
use crate::meta::{BlobStatInner, DataStatInner, MemBlobStat, MemDataStat, Numerics, PageTable};
use crate::types::header::{TagFlag, TagKind};
use crate::types::refbox::{BoxRef, RemoteView};
use crate::types::traits::{IAsSlice, IHeader};
use crate::utils::NULL_ADDR;
use crate::utils::bitmap::BitMap;
use crate::utils::block::Block;
use crate::utils::data::{AddrPair, GatherWriter, Interval, Position};
use crate::utils::{CachePad, Handle, INIT_ID, NULL_PID};
use crate::{OpCode, static_assert};
use std::alloc::{Layout, alloc_zeroed};
use std::cell::Cell;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::Hasher;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::ptr::addr_of_mut;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64};

pub(crate) struct FlushData {
    arena: Handle<Arena>,
    cb: Box<dyn FnOnce()>,
}

unsafe impl Send for FlushData {}

impl FlushData {
    pub fn new(arena: Handle<Arena>, cb: Box<dyn FnOnce()>) -> Self {
        Self { arena, cb }
    }

    pub fn id(&self) -> u64 {
        self.arena.id()
    }

    pub fn mark_done(self) {
        (self.cb)()
    }
}

impl Deref for FlushData {
    type Target = Arena;

    fn deref(&self) -> &Self::Target {
        &self.arena
    }
}

// use C repr to fix the layout
#[repr(C)]
pub(crate) struct Arena {
    id: Cell<u64>,
    items: DashMap<u64, BoxRef>,
    /// flush LSN
    pub(crate) flsn: Box<[CachePad<AtomicU64>]>,
    pub(crate) real_size: AtomicU64,
    // pack file_id and seq
    offset: AtomicU64,
    cap: usize,
    refs: AtomicU32,
    pub(crate) state: AtomicU16,
    workers: u8,
}

impl Deref for Arena {
    type Target = DashMap<u64, BoxRef>;

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

impl Arena {
    /// memory can be allocated
    pub(crate) const HOT: u16 = 4;
    /// memory no longer available for allocating
    pub(crate) const WARM: u16 = 3;
    /// waiting for flush
    pub(crate) const COLD: u16 = 2;
    /// flushed to disk
    pub(crate) const FLUSH: u16 = 1;

    fn alloc_flsn(n: usize) -> Box<[CachePad<AtomicU64>]> {
        static_assert!(size_of::<CachePad<Position>>() == 64);
        static_assert!(align_of::<CachePad<Position>>() == 64);
        let layout = Layout::from_size_align(64 * n, 64).unwrap();
        unsafe {
            let p = alloc_zeroed(layout);
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                p.cast::<CachePad<AtomicU64>>(),
                n,
            ))
        }
    }

    pub(crate) fn new(cap: usize, workers: u8) -> Self {
        Self {
            items: DashMap::with_capacity(16 << 10),
            flsn: Self::alloc_flsn(workers as usize),
            id: Cell::new(INIT_ID),
            refs: AtomicU32::new(0),
            offset: AtomicU64::new(0),
            real_size: AtomicU64::new(0),
            cap,
            state: AtomicU16::new(Self::FLUSH),
            workers,
        }
    }

    pub(crate) fn reset(&self, id: u64) {
        self.id.set(id);
        assert_eq!(self.state(), Self::FLUSH);
        self.set_state(Arena::FLUSH, Arena::HOT);
        self.offset.store(0, Relaxed);
        self.real_size.store(0, Relaxed);
        assert!(self.unref());
        assert!(self.items.is_empty());
    }

    pub(crate) fn cap(&self) -> usize {
        self.cap
    }

    pub(crate) fn workers(&self) -> u8 {
        self.flsn.len() as u8
    }

    fn alloc_size(&self, size: u32) -> Result<(), OpCode> {
        let mut cur = self.real_size.load(Relaxed);

        loop {
            // it's possible that other thread change the state to WARM
            if self.state() != Self::HOT {
                return Err(OpCode::Again);
            }

            // this allow us over alloc once
            if cur > self.cap as u64 {
                return Err(OpCode::NeedMore);
            }

            let new = cur + size as u64;
            match self.real_size.compare_exchange(cur, new, AcqRel, Acquire) {
                Ok(_) => {
                    let off = self.offset.fetch_add(1_u64, Relaxed);
                    if off >= u32::MAX as u64 {
                        return Err(OpCode::NeedMore);
                    }
                    return Ok(());
                }
                Err(e) => cur = e,
            }
        }
    }

    fn alloc_at(&self, numerics: &Numerics, size: u32) -> BoxRef {
        let addr = numerics.address.fetch_add(1, Relaxed);
        let p = BoxRef::alloc(size, addr);
        self.items.insert(addr, p.clone());
        p
    }

    pub fn alloc(&self, numerics: &Numerics, size: u32) -> Result<BoxRef, OpCode> {
        let real_size = BoxRef::real_size(size);
        self.inc_ref();
        self.alloc_size(real_size).inspect_err(|_| self.dec_ref())?;
        Ok(self.alloc_at(numerics, size))
    }

    pub(crate) fn dealloc(&self, addr: u64, len: usize) {
        if self.items.remove(&addr).is_some() {
            self.real_size.fetch_sub(len as u64, AcqRel);
        }
    }

    #[inline]
    pub(crate) fn load(&self, addr: u64) -> Option<BoxRef> {
        self.items.get(&addr).map(|x| x.value().clone())
    }

    pub(crate) fn set_state(&self, cur: u16, new: u16) -> u16 {
        self.state
            .compare_exchange(cur, new, AcqRel, Acquire)
            .unwrap_or_else(|x| x)
    }

    pub(crate) fn state(&self) -> u16 {
        self.state.load(Relaxed)
    }

    /// we can't remove entry, although it has performance boost, but it may cause further lookup
    /// fail (because we allow load from WARM and COLD arena which is not flushed)
    ///
    /// if we back up the removed entry, the previous boost will be lost, and it will slow down front
    /// thread
    pub(crate) fn recycle(&self, addr: u64) -> bool {
        let addr = RemoteView::untagged(addr);
        if let Some(mut x) = self.items.get_mut(&addr) {
            let h = x.value_mut().header_mut();
            h.flag = TagFlag::TombStone;
            let _old = self.real_size.fetch_sub(h.total_size as u64, AcqRel);
            #[cfg(feature = "extra_check")]
            assert!(_old >= h.total_size as u64);
            true
        } else {
            false
        }
    }

    pub(crate) fn record_lsn(&mut self, worker_id: usize, seq: u64) {
        self.flsn[worker_id].store(seq, Relaxed);
    }

    pub(crate) fn inc_ref(&self) {
        self.refs.fetch_add(1, Relaxed);
    }

    pub(crate) fn dec_ref(&self) {
        self.refs.fetch_sub(1, AcqRel);
    }

    pub(crate) fn refcnt(&self) -> u32 {
        self.refs.load(Acquire)
    }

    fn unref(&self) -> bool {
        self.refs.load(Acquire) == 0
    }

    pub(crate) fn id(&self) -> u64 {
        self.id.get()
    }
}

impl Debug for Arena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Arena")
            .field("items ", &self.items.len())
            .field("state", &self.state)
            .field("offset", &self.offset)
            .field("id", &self.id.get())
            .finish()
    }
}

/// the layout of a flushed arena is:
/// ```text
/// +-------------+-----------+-------------+--------+
/// | data frames | intervals | relocations | footer |
/// +-------------+-----------+-------------+--------+
/// ```
/// write file from frames to footer while read file from footer to relocations
#[repr(C, packed(1))]
#[derive(Default, Debug)]
pub(crate) struct DataFooter {
    /// monotonically increasing, it's file_id on flush, average of file_id on compaction
    pub(crate) up2: u64,
    /// item's relocation table
    pub(crate) nr_reloc: u32,
    pub(crate) nr_intervals: u32,
    pub(crate) reloc_crc: u32,
    pub(crate) interval_crc: u32,
}

impl IAsSlice for DataFooter {}

impl IFooter for DataFooter {
    fn interval_crc(&self) -> u32 {
        self.interval_crc
    }

    fn reloc_crc(&self) -> u32 {
        self.reloc_crc
    }

    fn nr_interval(&self) -> usize {
        self.nr_intervals as usize
    }

    fn nr_reloc(&self) -> usize {
        self.nr_reloc as usize
    }
}

/// the layout of a blob file is:
/// ```text
/// +-------+-----------+-------------+--------+
/// | value | intervals | relocations | footer |
/// +-------+-----------+-------------+--------+
/// ```
#[repr(C, packed(1))]
#[derive(Default, Debug)]
pub(crate) struct BlobFooter {
    pub(crate) nr_reloc: u32,
    pub(crate) nr_intervals: u32,
    pub(crate) reloc_crc: u32,
    pub(crate) interval_crc: u32,
}

impl IAsSlice for BlobFooter {}

impl IFooter for BlobFooter {
    fn interval_crc(&self) -> u32 {
        self.interval_crc
    }

    fn reloc_crc(&self) -> u32 {
        self.reloc_crc
    }

    fn nr_interval(&self) -> usize {
        self.nr_intervals as usize
    }

    fn nr_reloc(&self) -> usize {
        self.nr_reloc as usize
    }
}

/// build both data and blob file
/// NOTE: the blob data may be not ordered by key especially after compaction, this is absolutely ok
/// for SSD, because it's good at random read
pub(crate) struct FileBuilder {
    data_active_size: usize,
    blob_active_size: usize,
    /// never flush to file
    pub(crate) data_junks: Vec<u64>,
    pub(crate) blob_junks: Vec<u64>,
    data_interval: Interval,
    blob_interval: Interval,
    data: Vec<BoxRef>,
    blobs: VecDeque<BoxRef>,
}

impl FileBuilder {
    fn update_addr(ivl: &mut Interval, addr: u64) {
        ivl.lo = ivl.lo.min(addr);
        ivl.hi = ivl.hi.max(addr);
    }

    pub(crate) fn new() -> Self {
        Self {
            data_active_size: 0,
            blob_active_size: 0,
            data_junks: Vec::new(),
            blob_junks: Vec::new(),
            data_interval: Interval::new(u64::MAX, 0),
            blob_interval: Interval::new(u64::MAX, 0),
            data: Vec::new(),
            blobs: VecDeque::new(),
        }
    }

    pub(crate) fn add(&mut self, f: BoxRef) {
        let h = f.header();
        match h.flag {
            // NOTE: the pid maybe NULL_PID when it's a sibling or remote page
            TagFlag::Normal | TagFlag::Sibling => {
                // all blob were allocated by RemoteView
                if h.kind == TagKind::Remote {
                    self.blob_active_size += f.dump_len();
                    Self::update_addr(&mut self.blob_interval, h.addr);
                    self.blobs.push_back(f);
                } else {
                    self.data_active_size += f.dump_len();
                    Self::update_addr(&mut self.data_interval, h.addr);
                    self.data.push(f);
                }
            }
            TagFlag::Junk => {
                let tmp = f.data_slice::<u64>();
                for &x in tmp {
                    if RemoteView::is_tagged(x) {
                        self.blob_junks.push(RemoteView::untagged(x));
                    } else {
                        self.data_junks.push(x);
                    }
                }
            }
            TagFlag::TombStone | TagFlag::Unmap => {}
        }
    }

    pub(crate) fn has_blob(&self) -> bool {
        !self.blobs.is_empty()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty() && self.data_junks.is_empty() && self.blobs.is_empty()
    }

    pub(crate) fn data_stat(&self, id: u64, tick: u64) -> MemDataStat {
        let n = self.data.len() as u32;
        MemDataStat {
            inner: DataStatInner {
                file_id: id,
                up1: tick,
                up2: tick,
                active_elems: n,
                total_elems: n,
                active_size: self.data_active_size,
                total_size: self.data_active_size,
            },
            mask: BitMap::new(n),
        }
    }

    pub(crate) fn blob_stat(&self, id: u64) -> MemBlobStat {
        let n = self.blobs.len() as u32;
        MemBlobStat {
            inner: BlobStatInner {
                file_id: id,
                active_size: self.blob_active_size,
                nr_active: n,
                nr_total: n,
            },
            mask: BitMap::new(n),
        }
    }

    pub(crate) fn build_data(&mut self, tick: u64, path: PathBuf) -> Interval {
        let mut pos: usize = 0;
        let mut w = GatherWriter::trunc(&path, 64);
        let mut relocs = Vec::with_capacity(self.data.len() * AddrPair::LEN);

        for (seq, f) in self.data.iter().enumerate() {
            let h = f.header();
            let s = f.dump_slice();
            let mut crc = Crc32cHasher::default();
            crc.write(s);
            w.queue(s);
            let reloc = AddrPair::new(h.addr, pos, s.len() as u32, seq as u32, crc.finish() as u32);
            relocs.extend_from_slice(reloc.as_slice());
            pos += s.len();
        }

        let mut interval_crc = Crc32cHasher::default();

        let is = self.data_interval.as_slice();
        interval_crc.write(is);
        w.queue(is);

        let mut reloc_crc = Crc32cHasher::default();
        let rs = relocs.as_slice();
        reloc_crc.write(rs);
        w.queue(rs);

        let hdr = DataFooter {
            up2: tick,
            nr_reloc: self.data.len() as u32,
            nr_intervals: 1,
            reloc_crc: reloc_crc.finish() as u32,
            interval_crc: interval_crc.finish() as u32,
        };

        w.queue(hdr.as_slice());
        w.flush();
        w.sync();

        self.data_interval
    }

    pub(crate) fn build_blob(&mut self, path: PathBuf) -> Interval {
        let mut pos = 0;
        let mut w = GatherWriter::trunc(&path, 64);
        let mut relocs = Vec::with_capacity(self.blobs.len() * AddrPair::LEN);

        for (seq, f) in self.blobs.iter().enumerate() {
            let h = f.header();
            let s = f.dump_slice();
            let mut crc = Crc32cHasher::default();
            crc.write(s);
            w.queue(s);
            let reloc = AddrPair::new(h.addr, pos, s.len() as u32, seq as u32, crc.finish() as u32);
            relocs.extend_from_slice(reloc.as_slice());
            pos += s.len();
        }

        let mut interval_crc = Crc32cHasher::default();
        let is = self.blob_interval.as_slice();
        interval_crc.write(is);
        w.queue(is);

        let mut reloc_crc = Crc32cHasher::default();
        let rs = relocs.as_slice();
        reloc_crc.write(rs);
        w.queue(rs);

        let hdr = BlobFooter {
            nr_reloc: self.blobs.len() as u32,
            nr_intervals: 1,
            reloc_crc: reloc_crc.finish() as u32,
            interval_crc: interval_crc.finish() as u32,
        };

        w.queue(hdr.as_slice());
        w.flush();
        w.sync();

        self.blob_interval
    }
}

pub(crate) struct MetaReader<T: IFooter> {
    file: File,
    ivl_buf: Option<Block>,
    reloc_buf: Option<Block>,
    footer: T,
    end: u64,
}

impl<T> MetaReader<T>
where
    T: IFooter,
{
    pub(crate) fn new<P: AsRef<Path>>(path: P) -> Result<Self, OpCode> {
        let file = File::options()
            .read(true)
            .trunc(false)
            .open(&path)
            .map_err(|x| {
                log::error!("can't open {:?} {:?}", x, path.as_ref());
                OpCode::IoError
            })?;
        let end = file.size().expect("can't get file size");
        if end < T::LEN as u64 {
            return Err(OpCode::NeedMore);
        }
        let mut footer = T::default();
        let tmp = unsafe {
            let p = addr_of_mut!(footer);
            std::slice::from_raw_parts_mut(p.cast::<u8>(), T::LEN)
        };
        file.read(tmp, end - T::LEN as u64)
            .map_err(|_| OpCode::IoError)?;

        Ok(Self {
            file,
            ivl_buf: None,
            reloc_buf: None,
            footer,
            end,
        })
    }

    pub(crate) fn get_reloc<'a>(&mut self) -> Result<&'a [AddrPair], OpCode> {
        if let Some(b) = self.reloc_buf.as_ref() {
            return Ok(b.slice(0, self.footer.nr_reloc()));
        }
        let len = self.footer.reloc_len();
        self.reloc_buf = Some(Block::alloc(len));
        let s = self.reloc_buf.as_ref().unwrap().mut_slice(0, len);
        self.read_meta(
            s,
            self.end - (len + T::LEN) as u64,
            self.footer.nr_reloc(),
            self.footer.reloc_crc(),
        )
    }

    pub(crate) fn get_interval<'a>(&mut self) -> Result<&'a [Interval], OpCode> {
        if let Some(b) = self.ivl_buf.as_ref() {
            return Ok(b.slice(0, self.footer.nr_interval()));
        }
        let len = self.footer.interval_len();
        self.ivl_buf = Some(Block::alloc(len));
        let s = self.ivl_buf.as_ref().unwrap().mut_slice(0, len);
        self.read_meta(
            s,
            self.end - (len + self.footer.reloc_len() + T::LEN) as u64,
            self.footer.nr_interval(),
            self.footer.interval_crc(),
        )
    }

    fn read_meta<'a, U>(
        &self,
        dst: &mut [u8],
        off: u64,
        count: usize,
        crc: u32,
    ) -> Result<&'a [U], OpCode> {
        self.file.read(dst, off).map_err(|_| OpCode::IoError)?;
        let mut h = Crc32cHasher::default();
        h.write(dst);
        if h.finish() as u32 != crc {
            log::error!("checksum mismatch, expect {} get {}", crc, h.finish());
            panic!("checksum mismatch, expect {} get {}", crc, h.finish());
        }
        Ok(unsafe { std::slice::from_raw_parts(dst.as_ptr().cast::<U>(), count) })
    }

    pub(crate) fn take(self) -> File {
        self.file
    }
}

pub(crate) struct MapBuilder {
    table: PageTable,
}

impl MapBuilder {
    pub(crate) fn new() -> Self {
        Self {
            table: PageTable::default(),
        }
    }

    fn add_impl(&mut self, pid: u64, addr: u64, is_unmap: bool) {
        debug_assert_ne!(pid, NULL_PID);
        self.table.add(pid, if is_unmap { NULL_ADDR } else { addr });
    }

    pub(crate) fn add(&mut self, f: &BoxRef) {
        let h = f.header();
        match h.flag {
            TagFlag::Normal => {
                // ignore those failed in CAS
                if h.pid != NULL_PID {
                    self.add_impl(h.pid, h.addr, false);
                }
            }
            TagFlag::Unmap => {
                self.add_impl(h.pid, h.addr, true);
            }
            TagFlag::Sibling => {
                assert_eq!(h.pid, NULL_PID);
            }
            _ => {}
        }
    }

    pub(crate) fn table(self) -> PageTable {
        self.table
    }
}

#[cfg(test)]
mod test {
    use crate::{
        Options, RandomPath,
        map::data::{DataFooter, MetaReader},
        types::{refbox::BoxRef, traits::IHeader},
        utils::INIT_ID,
    };

    use super::FileBuilder;

    #[test]
    fn data_dump_load() {
        let path = RandomPath::new();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;

        let _ = opt.create_dir();

        let (pid, addr) = (114514, 1919810);
        let mut p = BoxRef::alloc(233, addr);
        p.header_mut().pid = pid;
        let (pid1, addr1) = (192, 68);
        let mut p1 = BoxRef::alloc(666, addr1);
        p1.header_mut().pid = pid1;

        let mut builder = FileBuilder::new();

        builder.add(p.clone());
        builder.add(p1.clone());

        let path = opt.data_file(INIT_ID);
        builder.build_data(0, path);

        let mut loader = MetaReader::<DataFooter>::new(opt.data_file(INIT_ID)).unwrap();

        let reloc = loader.get_reloc().unwrap();
        let intervals = loader.get_interval().unwrap();

        assert_eq!(reloc.len(), 2);
        assert_eq!(intervals.len(), 1);

        assert_eq!({ intervals[0].lo }, addr1);
        assert_eq!({ intervals[0].hi }, addr);

        let r = &reloc[0];
        assert_eq!({ r.key }, addr);
        assert_eq!({ r.val.off }, 0);
        assert_eq!({ r.val.len }, p.dump_len() as u32);

        let r1 = &reloc[1];
        assert_eq!({ r1.key }, addr1);
        assert_eq!({ r1.val.len }, p1.dump_len() as u32);
    }
}
