use crate::io::{File, GatherIO};
use crate::map::buffer::{JunkAddrSlot, JunkSlot, PageSlot, PidSlot};
use crate::map::flow::CheckpointFlow;
use crate::map::table::PageMap;
use crc32c::Crc32cHasher;
use dashmap::{DashMap, DashSet};
use parking_lot::{Condvar, Mutex, RwLock};

use crate::map::{IFooter, JunksMap, PagesMap};
use crate::meta::{BlobStatInner, DataStatInner, MemBlobStat, MemDataStat, PageTable};
use crate::types::header::{NodeType, TagFlag, TagKind};
use crate::types::refbox::{BaseView, BoxRef, RemoteView};
use crate::types::traits::{IAsSlice, IHeader};
use crate::utils::bitmap::BitMap;
use crate::utils::compress::{COMPRESS_MIN_LEN, CompressorPool};
use crate::utils::data::{AddrPair, GatherWriter, GroupPositions, Interval};
use crate::utils::{MutRef, NULL_ADDR};
use crate::utils::{NULL_PID, OpCode};
use rustc_hash::{FxHashSet, FxHasher};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::BuildHasherDefault;
use std::hash::Hasher;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::ptr::addr_of_mut;
use std::sync::Arc;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::atomic::{AtomicU64, AtomicUsize};

type FastMap<K, V> = DashMap<K, V, BuildHasherDefault<FxHasher>>;
type FastSet<K> = DashSet<K, BuildHasherDefault<FxHasher>>;

pub(crate) struct EpochInflight {
    cnt: AtomicUsize,
    waiters: AtomicUsize,
    mu: Mutex<()>,
    cv: Condvar,
}

impl EpochInflight {
    pub(crate) fn new() -> Self {
        Self {
            cnt: AtomicUsize::new(0),
            waiters: AtomicUsize::new(0),
            mu: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    pub(crate) fn enter(&self) {
        self.cnt.fetch_add(1, AcqRel);
    }

    pub(crate) fn leave(&self) {
        // Fast path: no waiter (checkpoint thread) means no need to lock/notify.
        if self.cnt.fetch_sub(1, AcqRel) == 1 && self.waiters.load(Acquire) != 0 {
            let _g = self.mu.lock();
            self.cv.notify_all();
        }
    }

    pub(crate) fn wait_zero(&self) {
        if self.cnt.load(Acquire) == 0 {
            return;
        }
        self.waiters.fetch_add(1, AcqRel);
        if self.cnt.load(Acquire) == 0 {
            self.waiters.fetch_sub(1, AcqRel);
            return;
        }
        let mut g = self.mu.lock();
        while self.cnt.load(Acquire) != 0 {
            self.cv.wait(&mut g);
        }
        self.waiters.fetch_sub(1, AcqRel);
    }
}

pub(crate) struct PidMap {
    live: FastMap<u64, u64>,
}

impl PidMap {
    pub(crate) fn new() -> Self {
        Self {
            live: FastMap::with_hasher(BuildHasherDefault::default()),
        }
    }

    pub(crate) fn mark(&self, pid: u64, addr: u64) {
        self.live
            .entry(pid)
            .and_modify(|cur| *cur = (*cur).max(addr))
            .or_insert(addr);
    }
}

pub(crate) struct PidSet {
    live: FastSet<u64>,
}

impl PidSet {
    pub(crate) fn new() -> Self {
        Self {
            live: FastSet::with_hasher(BuildHasherDefault::default()),
        }
    }

    pub(crate) fn mark(&self, pid: u64) {
        self.live.insert(pid);
    }
}

impl Default for PidSet {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct AddrSet {
    live: FastSet<u64>,
}

impl AddrSet {
    pub(crate) fn new() -> Self {
        Self {
            live: FastSet::with_hasher(BuildHasherDefault::default()),
        }
    }

    pub(crate) fn mark(&self, addr: u64) {
        self.live.insert(addr);
    }
}

impl Default for AddrSet {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for AddrSet {
    type Target = FastSet<u64>;
    fn deref(&self) -> &Self::Target {
        &self.live
    }
}

impl Deref for PidSet {
    type Target = FastSet<u64>;
    fn deref(&self) -> &Self::Target {
        &self.live
    }
}

pub(crate) struct CheckpointTask {
    pub(crate) bucket_id: u64,
    pub(crate) enable_compression: bool,
    pub(crate) compressors: Arc<CompressorPool>,
    pub(crate) table: MutRef<PageMap>,
    pub(crate) dirty_roots: Arc<PidMap>,
    pub(crate) unmap_pid: Arc<PidSet>,
    pub(crate) epoch_inflight: Arc<EpochInflight>,
    pub(crate) pages: Arc<PagesMap>,
    pub(crate) sealed_bytes: Arc<AtomicUsize>,
    pub(crate) sealed_bytes_init: usize,
    pub(crate) retired: Arc<JunksMap>,
    pub(crate) new_junks: Arc<AddrSet>,
    pub(crate) page_epoch: Arc<RwLock<PageSlot>>,
    pub(crate) retired_epoch: Arc<RwLock<JunkSlot>>,
    pub(crate) junk_epoch: Arc<RwLock<JunkAddrSlot>>,
    pub(crate) root_epoch: Arc<RwLock<PidSlot>>,
    pub(crate) snap_addr: u64,
    pub(crate) page_recycle: Arc<Mutex<Vec<PagesMap>>>,
    pub(crate) retired_recycle: Arc<Mutex<Vec<JunksMap>>>,
    pub(crate) count: Arc<AtomicU64>,
    pub(crate) last_chkpt_lsn: MutRef<GroupPositions>,
    pub(crate) flow: CheckpointFlow,
}

unsafe impl Send for CheckpointTask {}

pub(crate) struct Snapshot {
    pub(crate) pages: Vec<BoxRef>,
    pub(crate) unmap_pid: Arc<PidSet>,
    pub(crate) blob_junk: Vec<u64>,
    pub(crate) data_junk: Vec<u64>,
}

impl CheckpointTask {
    const RECYCLE_BIN_MAX: usize = 2;

    fn dedup_junks(junks: &mut Vec<u64>) {
        if junks.len() < 2 {
            return;
        }

        let mut seen = FxHashSet::default();
        junks.retain(|addr| seen.insert(*addr));
    }

    fn recycle_generation<T, F>(
        generation: Arc<T>,
        recycle: &Arc<Mutex<Vec<T>>>,
        unique_msg: &'static str,
        clear: F,
    ) where
        F: FnOnce(&T),
    {
        let map = Arc::try_unwrap(generation).unwrap_or_else(|_| panic!("{unique_msg}"));
        clear(&map);
        let mut recycle_lk = recycle.lock();
        if recycle_lk.len() < Self::RECYCLE_BIN_MAX {
            recycle_lk.push(map);
        }
    }

    pub(crate) fn mark_io_built(&self, bytes: u64, elapsed: std::time::Duration) {
        self.flow.mark_io_built(bytes, elapsed);
    }

    pub(crate) fn mark_checkpoint_progress(&self, bytes: u64) {
        self.flow.mark_progress(bytes);
    }

    pub(crate) fn release_persisted_pages(&self, addrs: &[u64]) {
        for &addr in addrs {
            if let Some((_, page)) = self.pages.remove(&addr) {
                let sz = page.header().total_size as usize;
                let old = self.sealed_bytes.fetch_sub(sz, AcqRel);
                assert!(
                    old >= sz,
                    "sealed bytes underflow while releasing persisted addr {addr}: old={old}, size={sz}"
                );
            }
        }
    }

    // move live addr > snap_addr pages from sealed to hot under one publication boundary
    // without this lock, readers can observe the remove-from-sealed before insert-into-hot
    // and incorrectly conclude that the live page is no longer dirty, falling back to disk
    // lookup even though the page is not durable yet
    fn rehot_pages(&self, addrs: &[u64], hot_pages: &PagesMap, hot_bytes: &AtomicUsize) {
        if addrs.is_empty() {
            return;
        }
        let _epoch = self.page_epoch.write();
        for &addr in addrs {
            // it's possible because of addr > snap_addr
            if hot_pages.contains_key(&addr) {
                continue;
            }

            if let Some((_, page)) = self.pages.remove(&addr) {
                let sz = page.header().total_size as usize;
                let old = self.sealed_bytes.fetch_sub(sz, AcqRel);
                assert!(
                    old >= sz,
                    "sealed bytes underflow while carrying over addr {addr}: old={old}, size={sz}"
                );
                hot_bytes.fetch_add(sz, AcqRel);
                hot_pages.insert(addr, page);
            }
        }
    }

    // move retired lineage for the same carry-over set under one publication boundary
    // without this lock, readers can miss the retired chain while the page has already been
    // re-published in hot, breaking the expectation that dirty-page visibility and retired
    // metadata move together across generations
    fn rehot_junks(&self, base_addrs: &[u64], hot_retired: &JunksMap) {
        if base_addrs.is_empty() {
            return;
        }
        let _epoch = self.retired_epoch.write();
        for &base_addr in base_addrs {
            if let Some((_, mut chain)) = self.retired.remove(&base_addr) {
                if let Some(mut cur) = hot_retired.get_mut(&base_addr) {
                    cur.frontier.merge_sparse(&chain.frontier);
                    cur.addrs.append(&mut chain.addrs);
                } else {
                    hot_retired.insert(base_addr, chain);
                }
            }
        }
    }

    fn carry_over_addrs(
        &self,
        addrs: &mut Vec<u64>,
        hot_pages: &PagesMap,
        hot_bytes: &AtomicUsize,
        hot_retired: &JunksMap,
    ) {
        if addrs.is_empty() {
            return;
        }
        Self::dedup_junks(addrs);
        self.rehot_pages(addrs, hot_pages, hot_bytes);
        self.rehot_junks(addrs, hot_retired);
        addrs.clear();
    }

    fn enqueue_leaf_refs(
        base: BaseView,
        q: &mut VecDeque<u64>,
        siblings: &mut Vec<u64>,
        remotes: &mut Vec<u64>,
    ) {
        siblings.clear();
        if base.load_sibling_heads_hint(siblings) {
            for &addr in siblings.iter() {
                q.push_back(addr);
            }
        }

        remotes.clear();
        if base.load_remote_hints(remotes) {
            for &addr in remotes.iter() {
                q.push_back(addr);
            }
        }
    }

    pub(crate) fn snapshot(&mut self) -> Snapshot {
        self.epoch_inflight.wait_zero();
        // old-generation writers can still append to sealed_bytes between epoch cut and wait_zero
        // baseline must be sampled after wait_zero; from here sealed_bytes should only decrease
        self.sealed_bytes_init = self.sealed_bytes.load(Acquire);

        let start_chkpt_lsn = *self.last_chkpt_lsn.deref();
        let mut chkpt_lsn = start_chkpt_lsn;
        let unmap_pid = std::mem::take(&mut self.unmap_pid);
        let mut pages = Vec::new();
        let mut blob_junk = Vec::new();
        let mut data_junk = Vec::new();
        let page_epoch = self.page_epoch.read();
        let hot_pages = page_epoch.hot.clone();
        let hot_bytes = page_epoch.hot_bytes.clone();
        drop(page_epoch);
        let hot_retired = self.retired_epoch.read().hot.clone();
        let hot_dirty_roots = self.root_epoch.read().root_map.clone();

        let mut queue = VecDeque::new();
        let mut root_carry = Vec::new();
        for i in &self.dirty_roots.live {
            let (pid, addr) = (*i.key(), *i.value());
            if unmap_pid.contains(&pid) {
                continue;
            }
            #[cfg(feature = "extra_check")]
            assert!(
                !self.new_junks.contains(&addr),
                "dirty root {} unexpectedly points to new junk addr {}",
                pid,
                addr
            );
            if addr > self.snap_addr {
                hot_dirty_roots.mark(pid, addr);
                root_carry.push(addr);
            }
            if self.pages.contains_key(&addr) || hot_pages.contains_key(&addr) {
                queue.push_back(addr);
            }
        }
        self.carry_over_addrs(&mut root_carry, &hot_pages, &hot_bytes, &hot_retired);

        let mut seen = FxHashSet::default();
        let mut siblings = Vec::new();
        let mut remote_hints = Vec::new();
        let mut pending_carry = Vec::new();

        while let Some(addr) = queue.pop_front() {
            if !seen.insert(addr) {
                continue;
            }
            #[cfg(feature = "extra_check")]
            assert!(
                !self.new_junks.contains(&addr),
                "checkpoint live graph reached new junk addr {}",
                addr
            );
            if self.new_junks.contains(&addr) {
                continue;
            }

            if addr > self.snap_addr {
                pending_carry.push(addr);
                if pending_carry.len() >= 64 {
                    self.carry_over_addrs(&mut pending_carry, &hot_pages, &hot_bytes, &hot_retired);
                }
            }

            let (b, from_sealed) = if let Some(x) = self.pages.get(&addr) {
                (x.value().clone(), true)
            } else if let Some(x) = hot_pages.get(&addr) {
                (x.value().clone(), false)
            } else {
                #[cfg(feature = "extra_check")]
                assert!(
                    addr <= self.snap_addr,
                    "checkpoint live addr {} above snap_addr {} disappeared from dirty generations",
                    addr,
                    self.snap_addr
                );
                // the addr is no longer dirty in this generation, treat it as
                // already persisted or reclaimed
                continue;
            };

            let h = b.header();
            let mut chain_frontier = false;
            if h.link != NULL_ADDR {
                queue.push_back(h.link);
            }

            match h.flag {
                TagFlag::Normal | TagFlag::Sibling => {
                    if h.kind == TagKind::Base {
                        let base = b.view().as_base();
                        if from_sealed && let Some(v) = self.retired.get(&h.addr) {
                            if !v.addrs.is_empty() {
                                assert!(
                                    !v.frontier.is_empty(),
                                    "retired chain for base {} missing frontier",
                                    h.addr
                                );
                            }
                            for &x in &v.addrs {
                                let logical = if RemoteView::is_tagged(x) {
                                    RemoteView::untagged(x)
                                } else {
                                    x
                                };
                                if logical > self.snap_addr {
                                    continue;
                                }
                                // current-generation junk that is still in sealed pages has not
                                // been persisted, so metadata has nothing to retire yet
                                if self.new_junks.contains(&logical) {
                                    continue;
                                }
                                if RemoteView::is_tagged(x) {
                                    blob_junk.push(logical);
                                } else {
                                    data_junk.push(logical);
                                }
                            }
                            v.frontier.apply_to(&mut chkpt_lsn);
                            chain_frontier = true;
                        }
                        if h.node_type == NodeType::Leaf {
                            Self::enqueue_leaf_refs(
                                base,
                                &mut queue,
                                &mut siblings,
                                &mut remote_hints,
                            );
                        }
                    } else if h.kind == TagKind::Delta && h.node_type == NodeType::Leaf {
                        let remote = b.view().as_delta().val().get_remote();
                        if remote != NULL_ADDR {
                            queue.push_back(remote);
                        }
                    }
                }
            }

            if !from_sealed || h.addr > self.snap_addr {
                continue;
            }

            if !chain_frontier {
                let pos = h.group as usize;
                debug_assert!(pos < start_chkpt_lsn.len());
                chkpt_lsn[pos] = chkpt_lsn[pos].max(h.lsn);
            }
            pages.push(b);
        }

        self.carry_over_addrs(&mut pending_carry, &hot_pages, &hot_bytes, &hot_retired);

        // keep checkpoint junk output logically unique even if retired lineage
        // carries duplicates from shared history page references
        Self::dedup_junks(&mut blob_junk);
        Self::dedup_junks(&mut data_junk);

        #[cfg(feature = "extra_check")]
        {
            for &addr in &data_junk {
                assert!(
                    !seen.contains(&addr),
                    "checkpoint emitted live data addr {} as junk",
                    addr
                );
            }
            for &addr in &blob_junk {
                assert!(
                    !seen.contains(&addr),
                    "checkpoint emitted live blob addr {} as junk",
                    addr
                );
            }
        }

        *self.last_chkpt_lsn.raw_ref() = chkpt_lsn;
        Snapshot {
            pages,
            unmap_pid,
            blob_junk,
            data_junk,
        }
    }

    pub(crate) fn done(self, snapshot: Snapshot) {
        let bytes_left = self.sealed_bytes.swap(0, AcqRel);
        assert!(
            bytes_left <= self.sealed_bytes_init,
            "sealed bytes increased during checkpoint: init={} now={}",
            self.sealed_bytes_init,
            bytes_left
        );
        for x in snapshot.unmap_pid.iter() {
            self.table.recycle_pid(*x.key())
        }
        self.finish();
    }

    pub(crate) fn force_done(self) {
        // this is only reached from FlushDirective::Skip (unload/delete bucket)
        // the bucket is being reclaimed, so completion signal is enough
        self.count.fetch_add(1, AcqRel);
    }

    fn finish(self) {
        {
            let mut page_epoch = self.page_epoch.write();
            page_epoch.sealed = None;
            page_epoch.sealed_bytes = None;
            drop(page_epoch);
            self.retired_epoch.write().sealed = None;
            self.junk_epoch.write().sealed = None;
        }

        Self::recycle_generation(
            self.pages,
            &self.page_recycle,
            "sealed page generation must be uniquely owned at finish",
            |m| m.clear(),
        );
        Self::recycle_generation(
            self.retired,
            &self.retired_recycle,
            "sealed retired generation must be uniquely owned at finish",
            |m| m.clear(),
        );

        self.flow.finish();
        self.count.fetch_add(1, AcqRel);
    }
}

/// the layout of a flushed batch is:
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
    bucket_id: u64,
    enable_compression: bool,
    compressors: Arc<CompressorPool>,
    data_active_size: usize,
    blob_active_size: usize,
    data: Vec<BoxRef>,
    blobs: Vec<BoxRef>,
}

pub(crate) struct BuiltDataFile {
    pub(crate) file_id: u64,
    pub(crate) interval: Interval,
    pub(crate) writer: GatherWriter,
    pub(crate) stat: MemDataStat,
    pub(crate) addrs: Vec<u64>,
}

pub(crate) struct BuiltBlobFile {
    pub(crate) file_id: u64,
    pub(crate) interval: Interval,
    pub(crate) writer: GatherWriter,
    pub(crate) stat: MemBlobStat,
    pub(crate) addrs: Vec<u64>,
}

struct DataChunkBuild {
    built: BuiltDataFile,
    consumed: usize,
    resident_size: usize,
}

struct BlobChunkBuild {
    built: BuiltBlobFile,
    consumed: usize,
    resident_size: usize,
}

struct RawRecordMeta {
    raw_len: u32,
    crc: u32,
}

struct ChunkRecordMeta {
    raw_len: u32,
    compressed_len: u32,
    dump_len: usize,
    crc: u32,
}

impl FileBuilder {
    fn update_addr(ivl: &mut Interval, addr: u64) {
        ivl.lo = ivl.lo.min(addr);
        ivl.hi = ivl.hi.max(addr);
    }

    pub(crate) fn new(
        bucket_id: u64,
        enable_compression: bool,
        compressors: Arc<CompressorPool>,
    ) -> Self {
        Self {
            bucket_id,
            enable_compression,
            compressors,
            data_active_size: 0,
            blob_active_size: 0,
            data: Vec::new(),
            blobs: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, f: BoxRef) {
        let h = f.header();
        // NOTE: the pid maybe NULL_PID when it's a sibling or remote page
        match h.flag {
            TagFlag::Normal | TagFlag::Sibling => {
                // all blob were allocated by RemoteView
                if h.kind == TagKind::Remote {
                    self.blob_active_size += f.dump_len();
                    self.blobs.push(f);
                } else {
                    self.data_active_size += f.dump_len();
                    self.data.push(f);
                }
            }
        }
    }

    pub(crate) fn has_blob(&self) -> bool {
        !self.blobs.is_empty()
    }

    pub(crate) fn has_data(&self) -> bool {
        !self.data.is_empty()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty() && self.blobs.is_empty()
    }

    pub(crate) fn io_bytes(&self) -> u64 {
        (self.data_active_size + self.blob_active_size) as u64
    }

    fn ensure_iov_room(
        need: usize,
        queued_iov: &mut usize,
        hdr_batch: &mut Vec<[u8; BoxRef::DUMP_HDR_LEN]>,
        w: &mut GatherWriter,
    ) {
        if *queued_iov + need > GatherWriter::DEFAULT_IOVCNT {
            w.flush();
            *queued_iov = 0;
            hdr_batch.clear();
        }
    }

    fn queue_raw_parts(
        head: &[u8],
        tail: Option<&[u8]>,
        w: &mut GatherWriter,
        queued_iov: &mut usize,
        hdr_batch: &mut Vec<[u8; BoxRef::DUMP_HDR_LEN]>,
    ) {
        if let Some(body) = tail {
            Self::ensure_iov_room(2, queued_iov, hdr_batch, w);
            hdr_batch.push(head.try_into().expect("dump header must match fixed len"));
            let hdr = hdr_batch.last().expect("queued dump header must exist");
            w.queue(hdr);
            w.queue(body);
            *queued_iov += 2;
        } else {
            Self::ensure_iov_room(1, queued_iov, hdr_batch, w);
            w.queue(head);
            *queued_iov += 1;
        }
    }

    fn queue_raw_record(
        f: &BoxRef,
        w: &mut GatherWriter,
        queued_iov: &mut usize,
        hdr_batch: &mut Vec<[u8; BoxRef::DUMP_HDR_LEN]>,
    ) {
        f.with_dump_parts(|head, tail| Self::queue_raw_parts(head, tail, w, queued_iov, hdr_batch));
    }

    fn queue_raw_record_with_meta(
        f: &BoxRef,
        w: &mut GatherWriter,
        queued_iov: &mut usize,
        hdr_batch: &mut Vec<[u8; BoxRef::DUMP_HDR_LEN]>,
    ) -> RawRecordMeta {
        f.with_dump_parts(|head, tail| {
            let mut crc = Crc32cHasher::default();
            crc.write(head);
            if let Some(body) = tail {
                crc.write(body);
            }
            Self::queue_raw_parts(head, tail, w, queued_iov, hdr_batch);
            RawRecordMeta {
                raw_len: (head.len() + tail.map_or(0, <[u8]>::len)) as u32,
                crc: crc.finish() as u32,
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn fill_head_chunk_records(
        &self,
        frames: &[BoxRef],
        cap: usize,
        w: &mut GatherWriter,
        relocs: &mut Vec<u8>,
        addrs: &mut Vec<u64>,
        queued_iov: &mut usize,
        hdr_batch: &mut Vec<[u8; BoxRef::DUMP_HDR_LEN]>,
    ) -> (usize, usize, usize, Interval) {
        #[cfg(feature = "extra_check")]
        for w in frames.windows(2) {
            assert!(
                w[0].header().addr < w[1].header().addr,
                "flush frames must be strictly sorted by addr: {} then {}",
                w[0].header().addr,
                w[1].header().addr
            );
        }

        let mut pos = 0usize;
        let mut consumed = 0usize;
        let mut active_size = 0usize;
        let mut resident_size = 0usize;
        let mut interval = Interval::new(u64::MAX, 0);
        let mut compressor = None;
        let mut idx = 0;
        while idx < frames.len() {
            let f = &frames[idx];
            let raw_len = f.dump_len();

            let raw_path = !self.enable_compression || raw_len < COMPRESS_MIN_LEN;
            let mut encoded = None;
            let meta = if raw_path {
                ChunkRecordMeta {
                    raw_len: raw_len as u32,
                    compressed_len: 0,
                    dump_len: raw_len,
                    crc: 0,
                }
            } else {
                let codec = compressor.get_or_insert_with(|| {
                    self.compressors
                        .borrow()
                        .expect("record encode must succeed")
                });
                let data = codec.encode_box(f).expect("record encode must succeed");
                let meta = ChunkRecordMeta {
                    raw_len: data.raw_len,
                    compressed_len: data.compressed_len,
                    dump_len: data.active_len(),
                    crc: data.crc,
                };
                encoded = Some(data);
                meta
            };

            if consumed != 0 && active_size + meta.dump_len > cap {
                break;
            }

            let meta = if raw_path {
                let raw = Self::queue_raw_record_with_meta(f, w, queued_iov, hdr_batch);
                debug_assert_eq!(raw.raw_len as usize, raw_len);
                ChunkRecordMeta {
                    raw_len: raw.raw_len,
                    compressed_len: 0,
                    dump_len: raw_len,
                    crc: raw.crc,
                }
            } else {
                let encoded =
                    encoded.expect("encoded payload must exist after compressed planning");
                let dump_len = encoded.active_len();
                let raw_len = encoded.raw_len;
                let stored_len = encoded.compressed_len;
                let crc = encoded.crc;
                if let Some(bytes) = encoded.bytes {
                    Self::ensure_iov_room(1, queued_iov, hdr_batch, w);
                    w.queue_owned(bytes);
                    *queued_iov += 1;
                } else {
                    Self::queue_raw_record(f, w, queued_iov, hdr_batch);
                }
                ChunkRecordMeta {
                    raw_len,
                    compressed_len: stored_len,
                    dump_len,
                    crc,
                }
            };
            let h = f.header();
            let addr = h.addr;
            if consumed == 0 {
                interval = Interval::new(addr, addr);
            } else {
                Self::update_addr(&mut interval, addr);
            }
            active_size += meta.dump_len;
            resident_size += h.total_size as usize;
            addrs.push(addr);
            let reloc = AddrPair::new(
                addr,
                pos,
                meta.raw_len,
                meta.compressed_len,
                consumed as u32,
                meta.crc,
            );
            relocs.extend_from_slice(reloc.as_slice());
            pos += meta.dump_len;
            consumed += 1;
            idx += 1;
        }

        debug_assert!(consumed > 0);
        (consumed, active_size, resident_size, interval)
    }

    fn build_data_head_chunk(
        &self,
        frames: &[BoxRef],
        max_file_size: usize,
        file_id: u64,
        path: PathBuf,
    ) -> DataChunkBuild {
        assert!(!frames.is_empty());
        let cap = max_file_size.max(1);
        let mut w = GatherWriter::trunc(&path, 64);
        let mut relocs = Vec::new();
        let mut addrs = Vec::new();
        let mut queued_iov = 0usize;
        let mut hdr_batch: Vec<[u8; BoxRef::DUMP_HDR_LEN]> =
            Vec::with_capacity(GatherWriter::DEFAULT_IOVCNT / 2);
        let (consumed, active_size, resident_size, interval) = self.fill_head_chunk_records(
            frames,
            cap,
            &mut w,
            &mut relocs,
            &mut addrs,
            &mut queued_iov,
            &mut hdr_batch,
        );

        let nr_intervals = 1;
        let mut h = Crc32cHasher::default();
        let is = interval.as_slice();
        h.write(is);
        let interval_crc = h.finish() as u32;
        Self::ensure_iov_room(1, &mut queued_iov, &mut hdr_batch, &mut w);
        w.queue(is);
        queued_iov += 1;

        let mut reloc_crc = Crc32cHasher::default();
        let rs = relocs.as_slice();
        reloc_crc.write(rs);
        Self::ensure_iov_room(1, &mut queued_iov, &mut hdr_batch, &mut w);
        w.queue(rs);
        queued_iov += 1;

        let hdr = DataFooter {
            up2: file_id,
            nr_reloc: consumed as u32,
            nr_intervals,
            reloc_crc: reloc_crc.finish() as u32,
            interval_crc,
        };

        Self::ensure_iov_room(1, &mut queued_iov, &mut hdr_batch, &mut w);
        w.queue(hdr.as_slice());
        w.flush();

        let n = consumed as u32;
        let stat = MemDataStat {
            inner: DataStatInner {
                file_id,
                up1: file_id,
                up2: file_id,
                active_elems: n,
                total_elems: n,
                active_size,
                total_size: active_size,
                bucket_id: self.bucket_id,
            },
            mask: Some(BitMap::new(n)),
        };
        DataChunkBuild {
            built: BuiltDataFile {
                file_id,
                interval,
                writer: w,
                stat,
                addrs,
            },
            consumed,
            resident_size,
        }
    }

    fn build_blob_head_chunk(
        &self,
        frames: &[BoxRef],
        max_file_size: usize,
        file_id: u64,
        path: PathBuf,
    ) -> BlobChunkBuild {
        assert!(!frames.is_empty());
        let cap = max_file_size.max(1);
        let mut w = GatherWriter::trunc(&path, 64);
        let mut relocs = Vec::new();
        let mut addrs = Vec::new();
        let mut queued_iov = 0usize;
        let mut hdr_batch: Vec<[u8; BoxRef::DUMP_HDR_LEN]> =
            Vec::with_capacity(GatherWriter::DEFAULT_IOVCNT / 2);
        let (consumed, active_size, resident_size, interval) = self.fill_head_chunk_records(
            frames,
            cap,
            &mut w,
            &mut relocs,
            &mut addrs,
            &mut queued_iov,
            &mut hdr_batch,
        );

        let mut interval_crc = Crc32cHasher::default();
        let is = interval.as_slice();
        interval_crc.write(is);
        Self::ensure_iov_room(1, &mut queued_iov, &mut hdr_batch, &mut w);
        w.queue(is);
        queued_iov += 1;

        let mut reloc_crc = Crc32cHasher::default();
        let rs = relocs.as_slice();
        reloc_crc.write(rs);
        Self::ensure_iov_room(1, &mut queued_iov, &mut hdr_batch, &mut w);
        w.queue(rs);
        queued_iov += 1;

        let hdr = BlobFooter {
            nr_reloc: consumed as u32,
            nr_intervals: 1,
            reloc_crc: reloc_crc.finish() as u32,
            interval_crc: interval_crc.finish() as u32,
        };

        Self::ensure_iov_room(1, &mut queued_iov, &mut hdr_batch, &mut w);
        w.queue(hdr.as_slice());
        w.flush();
        let n = consumed as u32;
        let stat = MemBlobStat {
            inner: BlobStatInner {
                file_id,
                active_size,
                nr_active: n,
                nr_total: n,
                bucket_id: self.bucket_id,
            },
            mask: Some(BitMap::new(n)),
        };
        BlobChunkBuild {
            built: BuiltBlobFile {
                file_id,
                interval,
                writer: w,
                stat,
                addrs,
            },
            consumed,
            resident_size,
        }
    }

    pub(crate) fn flush_data_files<F, P, O>(
        &mut self,
        max_file_size: usize,
        mut alloc: F,
        mut on_flushed: P,
        mut on_file: O,
    ) -> Vec<BuiltDataFile>
    where
        F: FnMut() -> (u64, PathBuf),
        P: FnMut(u64),
        O: FnMut(&BuiltDataFile),
    {
        // sort by addr is necessary to avoid interval overlap
        self.data.sort_unstable_by_key(|x| x.header().addr);
        let mut out = Vec::new();
        while !self.data.is_empty() {
            let (file_id, path) = alloc();
            let chunk = self.build_data_head_chunk(&self.data, max_file_size, file_id, path);
            on_flushed(chunk.resident_size as u64);
            self.data.drain(..chunk.consumed);
            on_file(&chunk.built);
            out.push(chunk.built);
        }
        out
    }

    pub(crate) fn flush_blob_files<F, P, O>(
        &mut self,
        max_file_size: usize,
        mut alloc: F,
        mut on_flushed: P,
        mut on_file: O,
    ) -> Vec<BuiltBlobFile>
    where
        F: FnMut() -> (u64, PathBuf),
        P: FnMut(u64),
        O: FnMut(&BuiltBlobFile),
    {
        // sort by addr is necessary to avoid interval overlap
        self.blobs.sort_unstable_by_key(|x| x.header().addr);
        let mut out = Vec::new();
        while !self.blobs.is_empty() {
            let (file_id, path) = alloc();
            let chunk = self.build_blob_head_chunk(&self.blobs, max_file_size, file_id, path);
            on_flushed(chunk.resident_size as u64);
            self.blobs.drain(..chunk.consumed);
            on_file(&chunk.built);
            out.push(chunk.built);
        }
        out
    }
}

pub(crate) struct MetaReader<T: IFooter> {
    file: File,
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
            return Err(OpCode::NoSpace);
        }
        let mut footer = T::default();
        let tmp = unsafe {
            let p = addr_of_mut!(footer);
            std::slice::from_raw_parts_mut(p.cast::<u8>(), T::LEN)
        };
        file.read(tmp, end - T::LEN as u64)
            .map_err(|_| OpCode::Corruption)?;

        Ok(Self { file, footer, end })
    }

    pub(crate) fn get_reloc(&mut self) -> Result<Box<[AddrPair]>, OpCode> {
        let len = self.footer.reloc_len();
        self.read_meta(
            self.end - (len + T::LEN) as u64,
            self.footer.nr_reloc(),
            self.footer.reloc_crc(),
        )
    }

    pub(crate) fn get_interval(&mut self) -> Result<Box<[Interval]>, OpCode> {
        let len = self.footer.interval_len();
        self.read_meta(
            self.end - (len + self.footer.reloc_len() + T::LEN) as u64,
            self.footer.nr_interval(),
            self.footer.interval_crc(),
        )
    }

    fn read_meta<U>(&self, off: u64, count: usize, crc: u32) -> Result<Box<[U]>, OpCode> {
        let len = size_of::<U>().checked_mul(count).ok_or(OpCode::NoSpace)?;
        let mut data = Box::<[MaybeUninit<U>]>::new_uninit_slice(count);
        let dst = unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr().cast::<u8>(), len) };
        self.file.read(dst, off).map_err(|_| OpCode::IoError)?;
        let mut h = Crc32cHasher::default();
        h.write(dst);
        let actual_crc = h.finish() as u32;
        if actual_crc != crc {
            log::error!("checksum mismatch, expect {} get {}", crc, actual_crc);
            return Err(OpCode::Corruption);
        }
        let p = Box::into_raw(data) as *mut [U];
        Ok(unsafe { Box::from_raw(p) })
    }

    pub(crate) fn take(self) -> File {
        self.file
    }
}

pub(crate) struct MapBuilder {
    table: PageTable,
}

impl MapBuilder {
    pub(crate) fn new(bucket_id: u64, unmap_pid: &PidSet) -> Self {
        let mut table = PageTable::default();
        table.bucket_id = bucket_id;
        let mut this = Self { table };
        for x in unmap_pid.iter() {
            this.add_impl(*x, NULL_ADDR);
        }
        this
    }

    fn add_impl(&mut self, pid: u64, addr: u64) {
        debug_assert_ne!(pid, NULL_PID);
        self.table.add(pid, addr);
    }

    pub(crate) fn add(&mut self, f: &BoxRef) {
        let h = f.header();
        match h.flag {
            TagFlag::Normal => {
                // ignore those failed in CAS
                if h.pid != NULL_PID {
                    self.add_impl(h.pid, h.addr);
                }
            }
            TagFlag::Sibling => {
                assert_eq!(h.pid, NULL_PID);
            }
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
        types::{
            header::{NodeType, TagKind},
            refbox::BoxRef,
            traits::IHeader,
        },
        utils::{INIT_ID, compress::CompressorPool},
    };

    use super::FileBuilder;

    #[test]
    fn data_dump_load() {
        let path = RandomPath::new();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        let opt = opt.validate().unwrap();

        let _ = opt.create_dir();

        let (pid, addr) = (114514, 1919810);
        let mut p = BoxRef::alloc(233, addr);
        p.header_mut().pid = pid;
        p.header_mut().kind = TagKind::Delta;
        p.header_mut().node_type = NodeType::Leaf;
        let (pid1, addr1) = (192, 68);
        let mut p1 = BoxRef::alloc(666, addr1);
        p1.header_mut().pid = pid1;
        p1.header_mut().kind = TagKind::Delta;
        p1.header_mut().node_type = NodeType::Leaf;

        let mut builder = FileBuilder::new(0, false, CompressorPool::new());

        builder.add(p.clone());
        builder.add(p1.clone());

        let mut file_id = INIT_ID;
        let files = builder.flush_data_files(
            opt.data_file_size,
            || {
                let id = file_id;
                file_id += 1;
                (id, opt.data_file(id))
            },
            |_| {},
            |_| {},
        );
        assert_eq!(files.len(), 1);

        let mut loader = MetaReader::<DataFooter>::new(opt.data_file(INIT_ID)).unwrap();

        let reloc = loader.get_reloc().unwrap();
        let intervals = loader.get_interval().unwrap();

        assert_eq!(reloc.len(), 2);
        assert_eq!(intervals.len(), 1);

        assert_eq!({ intervals[0].lo }, addr1);
        assert_eq!({ intervals[0].hi }, addr);

        let r = reloc
            .iter()
            .find(|x| x.key == addr)
            .expect("reloc for addr must exist");
        assert_eq!({ r.val.off }, p1.dump_len());
        assert_eq!({ r.val.raw_len }, p.dump_len() as u32);
        assert_eq!({ r.val.compressed_len }, 0);

        let r1 = reloc
            .iter()
            .find(|x| x.key == addr1)
            .expect("reloc for addr1 must exist");
        assert_eq!({ r1.val.off }, 0);
        assert_eq!({ r1.val.raw_len }, p1.dump_len() as u32);
        assert_eq!({ r1.val.compressed_len }, 0);
    }
}
