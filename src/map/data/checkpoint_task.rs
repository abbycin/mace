use crate::map::buffer::{JunkAddrSlot, JunkSlot, PageSlot, PidSlot};
use crate::map::flow::CheckpointFlow;
use crate::map::table::PageMap;
use crate::must_true;
use crossbeam_epoch::Guard;
use dashmap::{DashMap, DashSet};
use parking_lot::{Condvar, Mutex, RwLock};

use crate::map::{JunksMap, PagesMap};
use crate::types::header::{NodeType, TagFlag, TagKind};
use crate::types::refbox::{BaseView, BoxRef, RemoteView};
use crate::types::traits::IHeader;
use crate::utils::compress::CompressorPool;
use crate::utils::data::GroupPositions;
use crate::utils::{MutRef, NULL_ADDR};
use rustc_hash::{FxHashSet, FxHasher};
use std::collections::VecDeque;
use std::hash::BuildHasherDefault;
use std::ops::Deref;
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

pub(crate) type PidMap = FastMap<u64, u64>;
pub(crate) type PidSet = FastSet<u64>;
pub(crate) type AddrSet = FastSet<u64>;

pub(crate) fn mark_root(map: &PidMap, pid: u64, addr: u64) {
    map.entry(pid)
        .and_modify(|cur| *cur = (*cur).max(addr))
        .or_insert(addr);
}

#[cfg(feature = "extra_check")]
pub(crate) fn first_live(map: &PidMap) -> Option<(u64, u64)> {
    map.iter()
        .next()
        .map(|entry| (*entry.key(), *entry.value()))
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
    pub(crate) retired_pages: Arc<PagesMap>,
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
    /// force a full fsync only for exit checkpoints
    pub(crate) force_fsync: bool,
}

pub(crate) struct Snapshot {
    pub(crate) pages: Vec<BoxRef>,
    pub(crate) unmap_pid: Arc<PidSet>,
    pub(crate) blob_junk: Vec<u64>,
    pub(crate) data_junk: Vec<u64>,
}

struct SnapshotBuild {
    start_chkpt_lsn: GroupPositions,
    chkpt_lsn: GroupPositions,
    unmap_pid: Arc<PidSet>,
    pages: Vec<BoxRef>,
    blob_junk: Vec<u64>,
    data_junk: Vec<u64>,
    seen: FxHashSet<u64>,
    queue: VecDeque<u64>,
    siblings: Vec<u64>,
    remote_hints: Vec<u64>,
    pending_carry: Vec<u64>,
    hot_pages: Arc<PagesMap>,
    hot_bytes: Arc<AtomicUsize>,
    hot_retired: Arc<JunksMap>,
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

    pub(crate) fn mark_checkpoint_progress(&self, bytes: u64) {
        self.flow.mark_progress(bytes);
    }

    pub(crate) fn release_persisted_pages(&self, addrs: &[u64]) {
        for &addr in addrs {
            if let Some((_, page)) = self.pages.remove(&addr) {
                let sz = page.header().total_size as usize;
                let old = self.sealed_bytes.fetch_sub(sz, AcqRel);
                must_true!(
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
                must_true!(
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

    fn begin_snapshot(&mut self) -> SnapshotBuild {
        #[cfg(feature = "extra_check")]
        crate::testing::fire_checkpoint_sync_point(
            crate::testing::CheckpointSyncPoint::BeforeSnapshotWaitZero,
            self.bucket_id,
        );
        self.epoch_inflight.wait_zero();
        // old-generation writers can still append to sealed_bytes between epoch cut and wait_zero
        // baseline must be sampled after wait_zero; from here sealed_bytes should only decrease
        self.sealed_bytes_init = self.sealed_bytes.load(Acquire);

        let start_chkpt_lsn = *self.last_chkpt_lsn.deref();
        let chkpt_lsn = start_chkpt_lsn;
        let unmap_pid = std::mem::take(&mut self.unmap_pid);
        let page_epoch = self.page_epoch.read();
        let hot_pages = page_epoch.hot.clone();
        let hot_bytes = page_epoch.hot_bytes.clone();
        drop(page_epoch);
        let hot_retired = self.retired_epoch.read().hot.clone();
        let hot_dirty_roots = self.root_epoch.read().root_map.clone();

        let mut queue = VecDeque::new();
        let mut root_carry = Vec::new();
        for i in self.dirty_roots.iter() {
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
                mark_root(&hot_dirty_roots, pid, addr);
                root_carry.push(addr);
            }
            if self.pages.contains_key(&addr) || hot_pages.contains_key(&addr) {
                queue.push_back(addr);
            }
        }
        self.carry_over_addrs(&mut root_carry, &hot_pages, &hot_bytes, &hot_retired);

        SnapshotBuild {
            start_chkpt_lsn,
            chkpt_lsn,
            unmap_pid,
            pages: Vec::new(),
            blob_junk: Vec::new(),
            data_junk: Vec::new(),
            seen: FxHashSet::default(),
            queue,
            siblings: Vec::new(),
            remote_hints: Vec::new(),
            pending_carry: Vec::new(),
            hot_pages,
            hot_bytes,
            hot_retired,
        }
    }

    fn walk_live_graph(&mut self, build: &mut SnapshotBuild, _guard: &Guard) {
        let SnapshotBuild {
            start_chkpt_lsn,
            chkpt_lsn,
            pages,
            blob_junk,
            data_junk,
            seen,
            queue,
            siblings,
            remote_hints,
            pending_carry,
            hot_pages,
            hot_bytes,
            hot_retired,
            ..
        } = build;

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
                    self.carry_over_addrs(pending_carry, hot_pages, hot_bytes, hot_retired);
                }
            }

            let (b, from_sealed) = if let Some(x) = self.pages.get(&addr) {
                (x.value().clone(), true)
            } else if let Some(x) = hot_pages.get(&addr) {
                (x.value().clone(), false)
            } else if addr > self.snap_addr
                && let Some(x) = self.retired_pages.get(&addr)
            {
                #[cfg(feature = "extra_check")]
                crate::testing::fire_checkpoint_sync_point(
                    crate::testing::CheckpointSyncPoint::AfterRetiredPageFallbackRead(addr),
                    self.bucket_id,
                );
                // same-checkpoint writers can retire structural junk into the temporary fallback
                // map before the sealed snapshot finishes walking stale edges above snap_addr
                (x.value().clone(), true)
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
                                must_true!(
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
                            v.frontier.apply_to(chkpt_lsn);
                            chain_frontier = true;
                        }
                        if h.node_type == NodeType::Leaf {
                            Self::enqueue_leaf_refs(base, queue, siblings, remote_hints);
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
                must_true!(pos < start_chkpt_lsn.len());
                chkpt_lsn[pos] = chkpt_lsn[pos].max(h.lsn);
            }
            pages.push(b);
        }

        self.carry_over_addrs(pending_carry, hot_pages, hot_bytes, hot_retired);
    }

    #[cfg_attr(not(feature = "extra_check"), allow(unused_variables))]
    fn finish_snapshot(&mut self, build: SnapshotBuild, _guard: &Guard) -> Snapshot {
        let SnapshotBuild {
            pages,
            unmap_pid,
            mut blob_junk,
            mut data_junk,
            seen,
            ..
        } = build;

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

        #[cfg(feature = "extra_check")]
        crate::testing::fire_checkpoint_sync_point(
            crate::testing::CheckpointSyncPoint::AfterSnapshotBuilt,
            self.bucket_id,
        );

        let chkpt_lsn = build.chkpt_lsn;
        *self.last_chkpt_lsn.raw_ref() = chkpt_lsn;
        Snapshot {
            pages,
            unmap_pid,
            blob_junk,
            data_junk,
        }
    }

    pub(crate) fn snapshot(&mut self) -> Snapshot {
        // protect retired_pages defers registered before and during the checkpoint cut
        let snapshot_guard = crossbeam_epoch::pin();
        let mut build = self.begin_snapshot();
        self.walk_live_graph(&mut build, &snapshot_guard);
        self.finish_snapshot(build, &snapshot_guard)
    }

    pub(crate) fn done(self, snapshot: Snapshot) {
        let bytes_left = self.sealed_bytes.swap(0, AcqRel);
        must_true!(
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
        // this is only reached from FlushDirective::Skip (a logically deleted bucket)
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
