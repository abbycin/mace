//! checkpoint pool: generation cut entry, dirty-memory triggers, and flow interaction

use std::sync::{
    Arc,
    atomic::{
        AtomicU64, AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
};
use std::time::Duration;

use crossbeam_epoch::Guard;
use parking_lot::{Mutex, RwLock};

use crate::Options;
use crate::map::{
    JunksMap, PagesMap, RetiredChain, SparseFrontier,
    buffer::BucketState,
    data::{AddrSet, CheckpointTask, EpochInflight, PidMap, PidSet},
    table::PageMap,
};
use crate::must_ok;
use crate::must_true;
use crate::types::{
    refbox::{BoxRef, RemoteView},
    traits::IHeader,
};
use crate::utils::{
    MutRef,
    compress::CompressorPool,
    data::{GroupPositions, Position, init_group_pos},
    options::BucketOptions,
};

use super::generation::{
    Current, Cut, DIRTY_PAGE_INIT_CAP, EpochRegistry, JunkAddrSlot, JunkSlot, PageSlot, PidSlot,
    RecycleBins, WriteEpoch, mono_ms,
};
use crate::map::flow::{FlowController, ForegroundWritePermit};
use crate::map::flush::Checkpoint;

fn merge_header_frontier(dst: &mut SparseFrontier, page: &BoxRef) {
    let h = page.header();
    dst.merge_group(h.group, h.lsn);
}

pub(crate) struct Pool {
    max_hot_size: usize,
    max_mem_size: usize,
    enable_compression: bool,
    compressors: Arc<CompressorPool>,
    table: MutRef<PageMap>,
    chkpt: Checkpoint,
    last_chkpt_lsn: MutRef<GroupPositions>,
    flow: Arc<FlowController>,
    epochs: EpochRegistry,
    retired_pages: Arc<PagesMap>,
    recycle: RecycleBins,
    pub(crate) state: MutRef<BucketState>,
    pub(crate) bucket_id: u64,
    flush_in: AtomicU64,
    flush_out: Arc<AtomicU64>,
    last_checkpoint_ms: AtomicU64,
}

impl Pool {
    pub(super) fn new(
        opt: &BucketOptions,
        compressors: Arc<CompressorPool>,
        table: MutRef<PageMap>,
        state: MutRef<BucketState>,
        flow: Arc<FlowController>,
        flush: Checkpoint,
        bucket_id: u64,
    ) -> Self {
        let hot_pages = Arc::new(PagesMap::with_capacity_and_hasher(
            DIRTY_PAGE_INIT_CAP,
            Default::default(),
        ));
        let hot_bytes = Arc::new(AtomicUsize::new(0));
        let hot_retired = Arc::new(JunksMap::default());
        let hot_new_junks = Arc::new(AddrSet::default());
        let dirty_roots = Arc::new(PidMap::default());
        let unmap_pid = Arc::new(PidSet::default());
        let inflight = Arc::new(EpochInflight::new());
        let hot_epoch = Arc::new(Current {
            pages: hot_pages.clone(),
            bytes: hot_bytes.clone(),
            retired: hot_retired.clone(),
            new_junks: hot_new_junks.clone(),
            dirty_roots: dirty_roots.clone(),
            unmap_pid: unmap_pid.clone(),
            inflight: inflight.clone(),
        });

        Self {
            // trigger checkpoint by hot generation size; sealed generation may coexist while flushing
            max_hot_size: opt.checkpoint_size,
            // total dirty memory gate (hot + sealed): pool_capacity is the
            // durable gate; foreground admission may add a bounded drain burst
            // (<= pool/8, <= flush_unit) while a checkpoint is in flight, so
            // projected dirty is capped at pool_capacity + credit_burst
            // (actual dirty can overshoot between snapshots via per-op
            // estimate gaps and unadmitted eviction growth)
            max_mem_size: opt.pool_capacity,
            enable_compression: opt.enable_compression,
            compressors,
            table,
            chkpt: flush,
            last_chkpt_lsn: MutRef::new(init_group_pos()),
            flow,
            epochs: EpochRegistry {
                page: Arc::new(RwLock::new(PageSlot {
                    hot: hot_pages,
                    hot_bytes,
                    sealed: None,
                    sealed_bytes: None,
                })),
                retired: Arc::new(RwLock::new(JunkSlot {
                    hot: hot_retired,
                    sealed: None,
                })),
                junk_addr: Arc::new(RwLock::new(JunkAddrSlot {
                    hot: hot_new_junks,
                    sealed: None,
                })),
                root: Arc::new(RwLock::new(PidSlot {
                    root_map: dirty_roots,
                    unmap_pid,
                    inflight,
                })),
                hot: RwLock::new(hot_epoch),
                gate: RwLock::new(()),
            },
            retired_pages: Arc::new(PagesMap::with_capacity_and_hasher(
                DIRTY_PAGE_INIT_CAP,
                Default::default(),
            )),
            recycle: RecycleBins {
                pages: Arc::new(Mutex::new(Vec::new())),
                retired: Arc::new(Mutex::new(Vec::new())),
            },
            state,
            bucket_id,
            flush_in: AtomicU64::new(0),
            flush_out: Arc::new(AtomicU64::new(0)),
            last_checkpoint_ms: AtomicU64::new(mono_ms()),
        }
    }

    fn next_addr(&self) -> u64 {
        self.state.next_addr.fetch_add(1, Relaxed)
    }

    pub(crate) fn capture_epoch(&self) -> WriteEpoch {
        // sync with checkpoint's write lock so a writer never straddles two generations
        let _gate = self.epochs.gate.read();
        let epoch = self.epochs.hot.read().clone();
        let inflight = epoch.inflight.clone();
        inflight.enter();
        WriteEpoch {
            inner: Some(epoch),
            inflight,
        }
    }

    pub(crate) fn alloc_in(&self, pages: &PagesMap, bytes: &AtomicUsize, size: u32) -> BoxRef {
        let addr = self.next_addr();
        let b = BoxRef::alloc(size, addr);
        let sz = b.header().total_size as usize;
        bytes.fetch_add(sz, Relaxed);
        pages.insert(addr, b.clone());
        b
    }

    fn dirty_bytes_snapshot(&self) -> (usize, usize) {
        let epoch = self.epochs.page.read();
        let hot = epoch.hot_bytes.load(Acquire);
        // include in-flight sealed bytes to keep memory pressure conservative
        let sealed = epoch
            .sealed_bytes
            .as_ref()
            .map(|x| x.load(Acquire))
            .unwrap_or(0);
        (hot, hot.saturating_add(sealed))
    }

    pub(crate) fn try_checkpoint(&self) {
        let (hot_bytes, dirty_bytes) = self.dirty_bytes_snapshot();
        if hot_bytes >= self.max_hot_size || dirty_bytes >= self.max_mem_size {
            self.checkpoint(false);
        }
    }

    fn collect_junks_impl(map: &JunksMap, base_addr: u64, dst: &mut Vec<u64>) {
        if let Some(v) = map.get(&base_addr) {
            dst.extend(v.addrs.iter().copied());
        }
    }

    fn append_chain(dst: &mut RetiredChain, mut src: RetiredChain) {
        dst.frontier.merge_sparse(&src.frontier);
        if dst.addrs.is_empty() {
            dst.addrs = src.addrs;
        } else {
            dst.addrs.append(&mut src.addrs);
        }
    }

    fn merge_chain_frontier_impl(
        map: &JunksMap,
        base_addr: u64,
        frontier: &mut SparseFrontier,
    ) -> bool {
        if let Some(v) = map.get(&base_addr) {
            frontier.merge_sparse(&v.frontier);
            true
        } else {
            false
        }
    }

    fn merge_chain_impl(map: &JunksMap, base_addr: u64, dst: &mut RetiredChain) -> bool {
        if let Some(v) = map.get(&base_addr) {
            dst.addrs.extend(v.addrs.iter().copied());
            dst.frontier.merge_sparse(&v.frontier);
            true
        } else {
            false
        }
    }

    fn take_hot_chain_impl(map: &JunksMap, base_addr: u64, dst: &mut RetiredChain) -> bool {
        if let Some((_, v)) = map.remove(&base_addr) {
            Self::append_chain(dst, v);
            true
        } else {
            false
        }
    }

    fn collect_page_frontier_impl(map: &PagesMap, addr: u64, dst: &mut SparseFrontier) -> bool {
        if let Some(v) = map.get(&addr) {
            merge_header_frontier(dst, v.value());
            true
        } else {
            false
        }
    }

    pub(crate) fn collect_junks(
        &self,
        retired: &Arc<JunksMap>,
        base_addr: u64,
        dst: &mut Vec<u64>,
    ) {
        Self::collect_junks_impl(retired, base_addr, dst);

        let retired_epoch = self.epochs.retired.read();
        if let Some(map) = retired_epoch.sealed.as_ref()
            && !Arc::ptr_eq(map, retired)
        {
            Self::collect_junks_impl(map, base_addr, dst);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn transfer_junks(
        &self,
        w: &WriteEpoch,
        g: &Guard,
        old_base_addr: u64,
        new_base_addr: u64,
        structural_junks: Vec<u64>,
        mut compaction_junks: Vec<u64>,
        old_frontier: SparseFrontier,
    ) {
        let structural_len = structural_junks.len();
        let mut junks = structural_junks;
        junks.append(&mut compaction_junks);
        let mut structural_retire_now = Vec::with_capacity(structural_len);
        for (index, &raw) in junks.iter().enumerate() {
            let logical = if RemoteView::is_tagged(raw) {
                RemoteView::untagged(raw)
            } else {
                raw
            };
            // only new junk produced in this write generation should be tracked here
            if w.pages.contains_key(&logical) {
                w.new_junks.insert(logical);
            } else {
                if index < structural_len {
                    structural_retire_now.push(raw);
                }
            }
        }

        let mut inherited = RetiredChain::default();

        {
            // keep lock order consistent with checkpoint(): page -> retired
            // borrow sealed generations under lock instead of cloning Arc to preserve
            // "sealed generation uniquely owned by checkpoint task" invariant
            let page_epoch = self.epochs.page.read();
            let retired_epoch = self.epochs.retired.read();
            let sealed_pages = page_epoch.sealed.as_ref();
            let sealed_retired = retired_epoch.sealed.as_ref();
            let hot_has_chain = !w.retired.is_empty();
            let sealed_has_chain =
                sealed_retired.is_some_and(|map| !Arc::ptr_eq(map, &w.retired) && !map.is_empty());

            // phase 1. collect_inherited
            let mut has_old_base_chain = if hot_has_chain {
                Self::take_hot_chain_impl(&w.retired, old_base_addr, &mut inherited)
            } else {
                false
            };
            if sealed_has_chain && let Some(map) = sealed_retired {
                has_old_base_chain |= Self::merge_chain_impl(map, old_base_addr, &mut inherited);
            }

            if !inherited.addrs.is_empty() {
                must_true!(
                    !inherited.frontier.is_empty(),
                    "retired chain for base {} missing frontier",
                    old_base_addr
                );
            }
            inherited.frontier.merge_sparse(&old_frontier);

            if junks.is_empty() && inherited.addrs.is_empty() {
                return;
            }

            // phase 2 + phase 3. classify_new_junks + fold_frontier
            for &logical in junks.iter().filter(|&&raw| !RemoteView::is_tagged(raw)) {
                // old_base may have no inherited retired chain on its first replacement
                // in that case we still need to fold old_base's page frontier, otherwise
                // this group's checkpoint frontier can lag behind
                if logical == old_base_addr && has_old_base_chain {
                    continue;
                }
                if hot_has_chain
                    && Self::merge_chain_frontier_impl(&w.retired, logical, &mut inherited.frontier)
                {
                    continue;
                }
                if sealed_has_chain
                    && let Some(map) = sealed_retired
                    && Self::merge_chain_frontier_impl(map, logical, &mut inherited.frontier)
                {
                    continue;
                }
                if Self::collect_page_frontier_impl(&w.pages, logical, &mut inherited.frontier) {
                    continue;
                }
                if let Some(map) = sealed_pages {
                    let _ = Self::collect_page_frontier_impl(map, logical, &mut inherited.frontier);
                }
            }

            if !Self::collect_page_frontier_impl(&w.pages, new_base_addr, &mut inherited.frontier)
                && let Some(map) = sealed_pages
            {
                let _ =
                    Self::collect_page_frontier_impl(map, new_base_addr, &mut inherited.frontier);
            }
        }

        // phase 4. only earlier-generation structural junk retires at publish time.
        // same-generation structural junk must stay dirty until checkpoint filters `new_junks`,
        // so junk addr and owner page durability close together
        self.retire_junks(&w.pages, &w.bytes, g, &structural_retire_now);

        // append current-round junks after inherited lineage
        //
        // duplicates are tolerated here and normalized later in checkpoint snapshot
        inherited.addrs.append(&mut junks);

        // phase 5. attach_new_base
        if let Some(mut cur) = w.retired.get_mut(&new_base_addr) {
            cur.frontier.merge_sparse(&inherited.frontier);
            cur.addrs.append(&mut inherited.addrs);
        } else {
            w.retired.insert(new_base_addr, inherited);
        }
    }

    fn retire_junks(
        &self,
        pages: &Arc<PagesMap>,
        bytes: &Arc<AtomicUsize>,
        g: &Guard,
        junks: &[u64],
    ) {
        let mut retired_addrs = Vec::new();
        for &raw in junks {
            let logical = if RemoteView::is_tagged(raw) {
                RemoteView::untagged(raw)
            } else {
                raw
            };
            if let Some((_, page)) = pages.remove(&logical) {
                let sz = page.header().total_size as usize;
                let old_bytes = bytes.fetch_sub(sz, AcqRel);
                must_true!(old_bytes >= sz);
                self.retired_pages.insert(logical, page);
                retired_addrs.push(logical);
            }
        }
        if retired_addrs.is_empty() {
            return;
        }
        let retired_pages = self.retired_pages.clone();
        #[cfg(feature = "extra_check")]
        let deferred_addrs = retired_addrs.clone();
        g.defer(move || {
            for addr in retired_addrs {
                let _ = retired_pages.remove(&addr);
            }
        });
        #[cfg(feature = "extra_check")]
        for addr in deferred_addrs {
            crate::testing::fire_checkpoint_sync_point(
                crate::testing::CheckpointSyncPoint::AfterRetiredPageDeferred(addr),
                self.bucket_id,
            );
        }
    }

    #[cfg(feature = "extra_check")]
    pub(crate) fn test_retire_page(&self, epoch: &WriteEpoch, g: &Guard, addr: u64) {
        self.retire_junks(&epoch.pages, &epoch.bytes, g, &[addr]);
    }

    pub(crate) fn get_dirty_page(&self, addr: u64) -> Option<BoxRef> {
        let pages = self.epochs.page.read();
        if let Some(x) = pages.hot.get(&addr) {
            return Some(x.value().clone());
        }
        if let Some(sealed) = pages.sealed.as_ref()
            && let Some(x) = sealed.get(&addr)
        {
            return Some(x.value().clone());
        }
        if let Some(x) = self.retired_pages.get(&addr) {
            return Some(x.value().clone());
        }
        None
    }

    pub(crate) fn checkpoint_lsn(&self, group: u8) -> Position {
        let idx = group as usize;
        must_true!(idx < Options::MAX_CONCURRENT_WRITE as usize);
        self.last_chkpt_lsn[idx]
    }

    pub(super) fn checkpoint(&self, force_fsync: bool) {
        // allow at most one checkpoint entry at a time
        let flushed = self.flush_out.load(Acquire);
        if self
            .flush_in
            .compare_exchange(flushed, flushed + 1, AcqRel, Acquire)
            .is_err()
        {
            return;
        }
        self.last_checkpoint_ms.store(mono_ms(), Release);
        let Cut {
            pages,
            sealed_bytes,
            retired,
            new_junks,
            dirty_roots,
            unmap_pid,
            epoch_inflight,
            snap_addr,
        } = self.epochs.cut(&self.recycle, &self.state.next_addr);
        let sealed_init = sealed_bytes.load(Acquire);
        let flow = self.flow.begin_checkpoint();

        let task = CheckpointTask {
            bucket_id: self.bucket_id,
            enable_compression: self.enable_compression,
            compressors: self.compressors.clone(),
            table: self.table.clone(),
            dirty_roots,
            unmap_pid,
            epoch_inflight,
            pages,
            retired_pages: self.retired_pages.clone(),
            sealed_bytes,
            sealed_bytes_init: sealed_init,
            retired,
            new_junks,
            page_epoch: self.epochs.page.clone(),
            retired_epoch: self.epochs.retired.clone(),
            junk_epoch: self.epochs.junk_addr.clone(),
            root_epoch: self.epochs.root.clone(),
            snap_addr,
            page_recycle: self.recycle.pages.clone(),
            retired_recycle: self.recycle.retired.clone(),
            count: self.flush_out.clone(),
            last_chkpt_lsn: self.last_chkpt_lsn.clone(),
            flow,
            force_fsync,
        };

        must_ok!(self.chkpt.tx.send(task));
    }

    pub(super) fn checkpoint_and_wait_fresh(&self, force_fsync: bool) {
        // first wait any existing checkpoint round to finish
        self.wait_checkpoint();
        // then force observing at least one completed round after this barrier
        let mut seen = self.flush_out.load(Acquire);
        loop {
            self.checkpoint(force_fsync);
            self.wait_checkpoint();
            let now = self.flush_out.load(Acquire);
            if now > seen {
                break;
            }
            seen = now;
        }
    }

    pub(super) fn wait_checkpoint(&self) {
        while self.flush_in.load(Acquire) != self.flush_out.load(Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn checkpoint_inflight(&self) -> bool {
        self.flush_in.load(Acquire) != self.flush_out.load(Acquire)
    }

    pub(crate) fn before_foreground_write(&self, bytes: u64) -> ForegroundWritePermit {
        if self.flow.is_enabled() {
            let snapshot = || {
                let (hot_bytes, dirty_bytes) = self.dirty_bytes_snapshot();
                let checkpoint_inflight =
                    self.flush_in.load(Acquire) != self.flush_out.load(Acquire);
                (hot_bytes, dirty_bytes, checkpoint_inflight)
            };
            self.flow
                .acquire_foreground_permit(bytes, snapshot, || self.checkpoint(false))
        } else {
            self.flow.noop()
        }
    }

    pub(crate) fn has_pending_flush_data(&self) -> bool {
        if self.dirty_bytes_snapshot().1 != 0 {
            return true;
        }
        self.checkpoint_inflight()
    }

    pub(crate) fn nudge_checkpoint(&self, min_interval_ms: u64) {
        if self.dirty_bytes_snapshot().1 == 0 {
            return;
        }
        if self.flush_in.load(Acquire) != self.flush_out.load(Acquire) {
            return;
        }
        let last = self.last_checkpoint_ms.load(Acquire);
        if mono_ms().saturating_sub(last) < min_interval_ms {
            return;
        }
        self.checkpoint(false);
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.wait_checkpoint();
    }
}
