use parking_lot::{Mutex, RwLock};
use std::sync::{
    Arc, OnceLock,
    atomic::{
        AtomicBool, AtomicIsize, AtomicU64, AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
    mpsc::{Receiver, Sender},
};
use std::time::{Duration, Instant};

use crate::utils::data::init_group_pos;
use crate::{
    Options,
    cc::context::Context,
    map::{
        DataReader, JunksMap, Loader, PagesMap, RetiredChain, SharedState, SparseFrontier,
        cache::{CANDIDATE_RING_SIZE, CANDIDATE_SAMPLE_RATE, CacheState, CandidateRing, NodeCache},
        data::{CheckpointTask, EpochInflight, PidMap, PidSet},
        table::Swip,
    },
    types::{page::Page, traits::IHeader},
    utils::{
        Handle, MutRef, OpCode, ROOT_PID,
        data::{GroupPositions, Position},
        interval::IntervalMap,
        lru::ShardPriorityLru,
        options::ParsedOptions,
    },
};
use crossbeam_epoch::Guard;
use dashmap::DashMap;

use super::flow::{FlowController, ForegroundWritePermit};
use super::flush::{Checkpoint, CheckpointObserver};
use crate::map::table::{BucketState, PageMap};
use crate::types::refbox::{BoxRef, RemoteView};

struct DummyDataReader;

impl DataReader for DummyDataReader {
    fn load_data(
        &self,
        _bucket_id: u64,
        _addr: u64,
        _cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode> {
        Err(OpCode::NotFound)
    }

    fn load_blob(
        &self,
        _bucket_id: u64,
        _addr: u64,
        _cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode> {
        Err(OpCode::NotFound)
    }

    fn load_blob_uncached(&self, _bucket_id: u64, _addr: u64) -> Result<BoxRef, OpCode> {
        Err(OpCode::NotFound)
    }
}

fn merge_header_frontier(dst: &mut SparseFrontier, page: &BoxRef) {
    let h = page.header();
    dst.merge_group(h.group, h.lsn);
}

fn mono_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

pub(crate) struct Current {
    pub(crate) pages: Arc<PagesMap>,
    pub(crate) bytes: Arc<AtomicUsize>,
    pub(crate) retired: Arc<JunksMap>,
    pub(crate) dirty_roots: Arc<PidMap>,
    pub(crate) unmap_pid: Arc<PidSet>,
    pub(crate) inflight: Arc<EpochInflight>,
}

pub(crate) struct WriteEpoch {
    inner: Option<Arc<Current>>,
    inflight: Arc<EpochInflight>,
}

impl std::ops::Deref for WriteEpoch {
    type Target = Current;

    fn deref(&self) -> &Self::Target {
        self.inner
            .as_deref()
            .expect("write epoch inner must exist before drop")
    }
}

impl Drop for WriteEpoch {
    fn drop(&mut self) {
        // drop generation Arc first, then publish inflight leave to close the
        // tiny window where cnt can reach zero while the generation Arc is
        // still held by this WriteEpoch during drop glue.
        let _ = self.inner.take();
        self.inflight.leave();
    }
}

pub(crate) struct PageSlot {
    // current writable generation
    pub(crate) hot: Arc<PagesMap>,
    pub(crate) hot_bytes: Arc<AtomicUsize>,
    // last swapped generation kept visible while checkpoint is in-flight
    pub(crate) sealed: Option<Arc<PagesMap>>,
    pub(crate) sealed_bytes: Option<Arc<AtomicUsize>>,
}

pub(crate) struct JunkSlot {
    // current writable retired-chain generation
    pub(crate) hot: Arc<JunksMap>,
    // last swapped retired-chain generation visible to concurrent writers
    pub(crate) sealed: Option<Arc<JunksMap>>,
}

pub(crate) struct PidSlot {
    // pid->addr roots and pid-unmap marks for the current writable generation
    pub(crate) root_map: Arc<PidMap>,
    pub(crate) unmap_pid: Arc<PidSet>,
    // tracks writers holding the current generation
    pub(crate) inflight: Arc<EpochInflight>,
}

// groups all epoch-state handles to keep Pool fields readable
struct EpochRegistry {
    // mutable per-generation slots (hot/sealed)
    page: Arc<RwLock<PageSlot>>,
    retired: Arc<RwLock<JunkSlot>>,
    root: Arc<RwLock<PidSlot>>,
    // fast writer snapshot to avoid cloning 3 locks in capture_epoch()
    hot: RwLock<Arc<Current>>,
    // barrier that forms a complete epoch cut with checkpoint()
    gate: RwLock<()>,
}

// recyclable containers reused across checkpoints
struct RecycleBins {
    pages: Arc<Mutex<Vec<PagesMap>>>,
    retired: Arc<Mutex<Vec<JunksMap>>>,
}

pub(crate) struct Pool {
    max_hot_size: usize,
    max_mem_size: usize,
    table: MutRef<PageMap>,
    chkpt: Checkpoint,
    last_chkpt_lsn: MutRef<GroupPositions>,
    flow: Arc<FlowController>,
    epochs: EpochRegistry,
    // pages pending EBR reclamation after retire_junks()
    retired_pages: Arc<PagesMap>,
    recycle: RecycleBins,
    pub(crate) state: MutRef<BucketState>,
    pub(crate) bucket_id: u64,
    flush_in: AtomicU64,
    flush_out: Arc<AtomicU64>,
    last_checkpoint_ms: AtomicU64,
}

impl Pool {
    const DIRTY_PAGE_INIT_CAP: usize = 16 << 10;

    fn new(
        ctx: Handle<Context>,
        table: MutRef<PageMap>,
        state: MutRef<BucketState>,
        flow: Arc<FlowController>,
        flush: Checkpoint,
        bucket_id: u64,
    ) -> Self {
        let hot_pages = Arc::new(PagesMap::with_capacity_and_hasher(
            Self::DIRTY_PAGE_INIT_CAP,
            Default::default(),
        ));
        let hot_bytes = Arc::new(AtomicUsize::new(0));
        let hot_retired = Arc::new(JunksMap::default());
        let dirty_roots = Arc::new(PidMap::new());
        let unmap_pid = Arc::new(PidSet::new());
        let inflight = Arc::new(EpochInflight::new());
        let hot_epoch = Arc::new(Current {
            pages: hot_pages.clone(),
            bytes: hot_bytes.clone(),
            retired: hot_retired.clone(),
            dirty_roots: dirty_roots.clone(),
            unmap_pid: unmap_pid.clone(),
            inflight: inflight.clone(),
        });

        Self {
            // trigger checkpoint by hot generation size; sealed generation may coexist while flushing
            max_hot_size: ctx.opt.checkpoint_size,
            // total dirty memory hard cap (hot + sealed) as a safety net
            max_mem_size: ctx.opt.pool_capacity,
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
                root: Arc::new(RwLock::new(PidSlot {
                    root_map: dirty_roots,
                    unmap_pid,
                    inflight,
                })),
                hot: RwLock::new(hot_epoch),
                gate: RwLock::new(()),
            },
            retired_pages: Arc::new(PagesMap::with_capacity_and_hasher(
                Self::DIRTY_PAGE_INIT_CAP,
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

    fn take_recycled_pages(&self) -> PagesMap {
        self.recycle.pages.lock().pop().unwrap_or_else(|| {
            PagesMap::with_capacity_and_hasher(Self::DIRTY_PAGE_INIT_CAP, Default::default())
        })
    }

    fn take_recycled_retired(&self) -> JunksMap {
        self.recycle.retired.lock().pop().unwrap_or_default()
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
            self.checkpoint();
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

    pub(crate) fn transfer_junks(
        &self,
        w: &WriteEpoch,
        g: &Guard,
        old_base_addr: u64,
        new_base_addr: u64,
        mut junks: Vec<u64>,
    ) {
        #[cfg(feature = "extra_check")]
        {
            let mut h = std::collections::HashSet::new();
            for &i in junks.iter() {
                let x = if crate::types::refbox::RemoteView::is_tagged(i) {
                    crate::types::refbox::RemoteView::untagged(i)
                } else {
                    i
                };
                assert!(h.insert(x), "duplicated");
            }
        }

        let mut frontier_junks = Vec::with_capacity(junks.len());
        for &x in &junks {
            if !RemoteView::is_tagged(x) {
                frontier_junks.push(x);
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
                assert!(
                    !inherited.frontier.is_empty(),
                    "retired chain for base {} missing frontier",
                    old_base_addr
                );
            }

            if junks.is_empty() && inherited.addrs.is_empty() {
                return;
            }

            // phase 2 + phase 3. classify_new_junks + fold_frontier
            for &logical in &frontier_junks {
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

        // phase 4. retire_now
        self.retire_junks(&w.pages, &w.bytes, g, &junks);

        // append current-round junks after inherited lineage
        //
        // duplicate-free expectation:
        // - inherited chain for old_base is moved once (take/remove from hot map)
        // - this round's junks are logical-unique (extra_check above)
        // - replace/evict publication is single-winner CAS under node lock
        //
        // so `RetiredChain.addrs` should not accumulate duplicates in normal flow
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
                assert!(old_bytes >= sz);
                self.retired_pages.insert(logical, page);
                retired_addrs.push(logical);
            }
        }
        if retired_addrs.is_empty() {
            return;
        }
        let retired_pages = self.retired_pages.clone();
        g.defer(move || {
            for addr in retired_addrs {
                let _ = retired_pages.remove(&addr);
            }
        });
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
        debug_assert!(idx < Options::MAX_CONCURRENT_WRITE as usize);
        self.last_chkpt_lsn[idx]
    }

    fn checkpoint(&self) {
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
        let (pages, sealed_bytes, retired, dirty_roots, unmap_pid, epoch_inflight, snap_addr) = {
            // single critical section that performs a full epoch cut:
            // swap hot generations, publish sealed generations, and rotate writer roots/inflight
            let _gate = self.epochs.gate.write();
            let mut page_epoch = self.epochs.page.write();
            let mut retired_epoch = self.epochs.retired.write();
            let mut root_epoch = self.epochs.root.write();
            let old_pages =
                std::mem::replace(&mut page_epoch.hot, Arc::new(self.take_recycled_pages()));
            let old_hot_bytes =
                std::mem::replace(&mut page_epoch.hot_bytes, Arc::new(AtomicUsize::new(0)));
            let old_retired = std::mem::replace(
                &mut retired_epoch.hot,
                Arc::new(self.take_recycled_retired()),
            );
            let old_dirty = std::mem::replace(&mut root_epoch.root_map, Arc::new(PidMap::new()));
            let old_unmap = std::mem::replace(&mut root_epoch.unmap_pid, Arc::new(PidSet::new()));
            let old_inflight =
                std::mem::replace(&mut root_epoch.inflight, Arc::new(EpochInflight::new()));
            let snap_addr = self.state.next_addr.load(Acquire).saturating_sub(1);
            page_epoch.sealed = Some(old_pages.clone());
            page_epoch.sealed_bytes = Some(old_hot_bytes.clone());
            retired_epoch.sealed = Some(old_retired.clone());
            let next_hot_epoch = Arc::new(Current {
                pages: page_epoch.hot.clone(),
                bytes: page_epoch.hot_bytes.clone(),
                retired: retired_epoch.hot.clone(),
                dirty_roots: root_epoch.root_map.clone(),
                unmap_pid: root_epoch.unmap_pid.clone(),
                inflight: root_epoch.inflight.clone(),
            });
            *self.epochs.hot.write() = next_hot_epoch;
            (
                old_pages,
                old_hot_bytes,
                old_retired,
                old_dirty,
                old_unmap,
                old_inflight,
                snap_addr,
            )
        };
        let sealed_init = sealed_bytes.load(Acquire);
        let est_bytes = sealed_init.max(1) as u64;
        let flow = self.flow.begin_checkpoint(est_bytes);

        let task = CheckpointTask {
            bucket_id: self.bucket_id,
            table: self.table.clone(),
            dirty_roots,
            unmap_pid,
            epoch_inflight,
            pages,
            sealed_bytes,
            sealed_bytes_init: sealed_init,
            retired,
            page_epoch: self.epochs.page.clone(),
            retired_epoch: self.epochs.retired.clone(),
            root_epoch: self.epochs.root.clone(),
            snap_addr,
            page_recycle: self.recycle.pages.clone(),
            retired_recycle: self.recycle.retired.clone(),
            count: self.flush_out.clone(),
            last_chkpt_lsn: self.last_chkpt_lsn.clone(),
            flow,
        };

        self.chkpt
            .tx
            .send(task)
            .expect("flusher channel disconnected before flush publish");
    }

    fn wait_checkpoint(&self) {
        while self.flush_in.load(Acquire) != self.flush_out.load(Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
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
                .acquire_foreground_permit(bytes, snapshot, || self.checkpoint())
        } else {
            self.flow.noop()
        }
    }

    pub(crate) fn has_pending_flush_data(&self) -> bool {
        if self.dirty_bytes_snapshot().1 != 0 {
            return true;
        }
        self.flush_in.load(Acquire) != self.flush_out.load(Acquire)
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
        self.checkpoint();
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.wait_checkpoint();
    }
}

pub(crate) struct BucketContext {
    pub(crate) pool: Handle<Pool>,
    pub(crate) table: MutRef<PageMap>,
    pub(crate) state: MutRef<BucketState>,
    pub(crate) data_intervals: RwLock<IntervalMap>,
    pub(crate) blob_intervals: RwLock<IntervalMap>,
    pub(crate) lru: Handle<ShardPriorityLru<BoxRef>>,
    pub(crate) bucket_id: u64,
    pub(crate) reader: Arc<dyn DataReader>,
    ctx: Handle<Context>,
    cache: NodeCache,
    candidates: CandidateRing,
    tx: Sender<SharedState>,
    candidate_tick: AtomicU64,
    final_checkpointed: AtomicBool,
    reclaimed: AtomicBool,
}

impl BucketContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        ctx: Handle<Context>,
        state: MutRef<BucketState>,
        bucket_id: u64,
        table: MutRef<PageMap>,
        flush: Checkpoint,
        lru: Handle<ShardPriorityLru<BoxRef>>,
        reader: Arc<dyn DataReader>,
        used: Arc<AtomicIsize>,
        tx: Sender<SharedState>,
    ) -> Self {
        let flow = Arc::new(FlowController::new(ctx.opt.as_ref()));
        let pool = Handle::new(Pool::new(
            ctx,
            table.clone(),
            state.clone(),
            flow,
            flush,
            bucket_id,
        ));

        Self {
            pool,
            table,
            state,
            data_intervals: RwLock::new(IntervalMap::new()),
            blob_intervals: RwLock::new(IntervalMap::new()),
            lru,
            bucket_id,
            reader,
            ctx,
            cache: NodeCache::new(used),
            candidates: CandidateRing::new(CANDIDATE_RING_SIZE),
            tx,
            candidate_tick: AtomicU64::new(0),
            final_checkpointed: AtomicBool::new(false),
            reclaimed: AtomicBool::new(false),
        }
    }

    pub(crate) fn before_foreground_write(&self, bytes: u64) -> ForegroundWritePermit {
        self.pool.before_foreground_write(bytes)
    }

    pub(crate) fn has_pending_flush_data(&self) -> bool {
        self.pool.has_pending_flush_data()
    }

    pub(crate) fn nudge_checkpoint(&self, min_interval_ms: u64) {
        self.pool.nudge_checkpoint(min_interval_ms)
    }

    pub(crate) fn checkpoint(&self) {
        self.pool.checkpoint();
    }

    pub(crate) fn loader(&self, ctx: Handle<Context>) -> Loader {
        Loader {
            pool: self.pool,
            ctx,
            lru: self.lru,
            pinned: MutRef::new(DashMap::with_capacity(Loader::PIN_CAP)),
            bucket_id: self.bucket_id,
            reader: self.reader.clone(),
        }
    }

    pub(crate) fn cache(&self, p: Page<Loader>) {
        let size = p.size() as isize;
        let state = if p.is_intl() {
            CacheState::Hot
        } else {
            CacheState::Warm
        };
        self.cache.insert(p.pid(), state, size);
        self.candidates.push(p.pid());
        self.maybe_evict();
    }

    pub(crate) fn load(&self, pid: u64) -> Result<Option<Page<Loader>>, OpCode> {
        loop {
            let swip = Swip::new(self.table.get(pid));
            if swip.is_null() {
                return Ok(None);
            }
            if !swip.is_tagged() {
                // we delay cache warm up when the value of page map entry was updated
                return Ok(Some(Page::<Loader>::from_swip(swip.raw())));
            }
            let new = Page::load(self.loader(self.ctx), swip.untagged())?;
            if self.table.cas(pid, swip.raw(), new.swip()).is_ok() {
                self.cache(new);
                return Ok(Some(new));
            } else {
                new.reclaim();
            }
        }
    }

    pub(crate) fn warm(&self, pid: u64, size: usize) {
        self.cache.touch(pid, size as isize);
        // avoid sampling when cache is far from pressure to reduce atomic contention
        let used = self.cache.used();
        if used >= (self.ctx.opt.cache_capacity as isize >> 2) {
            self.maybe_push_candidate(pid);
        }
        self.maybe_evict();
    }

    pub(crate) fn cool(&self, pid: u64) -> Option<CacheState> {
        self.cache.cool(pid)
    }

    pub(crate) fn cache_state(&self, pid: u64) -> Option<CacheState> {
        self.cache.state(pid)
    }

    pub(crate) fn evict_cache(&self, pid: u64) -> bool {
        self.cache.evict(pid)
    }

    pub(crate) fn candidate_snapshot(&self) -> Vec<u64> {
        self.candidates.snapshot()
    }

    fn maybe_push_candidate(&self, pid: u64) {
        let tick = self.candidate_tick.fetch_add(1, Relaxed) + 1;
        if tick.is_multiple_of(CANDIDATE_SAMPLE_RATE) {
            self.candidates.push(pid);
        }
    }

    fn maybe_evict(&self) {
        let threshold = self.ctx.opt.cache_capacity as isize * 80 / 100;
        if self.cache.used() >= threshold {
            let _ = self.tx.send(SharedState::Evict);
        }
    }

    fn reclaim_pages(&self) {
        let end = self.table.len();
        for pid in ROOT_PID..end {
            let swip = Swip::new(self.table.get(pid));
            if swip.is_null() || swip.is_tagged() {
                continue;
            }
            let _ = self.cache.evict(pid);
            let page = Page::<Loader>::from_swip(swip.untagged());
            page.reclaim();
        }
    }

    fn flush_and_wait(&self) {
        self.pool.checkpoint();
        self.pool.wait_checkpoint();
    }

    pub(crate) fn checkpoint_before_reclaim(&self) {
        if self.reclaimed.load(Acquire) {
            return;
        }
        self.flush_and_wait();
        self.final_checkpointed.store(true, Release);
    }

    pub(crate) fn reclaim(&self) {
        if self
            .reclaimed
            .compare_exchange(false, true, AcqRel, Acquire)
            .is_err()
        {
            return;
        }
        // buckets may still be reclaimed without an explicit final checkpoint path
        if !self.final_checkpointed.swap(false, AcqRel) {
            self.flush_and_wait();
        }
        self.reclaim_pages();
        Handle::reclaim(&self.pool);
    }
}

impl Drop for BucketContext {
    fn drop(&mut self) {
        self.reclaim();
    }
}

pub(crate) struct BucketMgr {
    pub(crate) buckets: DashMap<u64, Arc<BucketContext>>,
    pub(crate) lru: Handle<ShardPriorityLru<BoxRef>>,
    pub(crate) used: Arc<AtomicIsize>,
    pub(crate) flush: Option<Checkpoint>,
    pub(crate) tx: Sender<SharedState>,
    pub(crate) rx: Receiver<()>,
    pub(crate) ctx: Handle<Context>,
    pub(crate) reader: Arc<dyn DataReader>,
}

impl BucketMgr {
    pub(crate) fn new(
        opt: Arc<ParsedOptions>,
        ctx: Handle<Context>,
        tx: Sender<SharedState>,
        rx: Receiver<()>,
    ) -> Self {
        let reader = Arc::new(DummyDataReader);
        let used = Arc::new(AtomicIsize::new(0));
        Self {
            buckets: DashMap::new(),
            lru: Handle::new(ShardPriorityLru::new(
                opt.lru_capacity,
                opt.high_priority_ratio,
                opt.lru_max_entries,
            )),
            used,
            flush: None,
            tx,
            rx,
            ctx,
            reader,
        }
    }

    pub(crate) fn set_context(&mut self, ctx: Handle<Context>, reader: Arc<dyn DataReader>) {
        self.ctx = ctx;
        self.reader = reader;
    }

    pub(crate) fn start(
        &mut self,
        ctx: Handle<Context>,
        reader: Arc<dyn DataReader>,
        observer: Arc<dyn CheckpointObserver>,
    ) {
        self.set_context(ctx, reader);
        self.flush = Some(Checkpoint::new(ctx, observer));
    }

    pub(crate) fn quit(&self) {
        // 1) stop evictor to avoid new eviction/compaction work while draining flushes
        let _ = self.tx.send(SharedState::Quit);
        let _ = self.rx.recv();

        // 2) do the final checkpoint for each bucket while flusher is still alive
        for ctx in self.buckets.iter() {
            ctx.checkpoint_before_reclaim();
        }

        // 3) stop flusher after outstanding flush tasks are drained
        if let Some(f) = self.flush.as_ref() {
            f.quit();
        }

        // 4) release bucket contexts after the final checkpoint is already done
        self.buckets.clear();

        // 5) release shared lru
        self.lru.reclaim();
    }

    pub(crate) fn del_bucket(&self, bucket_id: u64) {
        if let Some(ctx) = self.buckets.get(&bucket_id).map(|x| x.value().clone()) {
            ctx.checkpoint_before_reclaim();
        }
        let _ = self.buckets.remove(&bucket_id);
    }

    pub(crate) fn active_contexts(&self) -> Vec<Arc<BucketContext>> {
        self.buckets
            .iter()
            .filter_map(|x| {
                let ctx = x.value().clone();
                if ctx.state.is_deleting() || ctx.state.is_drop() {
                    None
                } else {
                    Some(ctx)
                }
            })
            .collect()
    }
}
