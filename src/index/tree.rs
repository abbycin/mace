use crate::Options;
use crate::cc::context::{Context, TxOutcome};
use crate::map::buffer::BucketContext;
use crate::map::publish::AllocGuard;
use crate::map::{Loader, Node, Page};
use crate::types::data::{HistRef, IterItem, Record, Val};
use crate::types::node::{Junk, MergeOp, RawLeafIter, RawLeafRevIter};
use crate::types::refbox::DeltaView;
use crate::types::traits::{IAsBoxRef, IBoxHeader, IDecode, IHeader, ILoader};
use crate::utils::data::Position;
use crate::utils::observe::{
    CounterMetric, HistogramMetric, LATENCY_SAMPLE_SHIFT, observe_elapsed, sampled_instant,
};
use crate::utils::{Handle, MutRef, NULL_ADDR, OpCode};
use crate::{
    Store,
    types::{
        data::{Index, IntlKey, Key, Ver},
        refbox::BoxRef,
        traits::{ICodec, IKey},
    },
    utils::{NULL_CMD, NULL_PID},
};
use crossbeam_epoch::Guard;
use std::cmp::Ordering::Equal;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::atomic::Ordering::Acquire;

/// A reference to a value in the storage engine.
#[derive(Clone)]
pub struct ValRef {
    raw: Record,
    _owner: BoxRef,
}

#[derive(Clone, Copy)]
pub(crate) struct LatestValMeta {
    pub(crate) ver: Ver,
    pub(crate) group_id: u8,
    pub(crate) is_del: bool,
}

impl ValRef {
    pub(crate) fn new(raw: Record, owner: BoxRef) -> Self {
        Self { raw, _owner: owner }
    }

    /// Returns the data as a byte slice.
    pub fn slice(&self) -> &[u8] {
        self.raw.data()
    }

    /// Converts the reference into a owned Vec<u8>.
    pub fn to_vec(self) -> Vec<u8> {
        self.raw.data().to_vec()
    }
}

#[derive(Clone)]
pub struct Tree {
    pub(crate) store: MutRef<Store>,
    pub(crate) root_index: Index,
    pub(crate) bucket: Arc<BucketContext>,
}

impl Tree {
    pub fn new(store: MutRef<Store>, root_pid: u64, bucket: Arc<BucketContext>) -> Self {
        let this = Self {
            store,
            root_index: Index::new(root_pid),
            bucket,
        };

        let addr = this.bucket.table.index(root_pid).load(Acquire);
        if addr == NULL_ADDR {
            this.init(root_pid);
        }
        this
    }

    fn init(&self, root_pid: u64) {
        let g = crossbeam_epoch::pin();
        let mut build = self.begin_build();
        let lsn = self.store.context.group(0).logging.lock().current_pos();
        let node = Node::new_leaf(&mut build, self.bucket.loader(self.store.context), 0, lsn);
        let mut page = Page::new(node);
        let mut publish = build.into_publish(&g);
        publish.map_to(&mut page, root_pid);
        publish.cache_after_commit(page);
        publish.commit();
    }

    fn txid(&self) -> u64 {
        self.store.context.compact_safe_txid()
    }

    pub(crate) fn bucket_id(&self) -> u64 {
        self.bucket.bucket_id
    }

    pub(crate) fn begin_build(&self) -> AllocGuard<'_> {
        AllocGuard::new(&self.store.opt, &self.bucket)
    }

    pub(crate) fn load_node(&self, g: &Guard, pid: u64) -> Result<Option<Page>, OpCode> {
        loop {
            if let Some(p) = self.bucket.load(pid)? {
                let child_pid = p.header().merging_child;
                if child_pid != NULL_PID {
                    self.merge_node(p, child_pid, g)?;
                    continue;
                }
                return Ok(Some(p));
            } else {
                return Ok(None);
            }
        }
    }

    // 1. mark child node as `merging`
    // 2. find it's left sibling, to merge child into it
    // 3. replace old left sibling with merged node
    // 4. unmap child pid from page table
    // 5. remove index to child from it's parent
    fn merge_node(&self, parent_ptr: Page, child_pid: u64, g: &Guard) -> Result<(), OpCode> {
        // NOTE: a big lock is necessary because the merge process must be exclusive
        let Some(_lk) = parent_ptr.try_lock() else {
            // return ok so cooperative callers avoid retry storms
            return Ok(());
        };

        if self.bucket.table.get(parent_ptr.pid()) != parent_ptr.swip() {
            return Ok(());
        }

        assert_ne!(child_pid, NULL_PID);
        assert!(parent_ptr.is_intl());
        let child_index = parent_ptr
            .intl_iter()
            .position(|(_, idx)| idx.pid == child_pid)
            .unwrap();
        // the "can_merge_child" check is somewhat failed
        assert_ne!(child_index, 0, "we can't handle merge the leftmost node");

        let safe_txid = self.txid();
        // 1.
        let child_ptr = if let Some(x) = self.set_node_merging(child_pid, g, safe_txid)? {
            x
        } else {
            // child_pid was unmapped (and crashed) but not removed from parent yet
            self.remove_node_index(parent_ptr, child_pid, g, safe_txid);
            return Ok(());
        };

        // 2.
        let mut merge_index = child_index - 1;
        let mut cursor_pid = parent_ptr
            .intl_iter()
            .nth(merge_index)
            .map(|(_, x)| x.pid)
            .unwrap();
        let mut child_unmapped = false;

        loop {
            let cursor_ptr = if let Some(x) = self.load_node(g, cursor_pid)? {
                x
            } else {
                // the cursor pid may already be merged away
                if merge_index == 0 {
                    return Ok(());
                }

                merge_index -= 1;
                cursor_pid = parent_ptr
                    .intl_iter()
                    .nth(merge_index)
                    .map(|(_, x)| x.pid)
                    .unwrap();
                continue;
            };

            // 3. necessary, because cursor node itself maybe split concurrently
            let Some(_cursor_lk) = cursor_ptr.try_lock() else {
                continue;
            };
            if self.bucket.table.get(cursor_ptr.pid()) != cursor_ptr.swip() {
                continue;
            }

            let next_pid = cursor_ptr.header().right_sibling;
            let mut build = self.begin_build();
            // verify this candidate still points to child as its right sibling
            if next_pid == child_pid {
                let (new_node, mut junks) =
                    cursor_ptr.merge_node(&mut build, &child_ptr, safe_txid);
                child_ptr.collect_junk(|x| junks.push(x));
                build.collect_retired(child_ptr.base_addr(), &mut junks);
                let mut publish = build.into_publish(g);
                // NOTE: keep replace and mark_unmap in one publish to avoid checkpoint cutting across
                // two write epochs and making one base addr appear in both dirty roots and junk pages
                publish.replace(cursor_ptr, new_node, junks);
                publish.mark_unmap(child_pid, child_ptr.swip());
                publish.commit();
                child_unmapped = true;
                break;
            }
            let hi = cursor_ptr.hi();
            let lo = child_ptr.lo();
            if hi >= Some(lo) {
                // another thread installed merged content after we loaded cursor
                break;
            } else {
                // another thread split cursor after we loaded it
                if next_pid != NULL_PID {
                    cursor_pid = next_pid
                } else {
                    // child may already be unmapped by another completed merge
                    break;
                }
            }
        }

        // 4. hide child from the in-memory page table before reclaiming it (or else scavenge process
        // may use it cause memory bug).
        // checkpoint will later make this NULL mapping durable and only then recycle child_pid into
        // the free list. otherwise readers/gc can still load child_pid and observe a reclaimed page.
        debug_assert_eq!(child_ptr.box_header().pid, child_pid);
        if !child_unmapped {
            self.begin_build().mark_unmap(child_pid, child_ptr.swip()); // child's junks were already collected
        }
        self.bucket.evict_cache(child_pid);
        g.defer(move || child_ptr.reclaim());

        // 5.
        self.remove_node_index(parent_ptr, child_pid, g, safe_txid);

        self.store
            .opt
            .observer
            .counter(CounterMetric::TreeNodeMerge, 1);

        Ok(())
    }

    // NOTE: caller must hold parent lock
    fn remove_node_index(&self, parent_ptr: Page, child_pid: u64, g: &Guard, safe_txid: u64) {
        debug_assert_eq!(parent_ptr.header().merging_child, child_pid);

        let mut build = self.begin_build();
        let (new_ptr, junks) = parent_ptr.process_merge(&mut build, MergeOp::Merged, safe_txid);
        let mut publish = build.into_publish(g);
        publish.replace(parent_ptr, new_ptr, junks);
        publish.commit();
    }

    // 1. load child node and check if it's merging
    // 2. return if it's merging
    // 3. or else create a new node with merging set to true
    // 4. replace old child node with the new node
    // NOTE: it must be protected by lock
    fn set_node_merging(
        &self,
        child_pid: u64,
        g: &Guard,
        safe_txid: u64,
    ) -> Result<Option<Page>, OpCode> {
        let page = if let Some(x) = self.load_node(g, child_pid)? {
            x
        } else {
            return Ok(None);
        };
        if page.header().merging {
            return Ok(Some(page));
        }
        let _lk = page.lock();
        if self.bucket.table.get(page.pid()) != page.swip() {
            return Err(OpCode::Again);
        }
        let mut build = self.begin_build();
        let (new_node, junks) = page.process_merge(&mut build, MergeOp::MarkChild, safe_txid);
        let mut publish = build.into_publish(g);
        let new_page = publish.replace(page, new_node, junks);
        publish.commit();
        Ok(Some(new_page))
    }

    /// split flow:
    /// 1. build lhs/rhs from `split_overlay`
    ///    - no delta: split base directly
    ///    - has delta: compact first then split
    /// 2. map rhs and wire `lhs.right_sibling = rhs.pid`
    /// 3. publish lhs at old pid so readers can follow sibling chain
    /// 4. if parent exists, install rhs separator into parent
    /// 5. if current node is root, build and publish a new root
    fn split_node(&self, node: Page, parent_opt: Option<Page>, g: &Guard) -> Result<(), OpCode> {
        let Some(node_lock) = node.try_lock() else {
            return Err(OpCode::Again);
        };
        if self.bucket.table.get(node.pid()) != node.swip() {
            return Err(OpCode::Again);
        }
        let safe_txid = self.txid();
        let mut build = self.begin_build();
        // 1.
        let (mut lnode, rnode) = node.split(&mut build);
        let mut rpage = Page::new(rnode);

        // 2.
        let mut publish = build.into_publish(g);
        let rpid = publish.map(&mut rpage);
        lnode.header_mut().right_sibling = rpid;

        // 3.
        let junk = Junk::new();
        let lpage = publish.replace(node, lnode, junk);
        publish.cache_after_commit(rpage);
        // drop lock early so cooperative threads can make progress
        drop(node_lock);
        // publish rpage to page table
        publish.commit();

        let lo = rpage.lo();
        if let Some(parent) = parent_opt {
            // cooperative threads may race to install the same separator
            let _lk = parent.lock();
            if self.bucket.table.get(parent.pid()) != parent.swip() {
                // another thread already finished this parent update
                return Ok(());
            }
            // 4.
            let mut build = self.begin_build();
            let Some((new_node, junk)) = parent.insert_index(&mut build, lo, rpid, safe_txid)
            else {
                // parent update raced with other tree change
                return Ok(());
            };
            let mut publish = build.into_publish(g);
            publish.replace(parent, new_node, junk);
            // publish new parent to page table
            publish.commit();
            self.store
                .opt
                .observer
                .counter(CounterMetric::TreeNodeSplit, 1);
        } else {
            // 5.
            self.split_root(g, lpage, rpid, lo, safe_txid)?;
        }

        Ok(())
    }

    fn split_root(
        &self,
        g: &Guard,
        root: Page,
        rpid: u64,
        lo: &[u8],
        safe_txid: u64,
    ) -> Result<(), OpCode> {
        let _lk = root.lock();
        if self.bucket.table.get(root.pid()) != root.swip() {
            return Err(OpCode::Again);
        };
        let mut build = self.begin_build();
        let lpid = build.reserve_pid(); // no early return, no leak is possible

        // compact root before building new root because step-3 publication can race with new writes
        let (mut lnode, junk) = root.compact(&mut build, safe_txid);
        lnode.header_mut().right_sibling = rpid;
        let (group, lsn) = lnode.get_group_lsn();
        let mut lpage = Page::new(lnode);

        let new_root_node = Node::new_root(
            &mut build,
            self.bucket.loader(self.store.context),
            &[
                (IntlKey::new([].as_slice()), Index::new(lpid)),
                (IntlKey::new(lo), Index::new(rpid)),
            ],
            group,
            lsn,
        );
        let mut publish = build.into_publish(g);
        publish.map_to(&mut lpage, lpid);
        let n = publish.replace(root, new_root_node, junk);
        assert_eq!(n.box_header().pid, self.root_index.pid);
        publish.cache_after_commit(lpage);
        // publish new root to global
        publish.commit();
        self.store
            .opt
            .observer
            .counter(CounterMetric::TreeNodeSplit, 1);
        Ok(())
    }

    fn find_leaf(&self, g: &Guard, k: &[u8]) -> Result<Page, OpCode> {
        loop {
            match self.try_find_leaf(g, k) {
                Err(OpCode::Again) => {
                    g.flush();
                    continue;
                }
                Err(e) => unreachable!("invalid opcode {:?}", e),
                o => return o,
            }
        }
    }

    fn try_find_leaf(&self, g: &Guard, key: &[u8]) -> Result<Page, OpCode> {
        let mut cursor = self.root_index.pid;
        let mut parent_opt: Option<Page> = None;
        let mut unsplit_parent_opt: Option<Page> = None;
        let mut leftmost = false;

        loop {
            let node_ptr = if let Some(x) = self.load_node(g, cursor)? {
                x
            } else {
                return Err(OpCode::Again);
            };

            if node_ptr.header().merging {
                return Err(OpCode::Again);
            }

            // node may already be replaced by smo, ensure key is still in [lo, hi)
            let lo = node_ptr.lo();
            if key < lo {
                return Err(OpCode::Again);
            }

            if node_ptr.should_split(self.store.opt.split_elems) {
                self.split_node(node_ptr, parent_opt, g)?;
                return Err(OpCode::Again);
            }

            // another thread may already split this node, detect by key >= hi and follow sibling
            let hi = node_ptr.hi();
            let is_splitting = if let Some(hi) = hi { key >= hi } else { false };

            if is_splitting {
                // search from right sibling
                let rpid = node_ptr.header().right_sibling;
                assert_ne!(rpid, NULL_PID);

                if unsplit_parent_opt.is_none() && parent_opt.is_some() {
                    unsplit_parent_opt = parent_opt;
                } else if parent_opt.is_none() && lo.is_empty() {
                    // root may be in partial split state:
                    // current page is lhs and rhs is already mapped but new root is not installed yet
                    // complete root installation cooperatively
                    assert_eq!(cursor, self.root_index.pid);
                    let safe_txid = self.txid();
                    let _ = self.split_root(g, node_ptr, rpid, hi.unwrap(), safe_txid);
                    return Err(OpCode::Again);
                }
                cursor = rpid;

                continue;
            }

            // complete pending parent separator installation cooperatively
            if let Some(unsplit) = unsplit_parent_opt.take() {
                let mut build = self.begin_build();
                let _lk = unsplit.lock();
                if self.bucket.table.get(unsplit.pid()) != unsplit.swip() {
                    // another thread already finished this parent update
                    return Err(OpCode::Again);
                }

                // create a new index in intl node
                let Some((split_node, junk)) =
                    unsplit.insert_index(&mut build, lo, cursor, self.txid())
                else {
                    return Err(OpCode::Again);
                };
                let mut publish = build.into_publish(g);
                publish.replace(unsplit, split_node, junk);
                publish.commit();
                self.store
                    .opt
                    .observer
                    .counter(CounterMetric::TreeNodeSplit, 1);
            }

            if !leftmost
                && let Some(parent) = parent_opt
                && node_ptr.should_merge()
            {
                self.try_merge(g, parent, node_ptr)?;
                return Err(OpCode::Again);
            }

            if node_ptr.is_intl() {
                assert_eq!(node_ptr.delta_len(), 0);
                let (is_leftmost, pid) = node_ptr.child_index(key);
                leftmost = is_leftmost;
                parent_opt = Some(node_ptr);
                cursor = pid;
            } else {
                if node_ptr.delta_len() >= self.store.opt.consolidate_threshold as usize {
                    self.try_compact(g, node_ptr);
                    // it may need split
                    continue;
                }
                return Ok(node_ptr);
            }
        }
    }

    fn find_prev_leaf(&self, g: &Guard, key: &[u8]) -> Result<Option<Page>, OpCode> {
        let mut cursor = self.root_index.pid;
        let mut path: Vec<(u64, u64, usize)> = Vec::new();

        loop {
            let Some(node) = self.load_node(g, cursor)? else {
                return Err(OpCode::Again);
            };
            if node.header().merging {
                return Err(OpCode::Again);
            }
            if key < node.lo() {
                return Err(OpCode::Again);
            }
            if let Some(hi) = node.hi()
                && key >= hi
            {
                return Err(OpCode::Again);
            }

            if !node.is_intl() {
                break;
            }

            let sst = node.sst::<IntlKey>();
            let pos = match sst.search_by(&IntlKey::new(key), |x, y| x.raw.cmp(y.raw)) {
                Ok(pos) => pos,
                Err(pos) => pos.max(1) - 1,
            };
            let (_, idx) = sst.kv_at::<Index>(pos);
            path.push((node.pid(), node.swip(), pos));
            cursor = idx.pid;
        }

        while let Some((parent_pid, parent_swip, child_pos)) = path.pop() {
            if child_pos == 0 {
                continue;
            }

            let Some(parent) = self.load_node(g, parent_pid)? else {
                return Err(OpCode::Again);
            };
            if parent.swip() != parent_swip {
                return Err(OpCode::Again);
            }
            if !parent.is_intl() {
                return Err(OpCode::Again);
            }

            let sst = parent.sst::<IntlKey>();
            if child_pos >= parent.header().elems as usize {
                return Err(OpCode::Again);
            }
            let (_, idx) = sst.kv_at::<Index>(child_pos - 1);
            let mut pid = idx.pid;

            loop {
                let Some(node) = self.load_node(g, pid)? else {
                    return Err(OpCode::Again);
                };
                if node.header().merging {
                    return Err(OpCode::Again);
                }
                if let Some(hi) = node.hi()
                    && key > hi
                {
                    let rpid = node.header().right_sibling;
                    if rpid == NULL_PID {
                        return Err(OpCode::Again);
                    }
                    pid = rpid;
                    continue;
                }
                if key <= node.lo() {
                    return Err(OpCode::Again);
                }
                if !node.is_intl() {
                    return Ok(Some(node));
                }

                let elems = node.header().elems as usize;
                if elems == 0 {
                    return Err(OpCode::Again);
                }
                let (_, rightmost) = node.sst::<IntlKey>().kv_at::<Index>(elems - 1);
                pid = rightmost.pid;
            }
        }

        Ok(None)
    }

    fn try_compact(&self, g: &Guard, page: Page) {
        let _lk = page.lock();
        if self.bucket.table.get(page.pid()) != page.swip() {
            return;
        };

        // consolidation never retry
        let mut build = self.begin_build();
        let (new_node, junk) = page.compact(&mut build, self.txid());
        let mut publish = build.into_publish(g);
        publish.replace(page, new_node, junk);
        publish.commit();
        self.store
            .opt
            .observer
            .counter(CounterMetric::TreeNodeConsolidate, 1);
    }

    pub(crate) fn try_scavenge(&self, pid: u64, g: &Guard) -> Result<bool, OpCode> {
        let page = if let Some(p) = self.load_node(g, pid)? {
            p
        } else {
            return Ok(false);
        };

        let h = page.header();
        if h.merging || h.merging_child != NULL_ADDR {
            return Ok(false);
        }

        let safe_txid = self.txid();
        let delta_len = page.delta_len();
        let threshold = self.store.opt.consolidate_threshold as usize;

        if delta_len >= threshold {
            self.try_compact(g, page);
            return Ok(true);
        }

        if page.ref_node().has_garbage(safe_txid) {
            self.try_compact(g, page);
            return Ok(true);
        }

        Ok(false)
    }

    fn try_merge(&self, g: &Guard, parent: Page, cur: Page) -> Result<(), OpCode> {
        let Some(lk) = parent.try_lock() else {
            return Err(OpCode::Again);
        };
        if self.bucket.table.get(parent.pid()) != parent.swip() {
            return Err(OpCode::Again);
        }
        if parent.header().merging_child != NULL_ADDR {
            return Err(OpCode::Again);
        }
        let pid = cur.pid();

        if parent.can_merge_child(cur.lo(), pid) {
            let mut build = self.begin_build();
            let (new_parent, j) =
                parent.process_merge(&mut build, MergeOp::MarkParent(pid), self.txid());
            let mut publish = build.into_publish(g);
            let new_page = publish.replace(parent, new_parent, j);
            publish.commit();
            drop(lk);
            self.merge_node(new_page, pid, g)?;
        }
        Ok(())
    }

    fn link<F>(
        &self,
        _g: &Guard,
        page: Page,
        k: &Key,
        v: &Record,
        mut check: F,
    ) -> Result<(), OpCode>
    where
        F: FnMut(Page, &Key) -> Result<(u8, Position), OpCode>,
    {
        loop {
            let Some(node) = page.try_lock() else {
                continue;
            };
            let lock_started = sampled_instant(k.txid(), LATENCY_SAMPLE_SHIFT);
            let pid = page.pid();
            // consolidate happened, we must retry from root
            if self.bucket.table.get(pid) != page.swip() {
                observe_elapsed(
                    self.store.opt.observer.as_ref(),
                    HistogramMetric::TreeLinkHoldMicros,
                    lock_started,
                );
                return Err(OpCode::Again);
            };

            let (group, pos) = check(page, k)?;
            let mut build = self.begin_build();
            let (k, v) = DeltaView::from_key_val(&mut build, k, v, group, pos);

            let addr = node.insert(k, v);
            build.mark_dirty(pid, addr);
            observe_elapsed(
                self.store.opt.observer.as_ref(),
                HistogramMetric::TreeLinkHoldMicros,
                lock_started,
            );
            drop(node);
            return Ok(());
        }
    }

    fn try_put(&self, g: &Guard, key: &Key, val: &Record) -> Result<(), OpCode> {
        let page = self.find_leaf(g, key.raw())?;

        // it never write log, so use default value is always OK
        self.link(g, page, key, val, |_, _| Ok((0, Position::default())))?;
        Ok(())
    }

    /// for non-txn use, such as registry and recovery
    pub fn put(&self, g: &Guard, key: Key, val: Record) -> Result<(), OpCode> {
        loop {
            match self.try_put(g, &key, &val) {
                Ok(_) => return Ok(()),
                Err(OpCode::Again) => {
                    self.store
                        .opt
                        .observer
                        .counter(CounterMetric::TreeRetryAgain, 1);
                    g.flush();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn try_update<F>(
        &self,
        g: &Guard,
        key: &Key,
        val: &Record,
        visible: &mut F,
    ) -> Result<Option<LatestValMeta>, OpCode>
    where
        F: FnMut(&Option<LatestValMeta>) -> Result<(u8, Position), OpCode>,
    {
        let page = self.find_leaf(g, key.raw)?;
        let mut r = None;

        self.link(g, page, key, val, |pg, k| {
            let tmp = pg.find_latest_meta(k);
            r = tmp.map(|meta| LatestValMeta {
                ver: meta.ver,
                group_id: meta.group_id,
                is_del: meta.is_del,
            });
            visible(&r)
        })?;

        Ok(r)
    }

    // NOTE: the `visible` function may be called multiple times
    pub fn update<F>(
        &self,
        g: &Guard,
        key: Key,
        val: Record,
        mut visible: F,
    ) -> Result<Option<LatestValMeta>, OpCode>
    where
        F: FnMut(&Option<LatestValMeta>) -> Result<(u8, Position), OpCode>,
    {
        let ksz = key.packed_size();
        if ksz > Options::MAX_KEY_SIZE || ksz + val.packed_size() > Options::MAX_KV_SIZE {
            return Err(OpCode::TooLarge);
        }
        loop {
            match self.try_update(g, &key, &val, &mut visible) {
                Ok(x) => return Ok(x),
                Err(OpCode::Again) => {
                    self.store
                        .opt
                        .observer
                        .counter(CounterMetric::TreeRetryAgain, 1);
                    self.store
                        .opt
                        .observer
                        .counter(CounterMetric::TxnRetryAgain, 1);
                    g.flush();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    // background abort-clean uses page-wide compaction so non-head aborted versions are also purged
    // this keeps cleanup crash-safe even when newer committed versions already cover the same key
    pub(crate) fn remove_aborted(&self, g: &Guard, raw: &[u8]) -> Result<bool, OpCode> {
        let page = self.find_leaf(g, raw)?;
        let Some(_lk) = page.try_lock() else {
            return Err(OpCode::Again);
        };
        if self.bucket.table.get(page.pid()) != page.swip() {
            return Err(OpCode::Again);
        }
        self.rewrite_node(g, page)
    }

    // foreground retry uses head-gated cleanup to avoid no-op page compaction under concurrent updates
    // if the aborted version is no longer the key head, gc path will handle full cleanup later
    pub(crate) fn remove_aborted_head(
        &self,
        g: &Guard,
        raw: &[u8],
        aborted_txid: u64,
    ) -> Result<bool, OpCode> {
        let page = self.find_leaf(g, raw)?;
        let Some(_lk) = page.try_lock() else {
            return Err(OpCode::Again);
        };
        if self.bucket.table.get(page.pid()) != page.swip() {
            return Err(OpCode::Again);
        }

        let Some((head_ver, _, _)) = page.find_latest(&Key::new(raw, Ver::new(u64::MAX, u32::MAX)))
        else {
            return Ok(false);
        };
        if head_ver.txid != aborted_txid {
            return Ok(false);
        }
        if self.store.context.get_aborted(aborted_txid) != Some(TxOutcome::Aborted) {
            return Ok(false);
        }

        self.rewrite_node(g, page)
    }

    #[inline]
    fn rewrite_node(&self, g: &Guard, page: Page) -> Result<bool, OpCode> {
        let mut build = self.begin_build();
        let (new_node, junk, removed) =
            page.remove_aborted(&mut build, self.txid(), self.store.context);
        if !removed {
            return Ok(false);
        }
        let mut publish = build.into_publish(g);
        publish.replace(page, new_node, junk);
        publish.commit();
        Ok(true)
    }

    /// return the latest key-val pair, by using Ikey::raw(), thanks to MVCC, the first match one is
    /// the latest one
    pub fn get<'b>(&'b self, g: &Guard, key: Key<'b>) -> Result<(Key<'b>, ValRef), OpCode> {
        let page = self.find_leaf(g, key.raw())?;

        let Some((ver, v, b)) = page.find_latest(&key) else {
            return Err(OpCode::NotFound);
        };

        Ok((Key::new(key.raw, ver), ValRef::new(v, b)))
    }

    pub fn range<'a, K, R, F>(&'a self, range: R, visible: F) -> Iter<'a>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
        F: FnMut(&Context, u64, u8) -> bool + 'a,
    {
        let cached_key = Handle::new(Vec::new());
        let lo = match range.start_bound() {
            Bound::Included(b) => Bound::Included(b.as_ref().to_vec()),
            Bound::Excluded(b) => Bound::Excluded(b.as_ref().to_vec()),
            Bound::Unbounded => Bound::Included(vec![]),
        };
        let hi = match range.end_bound() {
            Bound::Included(e) => Bound::Included(e.as_ref().to_vec()),
            Bound::Excluded(e) => Bound::Excluded(e.as_ref().to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Iter {
            tree: self,
            cached_key,
            lo,
            hi,
            iter: None,
            rev_iter: None,
            cache: None,
            iter_bound: None,
            checker: Box::new(visible),
            filter: Filter { has_last: false },
            guard: crossbeam_epoch::pin(),
        }
    }

    fn traverse_hist<L, F>(
        &self,
        l: &L,
        start_ts: u64,
        hist: HistRef,
        visible: &mut F,
    ) -> Result<ValRef, OpCode>
    where
        L: ILoader,
        F: FnMut(u64, u8) -> bool,
    {
        let mut addr = hist.page_addr;
        let mut pos = hist.slot as usize;
        let mut remaining = hist.count as usize;
        let mut first_segment = true;
        let target = Ver::new(start_ts, NULL_CMD);

        while addr != NULL_PID && remaining > 0 {
            let ptr = l.load(addr)?.as_base();
            let sst = ptr.sst::<Ver>();
            let elems = sst.header().elems as usize;
            if pos >= elems {
                addr = ptr.box_header().link;
                pos = 0;
                continue;
            }

            // history is key-local contiguous region, so only binary-search the active subrange
            // on the first page and then continue linearly across the bounded region
            let mut page_end = elems.min(pos.saturating_add(remaining));
            if first_segment {
                first_segment = false;
                let begin = Self::lower_bound_hist_subrange(&sst, pos, page_end, &target);
                let skipped = begin - pos;
                pos = begin;
                remaining = remaining.saturating_sub(skipped);
                page_end = elems.min(pos.saturating_add(remaining));
            }

            while pos < page_end && remaining > 0 {
                let (k, v) = sst.kv_at::<Val>(pos);
                if visible(k.txid, v.group_id()) {
                    if v.is_tombstone() {
                        return Err(OpCode::NotFound);
                    }
                    let (v, r) = v.get_record(l, true);
                    return Ok(ValRef::new(v, r.map_or(ptr.as_box(), |x| x)));
                }
                pos += 1;
                remaining -= 1;
            }

            if remaining == 0 {
                break;
            }
            addr = ptr.box_header().link;
            pos = 0;
        }
        Err(OpCode::NotFound)
    }

    fn lower_bound_hist_subrange(
        sst: &crate::types::sst::Sst<Ver>,
        mut lo: usize,
        mut hi: usize,
        target: &Ver,
    ) -> usize {
        while lo < hi {
            let mid = lo + ((hi - lo) >> 1);
            let key = sst.key_at(mid);
            if key.cmp(target).is_lt() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    pub fn traverse<F>(&self, g: &Guard, key: Key, mut visible: F) -> Result<ValRef, OpCode>
    where
        F: FnMut(u64, u8) -> bool,
    {
        let page = self.find_leaf(g, key.raw)?;

        let mut result = None;
        let search_key = Key::new(key.raw, Ver::new(u64::MAX, u32::MAX));
        page.visit_versions(
            search_key,
            |x, y| {
                let k = Key::decode_from(x.key());
                match k.raw.cmp(y.raw) {
                    Equal => y.txid.cmp(&k.txid), // compare txid is enough
                    o => o,
                }
            },
            |x| {
                let k = Key::decode_from(x.key());
                if k.raw.cmp(key.raw).is_ne() {
                    return true;
                }
                let val = x.val();
                if visible(k.txid, val.group_id()) {
                    if val.is_tombstone() {
                        result = Some(Err(OpCode::NotFound));
                        return true;
                    }
                    let (r, v) = val.get_record(&page.loader, true);
                    result = Some(Ok(ValRef::new(r, v.unwrap_or_else(|| x.as_box()))));
                    return true;
                }
                false
            },
        );

        if let Some(res) = result {
            return res;
        }

        // Key::raw is unique in sst
        let (k, val) = page.search_sst(&key).ok_or(OpCode::NotFound)?;
        if visible(k.txid, val.group_id()) {
            if val.is_tombstone() {
                return Err(OpCode::NotFound);
            }
            let (record, r) = val.get_record(&page.loader, true);
            return Ok(ValRef::new(record, r.unwrap_or_else(|| page.base_box())));
        }
        if let Some(hist) = val.get_hist() {
            return self.traverse_hist(&page.loader, key.txid, hist, &mut visible);
        }
        Err(OpCode::NotFound)
    }
}

/// An iterator over key-value pairs in a bucket.
pub struct Iter<'a> {
    tree: &'a Tree,
    cached_key: Handle<Vec<u8>>,
    lo: Bound<Vec<u8>>,
    hi: Bound<Vec<u8>>,
    iter: Option<RawLeafIter<'a, Loader>>,
    rev_iter: Option<RawLeafRevIter<'a, Loader>>,
    cache: Option<Box<Node>>,
    iter_bound: Option<Box<Bound<Vec<u8>>>>,
    checker: Box<dyn FnMut(&Context, u64, u8) -> bool + 'a>,
    filter: Filter,
    guard: Guard,
}

impl Drop for Iter<'_> {
    fn drop(&mut self) {
        // release iterator state before reclaiming shared key scratch
        self.iter.take();
        self.rev_iter.take();
        self.cache.take();
        self.iter_bound.take();
        self.cached_key.reclaim();
    }
}

impl Iter<'_> {
    fn low_key(&self) -> &[u8] {
        match self.lo {
            Bound::Unbounded => &[],
            Bound::Excluded(ref x) | Bound::Included(ref x) => x,
        }
    }

    fn high_key(&self) -> Option<&[u8]> {
        match self.hi {
            Bound::Unbounded => None,
            Bound::Excluded(ref x) | Bound::Included(ref x) => Some(x),
        }
    }

    fn collapsed(&self) -> bool {
        match (&self.lo, &self.hi) {
            (Bound::Included(b), Bound::Included(e))
            | (Bound::Excluded(b), Bound::Excluded(e))
            | (Bound::Included(b), Bound::Excluded(e))
            | (Bound::Excluded(b), Bound::Included(e)) => b > e,
            _ => false,
        }
    }

    fn find_leaf_for_next_back(&self) -> Result<Page, OpCode> {
        if let Some(k) = self.high_key() {
            let node = self.tree.find_leaf(&self.guard, k)?;
            if matches!(self.hi, Bound::Excluded(_)) && node.lo() >= k {
                return self
                    .tree
                    .find_prev_leaf(&self.guard, k)?
                    .ok_or(OpCode::NotFound);
            }
            return Ok(node);
        }

        let mut node = self.tree.find_leaf(&self.guard, self.low_key())?;
        loop {
            let rpid = node.header().right_sibling;
            if rpid == NULL_PID {
                return Ok(node);
            }
            let Some(next) = self.tree.load_node(&self.guard, rpid)? else {
                return Err(OpCode::Again);
            };
            node = next;
        }
    }

    fn get_next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.rev_iter.take();

        'retry: while !self.collapsed() {
            if self.iter.is_none() {
                let node = match self.tree.find_leaf(&self.guard, self.low_key()) {
                    Ok(node) => node,
                    Err(OpCode::Again) => {
                        self.guard.flush();
                        continue;
                    }
                    Err(OpCode::NotFound) => return None,
                    Err(e) => panic!("iter find_leaf failed: {e:?}"),
                };
                let next_node = node.ref_node();
                let next_bound = self.lo.clone();

                if let Some(cache) = self.cache.as_mut() {
                    **cache = next_node;
                } else {
                    self.cache = Some(Box::new(next_node));
                }

                if let Some(bound) = self.iter_bound.as_mut() {
                    **bound = next_bound;
                } else {
                    self.iter_bound = Some(Box::new(next_bound));
                }

                let cache = self.cache.as_ref().expect("must valid");
                let bound = self.iter_bound.as_ref().expect("must valid");
                self.iter = Some(unsafe {
                    std::mem::transmute::<RawLeafIter<'_, Loader>, RawLeafIter<'_, Loader>>(
                        cache.successor(bound.as_ref(), self.cached_key),
                    )
                });
            }

            let r = loop {
                let next = {
                    let iter = self.iter.as_mut().expect("must valid");
                    iter.try_next()
                };
                match next {
                    Ok(Some(item)) => {
                        let ok = match &self.lo {
                            Bound::Unbounded => true,
                            Bound::Included(b) => item.cmp_key(b.as_slice()).is_ge(),
                            Bound::Excluded(b) => item.cmp_key(b.as_slice()).is_gt(),
                        };
                        if ok
                            && (self.checker)(
                                &self.tree.store.context,
                                item.txid(),
                                item.group_id(),
                            )
                            && self.filter.check(&item)
                        {
                            break Some(item);
                        }
                    }
                    Ok(None) => break None,
                    Err(OpCode::Again | OpCode::NotFound) => {
                        self.iter.take();
                        continue 'retry;
                    }
                    Err(e) => panic!("iter load failed: {e:?}"),
                }
            };

            if let Some(item) = r {
                // reuse existing lower-bound buffer to avoid realloc per item
                let key = item.key();
                match &mut self.lo {
                    Bound::Included(v) | Bound::Excluded(v) => {
                        v.clear();
                        v.extend_from_slice(key);
                        // keep the variant as Excluded for next search step
                        self.lo = Bound::Excluded(std::mem::take(v));
                    }
                    Bound::Unbounded => {
                        self.lo = Bound::Excluded(key.to_vec());
                    }
                }

                match self.hi {
                    Bound::Unbounded => return Some(item),
                    Bound::Included(ref h) if item.cmp_key(h.as_slice()).is_le() => {
                        return Some(item);
                    }
                    Bound::Excluded(ref h) if item.cmp_key(h.as_slice()).is_lt() => {
                        return Some(item);
                    }
                    _ => return None,
                }
            } else {
                self.iter.take();
                let node = self.cache.as_ref().expect("must valid");
                if let Some(hi) = node.hi() {
                    self.lo = Bound::Included(hi.to_vec());
                    continue;
                }
                break;
            }
        }

        None
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = IterItem<'a, Loader>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next()
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.take();

        'retry: while !self.collapsed() {
            if self.rev_iter.is_none() {
                let node = match self.find_leaf_for_next_back() {
                    Ok(node) => node,
                    Err(OpCode::Again) => {
                        self.guard.flush();
                        continue;
                    }
                    Err(OpCode::NotFound) => return None,
                    Err(e) => panic!("iter find_leaf failed: {e:?}"),
                };
                let next_node = node.ref_node();
                if let Some(cache) = self.cache.as_mut() {
                    **cache = next_node;
                } else {
                    self.cache = Some(Box::new(next_node));
                }
                self.rev_iter = Some(unsafe {
                    std::mem::transmute::<RawLeafRevIter<'_, Loader>, RawLeafRevIter<'_, Loader>>(
                        self.cache.as_ref().expect("must valid").predecessor(
                            &self.lo,
                            &self.hi,
                            self.cached_key,
                        ),
                    )
                });
            }

            let res = loop {
                let next = {
                    let iter = self.rev_iter.as_mut().expect("must valid");
                    iter.try_next_back()
                };
                match next {
                    Ok(Some(item)) => {
                        let lo_ok = match &self.lo {
                            Bound::Unbounded => true,
                            Bound::Included(b) => item.cmp_key(b.as_slice()).is_ge(),
                            Bound::Excluded(b) => item.cmp_key(b.as_slice()).is_gt(),
                        };
                        let hi_ok = match &self.hi {
                            Bound::Unbounded => true,
                            Bound::Included(h) => item.cmp_key(h.as_slice()).is_le(),
                            Bound::Excluded(h) => item.cmp_key(h.as_slice()).is_lt(),
                        };
                        if lo_ok
                            && hi_ok
                            && (self.checker)(
                                &self.tree.store.context,
                                item.txid(),
                                item.group_id(),
                            )
                            && self.filter.check(&item)
                        {
                            break Some(item);
                        }
                    }
                    Ok(None) => break None,
                    Err(OpCode::Again | OpCode::NotFound) => {
                        self.rev_iter.take();
                        continue 'retry;
                    }
                    Err(e) => panic!("iter load failed: {e:?}"),
                }
            };

            if let Some(item) = res {
                let key = item.key();
                match &mut self.hi {
                    Bound::Included(v) | Bound::Excluded(v) => {
                        v.clear();
                        v.extend_from_slice(key);
                        self.hi = Bound::Excluded(std::mem::take(v));
                    }
                    Bound::Unbounded => {
                        self.hi = Bound::Excluded(key.to_vec());
                    }
                }
                return Some(item);
            }

            self.rev_iter.take();
            let lo = self.cache.as_ref().expect("must valid").lo();
            if lo.is_empty() {
                return None;
            }
            self.hi = Bound::Excluded(lo.to_vec());
        }

        None
    }
}

struct Filter {
    has_last: bool,
}

impl Filter {
    fn check<L: ILoader>(&mut self, item: &IterItem<L>) -> bool {
        // key() returns cached assembled key from previous accepted item
        if self.has_last && item.cmp_key(item.key()).is_eq() {
            return false;
        }
        let _ = item.assembled_key();
        self.has_last = true;
        !item.is_tombstone()
    }
}

#[cfg(test)]
mod test {
    use crate::{Mace, Options, RandomPath};
    use std::thread;

    #[test]
    fn concurrent_page_hit() {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.split_elems = 256;
        opt.tmp_store = true;
        let mace = Mace::new(opt.validate().unwrap()).unwrap();
        let db = mace.new_bucket("default").unwrap();

        let num_readers = 4;
        let num_iterations = 1000;

        thread::scope(|s| {
            for _ in 0..num_readers {
                let db = db.clone();
                s.spawn(move || {
                    for _ in 0..num_iterations {
                        let view = db.view().unwrap();
                        let mut count = 0;
                        for _ in view.seek("key") {
                            count += 1;
                        }
                        assert!(count >= 0);
                    }
                });
            }

            s.spawn(|| {
                for i in 0..num_iterations {
                    let kv = db.begin().unwrap();
                    let key = format!("key_{:05}", i);
                    kv.put(&key, &key).unwrap();
                    kv.commit().unwrap();
                }
            });
        });
    }
}
