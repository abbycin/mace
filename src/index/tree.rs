use crate::cc::{SnapshotStamp, is_visible_to};
use crate::map::buffer::BucketContext;
use crate::map::publish::AllocGuard;
use crate::map::{Loader, Node, Page};
use crate::types::data::{HistRef, IterItem, Record, Val};
use crate::types::node::{Junk, MergeOp, RawLeafIter, RawLeafRevIter};
use crate::types::refbox::DeltaView;
use crate::types::sst::Sst;
use crate::types::traits::{IAsBoxRef, IBoxHeader, IDecode, IHeader, ILoader};
use crate::utils::data::Position;
use crate::utils::observe::{
    CounterMetric, EventKind, HistogramMetric, LATENCY_SAMPLE_SHIFT, ObserveEvent, observe_elapsed,
    sampled_instant,
};
use crate::utils::{Handle, MutRef, NULL_ADDR, OpCode};
use crate::{Options, must_exist};
use crate::{
    Store,
    types::{
        data::{Index, IntlKey, Key, Ver},
        refbox::BoxRef,
        traits::{ICodec, IKey},
    },
    utils::{NULL_CMD, NULL_PID},
};
use crate::{hot_true, must_true};
use crossbeam_epoch::Guard;
use std::cmp::{Ordering, Ordering::Equal};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::atomic::Ordering::Acquire;

/// A reference to a value in the storage engine.
///
/// Page-backed values stay zero-copy; merge-folded values own their bytes because the
/// logical value only exists on the reader side and is never written back.
///
/// Flat 32-byte layout: [gid u8][flags u8][pad 6][ptr u8][len usize][owner Option<BoxRef>].
/// The folded bit marks an owned box at ptr/len; page values borrow the record's bytes
/// and hold the owning box in `_owner`.
pub struct ValRef {
    gid: u8,
    flags: u8,
    ptr: *const u8,
    len: usize,
    _owner: Option<BoxRef>,
}

// safety: ptr references immutable bytes kept alive by either _owner or the folded allocation
unsafe impl Send for ValRef {}
// safety: shared access exposes only immutable bytes and ownership remains inside ValRef
unsafe impl Sync for ValRef {}

const VAL_FOLDED: u8 = 1;
const VAL_MERGE: u8 = 2;

impl ValRef {
    #[inline(always)]
    pub(crate) fn new(raw: Record, owner: BoxRef) -> Self {
        Self {
            gid: raw.group_id(),
            flags: if raw.is_merge() { VAL_MERGE } else { 0 },
            ptr: raw.data().as_ptr(),
            len: raw.data().len(),
            _owner: Some(owner),
        }
    }

    pub(crate) fn folded(gid: u8, data: Vec<u8>) -> Self {
        let boxed: Box<[u8]> = data.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;
        Self {
            gid,
            flags: VAL_FOLDED,
            ptr,
            len,
            _owner: None,
        }
    }

    #[inline(always)]
    fn is_folded(&self) -> bool {
        self.flags & VAL_FOLDED != 0
    }

    /// Returns the data as a byte slice.
    #[inline(always)]
    pub fn slice(&self) -> &[u8] {
        // SAFETY: page values borrow a live record; folded values own the box
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Converts the reference into a owned Vec<u8>.
    pub fn to_vec(self) -> Vec<u8> {
        if self.is_folded() {
            let ptr = self.ptr;
            let len = self.len;
            std::mem::forget(self);
            let raw = std::ptr::slice_from_raw_parts_mut(ptr as *mut u8, len);
            // SAFETY: folded values exclusively own this allocation and `forget`
            // prevents ValRef::drop from freeing it before conversion
            return unsafe { Box::from_raw(raw).into_vec() };
        }
        self.slice().to_vec()
    }

    #[inline(always)]
    pub fn group_id(&self) -> u8 {
        self.gid
    }

    #[cfg(test)]
    pub(crate) fn is_merge(&self) -> bool {
        self.flags & VAL_MERGE != 0
    }
}

impl Drop for ValRef {
    fn drop(&mut self) {
        // folded values own the box; page ownership lives in `_owner`, which drops
        // on its own after this
        if self.is_folded() {
            let raw = std::ptr::slice_from_raw_parts_mut(self.ptr as *mut u8, self.len);
            // SAFETY: exactly one ValRef owns the folded box
            unsafe { drop(Box::from_raw(raw)) };
        }
    }
}

impl Clone for ValRef {
    fn clone(&self) -> Self {
        if self.is_folded() {
            // deep copy the folded box; both copies own an independent allocation
            Self::folded(self.gid, self.slice().to_vec())
        } else {
            Self {
                gid: self.gid,
                flags: self.flags,
                ptr: self.ptr,
                len: self.len,
                _owner: self._owner.clone(),
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct LatestValMeta {
    pub(crate) ver: Ver,
    pub(crate) group_id: u8,
    pub(crate) is_del: bool,
    /// head record is a merge operand
    pub(crate) is_merge: bool,
}

/// collects visible merge operands while walking a key's versions newest -> oldest,
/// then folds them against the first visible base/tombstone (or absence)
pub(crate) struct FoldCollector {
    /// (logical group id, operand bytes), newest first
    operands: Vec<(u8, Vec<u8>)>,
}

impl FoldCollector {
    pub(crate) fn new() -> Self {
        Self {
            operands: Vec::new(),
        }
    }

    /// collects one visible merge operand
    pub(crate) fn push(&mut self, gid: u8, operand: &[u8]) {
        self.operands.push((gid, operand.to_vec()));
    }

    /// resolves the final reader-side value.
    ///
    /// - combine operands left-associated, apply once onto the base;
    ///   `apply -> None` is a logical delete and surfaces as `NotFound`
    pub(crate) fn finish(
        &mut self,
        operator: &dyn crate::MergeOperator,
        base: Option<(Record, BoxRef)>,
        key: &[u8],
    ) -> Result<ValRef, OpCode> {
        let mut iter = std::mem::take(&mut self.operands).into_iter();
        let (first_gid, first) = iter.next().expect("operands non-empty");
        let mut acc = first;
        for (_, next) in iter {
            acc = operator.combine_operands(key, &acc, &next);
            if acc.is_empty() {
                return Err(OpCode::MergeContractViolation);
            }
            // a combine step already past the persisted limit is a contract
            // violation regardless of the final apply result
            if size_of::<u8>() + acc.len() > Options::MAX_KV_SIZE {
                return Err(OpCode::MergeContractViolation);
            }
        }
        let applied = match &base {
            None => operator.apply(key, None, &acc),
            Some((r, _)) => operator.apply(key, Some(r.data()), &acc),
        };
        match applied {
            None => Err(OpCode::NotFound),
            Some(v) if v.is_empty() => Err(OpCode::MergeContractViolation),
            Some(v) if size_of::<u8>() + v.len() > Options::MAX_KV_SIZE => {
                Err(OpCode::MergeContractViolation)
            }
            Some(v) => {
                let gid = match &base {
                    Some((r, _)) => r.group_id(),
                    None => first_gid,
                };
                Ok(ValRef::folded(gid, v))
            }
        }
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
        // recovery roots must not advance a frontier beyond materialized WAL
        let lsn = if self.store.context.recovering() {
            Position::MIN
        } else {
            self.store.context.group(0).logging.lock().current_pos()
        };
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
        AllocGuard::new(&self.bucket)
    }

    pub(crate) fn load_node(&self, g: &Guard, pid: u64) -> Result<Option<Page>, OpCode> {
        loop {
            if let Some(p) = self.bucket.load(pid) {
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

        must_true!(child_pid != NULL_PID, "child pid {}", child_pid);
        must_true!(parent_ptr.is_intl());
        let child_index = must_exist!(
            parent_ptr
                .intl_iter()
                .position(|(_, idx)| idx.pid == child_pid)
        );
        // the "can_merge_child" check is somewhat failed
        must_true!(child_index != 0, "we can't handle merge the leftmost node");

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
        let mut cursor_pid =
            must_exist!(parent_ptr.intl_iter().nth(merge_index).map(|(_, x)| x.pid));
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
                cursor_pid =
                    must_exist!(parent_ptr.intl_iter().nth(merge_index).map(|(_, x)| x.pid));
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
                    cursor_ptr.merge_node(&mut build, &child_ptr, safe_txid, self.store.context);
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

        // 4. hide child from the in-memory page table before reclaiming it
        // checkpoint will later make this NULL mapping durable and only then recycle child_pid into
        // the free list. otherwise readers/gc can still load child_pid and observe a reclaimed page.
        must_true!(eq child_ptr.box_header().pid, child_pid);
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
        must_true!(eq parent_ptr.header().merging_child, child_pid);

        let mut build = self.begin_build();
        let (new_ptr, junks) =
            parent_ptr.process_merge(&mut build, MergeOp::Merged, safe_txid, self.store.context);
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
        let (new_node, junks) = page.process_merge(
            &mut build,
            MergeOp::MarkChild,
            safe_txid,
            self.store.context,
        );
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
        let mut block = |k: &[u8]| {
            self.bucket.merge_blocked_keys.insert(k.to_vec());
        };
        let compact_op = self.bucket.merge_operator();
        let (mut lnode, junk) = root.compact(
            &mut build,
            safe_txid,
            compact_op,
            &mut block,
            Some(self.store.context),
        );
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
        must_true!(eq n.box_header().pid, self.root_index.pid);
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
        let recovering = self.store.context.recovering();
        loop {
            match self.try_find_leaf(g, recovering, k) {
                Err(OpCode::Again) => {
                    g.flush();
                    continue;
                }
                Err(e) => unreachable!("invalid opcode {:?}", e),
                o => return o,
            }
        }
    }

    fn try_find_leaf(&self, g: &Guard, recovering: bool, key: &[u8]) -> Result<Page, OpCode> {
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
            // recovery defers structural maintenance: redo only appends records and never
            // splits or consolidates, so no operator interpretation can run mid-recovery
            if !recovering && node_ptr.should_split(self.bucket.opt.split_elems) {
                // split precondition: a leaf must carry an empty delta chain before it
                // splits; consolidate first and re-enter the loop to split cleanly
                if !node_ptr.is_intl() && node_ptr.delta_len() > 0 {
                    self.try_compact(g, node_ptr);
                    return Err(OpCode::Again);
                }
                self.split_node(node_ptr, parent_opt, g)?;
                return Err(OpCode::Again);
            }

            let hi = node_ptr.hi();

            // another thread may already split this node, detect by key >= hi and follow sibling
            let is_splitting = if let Some(hi) = hi { key >= hi } else { false };

            if is_splitting {
                // search from right sibling
                let rpid = node_ptr.header().right_sibling;
                must_true!(ne rpid, NULL_PID);

                if parent_opt.is_none() && lo.is_empty() {
                    // root may be in partial split state:
                    // current page is lhs and rhs is already mapped but new root is not installed yet.
                    // complete root installation cooperatively; recovery only follows the existing
                    // sibling routing below and defers completion to post-recovery paths
                    must_true!(eq cursor, self.root_index.pid);
                    if !recovering {
                        let safe_txid = self.txid();
                        let _ = self.split_root(g, node_ptr, rpid, must_exist!(hi), safe_txid);
                        return Err(OpCode::Again);
                    }
                } else if !recovering && unsplit_parent_opt.is_none() && parent_opt.is_some() {
                    unsplit_parent_opt = parent_opt;
                }
                cursor = rpid;

                continue;
            }

            // complete pending parent separator installation cooperatively;
            // recovery skips the install: descent lands on lhs and is_splitting routes
            // to rhs via the sibling chain until a post-recovery pass installs it
            if !recovering && let Some(unsplit) = unsplit_parent_opt.take() {
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
                hot_true!(eq node_ptr.delta_len(), 0);
                let (is_leftmost, pid) = node_ptr.child_index(key);
                leftmost = is_leftmost;
                parent_opt = Some(node_ptr);
                cursor = pid;
            } else {
                // recovery keeps delta chains intact: consolidation may fold merge
                // operands and must never run while redo is still appending records
                if !recovering
                    && node_ptr.delta_len() >= self.bucket.opt.consolidate_threshold as usize
                {
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

            let (pos, pid) = must_exist!(node.sst::<IntlKey>().floor_pid_by_raw(key));
            path.push((node.pid(), node.swip(), pos));
            cursor = pid;
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

            if child_pos >= parent.header().elems as usize {
                return Err(OpCode::Again);
            }
            let mut pid = parent.sst::<IntlKey>().pid_at(child_pos - 1);

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
                pid = node.sst::<IntlKey>().pid_at(elems - 1);
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
        let mut block = |k: &[u8]| {
            self.bucket.merge_blocked_keys.insert(k.to_vec());
        };
        let compact_op = self.bucket.merge_operator();
        let (new_node, junk) = page.compact(
            &mut build,
            self.txid(),
            compact_op,
            &mut block,
            Some(self.store.context),
        );
        let mut publish = build.into_publish(g);
        publish.replace(page, new_node, junk);
        publish.commit();
        self.store
            .opt
            .observer
            .counter(CounterMetric::TreeNodeConsolidate, 1);
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
            let (new_parent, j) = parent.process_merge(
                &mut build,
                MergeOp::MarkParent(pid),
                self.txid(),
                self.store.context,
            );
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
                #[cfg(feature = "extra_check")]
                crate::testing::fire_tree_update_sync_point(
                    crate::testing::TreeUpdateSyncPoint::AfterTreeAgainBeforeLatestMetaRecheck,
                    pid,
                );
                return Err(OpCode::Again);
            }

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

    fn try_put(
        &self,
        g: &Guard,
        key: &Key,
        val: &Record,
        group: u8,
        pos: Position,
    ) -> Result<(), OpCode> {
        let page = self.find_leaf(g, key.raw())?;

        self.link(g, page, key, val, |_, _| Ok((group, pos)))?;
        Ok(())
    }

    /// for non-txn use, such as registry and recovery
    /// inserts a recovered WAL record while preserving its logical frontier
    pub(crate) fn put(
        &self,
        g: &Guard,
        key: Key,
        val: Record,
        group: u8,
        pos: Position,
    ) -> Result<(), OpCode> {
        loop {
            match self.try_put(g, &key, &val, group, pos) {
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
        #[cfg(feature = "extra_check")]
        crate::testing::fire_tree_update_sync_point(
            crate::testing::TreeUpdateSyncPoint::AfterFindLeafBeforeLink,
            page.pid(),
        );
        let mut r = None;

        self.link(g, page, key, val, |pg, k| {
            let tmp = pg.find_latest_meta(k);
            r = tmp.map(|meta| LatestValMeta {
                ver: meta.ver,
                group_id: meta.group_id,
                is_del: meta.is_del,
                is_merge: meta.is_merge,
            });
            #[cfg(feature = "extra_check")]
            crate::testing::fire_tree_update_sync_point(
                crate::testing::TreeUpdateSyncPoint::AfterLatestMetaCheckBeforeDeltaInsert,
                pg.pid(),
            );
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

        let Some((head_ver, head_val, _)) =
            page.find_latest(&Key::new(raw, Ver::new(u64::MAX, u32::MAX)))
        else {
            return Ok(false);
        };
        if head_ver.txid != aborted_txid {
            return Ok(false);
        }
        if !self
            .store
            .context
            .group(head_val.group_id() as usize)
            .is_retained_abort(aborted_txid)
        {
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

    pub fn range<'a, K, R>(&'a self, range: R, snapshot: SnapshotStamp) -> Iter<'a>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
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
            snapshot,
            has_last: false,
            failed: false,
            merge_mode: self.bucket.has_merge.load(Acquire),
            last_prefix_ptr: 0,
            last_prefix_len: 0,
            page_epoch: 0,
            last_page_epoch: 0,
            bound_prefix_ptr: 0,
            bound_prefix_len: 0,
            bound_page_epoch: 0,
            bound_from_item: false,
            guard: crossbeam_epoch::pin(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn traverse_hist<L, F>(
        &self,
        l: &L,
        start_ts: u64,
        hist: HistRef,
        key: &[u8],
        visible: &mut F,
        mut col: FoldCollector,
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
            let page = l.load_sibling(addr);
            let ptr = page.view().as_base();
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
                    // the envelope kind decides: tombstone is an absence barrier,
                    // merge is an operand to collect, anything else is a base value
                    if v.is_tombstone() {
                        return self.finish_fold(&mut col, None, key);
                    }
                    let (record, owner) = v.get_record(l);
                    if v.is_merge() {
                        // collect and keep walking older versions inside the region
                        self.push_fold_operand(&mut col, record.group_id(), record.data())?;
                    } else {
                        return self.finish_fold(
                            &mut col,
                            Some((record, owner.unwrap_or(page))),
                            key,
                        );
                    }
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
        // history exhausted without a base/tombstone barrier
        self.finish_fold(&mut col, None, key)
    }

    /// folds a collected operand chain against an optional base record; a folded
    /// value past the persisted limit reports the contract violation and observes
    /// it once per key
    fn finish_fold(
        &self,
        col: &mut FoldCollector,
        base: Option<(Record, BoxRef)>,
        key: &[u8],
    ) -> Result<ValRef, OpCode> {
        if col.operands.is_empty() {
            return match base {
                None => Err(OpCode::NotFound),
                Some((record, owner)) => Ok(ValRef::new(record, owner)),
            };
        }
        let operator = self
            .bucket
            .merge_operator()
            .expect("operands are collected only with a merge operator");
        let res = col.finish(operator, base, key);
        if matches!(&res, Err(OpCode::MergeContractViolation)) {
            self.block_merge_key(key);
        }
        res
    }

    #[inline(always)]
    fn push_fold_operand(
        &self,
        col: &mut FoldCollector,
        gid: u8,
        operand: &[u8],
    ) -> Result<(), OpCode> {
        if col.operands.is_empty() && self.bucket.merge_operator().is_none() {
            return Err(OpCode::Invalid);
        }
        col.push(gid, operand);
        Ok(())
    }

    /// records an exact key whose merge chain produced a contract violation:
    /// subsequent `TxnKV::merge` on it is rejected until a committed
    /// del/reset_merge clears it. the first observation of each key emits a
    /// fixed-cardinality observer counter/event and a bounded error log with
    /// bucket/key summary (never the full value or the operand list)
    fn block_merge_key(&self, key: &[u8]) {
        let first = self.bucket.merge_blocked_keys.insert(key.to_vec());
        if !first {
            return;
        }
        self.store
            .opt
            .observer
            .counter(CounterMetric::MergeContractViolation, 1);
        self.store.opt.observer.event(ObserveEvent {
            kind: EventKind::MergeContractViolation,
            bucket_id: self.bucket.bucket_id,
            txid: 0,
            file_id: 0,
            value: 0,
        });
        if let Ok(s) = std::str::from_utf8(key) {
            log::error!(
                "merge contract violation: bucket={} key={:?}",
                self.bucket.bucket_id,
                s
            );
        } else {
            log::error!(
                "merge contract violation: bucket={} key={:?}",
                self.bucket.bucket_id,
                key
            );
        }
    }

    fn lower_bound_hist_subrange(
        sst: &Sst<Ver>,
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

    fn traverse_sst<F>(
        &self,
        page: &Page,
        key: Key,
        visible: &mut F,
        sst: Option<(Ver, Val<'_>)>,
    ) -> Result<ValRef, OpCode>
    where
        F: FnMut(u64, u8) -> bool,
    {
        let (ver, val) = sst.ok_or(OpCode::NotFound)?;
        if !visible(ver.txid, val.group_id()) {
            return match val.get_hist() {
                Some(hist) => self.traverse_hist(
                    &page.loader,
                    key.txid,
                    hist,
                    key.raw,
                    visible,
                    FoldCollector::new(),
                ),
                None => Err(OpCode::NotFound),
            };
        }
        if val.is_tombstone() {
            return Err(OpCode::NotFound);
        }

        let (record, owner) = val.get_record(&page.loader);
        if !val.is_merge() {
            return Ok(ValRef::new(
                record,
                owner.unwrap_or_else(|| page.base_box()),
            ));
        }

        let mut col = FoldCollector::new();
        self.push_fold_operand(&mut col, record.group_id(), record.data())?;
        match val.get_hist() {
            Some(hist) => self.traverse_hist(&page.loader, key.txid, hist, key.raw, visible, col),
            None => self.finish_fold(&mut col, None, key.raw),
        }
    }

    /// resolves a key using a leaf page that has already been located
    fn traverse_page<F>(&self, page: Page, key: Key, mut visible: F) -> Result<ValRef, OpCode>
    where
        F: FnMut(u64, u8) -> bool,
    {
        if let Some(sst) = page.search_sst_if_delta_empty(&key) {
            return self.traverse_sst(&page, key, &mut visible, sst);
        }
        let mut col = FoldCollector::new();
        let mut decided: Option<Result<ValRef, OpCode>> = None;
        // the fast path is valid only for the newest visible version: no operand
        // can sit above it. later visible versions may have collected operands,
        // so they must go through the fold
        let mut first_visible = true;
        let search_key = Key::new(key.raw, Ver::new(u64::MAX, u32::MAX));
        page.visit_versions(
            search_key,
            |x, y| {
                let k = Key::decode_from(x.key());
                match k.raw.cmp(y.raw) {
                    Equal => k.ver.cmp(&y.ver),
                    o => o,
                }
            },
            |dv| {
                let k = Key::decode_from(dv.key());
                if k.raw.cmp(key.raw).is_ne() {
                    // walked past this key's version block
                    return true;
                }
                let val = dv.val();
                if !visible(k.txid, val.group_id()) {
                    // skip versions invisible to this snapshot
                    return false;
                }
                if first_visible {
                    first_visible = false;
                    // newest visible version: no operand sits above it, so a
                    // tombstone is absence and a plain is the value itself
                    if val.is_tombstone() {
                        decided = Some(Err(OpCode::NotFound));
                        return true;
                    }
                    let (record, owner) = val.get_record(&page.loader);
                    if val.is_merge() {
                        if let Err(e) =
                            self.push_fold_operand(&mut col, record.group_id(), record.data())
                        {
                            decided = Some(Err(e));
                            return true;
                        }
                        // keep walking older versions: the base barrier decides
                        return false;
                    }
                    decided = Some(Ok(ValRef::new(
                        record,
                        owner.unwrap_or_else(|| dv.as_box()),
                    )));
                    return true;
                }
                // older visible version: collected operands fold onto the
                // barrier (or absence for a tombstone)
                if val.is_tombstone() {
                    decided = Some(self.finish_fold(&mut col, None, key.raw));
                    return true;
                }
                let (record, owner) = val.get_record(&page.loader);
                if val.is_merge() {
                    if let Err(e) =
                        self.push_fold_operand(&mut col, record.group_id(), record.data())
                    {
                        decided = Some(Err(e));
                        return true;
                    }
                    return false;
                }
                decided = Some(self.finish_fold(
                    &mut col,
                    Some((record, owner.unwrap_or_else(|| dv.as_box()))),
                    key.raw,
                ));
                true
            },
        );

        if let Some(res) = decided {
            return res;
        }

        // the delta chain decided nothing: fall back to the sst base, then to the
        // history region, then to absence
        // Key::raw is unique in sst
        let (ver, val) = match page.search_sst_value(&key) {
            Some(x) => x,
            // nothing older exists beyond the delta chain
            None => {
                return self.finish_fold(&mut col, None, key.raw);
            }
        };
        if !visible(ver.txid, val.group_id()) {
            // the sst base is outside this snapshot: only older history can decide
            return match val.get_hist() {
                Some(hist) => {
                    self.traverse_hist(&page.loader, key.txid, hist, key.raw, &mut visible, col)
                }
                // invisible head with no history region: the chain ends here, so
                // absence is the base barrier and any collected operands still
                // fold against it
                None => self.finish_fold(&mut col, None, key.raw),
            };
        }
        if val.is_tombstone() {
            return self.finish_fold(&mut col, None, key.raw);
        }
        let (record, owner) = val.get_record(&page.loader);
        if val.is_merge() {
            self.push_fold_operand(&mut col, record.group_id(), record.data())?;
            return match val.get_hist() {
                Some(hist) => {
                    self.traverse_hist(&page.loader, key.txid, hist, key.raw, &mut visible, col)
                }
                // merge head with no history region: absence is the base barrier
                None => self.finish_fold(&mut col, None, key.raw),
            };
        }
        // no collected operands: the plain base is the value itself
        if col.operands.is_empty() {
            return Ok(ValRef::new(
                record,
                owner.unwrap_or_else(|| page.base_box()),
            ));
        }
        self.finish_fold(
            &mut col,
            Some((record, owner.unwrap_or_else(|| page.base_box()))),
            key.raw,
        )
    }

    /// resolves the logical value of one key for this snapshot.
    ///
    /// walks the key's versions newest -> oldest across three regions — the delta
    /// chain, the sst base, then the history region — skipping versions invisible
    /// to the snapshot. each visible version's envelope kind decides the outcome:
    /// a tombstone is an absence barrier, a merge operand is collected, and a plain
    /// base value is the barrier the collected operands fold onto. if no region
    /// yields a barrier, any collected operands fold against absence.
    pub fn traverse<F>(&self, g: &Guard, key: Key, mut visible: F) -> Result<ValRef, OpCode>
    where
        F: FnMut(u64, u8) -> bool,
    {
        let page = self.find_leaf(g, key.raw)?;
        self.traverse_page(page, key, &mut visible)
    }

    /// absolute latest head metadata without materializing values: operator-free and
    /// fold-free, used by abort-clean bookkeeping
    pub(crate) fn latest_head_meta(
        &self,
        g: &Guard,
        raw: &[u8],
    ) -> Result<Option<LatestValMeta>, OpCode> {
        let page = self.find_leaf(g, raw)?;
        Ok(page
            .find_latest_meta(&Key::new(raw, Ver::new(u64::MAX, u32::MAX)))
            .map(|m| LatestValMeta {
                ver: m.ver,
                group_id: m.group_id,
                is_del: m.is_del,
                is_merge: m.is_merge,
            }))
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
    /// snapshot driving visibility checks and merge resolution
    snapshot: SnapshotStamp,
    /// whether the shared key scratch contains a previously examined key
    has_last: bool,
    /// compatibility iterator must remain terminated after a fallible scan error
    failed: bool,
    /// merge records are rare; keep the plain scan branch free of merge folding work
    merge_mode: bool,
    /// prefix identity of the last candidate key kept in the scratch buffer
    last_prefix_ptr: usize,
    last_prefix_len: usize,
    page_epoch: u64,
    last_page_epoch: u64,
    bound_prefix_ptr: usize,
    bound_prefix_len: usize,
    bound_page_epoch: u64,
    bound_from_item: bool,
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

impl<'a> Iter<'a> {
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

    /// resolves one candidate key through the shared snapshot fold resolver.
    /// Deleted resolves to `None`; a live value resolves to `(gid, bytes)`.
    /// only visible merge heads reach this: plain and tombstone versions are
    /// decided by the candidate walk itself
    #[cold]
    #[inline(never)]
    fn resolve_item(&mut self, raw: &[u8]) -> Result<Option<(u8, Vec<u8>)>, OpCode> {
        let ctx = self.tree.store.context;
        let probe = Key::new(raw, Ver::new(self.snapshot.start_ts, NULL_CMD));
        // the candidate came from the current leaf, so retain its snapshot and
        // avoid repeating the root-to-leaf lookup used by point reads; cache owns
        // the node while this resolver runs, so the page view need not clone it
        let page = self
            .cache
            .as_ref()
            .filter(|node| raw >= node.lo() && node.hi().is_none_or(|hi| raw < hi))
            .map(|node| Page::from_swip(std::ptr::from_ref(node.as_ref()) as u64));
        let resolved = match page {
            Some(page) => self.tree.traverse_page(page, probe, |txid, gid| {
                is_visible_to(&ctx, self.snapshot, gid, txid)
            }),
            None => self.tree.traverse(&self.guard, probe, |txid, gid| {
                is_visible_to(&ctx, self.snapshot, gid, txid)
            }),
        };
        match resolved {
            // only apply -> None deletes; every other resolver Err is an engine
            // error and must surface through the reliable iterator API, never as
            // a skip
            Ok(vr) => Ok(Some((vr.group_id(), vr.to_vec()))),
            Err(OpCode::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn bounds_ok(&self, item: &IterItem<'a, Loader>, forward: bool) -> bool {
        let lo_ok = match &self.lo {
            Bound::Unbounded => true,
            Bound::Included(b) => item.cmp_key(b.as_slice()).is_ge(),
            Bound::Excluded(b) => item.cmp_key(b.as_slice()).is_gt(),
        };
        if forward {
            if !lo_ok {
                return false;
            }
            return match &self.hi {
                Bound::Unbounded => true,
                Bound::Included(h) => item.cmp_key(h.as_slice()).is_le(),
                Bound::Excluded(h) => item.cmp_key(h.as_slice()).is_lt(),
            };
        }
        let hi_ok = match &self.hi {
            Bound::Unbounded => true,
            Bound::Included(h) => item.cmp_key(h.as_slice()).is_le(),
            Bound::Excluded(h) => item.cmp_key(h.as_slice()).is_lt(),
        };
        if !hi_ok {
            return false;
        }
        match &self.lo {
            Bound::Unbounded => true,
            Bound::Included(b) => item.cmp_key(b.as_slice()).is_ge(),
            Bound::Excluded(b) => item.cmp_key(b.as_slice()).is_gt(),
        }
    }

    #[inline(always)]
    fn same_last_key(&self, item: &IterItem<'a, Loader>) -> bool {
        if !self.has_last {
            return false;
        }
        let cached = item.key();
        let (prefix_ptr, prefix_len) = item.prefix_identity();
        if self.page_epoch == self.last_page_epoch
            && prefix_ptr as usize == self.last_prefix_ptr
            && prefix_len == self.last_prefix_len
        {
            return item.base == &cached[self.last_prefix_len..];
        }
        item.cmp_key(cached).is_eq()
    }

    #[inline(always)]
    fn remember_candidate<L: ILoader>(&mut self, item: &IterItem<'_, L>) {
        self.has_last = true;
        let (prefix_ptr, prefix_len) = item.prefix_identity();
        self.last_prefix_ptr = prefix_ptr as usize;
        self.last_prefix_len = prefix_len;
        self.last_page_epoch = self.page_epoch;
    }

    #[inline(always)]
    fn lower_bound_ok(&self, item: &IterItem<'a, Loader>) -> bool {
        match &self.lo {
            Bound::Unbounded => true,
            Bound::Included(bound) => self.compare_lower(item, bound).is_ge(),
            Bound::Excluded(bound) => self.compare_lower(item, bound).is_gt(),
        }
    }

    #[inline(always)]
    fn compare_lower(&self, item: &IterItem<'a, Loader>, bound: &[u8]) -> Ordering {
        let (prefix_ptr, prefix_len) = item.prefix_identity();
        if self.bound_from_item
            && self.page_epoch == self.bound_page_epoch
            && prefix_ptr as usize == self.bound_prefix_ptr
            && prefix_len == self.bound_prefix_len
        {
            return item.base.cmp(&bound[self.bound_prefix_len..]);
        }
        item.cmp_key(bound)
    }

    #[inline(always)]
    fn remember_bound<L: ILoader>(&mut self, item: &IterItem<'_, L>) {
        let (prefix_ptr, prefix_len) = item.prefix_identity();
        self.bound_prefix_ptr = prefix_ptr as usize;
        self.bound_prefix_len = prefix_len;
        self.bound_page_epoch = self.page_epoch;
        self.bound_from_item = true;
    }

    #[inline(always)]
    pub fn try_next(&mut self) -> Result<Option<IterItem<'a, Loader>>, OpCode> {
        self.rev_iter.take();

        'retry: while !self.collapsed() {
            if self.iter.is_none() {
                let node = match self.tree.find_leaf(&self.guard, self.low_key()) {
                    Ok(node) => node,
                    Err(OpCode::Again) => {
                        self.guard.flush();
                        continue;
                    }
                    Err(OpCode::NotFound) => return Ok(None),
                    Err(e) => return Err(e),
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

                let cache = must_exist!(self.cache.as_ref());
                let bound = must_exist!(self.iter_bound.as_ref());
                self.page_epoch = self.page_epoch.wrapping_add(1);
                self.iter = Some(unsafe {
                    std::mem::transmute::<RawLeafIter<'_, Loader>, RawLeafIter<'_, Loader>>(
                        cache.successor(bound.as_ref(), self.cached_key),
                    )
                });
                #[cfg(feature = "extra_check")]
                crate::testing::fire_tree_update_sync_point(
                    crate::testing::TreeUpdateSyncPoint::AfterIteratorPageCaptureBeforeCandidateWalk,
                    must_exist!(self.cache.as_ref()).pid(),
                );
            }

            let r = loop {
                let next = {
                    let iter = must_exist!(self.iter.as_mut());
                    iter.try_next()
                };
                match next {
                    Ok(Some(item)) => {
                        let lo_ok = self.lower_bound_ok(&item);
                        if !lo_ok {
                            continue;
                        }
                        // step past versions invisible to this snapshot: the leaf
                        // iterator yields the key's older versions next, so an
                        // invisible head never needs the shared resolver
                        if !is_visible_to(
                            &self.tree.store.context,
                            self.snapshot,
                            item.val.group_id(),
                            item.txid(),
                        ) {
                            continue;
                        }
                        // The common non-merge workload only needs the original
                        // deduplication/tombstone filter. If a merge appears after
                        // iterator creation, switch to the full resolver below.
                        let kind = item.val.kind();
                        if !self.merge_mode {
                            if self.same_last_key(&item) {
                                continue;
                            }
                            let _ = item.assembled_key();
                            self.remember_candidate(&item);
                            if kind == Val::DEL_BIT {
                                continue;
                            }
                            if kind == 0 {
                                break Some(item);
                            }
                            self.merge_mode = true;
                            self.has_last = false;
                        }
                        // cached_key holds the last returned/resolved key; do not
                        // re-resolve older versions from the same key run
                        if self.same_last_key(&item) {
                            continue;
                        }
                        if kind == 0 {
                            let _ = item.assembled_key();
                            self.remember_candidate(&item);
                            break Some(item);
                        }
                        let raw = item.assembled_key();
                        self.remember_candidate(&item);
                        if kind == Val::DEL_BIT {
                            // a visible tombstone is the newest visible version of
                            // the run: no visible operand sits above it, so the
                            // run is deleted
                            continue;
                        }
                        // visible merge head: the shared resolver folds the chain
                        match self.resolve_item(raw.as_slice())? {
                            None => {
                                // the whole chain folded to "deleted": skip the run
                                continue;
                            }
                            Some((gid, data)) => {
                                break Some(item.with_folded(gid, data));
                            }
                        }
                    }
                    Ok(None) => break None,
                    Err(OpCode::Again | OpCode::NotFound) => {
                        self.iter.take();
                        continue 'retry;
                    }
                    Err(e) => return Err(e),
                }
            };

            if let Some(item) = r {
                // reuse existing lower-bound buffer to avoid realloc per item
                let key = item.key();
                self.remember_bound(&item);
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
                    Bound::Unbounded => return Ok(Some(item)),
                    Bound::Included(ref h) if item.cmp_key(h.as_slice()).is_le() => {
                        return Ok(Some(item));
                    }
                    Bound::Excluded(ref h) if item.cmp_key(h.as_slice()).is_lt() => {
                        return Ok(Some(item));
                    }
                    _ => return Ok(None),
                }
            } else {
                self.iter.take();
                let node = must_exist!(self.cache.as_ref());
                if let Some(hi) = node.hi() {
                    self.lo = Bound::Included(hi.to_vec());
                    continue;
                }
                break;
            }
        }

        Ok(None)
    }

    pub fn try_next_back(&mut self) -> Result<Option<IterItem<'a, Loader>>, OpCode> {
        self.iter.take();

        'retry: while !self.collapsed() {
            if self.rev_iter.is_none() {
                let node = match self.find_leaf_for_next_back() {
                    Ok(node) => node,
                    Err(OpCode::Again) => {
                        self.guard.flush();
                        continue;
                    }
                    Err(OpCode::NotFound) => return Ok(None),
                    Err(e) => return Err(e),
                };
                let next_node = node.ref_node();
                if let Some(cache) = self.cache.as_mut() {
                    **cache = next_node;
                } else {
                    self.cache = Some(Box::new(next_node));
                }
                self.page_epoch = self.page_epoch.wrapping_add(1);
                self.rev_iter = Some(unsafe {
                    std::mem::transmute::<RawLeafRevIter<'_, Loader>, RawLeafRevIter<'_, Loader>>(
                        must_exist!(self.cache.as_ref()).predecessor(
                            &self.lo,
                            &self.hi,
                            self.cached_key,
                        ),
                    )
                });
                #[cfg(feature = "extra_check")]
                crate::testing::fire_tree_update_sync_point(
                    crate::testing::TreeUpdateSyncPoint::AfterIteratorPageCaptureBeforeCandidateWalk,
                    must_exist!(self.cache.as_ref()).pid(),
                );
            }

            let res = loop {
                let next = {
                    let iter = must_exist!(self.rev_iter.as_mut());
                    iter.try_next_back()
                };
                match next {
                    Ok(Some(item)) => {
                        if !self.bounds_ok(&item, false) {
                            continue;
                        }
                        // step past versions invisible to this snapshot: the leaf
                        // iterator yields the key's older versions next, so an
                        // invisible head never needs the shared resolver
                        if !is_visible_to(
                            &self.tree.store.context,
                            self.snapshot,
                            item.val.group_id(),
                            item.txid(),
                        ) {
                            continue;
                        }
                        let kind = item.val.kind();
                        if !self.merge_mode {
                            if self.same_last_key(&item) {
                                continue;
                            }
                            let _ = item.assembled_key();
                            self.remember_candidate(&item);
                            if kind == Val::DEL_BIT {
                                continue;
                            }
                            if kind == 0 {
                                break Some(item);
                            }
                            self.merge_mode = true;
                            self.has_last = false;
                        }
                        // cached_key holds the last returned/resolved key; do not
                        // re-resolve older versions from the same key run
                        if self.same_last_key(&item) {
                            continue;
                        }
                        if kind == 0 {
                            let _ = item.assembled_key();
                            self.remember_candidate(&item);
                            break Some(item);
                        }
                        let raw = item.assembled_key();
                        self.remember_candidate(&item);
                        if kind == Val::DEL_BIT {
                            // a visible tombstone is the newest visible version of
                            // the run: no visible operand sits above it, so the
                            // run is deleted
                            continue;
                        }
                        // visible merge head: the shared resolver folds the chain
                        match self.resolve_item(raw.as_slice())? {
                            None => {
                                // the whole chain folded to "deleted": skip the run
                                continue;
                            }
                            Some((gid, data)) => {
                                break Some(item.with_folded(gid, data));
                            }
                        }
                    }
                    Ok(None) => break None,
                    Err(OpCode::Again | OpCode::NotFound) => {
                        self.rev_iter.take();
                        continue 'retry;
                    }
                    Err(e) => return Err(e),
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
                return Ok(Some(item));
            }

            self.rev_iter.take();
            let lo = must_exist!(self.cache.as_ref()).lo();
            if lo.is_empty() {
                return Ok(None);
            }
            self.hi = Bound::Excluded(lo.to_vec());
        }

        Ok(None)
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = IterItem<'a, Loader>;

    /// compatibility wrapper: terminates on the first iterator error
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.try_next() {
            Ok(item) => item,
            Err(_) => {
                self.failed = true;
                None
            }
        }
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    #[inline(always)]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.try_next_back() {
            Ok(item) => item,
            Err(_) => {
                self.failed = true;
                None
            }
        }
    }
}

#[cfg(feature = "extra_check")]
#[cfg(test)]
mod merge_test {
    use crate::MergeOperator;
    use crate::types::data::{Key, Ver};
    use crate::{BucketOptions, Mace, Options, RandomPath};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };
    use std::thread;

    #[derive(Default)]
    struct AddOp;

    impl MergeOperator for AddOp {
        fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
            let (l, r) = (decode_u64(left), decode_u64(right));
            l.wrapping_add(r).to_le_bytes().to_vec()
        }

        fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
            let o = decode_u64(operand);
            Some(match base {
                None => o.to_le_bytes().to_vec(),
                Some(b) => decode_u64(b).wrapping_add(o).to_le_bytes().to_vec(),
            })
        }
    }

    fn decode_u64(raw: &[u8]) -> u64 {
        u64::from_le_bytes(raw.try_into().expect("u64 operand"))
    }

    fn op() -> Option<Arc<dyn MergeOperator>> {
        Some(Arc::new(AddOp))
    }

    /// serializes the F1 window tests: the abort hook is a single global slot
    /// and two concurrent tests would cross-fire into each other's state
    static F1_HOOK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// F1 regression harness: hold a modified abort at the fact->abort-clean
    /// enqueue handoff (the shared WAL logging lock is held across the window),
    /// advance the collector past the aborted txid while the pending-abort-clean
    /// floor is not yet raised, then run a materializing consolidation directly
    /// (try_compact never takes the WAL lock, so it can run inside the window).
    ///
    /// The hook slot is process-wide, so the window is a condvar flag instead of
    /// a barrier: unrelated aborts (including same-txid aborts from other stores)
    /// merely park briefly and are released by the next dance, and the aborted
    /// transaction cannot complete its enqueue until the test dances — so the
    /// dance loop deterministically lands inside the target window.
    struct AbortWindow {
        /// set when a target window point fires; cleared by each dance
        open: Arc<AtomicBool>,
        /// park/release for abort threads waiting in the window
        cv: Arc<(std::sync::Mutex<()>, std::sync::Condvar)>,
        _reset: HookReset,
    }

    impl AbortWindow {
        /// `target` is the aborting txn's start_ts, stored by the test right
        /// before the abort (u64::MAX before arming); the hook ignores every
        /// other txid so unrelated tests' aborts never park (best effort: a
        /// colliding stray abort only adds one harmless dance)
        fn arm(target: Arc<AtomicU64>) -> Self {
            let open = Arc::new(AtomicBool::new(false));
            let cv = Arc::new((std::sync::Mutex::new(()), std::sync::Condvar::new()));
            let _reset = HookReset;
            crate::testing::set_txn_abort_hook(Some(Arc::new({
                let open = open.clone();
                let cv = cv.clone();
                move |point, txid| {
                    if point
                        != crate::testing::TxnAbortSyncPoint::AfterAbortFactBeforeAbortCleanEnqueue
                    {
                        return;
                    }
                    if txid != target.load(Ordering::Acquire) {
                        return;
                    }
                    open.store(true, Ordering::Release);
                    cv.1.notify_one();
                    let (lock, cvar) = &*cv;
                    let guard = lock.lock().unwrap();
                    let _ = cvar.wait_timeout_while(
                        guard,
                        std::time::Duration::from_millis(500),
                        |_| open.load(Ordering::Acquire),
                    );
                }
            })));
            Self { open, cv, _reset }
        }

        /// waits for an open window, runs `f` inside it, then closes the window
        /// and releases every parked abort thread
        fn dance<F>(&self, f: F)
        where
            F: FnOnce(),
        {
            let (lock, cvar) = &*self.cv;
            let mut guard = lock.lock().unwrap();
            while !self.open.load(Ordering::Acquire) {
                let (g, _) = cvar
                    .wait_timeout(guard, std::time::Duration::from_millis(500))
                    .unwrap();
                guard = g;
            }
            drop(guard);
            let _close = Close {
                open: &self.open,
                cv: &self.cv,
            };
            f();
        }
    }

    /// closes the window even when `dance`'s body panics, so parked stray
    /// abort threads are always released
    struct Close<'a> {
        open: &'a AtomicBool,
        cv: &'a (std::sync::Mutex<()>, std::sync::Condvar),
    }

    impl Drop for Close<'_> {
        fn drop(&mut self) {
            self.open.store(false, Ordering::Release);
            self.cv.1.notify_all();
        }
    }

    struct HookReset;

    impl Drop for HookReset {
        fn drop(&mut self) {
            crate::testing::clear_txn_abort_hook();
        }
    }

    /// drives safe_exclusive past `txid` deterministically (extra_check wake)
    fn wait_safe_past(db: &crate::store::store::Bucket, txid: u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while crate::testing::safe_exclusive(db) <= txid {
            assert!(
                std::time::Instant::now() < deadline,
                "collector must advance safe_exclusive past {txid}"
            );
            crate::testing::wake_cc_collector(db);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    fn fold_u64(
        mace: &Mace,
        bucket: &str,
        key: &[u8],
        operator: Option<Arc<dyn MergeOperator>>,
    ) -> u64 {
        let db = mace
            .open_bucket_with_options(
                bucket,
                BucketOptions {
                    merge_operator: operator,
                    ..BucketOptions::default()
                },
            )
            .expect("bucket must exist");
        let v = db.view().unwrap().get(key).unwrap();
        decode_u64(v.slice())
    }

    /// F1 regression, direction (a): a retained-aborted merge operand below a
    /// committed merge head (merge/merge coexistence) must never fold into a
    /// materialized base. without the abort predicate the synthesized row
    /// carries the committed boundary ver and the aborted bytes leak to every
    /// future snapshot: 10 +2(aborted) +3(committed) would read 15, not 13.
    #[test]
    fn aborted_operand_never_folds_into_materialized_base() {
        let _hook_guard = F1_HOOK_LOCK.lock().unwrap();
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.sync_on_write = true;
        let operator = op();
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace
            .new_bucket(
                "b",
                BucketOptions {
                    merge_operator: operator.clone(),
                    ..BucketOptions::default()
                },
            )
            .unwrap();

        let kv = db.begin().unwrap();
        kv.put("k", 10u64.to_le_bytes()).unwrap();
        kv.commit().unwrap();

        // the aborted merge must stay physically present below a newer
        // committed head: a write after the abort would eagerly clean the
        // aborted head, so the committed merge is admitted over the
        // still-active invisible head first
        let target = Arc::new(AtomicU64::new(u64::MAX));
        let window = AbortWindow::arm(target.clone());
        let (txid_tx, txid_rx) = std::sync::mpsc::channel();
        let (abort_tx, abort_rx) = std::sync::mpsc::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let db_for_abort = db.clone();
        let abort_handle = thread::spawn(move || {
            let kv = db_for_abort.begin().unwrap();
            let start_ts = crate::testing::txn_start_ts(&kv);
            kv.merge("k", 2u64.to_le_bytes()).unwrap();
            target.store(start_ts, Ordering::Release);
            txid_tx.send(start_ts).unwrap();
            abort_rx.recv().unwrap();
            drop(kv); // modified abort, parked at the fact->enqueue handoff
            done_tx.send(()).unwrap();
        });

        let t1 = txid_rx.recv().unwrap();
        let kv = db.begin().unwrap();
        let t2 = crate::testing::txn_start_ts(&kv);
        kv.merge("k", 3u64.to_le_bytes()).unwrap();
        kv.commit().unwrap();
        assert!(
            t2 > t1,
            "committed merge must be newer than the aborted one"
        );

        abort_tx.send(()).unwrap();
        // the abort cannot finish its enqueue until a dance closes its window:
        let mut completed = false;
        // the first dance that lands on our txid is deterministic
        for _ in 0..4 {
            window.dance(|| {
                // the race window: safe advances past both txids while the
                // pending-abort-clean floor is not yet raised
                wait_safe_past(&db, t2);

                // materializing consolidation over k's chain, inside the window
                let g = crossbeam_epoch::pin();
                let page = db.tree.find_leaf(&g, b"k").unwrap();
                db.tree.try_compact(&g, page);
                drop(g);

                assert_eq!(
                    fold_u64(&mace, "b", b"k", operator.clone()),
                    13,
                    "aborted operand must not leak into the materialized base"
                );
            });
            if done_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .is_ok()
            {
                completed = true;
                break;
            }
        }
        assert!(
            completed,
            "the aborted transaction must complete once its window is danced"
        );
        abort_handle.join().unwrap();
        drop(window); // release the process-wide hook slot before reopening

        // abort-clean ran over the already-folded chain: value must hold
        assert_eq!(fold_u64(&mace, "b", b"k", operator.clone()), 13);

        // durable closure across shutdown checkpoint and reopen (the frontier
        // covers the absorbed LSNs; the aborted txn is never redone); the
        // bucket handle must drop first so the first store fully shuts down
        // (its exit drain runs against intact WAL files) before the reopen
        drop(db);
        drop(mace);
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        assert_eq!(
            fold_u64(&mace, "b", b"k", operator),
            13,
            "no leak and no double-add after reopen"
        );
    }

    /// F1 regression, direction (b): an aborted merge as the newest chain
    /// member must never stamp the synthesized row with the aborted ver —
    /// abort-clean would then drop the whole row and lose the committed base.
    #[test]
    fn aborted_merge_head_keeps_committed_base_after_compact() {
        let _hook_guard = F1_HOOK_LOCK.lock().unwrap();
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.sync_on_write = true;
        let operator = op();
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace
            .new_bucket(
                "b",
                BucketOptions {
                    merge_operator: operator.clone(),
                    ..BucketOptions::default()
                },
            )
            .unwrap();

        let kv = db.begin().unwrap();
        kv.put("k", 10u64.to_le_bytes()).unwrap();
        kv.commit().unwrap();

        let target = Arc::new(AtomicU64::new(u64::MAX));
        let window = AbortWindow::arm(target.clone());
        let (txid_tx, txid_rx) = std::sync::mpsc::channel();
        let (abort_tx, abort_rx) = std::sync::mpsc::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let db_for_abort = db.clone();
        let abort_handle = thread::spawn(move || {
            let kv = db_for_abort.begin().unwrap();
            let start_ts = crate::testing::txn_start_ts(&kv);
            kv.merge("k", 2u64.to_le_bytes()).unwrap();
            target.store(start_ts, Ordering::Release);
            txid_tx.send(start_ts).unwrap();
            abort_rx.recv().unwrap();
            drop(kv); // modified abort, parked at the fact->enqueue handoff
            done_tx.send(()).unwrap();
        });

        let t1 = txid_rx.recv().unwrap();
        abort_tx.send(()).unwrap();
        let mut completed = false;
        for _ in 0..4 {
            window.dance(|| {
                wait_safe_past(&db, t1);

                // consolidation folds k's chain while the aborted head is physical
                let g = crossbeam_epoch::pin();
                let page = db.tree.find_leaf(&g, b"k").unwrap();
                db.tree.try_compact(&g, page);
                drop(g);

                assert_eq!(
                    fold_u64(&mace, "b", b"k", operator.clone()),
                    10,
                    "aborted head must not fold into a row stamped with the aborted ver"
                );
            });
            if done_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .is_ok()
            {
                completed = true;
                break;
            }
        }
        assert!(
            completed,
            "the aborted transaction must complete once its window is danced"
        );
        abort_handle.join().unwrap();
        drop(window); // release the process-wide hook slot before reopening

        // abort-clean must not find a synthesized row carrying the aborted ver
        assert_eq!(
            fold_u64(&mace, "b", b"k", operator.clone()),
            10,
            "committed base must survive abort-clean"
        );

        // durable closure: the bucket handle must drop first so the first
        // store fully shuts down (its exit drain runs against intact WAL
        // files) before the reopen
        drop(db);
        drop(mace);
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        assert_eq!(
            fold_u64(&mace, "b", b"k", operator),
            10,
            "base survives checkpoint and reopen"
        );
    }

    /// a retained-aborted plain value must not hide a committed base when the
    /// abort-clean task is still between fact publication and queue insertion
    #[test]
    fn aborted_plain_without_operator_keeps_committed_base() {
        let _hook_guard = F1_HOOK_LOCK.lock().unwrap();
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.sync_on_write = true;
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace.new_bucket("b", BucketOptions::default()).unwrap();

        let kv = db.begin().unwrap();
        kv.put("k", 10u64.to_le_bytes()).unwrap();
        kv.commit().unwrap();

        let target = Arc::new(AtomicU64::new(u64::MAX));
        let window = AbortWindow::arm(target.clone());
        let (txid_tx, txid_rx) = std::sync::mpsc::channel();
        let (abort_tx, abort_rx) = std::sync::mpsc::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let db_for_abort = db.clone();
        let abort_handle = thread::spawn(move || {
            let kv = db_for_abort.begin().unwrap();
            let start_ts = crate::testing::txn_start_ts(&kv);
            kv.upsert("k", 20u64.to_le_bytes()).unwrap();
            target.store(start_ts, Ordering::Release);
            txid_tx.send(start_ts).unwrap();
            abort_rx.recv().unwrap();
            drop(kv);
            done_tx.send(()).unwrap();
        });

        let t1 = txid_rx.recv().unwrap();
        abort_tx.send(()).unwrap();
        let check = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut completed = false;
            for _ in 0..4 {
                window.dance(|| {
                    wait_safe_past(&db, t1);
                    let g = crossbeam_epoch::pin();
                    let page = db.tree.find_leaf(&g, b"k").unwrap();
                    db.tree.try_compact(&g, page);
                    drop(g);

                    let value = db.view().unwrap().get("k").unwrap();
                    assert_eq!(decode_u64(value.slice()), 10);
                });
                if done_rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .is_ok()
                {
                    completed = true;
                    break;
                }
            }
            assert!(completed, "the aborted transaction must complete");
        }));
        abort_handle.join().unwrap();
        drop(window);
        check.unwrap();
    }

    /// a retained-aborted version above the compaction safe boundary remains
    /// in the page until the dedicated abort-clean rewrite removes it
    #[test]
    fn above_safe_retained_abort_stays_verbatim_during_compact() {
        let _hook_guard = F1_HOOK_LOCK.lock().unwrap();
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.sync_on_write = true;
        let operator = op();
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace
            .new_bucket(
                "b",
                BucketOptions {
                    merge_operator: operator,
                    ..BucketOptions::default()
                },
            )
            .unwrap();

        let kv = db.begin().unwrap();
        kv.put("k", 10u64.to_le_bytes()).unwrap();
        kv.commit().unwrap();
        // keep the safe boundary below the aborted transaction
        let safe_pin = db.begin().unwrap();

        let target = Arc::new(AtomicU64::new(u64::MAX));
        let window = AbortWindow::arm(target.clone());
        let (txid_tx, txid_rx) = std::sync::mpsc::channel();
        let (abort_tx, abort_rx) = std::sync::mpsc::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let db_for_abort = db.clone();
        let abort_handle = thread::spawn(move || {
            let kv = db_for_abort.begin().unwrap();
            let start_ts = crate::testing::txn_start_ts(&kv);
            kv.merge("k", 2u64.to_le_bytes()).unwrap();
            target.store(start_ts, Ordering::Release);
            txid_tx.send(start_ts).unwrap();
            abort_rx.recv().unwrap();
            drop(kv);
            done_tx.send(()).unwrap();
        });

        let t1 = txid_rx.recv().unwrap();
        abort_tx.send(()).unwrap();
        let check = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut completed = false;
            for _ in 0..4 {
                window.dance(|| {
                    assert!(crate::testing::safe_exclusive(&db) <= t1);
                    let g = crossbeam_epoch::pin();
                    let page = db.tree.find_leaf(&g, b"k").unwrap();
                    db.tree.try_compact(&g, page);
                    let page = db.tree.find_leaf(&g, b"k").unwrap();
                    let (head, _, _) = page
                        .find_latest(&Key::new(b"k", Ver::new(u64::MAX, u32::MAX)))
                        .unwrap();
                    assert_eq!(head.txid, t1);
                    drop(g);
                });
                if done_rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .is_ok()
                {
                    completed = true;
                    break;
                }
            }
            assert!(completed, "the aborted transaction must complete");
        }));
        abort_handle.join().unwrap();
        drop(window);
        drop(safe_pin);
        check.unwrap();
    }
}

#[cfg(test)]
mod test {
    use crate::{BucketOptions, Mace, Options, RandomPath};
    use std::thread;

    #[test]
    fn plain_scan_keeps_page_backed_value() {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace
            .new_bucket("default", BucketOptions::default())
            .unwrap();
        let value = vec![7; 4096];
        let kv = db.begin().unwrap();
        kv.put("key", &value).unwrap();
        kv.commit().unwrap();

        let view = db.view().unwrap();
        let mut iter = view.seek("key");
        let item = iter.try_next().unwrap().unwrap();
        assert!(!item.is_folded(), "plain scan must not materialize a copy");
        assert_eq!(item.val(), value);
    }

    #[test]
    fn concurrent_page_hit() {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace
            .new_bucket(
                "default",
                BucketOptions {
                    split_elems: 256,
                    ..BucketOptions::default()
                },
            )
            .unwrap();

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
