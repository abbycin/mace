use crate::hot_true;
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::{
    cmp::Ordering::{self, Equal, Greater, Less},
    ops::{Bound, Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
};

use crate::{
    cc::context::Context,
    must_exist,
    types::{
        base::{BaseIter, BaseRevIter},
        data::{
            Index, IntlKey, IntlSeg, IterItem, Key, LeafSeg, Record, Val, Ver,
            cmp_raw_with_prefixed_tail,
        },
        header::{BoxHeader, NodeType, RemoteHeader},
        refbox::{BaseView, BoxView, DeltaView, RemoteView},
        traits::{IAsBoxRef, IBoxHeader, IDecode, IFrameAlloc, IHeader, IKey, ILoader, IVal},
    },
    utils::{
        Handle, INIT_ORACLE, NULL_ADDR, NULL_CMD, NULL_ORACLE, NULL_PID, OpCode,
        data::Position,
        imtree::{ImTree, Iter, RangeIter},
        options::Options,
    },
};

use super::{header::TagKind, refbox::BoxRef};

pub(crate) type Junk = Vec<u64>;

pub(crate) enum MergeOp {
    Merged,
    MarkChild,
    MarkParent(u64),
}

#[derive(Clone, Copy)]
pub(crate) struct LatestMeta {
    pub(crate) ver: Ver,
    pub(crate) group_id: u8,
    pub(crate) is_del: bool,
    /// envelope carries a merge operand; runtime tag, never persisted in the payload
    pub(crate) is_merge: bool,
}

pub(crate) struct NodeState {
    addr: u64,
    total_size: usize,
    max_txid: u64,
    group: u8,
    /// for smo/compact use
    latest_lsn: Position,
    delta: ImTree<DeltaView>,
}

pub(crate) struct Node<L: ILoader> {
    /// the loader is remote/sibling loader, not node loader
    pub(crate) loader: L,
    mtx: Arc<Mutex<()>>,
    recent: AtomicBool,
    pub(crate) state: RwLock<NodeState>,
    inner: BaseView,
}

fn intl_cmp(x: &DeltaView, y: &DeltaView) -> Ordering {
    // for internal nodes, we never use the txid for insert
    IntlKey::encoded_raw(x.key()).cmp(IntlKey::encoded_raw(y.key()))
}

fn leaf_cmp(x: &DeltaView, y: &DeltaView) -> Ordering {
    let (lver, lraw) = Key::encoded_key_parts(x.key());
    let (rver, rraw) = Key::encoded_key_parts(y.key());
    match lraw.cmp(rraw) {
        Equal => Ver::decode_from(lver).cmp(&Ver::decode_from(rver)),
        ord => ord,
    }
}

fn null_cmp(_x: &DeltaView, _y: &DeltaView) -> Ordering {
    unimplemented!()
}

impl<L: ILoader> Drop for Node<L> {
    fn drop(&mut self) {}
}

impl<L> Node<L>
where
    L: ILoader,
{
    pub(crate) fn new(loader: L, b: BoxRef, group: u8, latest_lsn: Position) -> Self {
        let h = b.header();
        let (addr, total_size, max_txid) = (h.addr, h.total_size as usize, h.txid);
        let base = b.view().as_base();
        loader.pin(b);
        Self {
            loader,
            mtx: Arc::new(Mutex::new(())),
            recent: AtomicBool::new(false),
            state: RwLock::new(NodeState {
                addr,
                total_size,
                max_txid,
                group,
                latest_lsn,
                delta: ImTree::new(if base.header().is_index {
                    intl_cmp
                } else {
                    leaf_cmp
                }),
            }),
            inner: base,
        }
    }

    pub(crate) fn load(addr: u64, loader: L) -> Self {
        let d = loader.load_pinned(addr);
        let mut l = Self {
            loader,
            mtx: Arc::new(Mutex::new(())),
            recent: AtomicBool::new(false),
            state: RwLock::new(NodeState {
                addr: d.addr,
                total_size: d.total_size as usize,
                max_txid: d.header().txid,
                group: 0,
                latest_lsn: Position::MIN,
                delta: ImTree::new(null_cmp),
            }),
            inner: BaseView::null(),
        };
        Self::load_inner(&mut l, d);
        l
    }

    /// keep node-owned pages alive until the page itself is reclaimed
    /// NOTE: remote page are not saved and should be load from dirty pages or cache
    fn save_delta(&self, b: BoxRef) {
        self.loader.pin(b);
    }

    pub(crate) fn reference(&self) -> Self {
        let state = self.state.read();
        Self {
            loader: self.loader.copy(),
            mtx: self.mtx.clone(),
            recent: AtomicBool::new(self.recent.load(Relaxed)),
            state: RwLock::new(NodeState {
                addr: state.addr,
                total_size: state.total_size,
                max_txid: state.max_txid,
                group: state.group,
                latest_lsn: state.latest_lsn,
                delta: state.delta.clone(),
            }),
            inner: self.inner,
        }
    }

    pub(crate) fn pid(&self) -> u64 {
        self.inner.box_header().pid
    }

    pub(crate) fn set_pid(&mut self, pid: u64) {
        self.inner.box_header_mut().pid = pid;
    }

    pub(crate) fn new_leaf<A: IFrameAlloc>(
        a: &mut A,
        loader: L,
        group: u8,
        lsn: Position,
    ) -> Node<L> {
        let mut iter = PlainValSource::new(std::iter::empty::<(LeafSeg, Val)>());
        let b = BaseView::new_leaf(
            a,
            &loader,
            [].as_slice(),
            None,
            NULL_PID,
            &mut iter,
            INIT_ORACLE,
            group,
            lsn,
        );
        let (group, lsn) = {
            let h = b.header();
            (h.group, h.lsn)
        };
        Self::new(loader, b, group, lsn)
    }

    pub(crate) fn new_root<A: IFrameAlloc>(
        a: &mut A,
        loader: L,
        item: &[(IntlKey, Index)],
        group: u8,
        lsn: Position,
    ) -> Node<L> {
        let b = BaseView::new_intl(
            a,
            [].as_slice(),
            None,
            NULL_PID,
            || item.iter().map(|&(x, y)| (IntlSeg::new(&[], x.raw), y)),
            INIT_ORACLE,
            group,
            lsn,
        );
        let (group, lsn) = {
            let h = b.header();
            (h.group, h.lsn)
        };
        Self::new(loader, b, group, lsn)
    }

    /// the length of delta + base
    pub(crate) fn size(&self) -> usize {
        self.state.read().total_size
    }

    pub(crate) fn mark_recent(&self) {
        self.recent.store(true, Relaxed);
    }

    pub(crate) fn take_recent(&self) -> bool {
        self.recent.swap(false, Relaxed)
    }

    pub(crate) fn base_addr(&self) -> u64 {
        self.inner.box_header().addr
    }

    pub(crate) fn base_box(&self) -> BoxRef {
        self.inner.as_box()
    }

    pub(crate) fn collect_junk<F>(&self, mut emit: F)
    where
        F: FnMut(u64),
    {
        self.state
            .read()
            .delta
            .clone()
            .iter()
            .for_each(|x| emit(x.box_header().addr));
        emit(self.base_addr());
    }

    pub(crate) fn collect_frontier_and_junk<F>(&self, mut emit: F)
    where
        F: FnMut(u8, Position, u64),
    {
        self.state.read().delta.clone().iter().for_each(|x| {
            let h = x.box_header();
            emit(h.group, h.lsn, h.addr);
        });
        let h = self.inner.box_header();
        emit(h.group, h.lsn, h.addr);
    }

    fn set_comparator(&mut self, nt: NodeType) {
        let mut state = self.state.write();
        if nt == NodeType::Intl {
            state.delta.set_comparator(intl_cmp);
        } else {
            state.delta.set_comparator(leaf_cmp);
        }
    }

    pub(crate) fn get_group_lsn(&self) -> (u8, Position) {
        let state = self.state.read();
        (state.group, state.latest_lsn)
    }

    pub(crate) fn should_split(&self, split_elem: u16) -> bool {
        let h = self.header();
        let size_limited = h.elems >= split_elem;
        let no_conflict = !h.merging && h.merging_child == NULL_PID && h.elems >= 2;

        size_limited && no_conflict
    }

    pub(crate) fn should_merge(&self) -> bool {
        let h = self.header();
        // `split_elems` is the post-split size snapshot, <= 25% means merge candidate
        let size_limited = h.split_elems >= h.elems * 4;
        let no_conflict = !h.merging && h.merging_child == NULL_PID;
        size_limited && no_conflict
    }

    pub(crate) fn can_merge_child(&self, child_lo: &[u8], child_pid: u64) -> bool {
        hot_true!(eq self.box_header().node_type, NodeType::Intl);
        let h = self.header();
        if h.merging_child != NULL_PID || h.merging {
            return false;
        }
        if h.elems == 0 || child_lo < self.lo() {
            return false;
        }
        if let Some(hi) = self.hi()
            && child_lo >= hi
        {
            return false;
        }

        let (is_left_most, pid) = self.child_index(child_lo);
        !is_left_most && pid == child_pid
    }

    pub(crate) fn merge_node<A: IFrameAlloc>(
        &self,
        a: &mut A,
        other: &Node<L>,
        safe_txid: u64,
        abort_ctx: Handle<Context>,
    ) -> (Node<L>, Junk) {
        let mut junks = Vec::new();
        // base-to-base merges stay operator-free, but still need the abort
        // predicate so a retained aborted head cannot hide its committed base
        let mut no_block = |_: &[u8]| {};
        let lb = self.merge_to_base(
            a,
            &mut junks,
            safe_txid,
            None,
            &mut no_block,
            Some(abort_ctx),
        );
        let rb = other.merge_to_base(
            a,
            &mut junks,
            safe_txid,
            None,
            &mut no_block,
            Some(abort_ctx),
        );
        let (lhs, rhs) = (lb.view().as_base(), rb.view().as_base());

        #[cfg(feature = "extra_check")]
        assert_ne!(self.base_addr(), other.base_addr());
        // because we merge right node into the left one, which means the right node has been modified
        // before, so it has the latest group and lsn
        let (group, lsn) = other.get_group_lsn();
        let merged = lhs.merge(a, &self.loader, rhs, safe_txid, group, lsn);
        let (group, lsn) = {
            let h = merged.header();
            (h.group, h.lsn)
        };
        let mut node = Self::new(self.loader.copy_detached(), merged, group, lsn);
        node.header_mut().split_elems = self.header().split_elems;
        (node, junks)
    }

    pub(crate) fn child_index(&self, k: &[u8]) -> (bool, u64) {
        #[cfg(feature = "extra_check")]
        {
            assert!(self.header().is_index);
            // make sure k is in current node
            assert!(k >= self.lo());
            if let Some(hi) = self.hi() {
                assert!(hi > k);
            }
        }
        let (pos, pid) = must_exist!(self.sst::<IntlKey>().floor_pid_by_raw(k));
        (pos == 0, pid)
    }

    /// NOTE: before we add lock, it will search key in current node and return None if find, after
    /// add lock, the search is useless, so it was removed, but we keep return an Option<Node<L>>
    pub(crate) fn insert_index<A: IFrameAlloc>(
        &self,
        a: &mut A,
        key: &[u8],
        pid: u64,
        safe_txid: u64,
    ) -> Option<(Node<L>, Junk)> {
        #[cfg(feature = "extra_check")]
        if key < self.lo()
            || if let Some(hi) = self.hi() {
                hi <= key
            } else {
                false
            }
        {
            panic!("somehow it happens");
        }

        let (group, lsn) = self.get_group_lsn();
        let b =
            DeltaView::from_key_index(a, IntlKey::new(key), Index::new(pid), safe_txid, group, lsn);
        let mut no_block = |_: &[u8]| {};
        Some(
            self.insert(b)
                .compact(a, safe_txid, None, &mut no_block, None),
        ) // 1/SPLIT_ELEMS chance to run
    }

    fn decode_pefix<K>(
        &self,
        pos: usize,
        prefix_len: usize,
        lo: &[u8],
        hi: &Option<&[u8]>,
    ) -> (Vec<u8>, (usize, usize))
    where
        K: IKey,
    {
        let k = self.sst::<K>().key_at(pos);
        let mut sep = Vec::with_capacity(prefix_len + k.raw().len());
        sep.extend_from_slice(&lo[..prefix_len]);
        sep.extend_from_slice(k.raw());
        let new_prefix_len = (
            BaseView::calc_prefix(lo, &Some(sep.as_slice())),
            BaseView::calc_prefix(sep.as_slice(), hi),
        );
        (sep, new_prefix_len)
    }

    /// NOTE: no need to compact, since we check `should_split` is not use the delta size, i.e, when
    /// `should_split`, the node must have been compacted, and any insert to node will check first
    pub(crate) fn split<A: IFrameAlloc>(&self, a: &mut A) -> (Node<L>, Node<L>) {
        let h = self.inner.header();
        let prefix_len = h.prefix_len as usize;
        let elems = h.elems as usize;
        let sep = elems / 2;
        let lo = self.lo();
        let hi = self.hi();
        // both lhs and rhs keep current sibling, caller rewires lhs after rhs is mapped
        let sibling = self.header().right_sibling;
        let txid = self.box_header().txid;
        let (group, lsn) = self.get_group_lsn();

        let (l, r) = if h.is_index {
            let (sep_key, (llen, rlen)) = self.decode_pefix::<IntlKey>(sep, prefix_len, lo, &hi);
            let lhs_prefix = &lo[..llen];
            let rhs_prefix = &sep_key[..rlen];
            // prefix never shrinks in split
            let (ld, rd) = (lhs_prefix.len() - prefix_len, rhs_prefix.len() - prefix_len);
            (
                BaseView::new_intl(
                    a,
                    lo,
                    Some(sep_key.as_slice()),
                    sibling,
                    || {
                        self.inner
                            .range_iter::<L, IntlKey>(&self.loader, 0, sep)
                            .map(|(k, v)| (IntlSeg::new(lhs_prefix, &k.raw[ld..]), v))
                    },
                    txid,
                    group,
                    lsn,
                ),
                BaseView::new_intl(
                    a,
                    sep_key.as_slice(),
                    hi,
                    sibling,
                    || {
                        self.inner
                            .range_iter::<L, IntlKey>(&self.loader, sep, elems)
                            .map(|(k, v)| (IntlSeg::new(rhs_prefix, &k.raw[rd..]), v))
                    },
                    txid,
                    group,
                    lsn,
                ),
            )
        } else {
            let (sep_key, (llen, rlen)) = self.decode_pefix::<Key>(sep, prefix_len, lo, &hi);
            let lhs_prefix = &lo[..llen];
            // prefix never shrinks in split
            let (ld, rd) = (lhs_prefix.len() - prefix_len, rlen - prefix_len);
            let liter = self
                .inner
                .range_iter::<L, Key>(&self.loader, 0, sep)
                .map(|(k, v)| (LeafSeg::new(lhs_prefix, &k.raw[ld..], k.ver), v));
            let riter = self
                .inner
                .range_iter::<L, Key>(&self.loader, sep, elems)
                .map(|(k, v)| (LeafSeg::new(&sep_key[..rlen], &k.raw[rd..], k.ver), v));
            let mut liter = PlainValSource::new(liter);
            let mut riter = PlainValSource::new(riter);
            (
                BaseView::new_leaf(
                    a,
                    &self.loader,
                    lo,
                    Some(sep_key.as_slice()),
                    sibling,
                    &mut liter,
                    txid,
                    group,
                    lsn,
                ),
                BaseView::new_leaf(
                    a,
                    &self.loader,
                    sep_key.as_slice(),
                    hi,
                    sibling,
                    &mut riter,
                    txid,
                    group,
                    lsn,
                ),
            )
        };

        let (lgroup, llsn) = {
            let h = l.header();
            (h.group, h.lsn)
        };
        let (rgroup, rlsn) = {
            let h = r.header();
            (h.group, h.lsn)
        };
        let (mut lhs, mut rhs) = (
            Self::new(self.loader.copy_detached(), l, lgroup, llsn),
            Self::new(self.loader.copy_detached(), r, rgroup, rlsn),
        );
        lhs.header_mut().split_elems = sep as u16;
        rhs.header_mut().split_elems = (elems - sep) as u16;
        (lhs, rhs)
    }

    #[allow(clippy::iter_skip_zero)]
    pub(crate) fn intl_iter(&'_ self) -> IntlIter<'_, L> {
        hot_true!(eq self.box_header().node_type, NodeType::Intl);
        let len = self.header().prefix_len as usize;
        let lo = self.lo();
        IntlIter {
            prefix: &lo[..len],
            next_l: None,
            next_r: None,
            sst_iter: self
                .inner
                .range_iter(&self.loader, 0, self.inner.header().elems as usize),
            delta_iter: IterAdaptor::Iter(self.state.read().delta.iter().skip(0)),
        }
    }

    #[allow(clippy::iter_skip_zero)]
    fn plain_leaf_iter<'b>(
        &'b self,
        j: &'b mut Junk,
        safe_txid: u64,
        abort_ctx: Option<Handle<Context>>,
    ) -> PlainLeafIter<'b, L> {
        hot_true!(eq self.box_header().node_type, NodeType::Leaf);
        let len = self.header().prefix_len as usize;
        let lo = self.lo();
        PlainLeafIter {
            prefix: &lo[..len],
            next_l: None,
            next_r: None,
            sst_iter: self
                .inner
                .range_iter(&self.loader, 0, self.inner.header().elems as usize),
            delta_iter: IterAdaptor::Iter(self.state.read().delta.iter().skip(0)),
            filter: PlainLeafFilter::new(safe_txid, j, abort_ctx),
        }
    }

    #[allow(clippy::iter_skip_zero)]
    fn leaf_iter<'b>(
        &'b self,
        j: &'b mut Junk,
        safe_txid: u64,
        inline_size: usize,
        op: Option<&'b dyn crate::MergeOperator>,
        abort_ctx: Option<Handle<Context>>,
    ) -> LeafIter<'b, L> {
        hot_true!(eq self.box_header().node_type, NodeType::Leaf);
        let len = self.header().prefix_len as usize;
        let lo = self.lo();
        LeafIter {
            prefix: &lo[..len],
            next_l: None,
            next_r: None,
            pending: None,
            key_buf: Vec::new(),
            sst_iter: self
                .inner
                .range_iter(&self.loader, 0, self.inner.header().elems as usize),
            delta_iter: IterAdaptor::Iter(self.state.read().delta.iter().skip(0)),
            filter: LeafFilter::new(safe_txid, &self.loader, j, op, inline_size, abort_ctx),
        }
    }

    #[allow(clippy::iter_skip_zero)]
    fn leaf_iter_drop_aborted<'b>(
        &'b self,
        j: &'b mut Junk,
        safe_txid: u64,
        ctx: Handle<Context>,
    ) -> LeafIter<'b, L> {
        hot_true!(eq self.box_header().node_type, NodeType::Leaf);
        let len = self.header().prefix_len as usize;
        let lo = self.lo();
        LeafIter {
            prefix: &lo[..len],
            next_l: None,
            next_r: None,
            pending: None,
            key_buf: Vec::new(),
            sst_iter: self
                .inner
                .range_iter(&self.loader, 0, self.inner.header().elems as usize),
            delta_iter: IterAdaptor::Iter(self.state.read().delta.iter().skip(0)),
            filter: LeafFilter::with_drop_aborted(safe_txid, &self.loader, j, ctx),
        }
    }

    pub(crate) fn compact<A: IFrameAlloc>(
        &self,
        a: &mut A,
        safe_txid: u64,
        op: Option<&dyn crate::MergeOperator>,
        block: &mut dyn FnMut(&[u8]),
        abort_ctx: Option<Handle<Context>>,
    ) -> (Node<L>, Junk) {
        let mut junks = Junk::new();
        let b = self.merge_to_base(a, &mut junks, safe_txid, op, block, abort_ctx);
        let mut base = b.view().as_base();
        let h = base.header_mut();
        let old = self.header();
        h.merging = old.merging;
        h.merging_child = old.merging_child;
        let (group, lsn) = {
            let h = b.header();
            (h.group, h.lsn)
        };
        (Self::new(self.loader.copy_detached(), b, group, lsn), junks)
    }

    pub(crate) fn remove_aborted<A: IFrameAlloc>(
        &self,
        a: &mut A,
        safe_txid: u64,
        ctx: Handle<Context>,
    ) -> (Node<L>, Junk, bool) {
        let mut junks = Junk::new();
        let (b, removed) = self.merge_to_base_drop_aborted(a, &mut junks, safe_txid, ctx);
        let mut base = b.view().as_base();
        let h = base.header_mut();
        let old = self.header();
        h.merging = old.merging;
        h.merging_child = old.merging_child;
        let (group, lsn) = {
            let h = b.header();
            (h.group, h.lsn)
        };
        (
            Self::new(self.loader.copy_detached(), b, group, lsn),
            junks,
            removed,
        )
    }

    fn merge_to_base<A: IFrameAlloc>(
        &self,
        a: &mut A,
        j: &mut Junk,
        safe_txid: u64,
        op: Option<&dyn crate::MergeOperator>,
        block: &mut dyn FnMut(&[u8]),
        abort_ctx: Option<Handle<Context>>,
    ) -> BoxRef {
        let h = self.header();
        let lo = self.lo();
        let hi = self.hi();

        if h.is_index {
            let (group, lsn) = self.get_group_lsn();
            BaseView::new_intl(
                a,
                lo,
                hi,
                h.right_sibling,
                || self.intl_iter(),
                safe_txid,
                group,
                lsn,
            )
        } else {
            let (group, lsn) = self.get_group_lsn();
            if op.is_none() {
                let mut iter = self.plain_leaf_iter(j, safe_txid, abort_ctx);
                return BaseView::new_leaf(
                    a,
                    &self.loader,
                    lo,
                    hi,
                    h.right_sibling,
                    &mut iter,
                    safe_txid,
                    group,
                    lsn,
                );
            }
            let mut iter = self.leaf_iter(j, safe_txid, a.inline_size(), op, abort_ctx);
            let base = BaseView::new_leaf(
                a,
                &self.loader,
                lo,
                hi,
                h.right_sibling,
                &mut iter,
                safe_txid,
                group,
                lsn,
            );
            // record keys whose fold violated the merge contract
            for k in iter.filter.blocked_hits.drain(..) {
                block(&k);
            }
            base
        }
    }

    fn merge_to_base_drop_aborted<A: IFrameAlloc>(
        &self,
        a: &mut A,
        j: &mut Junk,
        safe_txid: u64,
        ctx: Handle<Context>,
    ) -> (BoxRef, bool) {
        let h = self.header();
        let lo = self.lo();
        let hi = self.hi();

        hot_true!(!h.is_index);
        let mut iter = self.leaf_iter_drop_aborted(j, safe_txid, ctx);
        let (group, lsn) = self.get_group_lsn();
        let base = BaseView::new_leaf(
            a,
            &self.loader,
            lo,
            hi,
            h.right_sibling,
            &mut iter,
            safe_txid,
            group,
            lsn,
        );
        (base, iter.filter.removed)
    }

    pub(crate) fn process_merge<A: IFrameAlloc>(
        &self,
        a: &mut A,
        op: MergeOp,
        safe_txid: u64,
        abort_ctx: Handle<Context>,
    ) -> (Node<L>, Junk) {
        match op {
            MergeOp::Merged => {
                hot_true!(eq self.box_header().node_type, NodeType::Intl);
                let mut key = None;
                let h = self.header();
                let merging_child = h.merging_child;
                for (k, v) in self.intl_iter() {
                    if v.pid == merging_child {
                        key = Some(k);
                        break;
                    }
                }
                let (group, lsn) = self.get_group_lsn();
                let b = DeltaView::from_key_index(
                    a,
                    must_exist!(key),
                    Index::null(),
                    safe_txid,
                    group,
                    lsn,
                );
                let tmp = self.insert(b);
                let mut no_block = |_: &[u8]| {};
                let (mut p, j) = tmp.compact(a, safe_txid, None, &mut no_block, None);
                p.header_mut().merging_child = NULL_PID;
                p.header_mut().split_elems = h.split_elems + 1;
                (p, j)
            }
            MergeOp::MarkParent(pid) => {
                hot_true!(eq self.box_header().node_type, NodeType::Intl);
                let mut no_block = |_: &[u8]| {};
                let (mut p, j) = self.compact(a, safe_txid, None, &mut no_block, None);
                p.header_mut().merging_child = pid;
                (p, j)
            }
            MergeOp::MarkChild => {
                hot_true!(eq self.box_header().node_type, NodeType::Leaf);
                let mut no_block = |_: &[u8]| {};
                let (mut p, j) = self.compact(a, safe_txid, None, &mut no_block, Some(abort_ctx));
                p.header_mut().merging = true;
                (p, j)
            }
        }
    }

    pub(crate) fn insert(&self, b: BoxRef) -> Node<L> {
        let mut k = b.view().as_delta();
        let h = k.box_header_mut();
        let th = self.box_header();
        let state = self.state.read();
        // link to old address
        h.link = state.addr;
        h.node_type = th.node_type;
        h.pid = th.pid;
        let addr = h.addr;

        // keep the delta page in node_pins, because checkpoint may concurrently happen
        self.loader.pin(b);

        Node {
            loader: self.loader.copy(),
            mtx: self.mtx.clone(),
            recent: AtomicBool::new(self.recent.load(Relaxed)),
            state: RwLock::new(NodeState {
                addr,
                total_size: state.total_size + h.total_size as usize,
                max_txid: h.txid,
                group: h.group,
                latest_lsn: h.lsn,
                delta: state.delta.update(k),
            }),
            inner: self.inner,
        }
    }

    // NOTE: it must be protected by lock
    fn insert_inplace(&self, mut k: DeltaView, remote_size: usize) -> u64 {
        let h = k.box_header_mut();
        let th = self.box_header();
        let addr = h.addr;

        let mut state = self.state.write();
        h.link = state.addr;
        h.node_type = th.node_type;
        h.pid = th.pid;

        state.addr = addr;
        state.total_size += h.total_size as usize + remote_size;
        state.max_txid = h.txid;
        state.group = h.group;
        state.latest_lsn = h.lsn;
        state.delta.put(k);
        addr
    }

    pub(crate) fn lock(&'_ self) -> NodeGuard<'_, L> {
        NodeGuard {
            _guard: self.mtx.lock(),
            node: self,
        }
    }

    pub(crate) fn try_lock(&'_ self) -> Option<NodeGuard<'_, L>> {
        let guard = self.mtx.try_lock()?;
        Some(NodeGuard {
            _guard: guard,
            node: self,
        })
    }

    pub(crate) fn latest_addr(&self) -> u64 {
        self.state.read().addr
    }

    /// when value is inlined return node (or delta) or else retuen remote, the node (or delta) is
    /// always valid when node is valid
    pub(crate) fn find_latest(&self, key: &Key) -> Option<(Ver, Record, BoxRef)> {
        hot_true!(!self.inner.header().is_index);
        let mut result = None;
        self.visit_versions(
            *key,
            |x, y| {
                let (ver, raw) = Key::encoded_key_parts(x.key());
                match raw.cmp(y.raw) {
                    Equal => Ver::decode_from(ver).cmp(&y.ver),
                    ord => ord,
                }
            },
            |dv| {
                let (ver, raw) = Key::encoded_key_parts(dv.key());
                if raw == key.raw() {
                    let v = dv.val();
                    let ver = Ver::decode_from(ver);
                    let (v, r) = v.get_record(&self.loader);
                    result = Some((ver, v, r.unwrap_or_else(|| self.base_box())));
                    return true;
                }
                false
            },
        );

        if result.is_some() {
            return result;
        }

        self.search_sst_value(key).map(|(ver, v)| {
            let (v, r) = v.get_record(&self.loader);
            (ver, v, r.unwrap_or_else(|| self.base_box()))
        })
    }

    pub(crate) fn find_latest_meta(&self, key: &Key) -> Option<LatestMeta> {
        hot_true!(!self.inner.header().is_index);
        let mut result = None;
        let probe = Key::new(key.raw(), Ver::new(u64::MAX, u32::MAX));
        self.visit_versions(
            probe,
            |x, y| {
                let (ver, raw) = Key::encoded_key_parts(x.key());
                match raw.cmp(y.raw) {
                    Equal => Ver::decode_from(ver).cmp(&y.ver),
                    ord => ord,
                }
            },
            |dv| {
                let (ver, raw) = Key::encoded_key_parts(dv.key());
                if raw == key.raw() {
                    let v = dv.val();
                    result = Some(LatestMeta {
                        ver: Ver::decode_from(ver),
                        group_id: v.group_id(),
                        is_del: v.is_tombstone(),
                        is_merge: v.is_merge(),
                    });
                    return true;
                }
                false
            },
        );

        if result.is_some() {
            return result;
        }

        self.search_sst_value(key).map(|(ver, v)| LatestMeta {
            ver,
            group_id: v.group_id(),
            is_del: v.is_tombstone(),
            is_merge: v.is_merge(),
        })
    }

    pub(crate) fn search_sst_value<'a>(&self, key: &Key) -> Option<(Ver, Val<'a>)> {
        hot_true!(eq self.box_header().node_type, NodeType::Leaf);
        self.inner.sst::<Key>().search_ver_val_by_raw(key.raw())
    }

    /// reads the immutable base only when no delta is present at the same
    /// state-lock observation; `None` means the caller must inspect deltas
    pub(crate) fn search_sst_if_delta_empty<'a>(
        &self,
        key: &Key,
    ) -> Option<Option<(Ver, Val<'a>)>> {
        hot_true!(eq self.box_header().node_type, NodeType::Leaf);
        let state = self.state.read();
        if state.delta.len() != 0 {
            return None;
        }
        Some(self.inner.sst::<Key>().search_ver_val_by_raw(key.raw()))
    }

    /// walks the delta chain from `key`'s position (newest first) and hands each
    /// version to `visit`; the visitor returns true to stop the walk early
    pub(crate) fn visit_versions<K, F>(
        &self,
        key: K,
        cmp: fn(&DeltaView, &K) -> Ordering,
        mut visit: F,
    ) where
        K: IKey,
        F: FnMut(DeltaView) -> bool,
    {
        self.state.read().delta.visit_from(&key, cmp, &mut visit);
    }

    #[allow(unused)]
    pub(crate) fn show(&self) {
        let h = self.box_header();
        let state = self.state.read();
        log::debug!(
            "---------- show delta {} {:?} elems {} ----------",
            h.pid,
            h.addr,
            state.delta.len()
        );
        if self.header().is_index {
            let it = state.delta.iter();
            for x in it {
                let k = IntlKey::decode_from(x.key());
                let v = Index::decode_from(x.index());
                log::debug!("{} => {}", k.to_string(), v);
            }
            let sst = self.sst::<IntlKey>();
            sst.show_intl(h.pid, h.addr);
        } else {
            let it = state.delta.iter();
            for x in it {
                let k = Key::decode_from(x.key());
                let val = x.val();
                let (r, _) = val.get_record(&self.loader);
                log::debug!("{} => {}", k.to_string(), r);
            }
            let sst = self.sst::<Key>();
            sst.show_leaf(&self.loader, h.pid, h.addr);
        }
    }

    pub(crate) fn box_header(&self) -> &BoxHeader {
        self.inner.box_header()
    }

    pub(crate) fn delta_len(&self) -> usize {
        self.state.read().delta.len()
    }

    fn load_inner(l: &mut Node<L>, mut d: BoxView) {
        let mut _one_base = true;
        let mut last_type = None;

        loop {
            let h = d.header();
            let state = l.state.get_mut();
            if last_type.is_none() {
                state.group = h.group;
                state.latest_lsn = h.lsn;
            }
            // the head page carries the latest group/lsn for future base rebuilds
            state.total_size += d.total_size as usize;
            state.max_txid = state.max_txid.max(h.txid);
            if let Some(_t) = last_type {
                hot_true!(eq _t, h.node_type);
            } else {
                l.set_comparator(h.node_type);
            }
            last_type = Some(h.node_type);

            match h.kind {
                TagKind::Delta => {
                    let delta = d.as_delta();
                    l.state.get_mut().delta.put(delta);
                }
                TagKind::Base => {
                    hot_true!(_one_base);
                    _one_base = false;
                    l.inner = d.as_base();
                }
                _ => unreachable!("bad kind {:?}", h.kind),
            }
            if d.link == NULL_ADDR {
                break;
            }
            d = l.loader.load_pinned(d.link);
        }
        hot_true!(!l.inner.is_null());
    }

    #[allow(clippy::iter_skip_zero)]
    pub(crate) fn successor<'a>(
        &'a self,
        b: &'a Bound<Vec<u8>>,
        cached_key: Handle<Vec<u8>>,
    ) -> RawLeafIter<'a, L> {
        fn cmp_fn(x: &DeltaView, y: &&[u8]) -> Ordering {
            Key::decode_from(x.key()).raw.cmp(y)
        }

        fn equal_fn(_x: &DeltaView, _y: &&[u8]) -> bool {
            true
        }

        // get the start position in both delta and sst
        let state = self.state.read();
        let (delta, pos) = match b {
            Bound::Unbounded => (IterAdaptor::Iter(state.delta.iter().skip(0)), 0),
            Bound::Included(b) => {
                let r = state
                    .delta
                    .range_from(b.as_slice(), cmp_fn, equal_fn)
                    .skip(0);

                let lo = self.lo();

                let pos = if b.as_slice() < lo {
                    Err(0)
                } else {
                    let key = Key::new(b, Ver::new(NULL_ORACLE, NULL_CMD));
                    self.sst::<Key>().lower_bound(&key)
                };
                (IterAdaptor::Range(r), pos.unwrap_or_else(|x| x))
            }
            Bound::Excluded(b) => {
                let iter = state.delta.range_from(b.as_slice(), cmp_fn, equal_fn);
                let delta = if let Some(cur) = iter.peek()
                    && Key::decode_from(cur.key()).raw == b.as_slice()
                {
                    iter.skip(1)
                } else {
                    iter.skip(0)
                };

                let lo = self.lo();
                let inner_pos = if b.as_slice() < lo {
                    Err(0)
                } else {
                    let key = Key::new(b, Ver::new(NULL_ORACLE, NULL_CMD));
                    self.sst::<Key>().lower_bound(&key)
                };
                let pos = match inner_pos {
                    Ok(x) => {
                        let (k, _) = self.sst::<Key>().kv_at::<Val>(x);
                        if k.raw == b.as_slice() { x + 1 } else { x }
                    }
                    Err(x) => x,
                };
                (IterAdaptor::Range(delta), pos)
            }
        };

        let lo = self.lo();
        let len = self.header().prefix_len as usize;

        RawLeafIter {
            cached_key,
            prefix: &lo[..len],
            next_l: None,
            next_r: None,
            delta_iter: delta,
            sst_iter: self
                .inner
                .range_iter(&self.loader, pos, self.header().elems as usize),
        }
    }

    pub(crate) fn predecessor<'a>(
        &'a self,
        lo: &'a Bound<Vec<u8>>,
        hi: &'a Bound<Vec<u8>>,
        cached_key: Handle<Vec<u8>>,
    ) -> RawLeafRevIter<'a, L> {
        fn cmp_fn(x: &DeltaView, y: &&[u8]) -> Ordering {
            Key::decode_from(x.key()).raw.cmp(y)
        }

        let state = self.state.read();
        let delta = match lo {
            Bound::Unbounded => IterAdaptorRev::IterRev {
                iter: state.delta.iter(),
                excluded: None,
                pending: None,
                group: Vec::new(),
            },
            Bound::Included(b) => IterAdaptorRev::RangeRev {
                iter: state.delta.range_from(b.as_slice(), cmp_fn, |_x, _y| true),
                excluded: None,
                pending: None,
                group: Vec::new(),
            },
            Bound::Excluded(b) => IterAdaptorRev::RangeRev {
                iter: state.delta.range_from(b.as_slice(), cmp_fn, |_x, _y| true),
                excluded: Some(b.as_slice()),
                pending: None,
                group: Vec::new(),
            },
        };

        let sst = self.sst::<Key>();
        let elems = self.header().elems as usize;
        let beg = match hi {
            Bound::Unbounded => elems,
            Bound::Included(h) => {
                let key = Key::new(h, Ver::new(NULL_ORACLE, NULL_CMD));
                match sst.lower_bound(&key) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                }
            }
            Bound::Excluded(h) => {
                let key = Key::new(h, Ver::new(u64::MAX, u32::MAX));
                match sst.lower_bound(&key) {
                    Ok(i) | Err(i) => i,
                }
            }
        };
        let cur = if beg == 0 { -1 } else { beg as isize - 1 };
        let end = match lo {
            Bound::Unbounded => 0,
            Bound::Included(b) => {
                let pos = if b.as_slice() < self.lo() {
                    0
                } else {
                    sst.lower_bound(&Key::new(b, Ver::new(u64::MAX, u32::MAX)))
                        .unwrap_or_else(|x| x)
                };
                pos as isize
            }
            Bound::Excluded(b) => {
                let pos = if b.as_slice() < self.lo() {
                    0
                } else {
                    let key = Key::new(b, Ver::new(NULL_ORACLE, NULL_CMD));
                    match sst.lower_bound(&key) {
                        Ok(i) => {
                            let (k, _) = sst.kv_at::<Val>(i);
                            if k.raw == b.as_slice() { i + 1 } else { i }
                        }
                        Err(i) => i,
                    }
                };
                pos as isize
            }
        };

        let prefix = &self.lo()[..self.header().prefix_len as usize];
        RawLeafRevIter {
            cached_key,
            prefix,
            next_l: None,
            next_r: None,
            sst_iter: BaseRevIter::new(&self.loader, sst, cur, end),
            delta_iter: delta,
        }
    }
}

pub(crate) struct NodeGuard<'a, L: ILoader> {
    _guard: MutexGuard<'a, ()>,
    node: &'a Node<L>,
}

impl<L> NodeGuard<'_, L>
where
    L: ILoader,
{
    pub(crate) fn insert(&self, k: BoxRef, remote: Option<BoxRef>) -> u64 {
        let remote_size = remote
            .as_ref()
            .map(|x| x.header().total_size as usize)
            .unwrap_or(0);
        let addr = self.node.insert_inplace(k.view().as_delta(), remote_size);
        self.node.save_delta(k); // keep the delta page itself alive until page reclamation
        addr
    }
}

impl<L> Deref for Node<L>
where
    L: ILoader,
{
    type Target = BaseView;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<L> DerefMut for Node<L>
where
    L: ILoader,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

enum IterAdaptor<'a, T> {
    Iter(std::iter::Skip<Iter<'a, DeltaView>>),
    Range(std::iter::Skip<RangeIter<'a, DeltaView, T>>),
}

impl<'a, T> Iterator for IterAdaptor<'a, T> {
    type Item = DeltaView;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IterAdaptor::Iter(i) => i.next(),
            IterAdaptor::Range(r) => r.next(),
        }
    }
}

enum IterAdaptorRev<'a> {
    IterRev {
        iter: Iter<'a, DeltaView>,
        excluded: Option<&'a [u8]>,
        pending: Option<DeltaView>,
        group: Vec<DeltaView>,
    },
    RangeRev {
        iter: RangeIter<'a, DeltaView, &'a [u8]>,
        excluded: Option<&'a [u8]>,
        pending: Option<DeltaView>,
        group: Vec<DeltaView>,
    },
}

impl IterAdaptorRev<'_> {
    fn next(&mut self) -> Option<DeltaView> {
        match self {
            IterAdaptorRev::IterRev {
                iter,
                excluded,
                pending,
                group,
            } => {
                if let Some(x) = group.pop() {
                    return Some(x);
                }

                let first = loop {
                    let x = pending.take().or_else(|| iter.next_back())?;
                    if excluded.is_none_or(|b| Key::decode_from(x.key()).raw != b) {
                        break x;
                    }
                };
                let raw = Key::decode_from(first.key()).raw;
                group.push(first);
                while let Some(x) = iter.next_back() {
                    let k = Key::decode_from(x.key());
                    if excluded.is_some_and(|b| k.raw == b) {
                        continue;
                    }
                    if k.raw == raw {
                        // reverse walk yields older->newer, keep full group then emit newest->older
                        group.push(x);
                        continue;
                    }
                    *pending = Some(x);
                    break;
                }
                group.pop()
            }
            IterAdaptorRev::RangeRev {
                iter,
                excluded,
                pending,
                group,
            } => {
                if let Some(x) = group.pop() {
                    return Some(x);
                }

                let first = loop {
                    let x = pending.take().or_else(|| iter.next_back())?;
                    if excluded.is_none_or(|b| Key::decode_from(x.key()).raw != b) {
                        break x;
                    }
                };
                let raw = Key::decode_from(first.key()).raw;
                group.push(first);
                while let Some(x) = iter.next_back() {
                    let k = Key::decode_from(x.key());
                    if excluded.is_some_and(|b| k.raw == b) {
                        continue;
                    }
                    if k.raw == raw {
                        group.push(x);
                        continue;
                    }
                    *pending = Some(x);
                    break;
                }
                group.pop()
            }
        }
    }
}

pub(crate) struct IntlIter<'a, L>
where
    L: ILoader,
{
    prefix: &'a [u8],
    next_l: Option<(IntlSeg<'a>, Index)>,
    next_r: Option<(IntlSeg<'a>, Index)>,
    sst_iter: BaseIter<'a, L, IntlKey<'a>>,
    delta_iter: IterAdaptor<'a, &'a [u8]>,
}

/// payload length field of an encoded synthetic envelope
fn synth_data_size(env: &[u8]) -> usize {
    let off = 1 + 1; // flags + gid, then the u32 length field
    u32::from_le_bytes([env[off], env[off + 1], env[off + 2], env[off + 3]]) as usize
}

/// inline record bytes ([gid][payload]) of an encoded synthetic envelope
fn synth_inline_bytes(env: &[u8]) -> &[u8] {
    let off = 1 + 1 + 4;
    let len = synth_data_size(env);
    &env[off..off + len]
}

/// a candidate version flowing into consolidation: either a page-backed value or a
/// synthesized fold result owned by the consolidation scratch buffer
#[derive(Clone)]
/// classification of a folded value against the persisted size limits
pub(crate) enum FoldClass {
    /// record fits inline
    Inline,
    /// record must materialize through the remote/blob path
    Blob,
    /// record exceeds MAX_KV_SIZE: the fold must not materialize
    TooLarge,
}

/// classify a folded record by its persisted size: [gid][data] is inline below `inline_size`,
/// blob-backed up to `MAX_KV_SIZE`, and too large beyond that
pub(crate) fn classify_fold_size(len: usize, inline_size: usize) -> FoldClass {
    let record_len = size_of::<u8>() + len;
    if record_len < inline_size {
        FoldClass::Inline
    } else if record_len <= Options::MAX_KV_SIZE {
        FoldClass::Blob
    } else {
        FoldClass::TooLarge
    }
}

#[derive(Clone)]
pub(crate) enum CompactVal<'a> {
    Page(Val<'a>),
    /// fully encoded synthetic envelope ([flags][gid][len][gid][payload] inline, or
    /// [flags|REMOTE][gid][len][addr] remote with a provisional address slot), owned
    /// so it can outlive the consolidation scratch buffer across iterator steps;
    /// `blob` carries the encoded record bytes ([gid][data]) when the fold result
    /// must materialize through the remote/blob path (allocated in the second pass
    /// of BaseView::new_leaf, which holds the frame allocator)
    Synth {
        env: Vec<u8>,
        blob: Option<Vec<u8>>,
    },
}

impl<'a> CompactVal<'a> {
    pub(crate) fn data_size(&self) -> usize {
        match self {
            CompactVal::Page(v) => v.data_size(),
            CompactVal::Synth { env, .. } => synth_data_size(env),
        }
    }

    pub(crate) fn is_remote(&self) -> bool {
        match self {
            CompactVal::Page(v) => v.get_remote() != NULL_ADDR,
            // a fold result destined for a blob counts toward the remote hint budget
            CompactVal::Synth { blob, .. } => blob.is_some(),
        }
    }

    pub(crate) fn get_remote(&self) -> u64 {
        match self {
            CompactVal::Page(v) => v.get_remote(),
            // blob-destined rows allocate their address during the second pass
            CompactVal::Synth { .. } => NULL_ADDR,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_tombstone(&self) -> bool {
        match self {
            CompactVal::Page(v) => v.is_tombstone(),
            CompactVal::Synth { env, .. } => env[0] & Val::DEL_BIT != 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_merge(&self) -> bool {
        match self {
            CompactVal::Page(v) => v.is_merge(),
            CompactVal::Synth { env, .. } => env[0] & Val::MERGE_BIT != 0,
        }
    }

    /// materializes the record behind this version (loader only needed for page values)
    pub(crate) fn get_record<L: ILoader>(&self, l: &L) -> (Record, Option<BoxRef>) {
        match self {
            CompactVal::Page(v) => v.get_record(l),
            // synthesized envelopes are always inline: payload region is [gid][data];
            // blob-destined rows decode from their record bytes directly
            CompactVal::Synth { env, blob } => {
                let rec = if let Some(data) = blob {
                    Record::from_slice(data)
                } else {
                    Record::from_slice(synth_inline_bytes(env))
                };
                let mut rec = rec;
                if env[0] & Val::MERGE_BIT != 0 {
                    rec.set_merge_tag();
                }
                (rec, None)
            }
        }
    }

    /// second pass: allocates the remote box for a blob-destined fold result and
    /// returns (record, remote address); page-backed and inline rows return None
    pub(crate) fn materialize_blob<A: IFrameAlloc>(
        &self,
        a: &mut A,
        group: u8,
        lsn: Position,
    ) -> Option<(Record, u64)> {
        match self {
            CompactVal::Synth {
                blob: Some(data), ..
            } => {
                let mut r = a.alloc((data.len() + size_of::<RemoteHeader>()) as u32);
                let h = r.header_mut();
                h.kind = TagKind::Remote;
                h.node_type = NodeType::Leaf;
                h.lsn = lsn;
                h.group = group;
                let mut view = r.view().as_remote();
                view.header_mut().size = data.len();
                view.raw_mut().copy_from_slice(data);
                Some((Record::from_slice(data), view.addr()))
            }
            _ => None,
        }
    }
}

/// adapts a plain page-backed version stream; folding never triggers for these callers
pub(crate) struct PlainValSource<'a, I> {
    iter: I,
    _pd: std::marker::PhantomData<&'a ()>,
}

impl<'a, I> PlainValSource<'a, I>
where
    I: Iterator<Item = (LeafSeg<'a>, Val<'a>)>,
{
    pub(crate) fn new(iter: I) -> Self {
        Self {
            iter,
            _pd: std::marker::PhantomData,
        }
    }
}

impl<'a, I> LeafCompactSource<'a> for PlainValSource<'a, I>
where
    I: Iterator<Item = (LeafSeg<'a>, Val<'a>)>,
{
    #[inline(always)]
    fn next_compact<'s>(
        &mut self,
        _scratch: &'s mut Vec<u8>,
    ) -> Option<(LeafSeg<'a>, CompactVal<'a>)> {
        self.iter.next().map(|(k, v)| (k, CompactVal::Page(v)))
    }
}

/// source of versions for consolidation with optional materialization
pub(crate) trait LeafCompactSource<'a> {
    fn next_compact<'s>(
        &mut self,
        scratch: &'s mut Vec<u8>,
    ) -> Option<(LeafSeg<'a>, CompactVal<'a>)>;
}

/// compact iterator for buckets without merge materialization
pub(crate) struct PlainLeafIter<'a, L>
where
    L: ILoader,
{
    prefix: &'a [u8],
    next_l: Option<(LeafSeg<'a>, Val<'a>)>,
    next_r: Option<(LeafSeg<'a>, Val<'a>)>,
    sst_iter: BaseIter<'a, L, Key<'a>>,
    delta_iter: IterAdaptor<'a, &'a [u8]>,
    filter: PlainLeafFilter<'a>,
}

impl<'a, L: ILoader> LeafCompactSource<'a> for PlainLeafIter<'a, L> {
    #[inline(always)]
    fn next_compact<'s>(
        &mut self,
        _scratch: &'s mut Vec<u8>,
    ) -> Option<(LeafSeg<'a>, CompactVal<'a>)> {
        loop {
            if self.next_r.is_none() {
                let (sst_iter, filter) = (&mut self.sst_iter, &mut self.filter);
                if let Some((k, v)) = sst_iter.next_with_sibling(|addr| filter.junks.push(addr)) {
                    self.next_r = Some((LeafSeg::new(self.prefix, k.raw, k.ver), v));
                }
            }
            if self.next_l.is_none()
                && let Some(x) = self.delta_iter.next()
            {
                let k = Key::decode_from(x.key());
                self.next_l = Some((
                    LeafSeg::new(self.prefix, &k.raw[self.prefix.len()..], k.ver),
                    x.val(),
                ));
            }
            let item = match (self.next_l.take(), self.next_r.take()) {
                (None, None) => return None,
                (None, Some(r)) => r,
                (Some(l), None) => l,
                (Some(l), Some(r)) => match l.0.cmp(&r.0) {
                    Less => {
                        self.next_r = Some(r);
                        l
                    }
                    Greater => {
                        self.next_l = Some(l);
                        r
                    }
                    Equal => unreachable!("never happen"),
                },
            };
            if self.filter.check(&item.0, &item.1) {
                return Some((item.0, CompactVal::Page(item.1)));
            }
        }
    }
}

pub(crate) struct LeafIter<'a, L>
where
    L: ILoader,
{
    prefix: &'a [u8],
    next_l: Option<(LeafSeg<'a>, Val<'a>)>,
    next_r: Option<(LeafSeg<'a>, Val<'a>)>,
    /// next candidate held while the current key is being drained
    pending: Option<(LeafSeg<'a>, Val<'a>)>,
    /// contiguous versions of the key currently being compacted
    key_buf: Vec<(LeafSeg<'a>, Val<'a>)>,
    sst_iter: BaseIter<'a, L, Key<'a>>,
    delta_iter: IterAdaptor<'a, &'a [u8]>,
    filter: LeafFilter<'a, L>,
}

impl<'a, L: ILoader> LeafCompactSource<'a> for LeafIter<'a, L> {
    #[inline(always)]
    fn next_compact<'s>(
        &mut self,
        scratch: &'s mut Vec<u8>,
    ) -> Option<(LeafSeg<'a>, CompactVal<'a>)> {
        if self.filter.operator.is_some() {
            return self.next_compact_key(scratch);
        }

        loop {
            // pending fold outcomes (a synthesized boundary row or a raw-preserving
            // re-emission) always precede the next input item
            if let Some(item) = self.filter.pop_emit() {
                return Some(item);
            }

            let item = self.next_candidate()?;

            match self.filter.check(&item) {
                FilterOut::Keep => return Some((item.0, CompactVal::Page(item.1))),
                FilterOut::Drop => {}
            }
        }
    }
}

impl<'a, L: ILoader> LeafIter<'a, L> {
    #[inline(always)]
    fn next_candidate(&mut self) -> Option<(LeafSeg<'a>, Val<'a>)> {
        if let Some(item) = self.pending.take() {
            return Some(item);
        }
        if self.next_r.is_none() {
            let (sst_iter, filter) = (&mut self.sst_iter, &mut self.filter);
            if let Some((k, v)) = sst_iter.next_with_sibling(|addr| filter.junks.push(addr)) {
                self.next_r = Some((LeafSeg::new(self.prefix, k.raw, k.ver), v));
            }
        }
        if self.next_l.is_none()
            && let Some(x) = self.delta_iter.next()
        {
            let k = Key::decode_from(x.key());
            self.next_l = Some((
                LeafSeg::new(self.prefix, &k.raw[self.prefix.len()..], k.ver),
                x.val(),
            ));
        }
        match (self.next_l.take(), self.next_r.take()) {
            (None, None) => None,
            (None, Some(r)) => Some(r),
            (Some(l), None) => Some(l),
            (Some(l), Some(r)) => match l.0.cmp(&r.0) {
                Less => {
                    self.next_r = Some(r);
                    Some(l)
                }
                Greater => {
                    self.next_l = Some(l);
                    Some(r)
                }
                Equal => unreachable!("never happen"),
            },
        }
    }

    fn next_compact_key<'s>(
        &mut self,
        scratch: &'s mut Vec<u8>,
    ) -> Option<(LeafSeg<'a>, CompactVal<'a>)> {
        loop {
            if let Some(item) = self.filter.pop_emit() {
                return Some(item);
            }
            if self.key_buf.is_empty() {
                let first = self.next_candidate()?;
                let raw = first.0;
                self.key_buf.push(first);
                while let Some(item) = self.next_candidate() {
                    if item.0.raw_cmp(&raw).is_eq() {
                        self.key_buf.push(item);
                    } else {
                        self.pending = Some(item);
                        break;
                    }
                }
                self.filter.process_key(&self.key_buf, scratch);
                self.key_buf.clear();
            }
        }
    }
}

pub(crate) struct RawLeafIter<'a, L>
where
    L: ILoader,
{
    cached_key: Handle<Vec<u8>>,
    prefix: &'a [u8],
    next_l: Option<IterItem<'a, L>>,
    next_r: Option<IterItem<'a, L>>,
    sst_iter: BaseIter<'a, L, Key<'a>>,
    delta_iter: IterAdaptor<'a, &'a [u8]>,
}

pub(crate) struct RawLeafRevIter<'a, L>
where
    L: ILoader,
{
    cached_key: Handle<Vec<u8>>,
    prefix: &'a [u8],
    next_l: Option<IterItem<'a, L>>,
    next_r: Option<IterItem<'a, L>>,
    sst_iter: BaseRevIter<'a, L, Key<'a>>,
    delta_iter: IterAdaptorRev<'a>,
}

impl<'a, L> Iterator for IntlIter<'a, L>
where
    L: ILoader,
{
    type Item = (IntlSeg<'a>, Index);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.next_l.is_none()
                && let Some(x) = self.delta_iter.next()
            {
                let k = IntlKey::decode_from(x.key());
                // NOTE: split raw into two parts which simplify comparation
                self.next_l = Some((
                    IntlSeg::new(self.prefix, &k.raw[self.prefix.len()..]),
                    Index::decode_from(x.index()),
                ));
            }

            if self.next_r.is_none()
                && let Some((k, v)) = self.sst_iter.next()
            {
                self.next_r = Some((IntlSeg::new(self.prefix, k.raw), v));
            }

            match (self.next_l, self.next_r) {
                (None, None) => return None,
                (None, Some(x)) => {
                    self.next_r = None;
                    return Some(x);
                }
                (Some(x), None) => {
                    self.next_l = None;
                    hot_true!(!x.1.is_tombstone());
                    return Some(x);
                }
                (Some(l), Some(r)) => match l.0.raw_cmp(&r.0) {
                    Equal => {
                        self.next_l = None;
                        self.next_r = None;
                        // when the latest one is marked as tombstone, skip all same `raw`s
                        // NOTE: there are at most same `raw` one in delta another in sst
                        if l.1.is_tombstone() {
                            continue;
                        }
                        return Some(l);
                    }
                    Greater => {
                        self.next_r = None;
                        return Some(r);
                    }
                    Less => {
                        self.next_l = None;
                        return Some(l);
                    }
                },
            }
        }
    }
}

impl<'a, L> Iterator for RawLeafIter<'a, L>
where
    L: ILoader,
{
    type Item = IterItem<'a, L>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().expect("must exist")
    }
}

impl<'a, L> RawLeafIter<'a, L>
where
    L: ILoader,
{
    pub(crate) fn try_next(&mut self) -> Result<Option<IterItem<'a, L>>, OpCode> {
        if self.next_l.is_none()
            && let Some(x) = self.delta_iter.next()
        {
            let k = Key::decode_from(x.key());
            self.next_l = Some(IterItem::new(
                self.cached_key,
                &[],
                k,
                x.val(),
                self.sst_iter.loader,
            ));
        }

        if self.next_r.is_none()
            && let Some((k, val)) = self.sst_iter.try_next_with_sibling(|_| {})?
        {
            self.next_r = Some(IterItem::new(
                self.cached_key,
                self.prefix,
                k,
                val,
                self.sst_iter.loader,
            ));
        }

        Ok(match (self.next_l.take(), self.next_r.take()) {
            (None, None) => None,
            (None, Some(r)) => Some(r),
            (Some(l), None) => Some(l),
            (Some(l), Some(r)) => {
                let ord = if self.prefix.is_empty() {
                    l.cmp(&r)
                } else {
                    cmp_raw_with_prefixed_tail(l.base, self.prefix, r.base)
                };
                match ord {
                    Less => {
                        self.next_r = Some(r);
                        Some(l)
                    }
                    Greater => {
                        self.next_l = Some(l);
                        Some(r)
                    }
                    Equal => {
                        // old key may be updated or deleted (or both), we simply return the latest one
                        // and do visibility check outside
                        self.next_r = Some(r);
                        Some(l)
                    }
                }
            }
        })
    }
}

impl<'a, L> RawLeafRevIter<'a, L>
where
    L: ILoader,
{
    pub(crate) fn try_next_back(&mut self) -> Result<Option<IterItem<'a, L>>, OpCode> {
        if self.next_l.is_none()
            && let Some(x) = self.delta_iter.next()
        {
            let k = Key::decode_from(x.key());
            self.next_l = Some(IterItem::new(
                self.cached_key,
                &[],
                k,
                x.val(),
                self.sst_iter.loader,
            ));
        }

        if self.next_r.is_none()
            && let Some((k, val)) = self.sst_iter.try_next_back_with_sibling(|_| {})?
        {
            self.next_r = Some(IterItem::new(
                self.cached_key,
                self.prefix,
                k,
                val,
                self.sst_iter.loader,
            ));
        }

        Ok(match (self.next_l.take(), self.next_r.take()) {
            (None, None) => None,
            (None, Some(r)) => Some(r),
            (Some(l), None) => Some(l),
            (Some(l), Some(r)) => {
                let ord = if self.prefix.is_empty() {
                    l.cmp(&r)
                } else {
                    cmp_raw_with_prefixed_tail(l.base, self.prefix, r.base)
                };
                match ord {
                    Less => {
                        self.next_l = Some(l);
                        Some(r)
                    }
                    Greater => {
                        self.next_r = Some(r);
                        Some(l)
                    }
                    Equal => {
                        self.next_r = Some(r);
                        Some(l)
                    }
                }
            }
        })
    }
}

/// outcome of the operator-free streaming filter for one candidate version
enum FilterOut {
    Keep,
    /// the version is consumed; its remote payload (if any) is junked
    Drop,
}

/// baseline compaction filter for pages that cannot materialize merge operands
struct PlainLeafFilter<'a> {
    txid: u64,
    last: Option<&'a [u8]>,
    junks: &'a mut Vec<u64>,
    skip_dup: bool,
    /// a safe tombstone shadows all older versions, including raw merge operands
    drop_merges: bool,
    abort_ctx: Option<Handle<Context>>,
}

impl<'a> PlainLeafFilter<'a> {
    fn new(txid: u64, junks: &'a mut Vec<u64>, abort_ctx: Option<Handle<Context>>) -> Self {
        Self {
            txid,
            last: None,
            junks,
            skip_dup: false,
            drop_merges: false,
            abort_ctx,
        }
    }

    #[inline(always)]
    fn check(&mut self, k: &LeafSeg<'a>, v: &Val<'a>) -> bool {
        if k.txid() <= self.txid
            && self
                .abort_ctx
                .is_some_and(|ctx| ctx.group(v.group_id() as usize).is_retained_abort(k.txid()))
        {
            self.collect(v);
            return false;
        }
        if v.is_merge() {
            if self.drop_merges && self.last == Some(k.raw()) {
                self.collect(v);
                return false;
            }
            // raw operands remain available when a plain base is retained; a
            // safe tombstone is the only version barrier that shadows them
            return true;
        }
        if let Some(last) = self.last
            && last == k.raw()
        {
            if self.skip_dup {
                if v.is_tombstone() {
                    self.drop_merges = true;
                }
                self.collect(v);
                return false;
            }
            if k.txid() > self.txid {
                return true;
            }
            self.skip_dup = true;
            if v.is_tombstone() {
                self.drop_merges = true;
                self.collect(v);
                return false;
            }
            return true;
        }

        self.last = Some(k.raw());
        self.skip_dup = k.txid() <= self.txid;
        self.drop_merges = false;
        if v.is_tombstone() && self.skip_dup {
            self.drop_merges = true;
            self.collect(v);
            return false;
        }
        true
    }

    #[inline(always)]
    fn collect(&mut self, v: &Val<'a>) {
        let remote = v.get_remote();
        if remote != NULL_ADDR {
            self.junks.push(RemoteView::tag(remote));
        }
    }
}

struct LeafFilter<'a, L> {
    txid: u64,
    loader: &'a L,
    last: Option<&'a [u8]>,
    junks: &'a mut Vec<u64>,
    skip_dup: bool,
    /// when present, merge runs at/below the safe boundary materialize into one
    /// synthesized base row; when absent the chain stays raw-preserving
    operator: Option<&'a dyn crate::MergeOperator>,
    /// absorbed below-safe merge run, newest first: (version, envelope) copies
    run_ops: Vec<(LeafSeg<'a>, Val<'a>)>,
    /// pending folded outcomes, popped newest-first by the iterator
    emit: Vec<(LeafSeg<'a>, CompactVal<'a>)>,
    /// scratch for the full user raw key (prefix + base) of the folded run:
    /// LeafSeg::raw() returns the stripped tail, which would dispatch the wrong
    /// algebra on a compressed page
    fold_key: Vec<u8>,
    /// keys whose fold hit an engine-detected contract violation during this
    /// consolidation; drained by merge_to_base and handed to the bucket block set
    blocked_hits: Vec<Vec<u8>>,
    inline_size: usize,
    drop_aborted_ctx: Option<Handle<Context>>,
    /// retained-abort predicate source for materializing compaction
    abort_ctx: Option<Handle<Context>>,
    removed: bool,
}

impl<'a, L: ILoader> LeafFilter<'a, L> {
    fn new(
        txid: u64,
        loader: &'a L,
        junks: &'a mut Vec<u64>,
        op: Option<&'a dyn crate::MergeOperator>,
        a_inline_size: usize,
        abort_ctx: Option<Handle<Context>>,
    ) -> Self {
        Self {
            txid,
            loader,
            last: None,
            junks,
            skip_dup: false,
            operator: op,
            run_ops: Vec::new(),
            emit: Vec::new(),
            fold_key: Vec::new(),
            blocked_hits: Vec::new(),
            inline_size: a_inline_size,
            drop_aborted_ctx: None,
            abort_ctx,
            removed: false,
        }
    }

    fn with_drop_aborted(
        txid: u64,
        loader: &'a L,
        junks: &'a mut Vec<u64>,
        ctx: Handle<Context>,
    ) -> Self {
        Self {
            txid,
            loader,
            last: None,
            junks,
            skip_dup: false,
            operator: None,
            run_ops: Vec::new(),
            emit: Vec::new(),
            fold_key: Vec::new(),
            blocked_hits: Vec::new(),
            inline_size: 0,
            drop_aborted_ctx: Some(ctx),
            abort_ctx: None,
            removed: false,
        }
    }

    #[inline(always)]
    fn pop_emit(&mut self) -> Option<(LeafSeg<'a>, CompactVal<'a>)> {
        self.emit.pop()
    }

    /// process one complete version chain; keeping the chain local makes the fold
    /// decision a single pass over contiguous memory and delays junking merge
    /// operands until a fold has succeeded
    fn process_key(&mut self, versions: &[(LeafSeg<'a>, Val<'a>)], synth: &mut Vec<u8>) {
        let mut out = std::mem::take(&mut self.emit);
        out.clear();
        if out.capacity() < versions.len() {
            out.reserve(versions.len() - out.capacity());
        }
        let mut i = 0;
        let mut raw_fallback = false;

        while i < versions.len() {
            let (k, v) = versions[i];
            if self.is_retained_abort(&k, &v) {
                self.collect(&v);
                i += 1;
                continue;
            }

            if raw_fallback {
                out.push((k, CompactVal::Page(v)));
                i += 1;
                continue;
            }

            if !self.run_ops.is_empty() {
                if v.is_merge() && k.txid() <= self.txid {
                    self.run_ops.push((k, v));
                    i += 1;
                    continue;
                }

                if v.is_merge() {
                    out.push((k, CompactVal::Page(v)));
                    i += 1;
                    continue;
                }

                let base_rec = if v.is_tombstone() {
                    None
                } else {
                    let (rec, _r) = v.get_record(self.loader);
                    Some(rec)
                };
                let base = base_rec.as_ref().map(|rec| rec.data());
                let boundary = self.run_ops[0].0;
                match self.fold_run(self.operator.expect("run requires operator"), base, synth) {
                    Ok(blob) => {
                        out.push((
                            boundary,
                            CompactVal::Synth {
                                env: synth.clone(),
                                blob,
                            },
                        ));
                        for (_, old) in std::mem::take(&mut self.run_ops) {
                            self.collect(&old);
                        }
                        self.collect(&v);
                        for &(_, old) in &versions[i + 1..] {
                            self.collect(&old);
                        }
                        break;
                    }
                    Err(_) => {
                        out.extend(
                            self.run_ops
                                .drain(..)
                                .map(|(seg, val)| (seg, CompactVal::Page(val))),
                        );
                        out.push((k, CompactVal::Page(v)));
                        raw_fallback = true;
                        i += 1;
                    }
                }
                continue;
            }

            if v.is_merge() {
                if k.txid() <= self.txid {
                    self.run_ops.push((k, v));
                } else {
                    out.push((k, CompactVal::Page(v)));
                }
                i += 1;
                continue;
            }

            if k.txid() <= self.txid {
                if !v.is_tombstone() {
                    out.push((k, CompactVal::Page(v)));
                } else {
                    self.collect(&v);
                }
                self.collect_remaining(versions, i + 1);
                break;
            }

            out.push((k, CompactVal::Page(v)));
            i += 1;
        }

        if !self.run_ops.is_empty() {
            let boundary = self.run_ops[0].0;
            match self.fold_run(self.operator.expect("run requires operator"), None, synth) {
                Ok(blob) => {
                    out.push((
                        boundary,
                        CompactVal::Synth {
                            env: synth.clone(),
                            blob,
                        },
                    ));
                    for (_, old) in std::mem::take(&mut self.run_ops) {
                        self.collect(&old);
                    }
                }
                Err(_) => {
                    out.extend(
                        self.run_ops
                            .drain(..)
                            .map(|(seg, val)| (seg, CompactVal::Page(val))),
                    );
                }
            }
        }

        out.reverse();
        self.emit = out;
    }

    #[inline(always)]
    fn is_retained_abort(&self, k: &LeafSeg<'a>, v: &Val<'a>) -> bool {
        k.txid() <= self.txid
            && self
                .abort_ctx
                .is_some_and(|ctx| ctx.group(v.group_id() as usize).is_retained_abort(k.txid()))
    }

    fn collect_remaining(&mut self, versions: &[(LeafSeg<'a>, Val<'a>)], start: usize) {
        for &(_, v) in &versions[start..] {
            self.collect(&v);
        }
    }

    /// outcome for the operator-free streaming path
    fn check_out(&mut self, k: &LeafSeg<'a>, v: &Val<'a>) -> FilterOut {
        if let Some(ctx) = self.drop_aborted_ctx
            && ctx.group(v.group_id() as usize).is_retained_abort(k.txid())
        {
            self.removed = true;
            return FilterOut::Drop;
        }
        if self.is_retained_abort(k, v) {
            return FilterOut::Drop;
        }
        if self.operator.is_none() {
            return self.check_plain_fast(k, v);
        }
        self.plain(k, v)
    }

    #[inline(always)]
    fn check_plain_fast(&mut self, k: &LeafSeg<'a>, v: &Val<'a>) -> FilterOut {
        if v.is_merge() && self.last == Some(k.raw()) && self.skip_dup {
            FilterOut::Drop
        } else if v.is_merge() {
            FilterOut::Keep
        } else {
            self.plain(k, v)
        }
    }

    /// plain (non-merge) version handling with the existing safe-txid trimming
    fn plain(&mut self, k: &LeafSeg<'a>, v: &Val<'a>) -> FilterOut {
        if let Some(last) = self.last
            && last == k.raw()
        {
            if self.skip_dup {
                return FilterOut::Drop;
            }

            if k.txid() > self.txid {
                return FilterOut::Keep;
            }

            // it's the oldest version, the rest versions will never be accessed by any txn
            self.skip_dup = true;
            // skip only when removed and is safe
            return if v.is_tombstone() {
                FilterOut::Drop
            } else {
                FilterOut::Keep
            };
        }

        self.last = Some(k.raw());
        self.skip_dup = k.txid() <= self.txid;
        // skip only when removed and is safe
        if v.is_tombstone() && self.skip_dup {
            return FilterOut::Drop;
        }
        FilterOut::Keep
    }

    /// folds the collected run (newest first) onto an optional base and encodes the
    /// synthesized envelope into `synth` as [flags][gid][len u32][gid][payload];
    /// the row carries the safe boundary version's logical group
    fn fold_run(
        &mut self,
        op: &dyn crate::MergeOperator,
        base: Option<&[u8]>,
        synth: &mut Vec<u8>,
    ) -> Result<Option<Vec<u8>>, OpCode> {
        let (b_seg, b_val) = self.run_ops[0];
        // the stable user raw key of the folded run: every run member shares it,
        // and it must be the FULL key (prefix + base) — LeafSeg::raw() returns
        // the stripped tail, which would dispatch the wrong algebra on a
        // compressed page
        let key = b_seg.full_key(&mut self.fold_key);
        let (b_rec, _r) = b_val.get_record(self.loader);
        let gid = b_rec.group_id();
        let mut acc: Option<Vec<u8>> = None;
        for &(_, val) in self.run_ops.iter().rev() {
            let (rec, _r) = val.get_record(self.loader);
            let data = rec.data();
            acc = Some(match acc {
                None => data.to_vec(),
                Some(prev) => {
                    // the operator is total; empty or oversized combine output is
                    // an engine-detected contract violation, not an operator error
                    let combined = op.combine_operands(key, &prev, data);
                    if combined.is_empty() {
                        self.blocked_hits.push(key.to_vec());
                        return Err(OpCode::MergeContractViolation);
                    }
                    if size_of::<u8>() + combined.len() > Options::MAX_KV_SIZE {
                        self.blocked_hits.push(key.to_vec());
                        return Err(OpCode::MergeContractViolation);
                    }
                    combined
                }
            });
        }
        let acc = acc.expect("run non-empty");
        let applied = match &base {
            None => op.apply(key, None, &acc),
            Some(b) => op.apply(key, Some(b), &acc),
        };
        let (flags, data) = match applied {
            None => (Val::DEL_BIT, Vec::new()), // apply -> None: tombstone-shaped fold
            Some(v) if v.is_empty() => {
                self.blocked_hits.push(key.to_vec());
                return Err(OpCode::MergeContractViolation);
            }
            Some(v) => (0u8, v),
        };
        let payload_len = size_of::<u8>() + data.len();
        match classify_fold_size(data.len(), self.inline_size) {
            FoldClass::TooLarge => {
                // the folded base cannot be persisted: keep the chain raw-preserving
                self.blocked_hits.push(key.to_vec());
                return Err(OpCode::MergeContractViolation);
            }
            FoldClass::Blob => {
                // remote envelope [flags|REMOTE][gid][len u32][addr u64]; the
                // address slot is a provisional zero filled by the second pass
                synth.clear();
                synth.push(flags | Val::REMOTE_BIT);
                synth.push(gid);
                synth.extend_from_slice(&(payload_len as u32).to_le_bytes());
                synth.extend_from_slice(&[0u8; 8]);
                let mut blob = Vec::with_capacity(payload_len);
                blob.push(gid);
                blob.extend_from_slice(&data);
                return Ok(Some(blob));
            }
            FoldClass::Inline => {}
        }
        synth.clear();
        synth.push(flags);
        synth.push(gid);
        synth.extend_from_slice(&(payload_len as u32).to_le_bytes());
        synth.push(gid);
        synth.extend_from_slice(&data);
        Ok(None)
    }

    #[inline(always)]
    fn check(&mut self, x: &(LeafSeg<'a>, Val<'a>)) -> FilterOut {
        let (k, v) = x;
        match self.check_out(k, v) {
            FilterOut::Drop => {
                self.collect(v);
                FilterOut::Drop
            }
            other => other,
        }
    }

    fn collect(&mut self, v: &Val) {
        let remote = v.get_remote();
        if remote != NULL_ADDR {
            self.junks.push(RemoteView::tag(remote));
        }
    }
}

#[cfg(test)]
mod test {
    use parking_lot::Mutex;
    use std::{
        collections::HashMap,
        rc::Rc,
        sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
    };

    use crate::{
        BucketOptions,
        types::{
            data::{Key, LeafSeg, Record, Val, Ver},
            node::{Junk, LeafFilter, NULL_ADDR, Node},
            refbox::{BoxRef, BoxView, DeltaView},
            traits::{ICodec, IFrameAlloc, IHeader, ILoader},
        },
        utils::data::Position,
    };

    struct AInner {
        map: Mutex<HashMap<u64, BoxRef>>,
        off: AtomicU64,
        remote_loads: AtomicUsize,
    }

    #[derive(Clone)]
    struct A {
        inner: Rc<AInner>,
    }

    impl A {
        fn new() -> Self {
            Self {
                inner: Rc::new(AInner {
                    map: Mutex::new(HashMap::new()),
                    off: AtomicU64::new(0),
                    remote_loads: AtomicUsize::new(0),
                }),
            }
        }

        fn load(&self, addr: u64) -> BoxRef {
            let lk = self.inner.map.lock();
            lk.get(&addr).unwrap().clone()
        }
    }

    impl IFrameAlloc for A {
        fn alloc(&mut self, size: u32) -> BoxRef {
            let addr = self
                .inner
                .off
                .fetch_add(BoxRef::real_size(size) as u64, Relaxed);
            let p = BoxRef::alloc(size, addr);
            let mut lk = self.inner.map.lock();
            lk.insert(addr, p.clone());
            p
        }

        fn inline_size(&self) -> usize {
            BucketOptions::MIN_INLINE_SIZE
        }
    }

    impl ILoader for A {
        fn load_pinned(&self, addr: u64) -> BoxView {
            self.load(addr).view()
        }

        fn pin(&self, data: BoxRef) {
            let mut lk = self.inner.map.lock();
            lk.insert(data.header().addr, data);
        }

        fn copy(&self) -> Self {
            self.clone()
        }

        fn copy_detached(&self) -> Self {
            self.clone()
        }

        fn load_sibling(&self, addr: u64) -> BoxRef {
            self.load(addr)
        }

        fn load_blob(&self, addr: u64, _cache: bool) -> BoxRef {
            self.inner.remote_loads.fetch_add(1, Relaxed);
            self.load(addr)
        }
    }

    #[test]
    fn find_latest_meta_skips_remote_value_load() {
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        let key = Key::new("blob".as_bytes(), Ver::new(1, 1));
        let value = vec![7u8; BucketOptions::MIN_INLINE_SIZE + 16];
        let record = Record::normal(1, &value);
        let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &record, 0, Position::MIN);
        node.insert_inplace(
            delta.view().as_delta(),
            remote
                .as_ref()
                .map(|x| x.header().total_size as usize)
                .unwrap_or(0),
        );
        let _ = remote;
        node.save_delta(delta);
        let (node, _) = node.compact(&mut a, 1, None, &mut |_| {}, None);

        a.inner.remote_loads.store(0, Relaxed);
        let meta = node
            .find_latest_meta(&key)
            .expect("latest meta should exist");
        assert_eq!(meta.ver, *key.ver());
        assert_eq!(meta.group_id, 1);
        assert!(!meta.is_del);
        assert_eq!(a.inner.remote_loads.load(Relaxed), 0);

        let latest = node.find_latest(&key).expect("latest value should exist");
        assert_eq!(latest.1.data(), value.as_slice());
        assert_eq!(a.inner.remote_loads.load(Relaxed), 1);
    }

    #[test]
    fn find_latest_meta_is_independent_of_probe_txid() {
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        let raw = "race".as_bytes();

        for txid in [20u64, 10u64] {
            let key = Key::new(raw, Ver::new(txid, 1));
            let value = format!("v{txid}").into_bytes();
            let record = Record::normal(1, &value);
            let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &record, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = remote;
            node.save_delta(delta);
        }

        let probe = Key::new(raw, Ver::new(5, 1));
        let meta = node
            .find_latest_meta(&probe)
            .expect("latest meta should follow raw-key head");
        assert_eq!(meta.ver.txid, 20);
        assert!(!meta.is_del);
    }

    #[test]
    fn inline_size_boundary_is_remote() {
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        let key = Key::new("blob".as_bytes(), Ver::new(1, 1));
        let value = vec![7u8; BucketOptions::MIN_INLINE_SIZE];
        let record = Record::normal(1, &value);
        let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &record, 0, Position::MIN);
        let remote = remote.expect("equal inline boundary must allocate remote");

        node.insert_inplace(delta.view().as_delta(), remote.header().total_size as usize);
        node.save_delta(delta);
        let (node, _) = node.compact(&mut a, 1, None, &mut |_| {}, None);

        a.inner.remote_loads.store(0, Relaxed);
        let latest = node.find_latest(&key).expect("latest value should exist");
        assert_eq!(latest.1.data(), value.as_slice());
        assert_eq!(a.inner.remote_loads.load(Relaxed), 1);
    }

    #[test]
    fn leaf_iter() {
        let mut a = A::new();
        let txid = AtomicU64::new(1);
        const CONSOLIDATE_THRESHOLD: usize = 64;
        let lsn = Position::MIN;
        let mut j = Junk::new();

        {
            let l = a.clone();
            let mut node = Node::new_leaf(&mut a, l, 0, Position::MIN);

            let (d1, r1) = DeltaView::from_key_val(
                &mut a,
                &Key::new("foo".as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 1)),
                &Record::normal(1, "1".as_bytes()),
                0,
                lsn,
            );
            let (d2, r2) = DeltaView::from_key_val(
                &mut a,
                &Key::new("foo".as_bytes(), Ver::new(txid.load(Relaxed), 2)),
                &Record::normal(1, "2".as_bytes()),
                0,
                lsn,
            );

            let (d3, r3) = DeltaView::from_key_val(
                &mut a,
                &Key::new("foo".as_bytes(), Ver::new(txid.load(Relaxed), 3)),
                &Record::remove(1),
                0,
                lsn,
            );

            node.insert_inplace(
                d1.view().as_delta(),
                r1.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r1;
            node.save_delta(d1);
            node.insert_inplace(
                d2.view().as_delta(),
                r2.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r2;
            node.save_delta(d2);
            (node, _) = node.compact(&mut a, 1, None, &mut |_| {}, None);

            let mut iter = node.leaf_iter(&mut j, 1, BucketOptions::MIN_INLINE_SIZE, None, None);
            assert_eq!(drain_len(&mut iter), 2);

            node.insert_inplace(
                d3.view().as_delta(),
                r3.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r3;
            node.save_delta(d3);
            let mut iter = node.leaf_iter(&mut j, 3, BucketOptions::MIN_INLINE_SIZE, None, None);
            assert_eq!(drain_len(&mut iter), 0);
        }

        let l = a.clone();
        let mut node = Node::new_leaf(&mut a, l, 0, Position::MIN);

        for i in 0..30 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 1));
            let v = Record::normal(1, raw.as_bytes());
            let (delta, r) = DeltaView::from_key_val(&mut a, &k, &v, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                r.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r;
            node.save_delta(delta);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                (node, _) = node.compact(&mut a, 3, None, &mut |_| {}, None);
            }
        }

        // this will create siblings
        for i in 0..20 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 0));
            let v = Record::remove(1);
            let (delta, r) = DeltaView::from_key_val(&mut a, &k, &v, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                r.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r;
            node.save_delta(delta);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                (node, _) = node.compact(&mut a, 3, None, &mut |_| {}, None);
            }
        }

        // this will mix siblings and new keys
        for i in 30..31 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 0));
            let v = Record::normal(1, raw.as_bytes());
            let (delta, r) = DeltaView::from_key_val(&mut a, &k, &v, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                r.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r;
            node.save_delta(delta);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                (node, _) = node.compact(&mut a, 3, None, &mut |_| {}, None);
            }
        }

        let mut last: Option<LeafSeg<'_>> = None;
        let mut iter = node.leaf_iter(&mut j, 3, BucketOptions::MIN_INLINE_SIZE, None, None);
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        // make sure the iterator produce ascending sorted value
        while let Some((k, _)) = iter.next_compact(&mut scratch) {
            if let Some(old) = last {
                assert!(old.cmp(&k).is_lt());
            }
            last = Some(k);
        }
    }

    #[test]
    fn compact_drops_latest_tombstone_instead_of_revealing_older_value() {
        let mut a = A::new();
        let l = a.clone();
        let lsn = Position::MIN;
        let mut node = Node::new_leaf(&mut a, l, 0, lsn);
        let mut txid = 1_u64;
        let v1 = vec![b'x'; BucketOptions::MIN_INLINE_SIZE + 1024];
        let v2 = vec![b'y'; BucketOptions::MIN_INLINE_SIZE + 1024];

        for i in 0..64 {
            let raw = format!("blob_{i:04}");
            let key = Key::new(raw.as_bytes(), Ver::new(txid, 0));
            txid += 1;
            let value = Record::normal(1, &v1);
            let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &value, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = remote;
            node.save_delta(delta);
        }
        (node, _) = node.compact(&mut a, txid, None, &mut |_| {}, None);

        for i in 0..64 {
            let raw = format!("blob_{i:04}");
            let key = Key::new(raw.as_bytes(), Ver::new(txid, 0));
            let value = Record::normal(1, &v2);
            let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &value, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = remote;
            node.save_delta(delta);
            txid += 1;
        }

        for i in 0..64 {
            let raw = format!("blob_{i:04}");
            let key = Key::new(raw.as_bytes(), Ver::new(txid, 0));
            txid += 1;
            let value = Record::remove(1);
            let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &value, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = remote;
            node.save_delta(delta);
        }

        let latest_txid = txid;
        (node, _) = node.compact(&mut a, latest_txid, None, &mut |_| {}, None);
        let mut junk = Junk::new();
        assert_eq!(
            drain_len(&mut node.leaf_iter(
                &mut junk,
                latest_txid,
                BucketOptions::MIN_INLINE_SIZE,
                None,
                None
            )),
            0
        );

        let probe = Key::new("blob_0000".as_bytes(), Ver::new(latest_txid, 0));
        assert!(node.find_latest(&probe).is_none());
    }

    fn drain_len<'a, L>(iter: &mut crate::types::node::LeafIter<'a, L>) -> usize
    where
        L: crate::types::traits::ILoader,
    {
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;
        let mut n = 0;
        while iter.next_compact(&mut scratch).is_some() {
            n += 1;
        }
        n
    }

    /// a safe tombstone must shadow every older version, including merge operands
    #[test]
    fn safe_delete_shadows_stale_merges_across_compact() {
        struct AddOp;
        impl crate::MergeOperator for AddOp {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                let v = match base {
                    None => d(operand),
                    Some(b) => d(b) + d(operand),
                };
                Some(v.to_le_bytes().to_vec())
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        // chain: Merge@tx10, Merge@tx20, Put@tx30, Delete@tx40, safe = 40
        fn build(a: &mut A, node: &Node<A>) {
            push_version(a, node, 10, 1, &1u64.to_le_bytes(), true);
            push_version(a, node, 20, 1, &2u64.to_le_bytes(), true);
            push_version(a, node, 30, 0, &30u64.to_le_bytes(), false);
            push_version(a, node, 40, 0, b"", false);
        }

        fn rows(_a: &A, node: &Node<A>, op: Option<&dyn crate::MergeOperator>) -> usize {
            let mut junk = Junk::new();
            let mut iter = node.leaf_iter(&mut junk, 40, BucketOptions::MIN_INLINE_SIZE, op, None);
            let mut scratch: Vec<u8> = Vec::new();
            use crate::types::node::LeafCompactSource;
            let mut n = 0;
            while let Some((seg, _)) = iter.next_compact(&mut scratch) {
                if seg.raw() == b"k" {
                    n += 1;
                }
            }
            n
        }

        // without an operator the operands must not survive the tombstone
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        build(&mut a, &node);
        let (node, _) = node.compact(&mut a, 40, None, &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node, None),
            0,
            "no stale operand survives the tombstone"
        );

        // with an operator the fold must not resurrect a value from shadowed operands
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        build(&mut a, &node);
        let (node, _) = node.compact(&mut a, 40, Some(&AddOp), &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node, Some(&AddOp)),
            0,
            "no folded value resurrects the deleted key"
        );
    }

    /// compact with a materializing operator folds the contiguous merge run at the
    /// safe boundary into one base row carrying the boundary version's Ver and
    /// logical group (Base(10)@tx10, Merge(+1)@tx20, Merge(+2)@tx30, safe=25 -> Base(11)@tx20 + Merge(+2)@tx30)
    #[test]
    fn compact_fold_passes_full_prefixed_key_to_operator() {
        struct CaptureOp(Mutex<Option<Vec<u8>>>);
        impl crate::MergeOperator for CaptureOp {
            fn combine_operands(&self, key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                *self.0.lock() = Some(key.to_vec());
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                *self.0.lock() = Some(key.to_vec());
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                Some(
                    match base {
                        None => d(operand),
                        Some(b) => d(b) + d(operand),
                    }
                    .to_le_bytes()
                    .to_vec(),
                )
            }
        }

        fn gen_merge_val(a: &mut A, v: u64) -> Val<'static> {
            let data = v.to_le_bytes();
            let r = Record::merge(0, &data);
            let sz = Val::calc_size(false, a.inline_size(), r.packed_size());
            let mut b = a.alloc(sz as u32);
            Val::encode_inline(b.data_slice_mut::<u8>(), None, &r);
            Val::from_raw(unsafe {
                std::mem::transmute::<&[u8], &'static [u8]>(b.data_slice::<u8>())
            })
        }

        // a merge run on the FULL key "sum:a" inside a page whose prefix is
        // "sum:" (prefix_len = 4): the operator must receive the full key, not
        // the prefix-stripped tail "a"
        let mut a = A::new();
        let l = a.clone();
        let mut junks = Vec::new();
        let op = CaptureOp(Mutex::new(None));
        let mut filter = LeafFilter::new(25, &l, &mut junks, Some(&op), a.inline_size(), None);
        filter.run_ops.push((
            LeafSeg::new(b"sum:".as_slice(), b"a".as_slice(), Ver::new(20, 1)),
            gen_merge_val(&mut a, 1),
        ));
        filter.run_ops.push((
            LeafSeg::new(b"sum:".as_slice(), b"a".as_slice(), Ver::new(10, 1)),
            gen_merge_val(&mut a, 10),
        ));
        let mut synth = Vec::new();
        filter
            .fold_run(&op, None, &mut synth)
            .expect("fold must succeed");
        let seen = op.0.lock().clone().expect("operator must run");
        assert_eq!(seen, b"sum:a", "operator must receive the full user key");
    }

    #[test]
    fn compact_folds_merge_run_at_boundary_ver_and_group() {
        struct AddOp;
        impl crate::MergeOperator for AddOp {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                if d(operand) == 0 {
                    // empty apply value: an engine-detected contract violation
                    return Some(Vec::new());
                }
                let v = match base {
                    None => d(operand),
                    Some(b) => d(b) + d(operand),
                };
                Some(v.to_le_bytes().to_vec())
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, 10, 0, &10u64.to_le_bytes(), false);
        push_version(&mut a, &node, 20, 1, &1u64.to_le_bytes(), true);
        push_version(&mut a, &node, 30, 2, &2u64.to_le_bytes(), true);

        let (node, _) = node.compact(&mut a, 25, Some(&AddOp), &mut |_| {}, None);
        let mut junk = Junk::new();
        let mut iter = node.leaf_iter(
            &mut junk,
            25,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&AddOp),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        let first = iter
            .next_compact(&mut scratch)
            .expect("above-safe merge row");
        assert_eq!(first.0.ver.txid, 30, "above-safe merge keeps its own Ver");
        assert!(first.1.is_merge(), "above-safe merge stays an operand");

        let second = iter.next_compact(&mut scratch).expect("folded base row");
        assert_eq!(
            second.0.ver.txid, 20,
            "folded base carries the safe boundary version's Ver"
        );
        let (rec, _) = second.1.get_record(&l);
        assert!(!rec.is_merge(), "folded base is a plain value");
        assert_eq!(
            rec.group_id(),
            1,
            "folded base keeps the boundary version's group"
        );
        assert_eq!(rec.data(), 11u64.to_le_bytes(), "fold equals the chain sum");

        assert!(
            iter.next_compact(&mut scratch).is_none(),
            "the absorbed base is trimmed"
        );
    }

    /// a fold contract violation (empty/oversized output) or an undecodable
    /// barrier keeps every version of that key verbatim instead of dropping the
    /// absorbed operands
    #[test]
    fn compact_empty_fold_output_stays_raw_preserving() {
        struct EmptyOnZero;
        impl crate::MergeOperator for EmptyOnZero {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                if d(left) == 0 || d(right) == 0 {
                    // empty combine output: an engine-detected contract violation
                    return Vec::new();
                }
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                if d(operand) == 0 {
                    // empty apply value: an engine-detected contract violation
                    return Some(Vec::new());
                }
                let v = match base {
                    None => d(operand),
                    Some(b) => d(b) + d(operand),
                };
                Some(v.to_le_bytes().to_vec())
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        // the zero operand makes the fold fail; every version must survive
        push_version(&mut a, &node, 10, 0, &10u64.to_le_bytes(), false);
        push_version(&mut a, &node, 20, 1, &0u64.to_le_bytes(), true);
        push_version(&mut a, &node, 30, 1, &2u64.to_le_bytes(), true);

        let (node, _) = node.compact(&mut a, 25, Some(&EmptyOnZero), &mut |_| {}, None);
        let mut junk = Junk::new();
        let mut iter = node.leaf_iter(
            &mut junk,
            25,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&EmptyOnZero),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        // all three versions survive the failed fold, newest first
        let first = iter
            .next_compact(&mut scratch)
            .expect("merge@tx30 survives");
        assert!(first.1.is_merge());
        let second = iter
            .next_compact(&mut scratch)
            .expect("merge@tx20 survives");
        assert!(second.1.is_merge());
        let third = iter.next_compact(&mut scratch).expect("base@tx10 survives");
        let (rec, _) = third.1.get_record(&l);
        assert!(!rec.is_merge());
        assert_eq!(rec.data(), 10u64.to_le_bytes());
        assert!(iter.next_compact(&mut scratch).is_none());
    }

    /// a tombstone closes the fold run: apply(None, combined) may produce a value
    /// or a tombstone-shaped row (DEL bit), never an empty plain value
    #[test]
    fn compact_folds_merge_run_onto_tombstone_barrier() {
        struct ApplyNoneAdds;
        impl crate::MergeOperator for ApplyNoneAdds {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                match base {
                    // deleting + operand keeps the key present
                    None => Some(d(operand).to_le_bytes().to_vec()),
                    Some(b) => Some((d(b) + d(operand)).to_le_bytes().to_vec()),
                }
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        // tombstone@tx10, Merge(+5)@tx20, Merge(+7)@tx30, safe=25
        push_version(&mut a, &node, 10, 0, b"", false);
        push_version(&mut a, &node, 20, 1, &5u64.to_le_bytes(), true);
        push_version(&mut a, &node, 30, 2, &7u64.to_le_bytes(), true);

        let (node, _) = node.compact(&mut a, 25, Some(&ApplyNoneAdds), &mut |_| {}, None);
        let mut junk = Junk::new();
        let mut iter = node.leaf_iter(
            &mut junk,
            25,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&ApplyNoneAdds),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        let first = iter.next_compact(&mut scratch).expect("above-safe merge");
        assert!(first.1.is_merge());
        let second = iter.next_compact(&mut scratch).expect("folded row");
        assert_eq!(second.0.ver.txid, 20, "boundary version's Ver");
        let (rec, _) = second.1.get_record(&l);
        assert_eq!(rec.group_id(), 1, "boundary version's group");
        assert_eq!(rec.data(), 5u64.to_le_bytes(), "apply(None, +5)");
        assert!(iter.next_compact(&mut scratch).is_none());
    }

    /// a raw-preserving chain behind a history pointer must be folded exactly once
    #[test]
    fn compact_folds_hist_backed_chain_exactly_once() {
        struct AddOp;
        impl crate::MergeOperator for AddOp {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                if d(operand) == 0 {
                    // empty apply value: an engine-detected contract violation
                    return Some(Vec::new());
                }
                let v = match base {
                    None => d(operand),
                    Some(b) => d(b) + d(operand),
                };
                Some(v.to_le_bytes().to_vec())
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        fn rows(a: &A, node: &Node<A>) -> Vec<(u64, bool, u64)> {
            let mut junk = Junk::new();
            let mut iter = node.leaf_iter(
                &mut junk,
                25,
                BucketOptions::MIN_INLINE_SIZE,
                Some(&AddOp),
                None,
            );
            let mut scratch: Vec<u8> = Vec::new();
            use crate::types::node::LeafCompactSource;
            let mut out = Vec::new();
            while let Some((seg, cv)) = iter.next_compact(&mut scratch) {
                let (rec, _) = cv.get_record(a);
                out.push((
                    seg.ver.txid,
                    rec.is_merge(),
                    u64::from_le_bytes(rec.data().try_into().unwrap()),
                ));
            }
            out
        }

        // probe 1: base10@tx10, m+1@tx15, m+2@tx20 -> region [m15, base10] behind m20
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, 10, 0, &10u64.to_le_bytes(), false);
        push_version(&mut a, &node, 15, 1, &1u64.to_le_bytes(), true);
        push_version(&mut a, &node, 20, 1, &2u64.to_le_bytes(), true);
        // raw-preserving consolidation regions the chain
        let (node, _) = node.compact(&mut a, 25, None, &mut |_| {}, None);
        // materializing consolidation folds it exactly once
        let (node, _) = node.compact(&mut a, 25, Some(&AddOp), &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node),
            vec![(20, false, 13)],
            "one folded row at the boundary Ver with the full chain sum"
        );

        // probe 2: a delta merge newer than the head joins the same fold run
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, 10, 0, &10u64.to_le_bytes(), false);
        push_version(&mut a, &node, 15, 1, &1u64.to_le_bytes(), true);
        push_version(&mut a, &node, 20, 1, &2u64.to_le_bytes(), true);
        let (node, _) = node.compact(&mut a, 25, None, &mut |_| {}, None);
        push_version(&mut a, &node, 22, 2, &5u64.to_le_bytes(), true);
        let (node, _) = node.compact(&mut a, 25, Some(&AddOp), &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node),
            vec![(22, false, 18)],
            "delta merge above the head folds in; boundary Ver is the newest run member"
        );
    }

    /// a blob-backed folded history row must not carry a dangling address
    #[test]
    fn blob_fold_as_history_sibling_materializes_remote() {
        struct ConcatOp;
        impl crate::MergeOperator for ConcatOp {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let mut out = left.to_vec();
                out.extend_from_slice(right);
                out
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let mut out = match base {
                    None => Vec::new(),
                    Some(b) => b.to_vec(),
                };
                out.extend_from_slice(operand);
                Some(out)
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        // base 3000B @ tx10, merge 1500B @ tx20 -> fold 4500B > 4096 inline -> blob;
        // merge @ tx30 sits above safe=25 and stays as the head with the folded
        // blob row behind it as a history sibling
        push_version(&mut a, &node, 10, 0, &vec![b'a'; 3000], false);
        push_version(&mut a, &node, 20, 1, &vec![b'b'; 1500], true);
        push_version(&mut a, &node, 30, 1, &[b'c'; 100], true);

        let (node, _) = node.compact(&mut a, 25, Some(&ConcatOp), &mut |_| {}, None);
        let mut junk = Junk::new();
        let mut iter = node.leaf_iter(
            &mut junk,
            25,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&ConcatOp),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        let mut rows = Vec::new();
        while let Some((seg, cv)) = iter.next_compact(&mut scratch) {
            let (rec, _) = cv.get_record(&l);
            rows.push((
                seg.ver.txid,
                cv.is_merge(),
                cv.get_remote(),
                rec.data().len(),
            ));
        }
        // merge@tx30 head + the blob-folded sibling; no dangling address anywhere
        assert_eq!(rows.len(), 2, "head plus one folded blob sibling");
        assert!(rows[0].1, "head stays an above-safe merge");
        let folded = rows
            .iter()
            .find(|(_, is_merge, _, _)| !is_merge)
            .expect("folded row");
        assert!(
            folded.2 != NULL_ADDR,
            "the blob sibling must carry a real remote address, got 0x{:x}",
            folded.2
        );
        assert_eq!(folded.3, 4500, "folded value survives the blob round trip");
    }

    /// the shared folded-size classification pins the inline/blob/too-large
    /// boundaries exactly (record length = gid byte + data)
    #[test]
    fn classify_fold_size_boundaries() {
        use crate::Options;
        use crate::types::node::{FoldClass, classify_fold_size};
        let inline = 64usize;
        // record_len = 1 + len; inline strictly below inline_size
        assert!(matches!(classify_fold_size(62, inline), FoldClass::Inline));
        // record_len == inline_size is the blob boundary (mirrors writer is_remote)
        assert!(matches!(classify_fold_size(63, inline), FoldClass::Blob));
        assert!(matches!(
            classify_fold_size(inline * 4, inline),
            FoldClass::Blob
        ));
        // up to and including MAX_KV_SIZE total record length is blob-able
        assert!(matches!(
            classify_fold_size(Options::MAX_KV_SIZE - 1, inline),
            FoldClass::Blob
        ));
        // one byte past MAX_KV_SIZE is too large
        assert!(matches!(
            classify_fold_size(Options::MAX_KV_SIZE, inline),
            FoldClass::TooLarge
        ));
    }

    /// a merges-only chain (no plain/tombstone base) folds with base None at the
    /// end of its buffered key; both key-change and walk-end cases use the same
    /// per-key processing path
    #[test]
    fn compact_folds_merges_only_chain_without_base() {
        struct AddOp;
        impl crate::MergeOperator for AddOp {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                if d(operand) == 0 {
                    // empty apply value: an engine-detected contract violation
                    return Some(Vec::new());
                }
                let v = match base {
                    None => d(operand),
                    Some(b) => d(b) + d(operand),
                };
                Some(v.to_le_bytes().to_vec())
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, key: &[u8], txid: u64, gid: u8, data: &[u8]) {
            let key = Key::new(key, Ver::new(txid, 1));
            let rec = Record::merge(gid, data);
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        fn rows(a: &A, node: &Node<A>) -> Vec<(u64, u64)> {
            let mut junk = Junk::new();
            let mut iter = node.leaf_iter(
                &mut junk,
                25,
                BucketOptions::MIN_INLINE_SIZE,
                Some(&AddOp),
                None,
            );
            let mut scratch: Vec<u8> = Vec::new();
            use crate::types::node::LeafCompactSource;
            let mut out = Vec::new();
            while let Some((seg, cv)) = iter.next_compact(&mut scratch) {
                let (rec, _) = cv.get_record(a);
                out.push((
                    seg.ver.txid,
                    u64::from_le_bytes(rec.data().try_into().unwrap()),
                ));
            }
            out
        }

        // first key: m+1@tx15, m+2@tx20 then a second key; the key buffer closes
        // on the key change with base None -> apply(None, 1+2) = 3 @ tx20 (boundary)
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, b"a", 15, 1, &1u64.to_le_bytes());
        push_version(&mut a, &node, b"a", 20, 1, &2u64.to_le_bytes());
        push_version(&mut a, &node, b"b", 21, 1, &9u64.to_le_bytes());
        let (node, _) = node.compact(&mut a, 25, Some(&AddOp), &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node),
            vec![(20, 3), (21, 9)],
            "key-change close folds with base None at the boundary Ver"
        );

        // last key: the run reaches the end of the walk and folds in the same path
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, b"a", 21, 1, &9u64.to_le_bytes());
        push_version(&mut a, &node, b"k", 15, 1, &1u64.to_le_bytes());
        push_version(&mut a, &node, b"k", 20, 1, &2u64.to_le_bytes());
        let (node, _) = node.compact(&mut a, 25, Some(&AddOp), &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node),
            vec![(21, 9), (20, 3)],
            "walk-end close folds with base None at the boundary Ver"
        );
    }

    /// a fold result beyond the inline boundary uses the blob path; results beyond
    /// MAX_KV_SIZE remain raw
    #[test]
    fn compact_folds_oversized_result_into_blob() {
        struct BigFold;
        impl crate::MergeOperator for BigFold {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let mut out = left.to_vec();
                out.extend_from_slice(right);
                out
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let mut out = match base {
                    None => Vec::new(),
                    Some(b) => b.to_vec(),
                };
                out.extend_from_slice(operand);
                Some(out)
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        let big = vec![b'x'; BucketOptions::MIN_INLINE_SIZE];
        push_version(&mut a, &node, 10, 0, &big, false);
        push_version(&mut a, &node, 20, 1, &1u64.to_le_bytes(), true);

        let (node, _) = node.compact(&mut a, 25, Some(&BigFold), &mut |_| {}, None);
        let mut junk = Junk::new();
        let mut iter = node.leaf_iter(
            &mut junk,
            25,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&BigFold),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        // the oversized fold result materializes through the remote/blob path:
        // one blob-backed base row whose record re-reads through the loader
        let row = iter.next_compact(&mut scratch).expect("folded blob row");
        assert_eq!(row.0.ver.txid, 20, "boundary version's Ver");
        assert!(!row.1.is_merge());
        assert!(
            row.1.is_remote(),
            "oversized fold must persist as a blob row"
        );
        let (rec, _) = row.1.get_record(&l);
        let mut expect = big.clone();
        expect.extend_from_slice(&1u64.to_le_bytes());
        assert_eq!(
            rec.data(),
            expect.as_slice(),
            "folded value survives blob round trip"
        );
        assert!(iter.next_compact(&mut scratch).is_none());
    }

    /// a fold whose apply returns None must persist a DEL-bit tombstone row
    #[test]
    fn compact_fold_result_can_be_tombstone_shaped() {
        struct DeleteOnFold;
        impl crate::MergeOperator for DeleteOnFold {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                match base {
                    // folding onto an existing base deletes the key
                    Some(_) => None,
                    None => Some(d(operand).to_le_bytes().to_vec()),
                }
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, 10, 0, &10u64.to_le_bytes(), false);
        push_version(&mut a, &node, 20, 1, &1u64.to_le_bytes(), true);

        let (node, _) = node.compact(&mut a, 25, Some(&DeleteOnFold), &mut |_| {}, None);
        let mut junk = Junk::new();
        // iterate below the boundary row's txid so the below-safe tombstone trim
        // (correct for reads) does not hide the row we want to inspect
        let mut iter = node.leaf_iter(
            &mut junk,
            19,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&DeleteOnFold),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        let row = iter.next_compact(&mut scratch).expect("folded row");
        assert_eq!(row.0.ver.txid, 20, "boundary version's Ver");
        assert!(
            row.1.is_tombstone(),
            "apply -> None must persist a DEL-bit tombstone row"
        );
        assert!(!row.1.is_merge());
        let (rec, _) = row.1.get_record(&l);
        assert!(rec.data().is_empty(), "the re-read record is a tombstone");
        assert!(rec.data().is_empty());
        assert!(iter.next_compact(&mut scratch).is_none());
    }
}
