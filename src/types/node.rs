use parking_lot::{Mutex, MutexGuard, RwLock};
use std::{
    cmp::Ordering::{self, Equal, Greater, Less},
    ops::{Bound, Deref, DerefMut},
    sync::Arc,
};

use crate::{
    types::{
        base::BaseIter,
        data::{Index, IntlKey, IntlSeg, IterItem, Key, LeafSeg, Record, Val, Ver},
        header::{BoxHeader, NodeType},
        imtree::{ImTree, Iter, RangeIter},
        refbox::{BaseView, BoxView, DeltaView, RemoteView},
        traits::{IAlloc, IAsBoxRef, IBoxHeader, IDecode, IHeader, IKey, ILoader, IVal},
    },
    utils::{Handle, NULL_ADDR, NULL_CMD, NULL_ORACLE, NULL_PID},
};

use super::{header::TagKind, refbox::BoxRef};

pub(crate) struct NodeState {
    addr: u64,
    total_size: usize,
    max_txid: u64,
    delta: ImTree<DeltaView>,
}

pub(crate) struct Node<L: ILoader> {
    /// the loader is remote/sibling loader, not node loader
    pub(crate) loader: L,
    mtx: Arc<Mutex<()>>,
    pub(crate) state: RwLock<NodeState>,
    inner: BaseView,
}

fn intl_cmp(x: &DeltaView, y: &DeltaView) -> Ordering {
    let l = IntlKey::decode_from(x.key());
    let r = IntlKey::decode_from(y.key());

    // for internal nodes, we never use the txid for insert
    l.raw.cmp(r.raw)
}

fn leaf_cmp(x: &DeltaView, y: &DeltaView) -> Ordering {
    let l = Key::decode_from(x.key());
    let r = Key::decode_from(y.key());
    l.cmp(&r)
}

fn null_cmp(_x: &DeltaView, _y: &DeltaView) -> Ordering {
    unimplemented!()
}

pub type Junks = Vec<u64>;

pub(crate) enum MergeOp {
    Merged,
    MarkChild,
    MarkParent(u64),
}

impl<L> Node<L>
where
    L: ILoader,
{
    fn new(loader: L, b: BoxRef) -> Self {
        Self::new_with_mtx(loader, b, Arc::new(Mutex::new(())))
    }

    fn new_with_mtx(loader: L, b: BoxRef, mtx: Arc<Mutex<()>>) -> Self {
        let h = b.header();
        let (addr, total_size, max_txid) = (h.addr, h.total_size as usize, h.txid);
        let base = b.view().as_base();
        loader.pin(b);
        Self {
            loader,
            mtx,
            state: RwLock::new(NodeState {
                addr,
                total_size,
                max_txid,
                delta: ImTree::new(if base.header().is_index {
                    intl_cmp
                } else {
                    leaf_cmp
                }),
            }),
            inner: base,
        }
    }

    /// a DeltaView's owner
    fn save(&self, b: BoxRef, r: Option<BoxRef>) {
        self.loader.pin(b);
        if let Some(x) = r {
            self.loader.pin(x);
        }
    }

    pub(crate) fn reference(&self) -> Self {
        let state = self.state.read();
        Self {
            loader: self.loader.shallow_copy(),
            mtx: self.mtx.clone(),
            state: RwLock::new(NodeState {
                addr: state.addr,
                total_size: state.total_size,
                max_txid: state.max_txid,
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

    pub(crate) fn new_leaf<A: IAlloc>(a: &mut A, loader: L) -> Node<L> {
        let empty: &[(LeafSeg, Val)] = &[];
        let mut iter = empty.iter().map(|x| (x.0, x.1));
        let b = BaseView::new_leaf(a, &loader, [].as_slice(), None, NULL_PID, &mut iter, 0);
        Self::new(loader, b)
    }

    pub(crate) fn new_root<A: IAlloc>(a: &mut A, loader: L, item: &[(IntlKey, Index)]) -> Node<L> {
        let b = BaseView::new_intl(
            a,
            [].as_slice(),
            None,
            NULL_PID,
            || item.iter().map(|&(x, y)| (IntlSeg::new(&[], x.raw), y)),
            0,
        );
        Self::new(loader, b)
    }

    /// the length of delta + base
    pub(crate) fn size(&self) -> usize {
        self.state.read().total_size
    }

    pub(crate) fn base_addr(&self) -> u64 {
        self.inner.box_header().addr
    }

    pub(crate) fn base_box(&self) -> BoxRef {
        self.inner.as_box()
    }

    // TODO: use iter will introduce Clone, which is not needed and will cause performace degradation
    pub(crate) fn garbage_collect<A: IAlloc>(&self, a: &mut A, junks: &[u64]) {
        // collect key-value addr, if value is remote and invisible, it has been collected in junks
        self.state
            .read()
            .delta
            .iter()
            .for_each(|x| a.collect(&[x.box_header().addr]));

        a.collect(junks);
        a.collect(&[self.base_addr()]);
    }

    pub(crate) fn load(addr: u64, loader: L) -> Option<Self> {
        let d = loader.load(addr)?;
        let mut l = Self {
            loader,
            mtx: Arc::new(Mutex::new(())),
            state: RwLock::new(NodeState {
                addr: d.addr,
                total_size: d.total_size as usize,
                max_txid: d.header().txid,
                delta: ImTree::new(null_cmp),
            }),
            inner: BaseView::null(),
        };
        Self::load_inner(&mut l, d)?;
        Some(l)
    }

    fn set_comparator(&mut self, nt: NodeType) {
        let mut state = self.state.write();
        if nt == NodeType::Intl {
            state.delta.set_comparator(intl_cmp);
        } else {
            state.delta.set_comparator(leaf_cmp);
        }
    }

    pub(crate) fn should_split(&self, split_elem: u16) -> bool {
        let h = self.header();
        let size_limited = h.elems >= split_elem;
        let no_conflict = !h.merging && h.merging_child == NULL_PID && h.elems >= 2;

        size_limited && no_conflict
    }

    pub(crate) fn should_merge(&self) -> bool {
        let h = self.header();
        // current elems is less or equal than original elems
        let size_limited = h.split_elems >= h.elems * 4;
        let no_conflict = !h.merging && h.merging_child == NULL_PID;
        size_limited && no_conflict
    }

    pub(crate) fn can_merge_child(&self, child_pid: u64) -> bool {
        debug_assert_eq!(self.box_header().node_type, NodeType::Intl);
        let h = self.header();
        // TODO: inefficient
        h.merging_child == NULL_PID
            && !h.merging
            && self.intl_iter().any(|(_, idx)| idx.pid == child_pid)
    }

    pub(crate) fn merge_node<A: IAlloc>(
        &self,
        a: &mut A,
        other: &Node<L>,
        safe_txid: u64,
    ) -> (Node<L>, Junks, Junks) {
        let (lb, lj) = self.merge_to_base(a, safe_txid);
        let (rb, rj) = other.merge_to_base(a, safe_txid);
        let (lhs, rhs) = (lb.view().as_base(), rb.view().as_base());

        #[cfg(feature = "extra_check")]
        assert_ne!(self.base_addr(), other.base_addr());
        let mut node = Self::new(
            self.loader.clone(),
            lhs.merge(a, &self.loader, rhs, safe_txid),
        );
        node.header_mut().split_elems = self.header().split_elems;
        (node, lj, rj)
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
        let sst = self.sst::<IntlKey>();
        let pos = match sst.search_by(&IntlKey::new(k), |x, y| x.raw.cmp(y.raw)) {
            Ok(pos) => pos,
            Err(pos) => pos.max(1) - 1,
        };

        let (_, v) = sst.kv_at::<Index>(pos);
        (pos == 0, v.pid)
    }

    /// NOTE: before we add lock, it will search key in current node and return None if find, after
    /// add lock, the search is useless, so it was removed, but we keep return an Option<Node<L>>
    pub(crate) fn insert_index<A: IAlloc>(
        &self,
        a: &mut A,
        key: &[u8],
        pid: u64,
        safe_txid: u64,
    ) -> Option<(Node<L>, Junks)> {
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

        let b = DeltaView::from_key_index(a, IntlKey::new(key), Index::new(pid), safe_txid);
        let view = b.view().as_delta();
        Some(self.insert(view).compact(a, safe_txid)) // 1/SPLIT_ELEMS chance to run
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
    pub(crate) fn split<A: IAlloc>(&self, a: &mut A) -> (Node<L>, Node<L>) {
        let h = self.inner.header();
        let prefix_len = h.prefix_len as usize;
        let elems = h.elems as usize;
        let sep = elems / 2;
        let lo = self.lo();
        let hi = self.hi();
        // both lhs and rhs node are set to current's sibling, update lhs' after rhs being mapped
        let sibling = self.header().right_sibling;
        let txid = self.box_header().txid;

        let (l, r) = if h.is_index {
            let (sep_key, (llen, rlen)) = self.decode_pefix::<IntlKey>(sep, prefix_len, lo, &hi);
            let lhs_prefix = &lo[..llen];
            let rhs_prefix = &sep_key[..rlen];
            // NOTE: the prefix will never shrink in split
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
                ),
            )
        } else {
            let (sep_key, (llen, rlen)) = self.decode_pefix::<Key>(sep, prefix_len, lo, &hi);
            let lhs_prefix = &lo[..llen];
            // NOTE: the prefix will never shrink in split
            let (ld, rd) = (lhs_prefix.len() - prefix_len, rlen - prefix_len);
            let mut liter = self
                .inner
                .range_iter::<L, Key>(&self.loader, 0, sep)
                .map(|(k, v)| (LeafSeg::new(lhs_prefix, &k.raw[ld..], k.ver), v));
            let mut riter = self
                .inner
                .range_iter::<L, Key>(&self.loader, sep, elems)
                .map(|(k, v)| (LeafSeg::new(&sep_key[..rlen], &k.raw[rd..], k.ver), v));
            (
                BaseView::new_leaf(
                    a,
                    &self.loader,
                    lo,
                    Some(sep_key.as_slice()),
                    sibling,
                    &mut liter,
                    txid,
                ),
                BaseView::new_leaf(
                    a,
                    &self.loader,
                    sep_key.as_slice(),
                    hi,
                    sibling,
                    &mut riter,
                    txid,
                ),
            )
        };

        let (mut lhs, mut rhs) = (
            Self::new(self.loader.clone(), l),
            Self::new(self.loader.clone(), r),
        );
        lhs.header_mut().split_elems = sep as u16;
        rhs.header_mut().split_elems = (elems - sep) as u16;
        (lhs, rhs)
    }

    #[allow(clippy::iter_skip_zero)]
    pub(crate) fn intl_iter(&'_ self) -> IntlIter<'_, L> {
        debug_assert_eq!(self.box_header().node_type, NodeType::Intl);
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
    fn leaf_iter(&'_ self, safe_txid: u64) -> LeafIter<'_, L> {
        debug_assert_eq!(self.box_header().node_type, NodeType::Leaf);
        let len = self.header().prefix_len as usize;
        let lo = self.lo();
        LeafIter {
            prefix: &lo[..len],
            next_l: None,
            next_r: None,
            sst_iter: self
                .inner
                .range_iter(&self.loader, 0, self.inner.header().elems as usize),
            delta_iter: IterAdaptor::Iter(self.state.read().delta.iter().skip(0)),
            filter: LeafFilter {
                txid: safe_txid,
                last: None,
                junks: Vec::new(),
                skip_dup: false,
            },
        }
    }

    pub(crate) fn compact<A: IAlloc>(&self, a: &mut A, safe_txid: u64) -> (Node<L>, Junks) {
        let (b, j) = self.merge_to_base(a, safe_txid);
        let old_h = self.header();
        let mut base = b.view().as_base();
        let new_h = base.header_mut();
        new_h.merging = old_h.merging;
        new_h.merging_child = old_h.merging_child;
        (Self::new(self.loader.clone(), b), j)
    }

    fn merge_to_base<A: IAlloc>(&self, a: &mut A, safe_txid: u64) -> (BoxRef, Junks) {
        let h = self.header();
        let lo = self.lo();
        let hi = self.hi();

        if h.is_index {
            (
                BaseView::new_intl(a, lo, hi, h.right_sibling, || self.intl_iter(), safe_txid),
                Vec::new(),
            )
        } else {
            let mut iter = self.leaf_iter(safe_txid);
            let b = BaseView::new_leaf(
                a,
                &self.loader,
                lo,
                hi,
                h.right_sibling,
                &mut iter,
                safe_txid,
            );
            (b, iter.filter.junks)
        }
    }

    pub(crate) fn process_merge<A: IAlloc>(
        &self,
        a: &mut A,
        op: MergeOp,
        safe_txid: u64,
    ) -> (Node<L>, Junks) {
        match op {
            MergeOp::Merged => {
                assert_eq!(self.box_header().node_type, NodeType::Intl);
                let mut key = None;
                let h = self.header();
                let merging_child = h.merging_child;
                for (k, v) in self.intl_iter() {
                    if v.pid == merging_child {
                        key = Some(k);
                        break;
                    }
                }
                let b = DeltaView::from_key_index(a, key.unwrap(), Index::null(), safe_txid);
                let tmp = self.insert(b.view().as_delta());
                let (mut p, j) = tmp.compact(a, safe_txid);
                p.header_mut().merging_child = NULL_PID;
                p.header_mut().split_elems = h.split_elems + 1;
                (p, j)
            }
            MergeOp::MarkParent(pid) => {
                assert_eq!(self.box_header().node_type, NodeType::Intl);
                let (mut p, j) = self.compact(a, safe_txid);
                p.header_mut().merging_child = pid;
                (p, j)
            }
            MergeOp::MarkChild => {
                assert_eq!(self.box_header().node_type, NodeType::Leaf);
                let (mut p, j) = self.compact(a, safe_txid);
                p.header_mut().merging = true;
                (p, j)
            }
        }
    }

    pub(crate) fn insert(&self, mut k: DeltaView) -> Node<L> {
        let h = k.box_header_mut();
        let th = self.box_header();
        let state = self.state.read();
        // link to old address
        h.link = state.addr;
        h.node_type = th.node_type;
        h.pid = th.pid;

        Node {
            loader: self.loader.shallow_copy(),
            mtx: self.mtx.clone(),
            state: RwLock::new(NodeState {
                addr: h.addr,
                total_size: state.total_size + h.total_size as usize,
                max_txid: h.txid,
                delta: state.delta.update(k),
            }),
            inner: self.inner,
        }
    }

    // NOTE: it must be protected by lock
    fn insert_inplace(&self, mut k: DeltaView) {
        let h = k.box_header_mut();
        let th = self.box_header();

        let mut state = self.state.write();
        h.link = state.addr;
        h.node_type = th.node_type;
        h.pid = th.pid;

        state.addr = h.addr;
        state.total_size += h.total_size as usize;
        state.max_txid = h.txid;
        state.delta.put(k);
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

    pub(crate) fn has_garbage(&self, safe_txid: u64) -> bool {
        if self.header().is_index {
            return false;
        }

        let state = self.state.read();
        // if the latest modification (delta or base) is stable,
        // and base has explicitly marked garbage, then it is a candidate for compaction
        if state.max_txid <= safe_txid {
            return self.inner.header().has_multiple_versions;
        }

        false
    }

    /// when value is inlined return node (or delta) or else retuen remote, the node (or delta) is
    /// always valid when node is valid, the returned key is valid too
    pub(crate) fn find_latest<K>(&self, key: &K) -> Option<(K, Record, BoxRef)>
    where
        K: IKey,
    {
        debug_assert!(!self.inner.header().is_index);
        let mut result = None;
        self.visit_versions(
            *key,
            |x, y| K::decode_from(x.key()).cmp(y),
            |x| {
                let k = K::decode_from(x.key());
                if k.raw() == key.raw() {
                    let v = x.val();
                    let (v, r) = v.get_record(&self.loader, true);
                    result = Some((k, v, r.unwrap_or_else(|| self.base_box())));
                    return true;
                }
                false
            },
        );

        if let Some(res) = result {
            return Some(res);
        }

        self.search_sst(key).map(|(k, v)| {
            let (v, r) = v.get_record(&self.loader, true);
            (k, v, r.unwrap_or_else(|| self.base_box()))
        })
    }

    pub(crate) fn search_sst<'a, K: IKey>(&self, key: &K) -> Option<(K, Val<'a>)> {
        debug_assert_eq!(self.box_header().node_type, NodeType::Leaf);
        if self.header().elems > 0 {
            let sst = self.inner.sst::<K>();
            let pos = sst.search_by(key, |x, y| x.raw().cmp(y.raw())).ok()?;
            Some(sst.kv_at(pos))
        } else {
            // it not just ROOT_PID may have empty elems, it's same to those nodes have been compacted
            // with all elems were filtered out
            None
        }
    }

    pub(crate) fn visit_versions<K, F>(&self, key: K, cmp: fn(&DeltaView, &K) -> Ordering, mut f: F)
    where
        K: IKey,
        F: FnMut(DeltaView) -> bool,
    {
        self.state.read().delta.visit_from(&key, cmp, &mut f);
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
                let (r, _) = val.get_record(&self.loader, true);
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

    fn load_inner(l: &mut Node<L>, mut d: BoxView) -> Option<()> {
        let mut one_base = true;
        let mut last_type = None;

        loop {
            let h = d.header();
            let state = l.state.get_mut();
            state.total_size += d.total_size as usize;
            state.max_txid = state.max_txid.max(h.txid);
            if let Some(t) = last_type {
                assert_eq!(t, h.node_type);
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
                    assert!(one_base);
                    one_base = false;
                    l.inner = d.as_base();
                }
                _ => unreachable!("bad kind {:?}", h.kind),
            }
            if d.link == NULL_ADDR {
                break;
            }
            d = l.loader.load(d.link)?;
        }
        assert!(!l.inner.is_null());
        Some(())
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
                    Ok(x) => x + 1, // exclude
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
}

pub(crate) struct NodeGuard<'a, L: ILoader> {
    _guard: MutexGuard<'a, ()>,
    node: &'a Node<L>,
}

impl<L> NodeGuard<'_, L>
where
    L: ILoader,
{
    pub(crate) fn insert(&self, k: BoxRef, v: Option<BoxRef>) {
        self.node.insert_inplace(k.view().as_delta());
        self.node.save(k, v); // save the delta itself until page is reclaimed
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

pub(crate) struct LeafIter<'a, L>
where
    L: ILoader,
{
    prefix: &'a [u8],
    next_l: Option<(LeafSeg<'a>, Val<'a>)>,
    next_r: Option<(LeafSeg<'a>, Val<'a>)>,
    sst_iter: BaseIter<'a, L, Key<'a>>,
    delta_iter: IterAdaptor<'a, &'a [u8]>,
    filter: LeafFilter<'a>,
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
                    debug_assert!(!x.1.is_tombstone());
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

impl<'a, L> Iterator for LeafIter<'a, L>
where
    L: ILoader,
{
    type Item = (LeafSeg<'a>, Val<'a>);

    // TODO: `self.next_l/r = Some(xx)` cause too many l1d-load-misses, maybe we can return Key
    // instead, and apply prefix then remove new prefix in `Base::new_leaf`
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.next_r.is_none()
                && let Some((k, v)) = self.sst_iter.next()
            {
                if let Some(sib) = v.get_sibling() {
                    self.filter.junks.push(sib);
                }
                self.next_r = Some((LeafSeg::new(self.prefix, k.raw, k.ver), v));
            }

            if self.next_l.is_none()
                && let Some(x) = self.delta_iter.next()
            {
                let k = Key::decode_from(x.key());
                let v = x.val();
                self.next_l = Some((
                    LeafSeg::new(self.prefix, &k.raw[self.prefix.len()..], k.ver),
                    v,
                ));
            }

            match (self.next_l, self.next_r) {
                (None, None) => return None,
                (None, Some(r)) => {
                    self.next_r = None;
                    if self.filter.check(&(r.0, r.1)) {
                        return Some(r);
                    }
                }
                (Some(l), None) => {
                    self.next_l = None;
                    if self.filter.check(&(l.0, l.1)) {
                        return Some(l);
                    }
                }
                (Some(l), Some(r)) => match l.0.cmp(&r.0) {
                    Less => {
                        self.next_l = None;
                        if self.filter.check(&(l.0, l.1)) {
                            return Some(l);
                        }
                    }
                    Greater => {
                        self.next_r = None;
                        if self.filter.check(&(r.0, r.1)) {
                            return Some(r);
                        }
                    }
                    // because txid is monotonically increasing, two `Key`s will never be equal
                    Equal => unreachable!("never happen"),
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
            && let Some((k, val)) = self.sst_iter.next()
        {
            self.next_r = Some(IterItem::new(
                self.cached_key,
                self.prefix,
                k,
                val,
                self.sst_iter.loader,
            ));
        }

        return match (self.next_l.take(), self.next_r.take()) {
            (None, None) => None,
            (None, Some(r)) => Some(r),
            (Some(l), None) => Some(l),
            (Some(l), Some(r)) => match l.cmp(&r) {
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
            },
        };
    }
}

struct LeafFilter<'a> {
    txid: u64,
    last: Option<&'a [u8]>,
    junks: Junks,
    skip_dup: bool,
}

impl<'a> LeafFilter<'a> {
    fn check_impl(&mut self, k: &LeafSeg<'a>, v: &Val) -> bool {
        if let Some(last) = self.last
            && last == k.raw()
        {
            if self.skip_dup {
                return false;
            }

            if k.txid() > self.txid {
                return true;
            }

            // it's the oldest version, the rest versions will never be accessed by any txn
            self.skip_dup = true;
            // skip only when removed and is safe
            return !v.is_tombstone();
        }

        self.last = Some(k.raw());
        self.skip_dup = k.txid() <= self.txid;
        // skip only when removed and is safe
        if v.is_tombstone() && self.skip_dup {
            return false;
        }
        true
    }

    #[inline(always)]
    fn check(&mut self, x: &(LeafSeg<'a>, Val)) -> bool {
        let (k, v) = x;
        if !self.check_impl(k, v) {
            self.collect(v);
            false
        } else {
            true
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
        sync::atomic::{AtomicU64, Ordering::Relaxed},
    };

    use crate::{
        Options,
        types::{
            data::{Key, Record, Ver},
            node::Node,
            refbox::{BoxRef, BoxView, DeltaView},
            traits::{IAlloc, IHeader, ILoader},
        },
    };

    struct AInner {
        map: Mutex<HashMap<u64, BoxRef>>,
        off: AtomicU64,
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
                }),
            }
        }

        fn load(&self, addr: u64) -> BoxRef {
            let lk = self.inner.map.lock();
            lk.get(&addr).unwrap().clone()
        }
    }

    impl IAlloc for A {
        fn allocate(&mut self, size: usize) -> BoxRef {
            let addr = self
                .inner
                .off
                .fetch_add(BoxRef::real_size(size as u32) as u64, Relaxed);
            let p = BoxRef::alloc(size as u32, addr);
            let mut lk = self.inner.map.lock();
            lk.insert(addr, p.clone());
            p
        }

        fn arena_size(&mut self) -> usize {
            64 << 20
        }

        fn collect(&mut self, _addr: &[u64]) {}

        fn inline_size(&self) -> usize {
            Options::INLINE_SIZE
        }
    }

    impl ILoader for A {
        fn load(&self, addr: u64) -> Option<BoxView> {
            Some(self.load(addr).view())
        }

        fn pin(&self, data: BoxRef) {
            let mut lk = self.inner.map.lock();
            lk.insert(data.header().addr, data);
        }

        fn shallow_copy(&self) -> Self {
            self.clone()
        }

        fn load_remote(&self, addr: u64) -> Option<BoxRef> {
            Some(self.load(addr))
        }
    }

    #[test]
    fn leaf_iter() {
        let mut a = A::new();
        let txid = AtomicU64::new(1);
        const CONSOLIDATE_THRESHOLD: usize = 64;

        {
            let l = a.clone();
            let mut node = Node::new_leaf(&mut a, l);

            let (d1, r1) = DeltaView::from_key_val(
                &mut a,
                &Key::new("foo".as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 1)),
                &Record::normal(1, "1".as_bytes()),
            );
            let (d2, r2) = DeltaView::from_key_val(
                &mut a,
                &Key::new("foo".as_bytes(), Ver::new(txid.load(Relaxed), 2)),
                &Record::normal(1, "2".as_bytes()),
            );

            let (d3, r3) = DeltaView::from_key_val(
                &mut a,
                &Key::new("foo".as_bytes(), Ver::new(txid.load(Relaxed), 3)),
                &Record::remove(1),
            );

            node.insert_inplace(d1.view().as_delta());
            node.save(d1, r1);
            node.insert_inplace(d2.view().as_delta());
            node.save(d2, r2);
            (node, _) = node.compact(&mut a, 1);

            let iter = node.leaf_iter(1);
            assert_eq!(iter.count(), 2);

            node.insert_inplace(d3.view().as_delta());
            node.save(d3, r3);
            let iter = node.leaf_iter(3);
            assert_eq!(iter.count(), 0);
        }

        let l = a.clone();
        let mut node = Node::new_leaf(&mut a, l);

        for i in 0..30 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 1));
            let v = Record::normal(1, raw.as_bytes());
            let (delta, r) = DeltaView::from_key_val(&mut a, &k, &v);
            node.insert_inplace(delta.view().as_delta());
            node.save(delta, r);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                (node, _) = node.compact(&mut a, 3);
            }
        }

        // this will create siblings
        for i in 0..20 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 0));
            let v = Record::remove(1);
            let (delta, r) = DeltaView::from_key_val(&mut a, &k, &v);
            node.insert_inplace(delta.view().as_delta());
            node.save(delta, r);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                (node, _) = node.compact(&mut a, 3);
            }
        }

        // this will mix siblings and new keys
        for i in 30..31 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 0));
            let v = Record::normal(1, raw.as_bytes());
            let (delta, r) = DeltaView::from_key_val(&mut a, &k, &v);
            node.insert_inplace(delta.view().as_delta());
            node.save(delta, r);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                (node, _) = node.compact(&mut a, 3);
            }
        }

        let mut last: Option<crate::types::data::LeafSeg<'_>> = None;
        let iter = node.leaf_iter(3);

        // make sure the iterator produce ascending sorted value
        for (k, _) in iter {
            if let Some(old) = last {
                assert!(old.cmp(&k).is_lt());
            }
            last = Some(k);
        }
    }
}
