use std::{
    cmp::Ordering::{self, Equal, Greater, Less},
    ops::{Bound, Deref, DerefMut},
    sync::{Arc, Mutex, MutexGuard, TryLockResult},
};

use crate::{
    types::{
        base::BaseIter,
        data::{Index, IntlKey, IntlSeg, Key, LeafSeg, Record, Value, Ver},
        header::{BoxHeader, NodeType},
        imtree::{ImTree, Iter, RangeIter},
        refbox::{BaseView, BoxView, DeltaView, KeyRef},
        traits::{IAlloc, IAsBoxRef, IBoxHeader, ICodec, IHeader, IKey, ILoader, IVal},
    },
    utils::{NULL_CMD, NULL_ORACLE, NULL_PID, unpack_id},
};

use super::{header::TagKind, refbox::BoxRef};

#[derive(Clone)]
pub(crate) struct Node<L: ILoader> {
    /// latest address of delta chain
    pub(super) addr: u64,
    total_size: usize,
    /// the loader is remote/sibling loader, not node loader
    loader: L,
    mtx: Arc<Mutex<()>>,
    delta: ImTree<DeltaView>,
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
        let (addr, total_size) = (h.addr, h.total_size as usize);
        let base = b.view().as_base();
        loader.pin(b);
        Self {
            addr,
            total_size,
            loader,
            mtx,
            delta: ImTree::new(if base.header().is_index {
                intl_cmp
            } else {
                leaf_cmp
            }),
            inner: base,
        }
    }

    pub(crate) fn loader(&self) -> &L {
        &self.loader
    }

    pub(crate) fn save(&self, b: BoxRef) {
        self.loader.pin(b);
    }

    pub(crate) fn pid(&self) -> u64 {
        self.inner.box_header().pid
    }

    pub(crate) fn set_pid(&mut self, pid: u64) {
        self.inner.box_header_mut().pid = pid;
    }

    pub(crate) fn new_leaf<A: IAlloc>(a: &mut A, loader: L) -> Node<L> {
        let empty: &[(LeafSeg, Value<Record>)] = &[];
        let b = BaseView::new_leaf(a, [].as_slice(), None, NULL_PID, || {
            empty.iter().map(|x| (x.0, x.1))
        });
        Self::new(loader, b)
    }

    pub(crate) fn new_root<A: IAlloc>(a: &mut A, loader: L, item: &[(IntlKey, Index)]) -> Node<L> {
        let b = BaseView::new_intl(a, [].as_slice(), None, NULL_PID, || {
            item.iter().map(|&(x, y)| (IntlSeg::new(&[], x.raw), y))
        });
        Self::new(loader, b)
    }

    pub(crate) fn size(&self) -> usize {
        self.total_size
    }

    pub(crate) fn base_addr(&self) -> u64 {
        self.inner.box_header().addr
    }

    pub(crate) fn garbage_collect<A: IAlloc>(&self, a: &mut A) {
        self.delta
            .iter()
            .for_each(|x| a.collect(&[x.box_header().addr]));

        a.collect(self.inner.remote());
        a.collect(&[self.base_addr()]);
    }

    pub(crate) fn load(addr: u64, loader: L) -> Self {
        let d = loader.pin_load(addr);
        let mut l = Self {
            loader,
            mtx: Arc::new(Mutex::new(())),
            delta: ImTree::new(null_cmp),
            inner: BaseView::null(),
            total_size: 0,
            addr,
        };
        Self::load_inner(&mut l, d);
        l
    }

    fn set_comparator(&mut self, nt: NodeType) {
        if nt == NodeType::Intl {
            self.delta.set_comparator(intl_cmp);
        } else {
            self.delta.set_comparator(leaf_cmp);
        }
    }

    /// we are not rely on node size for smo, but the max node size is close to INLINE_SIZE * SPLIT_ELEM
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
    ) -> Node<L> {
        let lb = self.merge_to_base(a, safe_txid);
        let rb = other.merge_to_base(a, safe_txid);
        let (lhs, rhs) = (lb.view().as_base(), rb.view().as_base());

        let mut node = Self::new(self.loader.clone(), lhs.merge(a, &self.loader, rhs));
        node.header_mut().split_elems = self.header().split_elems;
        node
    }

    pub(crate) fn lo(&self) -> &[u8] {
        self.inner.lo(&self.loader)
    }

    pub(crate) fn hi(&self) -> Option<&[u8]> {
        self.inner.hi(&self.loader)
    }

    pub(crate) fn child_index(&self, k: &[u8]) -> (bool, u64) {
        #[cfg(feature = "extra_check")]
        {
            assert!(self.header().is_index);
            // it's always compacted after node split, so the `map` is empty
            assert_eq!(self.delta.len(), 0);
            // make sure k is in current node
            assert!(k >= self.lo());
            if let Some(hi) = self.hi() {
                assert!(hi > k);
            }
        }
        let sst = self.sst::<IntlKey, Index>();
        let pos = match sst.search_by(&self.loader, &IntlKey::new(k), |x, y| x.raw.cmp(y.raw)) {
            Ok(pos) => pos,
            Err(pos) => pos.max(1) - 1,
        };

        let (_, v, _) = sst.get_unchecked(&self.loader, pos);
        (pos == 0, v.pid)
    }

    /// this method will compact list into sst for better query performance
    pub(crate) fn insert_index<A: IAlloc>(
        &self,
        a: &mut A,
        key: &[u8],
        pid: u64,
        safe_txid: u64,
    ) -> Option<Node<L>> {
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

        let b = DeltaView::from_key_val(a, IntlKey::new(key), Index::new(pid));
        let view = b.view().as_delta();
        let node = self.insert(view).compact(a, safe_txid, false); // 1/SPLIT_ELEMS chance to run
        node.loader.pin(b); // pin to new loader (the cloned one)
        Some(node)
    }

    fn decode_pefix<K, V>(
        &self,
        pos: usize,
        prefix_len: usize,
        lo: &[u8],
        hi: &Option<&[u8]>,
    ) -> (Vec<u8>, (usize, usize))
    where
        K: IKey,
        V: IVal,
    {
        let k = self.sst::<K, V>().key_at(&self.loader, pos);
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

        let (l, r) = if h.is_index {
            let (sep_key, (llen, rlen)) =
                self.decode_pefix::<IntlKey, Index>(sep, prefix_len, lo, &hi);
            let lhs_prefix = &lo[..llen];
            let rhs_prefix = &sep_key[..rlen];
            // NOTE: the prefix will never shrink in split
            let (ld, rd) = (lhs_prefix.len() - prefix_len, rhs_prefix.len() - prefix_len);
            (
                BaseView::new_intl(a, lo, Some(sep_key.as_slice()), sibling, || {
                    self.inner
                        .range_iter::<L, IntlKey, Index>(&self.loader, 0, sep)
                        .map(|(k, v)| (IntlSeg::new(lhs_prefix, &k.raw[ld..]), v))
                }),
                BaseView::new_intl(a, sep_key.as_slice(), hi, sibling, || {
                    self.inner
                        .range_iter::<L, IntlKey, Index>(&self.loader, sep, elems)
                        .map(|(k, v)| (IntlSeg::new(rhs_prefix, &k.raw[rd..]), v))
                }),
            )
        } else {
            let (sep_key, (llen, rlen)) =
                self.decode_pefix::<Key, Value<Record>>(sep, prefix_len, lo, &hi);
            let lhs_prefix = &lo[..llen];
            let rhs_prefix = &sep_key[..rlen];
            // NOTE: the prefix will never shrink in split
            let (ld, rd) = (lhs_prefix.len() - prefix_len, rhs_prefix.len() - prefix_len);
            (
                BaseView::new_leaf(a, lo, Some(sep_key.as_slice()), sibling, || {
                    self.inner
                        .range_iter::<L, Key, Value<Record>>(&self.loader, 0, sep)
                        .map(|(k, v)| (LeafSeg::new(lhs_prefix, &k.raw[ld..], k.ver), v))
                }),
                BaseView::new_leaf(a, sep_key.as_slice(), hi, sibling, || {
                    self.inner
                        .range_iter::<L, Key, Value<Record>>(&self.loader, sep, elems)
                        .map(|(k, v)| (LeafSeg::new(rhs_prefix, &k.raw[rd..], k.ver), v))
                }),
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
            delta_iter: IterAdaptor::Iter(self.delta.iter().skip(0)),
        }
    }

    #[allow(clippy::iter_skip_zero)]
    pub(crate) fn leaf_iter(&'_ self, safe_txid: u64) -> LeafIter<'_, L> {
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
            delta_iter: IterAdaptor::Iter(self.delta.iter().skip(0)),
            filter: LeafFilter {
                txid: safe_txid,
                last: None,
                skip_dup: false,
            },
        }
    }

    pub(crate) fn compact<A: IAlloc>(
        &self,
        a: &mut A,
        safe_txid: u64,
        share_pinned: bool,
    ) -> Node<L> {
        let b = self.merge_to_base(a, safe_txid);
        let old_h = self.header();
        let mut base = b.view().as_base();
        let new_h = base.header_mut();
        new_h.merging = old_h.merging;
        new_h.merging_child = old_h.merging_child;
        if share_pinned {
            Self::new_with_mtx(self.loader.shallow_copy(), b, self.mtx.clone())
        } else {
            Self::new(self.loader.clone(), b)
        }
    }

    fn merge_to_base<A: IAlloc>(&self, a: &mut A, safe_txid: u64) -> BoxRef {
        let h = self.header();
        let lo = self.lo();
        let hi = self.hi();

        if h.is_index {
            BaseView::new_intl(a, lo, hi, h.right_sibling, || self.intl_iter())
        } else {
            BaseView::new_leaf(a, lo, hi, h.right_sibling, || self.leaf_iter(safe_txid))
        }
    }

    pub(crate) fn process_merge<A: IAlloc>(
        &self,
        a: &mut A,
        op: MergeOp,
        safe_txid: u64,
    ) -> Node<L> {
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
                let b = DeltaView::from_key_val(a, key.unwrap(), Index::null());
                let tmp_node = self.insert(b.view().as_delta());
                let mut p = tmp_node.compact(a, safe_txid, false);
                p.header_mut().merging_child = NULL_PID;
                p.header_mut().split_elems = h.split_elems + 1;
                p.loader.pin(b); // pin to new loader (the cloned one)
                p
            }
            MergeOp::MarkParent(pid) => {
                assert_eq!(self.box_header().node_type, NodeType::Intl);
                let mut p = self.compact(a, safe_txid, false);
                p.header_mut().merging_child = pid;
                p
            }
            MergeOp::MarkChild => {
                assert_eq!(self.box_header().node_type, NodeType::Leaf);
                let mut p = self.compact(a, safe_txid, false);
                p.header_mut().merging = true;
                p
            }
        }
    }

    pub(crate) fn insert(&self, mut k: DeltaView) -> Node<L> {
        let h = k.box_header_mut();
        let th = self.box_header();
        // link to old address
        h.link = self.addr;
        h.node_type = th.node_type;
        h.pid = th.pid;

        Node {
            loader: self.loader.shallow_copy(),
            mtx: self.mtx.clone(),
            addr: h.addr, // save the new address
            total_size: self.total_size + h.total_size as usize,
            delta: self.delta.update(k),
            inner: self.inner,
        }
    }

    pub(crate) fn lock(&'_ self) -> MutexGuard<'_, ()> {
        self.mtx.lock().expect("never happen")
    }

    pub(crate) fn try_lock(&'_ self) -> TryLockResult<MutexGuard<'_, ()>> {
        self.mtx.try_lock()
    }

    pub(crate) fn latest_addr(&self) -> u64 {
        self.addr
    }

    pub(crate) fn find_latest<K, V, F>(&self, key: &K, f: F) -> Option<(K, V, BoxRef)>
    where
        K: IKey,
        V: IVal,
        F: Fn(&K, &K) -> Ordering,
    {
        let data = self.delta.find(key, |x, y| f(&K::decode_from(x.key()), y));
        if let Some(x) = data {
            let k = K::decode_from(x.key());
            if k.raw().cmp(key.raw()).is_eq() {
                return Some((k, V::decode_from(x.val()), x.as_box()));
            }
        }
        self.search_sst(key)
    }

    pub(crate) fn search_sst<K, V>(&self, key: &K) -> Option<(K, V, BoxRef)>
    where
        K: IKey,
        V: IVal,
    {
        if self.header().elems > 0 {
            let sst = self.inner.sst::<K, V>();
            let pos = sst
                .search_by(&self.loader, key, |x, y| x.raw().cmp(y.raw()))
                .ok()?;
            let (k, v, r) = sst.get_unchecked(&self.loader, pos);
            Some((k, v, r.map_or_else(|| self.inner.as_box(), |x| x.as_box())))
        } else {
            // it not just ROOT_PID may have empty elems, it's same to those nodes have been compacted
            // with all elems were filtered out
            None
        }
    }

    pub(crate) fn range_from<K>(
        &'_ self,
        key: K,
        cmp: fn(&DeltaView, &K) -> Ordering,
        equal: fn(&DeltaView, &K) -> bool,
    ) -> RangeIter<'_, DeltaView, K>
    where
        K: IKey,
    {
        self.delta.range_from(key, cmp, equal)
    }

    #[allow(unused)]
    pub(crate) fn show(&self) {
        let h = self.box_header();
        log::debug!(
            "---------- show delta {} {:?} elems {} ----------",
            h.pid,
            unpack_id(h.addr),
            self.delta.len()
        );
        if self.header().is_index {
            let it = self.delta.iter();
            for x in it {
                let k = IntlKey::decode_from(x.key());
                let v = Index::decode_from(x.val());
                log::debug!("{} => {}", k.to_string(), v.to_string());
            }
            let sst = self.sst::<IntlKey, Index>();
            sst.show(self.loader(), h.pid, h.addr);
        } else {
            let it = self.delta.iter();
            for x in it {
                let k = Key::decode_from(x.key());
                let v = Value::<Record>::decode_from(x.val());
                log::debug!("{} => {}", k.to_string(), v.to_string());
            }
            let sst = self.sst::<Key, Value<Record>>();
            sst.show(self.loader(), h.pid, h.addr);
        }
    }

    pub(crate) fn box_header(&self) -> &BoxHeader {
        self.inner.box_header()
    }

    pub(crate) fn delta_len(&self) -> usize {
        self.delta.len()
    }

    fn load_inner(l: &mut Node<L>, mut d: BoxView) {
        let mut one_base = true;
        let mut last_type = None;

        loop {
            let h = d.header();
            if let Some(t) = last_type {
                assert_eq!(t, h.node_type);
            } else {
                l.set_comparator(h.node_type);
            }
            last_type = Some(h.node_type);

            match h.kind {
                TagKind::Delta => {
                    let delta = d.as_delta();
                    l.delta.put(delta);
                }
                TagKind::Base => {
                    assert!(one_base);
                    one_base = false;
                    l.inner = d.as_base();
                }
                _ => unreachable!("bad kind {:?}", h.kind),
            }
            if h.link == 0 {
                break;
            }
            d = l.loader.pin_load(h.link);
        }
    }

    #[allow(clippy::iter_skip_zero)]
    pub(crate) fn successor(&self, b: &Bound<KeyRef>) -> RawLeafIter<'_, L> {
        fn cmp_fn(x: &DeltaView, y: &KeyRef) -> Ordering {
            Key::decode_from(x.key()).raw.cmp(y.key())
        }

        fn equal_fn(_x: &DeltaView, _y: &KeyRef) -> bool {
            true
        }

        // get the start position in both delta and sst
        let (delta, pos) = match b {
            Bound::Unbounded => (IterAdaptor::Iter(self.delta.iter().skip(0)), 0),
            Bound::Included(b) => {
                let r = self.delta.range_from(b.clone(), cmp_fn, equal_fn).skip(0);

                let lo = self.lo();

                let pos = if b.key() < lo {
                    Err(0)
                } else {
                    let key = Key::new(b.key(), Ver::new(NULL_ORACLE, NULL_CMD));
                    self.sst::<Key, Value<Record>>()
                        .lower_bound(&self.loader, &key)
                };

                (IterAdaptor::Range(r), pos.unwrap_or_else(|x| x))
            }
            Bound::Excluded(b) => {
                let iter = self.delta.range_from(b.clone(), cmp_fn, equal_fn);
                let delta = if self
                    .delta
                    .find(&b, |x, y| {
                        let k = Key::decode_from(x.key());
                        k.raw.cmp(y.key())
                    })
                    .is_some()
                {
                    iter.skip(1)
                } else {
                    iter.skip(0)
                };

                let lo = self.lo();
                let inner_pos = if b.key() < lo {
                    Err(0)
                } else {
                    let key = Key::new(b.key(), Ver::new(NULL_ORACLE, NULL_CMD));
                    self.sst::<Key, Value<Record>>()
                        .lower_bound(&self.loader, &key)
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
    type Item = &'a DeltaView;

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
    sst_iter: BaseIter<'a, L, IntlKey<'a>, Index>,
    delta_iter: IterAdaptor<'a, &'a [u8]>,
}

pub(crate) struct LeafIter<'a, L>
where
    L: ILoader,
{
    prefix: &'a [u8],
    next_l: Option<(LeafSeg<'a>, Value<Record>)>,
    next_r: Option<(LeafSeg<'a>, Value<Record>)>,
    sst_iter: BaseIter<'a, L, Key<'a>, Value<Record>>,
    delta_iter: IterAdaptor<'a, &'a [u8]>,
    filter: LeafFilter<'a>,
}

pub(crate) struct RawLeafIter<'a, L>
where
    L: ILoader,
{
    prefix: &'a [u8],
    next_l: Option<(Key<'a>, Value<Record>, KeyRef)>,
    next_r: Option<(Key<'a>, Value<Record>, KeyRef)>,
    sst_iter: BaseIter<'a, L, Key<'a>, Value<Record>>,
    delta_iter: IterAdaptor<'a, KeyRef>,
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
                    Index::decode_from(x.val()),
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
    type Item = (LeafSeg<'a>, Value<Record>);

    // TODO: `self.next_l/r = Some(xx)` cause too many l1d-load-misses, maybe we can return Key
    // instead, and apply prefix then remove new prefix in `Base::new_leaf`
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.next_r.is_none()
                && let Some((k, v)) = self.sst_iter.next()
            {
                debug_assert!(v.sibling().is_none());
                self.next_r = Some((LeafSeg::new(self.prefix, k.raw, k.ver), v));
            }

            if self.next_l.is_none()
                && let Some(x) = self.delta_iter.next()
            {
                let k = Key::decode_from(x.key());
                let v = Value::<Record>::decode_from(x.val());
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
    type Item = (Key<'a>, Value<Record>, KeyRef);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_l.is_none()
            && let Some(x) = self.delta_iter.next()
        {
            let k = Key::decode_from(x.key());
            let v = Value::<Record>::decode_from(x.val());
            self.next_l = Some((k, v, KeyRef::new(k.raw, x.as_box())));
        }

        if self.next_r.is_none()
            && let Some((k, v)) = self.sst_iter.next()
        {
            let kr = KeyRef::build(self.prefix, k.raw);
            self.next_r = Some((Key::new(kr.key(), k.ver), v, kr));
        }

        return match (self.next_l.take(), self.next_r.take()) {
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
        };
    }
}

struct LeafFilter<'a> {
    txid: u64,
    last: Option<&'a [u8]>,
    skip_dup: bool,
}

impl<'a> LeafFilter<'a> {
    fn check(&mut self, x: &(LeafSeg<'a>, Value<Record>)) -> bool {
        let (k, v) = x;
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
            match v {
                // skip only when removed and is safe
                Value::Del(_) => return false,
                _ => return true,
            }
        }

        self.last = Some(k.raw());
        self.skip_dup = k.txid() <= self.txid;
        match v {
            // skip only when removed and is safe
            Value::Del(_) if k.txid() <= self.txid => false,
            _ => true,
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        rc::Rc,
        sync::{
            Mutex,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
    };

    use crate::types::{
        data::{Key, Record, Value, Ver},
        node::Node,
        refbox::{BoxRef, BoxView, DeltaView},
        traits::{IAlloc, IHeader, IInlineSize, ILoader},
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
            let lk = self.inner.map.lock().unwrap();
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
            let mut lk = self.inner.map.lock().unwrap();
            lk.insert(addr, p.clone());
            p
        }

        fn arena_size(&mut self) -> u32 {
            64 << 20
        }

        fn collect(&mut self, _addr: &[u64]) {}
    }

    impl IInlineSize for A {
        fn inline_size(&self) -> u32 {
            2048
        }
    }

    impl ILoader for A {
        fn pin_load(&self, addr: u64) -> BoxView {
            self.load(addr).view()
        }

        fn pin(&self, data: BoxRef) {
            let mut lk = self.inner.map.lock().unwrap();
            lk.insert(data.header().addr, data);
        }

        fn shallow_copy(&self) -> Self {
            self.clone()
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

            let d1 = DeltaView::from_key_val(
                &mut a,
                Key::new("foo".as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 1)),
                Value::Put(Record::normal(1, "1".as_bytes())),
            );
            let d2 = DeltaView::from_key_val(
                &mut a,
                Key::new("foo".as_bytes(), Ver::new(txid.load(Relaxed), 2)),
                Value::Put(Record::normal(1, "2".as_bytes())),
            );

            let d3 = DeltaView::from_key_val(
                &mut a,
                Key::new("foo".as_bytes(), Ver::new(txid.load(Relaxed), 3)),
                Value::Del(Record::remove(1)),
            );

            node = node.insert(d1.view().as_delta());
            node.save(d1);
            node = node.insert(d2.view().as_delta());
            node.save(d2);
            node = node.compact(&mut a, 1, true);

            let iter = node.leaf_iter(1);
            assert_eq!(iter.count(), 2);

            node = node.insert(d3.view().as_delta());
            node.save(d3);
            let iter = node.leaf_iter(3);
            assert_eq!(iter.count(), 0);
        }

        let l = a.clone();
        let mut node = Node::new_leaf(&mut a, l);

        for i in 0..30 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 1));
            let v = Value::Put(Record::normal(1, raw.as_bytes()));
            let delta = DeltaView::from_key_val(&mut a, k, v);
            node = node.insert(delta.view().as_delta());
            node.save(delta);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                node = node.compact(&mut a, 3, true);
            }
        }

        // this will create siblings
        for i in 0..20 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 0));
            let v = Value::Del(Record::remove(1));
            let delta = DeltaView::from_key_val(&mut a, k, v);
            node = node.insert(delta.view().as_delta());
            node.save(delta);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                node = node.compact(&mut a, 3, true);
            }
        }

        // this will mix siblings and new keys
        for i in 30..31 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 0));
            let v = Value::Put(Record::normal(1, raw.as_bytes()));
            let delta = DeltaView::from_key_val(&mut a, k, v);
            node = node.insert(delta.view().as_delta());
            node.save(delta);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                node = node.compact(&mut a, 3, true);
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
