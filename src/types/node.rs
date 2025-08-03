use std::{
    cmp::Ordering::{self, Equal, Greater, Less},
    marker::PhantomData,
    ops::{Bound, Deref, DerefMut},
};

use crate::{
    types::{
        data::{Index, IntlKey, Key, Record, Value, Ver},
        header::{BoxHeader, NodeType},
        imtree::{ImTree, Iter, RangeIter},
        refbox::{IBoxHeader, ILoader, KeyRef},
        traits::{IAlloc, ICodec, IHolder, IKey, IVal},
    },
    utils::{INIT_ADDR, NULL_CMD, NULL_ORACLE, NULL_PID, ROOT_PID, unpack_id},
};

use super::{
    header::TagKind,
    refbox::{BaseIter, BaseRef, BoxRef, DeltaRef, IBoxRef},
};

#[derive(Clone)]
pub(crate) struct Node<L: ILoader> {
    /// latest address of delta chain
    pub(super) addr: u64,
    total_size: usize,
    /// the loader is remote/sibling loader, not node loader
    loader: L,
    delta: ImTree<DeltaRef>,
    inner: BaseRef,
}

fn intl_cmp(x: &DeltaRef, y: &DeltaRef) -> Ordering {
    let l = IntlKey::decode_from(x.key());
    let r = IntlKey::decode_from(y.key());

    // for internal nodes, we never use the txid for insert
    l.raw.cmp(r.raw)
}

fn leaf_cmp(x: &DeltaRef, y: &DeltaRef) -> Ordering {
    let l = Key::decode_from(x.key());
    let r = Key::decode_from(y.key());
    l.cmp(&r)
}

fn null_cmp(_x: &DeltaRef, _y: &DeltaRef) -> Ordering {
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
    fn new(loader: L, b: BaseRef) -> Self {
        let h = b.header();
        Self {
            addr: b.box_header().addr,
            total_size: h.size as usize,
            loader,
            delta: ImTree::new(if h.is_index { intl_cmp } else { leaf_cmp }),
            inner: b,
        }
    }

    pub(crate) fn loader(&self) -> &L {
        &self.loader
    }

    pub(crate) fn new_leaf<A: IAlloc>(a: &mut A, loader: L) -> Node<L> {
        #[derive(Clone, Copy)]
        struct EmptyHolder<'a>(PhantomData<&'a ()>);
        impl<'a> Iterator for EmptyHolder<'a> {
            type Item = (Key<'a>, Value<Record>);
            fn next(&mut self) -> Option<Self::Item> {
                None
            }
        }

        impl IHolder for EmptyHolder<'_> {
            fn take_holder(&mut self) -> BoxRef {
                BoxRef::null()
            }
        }

        let b = BaseRef::new_leaf(a, [].as_slice(), None, NULL_PID, || {
            EmptyHolder(PhantomData)
        });
        Self::new(loader, b)
    }

    pub(crate) fn new_root<A: IAlloc>(a: &mut A, loader: L, item: &[(IntlKey, Index)]) -> Node<L> {
        let b = BaseRef::new_intl(a, [].as_slice(), None, NULL_PID, || {
            item.iter().map(|&(x, y)| (x, y))
        });
        Self::new(loader, b)
    }

    pub(crate) fn size(&self) -> usize {
        self.total_size
    }

    pub(crate) fn garbage_collect<A: IAlloc>(&self, a: &mut A) {
        let iter = self.delta.iter();

        for d in iter {
            let x = d.box_header().addr;
            a.recycle(&[x]);
        }

        a.recycle(self.inner.remote());
        a.recycle(&[self.inner.box_header().addr]);
    }

    pub(crate) fn load(addr: u64, loader: L) -> Self {
        let d = loader.load(addr);
        let mut l = Self {
            loader,
            delta: ImTree::new(null_cmp),
            inner: BaseRef::null(),
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
        // TODO: these delta/sst should not allocate in arena which create too many garbage
        let lhs = self.merge_to_base(a, safe_txid);
        let rhs = other.merge_to_base(a, safe_txid);
        a.recycle(lhs.remote());
        a.recycle(rhs.remote());
        a.recycle(&[lhs.box_header().addr, rhs.box_header().addr]);

        let mut node = Self::new(self.loader.clone(), lhs.merge(a, self.loader.clone(), rhs));
        node.header_mut().split_elems = self.header().split_elems;
        node
    }

    pub(crate) fn lo(&self) -> (BoxRef, &[u8]) {
        self.inner.get_lo(&self.loader)
    }

    pub(crate) fn hi(&self) -> Option<(BoxRef, &[u8])> {
        self.inner.get_hi(&self.loader)
    }

    pub(crate) fn child_index(&self, k: &[u8]) -> (bool, u64) {
        #[cfg(feature = "extra_check")]
        {
            assert!(self.header().is_index);
            // it's always compacted after node split, so the `map` is empty
            assert_eq!(self.delta.len(), 0);
            // make sure k is in current node
            assert!(k >= self.lo().1);
            if let Some(hi) = self.hi() {
                assert!(hi.1 > k);
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
        if self
            .find_latest::<IntlKey, Index, _>(&IntlKey::new(key), |x, y| x.raw().cmp(y.raw()))
            .is_some()
        {
            // already inserted by other thread
            return None;
        }

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

        let delta = DeltaRef::new(a, IntlKey::new(key), Index::new(pid));
        let tmp = self.insert(delta);
        Some(tmp.compact(a, safe_txid))
    }

    /// NOTE: no need to compact, since we check `should_split` is not use the delta size, i.e, when
    /// `should_split`, the node must have been compacted, and any insert to node will check first
    pub(crate) fn split<A: IAlloc>(&self, a: &mut A) -> (Node<L>, Node<L>) {
        let h = self.inner.box_header();
        let elems = self.inner.header().elems as usize;
        let sep = elems / 2;
        let (_x, lo) = self.lo();
        let hi_opt = self.hi();
        let hi = hi_opt.as_ref().map(|x| x.1);
        // both lhs and rhs node are set to current's sibling, update lhs's after rhs was mapped
        let sibling = self.header().right_sibling;

        let (l, r) = if h.node_type == NodeType::Intl {
            let (sep_key, _r) = self.sst::<IntlKey, Index>().key_at(&self.loader, sep);
            (
                BaseRef::new_intl(a, lo, Some(sep_key.raw), sibling, || {
                    self.inner
                        .range_iter::<L, IntlKey, Index>(self.loader.clone(), 0, sep)
                        .map(|(k, v, _)| (k, v))
                }),
                BaseRef::new_intl(a, sep_key.raw, hi, sibling, || {
                    self.inner
                        .range_iter::<L, IntlKey, Index>(self.loader.clone(), sep, elems)
                        .map(|(k, v, _)| (k, v))
                }),
            )
        } else {
            let (sep_key, _r) = self.sst::<Key, Value<Record>>().key_at(&self.loader, sep);
            (
                BaseRef::new_leaf(a, lo, Some(sep_key.raw), sibling, || {
                    self.inner
                        .range_iter::<L, Key, Value<Record>>(self.loader.clone(), 0, sep)
                }),
                BaseRef::new_leaf(a, sep_key.raw, hi, sibling, || {
                    self.inner
                        .range_iter::<L, Key, Value<Record>>(self.loader.clone(), sep, elems)
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
    pub(crate) fn intl_iter(&self) -> IntlIter<L> {
        debug_assert_eq!(self.box_header().node_type, NodeType::Intl);
        IntlIter {
            next_l: None,
            next_r: None,
            sst_iter: self.inner.range_iter(
                self.loader.clone(),
                0,
                self.inner.header().elems as usize,
            ),
            delta_iter: IterAdaptor::Iter(self.delta.iter().skip(0)),
        }
    }

    #[allow(clippy::iter_skip_zero)]
    pub(crate) fn leaf_iter(&self, safe_tixd: u64) -> LeafIter<L> {
        debug_assert_eq!(self.box_header().node_type, NodeType::Leaf);
        LeafIter {
            sibling: None,
            sibling_pos: 0,
            remote: None,
            sib_key: [].as_slice(),
            loader: self.loader.clone(),
            next_l: None,
            next_r: None,
            sst_iter: self.inner.range_iter(
                self.loader.clone(),
                0,
                self.inner.header().elems as usize,
            ),
            delta_iter: IterAdaptor::Iter(self.delta.iter().skip(0)),
            filter: LeafFilter {
                txid: safe_tixd,
                last: None,
                skip_dup: false,
            },
        }
    }

    pub(crate) fn compact<A: IAlloc>(&self, a: &mut A, safe_txid: u64) -> Node<L> {
        let mut b = self.merge_to_base(a, safe_txid);
        let old_h = self.header();
        let new_h = b.header_mut();
        new_h.merging = old_h.merging;
        new_h.merging_child = old_h.merging_child;
        Self::new(self.loader.clone(), b)
    }

    fn merge_to_base<A: IAlloc>(&self, a: &mut A, safe_txid: u64) -> BaseRef {
        let h = self.header();
        let (_x, lo) = self.lo();
        let hi_opt = self.hi();
        let hi = hi_opt.as_ref().map(|x| x.1);

        if h.is_index {
            BaseRef::new_intl(a, lo, hi, h.right_sibling, || self.intl_iter())
        } else {
            BaseRef::new_leaf(a, lo, hi, h.right_sibling, || self.leaf_iter(safe_txid))
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
                        key = Some(k.raw);
                        break;
                    }
                }
                let delta = DeltaRef::new(a, IntlKey::new(key.unwrap()), Index::null());
                let tmp_node = self.insert(delta);
                let mut p = tmp_node.compact(a, safe_txid);
                p.header_mut().merging_child = NULL_PID;
                p.header_mut().split_elems = h.split_elems + 1;
                p
            }
            MergeOp::MarkParent(pid) => {
                assert_eq!(self.box_header().node_type, NodeType::Intl);
                let mut p = self.compact(a, safe_txid);
                p.header_mut().merging_child = pid;
                p
            }
            MergeOp::MarkChild => {
                assert_eq!(self.box_header().node_type, NodeType::Leaf);
                let mut p = self.compact(a, safe_txid);
                p.header_mut().merging = true;
                p
            }
        }
    }

    pub(crate) fn insert(&self, mut k: DeltaRef) -> Node<L> {
        let h = k.box_header_mut();
        let th = self.box_header();
        // link to old address
        h.link = self.addr;
        h.node_type = th.node_type;
        h.pid = th.pid;

        Node {
            loader: self.loader.clone(),
            addr: h.addr, // save the new address
            total_size: self.total_size + h.total_size as usize,
            delta: self.delta.update(k),
            inner: self.inner.clone(),
        }
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
        if self.inner.header().elems == 0 {
            debug_assert_eq!(self.inner.box_header().pid, ROOT_PID);
            return None;
        }
        let sst = self.inner.sst::<K, V>();
        let pos = sst
            .search_by(&self.loader, key, |x, y| x.raw().cmp(y.raw()))
            .ok()?;
        let (k, v, p) = sst.get_unchecked(&self.loader, pos);
        Some((
            k,
            v,
            if p.is_null() {
                self.inner.as_box()
            } else {
                p.as_box()
            },
        ))
    }

    pub(crate) fn range_from<K>(
        &self,
        key: K,
        cmp: fn(&DeltaRef, &K) -> Ordering,
        equal: fn(&DeltaRef, &K) -> bool,
    ) -> RangeIter<DeltaRef, K>
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

    pub(crate) fn box_header_mut(&mut self) -> &mut BoxHeader {
        self.inner.box_header_mut()
    }

    pub(crate) fn delta_len(&self) -> u32 {
        self.delta.len() as u32
    }

    fn load_inner(l: &mut Node<L>, mut d: BoxRef) {
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
            d = l.loader.load(h.link);
        }
    }

    #[allow(clippy::iter_skip_zero)]
    pub(crate) fn successor(&self, b: &Bound<KeyRef>) -> RawLeafIter<'_, L> {
        fn cmp_fn(x: &DeltaRef, y: &KeyRef) -> Ordering {
            Key::decode_from(x.key()).raw.cmp(y.key())
        }

        fn equal_fn(_x: &DeltaRef, _y: &KeyRef) -> bool {
            true
        }

        // get the start position in both delta and sst
        let (delta, pos) = match b {
            Bound::Unbounded => (IterAdaptor::Iter(self.delta.iter().skip(0)), 0),
            Bound::Included(b) => {
                let r = self.delta.range_from(b.clone(), cmp_fn, equal_fn).skip(0);

                let (_x, lo) = self.lo();

                let pos = if b.key() < lo {
                    Err(0)
                } else {
                    let key = Key::new(b.key(), NULL_ORACLE, NULL_CMD);
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

                let (_x, lo) = self.lo();
                let inner_pos = if b.key() < lo {
                    Err(0)
                } else {
                    let key = Key::new(b.key(), NULL_ORACLE, NULL_CMD);
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

        RawLeafIter {
            sib_key: &[],
            sibling: None,
            sibling_pos: 0,
            loader: self.loader.clone(),
            next_l: None,
            next_r: None,
            delta_iter: delta,
            sst_iter: self
                .inner
                .range_iter(self.loader.clone(), pos, self.header().elems as usize),
        }
    }
}

impl<L> Deref for Node<L>
where
    L: ILoader,
{
    type Target = BaseRef;

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
    Iter(std::iter::Skip<Iter<'a, DeltaRef>>),
    Range(std::iter::Skip<RangeIter<'a, DeltaRef, T>>),
}

impl<'a, T> Iterator for IterAdaptor<'a, T> {
    type Item = &'a DeltaRef;

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
    next_l: Option<(IntlKey<'a>, Index)>,
    next_r: Option<(IntlKey<'a>, Index)>,
    sst_iter: BaseIter<'a, L, IntlKey<'a>, Index>,
    delta_iter: IterAdaptor<'a, &'a [u8]>,
}

pub(crate) struct LeafIter<'a, L>
where
    L: ILoader,
{
    /// whether we are process sibling
    sibling: Option<BaseRef>,
    sibling_pos: usize,
    /// hold remote in case it was dropped
    remote: Option<BoxRef>,
    sib_key: &'a [u8],
    loader: L,
    next_l: Option<(Key<'a>, Value<Record>)>,
    next_r: Option<(Key<'a>, Value<Record>)>,
    sst_iter: BaseIter<'a, L, Key<'a>, Value<Record>>,
    delta_iter: IterAdaptor<'a, &'a [u8]>,
    filter: LeafFilter<'a>,
}

pub(crate) struct RawLeafIter<'a, L>
where
    L: ILoader,
{
    /// whether we are process sibling
    sibling: Option<BaseRef>,
    sibling_pos: usize,
    sib_key: &'a [u8],
    loader: L,
    next_l: Option<(Key<'a>, Value<Record>, KeyRef)>,
    next_r: Option<(Key<'a>, Value<Record>, KeyRef)>,
    sst_iter: BaseIter<'a, L, Key<'a>, Value<Record>>,
    delta_iter: IterAdaptor<'a, KeyRef>,
}

impl<'a, L> Iterator for IntlIter<'a, L>
where
    L: ILoader,
{
    type Item = (IntlKey<'a>, Index);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.next_l.is_none() {
                if let Some(x) = self.delta_iter.next() {
                    self.next_l =
                        Some((IntlKey::decode_from(x.key()), Index::decode_from(x.val())));
                }
            }

            if self.next_r.is_none() {
                if let Some((k, v, _remote)) = self.sst_iter.next() {
                    self.next_r = Some((k, v));
                }
            }

            match (self.next_l, self.next_r) {
                (None, None) => return None,
                (None, Some(x)) => {
                    self.next_r.take();
                    return Some(x);
                }
                (Some(x), None) => {
                    self.next_l.take();
                    debug_assert!(!x.1.is_tombstone());
                    return Some(x);
                }
                (Some(l), Some(r)) => match l.0.raw.cmp(r.0.raw) {
                    Equal => {
                        self.next_l.take();
                        self.next_r.take();
                        // when the latest one is marked as tombstone, skip all same `raw`s
                        // NOTE: there are at most same `raw` one in delta another in sst
                        if l.1.is_tombstone() {
                            continue;
                        }
                        return Some(l);
                    }
                    Greater => {
                        self.next_r.take();
                        return Some(r);
                    }
                    Less => {
                        self.next_l.take();
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
    type Item = (Key<'a>, Value<Record>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.next_r.is_none()
                && let Some(p) = self.sibling.as_ref()
            {
                if self.sibling_pos >= p.header().elems as usize {
                    self.sibling_pos = 0;
                    let link = p.box_header().link;
                    if link != INIT_ADDR {
                        self.sibling = Some(self.loader.load(link).as_base());
                        continue;
                    }
                    self.sibling.take();
                } else {
                    let (ver, v, r) = p
                        .sst::<Ver, Value<Record>>()
                        .get_unchecked(&self.loader, self.sibling_pos);
                    self.sibling_pos += 1;
                    self.next_r = Some((Key::new(self.sib_key, ver.txid, ver.cmd), v));
                    let b = if r.is_null() { p.as_box() } else { r.as_box() };
                    self.remote = Some(b);
                }
            }

            if self.next_l.is_none() {
                if let Some(x) = self.delta_iter.next() {
                    let k = Key::decode_from(x.key());
                    let v = Value::<Record>::decode_from(x.val());
                    self.next_l = Some((k, v));
                }
            }

            if self.next_r.is_none() {
                if let Some((k, v)) = self.sst_iter.next() {
                    self.remote = Some(self.sst_iter.take_holder());
                    if let Some(s) = v.sibling() {
                        self.sib_key = k.raw;
                        self.sibling_pos = 0;
                        self.sibling = Some(self.loader.load(s.addr()).as_base());
                        self.next_r = Some((k, v.unpack_sibling()));
                    } else {
                        self.next_r = Some((k, v));
                    }
                }
            }

            match (self.next_l, self.next_r) {
                (None, None) => return None,
                (None, Some(r)) => {
                    self.next_r.take();
                    if self.filter.check(&r) {
                        return Some(r);
                    }
                }
                (Some(l), None) => {
                    self.next_l.take();
                    if self.filter.check(&l) {
                        return Some(l);
                    }
                }
                (Some(l), Some(r)) => match l.0.cmp(&r.0) {
                    Less => {
                        self.next_l.take();
                        if self.filter.check(&l) {
                            return Some(l);
                        }
                    }
                    Greater => {
                        self.next_r.take();
                        if self.filter.check(&r) {
                            return Some(r);
                        }
                    }
                    Equal => unreachable!("never happen"),
                },
            }
        }
    }
}

impl<'a, L> IHolder for LeafIter<'a, L>
where
    L: ILoader,
{
    fn take_holder(&mut self) -> BoxRef {
        self.remote.take().map_or(BoxRef::null(), |x| x)
    }
}

impl<'a, L> Iterator for RawLeafIter<'a, L>
where
    L: ILoader,
{
    type Item = (Key<'a>, Value<Record>, KeyRef);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.next_r.is_none()
                && let Some(p) = self.sibling.as_ref()
            {
                if self.sibling_pos >= p.header().elems as usize {
                    self.sibling_pos = 0;
                    let link = p.box_header().link;
                    if link != INIT_ADDR {
                        self.sibling = Some(self.loader.load(link).as_base());
                        continue;
                    }
                    self.sibling = None;
                } else {
                    let (ver, v, remote) = p
                        .sst::<Ver, Value<Record>>()
                        .get_unchecked(&self.loader, self.sibling_pos);
                    self.sibling_pos += 1;
                    let b = if remote.is_null() {
                        p.as_box()
                    } else {
                        remote.as_box()
                    };
                    self.next_r = Some((
                        Key::new(self.sib_key, ver.txid, ver.cmd),
                        v,
                        KeyRef::new(self.sib_key, b),
                    ));
                }
            }

            if self.next_l.is_none() {
                if let Some(x) = self.delta_iter.next() {
                    let k = Key::decode_from(x.key());
                    let v = Value::<Record>::decode_from(x.val());
                    self.next_l = Some((k, v, KeyRef::new(k.raw, x.as_box())));
                }
            }

            if self.next_r.is_none() {
                if let Some((k, v)) = self.sst_iter.next() {
                    let kr = KeyRef::new(k.raw, self.sst_iter.take_holder());
                    if let Some(s) = v.sibling() {
                        self.sib_key = k.raw;
                        self.sibling = Some(self.loader.load(s.addr()).as_base());
                        self.next_r = Some((k, v.unpack_sibling(), kr));
                    } else {
                        self.next_r = Some((k, v, kr));
                    }
                }
            }

            match (self.next_l.clone(), self.next_r.clone()) {
                (None, None) => return None,
                (None, Some(r)) => {
                    self.next_r.take();
                    return Some(r);
                }
                (Some(l), None) => {
                    self.next_l.take();
                    return Some(l);
                }
                (Some(l), Some(r)) => match l.0.cmp(&r.0) {
                    Less => {
                        self.next_l.take();
                        return Some(l);
                    }
                    Greater => {
                        self.next_r.take();
                        return Some(r);
                    }
                    Equal => unreachable!("never happen"),
                },
            }
        }
    }
}

struct LeafFilter<'a> {
    txid: u64,
    last: Option<&'a [u8]>,
    skip_dup: bool,
}

impl<'a> LeafFilter<'a> {
    fn check(&mut self, x: &(Key<'a>, Value<Record>)) -> bool {
        let (k, v) = x;
        if let Some(last) = self.last {
            if last == k.raw {
                if self.skip_dup {
                    return false;
                }

                if k.txid > self.txid {
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
        }

        self.last = Some(k.raw);
        self.skip_dup = k.txid <= self.txid;
        match v {
            // skip only when removed and is safe
            Value::Del(_) if k.txid <= self.txid => false,
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

    use crate::{
        Options,
        types::{
            data::{Key, Record, Value},
            node::Node,
            refbox::{BoxRef, DeltaRef, ILoader},
            traits::{IAlloc, IInlineSize},
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

        fn recycle(&mut self, _p: &[u64]) {}
    }

    impl IInlineSize for A {
        fn inline_size(&self) -> u32 {
            2048
        }
    }

    impl ILoader for A {
        fn load(&self, addr: u64) -> BoxRef {
            let lk = self.inner.map.lock().unwrap();
            lk.get(&addr).unwrap().clone()
        }
    }

    #[test]
    fn leaf_iter() {
        let mut a = A::new();
        let txid = AtomicU64::new(1);

        {
            let l = a.clone();
            let mut node = Node::new_leaf(&mut a, l);

            let d1 = DeltaRef::new(
                &mut a,
                Key::new("foo".as_bytes(), txid.fetch_add(1, Relaxed), 1),
                Value::Put(Record::normal(1, "1".as_bytes())),
            );
            let d2 = DeltaRef::new(
                &mut a,
                Key::new("foo".as_bytes(), txid.load(Relaxed), 2),
                Value::Put(Record::normal(1, "2".as_bytes())),
            );

            let d3 = DeltaRef::new(
                &mut a,
                Key::new("foo".as_bytes(), txid.load(Relaxed), 3),
                Value::Del(Record::remove(1)),
            );

            node = node.insert(d1);
            node = node.insert(d2);

            node = node.compact(&mut a, 1);

            let iter = node.leaf_iter(1);
            assert_eq!(iter.count(), 2);

            node = node.insert(d3);
            let iter = node.leaf_iter(3);
            assert_eq!(iter.count(), 0);
        }

        let l = a.clone();
        let mut node = Node::new_leaf(&mut a, l);

        for i in 0..30 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), txid.fetch_add(1, Relaxed), 1);
            let v = Value::Put(Record::normal(1, raw.as_bytes()));
            let delta = DeltaRef::new(&mut a, k, v);
            node = node.insert(delta);

            if node.delta_len() >= Options::CONSOLIDATE_THRESHOLD {
                node = node.compact(&mut a, 3);
            }
        }

        // this will create siblings
        for i in 0..20 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), txid.fetch_add(1, Relaxed), 0);
            let v = Value::Del(Record::remove(1));
            let delta = DeltaRef::new(&mut a, k, v);
            node = node.insert(delta);

            if node.delta_len() >= Options::CONSOLIDATE_THRESHOLD {
                node = node.compact(&mut a, 3);
            }
        }

        // this will mix siblings and new keys
        for i in 30..31 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), txid.fetch_add(1, Relaxed), 0);
            let v = Value::Put(Record::normal(1, raw.as_bytes()));
            let delta = DeltaRef::new(&mut a, k, v);
            node = node.insert(delta);

            if node.delta_len() >= Options::CONSOLIDATE_THRESHOLD {
                node = node.compact(&mut a, 3);
            }
        }

        let mut last = None;
        let iter = node.leaf_iter(3);

        // make sure the iterator produce ascending sorted value
        for (k, _) in iter {
            if let Some(old) = last {
                assert!(old < k);
            }
            last = Some(k);
        }
    }
}
