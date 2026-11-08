use super::iter::{IterAdaptor, IterAdaptorRev, RawLeafIterCore};
use super::*;

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
    pub(super) addr: u64,
    pub(super) total_size: usize,
    pub(super) max_txid: u64,
    pub(super) group: u8,
    /// for smo/compact use
    pub(super) latest_lsn: Position,
    pub(super) delta: ImTree<DeltaView>,
}

pub(crate) struct Node<L: ILoader> {
    /// the loader is remote/sibling loader, not node loader
    pub(crate) loader: L,
    mtx: Arc<Mutex<()>>,
    recent: AtomicBool,
    pub(crate) state: RwLock<NodeState>,
    pub(super) inner: BaseView,
}

fn intl_cmp(x: &DeltaView, y: &DeltaView) -> Ordering {
    // for internal nodes, we never use the txid for insert
    crate::types::data::cmp_raw_bytes(IntlKey::encoded_raw(x.key()), IntlKey::encoded_raw(y.key()))
}

fn leaf_cmp(x: &DeltaView, y: &DeltaView) -> Ordering {
    let (lver, lraw) = Key::encoded_key_parts(x.key());
    let (rver, rraw) = Key::encoded_key_parts(y.key());
    match crate::types::data::cmp_raw_bytes(lraw, rraw) {
        Equal => Ver::decode_from(lver).cmp(&Ver::decode_from(rver)),
        ord => ord,
    }
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
                delta: ImTree::new(if d.header().node_type == NodeType::Intl {
                    intl_cmp
                } else {
                    leaf_cmp
                }),
            }),
            inner: BaseView::null(),
        };
        Self::load_inner(&mut l, d);
        l
    }

    /// keep node-owned pages alive until the page itself is reclaimed
    /// NOTE: remote page are not saved and should be load from dirty pages or cache
    pub(super) fn save_delta(&self, b: BoxRef) {
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
}

impl<L> Node<L>
where
    L: ILoader,
{
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
    pub(super) fn insert_inplace(&self, mut k: DeltaView, remote_size: usize) -> u64 {
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
                match crate::types::data::cmp_raw_bytes(raw, y.raw) {
                    Equal => Ver::decode_from(ver).cmp(&y.ver),
                    ord => ord,
                }
            },
            |dv| {
                let (ver, raw) = Key::encoded_key_parts(dv.key());
                // delta is sorted (raw asc, ver desc): once raw exceeds the probe
                // no later entry can match, stop instead of scanning to the tail
                let ord = crate::types::data::cmp_raw_bytes(raw, key.raw());
                if ord.is_gt() {
                    return true;
                }
                if ord.is_eq() {
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
        // fast path: a probe key strictly above the delta's greatest key cannot
        // collide with any delta entry (delta is ordered by raw asc), so skip the
        // ordered delta walk and consult only the base sst
        let delta_skip = {
            let state = self.state.read();
            match state.delta.max_key() {
                None => true,
                Some(dmax) => {
                    let (_, dmax_raw) = Key::encoded_key_parts(dmax.key());
                    crate::types::data::cmp_raw_bytes(key.raw(), dmax_raw).is_gt()
                }
            }
        };
        let mut result = None;
        if !delta_skip {
            let probe = Key::new(key.raw(), Ver::new(u64::MAX, u32::MAX));
            self.visit_versions(
                probe,
                |x, y| {
                    let (ver, raw) = Key::encoded_key_parts(x.key());
                    match crate::types::data::cmp_raw_bytes(raw, y.raw) {
                        Equal => Ver::decode_from(ver).cmp(&y.ver),
                        ord => ord,
                    }
                },
                |dv| {
                    let (ver, raw) = Key::encoded_key_parts(dv.key());
                    // delta is sorted (raw asc, ver desc): once raw exceeds the probe
                    // no later entry can match, stop instead of scanning to the tail
                    let ord = crate::types::data::cmp_raw_bytes(raw, key.raw());
                    if ord.is_gt() {
                        return true;
                    }
                    if ord.is_eq() {
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
        }

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

        RawLeafIterCore {
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
        RawLeafIterCore {
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
