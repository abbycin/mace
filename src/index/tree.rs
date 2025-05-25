use super::{
    data::{Index, Key, SLOT_LEN, Value},
    iter::{ItemIter, MergeIterBuilder},
    page::{
        DeltaType, IntlMergeIter, LeafMergeIter, MAINPG_HDR_LEN, MainPage, MainPageHdr, NodeType,
        PageMergeIter, RangeIter,
    },
    systxn::SysTxn,
};

use crate::{
    OpCode, Store,
    cc::data::Ver,
    index::{
        IAlloc,
        builder::{Delta, FuseBuilder},
        page::{NULL_INDEX, SibPage},
    },
    map::{buffer::Loader, data::FrameOwner},
    utils::{
        NULL_CMD,
        bytes::ByteArray,
        traits::{ICodec, IKey, IPageIter, IVal, IValCodec},
        unpack_id,
    },
};
use crate::{Record, utils::NULL_ORACLE};
use std::sync::atomic::Ordering::Relaxed;
use std::{collections::VecDeque, sync::Arc};

#[derive(Clone)]
pub struct Range<'a> {
    /// left and right boundary key of current page, key_pq is routing to current page, while key_qr
    /// is routing to new allocated split page
    key_pq: &'a [u8],
    key_qr: Option<&'a [u8]>,
}

impl Range<'_> {
    pub(crate) const fn new() -> Self {
        Self {
            key_pq: [].as_slice(),
            key_qr: None,
        }
    }
}

#[derive(Clone)]
pub struct ValRef<T>
where
    T: IValCodec,
{
    raw: Value<T>,
    _owner: FrameOwner,
}

impl<T> ValRef<T>
where
    T: IValCodec,
{
    fn new(raw: Value<T>, f: FrameOwner) -> Self {
        Self { raw, _owner: f }
    }

    pub fn slice(&self) -> &[u8] {
        self.raw.as_ref().data()
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.raw.as_ref().data().to_vec()
    }

    pub(crate) fn unwrap(&self) -> &T {
        self.raw.as_ref()
    }

    pub(crate) fn is_put(&self) -> bool {
        !self.is_del()
    }

    pub(crate) fn is_del(&self) -> bool {
        self.raw.is_del()
    }
}

impl<T> Drop for ValRef<T>
where
    T: IValCodec,
{
    fn drop(&mut self) {
        // explicitly impl Drop here to make sure lifetime chain work
    }
}

#[derive(Clone, Copy)]
pub struct View {
    pub page_id: u64,
    pub page_addr: u64,
    pub info: MainPageHdr,
}

#[derive(Clone)]
pub struct Tree {
    pub(crate) store: Arc<Store>,
    root_index: Index,
}

impl Tree {
    pub fn load(store: Arc<Store>, root_pid: u64) -> Self {
        Self {
            store: store.clone(),
            root_index: Index::new(root_pid, 0),
        }
    }

    pub fn new(store: Arc<Store>, root_pid: u64) -> Self {
        let this = Self {
            store: store.clone(),
            root_index: Index::new(root_pid, 0),
        };

        this.init();

        this
    }

    pub fn init(&self) {
        // build an empty page with no key-value
        let iter: ItemIter<(Key<'_>, Value<&[u8]>)> = ItemIter::default();
        let mut txn = SysTxn::new(&self.store);
        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).from(iter, txn.page_size());
        let mut f = txn.alloc(delta.size()).expect("can't alloc memory");
        let h = delta.build(f.payload(), &mut txn);
        assert_eq!(h.epoch(), 0);
        txn.update_unchecked(self.root_index.pid, &mut f);
    }

    #[inline]
    pub(crate) fn begin(&self) -> SysTxn {
        SysTxn::new(&self.store)
    }

    fn walk_page<F, K, V>(&self, mut addr: u64, mut f: F) -> Result<(), OpCode>
    where
        F: FnMut(FrameOwner, u64, &Loader, MainPage<K, V>) -> bool,
        K: IKey,
        V: IVal,
    {
        let loader = Loader::new(self.store.buffer.raw());
        while addr != 0 {
            let frame = self.store.buffer.load(addr);
            let page = MainPage::<K, V>::from(frame.payload());
            let next = page.header().link();

            // NOTE: frame is moved
            if f(frame, addr, &loader, page) {
                break;
            }
            addr = next;
        }
        Ok(())
    }

    fn find_leaf<T>(&self, key: &[u8]) -> Result<(View, Option<View>), OpCode>
    where
        T: IValCodec,
    {
        // used to pin frame that referenced by range
        let mut guard = Vec::new();
        loop {
            guard.clear();
            match self.try_find_leaf::<T>(&mut guard, key) {
                Ok((view, parent)) => return Ok((view, parent)),
                Err(OpCode::Again) => continue,
                Err(e) => unreachable!("invalid error {:?}", e),
            }
        }
    }

    pub(crate) fn page_view(store: &Store, pid: u64) -> View {
        let addr = store.page.get(pid);
        debug_assert_ne!(addr, 0);
        let f = store.buffer.load(addr);
        let b = f.payload();
        View {
            page_id: pid,
            page_addr: addr,
            info: b.into(),
        }
    }

    fn try_find_leaf<T>(
        &self,
        guard: &mut Vec<FrameOwner>,
        key: &[u8],
    ) -> Result<(View, Option<View>), OpCode>
    where
        T: IValCodec,
    {
        let mut index = self.root_index;
        let mut range = Range::new();
        let mut parent: Option<View> = None;

        loop {
            let view = Self::page_view(&self.store, index.pid);

            // split may happen during search, in this case new created nodes are
            // not inserted into parent yet, the insert is halfly done, the
            // search can reach the `split delta` whose epoch are not equal to
            // previous one, and any modification operation CAN NOT proceed,
            // since we simplified the `find_child` process to handle data only,
            // if any other modification operation is allowed, will cause leaf
            // node disordered, by the way, any operation (including lookup)
            // will go through this function and check the condition
            // NOTE: there's a race condition that multiple threads hold the same
            // `index` while one of them finished `parent_update` first and even
            // finished `leaf consolidation`, this will cause rest threads encounter
            // a non-split `view`, to fix this, we check whether parent was already
            // updated before `parent_update`
            if view.info.epoch() != index.epoch {
                let parent = parent.unwrap();
                let index = self.store.page.index(parent.page_id);
                let ok = index
                    .compare_exchange(parent.page_addr, parent.page_addr, Relaxed, Relaxed)
                    .is_ok();
                if ok && self.parent_update::<T>(view, parent, range).is_ok() {
                    // get rid of those keys were moved to new node, for prefix scan
                    self.force_consolidate::<T>(view.page_id);
                }
                // simplified: retry from root, thanks to the large fanout
                return Err(OpCode::Again);
            }

            if view.info.is_leaf() {
                return Ok((view, parent));
            }
            parent = Some(view);

            let (child_index, child_range) = self
                .find_child(guard, key, view.page_addr)
                .expect("child is always exist in B-Tree");
            index = child_index;
            range.key_pq = child_range.key_pq;
            if let Some(key_qr) = child_range.key_qr {
                range.key_qr = Some(key_qr);
            }
        }
    }

    fn find_child<'a>(
        &self,
        guard: &mut Vec<FrameOwner>,
        key: &[u8],
        addr: u64,
    ) -> Option<(Index, Range<'a>)> {
        let mut child = None;

        // stop when the child is in range
        let _ = self.walk_page(addr, |frame, _, l, pg: MainPage<&[u8], Index>| {
            debug_assert!(pg.header().is_intl());

            // skip inner `split-delta`
            if pg.header().is_data() {
                let (l, r) = match pg.search(l, &key) {
                    // equal to key, the range of child's key: [pos, pos+1)
                    Ok(pos) => (
                        pg.get(l, pos),
                        pos.checked_add(1).and_then(|next| pg.get(l, next)),
                    ),
                    // it's insert pos, the range of child's key: [pos - 1, pos)
                    // since the intl node has key-index paired, and the first
                    // key of intl node is `empty`, for example: in an extreme
                    // situation, the key may smaller than any other key in the
                    // node except the first one, then it's child page's index
                    // is at pos - 1, so the child's key range is: [pos-1, pos)
                    // see `split_root`
                    Err(pos) => (
                        pos.checked_sub(1).and_then(|prev| pg.get(l, prev)),
                        pg.get(l, pos),
                    ),
                };
                if let Some((key_pq, index)) = l {
                    if index != NULL_INDEX {
                        let range = Range {
                            key_pq,
                            key_qr: r.map(|(key_qr, _)| key_qr),
                        };
                        guard.push(frame);
                        child = Some((index, range));
                        return true;
                    }
                }
            }
            false
        });
        child
    }

    fn possible_successor<T>(&self, key: &[u8], prefix: &[u8]) -> Option<(u64, Option<Vec<u8>>)>
    where
        T: IValCodec,
    {
        let mut index = self.root_index;
        let mut range = Range::new();
        let mut parent: Option<View> = None;
        let mut successor = None;
        let mut found = false;
        // used to pin frame that referenced by range
        let mut guard = Vec::new();

        loop {
            let view = Self::page_view(&self.store, index.pid);
            if view.info.epoch() != index.epoch {
                let p = parent.unwrap();
                let slot = self.store.page.index(p.page_id);
                let ok = slot
                    .compare_exchange(p.page_addr, p.page_addr, Relaxed, Relaxed)
                    .is_ok();
                if ok && self.parent_update::<T>(view, p, range).is_ok() {
                    let _ = self.consolidate::<T>(view);
                }
                // reset and retry
                index = self.root_index;
                range = Range::new();
                parent = None;
                successor = None;
                found = false;
                guard.clear();
                continue;
            }

            if view.info.is_leaf() {
                return Some((view.page_addr, successor));
            }
            parent = Some(view);

            let mut child = None;
            let _ = self.walk_page(
                view.page_addr,
                |frame, _, loader, pg: MainPage<&[u8], Index>| {
                    let h = pg.header();
                    if h.is_data() {
                        let (pos, l, r) = match pg.search(loader, &key) {
                            Ok(pos) => (
                                Some(pos),
                                pg.get(loader, pos),
                                pos.checked_add(1).and_then(|next| pg.get(loader, next)),
                            ),
                            Err(pos) => (
                                pos.checked_sub(1),
                                pos.checked_sub(1).and_then(|prev| pg.get(loader, prev)),
                                pg.get(loader, pos),
                            ),
                        };

                        debug_assert!(
                            (pos.is_some() && l.is_some()) || (pos.is_none() && l.is_none())
                        );

                        if let Some((ikey, index)) = l {
                            let pos = pos.expect("can't be None");
                            // we will continue to find the insert position even `found` was set, it's
                            // necessary for performing `parent_update` correctly
                            if !found {
                                found = true;
                                // move to next node
                                if let Some((k, _)) = pg.get(loader, pos + 1) {
                                    if k.starts_with(prefix) {
                                        successor = Some(k.to_vec());
                                    }
                                }
                            }
                            if index != NULL_INDEX {
                                guard.push(frame);
                                let range = Range {
                                    key_pq: ikey,
                                    key_qr: r.map(|(key_qr, _)| key_qr),
                                };
                                child = Some((index, range));
                                return true;
                            }
                        }
                    }
                    false
                },
            );

            if let Some((child_index, child_range)) = child {
                index = child_index;
                range.key_pq = child_range.key_pq;
                if let Some(key_qr) = child_range.key_qr {
                    range.key_qr = Some(key_qr);
                }
                continue;
            }

            // terminate when we are reached the end of parent and can't find successor
            if view.info.is_base() {
                return None;
            }
        }
    }

    fn prepend<K, T, F>(
        &self,
        view: &mut View,
        key: &K,
        val: &Value<T>,
        mut check: F,
    ) -> Result<(), OpCode>
    where
        K: IKey,
        T: IValCodec,
        F: FnMut(u64, &K) -> Result<(), OpCode>,
    {
        let mut txn = self.begin();
        let mut delta =
            Delta::new(DeltaType::Data, NodeType::Leaf).with_item(txn.page_size(), (*key, *val));
        let mut f = txn.alloc(delta.size())?;
        let (new_addr, new_page) = (f.addr(), f.payload());
        debug_assert!(new_addr > view.page_addr);
        let h = delta.build(new_page, &mut txn);

        loop {
            check(view.page_addr, key)?;

            h.set_link(view.page_addr);
            h.set_depth(view.info.depth().saturating_add(1));
            h.set_epoch(view.info.epoch());

            match txn.update(view.page_id, view.page_addr, &mut f) {
                Ok(_) => {
                    view.page_addr = new_addr;
                    view.info = *h;
                    break;
                }
                Err(cur_addr) => {
                    // root split never update it's epoch, we don't know whether
                    // it split, so retry from the very beginning
                    // for non-root page, we can retry update unless the epoch
                    // was changed
                    if view.page_id != self.root_index.pid {
                        let f = self.store.buffer.load(cur_addr);
                        let b = f.payload();
                        let hdr: MainPageHdr = b.into();
                        // no split happen
                        if hdr.epoch() == view.info.epoch() {
                            view.page_addr = cur_addr;
                            view.info = hdr;
                            continue;
                        }
                    }
                    return Err(OpCode::Again);
                }
            }
        }

        Ok(())
    }

    fn try_put<K, T>(&self, key: &K, val: &Value<T>) -> Result<(), OpCode>
    where
        K: IKey,
        T: IValCodec,
    {
        let (mut view, _) = self.find_leaf::<T>(key.raw()).unwrap();

        if self.need_split(&view) && self.split::<T>(view).is_ok() {
            return Err(OpCode::Again);
        }

        self.prepend(&mut view, key, val, |_, _| Ok(()))?;

        self.try_consolidate::<T>(view);

        Ok(())
    }

    /// for non-txn use, such as registry and recovery
    pub fn put<K, T>(&self, key: K, val: Value<T>) -> Result<(), OpCode>
    where
        K: IKey,
        T: IValCodec,
    {
        loop {
            match self.try_put(&key, &val) {
                Ok(_) => return Ok(()),
                Err(OpCode::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    fn search<T>(&self, addr: u64, key: &Key) -> Option<(Key, ValRef<T>)>
    where
        T: IValCodec,
    {
        let mut r = None;
        let _ = self.walk_page(addr, |f, _, l, pg: MainPage<Key, Value<T>>| {
            let h = pg.header();

            if !h.is_data() {
                return false;
            }
            debug_assert!(h.is_leaf());
            if let Ok(pos) = pg.search_raw(l, key) {
                let (k, v) = pg.get(l, pos).unwrap();
                r = Some((k, ValRef::new(v, f.clone())));
                return true;
            }
            false
        });
        r
    }

    fn try_update<T1, T2, V>(
        &self,
        key: &Key,
        val: &Value<T1>,
        visible: &mut V,
    ) -> Result<Option<ValRef<T2>>, OpCode>
    where
        T1: IValCodec,
        T2: IValCodec,
        V: FnMut(&Option<(Key, ValRef<T2>)>) -> Result<(), OpCode>,
    {
        let (mut view, _) = self.find_leaf::<T2>(key.raw).unwrap();
        if self.need_split(&view) && self.split::<T2>(view).is_ok() {
            return Err(OpCode::Again);
        }
        let mut r = None;
        self.prepend(&mut view, key, val, |addr, key| {
            r = self.search(addr, key);
            visible(&r)
        })?;

        self.try_consolidate::<T2>(view);
        Ok(r.map(|x| x.1.clone()))
    }

    // NOTE: the `visible` function may be called multiple times
    pub fn update<T1, T2, V>(
        &self,
        key: Key,
        val: Value<T1>,
        mut visible: V,
    ) -> Result<Option<ValRef<T2>>, OpCode>
    where
        T1: IValCodec,
        T2: IValCodec,
        V: FnMut(&Option<(Key, ValRef<T2>)>) -> Result<(), OpCode>,
    {
        let size = key.packed_size() + val.packed_size() + SLOT_LEN + MAINPG_HDR_LEN;
        if size > self.store.opt.max_data_size() {
            return Err(OpCode::TooLarge);
        }
        loop {
            match self.try_update(&key, &val, &mut visible) {
                Ok(x) => return Ok(x),
                Err(OpCode::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    #[allow(dead_code)]
    pub fn show<T: IValCodec>(&self) {
        self.show_node::<T>(self.root_index.pid);
    }

    fn show_node<T: IValCodec>(&self, pid: u64) {
        let mut q = VecDeque::new();
        q.push_back(pid);

        log::info!("-------- show tree node {pid} ------------");
        while let Some(pid) = q.pop_front() {
            let addr = self.store.page.get(pid);
            let _ = self.walk_page(addr, |f, addr, loader, _: MainPage<Key, Value<T>>| {
                let h: MainPageHdr = f.payload().into();
                if h.is_intl() {
                    let pg = MainPage::<&[u8], Index>::from(f.payload());
                    let mut pids = Vec::with_capacity(h.elems as usize);
                    for i in 0..h.elems as usize {
                        if let Some((_, v)) = pg.get(loader, i) {
                            pids.push(v.pid);
                            q.push_back(v.pid);
                        }
                    }
                    log::info!("intl {} => {:?}", f.page_id(), pids);
                }

                if h.is_leaf() && !h.is_split() {
                    log::info!("pid {}", f.page_id());
                    let pg = MainPage::<Key, Value<T>>::from(f.payload());
                    for i in 0..h.elems as usize {
                        let (k, v) = pg.get(loader, i).unwrap();
                        log::info!(
                            "{} => {}\t{:?}",
                            k.to_string(),
                            v.to_string(),
                            unpack_id(addr)
                        );
                        if let Some(s) = v.sibling() {
                            let f = self.store.buffer.load(s.addr());
                            let slotted = SibPage::<Ver, Value<T>>::from(f.payload());
                            slotted.show(loader, s.addr());
                        }
                    }
                }
                false
            });
        }
        log::debug!("[\t\t end \t\t]");
    }

    /// return the latest key-val pair, by using Ikey::raw(), thanks to MVCC, the first match one is
    /// the latest one
    pub fn get<K, T>(&self, key: K) -> Result<(K, ValRef<T>), OpCode>
    where
        K: IKey,
        T: IValCodec,
    {
        let (view, _) = self.find_leaf::<T>(key.raw())?;
        let mut data = Err(OpCode::NotFound);
        let _ = self.walk_page(view.page_addr, |frame, _, l, pg: MainPage<K, Value<T>>| {
            if pg.header().is_data() {
                if let Ok(pos) = pg.search_raw(l, &key) {
                    let (k, v) = pg.get(l, pos).unwrap();
                    data = Ok((
                        k,
                        ValRef {
                            raw: v,
                            _owner: frame.clone(),
                        },
                    ));
                    return true;
                }
            }
            false
        });
        data
    }

    fn traverse_sibling<T, F>(
        &self,
        start_ts: u64,
        mut addr: u64,
        l: &Loader,
        visible: &mut F,
    ) -> Result<ValRef<T>, OpCode>
    where
        T: IValCodec,
        F: FnMut(u64, &T) -> bool,
    {
        let ver = Ver::new(start_ts, NULL_CMD);
        while addr != 0 {
            let frame = self.store.buffer.load(addr);
            let page = SibPage::<Ver, Value<T>>::from(frame.payload());
            let h = page.header();

            let pos = page.lower_bound(l, &ver).unwrap_or_else(|pos| pos);
            if pos < h.elems as usize {
                let (k, v) = page.get(l, pos).unwrap();

                if visible(k.txid, v.as_ref()) {
                    if v.is_del() {
                        return Err(OpCode::NotFound);
                    }
                    return Ok(ValRef {
                        raw: v,
                        _owner: frame,
                    });
                }
            }
            addr = h.link;
        }
        Err(OpCode::NotFound)
    }

    pub fn traverse<T, F>(&self, key: Key, mut visible: F) -> Result<ValRef<T>, OpCode>
    where
        T: IValCodec,
        F: FnMut(u64, &T) -> bool,
    {
        let (view, _) = self.find_leaf::<T>(key.raw)?;
        let mut latest: Option<ValRef<T>> = None;

        let _ = self.walk_page(view.page_addr, |f, _, l, pg: MainPage<Key, Value<T>>| {
            let h = pg.header();
            if !h.is_data() {
                return false;
            }
            debug_assert!(h.is_leaf());
            let Ok(pos) = pg.search_raw(l, &key) else {
                return false;
            };
            let (k, v) = pg.get(l, pos).unwrap();
            if visible(k.txid, v.as_ref()) {
                latest = Some(ValRef::new(v, f));
                return true;
            }
            // all rest `raw` will be checked here
            if let Some(s) = v.sibling() {
                if let Ok(val) = self.traverse_sibling(key.txid, s.addr(), l, &mut visible) {
                    latest = Some(val);
                }
                return true;
            }
            // check next delta
            false
        });
        latest.map_or(Err(OpCode::NotFound), |v| {
            if v.is_del() {
                Err(OpCode::NotFound)
            } else {
                Ok(v)
            }
        })
    }

    fn need_consolidate(&self, info: &MainPageHdr) -> bool {
        let max_depth = if info.is_intl() {
            self.store.opt.intl_consolidate_threshold
        } else {
            self.store.opt.consolidate_threshold
        };

        info.depth() >= max_depth
    }

    fn decode_split_delta<'a>(b: ByteArray, l: &Loader) -> (&'a [u8], Index) {
        let pg = MainPage::<&[u8], Index>::from(b);
        pg.get(l, 0).expect("invalid delta")
    }

    // NOTE: more than one `split-delta` is impossible appears in delta chain and since we always do
    // consolidating before split, the possible delta chain:
    // add -> del -> add -> split -> base
    fn collect_delta<'a, K, V>(
        &self,
        txn: &mut SysTxn,
        view: &View,
    ) -> (PageMergeIter<'a, K, V, Loader>, MainPageHdr)
    where
        K: IKey,
        V: IVal,
    {
        let cap = view.info.depth() as usize;
        let mut builder = MergeIterBuilder::new(cap);
        let mut info = view.info;
        let mut split = None;

        // intl page never perform partial consolidation
        let _ = self.walk_page(view.page_addr, |frame, _, l, pg| {
            let h = pg.header();
            txn.gc(frame);

            match h.delta_type() {
                DeltaType::Data => {
                    builder.add(RangeIter::new(pg.clone(), l.clone(), 0..h.elems as usize));
                }
                DeltaType::Split => {
                    if split.is_none() {
                        let (key, _) = Self::decode_split_delta(pg.raw(), l);
                        split = Some(key);
                    }
                }
            }

            info = *h;
            false
        });

        let iter = PageMergeIter::new(builder.build(), split);
        (iter, info)
    }

    fn get_txid(&self) -> u64 {
        self.store.context.wmk_oldest()
    }

    fn do_consolidate<'a, F, I, K, V>(&self, mut view: View, f: F) -> Result<View, OpCode>
    where
        F: Fn(PageMergeIter<'a, K, V, Loader>, u64) -> I,
        I: IPageIter<Item = (K, V)>,
        K: IKey,
        V: IVal,
    {
        let mut txn = self.begin();
        let (iter, info) = self.collect_delta(&mut txn, &view);
        let iter = f(iter, self.get_txid());
        let mut delta =
            Delta::new(view.info.delta_type(), view.info.node_type()).from(iter, txn.page_size());
        let mut frame = txn.alloc(delta.size())?;
        let (new_addr, new_page) = (frame.addr(), frame.payload());
        debug_assert!(new_addr > view.page_addr);

        let h = delta.build(new_page, &mut txn);
        h.set_epoch(view.info.epoch());
        h.set_depth(info.depth());
        h.set_link(info.link());

        txn.update(view.page_id, view.page_addr, &mut frame)
            .map(|_| {
                view.page_addr = new_addr;
                view.info = new_page.into();
                view
            })
            .map_err(|_| OpCode::Again)
    }

    fn add_sibling<'a, T, F>(
        &self,
        raw: &'a [u8],
        l: &Loader,
        b: &mut LeafMergeIter<'a, T>,
        mut addr: u64,
        mut save_fn: F,
    ) where
        T: IValCodec,
        F: FnMut(FrameOwner),
    {
        while addr != 0 {
            let frame = self.store.buffer.load(addr);
            let pg = SibPage::<Ver, Value<T>>::from(frame.payload());
            let h = pg.header();
            save_fn(frame);

            for i in 0..h.elems as usize {
                let (k, v) = pg.get(l, i).unwrap();
                b.add(Key::new(raw, k.txid, k.cmd), v);
            }
            addr = h.link();
        }
    }

    fn collect_leaf<T: IValCodec>(
        &self,
        txn: &mut SysTxn,
        view: &View,
        b: &mut LeafMergeIter<'_, T>,
    ) -> MainPageHdr {
        let mut info = view.info;
        let mut split = None;
        let mut sz = MAINPG_HDR_LEN;
        let limit = self.store.opt.max_data_size();
        let is_root = self.is_root(view.page_id);

        let _ = self.walk_page(
            view.page_addr,
            |frame, _, loader, pg: MainPage<Key, Value<T>>| {
                let h = pg.header();
                txn.gc(frame);

                debug_assert!(h.is_leaf());

                if h.is_data() {
                    for i in 0..h.elems as usize {
                        let (k, v) = pg.get(loader, i).unwrap();
                        if let Some(sp) = split {
                            if k.raw >= sp {
                                // split was happened only after consolidation, thus only the base page
                                // will contain keys > split key, the rest kvs were splitted to another
                                // node, so break
                                debug_assert!(h.is_base());
                                break;
                            }
                        }
                        if !is_root {
                            sz += k.packed_size() + v.packed_size() + SLOT_LEN;
                            // we must consume the split-delta to make sure there's at most one split -
                            // delta in delta chain
                            if sz > limit && split.is_none() {
                                return true;
                            }
                        }
                        if let Some(s) = v.sibling() {
                            // add latest version first
                            b.add(k, s.to(*v.as_ref()));
                            self.add_sibling::<T, _>(k.raw, loader, b, s.addr(), |f| txn.gc(f));
                        } else {
                            b.add(k, v);
                        }
                    }
                } else if split.is_none() {
                    let (k, _) = Self::decode_split_delta(pg.raw(), loader);
                    split = Some(k);
                }

                info = *h;
                false
            },
        );
        info
    }

    fn consolidate_leaf<T: IValCodec>(&self, mut view: View) -> Result<View, OpCode> {
        let cap = view.info.depth() as usize;
        let mut iter = LeafMergeIter::new(cap);
        let mut txn = self.begin();
        let info = self.collect_leaf(&mut txn, &view, &mut iter);
        iter.purge(self.get_txid());

        let mut builder = FuseBuilder::<T>::new(info.delta_type(), info.node_type());
        builder.prepare(iter, &txn);
        let (mut pg, mut f) = builder.build(&mut txn)?;
        let new_addr = f.addr();
        debug_assert!(new_addr > view.page_addr);

        let h = pg.header_mut();
        h.set_epoch(view.info.epoch());
        h.set_depth(info.depth());
        h.set_link(info.link());

        debug_assert_eq!(h.delta_type(), info.delta_type());
        debug_assert_eq!(h.node_type(), info.node_type());

        txn.update(view.page_id, view.page_addr, &mut f)
            .map(|_| {
                view.page_addr = new_addr;
                view.info = *h;
                view
            })
            .map_err(|_| OpCode::Again)
    }

    /// NOTE: consolidate never retry
    #[inline]
    fn consolidate<T>(&self, view: View) -> Result<View, OpCode>
    where
        T: IValCodec,
    {
        match view.info.node_type() {
            NodeType::Intl => self.do_consolidate(view, IntlMergeIter::new),
            NodeType::Leaf => self.consolidate_leaf::<T>(view),
        }
    }

    #[inline]
    fn force_consolidate<T>(&self, pid: u64)
    where
        T: IValCodec,
    {
        loop {
            let view = Self::page_view(&self.store, pid);
            if self.consolidate::<T>(view).is_ok() {
                break;
            }
        }
    }

    fn try_consolidate<T>(&self, view: View)
    where
        T: IValCodec,
    {
        if self.need_consolidate(&view.info) {
            if let Ok(v) = self.consolidate::<T>(view) {
                if self.need_split(&v) {
                    let _ = self.split::<T>(v);
                    // TODO: when consolidate result no delta, we should perform `merge`
                }
            }
        }
    }

    /// NOTE: this is the second phase of `split` smo
    fn parent_update<T>(&self, view: View, mut parent: View, range: Range<'_>) -> Result<(), OpCode>
    where
        T: IValCodec,
    {
        assert!(view.info.is_split());

        let lkey = range.key_pq;
        // it's important to apply split-delta's epoch to left entry, this will
        // prevent parent_update from being executed again for the same delta
        let lidx = Index::new(view.page_id, view.info.epoch());
        let f = self.store.buffer.load(view.page_addr);
        let b = f.payload();
        let loader = Loader::new(self.store.buffer.raw());
        let page = MainPage::<&[u8], Index, MainPageHdr>::from(b);
        let (split_key, split_idx) = {
            // the `page` is a `split-delta` see `Self::split_non_root`
            page.get(&loader, 0).expect("invalid page")
        };
        // add placeholder to help find_child
        let entry_delta = if let Some(rkey) = range.key_qr {
            vec![(lkey, lidx), (split_key, split_idx), (rkey, NULL_INDEX)]
        } else {
            vec![(lkey, lidx), (split_key, split_idx)]
        };

        let mut txn = self.begin();
        let mut d =
            Delta::new(DeltaType::Data, NodeType::Intl).with_slice(txn.page_size(), &entry_delta);
        let mut f = txn.alloc(d.size())?;
        let (new_addr, new_page) = (f.addr(), f.payload());
        debug_assert!(new_addr > view.page_addr);

        let h = d.build(new_page, &mut txn);

        let m = &parent.info.meta;
        h.set_depth(m.depth().saturating_add(1));
        h.set_epoch(m.epoch());
        h.set_link(parent.page_addr);

        txn.update(parent.page_id, parent.page_addr, &mut f)
            .map(|_| {
                parent.page_addr = new_addr;
                parent.info = new_page.into();
            })
            .map_err(|_| OpCode::Again)?;
        // force consolidate make it ordered for prefix scan
        self.force_consolidate::<T>(parent.page_id);
        Ok(())
    }

    fn find_page_splitter<K, V>(
        loader: &Loader,
        page: MainPage<K, V>,
    ) -> Option<(K, RangeIter<K, V, Loader>, RangeIter<K, V, Loader>)>
    where
        K: IKey,
        V: IVal,
    {
        let elems = page.header().elems as usize;
        if let Some((sep, _)) = page.get(loader, elems / 2) {
            // NOTE: avoid splitting same key into two pages
            if let Ok(pos) = page.search_raw(loader, &sep) {
                let l = RangeIter::new(page.clone(), loader.clone(), 0..pos);
                let r = RangeIter::new(page.clone(), loader.clone(), pos..elems);
                return Some((page.key_at(loader, pos), l, r));
            } else {
                return None;
            }
        }
        None
    }

    fn split_root<K, V>(&self, view: View) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
    {
        assert_eq!(view.page_id, self.root_index.pid);
        assert_eq!(view.info.epoch(), 0);
        // the root delta chain must be fully consolidated, or else there's no where to link the
        // reset delta chain (either put to left or right child may cause error)
        assert_eq!(view.info.depth(), 1);
        let loader = Loader::new(self.store.buffer.raw());

        let f = self.store.buffer.load(view.page_addr);
        let b = f.payload();
        let page = MainPage::<K, V>::from(b);
        let Some((sep_key, li, ri)) = Self::find_page_splitter(&loader, page) else {
            return Ok(());
        };

        let mut txn = self.begin();
        let pg_sz = txn.page_size();
        let l = {
            let mut d = Delta::new(DeltaType::Data, view.info.node_type()).from(li, pg_sz);
            let mut f = txn.alloc(d.size())?;
            let lpage = f.payload();
            d.build(lpage, &mut txn);
            txn.map(&mut f)
        };
        let r = {
            let mut d = Delta::new(DeltaType::Data, view.info.node_type()).from(ri, pg_sz);
            let mut f = txn.alloc(d.size())?;
            let rpage = f.payload();
            d.build(rpage, &mut txn);
            txn.map(&mut f)
        };

        let s = [
            // add an empty key to simplify operations, such as `find_child`
            // the keys < sep_key are at left, those <= sep_key are at right
            ([].as_slice(), Index::new(l, 0)),
            (sep_key.raw(), Index::new(r, 0)),
        ];

        // the new root
        let mut delta = Delta::new(DeltaType::Data, NodeType::Intl).with_slice(pg_sz, &s);
        let mut f = txn.alloc(delta.size())?;
        let (new_addr, new_page) = (f.addr(), f.payload());
        debug_assert!(new_addr > view.page_addr);

        delta.build(new_page, &mut txn);
        txn.update(view.page_id, view.page_addr, &mut f)
            .map_err(|_| OpCode::Again)
    }

    #[inline(always)]
    fn is_root(&self, pid: u64) -> bool {
        self.root_index.pid == pid
    }

    fn split_non_root<K, V>(&self, view: View) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
    {
        if self.is_root(view.page_id) {
            return self.split_root::<K, V>(view);
        }

        let mut txn = self.begin();
        let loader = Loader::new(self.store.buffer.raw());
        let f = self.store.buffer.load(view.page_addr);
        let b = f.payload();
        let page = MainPage::<K, V>::from(b);
        let Some((sep, _, ri)) = Self::find_page_splitter(&loader, page) else {
            return Ok(());
        };
        let pg_sz = txn.page_size();
        // new split page
        let rpid: u64 = {
            let mut d = Delta::new(DeltaType::Data, view.info.node_type()).from(ri, pg_sz);
            let mut f = txn.alloc(d.size())?;
            let new_page = f.payload();

            d.build(new_page, &mut txn);
            txn.map(&mut f)
        };

        // split-delta contains: split-key + new split page id
        let mut delta = Delta::new(DeltaType::Split, view.info.node_type())
            .with_item(pg_sz, (sep.raw(), Index::new(rpid, 0)));
        let mut f = txn.alloc(delta.size())?;
        let (new_addr, new_page) = (f.addr(), f.payload());
        debug_assert!(new_addr > view.page_addr);
        let h = delta.build(new_page, &mut txn);

        h.set_epoch(view.info.epoch() + 1); // identify a split
        h.set_depth(view.info.depth().saturating_add(1));
        h.set_link(view.page_addr);

        txn.update(view.page_id, view.page_addr, &mut f)
            .map_err(|_| OpCode::Again)?;

        Ok(())
    }

    fn need_split(&self, view: &View) -> bool {
        if view.info.is_split() || view.info.elems < 2 {
            return false;
        }

        let max_size = if view.info.is_split() {
            self.store.opt.intl_split_size
        } else {
            self.store.opt.page_size
        };

        view.info.len >= max_size
    }

    fn split<T>(&self, view: View) -> Result<(), OpCode>
    where
        T: IValCodec,
    {
        debug_assert!(view.info.is_data());
        debug_assert!(view.info.elems > 1);

        match view.info.node_type() {
            NodeType::Intl => self.split_non_root::<&[u8], Index>(view),
            NodeType::Leaf => self.split_non_root::<Key, Value<T>>(view),
        }
    }
}

pub struct SeekIter<'a, 'b, T = Record<'b>>
where
    T: IValCodec + 'b,
{
    tree: &'a Tree,
    curr: Vec<u8>,
    next: Vec<u8>,
    prefix: Vec<u8>,
    frames: VecDeque<FrameOwner>,
    iter: LeafMergeIter<'b, T>,
    check: Box<dyn FnMut(u64, &T) -> bool + 'a>,
    dtor: Box<dyn Fn() + 'a>,
    finish: bool,
    stop: bool,
}

impl<'a, 'b, T> SeekIter<'a, 'b, T>
where
    T: IValCodec + 'b,
{
    pub(crate) fn new<K, F, D>(tree: &'a Tree, prefix: K, f: F, dtor: D) -> Self
    where
        K: AsRef<[u8]>,
        F: FnMut(u64, &T) -> bool + 'a,
        D: Fn() + 'a,
    {
        let prefix = prefix.as_ref().to_vec();
        let mut this = Self {
            tree,
            curr: Self::pre_prefix(&prefix),
            next: Self::next_prefix(&prefix),
            prefix,
            frames: VecDeque::new(),
            iter: LeafMergeIter::new(64),
            check: Box::new(f),
            dtor: Box::new(dtor),
            finish: false,
            stop: false,
        };
        this.collect_frames();
        this
    }

    fn next_prefix(prefix: &[u8]) -> Vec<u8> {
        debug_assert!(!prefix.is_empty());
        let mut x = prefix.to_vec();
        if *x.last().unwrap() == u8::MAX {
            x.push(0);
        } else {
            *x.last_mut().unwrap() += 1;
        }
        x
    }

    fn pre_prefix(prefix: &[u8]) -> Vec<u8> {
        debug_assert!(!prefix.is_empty());
        let mut x = prefix.to_vec();
        if *x.last().unwrap() > 0 {
            *x.last_mut().unwrap() -= 1;
        } else {
            x.pop();
        }
        x
    }

    #[inline]
    fn has_prefix(pg: &MainPage<Key, Value<T>>, l: &Loader, pos: usize, prefix: &[u8]) -> bool {
        pg.key_at(l, pos).raw.starts_with(prefix)
    }

    fn collect_frames(&mut self) {
        if !self.finish {
            if let Some((addr, next)) = self.tree.possible_successor::<T>(&self.curr, &self.prefix)
            {
                self.iter.reset();
                let mut key = Key::new([].as_slice(), NULL_ORACLE, NULL_CMD);
                self.tree
                    .walk_page(addr, |frame, _, loader, pg: MainPage<Key, Value<T>>| {
                        let h = pg.header();
                        if h.is_data() {
                            debug_assert!(h.is_leaf());
                            key.raw = &self.curr;
                            let lo = pg.search_raw(loader, &key).unwrap_or_else(|x| x);
                            if lo == h.elems as usize
                                || !Self::has_prefix(&pg, loader, lo, &self.prefix)
                            {
                                return false;
                            }
                            key.raw = &self.next;
                            let hi = pg.search_raw(loader, &key).unwrap_or_else(|x| x);
                            for i in lo..hi {
                                let (k, v) = pg.get(loader, i).unwrap();
                                if let Some(s) = v.sibling() {
                                    self.iter.add(k, s.to(*v.as_ref()));
                                    self.tree.add_sibling(
                                        k.raw,
                                        loader,
                                        &mut self.iter,
                                        s.addr(),
                                        |f| self.frames.push_back(f),
                                    );
                                } else {
                                    self.iter.add(k, v);
                                }
                            }
                            self.frames.push_back(frame);
                        }
                        false
                    })
                    .unwrap();
                // retain visible and latest keys
                self.iter
                    .retain(|&(k, v)| (*self.check)(k.txid, v.as_ref()));
                self.curr = next.unwrap_or_default().to_vec();
                self.finish = self.curr.is_empty();
            }
            return;
        }
        self.stop = true;
    }

    fn get_next(&mut self) -> Option<<Self as Iterator>::Item> {
        while !self.stop {
            if let Some((k, v)) = self.iter.next() {
                return Some((k.raw, v.as_ref().data()));
            }

            self.collect_frames();
        }
        (*self.dtor)();
        None
    }
}

impl<'b, T> Iterator for SeekIter<'_, 'b, T>
where
    T: IValCodec + 'b,
{
    type Item = (&'b [u8], &'b [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;
    use std::sync::Arc;

    use super::{Store, Tree};
    use crate::store::recovery::Recovery;
    use crate::utils::traits::IValCodec;

    use crate::{
        OpCode, Options,
        index::{Key, data::Value},
    };
    use crate::{ROOT_PID, RandomPath};

    struct TreeRef {
        tree: Tree,
    }

    impl Deref for TreeRef {
        type Target = Tree;
        fn deref(&self) -> &Self::Target {
            &self.tree
        }
    }

    impl Drop for TreeRef {
        fn drop(&mut self) {
            self.tree.store.quit();
        }
    }

    fn new_tree(opt: Options) -> TreeRef {
        let _ = std::fs::remove_dir_all(&opt.db_root);
        let opt = Arc::new(opt.validate().unwrap());
        let (meta, table, mapping, desc) = Recovery::new(opt.clone()).phase1();
        let store = Arc::new(Store::new(table, opt, meta.clone(), mapping, &desc).unwrap());
        TreeRef {
            tree: Tree::new(store, ROOT_PID),
        }
    }

    #[test]
    fn traverse() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let opt = Options::new(&*path);
        let s = new_tree(opt);

        s.put(
            Key::new("elder".as_bytes(), 1, 0),
            Value::Put("naive".as_bytes()),
        )?;
        s.put(
            Key::new("elder".as_bytes(), 3, 1),
            Value::Put("ha".as_bytes()),
        )?;
        let mut out = None;
        let r = s.traverse::<&[u8], _>(Key::new("elder".as_bytes(), 2, 0), |txid, v| {
            if txid < 2 {
                out = Some(IValCodec::to_string(v));
                true
            } else {
                false
            }
        });
        assert!(r.is_ok());
        assert_eq!(out, Some("naive".to_string()));
        Ok(())
    }

    #[test]
    fn traverse2() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.consolidate_threshold = 4;
        let s = new_tree(opt);

        let raw = "1".as_bytes();

        s.put(Key::new(raw, 1, 0), Value::Put("10".as_bytes()))?;
        s.put(Key::new(raw, 1, 1), Value::Put("11".as_bytes()))?;
        s.put(Key::new(raw, 2, 0), Value::Put("12".as_bytes()))?;
        s.put(Key::new(raw, 2, 1), Value::Put("13".as_bytes()))?;
        s.put(Key::new(raw, 3, 0), Value::Put("14".as_bytes()))?;
        s.put(Key::new(raw, 3, 0), Value::Put("15".as_bytes()))?;

        let r = s.traverse::<&[u8], _>(Key::new(raw, 1, u32::MAX), |txid, _v| txid < 2);
        assert!(r.is_ok());
        let out = r.unwrap();
        assert_eq!(out.slice(), "11".as_bytes());

        Ok(())
    }
}
