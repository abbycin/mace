use super::{
    data::{Index, Key, Value},
    iter::{ItemIter, MergeIterBuilder},
    page::{
        DeltaType, IntlMergeIter, IntlPage, LeafMergeIter, LeafPage, NodeType, Page, PageHeader,
        PageMergeIter, RangeIter, PAGE_HEADER_SIZE,
    },
    slotted::SlottedPage,
    systxn::SysTxn,
};

use crate::utils::NULL_ORACLE;
use crate::{
    cc::data::Ver,
    index::{
        builder::{Delta, FuseBuilder},
        page::NULL_INDEX,
    },
    map::data::FrameOwner,
    utils::{
        bytes::ByteArray,
        traits::{ICodec, IKey, IPageIter, IVal, IValCodec},
        unpack_id, NULL_CMD, NULL_PID, ROOT_PID,
    },
    OpCode, Store,
};
use std::{collections::VecDeque, sync::Arc};

struct RegistryIter<'a> {
    iter: PageMergeIter<'a, Key<'a>, Value<&'a [u8]>>,
    last: Option<&'a [u8]>,
}

impl<'a> RegistryIter<'a> {
    fn new(iter: PageMergeIter<'a, Key<'a>, Value<&'a [u8]>>, _txid: u64) -> Self {
        Self { iter, last: None }
    }
}

impl<'a> Iterator for RegistryIter<'a> {
    type Item = (Key<'a>, Value<&'a [u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        for (k, v) in &mut self.iter {
            if let Some(last) = self.last {
                if k.raw == last {
                    continue;
                }
            }
            self.last = Some(k.raw);
            return Some((k, v));
        }
        None
    }
}

impl IPageIter for RegistryIter<'_> {
    fn rewind(&mut self) {
        self.iter.rewind();
        self.last = None;
    }
}

#[derive(Clone, Copy)]
pub struct Range<'a> {
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

    pub fn data(&self) -> &[u8] {
        self.raw.as_ref().data()
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

#[derive(Clone, Copy)]
pub struct View<'a> {
    pub page_id: u64,
    pub page_addr: u64,
    pub info: PageHeader,
    /// left and right boundary key of current page, key_pq is routing to current page, while key_qr
    /// is routing to new allocated split page
    pub range: Range<'a>,
}

#[derive(Clone)]
pub struct Tree {
    pub(crate) store: Arc<Store>,
    root_index: Index,
    id: u64,
}

impl Tree {
    pub fn load(store: Arc<Store>, root_pid: u64, id: u64) -> Self {
        Self {
            store: store.clone(),
            root_index: Index::new(root_pid, 0),
            id,
        }
    }

    pub fn new(store: Arc<Store>, root_pid: u64, id: u64) -> Self {
        let this = Self {
            store: store.clone(),
            root_index: Index::new(root_pid, 0),
            id,
        };

        this.init();

        this
    }

    pub fn init(&self) {
        // build an empty page with no key-value
        let iter: ItemIter<(Key<'_>, Value<&[u8]>)> = ItemIter::default();
        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).from(iter);
        let mut txn = SysTxn::new(&self.store);
        let mut f = txn.alloc(delta.size()).expect("can't alloc memory");
        let mut page = Page::from(f.payload());
        delta.build(&mut page);
        assert_eq!(page.header().epoch(), 0);
        txn.update_unchecked(self.root_index.pid, &mut f);
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn root_pid(&self) -> u64 {
        self.root_index.pid
    }

    fn remove_dfs(&self, pid: u64) {
        let mut link = self.store.page.get(pid);
        let saved = link;

        while link != NULL_PID {
            let f = self.store.buffer.load(link);
            let h: PageHeader = f.payload().into();

            if h.is_leaf() && h.is_split() {
                let pg = Page::<&[u8], Index>::from(f.payload());
                let idx = pg.val_at(0);
                // the split-delta may be already unmapped, because it may co-exist in leaf and intl
                // we must perform CAS in case the pid was reused
                let _ = self.store.page.unmap(idx.pid, link).inspect_err(|_| {
                    log::warn!("pid {} to {:?} already unmapped", idx.pid, unpack_id(link));
                });
                break;
            }

            if h.is_intl() {
                let pg = Page::<&[u8], Index>::from(f.payload());
                for i in (0..h.elems as usize).rev() {
                    let idx = pg.val_at(i);
                    self.remove_dfs(idx.pid);
                }
            }
            link = h.link();
        }

        let _ = self.store.page.unmap(pid, saved).inspect_err(|_| {
            log::warn!("pid {} to {:?} already unmapped", pid, unpack_id(saved));
        });
    }

    // TODO: Garbage collect the unmapped frames (including it's sibling)
    // caller must ensure we are the only one who is accessing the tree
    pub fn remove_all(&self) {
        self.remove_dfs(self.root_index.pid);
    }

    #[inline]
    pub(crate) fn begin(&self) -> SysTxn {
        SysTxn::new(&self.store)
    }

    #[inline]
    pub fn is_mgr(&self) -> bool {
        self.root_index.pid == ROOT_PID
    }

    fn walk_page<F, K, V>(&self, mut addr: u64, mut f: F) -> Result<(), OpCode>
    where
        F: FnMut(FrameOwner, u64, Page<K, V>) -> bool,
        K: IKey,
        V: IVal,
    {
        while addr != 0 {
            let frame = self.store.buffer.load(addr);
            let page = Page::<K, V>::from(frame.payload());
            let next = page.header().link();

            // NOTE: frame is moved
            if f(frame, addr, page) {
                break;
            }
            addr = next;
        }
        Ok(())
    }

    fn find_leaf<T>(
        &self,
        txn: &mut SysTxn,
        key: &[u8],
    ) -> Result<(View<'_>, Option<View<'_>>), OpCode>
    where
        T: IValCodec,
    {
        loop {
            txn.unpin_all();
            match self.try_find_leaf::<T>(txn, key) {
                Ok((view, parent)) => return Ok((view, parent)),
                Err(OpCode::Again) => continue,
                Err(e) => unreachable!("invalid error {:?}", e),
            }
        }
    }

    pub(crate) fn page_view<'b>(
        store: &Store,
        pid: u64,
        range: Range<'b>,
    ) -> (FrameOwner, View<'b>) {
        let addr = store.page.get(pid);
        assert_ne!(addr, 0);
        let f = store.buffer.load(addr);
        let b = f.payload();
        (
            f,
            View {
                page_id: pid,
                page_addr: addr,
                info: b.into(),
                range,
            },
        )
    }

    fn try_find_leaf<T>(
        &self,
        txn: &mut SysTxn,
        key: &[u8],
    ) -> Result<(View<'_>, Option<View<'_>>), OpCode>
    where
        T: IValCodec,
    {
        let mut index = self.root_index;
        let mut range = Range::new();
        let mut parent = None;

        loop {
            let (frame, view) = Self::page_view(&self.store, index.pid, range);
            txn.pin(frame);

            // split may happen during search, in this case new created node are
            // not inserted into parent yet, the insert is halfly done, the
            // search can reach the `split delta` whose epoch are not equal to
            // previous one, and any modification operation CAN NOT proceed,
            // since we simplified the `find_child` process to handle data only,
            // if any other modification operation is allowed, will cause leaf
            // node disordered, by the way, any operation (including lookup)
            // will go through this function and check the conditionW
            if view.info.epoch() != index.epoch {
                if self.parent_update::<T>(txn, view, parent).is_ok() {
                    // get rid of those keys were moved to new node, for prefix scan
                    while self.consolidate::<T>(txn, view).is_err() {
                        std::hint::spin_loop();
                    }
                }
                // simplified: retry from root, thanks to the large fanout
                return Err(OpCode::Again);
            }

            if view.info.is_leaf() {
                return Ok((view, parent));
            }
            parent = Some(view);

            let (child_index, child_range) = self
                .find_child(txn, key, view.page_addr)
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
        txn: &mut SysTxn,
        key: &[u8],
        addr: u64,
    ) -> Option<(Index, Range<'a>)> {
        let mut child = None;

        // stop when the child is in range
        let _ = self.walk_page(addr, |frame, _, pg: Page<&[u8], Index>| {
            debug_assert!(pg.header().is_intl());

            // skip inner `split-delta`
            if pg.header().is_data() {
                let (l, r) = match pg.search(&key) {
                    // equal to key, the range of child's key: [pos, pos+1)
                    Ok(pos) => (
                        pg.get(pos),
                        pos.checked_add(1).and_then(|next| pg.get(next)),
                    ),
                    // it's insert pos, the range of child's key: [pos - 1, pos)
                    // since the intl node has key-index paired, and the first
                    // key of intl node is `empty`, for example: in an extreme
                    // situation, the key may smaller than any other key in the
                    // node except the first one, then it's child page's index
                    // is at pos - 1, so the child's key range is: [pos-1, pos)
                    // see `split_root`
                    Err(pos) => (
                        pos.checked_sub(1).and_then(|prev| pg.get(prev)),
                        pg.get(pos),
                    ),
                };
                if let Some((key_pq, index)) = l {
                    if index != NULL_INDEX {
                        let range = Range {
                            key_pq,
                            key_qr: r.map(|(key_qr, _)| key_qr),
                        };
                        txn.pin(frame);
                        child = Some((index, range));
                        return true;
                    }
                }
            }
            false
        });
        child
    }

    fn possible_successor<T>(
        &self,
        txn: &mut SysTxn,
        key: &[u8],
        prefix: &[u8],
    ) -> Option<(FrameOwner, Option<&[u8]>)>
    where
        T: IValCodec,
    {
        let mut index = self.root_index;
        let mut range = Range::new();
        let mut parent = None;
        let mut successor = None;
        let mut found = false;

        loop {
            let (frame, view) = Self::page_view(&self.store, index.pid, range);
            if view.info.epoch() != index.epoch {
                if self.parent_update::<T>(txn, view, parent).is_ok() {
                    let _ = self.consolidate::<T>(txn, view);
                }
                // reset and retry
                index = self.root_index;
                range = Range::new();
                parent = None;
                successor = None;
                found = false;
                txn.unpin_all();
                continue;
            }

            if view.info.is_leaf() {
                return Some((frame, successor));
            }
            parent = Some(view);
            txn.pin(frame);

            let mut child = None;
            let _ = self.walk_page(view.page_addr, |frame, _, pg: Page<&[u8], Index>| {
                let h = pg.header();
                if h.is_data() {
                    let (pos, l, r) = match pg.search(&key) {
                        Ok(pos) => (
                            Some(pos),
                            pg.get(pos),
                            pos.checked_add(1).and_then(|next| pg.get(next)),
                        ),
                        Err(pos) => (
                            pos.checked_sub(1),
                            pos.checked_sub(1).and_then(|prev| pg.get(prev)),
                            pg.get(pos),
                        ),
                    };

                    debug_assert!((pos.is_some() && l.is_some()) || (pos.is_none() && l.is_none()));

                    if let Some((ikey, index)) = l {
                        let pos = pos.expect("can't be None");
                        txn.pin(frame);
                        // we will continue to find the insert position even `found` was set, it's
                        // necessary for performing `parent_update` correctly
                        if !found {
                            found = true;
                            // move to next node
                            if let Some((k, _)) = pg.get(pos + 1) {
                                if k.starts_with(prefix) {
                                    successor = Some(k);
                                }
                            }
                        }
                        if index != NULL_INDEX {
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
            });

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
        txn: &mut SysTxn,
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
        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).with_item((*key, *val));
        // NOTE: restrict logical node size
        if delta.size() > self.store.opt.page_size - PAGE_HEADER_SIZE {
            return Err(OpCode::TooLarge);
        }
        let mut f = txn.alloc(delta.size())?;
        let (new_addr, mut new_page) = (f.addr(), Page::from(f.payload()));
        debug_assert!(new_addr > view.page_addr);
        delta.build(&mut new_page);

        let h = new_page.header_mut();
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
                        let hdr: PageHeader = b.into();
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
        let mut txn = self.begin();
        let (mut view, _) = self.find_leaf::<T>(&mut txn, key.raw()).unwrap();

        if self.need_split(&view) && self.split::<T>(&mut txn, view).is_ok() {
            return Err(OpCode::Again);
        }

        self.prepend(&mut txn, &mut view, key, val, |_, _| Ok(()))?;

        let _ = self.try_consolidate::<T>(&mut txn, view);

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
        let _ = self.walk_page(addr, |f, _, pg: Page<Key, Value<T>>| {
            let h = pg.header();

            if !h.is_data() {
                return false;
            }
            debug_assert!(h.is_leaf());
            if let Ok(pos) = pg.search_raw(key) {
                let (k, v) = pg.get(pos).unwrap();
                r = Some((k, ValRef::new(v, f.clone())));
                return true;
            }
            false
        });
        r
    }

    fn try_update<T1, T2, V>(
        &self,
        txn: &mut SysTxn,
        key: &Key,
        val: &Value<T1>,
        visible: &mut V,
    ) -> Result<Option<ValRef<T2>>, OpCode>
    where
        T1: IValCodec,
        T2: IValCodec,
        V: FnMut(&Option<(Key, ValRef<T2>)>) -> Result<(), OpCode>,
    {
        let (mut view, _) = self.find_leaf::<T2>(txn, key.raw).unwrap();
        if self.need_split(&view) && self.split::<T2>(txn, view).is_ok() {
            return Err(OpCode::Again);
        }
        let mut r = None;
        self.prepend(txn, &mut view, key, val, |addr, key| {
            r = self.search(addr, key);
            visible(&r)
        })?;

        let _ = self.try_consolidate::<T2>(txn, view);
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
        let mut txn = self.begin();
        loop {
            match self.try_update(&mut txn, &key, &val, &mut visible) {
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
            let _ = self.walk_page(addr, |f, addr, _: Page<Key, Value<T>>| {
                let h: PageHeader = f.payload().into();
                if h.is_intl() {
                    let pg = IntlPage::from(f.payload());
                    let mut pids = Vec::with_capacity(h.elems as usize);
                    for i in 0..h.elems as usize {
                        if let Some((_, v)) = pg.get(i) {
                            pids.push(v.pid);
                            q.push_back(v.pid);
                        }
                    }
                    log::info!("intl {} => {:?}", f.page_id(), pids);
                }

                if h.is_leaf() && !h.is_split() {
                    log::info!("pid {}", f.page_id());
                    let pg = LeafPage::<T>::from(f.payload());
                    for i in 0..h.elems as usize {
                        let (k, v) = pg.get(i).unwrap();
                        log::info!(
                            "{} => {}\t{:?}",
                            k.to_string(),
                            v.to_string(),
                            unpack_id(addr)
                        );
                        if let Some(s) = v.sibling() {
                            let f = self.store.buffer.load(s.addr());
                            let slotted = SlottedPage::<Ver, T>::from(f.payload());
                            slotted.show();
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
        let mut txn = self.begin();
        let (view, _) = self.find_leaf::<T>(&mut txn, key.raw())?;
        let mut data = Err(OpCode::NotFound);
        let _ = self.walk_page(view.page_addr, |frame, _, pg: Page<K, Value<T>>| {
            if pg.header().is_data() {
                if let Ok(pos) = pg.search_raw(&key) {
                    let (k, v) = pg.get(pos).unwrap();
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
        visible: &mut F,
    ) -> Result<ValRef<T>, OpCode>
    where
        T: IValCodec,
        F: FnMut(u64, &T) -> bool,
    {
        let ver = Ver::new(start_ts, NULL_CMD);
        while addr != 0 {
            let frame = self.store.buffer.load(addr);
            let page = SlottedPage::<Ver, T>::from(frame.payload());
            let h = page.header();

            let pos = page.lower_bound(&ver).unwrap_or_else(|pos| pos);
            if pos < h.elems as usize {
                let (k, v) = page.get(pos);

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
        let mut txn = self.begin();
        let (view, _) = self.find_leaf::<T>(&mut txn, key.raw)?;
        let mut latest: Option<ValRef<T>> = None;
        let _ = self.walk_page(view.page_addr, |f, _, pg: Page<Key, Value<T>>| {
            let h = pg.header();
            if !h.is_data() {
                return false;
            }
            debug_assert!(h.is_leaf());
            let Ok(pos) = pg.search_raw(&key) else {
                return false;
            };
            let (k, v) = pg.get(pos).unwrap();
            if visible(k.txid, v.as_ref()) {
                latest = Some(ValRef::new(v, f));
                return true;
            }
            // all rest `raw` will be checked here
            if let Some(s) = v.sibling() {
                if let Ok(val) = self.traverse_sibling(key.txid, s.addr(), &mut visible) {
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

    fn need_consolidate(&self, info: &PageHeader) -> bool {
        let mut max_depth = self.store.opt.consolidate_threshold;
        if info.is_intl() {
            // delta has greater impact on inner node
            max_depth /= 2;
        }

        info.depth() >= max_depth
    }

    fn decode_split_delta<'a>(b: ByteArray) -> (&'a [u8], Index) {
        let pg = Page::<&[u8], Index>::from(b);
        pg.get(0).expect("invalid delta")
    }

    // NOTE: more than one `split-delta` is impossible appears in delta chain and since we always do
    // consolidating before split, the possible delta chain:
    // add -> del -> add -> split -> base
    fn collect_delta<'a, K, V>(
        &'a self,
        txn: &mut SysTxn,
        view: &View<'a>,
    ) -> (PageMergeIter<'a, K, V>, PageHeader)
    where
        K: IKey,
        V: IVal,
    {
        let cap = view.info.depth() as usize;
        let mut builder = MergeIterBuilder::new(cap);
        let mut info = view.info;
        let mut split = None;

        // intl page never perform partial consolidation
        let _ = self.walk_page(view.page_addr, |frame, _, pg| {
            let h = pg.header();
            txn.gc(frame);

            match h.delta_type() {
                DeltaType::Data => {
                    builder.add(RangeIter::new(pg, 0..h.elems as usize));
                }
                DeltaType::Split => {
                    if split.is_none() {
                        let (key, _) = Self::decode_split_delta(pg.raw());
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

    fn do_consolidate<'a, F, I, K, V>(
        &'a self,
        txn: &mut SysTxn,
        mut view: View<'a>,
        f: F,
    ) -> Result<View<'a>, OpCode>
    where
        F: Fn(PageMergeIter<'a, K, V>, u64) -> I,
        I: IPageIter<Item = (K, V)>,
        K: IKey,
        V: IVal,
    {
        let (iter, info) = self.collect_delta(txn, &view);
        let iter = f(iter, self.get_txid());
        let mut delta = Delta::new(view.info.delta_type(), view.info.node_type()).from(iter);
        let mut frame = txn.alloc(delta.size())?;
        let (new_addr, mut new_page) = (frame.addr(), Page::from(frame.payload()));
        debug_assert!(new_addr > view.page_addr);

        delta.build(&mut new_page);
        let h = new_page.header_mut();
        h.set_epoch(view.info.epoch());
        h.set_depth(info.depth());
        h.set_link(info.link());

        txn.update(view.page_id, view.page_addr, &mut frame)
            .map(|_| {
                view.page_addr = new_addr;
                view.info = *new_page.header();
                view
            })
            .map_err(|_| OpCode::Again)
    }

    fn add_sibling<'a, T, F>(
        &'a self,
        raw: &'a [u8],
        b: &mut LeafMergeIter<'a, T>,
        mut addr: u64,
        mut save_fn: F,
    ) where
        T: IValCodec,
        F: FnMut(FrameOwner),
    {
        while addr != 0 {
            let frame = self.store.buffer.load(addr);
            let pg = SlottedPage::<Ver, T>::from(frame.payload());
            let h = pg.header();
            save_fn(frame);

            for i in 0..h.elems as usize {
                let (k, v) = pg.get(i);
                b.add(Key::new(raw, k.txid, k.cmd), v);
            }
            addr = h.link();
        }
    }

    fn collect_leaf<'a, T: IValCodec>(
        &'a self,
        txn: &mut SysTxn,
        view: &View<'a>,
        b: &mut LeafMergeIter<'a, T>,
    ) -> PageHeader {
        let mut info = view.info;
        let mut split = None;
        let mut sz = 0;
        let limit = (self.store.opt.page_size - PAGE_HEADER_SIZE) * 2;
        let is_root = self.is_root(view.page_id);

        let _ = self.walk_page(view.page_addr, |frame, _, pg: Page<Key, Value<T>>| {
            let h = pg.header();
            txn.gc(frame);

            debug_assert!(h.is_leaf());

            if h.is_data() {
                for i in 0..h.elems as usize {
                    let (k, v) = pg.get(i).unwrap();
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
                        sz += k.packed_size() + v.packed_size();
                        // we must consume the split-delta to make sure there's at most one split -
                        // delta in delta chain
                        if sz > limit && split.is_none() {
                            return true;
                        }
                    }
                    if let Some(s) = v.sibling() {
                        // add latest version first
                        b.add(k, s.to(*v.as_ref()));
                        self.add_sibling::<T, _>(k.raw, b, s.addr(), |f| txn.gc(f));
                    } else {
                        b.add(k, v);
                    }
                }
            } else if split.is_none() {
                let (k, _) = Self::decode_split_delta(pg.raw());
                split = Some(k);
            }

            info = *h;
            false
        });
        info
    }

    fn consolidate_leaf<'a, T: IValCodec>(
        &'a self,
        txn: &mut SysTxn,
        mut view: View<'a>,
    ) -> Result<View<'a>, OpCode> {
        let cap = view.info.depth() as usize;
        let mut iter = LeafMergeIter::new(cap);
        let info = self.collect_leaf(txn, &view, &mut iter);
        iter.purge(self.get_txid());

        let mut builder = FuseBuilder::<T>::new(info.delta_type(), info.node_type());
        builder.prepare(iter);
        let mut f = builder.build(txn)?;
        let (new_addr, mut new_page) = (f.addr(), Page::<Key, Value<T>>::from(f.payload()));
        debug_assert!(new_addr > view.page_addr);

        let h = new_page.header_mut();
        h.set_epoch(view.info.epoch());
        h.set_depth(info.depth());
        h.set_link(info.link());

        assert_eq!(h.delta_type(), info.delta_type());
        assert_eq!(h.node_type(), info.node_type());

        txn.update(view.page_id, view.page_addr, &mut f)
            .map(|_| {
                view.page_addr = new_addr;
                view.info = *h;
                view
            })
            .map_err(|_| OpCode::Again)
    }

    /// NOTE: consolidate never retry
    fn consolidate<'a, T>(&'a self, txn: &mut SysTxn, view: View<'a>) -> Result<View<'a>, OpCode>
    where
        T: IValCodec + 'a,
    {
        if self.is_mgr() {
            return self.do_consolidate(txn, view, RegistryIter::new);
        }
        match view.info.node_type() {
            NodeType::Intl => self.do_consolidate(txn, view, IntlMergeIter::new),
            NodeType::Leaf => self.consolidate_leaf::<T>(txn, view),
        }
    }

    fn try_consolidate<'a, T>(&'a self, txn: &mut SysTxn, mut view: View<'a>) -> Result<(), OpCode>
    where
        T: IValCodec + 'a,
    {
        if self.need_consolidate(&view.info) {
            view = self.consolidate::<T>(txn, view)?;
            if self.need_split(&view) {
                return self.split::<T>(txn, view);
            }
            // TODO: when consolidate result no delta, we should perform `merge`
            Ok(())
        } else {
            Ok(())
        }
    }

    /// NOTE: this is the second phase of `split` smo
    fn parent_update<'a, T>(
        &self,
        txn: &mut SysTxn,
        view: View,
        parent: Option<View<'a>>,
    ) -> Result<(), OpCode>
    where
        T: IValCodec,
    {
        assert!(view.info.is_split());

        let Some(mut parent) = parent else {
            return Err(OpCode::Invalid);
        };

        let lkey = view.range.key_pq;
        // it's important to apply split-delta's epoch to left entry, this will
        // prevent parent_update from being executed again for the same delta
        let lidx = Index::new(view.page_id, view.info.epoch());
        let f = self.store.buffer.load(view.page_addr);
        let b = f.payload();
        let page = Page::from(b);
        let (split_key, split_idx) = {
            // the `page` is a `split-delta` see `Self::split_non_root`
            page.get(0).expect("invalid page")
        };
        // add placeholder to help find_child
        let entry_delta = if let Some(rkey) = view.range.key_qr {
            vec![(lkey, lidx), (split_key, split_idx), (rkey, NULL_INDEX)]
        } else {
            vec![(lkey, lidx), (split_key, split_idx)]
        };

        let mut d = Delta::new(DeltaType::Data, NodeType::Intl).with_slice(&entry_delta);
        let mut f = txn.alloc(d.size())?;
        let (new_addr, mut new_page) = (f.addr(), Page::from(f.payload()));
        debug_assert!(new_addr > view.page_addr);

        d.build(&mut new_page);

        let m = &parent.info.meta;
        let h = new_page.header_mut();
        h.set_depth(m.depth().saturating_add(1));
        h.set_epoch(m.epoch());
        h.set_link(parent.page_addr);

        txn.update(parent.page_id, parent.page_addr, &mut f)
            .map(|_| {
                parent.page_addr = new_addr;
                parent.info = *new_page.header();
            })
            .map_err(|_| OpCode::Again)?;
        // force consolidate make it ordered for prefix scan
        while self.consolidate::<T>(txn, parent).is_err() {
            std::hint::spin_loop();
        }
        Ok(())
    }

    fn find_page_splitter<K, V>(page: Page<K, V>) -> Option<(K, RangeIter<K, V>, RangeIter<K, V>)>
    where
        K: IKey,
        V: IVal,
    {
        let elems = page.header().elems as usize;
        if let Some((sep, _)) = page.get(elems / 2) {
            // NOTE: avoid splitting same key into two pages
            if let Ok(pos) = page.search_raw(&sep) {
                let l = RangeIter::new(page, 0..pos);
                let r = RangeIter::new(page, pos..elems);
                return Some((page.key_at(pos), l, r));
            } else {
                return None;
            }
        }
        None
    }

    fn split_root<K, V>(&self, txn: &mut SysTxn, view: View) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
    {
        assert_eq!(view.page_id, self.root_index.pid);
        assert_eq!(view.info.epoch(), 0);
        // the root delta chain must be fully consolidated, or else there's no where to link the
        // reset delta chain (either put to left or right child may cause error)
        assert_eq!(view.info.depth(), 1);

        let f = self.store.buffer.load(view.page_addr);
        let b = f.payload();
        let page = Page::<K, V>::from(b);
        let Some((sep_key, li, ri)) = Self::find_page_splitter(page) else {
            return Ok(());
        };

        let l = {
            let mut d = Delta::new(DeltaType::Data, view.info.node_type()).from(li);
            let mut f = txn.alloc(d.size())?;
            let mut lpage = Page::from(f.payload());
            d.build(&mut lpage);
            txn.map(&mut f)
        };
        let r = {
            let mut d = Delta::new(DeltaType::Data, view.info.node_type()).from(ri);
            let mut f = txn.alloc(d.size())?;
            let mut rpage = Page::from(f.payload());
            d.build(&mut rpage);
            txn.map(&mut f)
        };

        let s = [
            // add an empty key to simplify operations, such as `find_child`
            // the keys < sep_key are at left, those <= sep_key are at right
            ([].as_slice(), Index::new(l, 0)),
            (sep_key.raw(), Index::new(r, 0)),
        ];

        // the new root
        let mut delta = Delta::new(DeltaType::Data, NodeType::Intl).with_slice(&s);
        let mut f = txn.alloc(delta.size())?;
        let (new_addr, mut new_page) = (f.addr(), Page::from(f.payload()));
        debug_assert!(new_addr > view.page_addr);

        delta.build(&mut new_page);
        txn.update(view.page_id, view.page_addr, &mut f)
            .map_err(|_| OpCode::Again)
    }

    #[inline(always)]
    fn is_root(&self, pid: u64) -> bool {
        self.root_index.pid == pid
    }

    fn split_non_root<K, V>(&self, txn: &mut SysTxn, view: View) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
    {
        if self.is_root(view.page_id) {
            return self.split_root::<K, V>(txn, view);
        }

        let f = self.store.buffer.load(view.page_addr);
        let b = f.payload();
        let page = Page::<K, V>::from(b);
        let Some((sep, _, ri)) = Self::find_page_splitter(page) else {
            return Ok(());
        };
        // new split page
        let rpid: u64 = {
            let mut d = Delta::new(DeltaType::Data, view.info.node_type()).from(ri);
            let mut f = txn.alloc(d.size())?;
            let mut new_page = Page::from(f.payload());

            d.build(&mut new_page);
            txn.map(&mut f)
        };

        // split-delta contains: split-key + new split page id
        let mut delta = Delta::new(DeltaType::Split, view.info.node_type())
            .with_item((sep.raw(), Index::new(rpid, 0)));
        let mut f = txn.alloc(delta.size())?;
        let (new_addr, mut new_page) = (f.addr(), Page::from(f.payload()));
        debug_assert!(new_addr > view.page_addr);
        delta.build(&mut new_page);

        let h = new_page.header_mut();
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

        let mut max_size = self.store.opt.page_size as u32;
        if view.info.is_intl() {
            max_size /= 2;
        }
        view.info.len >= max_size
    }

    fn split<T>(&self, txn: &mut SysTxn, view: View) -> Result<(), OpCode>
    where
        T: IValCodec,
    {
        assert!(view.info.is_data());
        assert!(view.info.elems > 1);

        match view.info.node_type() {
            NodeType::Intl => self.split_non_root::<&[u8], Index>(txn, view),
            NodeType::Leaf => self.split_non_root::<Key, Value<T>>(txn, view),
        }
    }
}

pub struct SeekIter<'a, T>
where
    T: IValCodec,
{
    tree: &'a Tree,
    curr: Vec<u8>,
    next: Vec<u8>,
    prefix: &'a [u8],
    frames: VecDeque<FrameOwner>,
    iter: LeafMergeIter<'a, T>,
    check: Box<dyn FnMut(u64, &T) -> bool>,
    finish: bool,
    stop: bool,
}

impl<'a, T> SeekIter<'a, T>
where
    T: IValCodec + 'a,
{
    pub(crate) fn new<K, F>(tree: &'a Tree, prefix: K, f: F) -> Self
    where
        K: AsRef<[u8]>,
        F: FnMut(u64, &T) -> bool + 'static,
    {
        let prefix = Self::extend_lifetime(prefix.as_ref());

        let mut this = Self {
            tree,
            curr: Self::pre_prefix(prefix),
            next: Self::next_prefix(prefix),
            prefix,
            frames: VecDeque::new(),
            iter: LeafMergeIter::new(64),
            check: Box::new(f),
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
    fn has_prefix(pg: &Page<Key, Value<T>>, pos: usize, prefix: &[u8]) -> bool {
        pg.key_at(pos).raw.starts_with(prefix)
    }

    fn collect_frames(&mut self) {
        if !self.finish {
            let mut txn = self.tree.begin();
            if let Some((f, next)) =
                self.tree
                    .possible_successor::<T>(&mut txn, &self.curr, self.prefix)
            {
                self.iter.reset();
                let mut key = Key::new([].as_slice(), NULL_ORACLE, NULL_CMD);
                self.tree
                    .walk_page(f.addr(), |frame, _, pg: Page<Key, Value<T>>| {
                        let h = pg.header();
                        if h.is_data() {
                            debug_assert!(h.is_leaf());
                            key.raw = &self.curr;
                            let lo = pg.search_raw(&key).unwrap_or_else(|x| x);
                            if lo == h.elems as usize || !Self::has_prefix(&pg, lo, self.prefix) {
                                return false;
                            }
                            key.raw = &self.next;
                            let hi = pg.search_raw(&key).unwrap_or_else(|x| x);
                            for i in lo..hi {
                                let (k, v) = pg.get(i).unwrap();
                                if let Some(s) = v.sibling() {
                                    self.iter.add(k, s.to(*v.as_ref()));
                                    self.tree.add_sibling(k.raw, &mut self.iter, s.addr(), |f| {
                                        self.frames.push_back(f)
                                    });
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

    #[inline]
    const fn extend_lifetime(x: &[u8]) -> &'static [u8] {
        unsafe { std::slice::from_raw_parts(x.as_ptr(), x.len()) }
    }

    fn get_next(&mut self) -> Option<<Self as Iterator>::Item> {
        while !self.stop {
            if let Some((k, v)) = self.iter.next() {
                return Some((k.raw, v.as_ref().data()));
            }

            self.collect_frames();
        }
        None
    }
}

impl<'a, T> Iterator for SeekIter<'a, T>
where
    T: IValCodec + 'a,
{
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next()
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;
    use std::sync::Arc;

    use super::{Store, Tree};
    use crate::index::registry::Registry;
    use crate::store::recovery::Recovery;
    use crate::utils::traits::IValCodec;

    use crate::RandomPath;
    use crate::{
        index::{data::Value, Key},
        utils::ROOT_PID,
        OpCode, Options,
    };

    fn new_mgr<T: AsRef<Path>>(path: T) -> Result<Registry, OpCode> {
        let _ = std::fs::remove_dir_all(&path);
        let opt = Arc::new(Options::new(&path));
        let (meta, table, mapping) = Recovery::new(opt.clone()).phase1();
        let store = Arc::new(Store::new(table, opt, meta.clone(), mapping).unwrap());
        Ok(Registry::new(store))
    }

    fn put_test(t: &Tree, k: Key, v: Value<&[u8]>) {
        assert!(t.put(k, v).is_ok());
    }

    fn get_test<const OK: bool>(t: &Tree, k: Key, v: Value<&[u8]>) {
        let (rk, rv) = t.get::<Key, &[u8]>(k).unwrap();
        assert!(k == rk);
        if OK {
            assert!(rv.is_put());
            assert!(rv.raw.as_ref() == v.as_ref());
        } else {
            assert!(rv.is_del());
        }
    }

    fn del_test(t: &Tree, k: Key, v: Value<&[u8]>) {
        t.put(k, v).unwrap();
    }

    #[test]
    fn multiple_tree() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let mgr = new_mgr(path.clone())?;

        let sb1 = mgr.create_tree()?;
        let sb2 = mgr.create_tree()?;
        let (k1, v1) = (
            Key::new("elder".as_bytes(), 1, 0),
            Value::Put("naive".as_bytes()),
        );
        let (k2, v2) = (
            Key::new("senpai".as_bytes(), 1, 0),
            Value::Put("114514".as_bytes()),
        );

        put_test(&sb1, k1, v1);

        put_test(&sb2, k2, v2);

        get_test::<true>(&sb1, k1, v1);
        get_test::<true>(&sb2, k2, v2);

        del_test(&sb1, k1, Value::Del("del".as_bytes()));
        del_test(&sb2, k2, Value::Del("del".as_bytes()));

        get_test::<false>(&sb1, k1, v1);
        get_test::<false>(&sb2, k2, v2);

        let s1 = mgr.get_tree(sb1.id())?;
        let s2 = mgr.get_tree(sb2.id())?;

        get_test::<false>(&s1, k1, v1);
        get_test::<false>(&s2, k2, v2);

        assert!(mgr.tree.get::<Key, &[u8]>(k1).is_err());
        Ok(())
    }

    #[test]
    fn more_subtree() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let mgr = new_mgr(path.clone())?;

        for i in 0..20 {
            let s = mgr.create_tree()?;
            assert_eq!(s.root_index.pid, i as u64 + ROOT_PID + 1);
        }

        let t1 = mgr.get_tree(2)?;
        let t2 = mgr.get_tree(2)?;
        let e = mgr.remove_tree(t1.id()).err();
        assert_eq!(e, Some(OpCode::Again));
        let o = mgr.remove_tree(t2.id());
        assert!(o.is_ok());

        Ok(())
    }

    #[test]
    fn traverse() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let mgr = new_mgr(path.clone())?;
        let s = mgr.create_tree()?;

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
        let opt = Arc::new(opt);
        let (meta, table, mapping) = Recovery::new(opt.clone()).phase1();
        let store = Arc::new(Store::new(table, opt, meta.clone(), mapping).unwrap());
        let mgr = Registry::new(store);
        let s = mgr.create_tree()?;

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
        assert_eq!(out.data(), "11".as_bytes());

        Ok(())
    }
}
