use super::{Node, Page, systxn::SysTxn};

use crate::map::buffer::Loader;
use crate::types::data::Record;
use crate::types::node::RawLeafIter;
use crate::types::refbox::{DeltaView, KeyRef};
use crate::types::traits::{IAsBoxRef, IBoxHeader, IHeader, ILoader};
use crate::utils::ROOT_PID;
use crate::{
    OpCode, Store,
    types::{
        data::{Index, IntlKey, Key, Value, Ver},
        node::MergeOp,
        refbox::BoxRef,
        traits::{ICodec, IKey, ITree, IVal, IValCodec},
    },
    utils::{NULL_CMD, NULL_PID},
};
use crossbeam_epoch::Guard;
use std::cmp::Ordering::Equal;
use std::ops::{Bound, RangeBounds};
#[cfg(feature = "metric")]
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::{borrow::Cow, sync::Arc};

#[cfg(feature = "metric")]
macro_rules! inc_cas {
    ($field: ident) => {
        G_CAS.$field.fetch_add(1, Relaxed)
    };
}

#[cfg(not(feature = "metric"))]
macro_rules! inc_cas {
    ($filed: ident) => {};
}

#[derive(Clone)]
pub struct ValRef {
    raw: Value<Record>,
    _owner: BoxRef,
}

impl ValRef {
    fn new(raw: Value<Record>, f: BoxRef) -> Self {
        Self { raw, _owner: f }
    }

    pub fn slice(&self) -> &[u8] {
        self.raw.as_ref().data()
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.raw.as_ref().data().to_vec()
    }

    pub(crate) fn unwrap(&self) -> &Record {
        self.raw.as_ref()
    }

    pub(crate) fn is_put(&self) -> bool {
        !self.is_del()
    }

    pub(crate) fn is_del(&self) -> bool {
        self.raw.is_del()
    }
}

impl Drop for ValRef {
    fn drop(&mut self) {
        // explicitly impl Drop here to make sure lifetime chain work
    }
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
            root_index: Index::new(root_pid),
        }
    }

    pub fn new(store: Arc<Store>, root_pid: u64) -> Self {
        let this = Self::load(store, root_pid);
        this.init();

        this
    }

    fn init(&self) {
        let g = crossbeam_epoch::pin();
        let mut txn = self.begin(&g);
        let node = Node::new_leaf(&mut txn, self.store.buffer.loader());
        let mut new_page = Page::new(node);
        let root_pid = txn.map(&mut new_page);
        self.store.buffer.cache(new_page);
        assert_eq!(root_pid, self.root_index.pid);
        txn.commit();
    }

    fn txid(&self) -> u64 {
        self.store.context.meta.safe_tixd()
    }

    pub(crate) fn begin<'a>(&'a self, g: &'a Guard) -> SysTxn<'a> {
        SysTxn::new(&self.store, g)
    }

    pub(crate) fn load_node(
        &self,
        store: &Store,
        g: &Guard,
        pid: u64,
    ) -> Result<Option<Page>, OpCode> {
        if let Some(p) = store.buffer.load(pid) {
            let child_pid = p.header().merging_child;
            if child_pid != 0 {
                self.merge_node(&p, child_pid, g)?;
            }
            Ok(Some(p))
        } else {
            Ok(None)
        }
    }

    // 1. mark child node as `merging`
    // 2. find it's left sibling, to merge child into it
    // 3. replace old left sibling with merged node
    // 4. remove index to child from it's parent
    // 5. unmap child pid from page table
    fn merge_node(&self, parent_ptr: &Page, child_pid: u64, g: &Guard) -> Result<(), OpCode> {
        // NOTE: a large lock is necessary because the merge process must be exclusive
        let Ok(_lk) = parent_ptr.try_lock() else {
            // retrun Ok let cooperative threads not retry
            return Ok(());
        };
        let safe_txid = self.txid();
        assert_ne!(child_pid, NULL_PID);
        // 1.
        let child_ptr = if let Some(x) = self.set_node_merging(child_pid, g, safe_txid)? {
            x
        } else {
            return Ok(());
        };
        assert!(parent_ptr.is_intl());
        let child_index = parent_ptr
            .intl_iter()
            .position(|(_, idx)| idx.pid == child_pid)
            .unwrap();
        assert_ne!(child_index, 0, "we can't handle merge the leftmost node");

        // 2.
        let mut merge_index = child_index - 1;
        let mut cursor_pid = parent_ptr
            .intl_iter()
            .nth(merge_index)
            .map(|(_, x)| x.pid)
            .unwrap();

        loop {
            let cursor_ptr = if let Some(x) = self.load_node(&self.store, g, cursor_pid)? {
                x
            } else {
                // the pid has been merged
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

            // 3.
            let next_pid = cursor_ptr.header().right_sibling;
            let mut txn = self.begin(g);
            // further check if it's really the left sibling of child
            if next_pid == child_pid {
                let new_node = cursor_ptr.merge_node(&mut txn, &child_ptr, safe_txid);
                inc_cas!(merge);
                if txn.replace(cursor_ptr, new_node).is_ok() {
                    child_ptr.garbage_collect(&mut txn);
                    txn.commit();
                    break;
                }
                inc_cas!(merge_fail);
                // retry merge
                continue;
            }
            let hi = cursor_ptr.hi();
            let lo = child_ptr.lo();
            if hi >= Some(lo) {
                // another thread has installed the merged node after we get the cursor
                break;
            } else {
                // another thread has installed the splitted left sibling after we get the cursor
                if next_pid != NULL_PID {
                    cursor_pid = next_pid
                } else {
                    // another thread has finished `merge_node`, the child_pid has been unmapped
                    break;
                }
            }
        }

        // 4.
        if !self.remove_node_index(parent_ptr, child_pid, g, safe_txid)? {
            return Ok(());
        }

        // 5.
        debug_assert_eq!(child_ptr.box_header().pid, child_pid);
        let mut txn = self.begin(g);
        txn.unmap(child_ptr)?;
        txn.commit();

        Ok(())
    }

    fn remove_node_index(
        &self,
        parent_ptr: &Page,
        child_pid: u64,
        g: &Guard,
        safe_txid: u64,
    ) -> Result<bool, OpCode> {
        let mut parent = Cow::Borrowed(parent_ptr);
        loop {
            let mut txn = self.begin(g);
            let new_ptr = parent_ptr.process_merge(&mut txn, MergeOp::Merged, safe_txid);
            inc_cas!(remove_node);
            if txn.replace(*parent, new_ptr).is_ok() {
                txn.commit();
                return Ok(true);
            }
            inc_cas!(remove_node_fail);
            let new_ptr =
                if let Some(x) = self.load_node(&self.store, g, parent_ptr.box_header().pid)? {
                    x
                } else {
                    return Ok(false);
                };
            if new_ptr.header().merging_child != child_pid {
                return Ok(false);
            }
            parent = Cow::Owned(new_ptr);
        }
    }

    // 1. load child node and check if it's merging
    // 2. return if it's merging
    // 3. or else create a new node with merging set to true
    // 4. replace old child node with the new node
    fn set_node_merging(
        &self,
        child_pid: u64,
        g: &Guard,
        safe_txid: u64,
    ) -> Result<Option<Page>, OpCode> {
        loop {
            let page = if let Some(x) = self.load_node(&self.store, g, child_pid)? {
                x
            } else {
                return Ok(None);
            };

            if page.header().merging {
                return Ok(Some(page));
            }

            let mut txn = self.begin(g);
            let new_node = page.process_merge(&mut txn, MergeOp::MarkChild, safe_txid);
            inc_cas!(mark_merge);
            if let Ok(new_page) = txn.replace(page, new_node) {
                txn.commit();
                return Ok(Some(new_page));
            }
            inc_cas!(mark_merge_fail);
        }
    }

    /// 1. split node into two parts
    /// 2. map rhs to page table, link lhs's right_sibling to rhs
    /// 3. replace node with left, so that other thread can notice splitting
    /// 4. if node is not root (parent_opt is not None)
    ///    - insert rhs to parent index, return new node
    ///    - replace parent with new node
    /// 5. or else
    ///    - create a new copy of left page (which just replaced current node)
    ///    - map left page to page table
    ///    - create a new node with lhs and rhs in it's index
    ///    - replace root with new node
    ///
    fn split_node(&self, node: Page, parent_opt: Option<Page>, g: &Guard) -> Result<(), OpCode> {
        let Ok(node_lock) = node.try_lock() else {
            return Err(OpCode::Again);
        };
        let safe_txid = self.store.context.meta.safe_tixd();
        let mut txn = self.begin(g);
        // 1.
        let (mut lnode, rnode) = node.split(&mut txn);
        let mut rpage = Page::new(rnode);

        txn.pin(rpage);

        // 2.
        let rpid = txn.map(&mut rpage);
        lnode.header_mut().right_sibling = rpid;

        // 3.
        inc_cas!(split1);
        let lpage = txn.replace(node, lnode).inspect_err(|_| {
            inc_cas!(split_fail1);
        })?;
        self.store.buffer.cache(rpage);
        // drop lock early let cooperative threads have chance to make progress
        drop(node_lock);
        // publish rpage to global
        txn.commit();

        let lo = rpage.lo();
        if let Some(parent) = parent_opt {
            // multiple threads (cooperative) may concurrently update parent
            let _lk = parent.lock();
            if self.store.page.get(parent.pid()) != parent.swip() {
                // other thread has finished same job
                return Ok(());
            }
            // 4.
            let new_node = parent.insert_index(&mut txn, lo, rpid, safe_txid);
            if new_node.is_none() {
                // may conflict with other thread
                return Ok(());
            }
            inc_cas!(split2);
            txn.replace(parent, new_node.unwrap()).inspect_err(|_| {
                inc_cas!(split_fail2);
            })?;
            // publish new parent to global
            txn.commit();
        } else {
            // 4.
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
        if self.store.page.get(root.pid()) != root.swip() {
            return Err(OpCode::Again);
        };
        let mut txn = self.begin(g);

        // compact is required, since other thread may already insert new data after step 3
        let mut lnode = root.compact(&mut txn, safe_txid, false);
        lnode.header_mut().right_sibling = rpid;
        let mut lpage = Page::new(lnode);

        txn.pin(lpage);

        let lpid = txn.map(&mut lpage);

        let new_root_node = Node::new_root(
            &mut txn,
            self.store.buffer.loader(),
            &[
                (IntlKey::new([].as_slice()), Index::new(lpid)),
                (IntlKey::new(lo), Index::new(rpid)),
            ],
        );

        inc_cas!(split_root);
        let n = txn.replace(root, new_root_node).inspect_err(|_| {
            inc_cas!(split_root_fail);
        })?;
        assert_eq!(n.box_header().pid, ROOT_PID);
        // publish new root to global
        txn.commit();
        self.store.buffer.cache(lpage);
        Ok(())
    }

    fn find_leaf(&self, g: &Guard, k: &[u8]) -> Result<Page, OpCode> {
        loop {
            match self.try_find_leaf(g, k) {
                Err(OpCode::Again) => continue,
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
            let node_ptr = if let Some(x) = self.load_node(&self.store, g, cursor)? {
                x
            } else {
                return Err(OpCode::Again);
            };

            if node_ptr.header().merging {
                return Err(OpCode::Again);
            }

            // the node it self may be obsoleted by smo, we must make sure key is in [lo, hi)
            let lo = node_ptr.lo();
            if key < lo {
                return Err(OpCode::Again);
            }

            if node_ptr.should_split(self.store.opt.split_elems) {
                self.split_node(node_ptr, parent_opt, g)?;
                return Err(OpCode::Again);
            }

            // another thread replace the old node which the cursor pointed to with a new node just
            // splitted
            let hi = node_ptr.hi();
            let is_splitting = if let Some(hi) = hi { key >= hi } else { false };

            if is_splitting {
                // search from right sibling
                let rpid = node_ptr.header().right_sibling;
                assert_ne!(rpid, NULL_PID);

                if unsplit_parent_opt.is_none() && parent_opt.is_some() {
                    unsplit_parent_opt = parent_opt;
                } else if parent_opt.is_none() && lo.is_empty() {
                    // the paritially-split root, node_ptr itself is root and it's already broken
                    // into two parts and the lhs part is current node_ptr but not install the new
                    // root yet, here we complete the new root install phase
                    assert_eq!(cursor, self.root_index.pid);
                    let safe_txid = self.store.context.meta.safe_tixd();
                    let _ = self.split_root(g, node_ptr, rpid, hi.unwrap(), safe_txid);
                    return Err(OpCode::Again);
                }
                cursor = rpid;

                continue;
            }

            // cooperative the split
            if let Some(unsplit) = unsplit_parent_opt.take() {
                let mut txn = self.begin(g);
                let _lk = unsplit.lock();
                if self.store.page.get(unsplit.pid()) != unsplit.swip() {
                    // other thread has finished same job
                    return Err(OpCode::Again);
                }

                // create a new index in intl node
                let split_node = unsplit.insert_index(&mut txn, lo, cursor, self.txid());

                if split_node.is_none() {
                    return Err(OpCode::Again);
                }

                inc_cas!(coop);
                txn.replace(unsplit, split_node.unwrap()).inspect_err(|_| {
                    inc_cas!(coop_fail);
                })?;
                txn.commit();
            }

            if !leftmost && parent_opt.is_some() && node_ptr.should_merge() {
                self.try_merge(g, parent_opt.unwrap(), node_ptr)?;
                return Err(OpCode::Again);
            }

            if node_ptr.is_intl() {
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

    fn try_compact(&self, g: &Guard, page: Page) {
        let _lk = page.lock();
        if self.store.page.get(page.pid()) != page.swip() {
            return;
        };

        // consolidation never retry
        let mut txn = self.begin(g);
        let new_node = page.compact(&mut txn, self.txid(), true);
        inc_cas!(compact);
        if txn.replace(page, new_node).is_ok() {
            txn.commit();
        } else {
            inc_cas!(compact_fail);
        }
    }

    fn try_merge(&self, g: &Guard, parent: Page, cur: Page) -> Result<(), OpCode> {
        let Ok(lk) = parent.try_lock() else {
            return Err(OpCode::Again);
        };
        assert_eq!(parent.header().merging_child, NULL_PID);
        let mut txn = self.begin(g);
        let pid = cur.pid();

        if parent.can_merge_child(pid) {
            let new_parent = parent.process_merge(&mut txn, MergeOp::MarkParent(pid), self.txid());
            inc_cas!(try_merge);
            let new_page = txn.replace(parent, new_parent).inspect_err(|_| {
                inc_cas!(try_merge_fail);
            })?;
            txn.commit();
            drop(lk);
            self.merge_node(&new_page, pid, g)?;
        }
        Ok(())
    }

    fn link<K, V, F>(&self, g: &Guard, old: Page, k: &K, v: &V, mut check: F) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
        F: FnMut(Page, &K) -> Result<(), OpCode>,
    {
        check(old, k)?;

        let mut txn = self.begin(g);
        let b = DeltaView::from_key_val(&mut txn, *k, *v);
        let mut new = Page::new(old.insert(b.view().as_delta()));

        txn.pin(new);

        // because each thread contains their private data, we have to load the current page by pid
        // and retry (insert delta to the loaded page) on failure caused by both insert/compaction,
        // meanwhile, smo may also cause update fail, we simply restart the whole insert procedure
        // TODO: potentail decline in performance
        inc_cas!(link);
        if txn.update(old, &mut new).is_err() {
            inc_cas!(link_fail);
            return Err(OpCode::Again);
        }

        txn.commit();
        new.save(b); // save the delta itself until page was reclaimed

        Ok(())
    }

    fn try_put<K, V>(&self, g: &Guard, key: &K, val: &V) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
    {
        let page = self.find_leaf(g, key.raw())?;

        self.link(g, page, key, val, |_, _| Ok(()))?;
        Ok(())
    }

    /// for non-txn use, such as registry and recovery
    pub fn put<K, V>(&self, g: &Guard, key: K, val: V) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
    {
        loop {
            match self.try_put::<K, V>(g, &key, &val) {
                Ok(_) => return Ok(()),
                Err(OpCode::Again) => {
                    g.flush();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn try_update<T, F>(
        &self,
        g: &Guard,
        key: &Key,
        val: &Value<T>,
        visible: &mut F,
    ) -> Result<Option<ValRef>, OpCode>
    where
        T: IValCodec,
        F: FnMut(&Option<(Key, ValRef)>) -> Result<(), OpCode>,
    {
        let page = self.find_leaf(g, key.raw)?;
        let mut r = None;

        self.link(g, page, key, val, |pg, k| {
            let tmp = pg.find_latest(k, |x, y| x.raw.cmp(y.raw));
            // use the full key from input argument and the version from the exists latest one
            r = tmp.map(|(x, y, t)| (Key::new(k.raw, x.ver), ValRef::new(y, t)));
            visible(&r)
        })?;

        Ok(r.map(|x| x.1.clone()))
    }

    // NOTE: the `visible` function may be called multiple times
    pub fn update<T, V>(
        &self,
        g: &Guard,
        key: Key,
        val: Value<T>,
        mut visible: V,
    ) -> Result<Option<ValRef>, OpCode>
    where
        T: IValCodec,
        V: FnMut(&Option<(Key, ValRef)>) -> Result<(), OpCode>,
    {
        let size = key.packed_size() + val.packed_size();
        if size > self.store.opt.max_data_size() {
            return Err(OpCode::TooLarge);
        }
        loop {
            match self.try_update(g, &key, &val, &mut visible) {
                Ok(x) => return Ok(x),
                Err(OpCode::Again) => {
                    g.flush();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// return the latest key-val pair, by using Ikey::raw(), thanks to MVCC, the first match one is
    /// the latest one
    pub fn get<'b>(&'b self, g: &Guard, key: Key<'b>) -> Result<(Key<'b>, ValRef), OpCode> {
        let page = self.find_leaf(g, key.raw())?;

        let Some((k, v, p)) =
            page.find_latest::<Key, Value<Record>, _>(&key, |x, y| x.raw().cmp(y.raw()))
        else {
            return Err(OpCode::NotFound);
        };

        Ok((k, ValRef::new(v, p)))
    }

    pub fn range<'a, K, R, F, D>(&'a self, range: R, visible: F, dtor: D) -> Iter<'a>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
        F: FnMut(u64, &Record) -> bool + 'a,
        D: Fn() + 'a,
    {
        let lo = match range.start_bound() {
            Bound::Included(b) => Bound::Included(KeyRef::copy(b.as_ref())),
            Bound::Excluded(b) => Bound::Excluded(KeyRef::copy(b.as_ref())),
            Bound::Unbounded => Bound::Included(KeyRef::copy(&[])),
        };
        let hi = match range.end_bound() {
            Bound::Included(e) => Bound::Included(e.as_ref().to_vec()),
            Bound::Excluded(e) => Bound::Excluded(e.as_ref().to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Iter {
            tree: self,
            lo,
            hi,
            iter: None,
            cache: None,
            checker: Box::new(visible),
            dtor: Box::new(dtor),
            filter: Filter {
                last: None,
                holder: None,
            },
        }
    }

    fn traverse_sibling<L, F>(
        &self,
        l: &L,
        start_ts: u64,
        mut addr: u64,
        visible: &mut F,
    ) -> Result<ValRef, OpCode>
    where
        L: ILoader,
        F: FnMut(u64, &Record) -> bool,
    {
        let ver = Ver::new(start_ts, NULL_CMD);

        while addr != NULL_PID {
            let ptr = l.pin_load(addr).as_base();
            let sst = ptr.sst::<Ver, Value<Record>>();
            let pos = sst.lower_bound(l, &ver).unwrap_or_else(|pos| pos);
            if pos < sst.header().elems as usize {
                let (k, v, r) = sst.get_unchecked(l, pos);

                if visible(k.txid, v.as_ref()) {
                    if v.is_del() {
                        return Err(OpCode::NotFound);
                    }
                    return Ok(ValRef::new(
                        v,
                        r.map_or_else(|| ptr.as_box(), |x| x.as_box()),
                    ));
                }
            }
            addr = ptr.box_header().link;
        }
        Err(OpCode::NotFound)
    }

    pub fn traverse<F>(&self, g: &Guard, key: Key, mut visible: F) -> Result<ValRef, OpCode>
    where
        F: FnMut(u64, &Record) -> bool,
    {
        let page = self.find_leaf(g, key.raw)?;

        let it = page.range_from(
            key,
            |x, y| {
                let k = Key::decode_from(x.key());
                match k.raw.cmp(y.raw) {
                    Equal => y.txid.cmp(&k.txid),
                    o => o,
                }
            },
            |x, y| Key::decode_from(x.key()).raw.cmp(y.raw).is_eq(),
        );

        for x in it {
            let v = Value::<Record>::decode_from(x.val());
            if v.is_del() {
                return Err(OpCode::NotFound);
            }
            let k = Key::decode_from(x.key());
            if visible(k.txid, v.as_ref()) {
                return Ok(ValRef::new(v, x.as_box()));
            }
        }

        // Key::raw is unique in sst
        let (k, v, t) = page
            .search_sst::<Key, Value<Record>>(&key)
            .ok_or(OpCode::NotFound)?;
        if v.is_del() {
            return Err(OpCode::NotFound);
        }
        if visible(k.txid, v.as_ref()) {
            return Ok(ValRef::new(v, t));
        }
        if let Some(s) = v.sibling() {
            return self.traverse_sibling(page.loader(), key.txid, s.addr(), &mut visible);
        }
        Err(OpCode::NotFound)
    }
}

impl ITree for Tree {
    fn put<K, V>(&self, g: &Guard, k: K, v: V)
    where
        K: crate::types::traits::IKey,
        V: crate::types::traits::IVal,
    {
        self.put(g, k, v).unwrap()
    }
}

pub struct Iter<'a> {
    tree: &'a Tree,
    lo: Bound<KeyRef>,
    hi: Bound<Vec<u8>>,
    iter: Option<RawLeafIter<'a, Loader>>,
    cache: Option<Node>,
    checker: Box<dyn FnMut(u64, &Record) -> bool + 'a>,
    dtor: Box<dyn Fn() + 'a>,
    filter: Filter<'a>,
}

impl Drop for Iter<'_> {
    fn drop(&mut self) {
        (self.dtor)();
    }
}

impl Iter<'_> {
    fn low_key(&self) -> &[u8] {
        match self.lo {
            Bound::Unbounded => &[],
            Bound::Excluded(ref x) | Bound::Included(ref x) => x.key(),
        }
    }

    fn is_lt_upper(&self, node: &Node) -> bool {
        if let Some(hi) = node.hi() {
            match self.lo.as_ref() {
                Bound::Excluded(b) if hi >= b.key() => true,
                Bound::Included(b) if hi > b.key() => true,
                _ => false,
            }
        } else {
            true
        }
    }

    fn is_gt_lower(&self, node: &Node) -> bool {
        let lo = node.lo();
        match self.lo.as_ref() {
            Bound::Excluded(b) | Bound::Included(b) if lo <= b.key() => true,
            _ => lo.is_empty(),
        }
    }

    fn collapsed(&self) -> bool {
        match (&self.lo, &self.hi) {
            (Bound::Included(b), Bound::Included(e))
            | (Bound::Excluded(b), Bound::Excluded(e))
            | (Bound::Included(b), Bound::Excluded(e))
            | (Bound::Excluded(b), Bound::Included(e)) => b.key() > e,
            _ => false,
        }
    }

    fn predecessor(&self, x: &[u8]) -> Option<Vec<u8>> {
        let mut tmp = x.to_vec();

        if let Some(x) = tmp.last_mut() {
            if *x > 0 {
                *x -= 1;
            } else {
                tmp.pop();
            }
        } else {
            return None;
        }
        Some(tmp)
    }

    fn get_next(&mut self) -> Option<<Self as Iterator>::Item> {
        let g = crossbeam_epoch::pin();

        while !self.collapsed() {
            let node = if let Some(node) = self.cache.take() {
                node
            } else {
                let p = self.tree.find_leaf(&g, self.low_key()).unwrap();
                p.clone_node()
            };

            self.cache = Some(node);
            let node = self.cache.as_ref().unwrap();

            if !self.is_lt_upper(node) {
                // merged or exhausted
                let p = self.tree.find_leaf(&g, self.low_key()).unwrap();
                self.cache = Some(p.clone_node());
                self.iter.take();
                continue;
            } else if !self.is_gt_lower(node) {
                // split or exhuasted
                let lo = node.lo();
                let next = self.predecessor(lo)?;
                let p = self.tree.find_leaf(&g, &next).unwrap();
                self.cache = Some(p.clone_node());
                self.iter.take();
                continue;
            }

            if self.iter.is_none() {
                let iter = node.successor(&self.lo);
                // iter is always valid when node is valid
                self.iter = Some(unsafe {
                    std::mem::transmute::<RawLeafIter<'_, Loader>, RawLeafIter<'_, Loader>>(iter)
                });
            }

            let iter = self.iter.as_mut().unwrap();
            let r = iter.find(|(k, v, r)| {
                let ok = match &self.lo {
                    Bound::Unbounded => true,
                    Bound::Included(b) => k.raw >= b.key(),
                    Bound::Excluded(b) => k.raw > b.key(),
                };
                if ok && (self.checker)(k.txid, v.as_ref()) {
                    self.filter.check(k.raw, v.is_del(), r.clone())
                } else {
                    false
                }
            });

            if let Some((k, v, b)) = r.map(|(k, v, b)| (k.raw, v, b)) {
                self.lo = Bound::Excluded(b);
                // it's safe, since `b` has been saved
                let v = unsafe { std::mem::transmute::<&[u8], &[u8]>(v.as_ref().data()) };

                match self.hi {
                    Bound::Unbounded => return Some((k, v)),
                    Bound::Included(ref h) if h.as_slice() >= k => return Some((k, v)),
                    Bound::Excluded(ref h) if h.as_slice() > k => return Some((k, v)),
                    _ => return None,
                }
            } else {
                self.iter.take();
                if let Some(hi) = node.hi() {
                    self.lo = Bound::Included(KeyRef::copy(hi));
                    continue;
                }
                return None;
            }
        }
        None
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next()
    }
}

struct Filter<'a> {
    last: Option<&'a [u8]>,
    holder: Option<KeyRef>,
}

impl<'a> Filter<'a> {
    fn check(&mut self, k: &'a [u8], is_del: bool, r: KeyRef) -> bool {
        if let Some(last) = self.last
            && last == k
        {
            return false;
        }
        self.holder = Some(r);
        self.last = Some(k);
        !is_del
    }
}

#[cfg(feature = "metric")]
#[derive(Debug)]
pub struct CASstatus {
    merge: AtomicUsize,
    merge_fail: AtomicUsize,
    remove_node: AtomicUsize,
    remove_node_fail: AtomicUsize,
    mark_merge: AtomicUsize,
    mark_merge_fail: AtomicUsize,
    split1: AtomicUsize,
    split_fail1: AtomicUsize,
    split2: AtomicUsize,
    split_fail2: AtomicUsize,
    split_root: AtomicUsize,
    split_root_fail: AtomicUsize,
    coop: AtomicUsize,
    coop_fail: AtomicUsize,
    try_merge: AtomicUsize,
    try_merge_fail: AtomicUsize,
    link: AtomicUsize,
    link_fail: AtomicUsize,
    compact: AtomicUsize,
    compact_fail: AtomicUsize,
}

#[cfg(feature = "metric")]
static G_CAS: CASstatus = CASstatus {
    merge: AtomicUsize::new(0),
    merge_fail: AtomicUsize::new(0),
    remove_node: AtomicUsize::new(0),
    remove_node_fail: AtomicUsize::new(0),
    mark_merge: AtomicUsize::new(0),
    mark_merge_fail: AtomicUsize::new(0),
    split1: AtomicUsize::new(0),
    split_fail1: AtomicUsize::new(0),
    split2: AtomicUsize::new(0),
    split_fail2: AtomicUsize::new(0),
    split_root: AtomicUsize::new(0),
    split_root_fail: AtomicUsize::new(0),
    coop: AtomicUsize::new(0),
    coop_fail: AtomicUsize::new(0),
    try_merge: AtomicUsize::new(0),
    try_merge_fail: AtomicUsize::new(0),
    link: AtomicUsize::new(0),
    link_fail: AtomicUsize::new(0),
    compact: AtomicUsize::new(0),
    compact_fail: AtomicUsize::new(0),
};

#[cfg(feature = "metric")]
pub fn g_cas_status() -> &'static CASstatus {
    &G_CAS
}
