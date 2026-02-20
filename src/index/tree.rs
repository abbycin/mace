use super::{Node, Page, systxn::SysTxn};

use crate::Options;
use crate::cc::context::Context;
use crate::map::buffer::{BucketContext, Loader};
use crate::types::data::{IterItem, Record, Val};
use crate::types::node::RawLeafIter;
use crate::types::refbox::DeltaView;
use crate::types::traits::{IAsBoxRef, IBoxHeader, IDecode, IHeader, ILoader};
use crate::utils::data::Position;
use crate::utils::{Handle, MutRef, NULL_ADDR, OpCode};
use crate::{
    Store,
    types::{
        data::{Index, IntlKey, Key, Ver},
        refbox::BoxRef,
        traits::{ICodec, IKey, ITree, IVal},
    },
    utils::{NULL_CMD, NULL_PID},
};
use crossbeam_epoch::Guard;
use std::borrow::Cow;
use std::cmp::Ordering::Equal;
use std::ops::{Bound, Deref, RangeBounds};
use std::sync::Arc;
use std::sync::atomic::Ordering::Acquire;

/// A reference to a value in the storage engine.
#[derive(Clone)]
pub struct ValRef {
    raw: Record,
    _owner: BoxRef,
}

impl ValRef {
    fn new(raw: Record, f: BoxRef) -> Self {
        Self { raw, _owner: f }
    }

    /// Returns the data as a byte slice.
    pub fn slice(&self) -> &[u8] {
        self.raw.data()
    }

    /// Converts the reference into a owned Vec<u8>.
    pub fn to_vec(self) -> Vec<u8> {
        self.raw.data().to_vec()
    }

    pub(crate) fn unwrap(&self) -> &Record {
        &self.raw
    }

    pub(crate) fn is_put(&self) -> bool {
        !self.is_del()
    }

    pub(crate) fn is_del(&self) -> bool {
        self.raw.is_tombstone()
    }
}

impl Drop for ValRef {
    fn drop(&mut self) {
        // explicitly impl Drop here to make sure lifetime chain work
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
        let mut txn = self.begin(&g);
        let node = Node::new_leaf(&mut txn, self.bucket.loader(self.store.context));
        let mut page = Page::new(node);
        txn.map_to(&mut page, root_pid);
        self.bucket.cache(page);
        txn.commit();
    }

    fn txid(&self) -> u64 {
        self.store.context.safe_txid()
    }

    pub(crate) fn bucket_id(&self) -> u64 {
        self.bucket.bucket_id
    }

    pub(crate) fn begin<'a>(&'a self, g: &'a Guard) -> SysTxn<'a> {
        SysTxn::new(&self.bucket.table, &self.store.opt, g, &self.bucket)
    }

    pub(crate) fn load_node(&self, g: &Guard, pid: u64) -> Result<Option<Page>, OpCode> {
        loop {
            if let Some(p) = self.bucket.load(pid)? {
                let child_pid = p.runtime_merging_child();
                if child_pid != NULL_PID {
                    self.merge_node(&p, child_pid, g)?;
                    continue;
                }
                return Ok(Some(p));
            } else {
                return Ok(None);
            }
        }
    }

    // merge flow:
    // 1. mark child as runtime merging
    // 2. find a live left sibling that still links to child
    // 3. replace that sibling with merged content
    // 4. remove child index from parent
    // 5. unmap child pid from page table
    fn merge_node(&self, parent_ptr: &Page, child_pid: u64, g: &Guard) -> Result<(), OpCode> {
        struct MergeMarkGuard {
            parent: Page,
            child: Page,
            child_pid: u64,
            armed: bool,
        }

        impl MergeMarkGuard {
            fn new(parent: Page, child: Page, child_pid: u64) -> Self {
                Self {
                    parent,
                    child,
                    child_pid,
                    armed: true,
                }
            }

            fn clear(&mut self) {
                if !self.armed {
                    return;
                }
                self.child.clear_runtime_merging();
                let _ = self.parent.clear_runtime_merging_child(self.child_pid);
                self.armed = false;
            }
        }

        impl Drop for MergeMarkGuard {
            fn drop(&mut self) {
                self.clear();
            }
        }

        // parent lock serializes merge on this parent
        let Some(_lk) = parent_ptr.try_lock() else {
            // return ok so cooperative callers avoid retry storms
            return Ok(());
        };

        if self.bucket.table.get(parent_ptr.pid()) != parent_ptr.swip() {
            return Ok(());
        }

        assert_ne!(child_pid, NULL_PID);
        // 1.
        let child_ptr = if let Some(x) = self.set_node_merging(child_pid, g)? {
            x
        } else {
            return Ok(());
        };
        let mut merge_mark = MergeMarkGuard::new(*parent_ptr, child_ptr, child_pid);
        assert!(parent_ptr.is_intl());
        let Some(child_index) = parent_ptr
            .intl_iter()
            .position(|(_, idx)| idx.pid == child_pid)
        else {
            return Ok(());
        };
        if child_index == 0 {
            return Ok(());
        }

        // 2.
        let mut merge_index = child_index - 1;
        let Some(mut cursor_pid) = parent_ptr.intl_iter().nth(merge_index).map(|(_, x)| x.pid)
        else {
            return Ok(());
        };

        let safe_txid = self.txid();
        loop {
            let cursor_ptr = if let Some(x) = self.load_node(g, cursor_pid)? {
                x
            } else {
                // the cursor pid may already be merged away
                if merge_index == 0 {
                    return Ok(());
                }

                merge_index -= 1;
                let Some(pid) = parent_ptr.intl_iter().nth(merge_index).map(|(_, x)| x.pid) else {
                    return Ok(());
                };
                cursor_pid = pid;
                continue;
            };

            // 3.
            let next_pid = cursor_ptr.header().right_sibling;
            let mut txn = self.begin(g);
            // verify this candidate still points to child as its right sibling
            if next_pid == child_pid {
                let (new_node, lj, rj) = cursor_ptr.merge_node(&mut txn, &child_ptr, safe_txid);
                if txn.replace(cursor_ptr, new_node, &lj).is_ok() {
                    child_ptr.garbage_collect(&mut txn, &rj);
                    txn.commit();
                    break;
                }
                // retry merge on replace race
                continue;
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

        // 4.
        if !self.remove_node_index(parent_ptr, child_pid, g, safe_txid)? {
            return Ok(());
        }

        // 5.
        debug_assert_eq!(child_ptr.box_header().pid, child_pid);
        merge_mark.clear();
        let mut txn = self.begin(g);
        txn.unmap(child_ptr, &[])?; // child's junks were already collected
        txn.commit();

        Ok(())
    }

    // caller must hold parent lock
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
            let (new_ptr, j) = {
                let Some(x) = parent.remove_index(&mut txn, child_pid, safe_txid) else {
                    return Ok(false);
                };
                x
            };
            if let Ok(new_parent) = txn.replace(*parent, new_ptr, &j) {
                txn.commit();
                let _ = parent_ptr.clear_runtime_merging_child(child_pid);
                let _ = new_parent.clear_runtime_merging_child(child_pid);
                return Ok(true);
            }
            let new_ptr = if let Some(x) = self.load_node(g, parent_ptr.box_header().pid)? {
                x
            } else {
                return Ok(false);
            };
            if new_ptr.runtime_merging_child() != child_pid {
                return Ok(false);
            }
            parent = Cow::Owned(new_ptr);
        }
    }

    // 1. load child
    // 2. return immediately if child is already marked merging
    // 3. otherwise set runtime merging flag with cas and retry on race
    // caller must hold parent lock so parent and child markers stay paired
    fn set_node_merging(&self, child_pid: u64, g: &Guard) -> Result<Option<Page>, OpCode> {
        loop {
            let page = if let Some(x) = self.load_node(g, child_pid)? {
                x
            } else {
                return Ok(None);
            };

            if page.runtime_merging() {
                return Ok(Some(page));
            }
            if page.try_mark_runtime_merging() {
                return Ok(Some(page));
            }
        }
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
        let safe_txid = self.store.context.numerics.safe_tixd();
        let mut txn = self.begin(g);
        // 1.
        let (mut lnode, rnode, split_junks) = node.split_overlay(&mut txn, safe_txid);
        let mut rpage = Page::new(rnode);

        // 2.
        let rpid = txn.map(&mut rpage);
        lnode.header_mut().right_sibling = rpid;

        // 3.
        let lpage = txn.replace(node, lnode, &split_junks).inspect_err(|_| {})?;
        self.bucket.cache(rpage);
        // drop lock early so cooperative threads can make progress
        drop(node_lock);
        // publish rpage to page table
        txn.commit();

        let lo = rpage.lo();
        if let Some(parent) = parent_opt {
            // cooperative threads may race to install the same separator
            let _lk = parent.lock();
            if self.bucket.table.get(parent.pid()) != parent.swip() {
                // another thread already finished this parent update
                return Ok(());
            }
            // 4.
            let Some((new_node, j)) = parent.insert_index(&mut txn, lo, rpid, safe_txid) else {
                // parent update raced with other structural change
                return Ok(());
            };
            txn.replace(parent, new_node, &j).inspect_err(|_| {})?;
            // publish new parent to page table
            txn.commit();
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
        let mut txn = self.begin(g);

        // compact root before building new root because step-3 publication can race with new writes
        let (mut lnode, j) = root.compact(&mut txn, safe_txid);
        lnode.header_mut().right_sibling = rpid;
        let mut lpage = Page::new(lnode);

        let lpid = txn.map(&mut lpage);

        let new_root_node = Node::new_root(
            &mut txn,
            self.bucket.loader(self.store.context),
            &[
                (IntlKey::new([].as_slice()), Index::new(lpid)),
                (IntlKey::new(lo), Index::new(rpid)),
            ],
        );
        let n = txn.replace(root, new_root_node, &j).inspect_err(|_| {})?;
        assert_eq!(n.box_header().pid, self.root_index.pid);
        // publish new root to global
        txn.commit();
        self.bucket.cache(lpage);
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
            let node_ptr = if let Some(x) = self.load_node(g, cursor)? {
                x
            } else {
                return Err(OpCode::Again);
            };

            if node_ptr.is_smo_busy() {
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
                    let safe_txid = self.store.context.numerics.safe_tixd();
                    let _ = self.split_root(g, node_ptr, rpid, hi.unwrap(), safe_txid);
                    return Err(OpCode::Again);
                }
                cursor = rpid;

                continue;
            }

            // complete pending parent separator installation cooperatively
            if let Some(unsplit) = unsplit_parent_opt.take() {
                let mut txn = self.begin(g);
                let _lk = unsplit.lock();
                if self.bucket.table.get(unsplit.pid()) != unsplit.swip() {
                    // another thread already finished this parent update
                    return Err(OpCode::Again);
                }

                // create a new index in intl node
                let Some((split_node, j)) = unsplit.insert_index(&mut txn, lo, cursor, self.txid())
                else {
                    return Err(OpCode::Again);
                };
                txn.replace(unsplit, split_node, &j).inspect_err(|_| {})?;
                txn.commit();
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

    fn try_compact(&self, g: &Guard, page: Page) {
        let _lk = page.lock();
        if self.bucket.table.get(page.pid()) != page.swip() {
            return;
        };

        // consolidation never retry
        let mut txn = self.begin(g);
        let (new_node, j) = page.compact(&mut txn, self.txid());
        if txn.replace(page, new_node, &j).is_ok() {
            txn.commit();
        }
    }

    pub(crate) fn try_scavenge(&self, pid: u64, g: &Guard) -> Result<bool, OpCode> {
        let page = if let Some(p) = self.load_node(g, pid)? {
            p
        } else {
            return Ok(false);
        };

        if page.is_smo_busy() {
            return Ok(false);
        }

        let safe_txid = self.store.context.safe_txid();
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
        if parent.runtime_merging_child() != NULL_PID {
            return Err(OpCode::Again);
        }
        let pid = cur.pid();

        if parent.can_merge_child_runtime(cur.lo(), pid)
            && parent.try_mark_runtime_merging_child(pid)
        {
            drop(lk);
            self.merge_node(&parent, pid, g)?;
        }
        Ok(())
    }

    fn link<K, V, F>(&self, g: &Guard, page: Page, k: &K, v: &V, mut check: F) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
        F: FnMut(Page, &K) -> Result<(u8, Position), OpCode>,
    {
        loop {
            let Some(node) = page.try_lock() else {
                continue;
            };
            // consolidate happened, we must retry from root
            if self.bucket.table.get(page.pid()) != page.swip() {
                return Err(OpCode::Again);
            };

            let (group_id, pos) = check(page, k)?;
            let mut txn = self.begin(g);
            let (k, v) = DeltaView::from_key_val(&mut txn, k, v);

            node.insert(k, v);
            drop(node);
            txn.record_and_commit(group_id as usize, pos);
            return Ok(());
        }
    }

    fn try_put<K, V>(&self, g: &Guard, key: &K, val: &V) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
    {
        let page = self.find_leaf(g, key.raw())?;

        // it never write log, so use default value is always OK
        self.link(g, page, key, val, |_, _| Ok((0, Position::default())))?;
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

    fn try_update<V, F>(
        &self,
        g: &Guard,
        key: &Key,
        val: &V,
        visible: &mut F,
    ) -> Result<Option<ValRef>, OpCode>
    where
        V: IVal,
        F: FnMut(&Option<(Key, ValRef)>) -> Result<(u8, Position), OpCode>,
    {
        let page = self.find_leaf(g, key.raw)?;
        let mut r = None;

        self.link(g, page, key, val, |pg, k| {
            let tmp = pg.find_latest(k);
            // use the full key from input argument and the version from the exists latest one
            r = tmp.map(|(x, y, b)| (Key::new(k.raw, x.ver), ValRef::new(y, b)));
            visible(&r)
        })?;

        Ok(r.map(|x| x.1.clone()))
    }

    // NOTE: the `visible` function may be called multiple times
    pub fn update<V, F>(
        &self,
        g: &Guard,
        key: Key,
        val: V,
        mut visible: F,
    ) -> Result<Option<ValRef>, OpCode>
    where
        V: IVal,
        F: FnMut(&Option<(Key, ValRef)>) -> Result<(u8, Position), OpCode>,
    {
        let size = key.packed_size() + val.packed_size();
        if size > Options::MAX_KV_SIZE {
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

        let Some((k, v, b)) = page.find_latest(&key) else {
            return Err(OpCode::NotFound);
        };

        Ok((k, ValRef::new(v, b)))
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
            cache: None,
            iter_bound: None,
            checker: Box::new(visible),
            filter: Filter {
                last: Vec::new(),
                has_last: false,
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
        F: FnMut(u64, u8) -> bool,
    {
        let ver = Ver::new(start_ts, NULL_CMD);

        while addr != NULL_PID {
            let ptr = l.load(addr)?.as_base();
            let sst = ptr.sst::<Ver>();
            let mut pos = sst.lower_bound(&ver).unwrap_or_else(|pos| pos);
            while pos < sst.header().elems as usize {
                let (k, v) = sst.kv_at::<Val>(pos);
                if visible(k.txid, v.group_id()) {
                    if v.is_tombstone() {
                        return Err(OpCode::NotFound);
                    }
                    let (v, r) = v.get_record(l, true);
                    return Ok(ValRef::new(v, r.map_or(ptr.as_box(), |x| x)));
                }
                pos += 1;
            }
            addr = ptr.box_header().link;
        }
        Err(OpCode::NotFound)
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
                if val.is_tombstone() {
                    result = Some(Err(OpCode::NotFound));
                    return true;
                }
                if visible(k.txid, val.group_id()) {
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
        if val.is_tombstone() {
            return Err(OpCode::NotFound);
        }
        if visible(k.txid, val.group_id()) {
            let (record, r) = val.get_record(&page.loader, true);
            return Ok(ValRef::new(record, r.unwrap_or_else(|| page.base_box())));
        }
        if let Some(addr) = val.get_sibling() {
            return self.traverse_sibling(&page.loader, key.txid, addr, &mut visible);
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

/// An iterator over key-value pairs in a bucket.
pub struct Iter<'a> {
    tree: &'a Tree,
    cached_key: Handle<Vec<u8>>,
    lo: Bound<Vec<u8>>,
    hi: Bound<Vec<u8>>,
    iter: Option<RawLeafIter<'a, Loader>>,
    cache: Option<Box<Node>>,
    iter_bound: Option<Box<Bound<Vec<u8>>>>,
    checker: Box<dyn FnMut(&Context, u64, u8) -> bool + 'a>,
    filter: Filter,
}

impl Drop for Iter<'_> {
    fn drop(&mut self) {
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

    fn collapsed(&self) -> bool {
        match (&self.lo, &self.hi) {
            (Bound::Included(b), Bound::Included(e))
            | (Bound::Excluded(b), Bound::Excluded(e))
            | (Bound::Included(b), Bound::Excluded(e))
            | (Bound::Excluded(b), Bound::Included(e)) => b > e,
            _ => false,
        }
    }

    fn get_next(&mut self) -> Option<<Self as Iterator>::Item> {
        while !self.collapsed() {
            if self.iter.is_none() {
                let g = crossbeam_epoch::pin();
                let node = self.tree.find_leaf(&g, self.low_key()).expect("must exist");
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

            let iter = self.iter.as_mut().expect("must valid");
            let r = iter.find(|item| {
                let ok = match &self.lo {
                    Bound::Unbounded => true,
                    Bound::Included(b) => item.cmp_key(b.as_slice()).is_ge(),
                    Bound::Excluded(b) => item.cmp_key(b.as_slice()).is_gt(),
                };
                if ok && (self.checker)(&self.tree.store.context, item.txid(), item.group_id()) {
                    self.filter.check(item)
                } else {
                    false
                }
            });

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

struct Filter {
    last: Vec<u8>,
    has_last: bool,
}

impl Filter {
    fn check<L: ILoader>(&mut self, item: &IterItem<L>) -> bool {
        if self.has_last && item.cmp_key(&self.last).is_eq() {
            return false;
        }
        let last = item.assembled_key();
        self.last.clear();
        self.last.extend_from_slice(last.deref());
        self.has_last = true;
        !item.is_tombstone()
    }
}

#[cfg(test)]
mod test {
    use crate::{Mace, Options, RandomPath};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::Relaxed},
    };
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

    #[test]
    fn concurrent_page_hit_runtime_smo() {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.split_elems = 128;
        opt.consolidate_threshold = 32;
        opt.tmp_store = true;
        let mace = Mace::new(opt.validate().unwrap()).unwrap();
        let db = mace.new_bucket("default").unwrap();

        let num_readers = 4;
        let num_iterations = 1500;

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
                    if i > 256 {
                        let old = format!("key_{:05}", i - 256);
                        let _ = kv.del(&old);
                    }
                    kv.commit().unwrap();
                }
            });
        });
    }

    #[test]
    fn runtime_smo_merge_retry_idempotence_stress() {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.split_elems = 64;
        opt.consolidate_threshold = 16;
        opt.tmp_store = true;
        let mace = Mace::new(opt.validate().unwrap()).unwrap();
        let db = mace.new_bucket("default").unwrap();
        let init_keys = 1024;

        for i in 0..init_keys {
            let kv = db.begin().unwrap();
            let key = format!("k_{:04}", i);
            kv.put(&key, &key).unwrap();
            kv.commit().unwrap();
        }

        let write_threads = 4;
        let read_threads = 2;
        let write_iterations = 1500;
        let read_iterations = 1200;
        let success = Arc::new(AtomicUsize::new(0));

        thread::scope(|s| {
            for tid in 0..write_threads {
                let db = db.clone();
                let success = success.clone();
                s.spawn(move || {
                    for i in 0..write_iterations {
                        let idx = (i * 37 + tid * 17) % init_keys;
                        let key = format!("k_{:04}", idx);
                        let do_del = (i + tid) % 3 == 0;

                        for _ in 0..16 {
                            let kv = db.begin().unwrap();
                            let res = if do_del {
                                kv.del(&key).map(|_| ())
                            } else {
                                kv.put(&key, &key)
                            };

                            match res {
                                Ok(_) => match kv.commit() {
                                    Ok(_) => {
                                        success.fetch_add(1, Relaxed);
                                        break;
                                    }
                                    Err(crate::OpCode::AbortTx | crate::OpCode::Again) => continue,
                                    Err(e) => panic!("commit fail: {e:?}"),
                                },
                                Err(crate::OpCode::NotFound) if do_del => {
                                    success.fetch_add(1, Relaxed);
                                    break;
                                }
                                Err(crate::OpCode::AbortTx | crate::OpCode::Again) => continue,
                                Err(e) => panic!("modify fail: {e:?}"),
                            }
                        }
                    }
                });
            }

            for _ in 0..read_threads {
                let db = db.clone();
                s.spawn(move || {
                    for _ in 0..read_iterations {
                        let view = db.view().unwrap();
                        let mut seen = 0usize;
                        for _ in view.seek("k_") {
                            seen += 1;
                            if seen > init_keys {
                                break;
                            }
                        }
                        assert!(seen <= init_keys);
                    }
                });
            }
        });

        assert!(success.load(Relaxed) > write_threads * write_iterations / 8);
    }
}
