//! NOTE: the Bw-Tree requires the Transaction module ensure no `write-write` anomaly
//! the System Transaction is ensured by Bw-Tree itself
//! NOTE: the Bw-Tree implementation here is split when full and merge when empty
use super::{
    data::{Index, Key, Value},
    iter::{ItemIter, MergeIterBuilder},
    page::{
        Delta, DeltaType, IntlMergeIter, LeafMergeIter, NodeType, Page, PageHeader, PageMergeIter,
        RangeIter, ROOT_INDEX,
    },
    systxn::SysTxn,
    traits::{IKey, IPageIter, IVal},
};

use crate::{
    map::data::FrameOwner,
    tree::page::NULL_INDEX,
    utils::{byte_array::ByteArray, NAN_PID, ROOT_PID},
    OpCode, Store,
};
use core::str;
use std::sync::Arc;

#[derive(Clone, Copy)]
pub struct Range<'a> {
    key_pq: &'a [u8],
    key_qr: Option<&'a [u8]>,
}

impl<'a> Range<'a> {
    const fn new() -> Self {
        Self {
            key_pq: [].as_slice(),
            key_qr: None,
        }
    }
}

pub struct Val<'a> {
    raw: &'a [u8],
    owner: Option<Arc<FrameOwner>>,
}

impl Val<'_> {
    pub fn to_vec(mut self) -> Vec<u8> {
        let v = self.raw.to_vec();
        self.owner.take();
        v
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

struct ChainInfo<'a, K, V>
where
    K: IKey,
    V: IVal,
{
    iter: PageMergeIter<'a, K, V>,
    page: PageHeader,
    junks: Vec<u64>,
}

pub struct Tree {
    store: Arc<Store>,
}

impl Tree {
    pub fn new(store: Arc<Store>) -> Result<Self, OpCode> {
        let this = Self {
            store: store.clone(),
        };

        if !store.is_fresh() {
            return Ok(this);
        }
        // build an empty page with no key-value
        let iter: ItemIter<(Key<'_>, Value<'_>)> = ItemIter::default();
        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).from_iter(iter);
        let mut txn = Self::begin_impl(&store);
        let (addr, page) = txn.alloc(delta.size())?;

        delta.build(&page);
        txn.update(ROOT_PID, NAN_PID, addr).expect("can't update");
        Ok(this)
    }

    fn begin_impl(store: &Store) -> SysTxn {
        SysTxn::new(store)
    }

    fn begin(&self) -> SysTxn {
        Self::begin_impl(&self.store)
    }

    fn walk_page<F, K, V>(&self, mut addr: u64, mut f: F) -> Result<(), OpCode>
    where
        F: FnMut(Arc<FrameOwner>, u64, Page<K, V>) -> bool,
        K: IKey,
        V: IVal,
    {
        while addr != 0 {
            let frame = self.store.buffer.load(addr)?;
            let page = Page::from(frame.payload());

            if f(frame, addr, page) {
                break;
            }
            addr = page.header().link();
        }
        Ok(())
    }

    fn find_leaf(
        &self,
        txn: &mut SysTxn,
        key: &Key,
    ) -> Result<(View<'_>, Option<View<'_>>), OpCode> {
        loop {
            txn.unpin_all();
            match self.try_find_leaf(txn, key.raw) {
                Ok((view, parent)) => return Ok((view, parent)),
                Err(OpCode::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    fn page_view<'b>(&self, pid: u64, range: Range<'b>) -> Result<View<'b>, OpCode> {
        let addr = self.store.page.get(pid);
        assert_ne!(addr, 0);
        let b = self.store.buffer.load(addr)?.payload();
        Ok(View {
            page_id: pid,
            page_addr: addr,
            info: b.into(),
            range,
        })
    }

    fn try_find_leaf(
        &self,
        txn: &mut SysTxn,
        key: &[u8],
    ) -> Result<(View<'_>, Option<View<'_>>), OpCode> {
        let mut index = ROOT_INDEX;
        let mut range = Range::new();
        let mut parent = None;

        loop {
            let view = self.page_view(index.pid, range)?;

            // split may happen during search, in this case new created node are
            // not inserted into parent yet, the insert is halfly done, the
            // search can reach the `split delta` whose epoch are not equal to
            // previous one, and any modification operation CAN NOT proceed,
            // since we simplified the `find_child` process to handle data only,
            // if any other modification operation is allowed, will cause leaf
            // node disordered, by the way, any operation (including lookup)
            // will go through this function and check the condition
            if view.info.epoch() != index.epoch {
                let _ = self.parent_update(txn, view, parent);
                // simplified: retry from root, thanks to the large fanout
                return Err(OpCode::Again);
            }

            if view.info.is_leaf() {
                return Ok((view, parent));
            }

            let (child_index, child_range) = self
                .find_child(txn, key, &view)
                .expect("child is always exist in B-Tree");
            index = child_index;
            range.key_pq = child_range.key_pq;
            if let Some(key_qr) = child_range.key_qr {
                range.key_qr = Some(key_qr);
            }

            parent = Some(view);
        }
    }

    fn find_child<'a>(
        &self,
        txn: &mut SysTxn,
        key: &[u8],
        view: &View,
    ) -> Option<(Index, Range<'a>)> {
        let mut child = None;

        // stop when the child is in range
        let _ = self.walk_page(view.page_addr, |frame, _, pg: Page<&[u8], Index>| {
            debug_assert!(pg.header().is_intl());

            // skip inner `split-delta`
            if pg.header().is_data() {
                let (l, r) = match pg.lower_bound(key) {
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
                        txn.pin_frame(&frame);
                        child = Some((index, range));
                        return true;
                    }
                }
            }
            false
        });
        child
    }

    pub fn try_put(&self, key: Key, val: Value) -> Result<(), OpCode> {
        let mut txn = self.begin();
        let (mut view, _) = self.find_leaf(&mut txn, &key).unwrap();

        if self.need_split(&view) && self.split(&mut txn, view).is_ok() {
            return Err(OpCode::Again);
        }

        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).from_item((key, val));
        // NOTE: restrict logical node size
        if delta.size() > self.store.opt.page_size_threshold as usize {
            return Err(OpCode::TooLarge);
        }
        let (new_addr, new_page) = txn.alloc(delta.size())?;

        delta.build(&new_page);

        let h = new_page.header();
        loop {
            h.set_link(view.page_addr);
            h.set_depth(view.info.depth().saturating_add(1));
            h.set_epoch(view.info.epoch());

            match txn.update(view.page_id, view.page_addr, new_addr) {
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
                    if view.page_id != ROOT_PID {
                        let b = self.store.buffer.load(cur_addr)?.payload();
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

        let _ = self.try_consolidate(&mut txn, view);

        Ok(())
    }

    pub fn put(&self, gsn: u64, key: &[u8], val: Value) -> Result<(), OpCode> {
        let key = Key::new(key, gsn);
        loop {
            match self.try_put(key, val) {
                Ok(_) => return Ok(()),
                Err(OpCode::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// TODO: visibility checking
    pub fn del(&self, gsn: u64, key: &[u8]) -> Result<Val, OpCode> {
        match self.get(gsn, key) {
            Err(e) => Err(e),
            Ok(x) => {
                self.put(gsn, key, Value::Del)?;
                Ok(x)
            }
        }
    }

    /// TODO: visibility checking, so use [u8] reference here
    pub fn get(&self, gsn: u64, key: &[u8]) -> Result<Val, OpCode> {
        let mut txn = self.begin();
        let key = Key::new(key, gsn);
        let (view, _) = self.find_leaf(&mut txn, &key)?;
        let mut val = Err(OpCode::NotFound);
        // skip `split-delta`, see `find_child`
        let _ = self.walk_page(view.page_addr, |frame, _, pg: Page<Key, Value>| {
            if pg.header().is_data() {
                match pg.lower_bound(key.raw) {
                    Err(_) => return false,
                    Ok(slot) => match pg.val_at(slot) {
                        Value::Put(v) => {
                            let k = pg.key_at(slot);
                            debug_assert!(k.lsn <= key.lsn);
                            val = Ok(Val {
                                raw: v,
                                owner: Some(frame.clone()),
                            });
                            return true;
                        }
                        Value::Del => {
                            return true; // terminate search early
                        }
                    },
                }
            }
            false
        });
        val
    }

    #[allow(dead_code)]
    fn slice_to_str(x: &[u8]) -> &str {
        unsafe { std::str::from_utf8_unchecked(x) }
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
    ) -> Result<ChainInfo<'a, K, V>, OpCode>
    where
        K: IKey,
        V: IVal,
    {
        let cap = view.info.depth() as usize;
        let mut builder = MergeIterBuilder::new(cap);
        let mut info = view.info;
        let mut junks = Vec::with_capacity(cap);
        let mut split = None;

        self.walk_page(view.page_addr, |frame, addr, pg| {
            let h = pg.header();
            txn.pin_frame(&frame);

            match h.delta_type() {
                DeltaType::Data => {
                    builder.add(RangeIter::new(pg, 0..h.elems() as usize));
                }
                DeltaType::Split => {
                    if split.is_none() {
                        let (key, _) = Self::decode_split_delta(pg.raw());
                        split = Some(key);
                    }
                }
            }

            info = *h;
            junks.push(addr);
            false
        })?;

        let iter = PageMergeIter::new(builder.build(), split);
        Ok(ChainInfo {
            iter,
            page: info,
            junks,
        })
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
        let info = self.collect_delta(txn, &view)?;
        let lsn = 0; // TODO: use selft.cc.min_flush_gsn()
        let iter = f(info.iter, lsn);
        let mut delta = Delta::new(view.info.delta_type(), view.info.node_type()).from_iter(iter);
        let mut txn = self.begin();
        let (new_addr, new_page) = txn.alloc(delta.size())?;

        delta.build(&new_page);
        let h = new_page.header();
        h.set_epoch(view.info.epoch());
        h.set_depth(info.page.depth());
        h.set_link(info.page.link());

        txn.replace(view.page_id, view.page_addr, new_addr, &info.junks)
            .map(|_| {
                view.page_addr = new_addr;
                view.info = *new_page.header();
                view
            })
            .map_err(|_| OpCode::Again)
    }

    /// NOTE: consolidate never retry
    fn consolidate<'a>(&'a self, txn: &mut SysTxn, view: View<'a>) -> Result<View<'a>, OpCode> {
        match view.info.node_type() {
            NodeType::Intl => self.do_consolidate(txn, view, IntlMergeIter::new),
            NodeType::Leaf => self.do_consolidate(txn, view, LeafMergeIter::new),
        }
    }

    fn try_consolidate<'a>(&'a self, txn: &mut SysTxn, mut view: View<'a>) -> Result<(), OpCode> {
        if self.need_consolidate(&view.info) {
            view = self.consolidate(txn, view)?;
            if self.need_split(&view) {
                return self.split(txn, view);
            }
            // TODO: when consolidate result no delta, we should perform `merge`
        }
        Ok(())
    }

    /// NOTE: this is the second phase of `split` smo
    fn parent_update(
        &self,
        txn: &mut SysTxn,
        view: View,
        parent: Option<View>,
    ) -> Result<(), OpCode> {
        assert!(view.info.is_split());

        let Some(mut parent) = parent else {
            return Err(OpCode::Invalid);
        };

        let lkey = view.range.key_pq;
        // it's important to apply split-delta's epoch to left entry, this will
        // prevent parent_update from being executed again for the same delta
        let lidx = Index::new(view.page_id, view.info.epoch());
        let b = self.store.buffer.load(view.page_addr)?.payload();
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
        let mut d = Delta::new(DeltaType::Data, NodeType::Intl).from_slice(&entry_delta);
        let (new_addr, new_page) = txn.alloc(d.size())?;
        d.build(&new_page);

        let m = &parent.info.meta;
        let h = new_page.header();
        h.set_depth(m.depth().saturating_add(1));
        h.set_epoch(m.epoch());
        h.set_link(parent.page_addr);

        txn.update(parent.page_id, parent.page_addr, new_addr)
            .map(|_| {
                parent.page_addr = new_addr;
                parent.info = *new_page.header();
            })
            .map_err(|_| OpCode::Again)?;

        let _ = self.try_consolidate(txn, parent);
        Ok(())
    }

    fn find_page_splitter<K, V>(page: Page<K, V>) -> Option<(K, RangeIter<K, V>, RangeIter<K, V>)>
    where
        K: IKey,
        V: IVal,
    {
        let elems = page.header().elems() as usize;
        if let Some((sep, _)) = page.get(elems / 2) {
            let pos = page.lower_bound(sep.raw()).unwrap_or_else(|x| x);
            if pos > 0 {
                let l = RangeIter::new(page, 0..pos);
                let r = RangeIter::new(page, pos..elems);
                return Some((sep, l, r));
            }
        }
        None
    }

    fn split_root<K, V>(&self, txn: &mut SysTxn, view: View) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
    {
        assert_eq!(view.page_id, ROOT_PID);
        assert_eq!(view.info.epoch(), 0);
        // the root delta chain must be fully consolidated, or else there's no where to link the
        // reset delta chain (either put to left or right child may cause error)
        assert_eq!(view.info.depth(), 1);

        let b = self.store.buffer.load(view.page_addr)?.payload();
        let page = Page::<K, V>::from(b);
        let Some((sep_key, li, ri)) = Self::find_page_splitter(page) else {
            return Ok(());
        };

        let l = {
            let mut d = Delta::new(DeltaType::Data, view.info.node_type()).from_iter(li);
            let (laddr, lpage) = txn.alloc(d.size())?;
            d.build(&lpage);
            txn.map(laddr)
        };
        let r = {
            let mut d = Delta::new(DeltaType::Data, view.info.node_type()).from_iter(ri);
            let (raddr, rpage) = txn.alloc(d.size())?;
            d.build(&rpage);
            txn.map(raddr)
        };

        let s = [
            // add an empty key to simplify operations, such as `find_child`
            // the keys < sep_key are at left, those <= sep_key are at right
            ([].as_slice(), Index::new(l, 0)),
            (sep_key.raw(), Index::new(r, 0)),
        ];

        // the new root
        let mut delta = Delta::new(DeltaType::Data, NodeType::Intl).from_slice(&s);
        let (new_addr, new_page) = txn.alloc(delta.size())?;

        delta.build(&new_page);

        txn.replace(view.page_id, view.page_addr, new_addr, &[view.page_addr])
    }

    fn split_non_root<K, V>(&self, txn: &mut SysTxn, view: View) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
    {
        if view.page_id == ROOT_PID {
            return self.split_root::<K, V>(txn, view);
        }

        let b = self.store.buffer.load(view.page_addr)?.payload();
        let page = Page::<K, V>::from(b);
        let Some((sep, _, ri)) = Self::find_page_splitter(page) else {
            return Ok(());
        };
        // new split page
        let rpid: u64 = {
            let mut d = Delta::new(DeltaType::Data, view.info.node_type()).from_iter(ri);
            let (new_addr, new_page) = txn.alloc(d.size())?;

            d.build(&new_page);
            txn.map(new_addr)
        };

        // split-delta contains: split-key + new split page id
        let mut delta = Delta::new(DeltaType::Split, view.info.node_type())
            .from_item((sep.raw(), Index::new(rpid, 0)));
        let (new_addr, new_page) = txn.alloc(delta.size())?;
        delta.build(&new_page);

        let h = new_page.header();
        h.set_epoch(view.info.epoch() + 1); // identify a split
        h.set_depth(view.info.depth().saturating_add(1));
        h.set_link(view.page_addr);

        txn.update(view.page_id, view.page_addr, new_addr)
            .map_err(|_| OpCode::Again)?;

        Ok(())
    }

    fn need_split(&self, view: &View) -> bool {
        if view.info.is_split() || view.info.elems() < 2 {
            return false;
        }

        let mut max_size = self.store.opt.page_size_threshold as u32;
        if view.info.is_intl() {
            max_size /= 2;
        }
        view.info.len() >= max_size
    }

    fn split(&self, txn: &mut SysTxn, view: View) -> Result<(), OpCode> {
        assert!(view.info.is_data());
        assert!(view.info.elems() > 1);

        match view.info.node_type() {
            NodeType::Intl => self.split_non_root::<&[u8], Index>(txn, view),
            NodeType::Leaf => self.split_non_root::<Key, Value>(txn, view),
        }
    }
}
