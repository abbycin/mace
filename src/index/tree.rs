use super::{
    data::{Index, Key, Value, Ver},
    iter::{ItemIter, MergeIterBuilder},
    page::{
        DeltaType, IntlMergeIter, IntlPage, LeafMergeIter, LeafPage, NodeType, Page, PageHeader,
        PageMergeIter, RangeIter,
    },
    slotted::SlottedPage,
    systxn::SysTxn,
};

use crate::{
    index::{
        builder::{Delta, FuseBuilder},
        page::NULL_INDEX,
    },
    map::data::FrameOwner,
    slice_to_number,
    utils::{
        byte_array::ByteArray,
        decode_u64,
        traits::{IKey, IPageIter, IVal, IValCodec},
        NULL_CMD, ROOT_PID,
    },
    OpCode, Store,
};
use core::str;
use std::{
    collections::{HashMap, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, Mutex, MutexGuard},
};

#[derive(Clone, Copy)]
pub struct Range<'a> {
    key_pq: &'a [u8],
    key_qr: Option<&'a [u8]>,
}

impl Range<'_> {
    const fn new() -> Self {
        Self {
            key_pq: [].as_slice(),
            key_qr: None,
        }
    }
}

#[derive(Clone)]
pub struct Val<T>
where
    T: IValCodec,
{
    raw: Value<T>,
    _owner: Arc<FrameOwner>,
}

impl<T> Val<T>
where
    T: IValCodec,
{
    fn new(raw: Value<T>, f: Arc<FrameOwner>) -> Self {
        Self { raw, _owner: f }
    }

    pub fn data(&self) -> &[u8] {
        self.raw.as_ref().data()
    }

    pub fn unwrap(&self) -> &T {
        self.raw.as_ref()
    }

    pub fn put(&self) -> &T {
        match self.raw {
            Value::Put(ref data) => data,
            Value::Sib(ref s) => s.as_ref(),
            _ => panic!("user should ensure it's a put"),
        }
    }

    pub fn is_put(&self) -> bool {
        !self.is_del()
    }

    pub fn is_del(&self) -> bool {
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

const NR_LOCKS: usize = 64;
const LOCKS_MASK: usize = NR_LOCKS - 1;

#[derive(Clone, Copy)]
#[repr(align(64))]
struct CachePadding;

#[derive(Clone)]
pub struct Tree {
    store: Arc<Store>,
    root_index: Index,
    name: Arc<String>,
    locks: Arc<[Mutex<CachePadding>; NR_LOCKS]>,
}

#[derive(Clone)]
pub(crate) struct Registry {
    tree: Tree,
    open_map: Arc<Mutex<HashMap<u64, usize>>>,
}

impl Registry {
    pub(crate) fn new(store: Arc<Store>) -> Result<Self, OpCode> {
        let name = "register".into();
        if !store.is_fresh(ROOT_PID) {
            Ok(Self {
                tree: Tree::load(store, ROOT_PID, name),
                open_map: Arc::new(Mutex::new(HashMap::new())),
            })
        } else {
            let pid = store.page.alloc().ok_or(OpCode::NoSpace)?;
            assert_eq!(pid, ROOT_PID);
            Ok(Self {
                tree: Tree::new(store, ROOT_PID, name)?,
                open_map: Arc::new(Mutex::new(HashMap::new())),
            })
        }
    }

    fn new_tree(&self, k: Key, name: String) -> Result<Tree, OpCode> {
        let pid = self.tree.store.page.alloc().ok_or(OpCode::NoSpace)?;
        match self.tree.put(k, Value::Put(&pid.to_le_bytes()[..])) {
            Ok(_) => {
                let (_, v) = self.tree.get::<&[u8]>(k).expect("impossible");
                let r = slice_to_number!(*v.data(), u64);
                assert_eq!(r, pid);
                Tree::new(self.tree.store.clone(), pid, name)
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) fn open(&self, name: impl AsRef<str>) -> Result<Tree, OpCode> {
        let mut map = self.open_map.lock().unwrap();
        let k = Key::new(name.as_ref().as_bytes(), 0, 0);
        let name: String = name.as_ref().into();
        let r = match self.tree.get::<&[u8]>(k) {
            Ok((_, v)) => {
                if v.is_del() {
                    self.new_tree(k, name)
                } else {
                    let n = v.data();
                    let pid = slice_to_number!(*n, u64);
                    Ok(Tree::load(self.tree.store.clone(), pid, name))
                }
            }
            Err(_) => self.new_tree(k, name),
        }?;
        if let Some(v) = map.get_mut(&r.root_index.pid) {
            *v += 1;
        } else {
            map.insert(r.root_index.pid, 1);
        }
        Ok(r)
    }

    pub(crate) fn remove(&self, tree: &Tree) -> Result<(), OpCode> {
        let mut map = self.open_map.lock().unwrap();
        let Some(v) = map.get_mut(&tree.root_index.pid) else {
            return Err(OpCode::NotFound);
        };
        *v -= 1;
        if *v != 0 {
            return Err(OpCode::Again);
        }
        map.remove(&tree.root_index.pid);
        self.tree.put::<&[u8]>(
            Key::new(tree.name().as_bytes(), 0, 0),
            Value::Del([].as_slice()),
        )
    }
}

impl Tree {
    pub fn load(store: Arc<Store>, root_pid: u64, name: String) -> Self {
        Self {
            store: store.clone(),
            root_index: Index::new(root_pid, 0),
            name: Arc::new(name),
            locks: Arc::new([const { Mutex::new(CachePadding) }; NR_LOCKS]),
        }
    }

    pub fn new(store: Arc<Store>, root_pid: u64, name: String) -> Result<Self, OpCode> {
        let this = Self {
            store: store.clone(),
            root_index: Index::new(root_pid, 0),
            name: Arc::new(name),
            locks: Arc::new([const { Mutex::new(CachePadding) }; NR_LOCKS]),
        };

        // build an empty page with no key-value
        let iter: ItemIter<(Key<'_>, Value<&[u8]>)> = ItemIter::default();
        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).from(iter);
        let mut txn = SysTxn::new(&store);
        let (addr, mut page) = txn.alloc(delta.size())?;

        delta.build(&mut page);
        assert_eq!(page.header().epoch(), 0);
        txn.update(root_pid, root_pid, addr).expect("can't update");
        Ok(this)
    }

    pub fn id(&self) -> u64 {
        self.root_index.pid
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    fn begin(&self) -> SysTxn {
        SysTxn::new(&self.store)
    }

    #[inline]
    fn is_mgr(&self) -> bool {
        self.root_index.pid == ROOT_PID
    }

    fn lock(&self, key: u64) -> MutexGuard<CachePadding> {
        self.locks[key as usize & LOCKS_MASK]
            .lock()
            .expect("can't lock")
    }

    fn walk_page<F, K, V>(&self, mut addr: u64, mut f: F) -> Result<(), OpCode>
    where
        F: FnMut(Arc<FrameOwner>, u64, Page<K, V>) -> bool,
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
        key: &Key,
    ) -> Result<(View<'_>, Option<View<'_>>), OpCode>
    where
        T: IValCodec,
    {
        loop {
            txn.unpin_all();
            match self.try_find_leaf::<T>(txn, key.raw) {
                Ok((view, parent)) => return Ok((view, parent)),
                Err(OpCode::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    fn page_view<'b>(&self, pid: u64, range: Range<'b>) -> View<'b> {
        let addr = self.store.page.get(pid);
        assert_ne!(addr, 0);
        let f = self.store.buffer.load(addr);
        let b = f.payload();
        View {
            page_id: pid,
            page_addr: addr,
            info: b.into(),
            range,
        }
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
            let view = self.page_view(index.pid, range);

            // split may happen during search, in this case new created node are
            // not inserted into parent yet, the insert is halfly done, the
            // search can reach the `split delta` whose epoch are not equal to
            // previous one, and any modification operation CAN NOT proceed,
            // since we simplified the `find_child` process to handle data only,
            // if any other modification operation is allowed, will cause leaf
            // node disordered, by the way, any operation (including lookup)
            // will go through this function and check the condition
            if view.info.epoch() != index.epoch {
                let _ = self.parent_update::<T>(txn, view, parent);
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

    pub fn try_put<T>(&self, key: &Key, val: &Value<T>) -> Result<(), OpCode>
    where
        T: IValCodec,
    {
        let mut txn = self.begin();
        let (mut view, _) = self.find_leaf::<T>(&mut txn, key).unwrap();

        if self.need_split(&view) && self.split::<T>(&mut txn, view).is_ok() {
            return Err(OpCode::Again);
        }

        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).with_item((*key, *val));
        // NOTE: restrict logical node size
        if delta.size() > self.store.opt.page_size_threshold {
            return Err(OpCode::TooLarge);
        }
        let (new_addr, mut new_page) = txn.alloc(delta.size())?;
        debug_assert!(new_addr > view.page_addr);
        delta.build(&mut new_page);

        let h = new_page.header_mut();
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

        let _ = self.try_consolidate::<T>(&mut txn, view);

        Ok(())
    }

    pub fn put<T>(&self, key: Key, val: Value<T>) -> Result<(), OpCode>
    where
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

    fn search<T>(&self, addr: u64, key: &Key) -> Option<(Key, Val<T>)>
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
                r = Some((k, Val::new(v, f.clone())));
                return true;
            }
            false
        });
        r
    }

    pub fn try_update<T, V>(
        &self,
        hash: u64,
        key: &Key,
        val: &Value<T>,
        visible: &mut V,
    ) -> Result<Option<Val<T>>, OpCode>
    where
        T: IValCodec,
        V: FnMut(&Option<(Key, Val<T>)>) -> Result<(), OpCode>,
    {
        let mut txn = self.begin();
        let (mut view, _) = self.find_leaf::<T>(&mut txn, key).unwrap();
        if self.need_split(&view) && self.split::<T>(&mut txn, view).is_ok() {
            return Err(OpCode::Again);
        }

        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).with_item((*key, *val));
        // NOTE: restrict logical node size
        if delta.size() > self.store.opt.page_size_threshold {
            return Err(OpCode::TooLarge);
        }
        let (new_addr, mut new_page) = txn.alloc(delta.size())?;
        debug_assert!(new_addr > view.page_addr);
        delta.build(&mut new_page);

        let guard = self.lock(hash);

        let r = self.search(view.page_addr, key);
        visible(&r)?;

        loop {
            let h = new_page.header_mut();
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

        drop(guard);

        let _ = self.try_consolidate::<T>(&mut txn, view);
        Ok(r.map(|x| x.1.clone()))
    }

    // NOTE: the `visible` function may be called multiple times
    pub fn update<T, V>(
        &self,
        key: Key,
        val: Value<T>,
        mut visible: V,
    ) -> Result<Option<Val<T>>, OpCode>
    where
        T: IValCodec,
        V: FnMut(&Option<(Key, Val<T>)>) -> Result<(), OpCode>,
    {
        let mut hasher = DefaultHasher::new();
        key.raw.hash(&mut hasher);
        let h = hasher.finish();
        loop {
            match self.try_update(h, &key, &val, &mut visible) {
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

        log::info!("-------- show tree ------------");
        while let Some(pid) = q.pop_front() {
            let addr = self.store.page.get(pid);
            let _ = self.walk_page(addr, |f, addr, _: Page<Key, Value<&[u8]>>| {
                let h: PageHeader = f.payload().into();
                if h.is_intl() {
                    let pg = IntlPage::from(f.payload());
                    for i in 0..h.elems as usize {
                        if let Some((_, v)) = pg.get(i) {
                            q.push_back(v.pid);
                        }
                    }
                }

                if h.is_leaf() {
                    let pg = LeafPage::<T>::from(f.payload());
                    for i in 0..h.elems as usize {
                        let (k, v) = pg.get(i).unwrap();
                        log::info!(
                            "{} => {}\t{:?}",
                            k.to_string(),
                            v.to_string(),
                            decode_u64(addr)
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

    /// return the latest key-val pair
    pub fn get<T>(&self, key: Key) -> Result<(Key, Val<T>), OpCode>
    where
        T: IValCodec,
    {
        let mut txn = self.begin();
        let (view, _) = self.find_leaf::<T>(&mut txn, &key)?;
        let mut data = Err(OpCode::NotFound);
        // skip `split-delta`, see `find_child`
        let _ = self.walk_page(view.page_addr, |frame, _, pg: Page<Key, Value<T>>| {
            if pg.header().is_data() {
                let pos = pg.search(&key).unwrap_or_else(|x| x);
                if let Some((k, v)) = pg.get(pos) {
                    if k.raw == key.raw {
                        data = Ok((
                            k,
                            Val {
                                raw: v,
                                _owner: frame.clone(),
                            },
                        ));
                        return true;
                    }
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
    ) -> Result<(Ver, Val<T>), OpCode>
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
            let (k, v) = page.get(pos);

            if visible(k.txid, v.as_ref()) {
                if v.is_del() {
                    return Err(OpCode::NotFound);
                }
                return Ok((
                    k,
                    Val {
                        raw: v,
                        _owner: frame,
                    },
                ));
            }
            addr = h.link;
        }
        Err(OpCode::NotFound)
    }

    /// we have to traverse the entire delta chain because the delta pages are not inserted inorder
    /// by start_ts, newer delta pages may succeed in CAS and be inserted before older pages
    pub fn traverse<T, F>(&self, key: Key, mut visible: F) -> Result<(u64, Val<T>), OpCode>
    where
        T: IValCodec,
        F: FnMut(u64, &T) -> bool,
    {
        let mut txn = self.begin();
        let (view, _) = self.find_leaf::<T>(&mut txn, &key)?;
        let mut latest: Option<(Ver, Val<T>)> = None;
        let _ = self.walk_page(view.page_addr, |f, _, pg: Page<Key, Value<T>>| {
            let h = pg.header();
            // break early, since delta is always newer than base
            if h.is_base() && latest.is_some() {
                return true;
            }
            if !h.is_data() {
                return false;
            }
            debug_assert!(h.is_leaf());
            let Ok(pos) = pg.search_raw(&key) else {
                return false;
            };
            let (k, v) = pg.get(pos).unwrap();
            if visible(k.txid, v.as_ref()) {
                let ver = *k.ver();
                if let Some((last_key, _)) = latest {
                    // select key with larger txid and cmd_id
                    if last_key > ver {
                        latest = Some((ver, Val::new(v, f)));
                    }
                } else {
                    latest = Some((ver, Val::new(v, f)));
                }
            } else {
                // all rest `raw` is checked here
                if let Some(s) = v.sibling() {
                    if let Ok((ver, val)) = self.traverse_sibling(key.txid, s.addr(), &mut visible)
                    {
                        latest = Some((ver, val));
                    }
                    return true;
                }
                // check next delta
            }
            false
        });
        if let Some((k, v)) = latest {
            return if v.is_del() {
                Err(OpCode::NotFound)
            } else {
                Ok((k.txid, v))
            };
        }
        Err(OpCode::NotFound)
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
    ) -> (PageMergeIter<'a, K, V>, PageHeader, Vec<u64>)
    where
        K: IKey,
        V: IVal,
    {
        let cap = view.info.depth() as usize;
        let mut builder = MergeIterBuilder::new(cap);
        let mut info = view.info;
        let mut junks = Vec::with_capacity(cap);
        let mut split = None;

        let _ = self.walk_page(view.page_addr, |frame, addr, pg| {
            let h = pg.header();
            txn.pin_frame(&frame);

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
            junks.push(addr);
            false
        });

        let iter = PageMergeIter::new(builder.build(), split);
        (iter, info, junks)
    }

    fn get_txid(&self) -> u64 {
        use std::sync::atomic::Ordering::Relaxed;
        self.store.context.wmk_oldest.load(Relaxed)
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
        let (iter, info, junks) = self.collect_delta(txn, &view);
        let iter = f(iter, self.get_txid());
        let mut delta = Delta::new(view.info.delta_type(), view.info.node_type()).from(iter);
        let mut txn = self.begin();
        let (new_addr, mut new_page) = txn.alloc(delta.size())?;
        debug_assert!(new_addr > view.page_addr);

        delta.build(&mut new_page);
        let h = new_page.header_mut();
        h.set_epoch(view.info.epoch());
        h.set_depth(info.depth());
        debug_assert_eq!(info.link(), 0); // since we are full consolidated
        h.set_link(info.link());

        txn.replace(view.page_id, view.page_addr, new_addr, &junks)
            .map(|_| {
                view.page_addr = new_addr;
                view.info = *new_page.header();
                view
            })
            .map_err(|_| OpCode::Again)
    }

    fn add_sibling<'a, T: IValCodec>(
        &'a self,
        txn: &mut SysTxn,
        junks: &mut Vec<u64>,
        src_key: Key<'a>,
        b: &mut LeafMergeIter<'a, T>,
        mut addr: u64,
    ) {
        let (ptr, len) = (src_key.raw.as_ptr(), src_key.raw.len());

        while addr != 0 {
            let frame = self.store.buffer.load(addr);
            let pg = SlottedPage::<Ver, T>::from(frame.payload());
            let h = pg.header();
            txn.pin_frame(&frame);

            for i in 0..h.elems as usize {
                let (k, v) = pg.get(i);
                let raw = unsafe { std::slice::from_raw_parts(ptr, len) };
                b.add(Key::new(raw, k.txid, k.cmd), v);
            }
            junks.push(addr);
            addr = h.link();
        }
    }

    fn collect_leaf<'a, T: IValCodec>(
        &'a self,
        txn: &mut SysTxn,
        view: &View<'a>,
    ) -> (LeafMergeIter<'a, T>, PageHeader, Vec<u64>) {
        let cap = view.info.depth() as usize;
        let mut b = LeafMergeIter::new(cap);
        let mut info = view.info;
        let mut junks = Vec::with_capacity(cap);
        let mut split = None;

        let _ = self.walk_page(view.page_addr, |frame, addr, pg: Page<Key, Value<T>>| {
            let h = pg.header();
            txn.pin_frame(&frame);

            debug_assert!(h.is_leaf());

            if h.is_data() {
                for i in 0..h.elems as usize {
                    let (k, v) = pg.get(i).unwrap();
                    if let Some(sp) = split {
                        if k.raw > sp {
                            // split is happened only after consolidation, thus only the base page
                            // will contain keys > split key
                            debug_assert!(h.is_base());
                            break;
                        }
                    }
                    if let Some(s) = v.sibling() {
                        // add latest version first
                        b.add(Key::new(k.raw, k.txid, k.cmd), s.to(*v.as_ref()));
                        self.add_sibling(txn, &mut junks, k, &mut b, s.addr());
                    } else {
                        b.add(k, v);
                    }
                }
            } else if split.is_none() {
                let (k, _) = Self::decode_split_delta(pg.raw());
                split = Some(k);
            }

            info = *h;
            junks.push(addr);
            false
        });
        (b, info, junks)
    }

    fn consolidate_leaf<'a, T: IValCodec>(
        &'a self,
        txn: &mut SysTxn,
        mut view: View<'a>,
    ) -> Result<View<'a>, OpCode> {
        let (mut iter, info, junks) = self.collect_leaf(txn, &view);
        iter.purge(self.get_txid());

        let mut builder = FuseBuilder::<T>::new(info.delta_type(), info.node_type());
        builder.prepare(iter);
        let (new_addr, mut new_page) = builder.build(txn)?;
        debug_assert!(new_addr > view.page_addr);

        let h = new_page.header_mut();
        h.set_epoch(view.info.epoch());
        h.set_depth(info.depth());
        debug_assert_eq!(info.link(), 0);
        h.set_link(info.link());

        assert_eq!(h.delta_type(), info.delta_type());
        assert_eq!(h.node_type(), info.node_type());

        txn.replace(view.page_id, view.page_addr, new_addr, &junks)
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
        }
        Ok(())
    }

    /// NOTE: this is the second phase of `split` smo
    fn parent_update<T>(
        &self,
        txn: &mut SysTxn,
        view: View,
        parent: Option<View>,
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
        let (new_addr, mut new_page) = txn.alloc(d.size())?;
        debug_assert!(new_addr > view.page_addr);

        d.build(&mut new_page);

        let m = &parent.info.meta;
        let h = new_page.header_mut();
        h.set_depth(m.depth().saturating_add(1));
        h.set_epoch(m.epoch());
        h.set_link(parent.page_addr);

        txn.update(parent.page_id, parent.page_addr, new_addr)
            .map(|_| {
                parent.page_addr = new_addr;
                parent.info = *new_page.header();
            })
            .map_err(|_| OpCode::Again)?;

        let _ = self.try_consolidate::<T>(txn, parent);
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
            let (laddr, mut lpage) = txn.alloc(d.size())?;
            d.build(&mut lpage);
            txn.map(laddr)
        };
        let r = {
            let mut d = Delta::new(DeltaType::Data, view.info.node_type()).from(ri);
            let (raddr, mut rpage) = txn.alloc(d.size())?;
            d.build(&mut rpage);
            txn.map(raddr)
        };

        let s = [
            // add an empty key to simplify operations, such as `find_child`
            // the keys < sep_key are at left, those <= sep_key are at right
            ([].as_slice(), Index::new(l, 0)),
            (sep_key.raw(), Index::new(r, 0)),
        ];

        // the new root
        let mut delta = Delta::new(DeltaType::Data, NodeType::Intl).with_slice(&s);
        let (new_addr, mut new_page) = txn.alloc(delta.size())?;
        debug_assert!(new_addr > view.page_addr);

        delta.build(&mut new_page);
        txn.replace(view.page_id, view.page_addr, new_addr, &[view.page_addr])
    }

    fn split_non_root<K, V>(&self, txn: &mut SysTxn, view: View) -> Result<(), OpCode>
    where
        K: IKey,
        V: IVal,
    {
        if view.page_id == self.root_index.pid {
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
            let (new_addr, mut new_page) = txn.alloc(d.size())?;

            d.build(&mut new_page);
            txn.map(new_addr)
        };

        // split-delta contains: split-key + new split page id
        let mut delta = Delta::new(DeltaType::Split, view.info.node_type())
            .with_item((sep.raw(), Index::new(rpid, 0)));
        let (new_addr, mut new_page) = txn.alloc(delta.size())?;
        debug_assert!(new_addr > view.page_addr);
        delta.build(&mut new_page);

        let h = new_page.header_mut();
        h.set_epoch(view.info.epoch() + 1); // identify a split
        h.set_depth(view.info.depth().saturating_add(1));
        h.set_link(view.page_addr);

        txn.update(view.page_id, view.page_addr, new_addr)
            .map_err(|_| OpCode::Again)?;

        Ok(())
    }

    fn need_split(&self, view: &View) -> bool {
        if view.info.is_split() || view.info.elems < 2 {
            return false;
        }

        let mut max_size = self.store.opt.page_size_threshold as u32;
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

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::{Registry, Store, Tree};
    use crate::utils::traits::IValCodec;
    use crate::RandomPath;
    use crate::{
        index::{data::Value, Key},
        utils::ROOT_PID,
        OpCode, Options,
    };

    fn new_mgr(path: PathBuf) -> Result<Registry, OpCode> {
        let _ = std::fs::remove_dir_all(&path);
        let store = Arc::new(Store::new(Options::new(&path))?);
        Registry::new(store)
    }

    fn put_test(t: &Tree, k: Key, v: Value<&[u8]>) {
        assert!(t.put(k, v).is_ok());
    }

    fn get_test<const OK: bool>(t: &Tree, k: Key, v: Value<&[u8]>) {
        let (rk, rv) = t.get::<&[u8]>(k).unwrap();
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

        let sb1 = mgr.open("mo")?;
        let sb2 = mgr.open("ha")?;
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

        let s1 = mgr.open("mo")?;
        let s2 = mgr.open("ha")?;

        get_test::<false>(&s1, k1, v1);
        get_test::<false>(&s2, k2, v2);

        assert!(mgr.tree.get::<&[u8]>(k1).is_err());
        Ok(())
    }

    #[test]
    fn more_subtree() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let mgr = new_mgr(path.clone())?;
        let mut name = Vec::new();

        for i in 0..20 {
            let x = format!("sub_{i}");
            assert!(mgr.open(&x).is_ok());
            name.push(x);
        }

        for (i, x) in name.iter().enumerate() {
            let s = mgr.open(x).unwrap();
            assert_eq!(s.root_index.pid, i as u64 + ROOT_PID + 1);
        }

        let t1 = mgr.open("t1").unwrap();
        let t2 = mgr.open("t1").unwrap();
        assert_eq!(t1.name(), t2.name());

        let e = mgr.remove(&t1).err();
        assert_eq!(e, Some(OpCode::Again));
        let o = mgr.remove(&t2);
        assert!(o.is_ok());

        Ok(())
    }

    #[test]
    fn traverse() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let mgr = new_mgr(path.clone())?;
        let s = mgr.open("mo")?;

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
        let store = Arc::new(Store::new(opt)?);
        let mgr = Registry::new(store)?;
        let s = mgr.open("s")?;

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
        assert_eq!(out.1.data(), "11".as_bytes());

        Ok(())
    }
}
