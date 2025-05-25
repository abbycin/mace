use std::{
    cmp::{self, Ordering},
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::{Deref, DerefMut, Range},
};

use crate::{
    index::data::Key,
    static_assert,
    utils::{
        NULL_PID,
        bytes::ByteArray,
        traits::{ICodec, IDataLoader, IInfer, IKey, IPageIter, IVal, IValCodec},
        unpack_id,
    },
};

use super::{
    data::{Index, SLOT_LEN, Slot, Value},
    iter::MergeIter,
};

pub(crate) const NULL_INDEX: Index = Index::new(NULL_PID, 0);

pub trait IPageHdr {
    fn elems(&self) -> usize;
}

#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub(crate) struct SibPageHdr {
    pub(crate) link: u64,
    pub(crate) elems: u32,
    page_size: u32,
}

impl SibPageHdr {
    pub fn init(&mut self, page_size: u32, elems: u32) {
        self.link = 0;
        self.elems = elems;
        self.page_size = page_size;
    }

    pub fn link(&self) -> u64 {
        self.link
    }
}

impl IPageHdr for SibPageHdr {
    fn elems(&self) -> usize {
        self.elems as usize
    }
}

/// - epoch , highest 48 bits
/// - delta chain length, middle 8 bits
/// - node type and delta type, lowest 8 bits
#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct Meta(u64);

impl Meta {
    /// NOTE: depth is not set here
    pub fn new(dt: DeltaType, nt: NodeType) -> Self {
        Self((dt as u16 | nt as u16) as u64)
    }

    pub fn epoch(&self) -> u64 {
        self.0 >> 16
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.0 = (epoch << 16) | self.0 & 0xffff;
    }

    pub fn depth(&self) -> u8 {
        ((self.0 >> 8) & 0xff) as u8
    }

    pub fn is_base(&self) -> bool {
        self.depth() == 1
    }

    pub fn set_depth(&mut self, depth: u8) {
        self.0 = (self.0 & !(0xff << 8)) | ((depth as u64) << 8);
    }

    pub fn is_leaf(&self) -> bool {
        match self.node_type() {
            NodeType::Intl => false,
            NodeType::Leaf => true,
        }
    }

    pub fn is_intl(&self) -> bool {
        !self.is_leaf()
    }

    pub fn is_data(&self) -> bool {
        match self.delta_type() {
            DeltaType::Data => true,
            DeltaType::Split => false,
        }
    }

    pub fn is_split(&self) -> bool {
        !self.is_data()
    }

    pub fn node_type(&self) -> NodeType {
        (self.0 as u8 & NODE_TYPE_MASK).into()
    }

    pub fn delta_type(&self) -> DeltaType {
        (self.0 as u8 & DELTA_TYPE_MASK).into()
    }
}

impl Display for Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Meta")
            .field("epoch", &self.epoch())
            .field("depth", &self.depth())
            .field("delta_type", &self.delta_type())
            .field("node_type", &self.node_type())
            .finish()
    }
}

impl Debug for Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_string())
    }
}

const DELTA_SPLIT_BIT: u8 = 0b1000_0000;
const DELTA_DATA_BIT: u8 = 0b0100_0000;
const NODE_LEAF_BIT: u8 = 0b0000_1000;
const NODE_INTL_BIT: u8 = 0b0000_0100;

const DELTA_TYPE_MASK: u8 = 0b1111_0000;
const NODE_TYPE_MASK: u8 = 0b0000_1111;

#[derive(PartialEq, Clone, Copy, Debug)]
#[repr(u8)]
pub enum DeltaType {
    Split = DELTA_SPLIT_BIT,
    Data = DELTA_DATA_BIT,
}

#[derive(PartialEq, Clone, Copy, Debug)]
#[repr(u8)]
pub enum NodeType {
    Leaf = NODE_LEAF_BIT,
    Intl = NODE_INTL_BIT,
}

impl From<u8> for DeltaType {
    fn from(value: u8) -> Self {
        match value & DELTA_TYPE_MASK {
            DELTA_DATA_BIT => DeltaType::Data,
            DELTA_SPLIT_BIT => DeltaType::Split,
            _ => unreachable!(),
        }
    }
}

impl From<u8> for NodeType {
    fn from(value: u8) -> Self {
        match value & NODE_TYPE_MASK {
            NODE_LEAF_BIT => NodeType::Leaf,
            NODE_INTL_BIT => NodeType::Intl,
            _ => unreachable!(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct MainPageHdr {
    pub meta: Meta,
    /// logical link to delta or base page (file_id + offset)
    link: u64,
    /// NOTE: the page has same count of both key-value and key-index pair
    pub elems: u32,
    /// page size (including header)
    pub len: u32,
}

impl From<ByteArray> for MainPageHdr {
    fn from(value: ByteArray) -> Self {
        unsafe { *(value.data() as *const MainPageHdr) }
    }
}

impl Debug for MainPageHdr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "PageHeader {{ {:?}, next: {} {:?}, elems: {}, len {} }}",
            self.meta,
            self.link,
            unpack_id(self.link),
            self.elems,
            self.len,
        ))
    }
}

impl MainPageHdr {
    pub fn set_link(&mut self, link: u64) -> &mut Self {
        self.link = link;
        self
    }

    pub fn link(&self) -> u64 {
        self.link
    }
}

impl IPageHdr for MainPageHdr {
    fn elems(&self) -> usize {
        self.elems as usize
    }
}

impl Deref for MainPageHdr {
    type Target = Meta;
    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}

impl DerefMut for MainPageHdr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.meta
    }
}

pub const MAINPG_HDR_LEN: usize = size_of::<MainPageHdr>();
static_assert!(MAINPG_HDR_LEN == 24);

#[derive(Clone)]
pub struct MainPage<K, V, H = MainPageHdr>
where
    H: IPageHdr,
{
    raw: ByteArray,
    _marker: PhantomData<(K, V, H)>,
}

pub type SibPage<K, V> = MainPage<K, V, SibPageHdr>;

pub const SIBPG_HDR_LEN: usize = size_of::<SibPageHdr>();

impl<K, V, H> MainPage<K, V, H>
where
    K: IKey,
    V: IVal,
    H: IPageHdr,
{
    const HDR_LEN: usize = size_of::<H>();

    pub fn from(b: ByteArray) -> Self {
        Self {
            raw: b,
            _marker: PhantomData,
        }
    }

    pub fn raw(&self) -> ByteArray {
        self.raw
    }

    pub fn header(&self) -> &H {
        unsafe { &*self.raw.data().cast::<H>() }
    }

    pub fn header_mut(&mut self) -> &mut H {
        unsafe { &mut *self.raw.data().cast::<H>() }
    }

    #[inline]
    fn offset(&self, pos: usize) -> usize {
        self.raw.read::<u32>(Self::HDR_LEN + pos * SLOT_LEN) as usize
    }

    pub fn key_at<L: IDataLoader>(&self, l: &L, pos: usize) -> K {
        let off = self.offset(pos);
        debug_assert!(self.raw.len() > off);
        let o = Slot::decode_from(self.raw.skip(off));
        if o.is_inline() {
            K::decode_from(self.raw.skip(off + o.packed_size()))
        } else {
            let f = l.load_data(o.addr());
            K::decode_from(f.infer())
        }
    }

    #[allow(unused)]
    pub fn val_at<L: IDataLoader>(&self, l: &L, pos: usize) -> V {
        let (_, v) = self.get(l, pos).unwrap();
        v
    }

    pub fn get<L: IDataLoader>(&self, l: &L, pos: usize) -> Option<(K, V)> {
        if pos >= self.header().elems() {
            None
        } else {
            let off = self.offset(pos);
            let o = Slot::decode_from(self.raw.skip(off));
            if o.is_inline() {
                let oz = o.packed_size();
                let k = K::decode_from(self.raw.skip(off + oz));
                let v = V::decode_from(self.raw.skip(off + oz + k.packed_size()));
                Some((k, v))
            } else {
                let f = l.load_data(o.addr());
                let b = f.infer();
                let k = K::decode_from(b);
                let v = V::decode_from(b.skip(k.packed_size()));
                Some((k, v))
            }
        }
    }

    /// NOTE: no duplication is allowed
    pub(crate) fn lower_bound<L: IDataLoader>(&self, l: &L, key: &K) -> Result<usize, usize> {
        let cnt = self.header().elems();
        let mut lo: usize = 0;
        let mut hi: usize = cnt;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.key_at(l, mid).cmp(key) {
                Ordering::Less => lo = mid + 1,
                _ => hi = mid,
            }
        }

        if lo == cnt { Err(lo) } else { Ok(lo) }
    }

    fn search_by<L, F>(&self, l: &L, key: &K, f: F) -> Result<usize, usize>
    where
        L: IDataLoader,
        F: Fn(&K, &K) -> Ordering,
    {
        let h = self.header();
        let mut lo = 0;
        let mut hi = h.elems();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let k = self.key_at(l, mid);
            match f(&k, key) {
                cmp::Ordering::Equal => return Ok(mid),
                cmp::Ordering::Greater => {
                    hi = mid;
                }
                cmp::Ordering::Less => {
                    lo = mid + 1;
                }
            }
        }
        Err(lo)
    }

    pub fn search<L: IDataLoader>(&self, l: &L, key: &K) -> Result<usize, usize> {
        self.search_by(l, key, |x, y| x.cmp(y))
    }

    pub fn search_raw<L: IDataLoader>(&self, l: &L, key: &K) -> Result<usize, usize> {
        self.search_by(l, key, |x, y| x.raw().cmp(y.raw()))
    }

    #[allow(dead_code)]
    pub fn show<L: IDataLoader>(&self, l: &L, addr: u64) {
        let n = self.header().elems();
        log::debug!("------------- show page -----------",);
        for i in 0..n {
            let (k, v) = self.get(l, i).unwrap();
            log::debug!(
                "{} => {}\t{}",
                k.to_string(),
                v.to_string(),
                unpack_id(addr).1
            );
        }
        log::debug!("--------------- end --------------");
    }
}

pub struct RangeIter<K, V, L>
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    page: MainPage<K, V>,
    l: L,
    range: Range<usize>,
    index: usize,
}

impl<K, V, L> RangeIter<K, V, L>
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    pub fn new(page: MainPage<K, V>, l: L, range: Range<usize>) -> Self {
        let index = range.start;
        Self {
            page,
            l,
            range,
            index,
        }
    }
}

impl<K, V, L> Iterator for RangeIter<K, V, L>
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.range.end {
            let (k, v) = self.page.get(&self.l, self.index).unwrap();
            self.index += 1;
            Some((k, v))
        } else {
            None
        }
    }
}

impl<K, V, L> IPageIter for RangeIter<K, V, L>
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    fn rewind(&mut self) {
        self.index = self.range.start;
    }
}

pub struct PageMergeIter<'a, K, V, L>
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    iter: MergeIter<RangeIter<K, V, L>>,
    split: Option<&'a [u8]>,
}

impl<'a, K, V, L> PageMergeIter<'a, K, V, L>
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    pub fn new(iter: MergeIter<RangeIter<K, V, L>>, split: Option<&'a [u8]>) -> Self {
        Self { iter, split }
    }
}

impl<K, V, L> Iterator for PageMergeIter<'_, K, V, L>
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let (k, v) = self.iter.next()?;

        if let Some(split) = self.split {
            if k.raw() >= split {
                return None;
            }
        }
        Some((k, v))
    }
}

impl<K, V, L> IPageIter for PageMergeIter<'_, K, V, L>
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

pub struct LeafMergeIter<'a, T: IValCodec> {
    data: Vec<(Key<'a>, Value<T>)>,
    index: usize,
}

impl<'a, T> Iterator for LeafMergeIter<'a, T>
where
    T: IValCodec + 'a,
{
    type Item = &'a (Key<'a>, Value<T>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.data.len() {
            let r = &self.data[self.index];
            self.index += 1;
            unsafe {
                let p = r as *const (Key, Value<T>);
                Some(&*p)
            }
        } else {
            None
        }
    }
}

impl<'a, T: IValCodec> LeafMergeIter<'a, T> {
    pub fn new(cap: usize) -> Self {
        Self {
            data: Vec::with_capacity(cap),
            index: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn add(&mut self, k: Key<'a>, v: Value<T>) {
        self.data.push((k, v));
    }

    pub fn rewind(&mut self) {
        self.index = 0;
    }

    pub fn seek_to(&mut self, pos: usize) {
        self.index = pos;
    }

    pub fn curr_pos(&mut self) -> usize {
        self.index
    }

    pub fn reset(&mut self) {
        self.index = 0;
        self.data.clear();
    }

    pub fn sort(&mut self) {
        // stable sort for less cache/branch misses
        self.data.sort_by(|x, y| x.0.cmp(&y.0));
    }

    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&(Key<'a>, Value<T>)) -> bool,
    {
        if self.data.is_empty() {
            return;
        }
        self.sort();

        let len = self.data.len();
        let mut l = 0;
        let mut last = self.data[0].0.raw;
        let mut group = 0;

        for r in 1..=len {
            if r == len || self.data[r].0.raw != last {
                let cur = self.data[group];

                if !cur.1.is_del() && f(&cur) {
                    if l != group {
                        self.data[l] = cur;
                    }
                    l += 1;
                }

                if r < len {
                    last = self.data[r].0.raw;
                    group = r;
                }
            }
        }
        self.data.resize(l, Default::default());
    }

    // remove duplicated version or tombstone less than txid
    pub fn purge(&mut self, txid: u64) {
        if self.data.is_empty() {
            return;
        }
        let mut tmp = Vec::with_capacity(self.data.len());
        let mut last_key = None;
        let mut skip_dup = false;

        // required
        self.sort();

        for (k, v) in self.data.iter() {
            if let Some(last) = last_key {
                if last == k.raw() {
                    if skip_dup {
                        continue;
                    }

                    if k.txid > txid {
                        tmp.push((*k, *v));
                        continue;
                    }

                    // it's the oldest version, the reset versions will never be accessed by all txn
                    skip_dup = true;
                    match *v {
                        // skip only when removed and is safe
                        Value::Del(_) => continue,
                        _ => {
                            tmp.push((*k, *v));
                            continue;
                        }
                    }
                }
            }

            last_key = Some(k.raw());
            skip_dup = k.txid <= txid;
            match *v {
                // skip only when removed and is safe
                Value::Del(_) if k.txid <= txid => continue,
                _ => tmp.push((*k, *v)),
            }
        }

        std::mem::swap(&mut self.data, &mut tmp);
    }
}

pub struct IntlMergeIter<'a, L>
where
    L: IDataLoader,
{
    iter: PageMergeIter<'a, &'a [u8], Index, L>,
    last: Option<&'a [u8]>,
}

impl<'a, L> IntlMergeIter<'a, L>
where
    L: IDataLoader,
{
    pub fn new(iter: PageMergeIter<'a, &'a [u8], Index, L>, _txid: u64) -> Self {
        Self { iter, last: None }
    }
}

impl<'a, L> Iterator for IntlMergeIter<'a, L>
where
    L: IDataLoader,
{
    type Item = (&'a [u8], Index);

    fn next(&mut self) -> Option<Self::Item> {
        for (k, i) in &mut self.iter {
            // skip range placeholder
            if i == NULL_INDEX {
                continue;
            }

            // skip old duplicated index
            if let Some(last) = self.last {
                if k == last {
                    continue;
                }
            }

            self.last = Some(k);
            return Some((k, i));
        }
        None
    }
}

impl<L> IPageIter for IntlMergeIter<'_, L>
where
    L: IDataLoader,
{
    fn rewind(&mut self) {
        self.iter.rewind();
        self.last = None;
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::Relaxed;

    use crate::OpCode;
    use crate::index::IAlloc;
    use crate::index::builder::Delta;
    use crate::index::data::{Index, Key, Slot, Value};
    use crate::index::iter::MergeIterBuilder;
    use crate::index::page::{
        DeltaType, MAINPG_HDR_LEN, MainPage, NodeType, PageMergeIter, SLOT_LEN,
    };
    use crate::map::data::{FrameOwner, FrameRef};
    use crate::utils::block::Block;
    use crate::utils::traits::{ICodec, IDataLoader, IKey, IVal};
    use crate::utils::{AMutRef, pack_id};

    use super::{IntlMergeIter, MainPageHdr, Meta, NULL_INDEX, RangeIter};

    #[test]
    fn test_meta() {
        let mut m = Meta::new(DeltaType::Data, NodeType::Leaf);

        assert!(m.is_data());
        assert!(m.is_leaf());

        assert!(!m.is_base());

        m.set_depth(114);
        m.set_depth(1);
        assert_eq!(m.depth(), 1);
        assert!(m.is_base());

        m.set_epoch(514);
        m.set_epoch(114);
        assert_eq!(m.epoch(), 114);
    }

    struct DummyAlloc {
        loader: DummyLoader,
    }

    impl DummyAlloc {
        fn new(l: DummyLoader) -> Self {
            Self { loader: l }
        }
    }

    impl IAlloc for DummyAlloc {
        fn allocate(&mut self, size: usize) -> Result<FrameRef, OpCode> {
            self.loader.alloc(size)
        }

        fn page_size(&self) -> u32 {
            512
        }

        fn limit_size(&self) -> u32 {
            self.page_size() / 2
        }
    }

    #[derive(Clone)]
    struct DummyLoader {
        map: AMutRef<HashMap<u64, FrameOwner>>,
        addr: AMutRef<AtomicU64>,
    }

    impl DummyLoader {
        fn new() -> Self {
            Self {
                map: AMutRef::new(HashMap::new()),
                addr: AMutRef::new(AtomicU64::new(pack_id(1, 0))),
            }
        }

        fn alloc(&mut self, size: usize) -> Result<FrameRef, OpCode> {
            let f = FrameOwner::alloc(size);
            let addr = self.addr.fetch_add(size as u64, Relaxed);
            self.map.insert(addr, f.clone());
            let mut fr = f.as_ref();
            fr.set_addr(addr);
            Ok(fr)
        }
    }

    impl IDataLoader for DummyLoader {
        type Out = FrameOwner;

        fn load_data(&self, addr: u64) -> Self::Out {
            self.map.get(&addr).unwrap().clone()
        }
    }

    #[test]
    fn test_page() {
        let pairs = [
            ("key1", "val1"),
            ("key2", "val2"),
            ("key3", "val3"),
            ("key4", "val4"),
            ("key5", "val5"),
            ("key6", "val6"),
            ("key7", "val7"),
            ("key8", "val8"),
        ];
        let loader = DummyLoader::new();
        let mut da = DummyAlloc::new(loader.clone());

        let kv: Vec<(Key, Value<&[u8]>)> = pairs
            .iter()
            .map(|(k, v)| (Key::new(k.as_bytes(), 0, 0), (Value::Put(v.as_bytes()))))
            .collect();
        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).with_slice(da.page_size(), &kv);
        let x = Block::aligned_alloc(delta.size(), align_of::<MainPageHdr>());
        let a = x.view(0, x.len());

        let h = delta.build(a, &mut da);

        assert_eq!(h.elems, pairs.len() as u32);
        assert!(h.meta.is_leaf());
        let k = Key::new("key1".as_bytes(), 0, 0);
        let v = Value::Put("val1".as_bytes());
        let hdr_size = MAINPG_HDR_LEN;
        let index_table_size = h.elems as usize * SLOT_LEN;
        let slot_sz = Slot::inline().packed_size();
        let expect_len = (k.packed_size() + v.packed_size() + slot_sz) * pairs.len()
            + hdr_size
            + index_table_size;
        assert_eq!(h.len as usize, expect_len);

        let page = MainPage::<Key, Value<&[u8]>>::from(a);

        let k = page.key_at(&loader, 0);
        assert_eq!(k.raw, "key1".as_bytes());

        let (k, v) = page.get(&loader, 0).unwrap();
        assert_eq!(k.raw, "key1".as_bytes());
        assert!(!v.is_del());
        assert_eq!(*v.as_ref(), "val1".as_bytes());

        match page.search(&loader, &Key::new("key2".as_bytes(), 0, 0)) {
            Ok(pos) => {
                assert_eq!(pos, 1);
            }
            Err(_) => unreachable!(),
        }

        match page.search(&loader, &Key::new("key0".as_bytes(), 0, 0)) {
            Ok(_) => unreachable!(),
            Err(pos) => assert_eq!(pos, 0),
        }

        match page.search(&loader, &Key::new("keyz".as_bytes(), 0, 0)) {
            Ok(_) => unreachable!(),
            Err(pos) => assert_eq!(pos, pairs.len()),
        }

        // ------------------------ big kv --------------------------------

        let (kvec, vvec) = (vec![114; 256], vec![233; 256]);
        let (k, v) = (
            Key::new(kvec.as_slice(), 1919810, 114514),
            Value::Put(vvec.as_slice()),
        );
        let kv = [(k, v)];

        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).with_slice(da.page_size(), &kv);
        let x = Block::aligned_alloc(delta.size(), align_of::<MainPageHdr>());
        let a = x.view(0, x.len());

        delta.build(a, &mut da);

        let page = MainPage::<Key, Value<&[u8]>>::from(a);

        let (k, v) = page.get(&loader, 0).unwrap();
        assert_eq!(k.raw, kvec.as_slice());
        assert_eq!(*v.as_ref(), vvec.as_slice());
    }

    fn must_match<I, K, V, L>(src: I, l: &L, p: MainPage<K, V>)
    where
        I: Iterator<Item = (K, V)>,
        K: IKey,
        V: IVal,
        L: IDataLoader,
    {
        for (k, v) in src {
            let pos = p.search(l, &k).expect("fail");
            let x = p.val_at(l, pos);
            assert_eq!(x.to_string(), v.to_string());
        }
    }

    #[test]
    fn intl_iter() {
        let loader = DummyLoader::new();
        let mut da = DummyAlloc::new(loader.clone());
        let s1 = [
            ([].as_slice(), Index::new(1, 0)),
            ("key1".as_bytes(), Index::new(2, 0)),
            ("key3".as_bytes(), NULL_INDEX),
        ];
        let s2 = [
            ("key3".as_bytes(), Index::new(3, 0)),
            ("key4".as_bytes(), NULL_INDEX),
        ];
        let s3 = [("key5".as_bytes(), Index::new(4, 0))];

        let mut delta1 =
            Delta::new(DeltaType::Data, NodeType::Intl).with_slice(da.page_size(), &s1);
        let mut delta2 =
            Delta::new(DeltaType::Data, NodeType::Intl).with_slice(da.page_size(), &s2);
        let mut delta3 =
            Delta::new(DeltaType::Data, NodeType::Intl).with_slice(da.page_size(), &s3);

        let x1 = Block::aligned_alloc(delta1.size(), align_of::<MainPageHdr>());
        let x2 = Block::aligned_alloc(delta1.size(), align_of::<MainPageHdr>());
        let x3 = Block::aligned_alloc(delta1.size(), align_of::<MainPageHdr>());
        let b1 = x1.view(0, x1.len());
        let b2 = x2.view(0, x2.len());
        let b3 = x3.view(0, x3.len());

        let pg1 = MainPage::<&[u8], Index>::from(b1);
        let pg2 = MainPage::<&[u8], Index>::from(b2);
        let pg3 = MainPage::<&[u8], Index>::from(b3);

        delta1.build(b1, &mut da);
        delta2.build(b2, &mut da);
        delta3.build(b3, &mut da);

        must_match(s1.into_iter(), &loader, pg1.clone());
        must_match(s2.into_iter(), &loader, pg2.clone());
        must_match(s3.into_iter(), &loader, pg3.clone());

        let mut builder = MergeIterBuilder::new(3);

        builder.add(RangeIter::new(
            pg1.clone(),
            loader.clone(),
            0..pg1.header().elems as usize,
        ));
        builder.add(RangeIter::new(
            pg2.clone(),
            loader.clone(),
            0..pg2.header().elems as usize,
        ));
        builder.add(RangeIter::new(
            pg3.clone(),
            loader.clone(),
            0..pg3.header().elems as usize,
        ));

        let iter = IntlMergeIter::new(PageMergeIter::new(builder.build(), None), 0);
        let mut delta = Delta::new(DeltaType::Data, NodeType::Intl).from(iter, da.page_size());
        let x4 = Block::aligned_alloc(delta.size(), align_of::<MainPageHdr>());
        let b = x4.view(0, x4.len());
        let pg = MainPage::<&[u8], Index>::from(b);

        delta.build(b, &mut da);

        let expact = [
            ([].as_slice(), Index::new(1, 0)),
            ("key1".as_bytes(), Index::new(2, 0)),
            ("key3".as_bytes(), Index::new(3, 0)),
            ("key5".as_bytes(), Index::new(4, 0)),
        ];

        must_match(expact.into_iter(), &loader, pg);
    }
}
