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
        bytes::ByteArray,
        traits::{ICodec, IKey, IPageIter, IVal, IValCodec},
        unpack_id, NULL_PID,
    },
};

use super::{
    data::{Index, Slot, Value, SLOT_LEN},
    iter::MergeIter,
};

pub(crate) const NULL_INDEX: Index = Index::new(NULL_PID, 0);

pub(crate) struct PageBuilder {
    payload: ByteArray,
    slots: ByteArray,
    payload_index: usize,
    slot_index: usize,
    offset: u32,
    slot_size: u32,
}

impl PageBuilder {
    pub(crate) fn from(b: &ByteArray, elems: usize) -> Self {
        let slot_size = elems * SLOT_LEN + PAGE_HEADER_SIZE;
        Self {
            slots: b.sub_array(PAGE_HEADER_SIZE, slot_size - PAGE_HEADER_SIZE),
            payload: b.sub_array(slot_size, b.len() - slot_size),
            payload_index: 0,
            slot_index: 0,
            offset: slot_size as u32,
            slot_size: slot_size as u32,
        }
    }

    pub(crate) fn add<K, V>(&mut self, k: &K, v: &V)
    where
        K: ICodec,
        V: ICodec,
    {
        let (ksz, vsz) = (k.packed_size(), v.packed_size());
        let total_sz = ksz + vsz;

        let (kdst, vdst) = self
            .payload
            .as_mut_slice::<u8>(self.payload_index, total_sz)
            .split_at_mut(ksz);

        k.encode_to(kdst);
        v.encode_to(vdst);

        let sdst = self.slots.as_mut_slice(self.slot_index, SLOT_LEN);
        let slot = Slot::from(sdst);
        slot.reset(
            self.slot_size + self.payload_index as u32,
            ksz as u32,
            total_sz as u32,
        );

        self.slot_index += SLOT_LEN;
        self.payload_index += total_sz;
        self.offset += total_sz as u32;
    }
}

#[derive(Clone, Copy)]
pub struct Page<K, V> {
    raw: ByteArray,
    _marker: PhantomData<(K, V)>,
}

pub struct LeafPage<T>
where
    T: IValCodec,
{
    raw: ByteArray,
    _marker: PhantomData<T>,
}

pub struct IntlPage {
    raw: ByteArray,
}

/// consists of:
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
pub struct PageHeader {
    /// visibility check on leaf node
    pub meta: Meta,
    /// physical link to delta or base page (file_id + offset)
    link: u64,
    /// NOTE: the page has same count of both key-value and key-index pair
    pub elems: u32,
    /// logical page size (including header)
    pub len: u32,
}

impl From<ByteArray> for PageHeader {
    fn from(value: ByteArray) -> Self {
        unsafe { *(value.data() as *const PageHeader) }
    }
}

impl Debug for PageHeader {
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

impl PageHeader {
    pub fn set_link(&mut self, link: u64) -> &mut Self {
        self.link = link;
        self
    }

    pub fn link(&self) -> u64 {
        self.link
    }
}

impl Deref for PageHeader {
    type Target = Meta;
    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}

impl DerefMut for PageHeader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.meta
    }
}

pub const PAGE_HEADER_SIZE: usize = size_of::<PageHeader>();
static_assert!(PAGE_HEADER_SIZE == 24);

impl<K, V> From<ByteArray> for Page<K, V>
where
    K: IKey,
    V: IVal,
{
    fn from(value: ByteArray) -> Self {
        Self {
            raw: value,
            _marker: PhantomData,
        }
    }
}

impl<K, V> Page<K, V>
where
    K: IKey,
    V: IVal,
{
    pub fn raw(&self) -> ByteArray {
        self.raw
    }

    pub fn header(&self) -> &PageHeader {
        unsafe { &*self.raw.data().cast::<PageHeader>() }
    }

    pub fn header_mut(&mut self) -> &mut PageHeader {
        unsafe { &mut *self.raw.data().cast::<PageHeader>() }
    }

    fn offset(&self, pos: usize) -> &mut Slot {
        let s = self
            .raw
            .as_mut_slice::<u8>(PAGE_HEADER_SIZE + SLOT_LEN * pos, SLOT_LEN);
        Slot::from(s)
    }

    pub fn key_at(&self, pos: usize) -> K {
        let slot = self.offset(pos);
        debug_assert!(self.raw.len() > slot.key_len());
        let raw = self.raw.sub_array(slot.key_off(), slot.key_len());
        K::decode_from(raw)
    }

    #[allow(unused)]
    pub fn val_at(&self, pos: usize) -> V {
        let slot = self.offset(pos);
        debug_assert!(self.raw.len() >= slot.len());
        V::decode_from(self.raw.sub_array(slot.val_off(), slot.val_len()))
    }

    pub fn get(&self, pos: usize) -> Option<(K, V)> {
        if pos >= self.header().elems as usize {
            None
        } else {
            let slot = self.offset(pos);
            let k = K::decode_from(self.raw.sub_array(slot.key_off(), slot.key_len()));
            let v = V::decode_from(self.raw.sub_array(slot.val_off(), slot.val_len()));
            Some((k, v))
        }
    }

    fn search_by<F>(&self, key: &K, f: F) -> Result<usize, usize>
    where
        F: Fn(&K, &K) -> Ordering,
    {
        let h = self.header();
        let mut lo = 0;
        let mut hi = h.elems as usize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let k = self.key_at(mid);
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

    pub fn search(&self, key: &K) -> Result<usize, usize> {
        self.search_by(key, |x, y| x.cmp(y))
    }

    pub fn search_raw(&self, key: &K) -> Result<usize, usize> {
        self.search_by(key, |x, y| x.raw().cmp(y.raw()))
    }

    #[allow(dead_code)]
    pub fn show(&self, addr: u64) {
        let n = self.header().elems as usize;
        log::debug!("------------- show page -----------",);
        for i in 0..n {
            let (k, v) = self.get(i).unwrap();
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

impl<T> From<ByteArray> for LeafPage<T>
where
    T: IValCodec,
{
    fn from(value: ByteArray) -> Self {
        Self {
            raw: value,
            _marker: PhantomData,
        }
    }
}

impl From<ByteArray> for IntlPage {
    fn from(value: ByteArray) -> Self {
        Self { raw: value }
    }
}

impl<T> LeafPage<T>
where
    T: IValCodec,
{
    pub fn get<'a>(&self, pos: usize) -> Option<(Key<'a>, Value<T>)> {
        Page::<Key<'a>, Value<T>>::from(self.raw).get(pos)
    }
}

impl IntlPage {
    pub fn get<'a>(&self, pos: usize) -> Option<(&'a [u8], Index)> {
        Page::<&'a [u8], Index>::from(self.raw).get(pos)
    }
}

pub struct RangeIter<K, V>
where
    K: IKey,
    V: IVal,
{
    page: Page<K, V>,
    range: Range<usize>,
    index: usize,
}

impl<K, V> RangeIter<K, V>
where
    K: IKey,
    V: IVal,
{
    pub fn new(page: Page<K, V>, range: Range<usize>) -> Self {
        let index = range.start;
        Self { page, range, index }
    }
}

impl<K, V> Iterator for RangeIter<K, V>
where
    K: IKey,
    V: IVal,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.range.end {
            let (k, v) = self.page.get(self.index).unwrap();
            self.index += 1;
            Some((k, v))
        } else {
            None
        }
    }
}

impl<K, V> IPageIter for RangeIter<K, V>
where
    K: IKey,
    V: IVal,
{
    fn rewind(&mut self) {
        self.index = self.range.start;
    }
}

pub struct PageMergeIter<'a, K, V>
where
    K: IKey,
    V: IVal,
{
    iter: MergeIter<RangeIter<K, V>>,
    split: Option<&'a [u8]>,
}

impl<'a, K, V> PageMergeIter<'a, K, V>
where
    K: IKey,
    V: IVal,
{
    pub fn new(iter: MergeIter<RangeIter<K, V>>, split: Option<&'a [u8]>) -> Self {
        Self { iter, split }
    }
}

impl<K, V> Iterator for PageMergeIter<'_, K, V>
where
    K: IKey,
    V: IVal,
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

impl<K, V> IPageIter for PageMergeIter<'_, K, V>
where
    K: IKey,
    V: IVal,
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

    pub fn reset(&mut self) {
        self.index = 0;
        self.data.clear();
    }

    pub fn sort(&mut self) {
        self.data.sort_unstable_by(|x, y| x.0.cmp(&y.0));
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
        let mut tmp = Vec::with_capacity(self.data.len() * 2 / 3);
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

pub struct IntlMergeIter<'a> {
    iter: PageMergeIter<'a, &'a [u8], Index>,
    last: Option<&'a [u8]>,
}

impl<'a> IntlMergeIter<'a> {
    pub fn new(iter: PageMergeIter<'a, &'a [u8], Index>, _txid: u64) -> Self {
        Self { iter, last: None }
    }
}

impl<'a> Iterator for IntlMergeIter<'a> {
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

impl IPageIter for IntlMergeIter<'_> {
    fn rewind(&mut self) {
        self.iter.rewind();
        self.last = None;
    }
}

#[cfg(test)]
mod test {
    use crate::index::builder::Delta;
    use crate::index::data::{Index, Key, Value};
    use crate::index::iter::MergeIterBuilder;
    use crate::index::page::{
        DeltaType, NodeType, Page, PageMergeIter, PAGE_HEADER_SIZE, SLOT_LEN,
    };
    use crate::utils::block::Block;
    use crate::utils::traits::{ICodec, IKey, IVal};

    use super::{IntlMergeIter, Meta, PageHeader, RangeIter, NULL_INDEX};

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

        let kv: Vec<(Key, Value<&[u8]>)> = pairs
            .iter()
            .map(|(k, v)| (Key::new(k.as_bytes(), 0, 0), (Value::Put(v.as_bytes()))))
            .collect();
        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).with_slice(&kv);
        let x = Block::aligned_alloc(delta.size(), align_of::<PageHeader>());
        let a = x.view(0, x.len());
        let mut page = Page::from(a);

        delta.build(&mut page);

        let h = page.header();
        assert_eq!(h.elems, pairs.len() as u32);
        assert!(h.meta.is_leaf());
        let k = Key::new("key1".as_bytes(), 0, 0);
        let v = Value::Put("val1".as_bytes());
        let hdr_size = PAGE_HEADER_SIZE;
        let slot_size = h.elems as usize * SLOT_LEN;
        let expect_len = (k.packed_size() + v.packed_size()) * pairs.len() + hdr_size + slot_size;
        assert_eq!(h.len as usize, expect_len);

        let page = Page::<Key, Value<&[u8]>>::from(page.raw);
        assert_eq!(page.offset(0).key_off(), PAGE_HEADER_SIZE + slot_size);

        let k = page.key_at(0);
        assert_eq!(k.raw, "key1".as_bytes());

        let (k, v) = page.get(0).unwrap();
        assert_eq!(k.raw, "key1".as_bytes());
        assert!(!v.is_del());
        assert_eq!(*v.as_ref(), "val1".as_bytes());

        match page.search(&Key::new("key2".as_bytes(), 0, 0)) {
            Ok(pos) => {
                assert_eq!(pos, 1);
            }
            Err(_) => unreachable!(),
        }

        match page.search(&Key::new("key0".as_bytes(), 0, 0)) {
            Ok(_) => unreachable!(),
            Err(pos) => assert_eq!(pos, 0),
        }

        match page.search(&Key::new("keyz".as_bytes(), 0, 0)) {
            Ok(_) => unreachable!(),
            Err(pos) => assert_eq!(pos, pairs.len()),
        }
    }

    fn must_match<I, K, V>(src: I, p: Page<K, V>)
    where
        I: Iterator<Item = (K, V)>,
        K: IKey,
        V: IVal,
    {
        for (k, v) in src {
            let pos = p.search(&k).expect("fail");
            let x = p.val_at(pos);
            assert_eq!(x.to_string(), v.to_string());
        }
    }

    #[test]
    fn intl_iter() {
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

        let mut delta1 = Delta::new(DeltaType::Data, NodeType::Intl).with_slice(&s1);
        let mut delta2 = Delta::new(DeltaType::Data, NodeType::Intl).with_slice(&s2);
        let mut delta3 = Delta::new(DeltaType::Data, NodeType::Intl).with_slice(&s3);

        let x1 = Block::aligned_alloc(delta1.size(), align_of::<PageHeader>());
        let x2 = Block::aligned_alloc(delta1.size(), align_of::<PageHeader>());
        let x3 = Block::aligned_alloc(delta1.size(), align_of::<PageHeader>());
        let b1 = x1.view(0, x1.len());
        let b2 = x2.view(0, x2.len());
        let b3 = x3.view(0, x3.len());

        let mut pg1 = Page::<&[u8], Index>::from(b1);
        let mut pg2 = Page::<&[u8], Index>::from(b2);
        let mut pg3 = Page::<&[u8], Index>::from(b3);

        delta1.build(&mut pg1);
        delta2.build(&mut pg2);
        delta3.build(&mut pg3);

        must_match(s1.into_iter(), pg1);
        must_match(s2.into_iter(), pg2);
        must_match(s3.into_iter(), pg3);

        let mut builder = MergeIterBuilder::new(3);

        builder.add(RangeIter::new(pg1, 0..pg1.header().elems as usize));
        builder.add(RangeIter::new(pg2, 0..pg2.header().elems as usize));
        builder.add(RangeIter::new(pg3, 0..pg3.header().elems as usize));

        let iter = IntlMergeIter::new(PageMergeIter::new(builder.build(), None), 0);
        let mut delta = Delta::new(DeltaType::Data, NodeType::Intl).from(iter);
        let x4 = Block::aligned_alloc(delta.size(), align_of::<PageHeader>());
        let b = x4.view(0, x4.len());
        let mut pg = Page::<&[u8], Index>::from(b);

        delta.build(&mut pg);

        let expact = [
            ([].as_slice(), Index::new(1, 0)),
            ("key1".as_bytes(), Index::new(2, 0)),
            ("key3".as_bytes(), Index::new(3, 0)),
            ("key5".as_bytes(), Index::new(4, 0)),
        ];

        must_match(expact.into_iter(), pg);
    }
}
