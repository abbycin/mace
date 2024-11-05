use std::{
    cmp,
    fmt::Debug,
    hash::Hasher,
    marker::PhantomData,
    ops::{Deref, DerefMut, Range},
};

use crate::{
    slice_to_number, static_assert,
    tree::data::Key,
    utils::{byte_array::ByteArray, decode_u64, encode_u64, NAN_PID, ROOT_PID},
};

use super::{
    data::{Index, Value},
    iter::{ItemIter, MergeIter, SliceIter},
    traits::{ICodec, IKey, IPageIter, IVal},
};

pub(crate) const ROOT_INDEX: Index = Index::new(ROOT_PID, 0);
pub(crate) const NULL_INDEX: Index = Index::new(NAN_PID, 0);

const PAGE_KV_SIZE_BYTES: u32 = size_of::<u32>() as u32;
const SLOT_ELEM_LEN: usize = size_of::<u64>();

/// for `intl node` the page is consist of header + slot + key + PID (page table index, ie, logical link)
/// for `leaf node` the page is consist of header + slot + key + value
/// example layout:
/// ```text
/// +----------------------------+
/// |          header            |
/// +----------------------------+
/// | offset | offset | ... ...  | slot (key_off << 32 | val_off)
/// +----------------------------+
/// |key len | key data | val len|
/// |val data| ...               |
/// +----------------------------+
/// ```
#[derive(Clone, Copy)]
pub struct Page<K, V> {
    raw: ByteArray,
    _marker: PhantomData<(K, V)>,
}

pub struct LeafPage {
    raw: ByteArray,
}

pub struct IntlPage {
    raw: ByteArray,
}

pub struct PageBuilder {
    payload: ByteArray,
    slots: ByteArray,
    payload_index: isize,
    slot_index: isize,
    offset: u32,
}

impl PageBuilder {
    fn from(b: &ByteArray, elems: usize) -> Self {
        let slot_size = elems * SLOT_ELEM_LEN + PAGE_HEADER_SIZE;
        Self {
            slots: b.sub_array(PAGE_HEADER_SIZE..slot_size),
            payload: b.sub_array(slot_size..b.len()),
            payload_index: 0,
            slot_index: 0,
            offset: slot_size as u32,
        }
    }

    fn add<K, V>(&mut self, k: K, v: V)
    where
        K: ICodec,
        V: ICodec,
    {
        let (ksz, vsz) = (k.packed_size(), v.packed_size());
        let total_sz = ksz + vsz;

        let (kdst, vdst) = self
            .payload
            .to_mut_slice::<u8>(self.payload_index, total_sz)
            .split_at_mut(ksz);

        k.encode_to(kdst);
        v.encode_to(vdst);

        let sdst = self.slots.to_mut_slice(self.slot_index, SLOT_ELEM_LEN);
        sdst.copy_from_slice(&encode_u64(self.offset, self.offset + ksz as u32).to_le_bytes());

        self.slot_index += SLOT_ELEM_LEN as isize;
        self.payload_index += total_sz as isize;
        self.offset += total_sz as u32;
    }
}

/// consists of:
/// - epoch , highest 48 bits
/// - delta chain length, middle 8 bits
/// - node type and delta type, lowest 8 bits
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Meta(u64);

impl Meta {
    /// NOTE: depth is not set here
    pub fn new(dt: DeltaType, nt: NodeType) -> Self {
        let this = Self((dt as u16 | nt as u16) as u64);
        this
    }

    pub fn epoch(&self) -> u64 {
        self.0 >> 16
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.0 = epoch << 16 | self.0 & 0xffff;
    }

    pub fn depth(&self) -> u8 {
        ((self.0 >> 8) & 0xff) as u8
    }

    pub fn is_base(&self) -> bool {
        self.depth() == 0
    }

    pub fn set_depth(&mut self, depth: u8) {
        self.0 = (self.0 & !(0xff << 8)) | (depth as u64) << 8;
    }

    pub fn is_leaf(&self) -> bool {
        match self.node_type() {
            NodeType::Intl => false,
            NodeType::Leaf => true,
        }
    }

    pub fn is_intl(&self) -> bool {
        return !self.is_leaf();
    }

    pub fn is_data(&self) -> bool {
        match self.delta_type() {
            DeltaType::Data => true,
            DeltaType::Split => false,
        }
    }

    pub fn is_split(&self) -> bool {
        return !self.is_data();
    }

    pub fn node_type(&self) -> NodeType {
        (self.0 as u8 & NODE_TYPE_MASK).into()
    }

    pub fn delta_type(&self) -> DeltaType {
        (self.0 as u8 & DELTA_TYPE_MASK).into()
    }

    fn to_string(&self) -> String {
        format!(
            "Meta {{ epoch: {}, depth: {}, DeltaType: {:?}, NodeType: {:?} }}",
            self.epoch(),
            self.depth(),
            self.delta_type(),
            self.node_type()
        )
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

pub struct Delta<T> {
    kind: DeltaType,
    class: NodeType,
    elems: u16,
    size: u32,
    iter: Option<T>,
}

impl<T, K, V> Delta<T>
where
    T: IPageIter<Item = (K, V)>,
    K: IKey,
    V: IVal,
{
    pub fn new(kind: DeltaType, class: NodeType) -> Self {
        Self {
            kind,
            class,
            elems: 0,
            size: 0,
            iter: None,
        }
    }

    // move self
    pub fn from_iter(mut self, mut iter: T) -> Self {
        for (k, v) in &mut iter {
            self.elems += 1;
            self.size += (k.packed_size() + v.packed_size()) as u32;
        }
        self.size += self.elems as u32 * SLOT_ELEM_LEN as u32;
        self.iter = Some(iter);
        self
    }

    pub fn size(&self) -> usize {
        size_of::<PageHeader>() + self.size as usize
    }

    fn build_header(&self, h: &mut PageHeader) {
        h.meta = Meta::new(self.kind, self.class);
        h.set_depth(1);
        h.link = 0;
        h.len = self.size() as u32;
        h.elems = self.elems as u32;
    }

    pub fn build(&mut self, page: &Page<K, V>) {
        self.build_header(page.header());

        if let Some(iter) = self.iter.as_mut() {
            let mut buf = PageBuilder::from(&page.raw(), self.elems as usize);
            iter.rewind();
            for (k, v) in iter {
                buf.add(k, v);
            }
        }
        // log::debug!("head.meta {:?}", page.header().meta);
    }
}

impl<K, V> Delta<ItemIter<(K, V)>>
where
    K: IKey,
    V: IVal,
{
    pub fn from_item(self, item: (K, V)) -> Self {
        self.from_iter(ItemIter::new(item))
    }
}

impl<'a, K, V> Delta<SliceIter<'a, K, V>>
where
    K: IKey,
    V: IVal,
{
    pub fn from_slice(self, s: &'a [(K, V)]) -> Self {
        self.from_iter(SliceIter::new(s))
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PageHeader {
    /// visibility check on leaf node
    pub meta: Meta,
    /// physical link to delta or base page (file_id + offset)
    link: u64,
    /// NOTE: the page has same count of both key-value and key-index pair
    elems: u32,
    /// logical page size (including header)
    len: u32,
}

impl From<ByteArray> for PageHeader {
    fn from(value: ByteArray) -> Self {
        unsafe { *(value.data() as *const PageHeader) }
    }
}

impl Debug for PageHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "PageHeader {{ {:?}, next: {}, elems: {}, len {} }}",
            self.meta,
            decode_u64(self.link).1,
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

    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn payload_len(&self) -> usize {
        self.len as usize - PAGE_HEADER_SIZE
    }

    pub fn elems(&self) -> u32 {
        self.elems
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

const PAGE_HEADER_SIZE: usize = size_of::<PageHeader>();
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

    pub fn header(&self) -> &mut PageHeader {
        unsafe { &mut *(self.raw.data() as *mut PageHeader) }
    }

    fn offset(&self, pos: usize) -> (u32, u32) {
        let s = self.raw.to_slice::<u8>(
            (PAGE_HEADER_SIZE + SLOT_ELEM_LEN * pos) as isize,
            SLOT_ELEM_LEN,
        );
        decode_u64(slice_to_number!(s, u64))
    }

    pub fn key_at(&self, slot: usize) -> K {
        let (off, _) = self.offset(slot);
        let raw = self.raw.sub_array(off as usize..self.raw.len());
        K::decode_from(raw)
    }

    pub fn val_at(&self, slot: usize) -> V {
        let (_, off) = self.offset(slot);
        V::decode_from(self.raw.sub_array(off as usize..self.raw.len()))
    }

    pub fn get<'a>(&self, slot: usize) -> Option<(K, V)> {
        if slot >= self.header().elems as usize {
            None
        } else {
            let (koff, voff) = self.offset(slot);
            let k = K::decode_from(self.raw.sub_array(koff as usize..self.raw.len()));
            let v = V::decode_from(self.raw.sub_array(voff as usize..self.raw.len()));
            Some((k, v))
        }
    }

    pub fn lower_bound(&self, key: &[u8]) -> Result<usize, usize> {
        let h = self.header();
        let mut lo = 0;
        let mut hi = h.elems as usize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let l = self.key_at(mid);
            let raw = l.raw();
            match raw.cmp(key) {
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

    #[allow(dead_code)]
    fn to_str(x: &[u8]) -> &str {
        unsafe { std::str::from_utf8_unchecked(x) }
    }

    #[allow(dead_code)]
    pub fn show(&self, addr: u64) {
        let n = self.header().elems as usize;
        log::debug!("------------- show page -----------",);
        for i in 0..n {
            let (k, v) = self.get(i).unwrap();
            log::debug!(
                "klen {} {} => {}\t{}",
                k.raw().len(),
                Self::to_str(k.raw()),
                v.to_string(),
                decode_u64(addr).1
            );
        }
        log::debug!("--------------- end --------------");
    }
}

impl From<ByteArray> for LeafPage {
    fn from(value: ByteArray) -> Self {
        Self { raw: value }
    }
}

impl From<ByteArray> for IntlPage {
    fn from(value: ByteArray) -> Self {
        Self { raw: value }
    }
}

impl LeafPage {
    pub fn get<'a>(&self, pos: usize) -> Option<(Key<'a>, Value<'a>)> {
        Page::<Key<'a>, Value<'a>>::from(self.raw).get(pos)
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

impl<'a, K, V> Iterator for PageMergeIter<'a, K, V>
where
    K: IKey,
    V: IVal,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let Some((k, v)) = self.iter.next() else {
            return None;
        };

        if let Some(split) = self.split {
            if k.raw() >= split {
                return None;
            }
        }
        Some((k, v))
    }
}

impl<'a, K, V> IPageIter for PageMergeIter<'a, K, V>
where
    K: IKey,
    V: IVal,
{
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

pub struct LeafMergeIter<'a> {
    iter: PageMergeIter<'a, Key<'a>, Value<'a>>,
    last: Option<&'a [u8]>,
}

impl<'a> LeafMergeIter<'a> {
    pub fn new(iter: PageMergeIter<'a, Key<'a>, Value<'a>>, _lsn: u64) -> Self {
        Self { iter, last: None }
    }
}

impl<'a> Iterator for LeafMergeIter<'a> {
    type Item = (Key<'a>, Value<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        for (k, v) in &mut self.iter {
            if let Some(last) = self.last {
                if k.raw == last {
                    continue;
                }

                self.last = Some(k.raw);
                match v {
                    Value::Put(_) => return Some((k, v)),
                    Value::Del => continue,
                }
            }

            self.last = Some(k.raw);
            match v {
                Value::Del => continue,
                _ => return Some((k, v)),
            }
        }
        None
    }
}

impl<'a> IPageIter for LeafMergeIter<'a> {
    fn rewind(&mut self) {
        self.iter.rewind();
        self.last = None;
    }
}

pub struct IntlMergeIter<'a> {
    iter: PageMergeIter<'a, &'a [u8], Index>,
    last: Option<&'a [u8]>,
}

impl<'a> IntlMergeIter<'a> {
    pub fn new(iter: PageMergeIter<'a, &'a [u8], Index>, _lsn: u64) -> Self {
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

impl<'a> IPageIter for IntlMergeIter<'a> {
    fn rewind(&mut self) {
        self.iter.rewind();
        self.last = None;
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::tree::data::{Index, Key, Value};
    use crate::tree::iter::MergeIterBuilder;
    use crate::tree::page::{
        Delta, DeltaType, LeafMergeIter, NodeType, Page, PageMergeIter, PAGE_HEADER_SIZE,
        SLOT_ELEM_LEN,
    };
    use crate::tree::traits::{ICodec, IKey, IVal};
    use crate::utils::byte_array::ByteArray;

    use super::{IntlMergeIter, Meta, RangeIter, NULL_INDEX};

    #[test]
    fn test_meta() {
        let mut m = Meta::new(DeltaType::Data, NodeType::Leaf);

        assert!(m.is_data());
        assert!(m.is_leaf());

        assert!(m.is_base());

        m.set_depth(114);
        m.set_depth(1);
        assert_eq!(m.depth(), 1);
        assert!(!m.is_base());

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

        let kv: Vec<(Key, Value)> = pairs
            .iter()
            .map(|(k, v)| (Key::new(k.as_bytes(), 0), (Value::Put(v.as_bytes()))))
            .collect();
        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).from_slice(&kv);
        let a = ByteArray::alloc(delta.size());
        let page = Page::from(a);

        delta.build(&page);

        let h = page.header();
        assert_eq!(h.elems, pairs.len() as u32);
        assert!(h.meta.is_leaf());
        let k = Key::new("key1".as_bytes(), 0);
        let v = Value::Put("val1".as_bytes());
        let hdr_size = PAGE_HEADER_SIZE;
        let slot_size = h.elems as usize * SLOT_ELEM_LEN as usize;
        let expect_len = (k.packed_size() + v.packed_size()) * pairs.len() + hdr_size + slot_size;
        assert_eq!(h.len() as usize, expect_len);

        let page = Page::<Key, Value>::from(page.raw);
        assert_eq!(page.offset(0).0 as usize, PAGE_HEADER_SIZE + slot_size);

        let k = page.key_at(0);
        assert_eq!(k.raw, "key1".as_bytes());

        let (k, v) = page.get(0).unwrap();
        assert_eq!(k.raw, "key1".as_bytes());
        assert_eq!(v.put().unwrap(), "val1".as_bytes());

        match page.lower_bound("key2".as_bytes()) {
            Ok(pos) => {
                assert_eq!(pos, 1);
            }
            Err(_) => unreachable!(),
        }

        match page.lower_bound("key0".as_bytes()) {
            Ok(_) => unreachable!(),
            Err(pos) => assert_eq!(pos, 0),
        }

        match page.lower_bound("keyz".as_bytes()) {
            Ok(_) => unreachable!(),
            Err(pos) => assert_eq!(pos, pairs.len()),
        }

        ByteArray::free(a);
    }

    fn must_match<I, K, V>(src: I, p: Page<K, V>)
    where
        I: Iterator<Item = (K, V)>,
        K: IKey,
        V: IVal,
    {
        for (k, v) in src {
            let pos = p.lower_bound(k.raw()).expect("fail");
            let x = p.val_at(pos);
            assert_eq!(v, x);
        }
    }

    #[test]
    fn test_leaf_iter() {
        let s1 = [("key1", "val1"), ("key2", "val2"), ("key3", "val3")];
        let s2 = [("simple", "young"), ("naive!", "elder"), ("key4", "val4")];

        let mut v1: Vec<(Key, Value)> = s1
            .iter()
            .map(|(x, y)| (Key::new(x.as_bytes(), 114), Value::Put(y.as_bytes())))
            .collect();
        v1.sort_by(|x, y| x.0.cmp(&y.0));
        let mut delta1 = Delta::new(DeltaType::Data, NodeType::Leaf).from_slice(&v1);
        let mut v2: Vec<(Key, Value)> = s2
            .iter()
            .map(|(x, y)| (Key::new(x.as_bytes(), 115), Value::Put(y.as_bytes())))
            .collect();
        v2.sort_by(|x, y| x.0.cmp(&y.0));
        let mut delta2 = Delta::new(DeltaType::Data, NodeType::Leaf).from_slice(&v2);

        let mut v3 = vec![
            (
                Key::new("key3".as_bytes(), 116),
                Value::Put("new_val".as_bytes()),
            ),
            (Key::new("naive!".as_bytes(), 117), Value::Del),
        ];
        v3.sort_by(|x, y| x.0.cmp(&y.0));
        let mut delta3 = Delta::new(DeltaType::Data, NodeType::Leaf).from_slice(&v3);
        let mut map = HashMap::new();
        let tmp = [v1.clone(), v2.clone(), v3.clone()];

        for v in &tmp {
            v.iter()
                .map(|(k, v)| match v {
                    Value::Del => map.remove(k.raw()),
                    x => map.insert(k.raw(), x),
                })
                .count();
        }

        let b1 = ByteArray::alloc(delta1.size());
        let b2 = ByteArray::alloc(delta2.size());
        let b3 = ByteArray::alloc(delta3.size());

        let pgv1 = Page::<Key, Value>::from(b1);
        let pgv2 = Page::<Key, Value>::from(b2);
        let pgv3 = Page::<Key, Value>::from(b3);

        delta1.build(&pgv1);
        delta2.build(&pgv2);
        delta3.build(&pgv3);

        let (h1, h2, h3) = (pgv1.header(), pgv2.header(), pgv3.header());

        assert_eq!(h1.elems, v1.len() as u32);
        assert_eq!(h2.elems, v2.len() as u32);
        assert_eq!(h3.elems, v3.len() as u32);

        must_match(v1.into_iter(), pgv1);
        must_match(v2.into_iter(), pgv2);
        must_match(v3.into_iter(), pgv3);

        let mut builder = MergeIterBuilder::new(3);

        builder.add(RangeIter::new(pgv2, 0..h2.elems() as usize));
        builder.add(RangeIter::new(pgv1, 0..h1.elems() as usize));
        builder.add(RangeIter::new(pgv3, 0..h3.elems() as usize));

        let iter = LeafMergeIter::new(
            PageMergeIter::new(builder.build(), Some("simple".as_bytes())),
            1,
        );
        map.remove("simple".as_bytes());

        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf).from_iter(iter);
        let b = ByteArray::alloc(delta.size());
        let pgv = Page::<Key, Value>::from(b);

        delta.build(&pgv);

        let h = pgv.header();

        assert_eq!(h.elems() as usize, map.len());

        for (k, v) in map {
            let slot = pgv.lower_bound(k.raw()).expect("fail");
            let (x, y) = pgv.get(slot).expect("fail");
            assert_eq!(k, x.raw());
            assert_eq!(v, &y);
        }

        ByteArray::free(b1);
        ByteArray::free(b2);
        ByteArray::free(b3);
        ByteArray::free(b);
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

        let mut delta1 = Delta::new(DeltaType::Data, NodeType::Intl).from_slice(&s1);
        let mut delta2 = Delta::new(DeltaType::Data, NodeType::Intl).from_slice(&s2);
        let mut delta3 = Delta::new(DeltaType::Data, NodeType::Intl).from_slice(&s3);

        let b1 = ByteArray::alloc(delta1.size());
        let b2 = ByteArray::alloc(delta2.size());
        let b3 = ByteArray::alloc(delta2.size());

        let pg1 = Page::<&[u8], Index>::from(b1);
        let pg2 = Page::<&[u8], Index>::from(b2);
        let pg3 = Page::<&[u8], Index>::from(b3);

        delta1.build(&pg1);
        delta2.build(&pg2);
        delta3.build(&pg3);

        must_match(s1.into_iter(), pg1);
        must_match(s2.into_iter(), pg2);
        must_match(s3.into_iter(), pg3);

        let mut builder = MergeIterBuilder::new(3);

        builder.add(RangeIter::new(pg1, 0..pg1.header().elems() as usize));
        builder.add(RangeIter::new(pg2, 0..pg2.header().elems() as usize));
        builder.add(RangeIter::new(pg3, 0..pg3.header().elems() as usize));

        let iter = IntlMergeIter::new(PageMergeIter::new(builder.build(), None), 0);
        let mut delta = Delta::new(DeltaType::Data, NodeType::Intl).from_iter(iter);
        let b = ByteArray::alloc(delta.size());
        let pg = Page::<&[u8], Index>::from(b);

        delta.build(&pg);

        let expact = [
            ([].as_slice(), Index::new(1, 0)),
            ("key1".as_bytes(), Index::new(2, 0)),
            ("key3".as_bytes(), Index::new(3, 0)),
            ("key5".as_bytes(), Index::new(4, 0)),
        ];

        must_match(expact.into_iter(), pg);
    }
}
