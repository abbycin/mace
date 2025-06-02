use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::{Deref, DerefMut, Range},
};

use crate::{
    cc::data::Ver,
    index::data::Key,
    utils::{
        NULL_PID,
        bytes::ByteArray,
        raw_ptr_to_ref, raw_ptr_to_ref_mut,
        traits::{ICodec, ICollector, IDataLoader, IInfer, IKey, IPageIter, IVal, IValCodec},
        unpack_id,
    },
};

use super::{
    builder::{HINTS_CNT, key_to_hints},
    data::{Index, Slot, Value},
    iter::MergeIter,
};

pub(crate) const NULL_INDEX: Index = Index::new(NULL_PID, 0);

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

#[cold]
fn get_remote<K, V, L>(l: &L, addr: u64) -> (K, V, Option<L::Out>)
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    let f = l.load_data(addr);
    let b = f.infer();
    let k = K::decode_from(b);
    let v = V::decode_from(b.skip(k.packed_size()));
    (k, v, Some(f))
}

fn get_key<K, L>(src: *mut u8, off: usize, size: usize, l: &L) -> K
where
    K: IKey,
    L: IDataLoader,
{
    let p = unsafe { std::slice::from_raw_parts_mut(src.add(off), size - off) };
    let s = Slot::decode_from(p.into());
    if s.is_inline() {
        let tmp = &mut p[Slot::LOCAL_LEN..];
        K::decode_from(tmp.into())
    } else {
        let f = l.load_data(s.addr());
        K::decode_from(f.infer())
    }
}

fn get_kv<F, T, K, V, L>(
    src: *mut u8,
    f: &F,
    slots: &[T],
    pos: usize,
    size: usize,
    l: &L,
) -> (K, V, Option<L::Out>)
where
    F: Fn(&[T], usize) -> usize,
    T: Sized,
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    let off = f(slots, pos);
    let p = unsafe { std::slice::from_raw_parts_mut(src.add(off), size - off) };
    let s = Slot::decode_from(p.into());
    if s.is_inline() {
        let ks = &mut p[Slot::LOCAL_LEN..];
        let k = K::decode_from(ks.into());
        let vs = &mut p[Slot::LOCAL_LEN + k.packed_size()..];
        let v = V::decode_from(vs.into());
        (k, v, None)
    } else {
        get_remote(l, s.addr())
    }
}

fn show_page<F, T, K, V, L>(
    src: *mut u8,
    f: F,
    slots: &[T],
    size: usize,
    elems: usize,
    l: &L,
    addr: u64,
) where
    F: Fn(&[T], usize) -> usize,
    T: Sized,
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    log::debug!("------------- show page -----------",);
    for i in 0..elems {
        let (k, v, _) = get_kv::<F, T, K, V, L>(src, &f, slots, i, size, l);
        log::debug!(
            "{} => {}\t{}",
            k.to_string(),
            v.to_string(),
            unpack_id(addr).1
        );
    }
    log::debug!("--------------- end --------------");
}

#[derive(Clone, Copy, Default)]
pub(crate) struct MainPageInner {
    pub(crate) meta: Meta,
    // link to delta page
    pub(crate) link: u64,
    pub(crate) elems: u32,
    // total size
    pub(crate) size: u32,
    pub(crate) prefix_len: usize,
}

impl Deref for MainPageInner {
    type Target = Meta;
    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}

impl DerefMut for MainPageInner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.meta
    }
}

impl From<ByteArray> for MainPageInner {
    fn from(value: ByteArray) -> Self {
        let p: *mut MainPageInner = value.data().cast();
        unsafe { p.read_unaligned() }
    }
}

impl Debug for MainPageInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "MainPageInner {{ {:?}, next: {} {:?}, elems: {}, size: {}, prefix_len {}",
            self.meta,
            self.link,
            unpack_id(self.link),
            self.elems,
            self.size,
            self.prefix_len
        ))
    }
}

pub(crate) type MainSlotType = u64;
pub(crate) const MAIN_SLOT_LEN: usize = size_of::<MainSlotType>();
pub(crate) const MAIN_HDR_LEN: usize = size_of::<MainPageInner>();
pub(crate) const MAX_PREFIX_LEN: usize = 256;

/// the layout of main page is:
/// ```text
/// +---------+-------+-------------+--------+-----------+
/// | header  | hints | index table | prefix | key-value |
/// +---------+-------+-------------+--------+-----------+
/// ```
/// if the real prefix length exceed MAX_PREFIX_LEN, the prefix_len will be set to 0 to disbale the
/// hints acceleration. additionally, for delta page the prefix_len is set to 0 too (it's elems is 1)
#[derive(Clone, Copy)]
pub(crate) struct MainPage<K, V> {
    inner: *mut MainPageInner,
    hints: &'static [u32],
    slots: &'static [MainSlotType],
    prefix: &'static [u8],
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Deref for MainPage<K, V> {
    type Target = MainPageInner;

    fn deref(&self) -> &Self::Target {
        raw_ptr_to_ref(self.inner)
    }
}

impl<K, V> DerefMut for MainPage<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        raw_ptr_to_ref_mut(self.inner)
    }
}

impl<K, V> MainPage<K, V>
where
    K: IKey,
    V: IVal,
{
    /// from a initialized address
    pub(crate) fn from(b: ByteArray) -> Self {
        let inner: *mut MainPageInner = b.data().cast();
        use std::slice::from_raw_parts;
        unsafe {
            let hints = from_raw_parts(inner.add(1).cast::<u32>(), HINTS_CNT);
            let slots = from_raw_parts(
                hints.as_ptr().add(HINTS_CNT).cast::<u64>(),
                (*inner).elems as usize,
            );
            let prefix = if (*inner).prefix_len == 0 {
                [].as_slice()
            } else {
                from_raw_parts(
                    slots.as_ptr().add((*inner).elems as usize).cast::<u8>(),
                    (*inner).prefix_len,
                )
            };
            Self {
                inner,
                hints,
                slots,
                prefix,
                _marker: PhantomData,
            }
        }
    }

    /// initialize a page
    pub(crate) fn new(b: ByteArray, hdr: MainPageInner) -> Self {
        b.write(0, hdr);
        Self::from(b)
    }

    pub(crate) fn inner_mut<'a>(&mut self) -> &'a mut MainPageInner {
        raw_ptr_to_ref_mut(self.inner)
    }

    fn offset(slots: &[u64], pos: usize) -> usize {
        let (off, _) = unpack_id(slots[pos]);
        off as usize
    }

    pub(crate) fn cast<K1, V1>(&self) -> MainPage<K1, V1>
    where
        K1: IKey,
        V1: IVal,
    {
        MainPage::<K1, V1>::from(ByteArray::new(self.inner.cast(), self.size as usize))
    }

    pub(crate) fn key_at<L: IDataLoader>(&self, l: &L, pos: usize) -> K {
        get_key(
            self.inner.cast(),
            Self::offset(self.slots, pos),
            self.size as usize,
            l,
        )
    }

    #[allow(unused)]
    pub(crate) fn val_at<L: IDataLoader>(&self, l: &L, pos: usize) -> V {
        let (_, v, _) = self.get_unchecked(l, pos);
        v
    }

    pub(crate) fn get_unchecked<L>(&self, l: &L, pos: usize) -> (K, V, Option<L::Out>)
    where
        L: IDataLoader,
    {
        get_kv(
            self.inner.cast(),
            &Self::offset,
            self.slots,
            pos,
            self.size as usize,
            l,
        )
    }

    pub(crate) fn get<L>(&self, l: &L, pos: usize) -> Option<(K, V, Option<L::Out>)>
    where
        L: IDataLoader,
    {
        if pos < self.elems as usize {
            Some(self.get_unchecked(l, pos))
        } else {
            None
        }
    }

    pub(crate) fn search_hints(&self, k: &[u8]) -> (usize, usize) {
        let elems = self.elems as usize;
        let step = elems / (HINTS_CNT + 1);
        let mut l = 0;
        let cnt = HINTS_CNT.min(elems);
        let hint = key_to_hints(k.raw());

        while l < cnt {
            if self.hints[l] >= hint {
                break;
            }
            l += 1;
        }

        let mut r = l;
        while r < cnt {
            if self.hints[r] != hint {
                break;
            }
            r += 1;
        }

        (l * step, if r < cnt { (r + 1) * step } else { elems })
    }

    fn compare_prefix(lhs: &[u8], rhs: &[u8]) -> i32 {
        let len = lhs.len().min(rhs.len());
        let o = lhs[..len].cmp(&rhs[..len]) as i32;
        let diff = lhs.len() as i32 - rhs.len() as i32;
        if o != 0 { o } else { diff }
    }

    fn shrink_range(&self, raw: &[u8]) -> Result<(usize, usize), usize> {
        let x = if self.prefix_len != 0 {
            let prefix = &raw[..self.prefix_len.min(raw.len())];
            let o = Self::compare_prefix(prefix, self.prefix);
            if o < 0 {
                return Err(0);
            } else if o > 0 {
                return Err(self.elems as usize);
            }
            self.search_hints(&raw[self.prefix_len..])
        } else {
            self.search_hints(raw)
        };
        Ok(x)
    }

    pub(crate) fn search_by<L, F>(&self, l: &L, key: &K, f: F) -> Result<usize, usize>
    where
        L: IDataLoader,
        F: Fn(&K, &K) -> Ordering,
    {
        let (mut lo, mut hi) = self.shrink_range(key.raw())?;
        // here we can shrink search range by using hints and then do binary search using key without
        // prefix, but this will add more branch misses
        while lo < hi {
            let mid = lo + ((hi - lo) >> 1);
            let k = self.key_at(l, mid);
            match f(&k, key) {
                Ordering::Equal => return Ok(mid),
                Ordering::Greater => hi = mid,
                Ordering::Less => lo = mid + 1,
            }
        }
        Err(lo)
    }

    #[inline]
    pub(crate) fn search<L>(&self, l: &L, k: &K) -> Result<usize, usize>
    where
        L: IDataLoader,
    {
        self.search_by(l, k, |x, y| x.cmp(y))
    }

    #[inline]
    pub(crate) fn search_raw<L>(&self, l: &L, k: &K) -> Result<usize, usize>
    where
        L: IDataLoader,
    {
        self.search_by(l, k, |x, y| x.raw().cmp(y.raw()))
    }

    #[allow(unused)]
    pub(crate) fn show<L>(&self, l: &L, addr: u64)
    where
        L: IDataLoader,
    {
        show_page::<_, _, K, V, L>(
            self.inner.cast(),
            &Self::offset,
            self.slots,
            self.size as usize,
            self.elems as usize,
            l,
            addr,
        );
    }
}

pub(crate) struct VerPageInner {
    pub(crate) link: u64,
    pub(crate) elems: u32,
    pub(crate) size: u32,
}

impl<T> Deref for VerPage<T>
where
    T: IValCodec,
{
    type Target = VerPageInner;

    fn deref(&self) -> &Self::Target {
        raw_ptr_to_ref(self.inner)
    }
}

impl<T> DerefMut for VerPage<T>
where
    T: IValCodec,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        raw_ptr_to_ref_mut(self.inner)
    }
}

pub(crate) type VerSlotType = u32;
pub(crate) const VER_HDR_LEN: usize = size_of::<VerPageInner>();
pub(crate) const VER_SLOT_LEN: usize = size_of::<VerSlotType>();

/// the version page layout is:
/// ```text
/// +--------+--------------+---------------+
/// | header | index table  | version-value |
/// +--------+--------------+---------------+
/// ```
pub(crate) struct VerPage<T: IValCodec> {
    inner: *mut VerPageInner,
    slots: &'static [VerSlotType],
    _marker: PhantomData<T>,
}

impl<T> VerPage<T>
where
    T: IValCodec,
{
    /// from a initialized address
    pub(crate) fn from(b: ByteArray) -> Self {
        let inner: *mut VerPageInner = b.data().cast();
        let slots = unsafe {
            let elems = (*inner).elems as usize;
            std::slice::from_raw_parts(inner.add(1).cast::<u32>(), elems)
        };
        Self {
            inner,
            slots,
            _marker: PhantomData,
        }
    }

    /// initialize a page
    pub(crate) fn new(b: ByteArray, hdr: VerPageInner) -> Self {
        b.write(0, hdr);
        Self::from(b)
    }

    fn offset(slots: &[u32], pos: usize) -> usize {
        slots[pos] as usize
    }

    fn key_at<L>(&self, l: &L, pos: usize) -> Ver
    where
        L: IDataLoader,
    {
        get_key(
            self.inner.cast(),
            Self::offset(self.slots, pos),
            self.size as usize,
            l,
        )
    }

    pub(crate) fn get_unchecked<L>(&self, l: &L, pos: usize) -> (Ver, Value<T>, Option<L::Out>)
    where
        L: IDataLoader,
    {
        get_kv(
            self.inner.cast(),
            &Self::offset,
            self.slots,
            pos,
            self.size as usize,
            l,
        )
    }

    pub(crate) fn lower_bound<L>(&self, l: &L, k: &Ver) -> Result<usize, usize>
    where
        L: IDataLoader,
    {
        let cnt = self.elems as usize;
        let mut lo = 0;
        let mut hi = cnt;

        while lo < hi {
            let mid = lo + ((hi - lo) >> 1);
            match self.key_at(l, mid).cmp(k) {
                Ordering::Less => lo = mid + 1,
                _ => hi = mid,
            }
        }

        if lo == cnt { Err(lo) } else { Ok(lo) }
    }

    pub(crate) fn show<L>(&self, l: &L, addr: u64)
    where
        L: IDataLoader,
    {
        show_page::<_, _, Ver, Value<T>, L>(
            self.inner.cast(),
            &Self::offset,
            self.slots,
            self.size as usize,
            self.elems as usize,
            l,
            addr,
        );
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

pub struct RangeIterAdaptor<K, V, L>
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    pub iter: RangeIter<K, V, L>,
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
    type Item = (K, V, Option<L::Out>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.range.end {
            let (k, v, o) = self.page.get_unchecked(&self.l, self.index);
            self.index += 1;
            Some((k, v, o))
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

    fn first(&self) -> Option<Self::Item> {
        self.page.get(&self.l, 0)
    }
}

impl<K, V, L> Iterator for RangeIterAdaptor<K, V, L>
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v, _)) = self.iter.next() {
            Some((k, v))
        } else {
            None
        }
    }
}

impl<K, V, L> IPageIter for RangeIterAdaptor<K, V, L>
where
    K: IKey,
    V: IVal,
    L: IDataLoader,
{
    fn rewind(&mut self) {
        self.iter.rewind();
    }

    fn first(&self) -> Option<Self::Item> {
        self.iter.first().map(|(k, v, _)| (k, v))
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
    type Item = (K, V, Option<L::Out>);

    fn next(&mut self) -> Option<Self::Item> {
        let (k, v, o) = self.iter.next()?;

        if let Some(split) = self.split {
            if k.raw() >= split {
                return None;
            }
        }
        Some((k, v, o))
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

    fn first(&self) -> Option<Self::Item> {
        self.iter.first()
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

    pub fn first(&self) -> Option<(Key, Value<T>)> {
        self.data.first().copied()
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

pub struct IntlMergeIter<'a, L, C, O>
where
    L: IDataLoader<Out = O>,
    C: ICollector<Input = O>,
{
    iter: PageMergeIter<'a, &'a [u8], Index, L>,
    last: Option<&'a [u8]>,
    gc: &'a mut C,
}

impl<'a, L, C, O> IntlMergeIter<'a, L, C, O>
where
    L: IDataLoader<Out = O>,
    C: ICollector<Input = O>,
{
    pub fn new(iter: PageMergeIter<'a, &'a [u8], Index, L>, gc: &'a mut C, _txid: u64) -> Self {
        Self {
            iter,
            last: None,
            gc,
        }
    }
}

impl<'a, L, C, O> Iterator for IntlMergeIter<'a, L, C, O>
where
    L: IDataLoader<Out = O>,
    C: ICollector<Input = O>,
{
    type Item = (&'a [u8], Index);

    fn next(&mut self) -> Option<Self::Item> {
        for (k, i, o) in &mut self.iter {
            if let Some(o) = o {
                self.gc.collect(o);
            }
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

impl<L, C, O> IPageIter for IntlMergeIter<'_, L, C, O>
where
    L: IDataLoader<Out = O>,
    C: ICollector<Input = O>,
{
    fn rewind(&mut self) {
        self.iter.rewind();
        self.last = None;
    }

    fn first(&self) -> Option<Self::Item> {
        self.iter.first().map(|(k, v, _)| (k, v))
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::Relaxed;

    use crate::OpCode;
    use crate::index::IAlloc;
    use crate::index::builder::{Delta, HINTS_LEN};
    use crate::index::data::{Index, Key, Slot, Value};
    use crate::index::iter::MergeIterBuilder;
    use crate::index::page::{
        DeltaType, MAIN_HDR_LEN, MAIN_SLOT_LEN, MainPage, MainPageInner, NodeType, PageMergeIter,
    };
    use crate::map::data::{FrameOwner, FrameRef};
    use crate::utils::block::Block;
    use crate::utils::traits::{ICodec, ICollector, IDataLoader, IKey, IVal};
    use crate::utils::{AMutRef, pack_id};

    use super::{IntlMergeIter, Meta, NULL_INDEX, RangeIter};

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
        let x = Block::aligned_alloc(delta.size(), align_of::<MainPageInner>());
        let a = x.view(0, x.len());

        let h = delta.build(a, &mut da);

        assert_eq!(h.elems, pairs.len() as u32);
        assert!(h.meta.is_leaf());
        let k = Key::new("key1".as_bytes(), 0, 0);
        let v = Value::Put("val1".as_bytes());
        let hdr_size = MAIN_HDR_LEN;
        let index_table_size = h.elems as usize * MAIN_SLOT_LEN;
        let expect_len = (k.packed_size() + v.packed_size() + Slot::LOCAL_LEN) * pairs.len()
            + hdr_size + "key".len() // prefix
            + HINTS_LEN
            + index_table_size;
        assert_eq!(h.size as usize, expect_len);

        let page = MainPage::<Key, Value<&[u8]>>::from(a);

        let k = page.key_at(&loader, 0);
        assert_eq!(k.raw, "key1".as_bytes());

        let (k, v, _) = page.get_unchecked(&loader, 0);
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
        let x = Block::aligned_alloc(delta.size(), align_of::<MainPageInner>());
        let a = x.view(0, x.len());

        delta.build(a, &mut da);

        let page = MainPage::<Key, Value<&[u8]>>::from(a);

        let (k, v, _) = page.get_unchecked(&loader, 0);
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

        let x1 = Block::aligned_alloc(delta1.size(), align_of::<MainPageInner>());
        let x2 = Block::aligned_alloc(delta1.size(), align_of::<MainPageInner>());
        let x3 = Block::aligned_alloc(delta1.size(), align_of::<MainPageInner>());
        let b1 = x1.view(0, x1.len());
        let b2 = x2.view(0, x2.len());
        let b3 = x3.view(0, x3.len());

        delta1.build(b1, &mut da);
        delta2.build(b2, &mut da);
        delta3.build(b3, &mut da);

        let pg1 = MainPage::<&[u8], Index>::from(b1);
        let pg2 = MainPage::<&[u8], Index>::from(b2);
        let pg3 = MainPage::<&[u8], Index>::from(b3);

        must_match(s1.into_iter(), &loader, pg1);
        must_match(s2.into_iter(), &loader, pg2);
        must_match(s3.into_iter(), &loader, pg3);

        let mut builder = MergeIterBuilder::new(3);

        builder.add(RangeIter::new(pg1, loader.clone(), 0..pg1.elems as usize));
        builder.add(RangeIter::new(pg2, loader.clone(), 0..pg2.elems as usize));
        builder.add(RangeIter::new(pg3, loader.clone(), 0..pg3.elems as usize));

        let mut coll = Coll;
        let iter = IntlMergeIter::new(PageMergeIter::new(builder.build(), None), &mut coll, 0);
        let mut delta = Delta::new(DeltaType::Data, NodeType::Intl).from(iter, da.page_size());
        let x4 = Block::aligned_alloc(delta.size(), align_of::<MainPageInner>());
        let b = x4.view(0, x4.len());

        delta.build(b, &mut da);

        let pg = MainPage::<&[u8], Index>::from(b);

        let expact = [
            ([].as_slice(), Index::new(1, 0)),
            ("key1".as_bytes(), Index::new(2, 0)),
            ("key3".as_bytes(), Index::new(3, 0)),
            ("key5".as_bytes(), Index::new(4, 0)),
        ];

        must_match(expact.into_iter(), &loader, pg);
    }

    struct Coll;

    impl ICollector for Coll {
        type Input = FrameOwner;

        fn collect(&mut self, _x: Self::Input) {}
    }
}
