use crate::hot_true;
use std::{
    cell::UnsafeCell,
    cmp::Ordering,
    fmt::{Debug, Display},
    ops::Deref,
};

use crate::{
    must_exist, number_to_slice, slice_to_number,
    types::{
        refbox::BoxRef,
        traits::{ICodec, IDecode, IKey, IKeyCodec, ILoader, IVal},
    },
    utils::{ADDR_LEN, Handle, NULL_ADDR, NULL_PID, to_str, varint::Varint32},
};

#[derive(Default, PartialEq, Eq, Clone, Copy)]
pub struct IntlKey<'a> {
    pub raw: &'a [u8],
}

impl<'a> IntlKey<'a> {
    pub(crate) fn new(raw: &'a [u8]) -> Self {
        Self { raw }
    }

    #[inline(always)]
    pub(crate) fn encoded_raw(src: &[u8]) -> &[u8] {
        let (raw_len, n) = must_exist!(Varint32::decode(src), "invalid src: {:?}", src);
        &src[n..n + raw_len as usize]
    }
}

impl PartialOrd for IntlKey<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IntlKey<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.raw.cmp(other.raw)
    }
}

impl IKey for IntlKey<'_> {
    fn raw(&self) -> &[u8] {
        self.raw
    }

    fn txid(&self) -> u64 {
        unimplemented!("internal node has no txid");
    }

    fn to_string(&self) -> String {
        format!("raw {}", to_str(self.raw))
    }
}

impl ICodec for IntlKey<'_> {
    fn packed_size(&self) -> usize {
        Varint32::size(self.raw.len()) + self.raw.len()
    }

    fn encode_to(&self, to: &mut [u8]) {
        hot_true!(eq to.len(), self.packed_size());
        let (l, r) = to.split_at_mut(Varint32::size(to.len()));
        Varint32::encode(l, self.raw.len() as u32);
        r.copy_from_slice(self.raw);
    }
}

impl IDecode for IntlKey<'_> {
    fn decode_from(src: &[u8]) -> Self {
        let raw = Self::encoded_raw(src);
        Self {
            raw: unsafe { std::mem::transmute::<&[u8], &[u8]>(raw) },
        }
    }
}

impl IKeyCodec for IntlKey<'_> {
    fn remove_prefix(&self, prefix_len: usize) -> Self {
        hot_true!(self.raw.len() >= prefix_len);
        Self {
            raw: &self.raw[prefix_len..],
        }
    }
}

#[derive(Default, PartialEq, Eq, Clone, Copy, Debug)]
pub struct Key<'a> {
    pub raw: &'a [u8],
    pub ver: Ver,
}

impl<'a> Key<'a> {
    pub fn new(raw: &'a [u8], ver: Ver) -> Self {
        Self { raw, ver }
    }

    #[inline(always)]
    fn encoded_key(data: &[u8]) -> &[u8] {
        let (len, n) = must_exist!(Varint32::decode(data), "invalid data: {:?}", data);
        &data[n..n + len as usize]
    }

    #[inline(always)]
    pub(crate) fn encoded_key_parts(data: &[u8]) -> (&[u8], &[u8]) {
        Self::encoded_key(data).split_at(Ver::len())
    }

    #[inline(always)]
    pub(crate) fn encoded_parts(data: &[u8]) -> (Ver, &[u8]) {
        let (ver, raw) = Self::encoded_key_parts(data);
        (Ver::decode_from(ver), raw)
    }

    fn len(&self) -> usize {
        self.raw.len() + Ver::len()
    }

    pub fn ver(&self) -> &Ver {
        &self.ver
    }
}

impl Deref for Key<'_> {
    type Target = Ver;
    fn deref(&self) -> &Self::Target {
        &self.ver
    }
}

impl PartialOrd for Key<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key<'_> {
    /// NOTE: key is in ascending order, while txid is descending order, since txid is monotonically
    /// increasing, greater is newer
    fn cmp(&self, other: &Self) -> Ordering {
        match self.raw.cmp(other.raw) {
            // numbers are in descending order
            Ordering::Equal => self.ver.cmp(&other.ver),
            x => x,
        }
    }
}

impl IDecode for Key<'_> {
    fn decode_from(data: &[u8]) -> Self {
        let (ver, raw) = Self::encoded_parts(data);

        Self {
            raw: unsafe { std::mem::transmute::<&[u8], &[u8]>(raw) },
            ver,
        }
    }
}

impl ICodec for Key<'_> {
    fn packed_size(&self) -> usize {
        self.len() + Varint32::size(self.len())
    }

    fn encode_to(&self, to: &mut [u8]) {
        hot_true!(eq to.len(), self.packed_size());
        let (len, key) = to.split_at_mut(Varint32::size(to.len()));
        Varint32::encode(len, self.len() as u32);
        let (num, raw) = key.split_at_mut(Ver::len());
        self.ver.encode_to(num);
        raw.copy_from_slice(self.raw);
    }
}

impl IKeyCodec for Key<'_> {
    fn remove_prefix(&self, prefix_len: usize) -> Self {
        hot_true!(self.raw.len() >= prefix_len);
        Self {
            raw: &self.raw[prefix_len..],
            ver: self.ver,
        }
    }
}

impl IKey for Key<'_> {
    fn raw(&self) -> &[u8] {
        self.raw
    }

    fn txid(&self) -> u64 {
        self.txid
    }

    fn to_string(&self) -> String {
        format!(
            "<{} {}, {}-{}>",
            self.raw.len(),
            to_str(self.raw),
            self.ver.txid,
            self.ver.cmd
        )
    }
}

impl IKey for Ver {
    fn raw(&self) -> &[u8] {
        unsafe {
            let ptr = self as *const Self as *const u8;
            std::slice::from_raw_parts(ptr, Self::len())
        }
    }

    fn txid(&self) -> u64 {
        unimplemented!("Ver has no txid");
    }

    fn to_string(&self) -> String {
        format!("Ver {{ txid: {}, cmd: {} }}", self.txid, self.cmd)
    }
}

impl IKeyCodec for Ver {
    fn remove_prefix(&self, _prefix_len: usize) -> Self {
        *self
    }
}

impl ICodec for Ver {
    fn packed_size(&self) -> usize {
        Self::len()
    }

    fn encode_to(&self, to: &mut [u8]) {
        hot_true!(eq to.len(), Self::len());
        let (x, y) = to.split_at_mut(size_of::<u64>());
        number_to_slice!(self.txid, x);
        number_to_slice!(self.cmd, y);
    }
}

impl IDecode for Ver {
    fn decode_from(raw: &[u8]) -> Self {
        Self {
            txid: slice_to_number!(raw[..size_of::<u64>()], u64),
            cmd: slice_to_number!(
                raw[size_of::<u64>()..size_of::<u64>() + size_of::<u32>()],
                u32
            ),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct IntlSeg<'a> {
    prefix: &'a [u8],
    base: &'a [u8],
}

impl<'a> IntlSeg<'a> {
    pub(crate) fn new(prefix: &'a [u8], base: &'a [u8]) -> Self {
        Self { prefix, base }
    }

    /// compare in same node, they share same prefix
    pub(crate) fn raw_cmp(&self, other: &Self) -> Ordering {
        self.base.cmp(other.base)
    }

    fn len(&self) -> usize {
        self.prefix.len() + self.base.len()
    }
}

impl ICodec for IntlSeg<'_> {
    fn packed_size(&self) -> usize {
        self.len() + Varint32::size(self.len())
    }

    fn encode_to(&self, to: &mut [u8]) {
        let (len, data) = to.split_at_mut(Varint32::size(self.len()));
        let (p, b) = data.split_at_mut(self.prefix.len());
        Varint32::encode(len, self.len() as u32);
        p.copy_from_slice(self.prefix);
        b.copy_from_slice(self.base);
    }
}

impl IDecode for IntlSeg<'_> {
    fn decode_from(_raw: &[u8]) -> Self {
        unimplemented!()
    }
}

impl IKeyCodec for IntlSeg<'_> {
    fn remove_prefix(&self, prefix_len: usize) -> Self {
        hot_true!(self.len() >= prefix_len);
        if prefix_len >= self.prefix.len() {
            let rest = prefix_len - self.prefix.len();
            Self {
                prefix: &[],
                base: &self.base[rest..],
            }
        } else {
            Self {
                prefix: &self.prefix[prefix_len..],
                base: self.base,
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct LeafSeg<'a> {
    prefix: &'a [u8],
    base: &'a [u8],
    pub(crate) ver: Ver,
}

#[inline]
pub(crate) fn cmp_raw_with_prefixed_tail(
    lhs: &[u8],
    rhs_prefix_tail: &[u8],
    rhs_base: &[u8],
) -> Ordering {
    let n = lhs.len().min(rhs_prefix_tail.len());
    let o = lhs[..n].cmp(&rhs_prefix_tail[..n]);
    if o != Ordering::Equal {
        return o;
    }
    if lhs.len() < rhs_prefix_tail.len() {
        return Ordering::Less;
    }
    lhs[n..].cmp(rhs_base)
}

#[inline]
fn full_raw_cmp(l_prefix: &[u8], l_raw: &[u8], r_prefix: &[u8], r_raw: &[u8]) -> Ordering {
    let n = l_prefix.len().min(r_prefix.len());
    match l_prefix[..n].cmp(&r_prefix[..n]) {
        Ordering::Equal => match l_prefix.len().cmp(&r_prefix.len()) {
            Ordering::Equal => l_raw.cmp(r_raw),
            Ordering::Less => cmp_raw_with_prefixed_tail(l_raw, &r_prefix[n..], r_raw),
            Ordering::Greater => cmp_raw_with_prefixed_tail(r_raw, &l_prefix[n..], l_raw).reverse(),
        },
        x => x,
    }
}

impl<'a> LeafSeg<'a> {
    pub(crate) fn new(prefix: &'a [u8], base: &'a [u8], ver: Ver) -> Self {
        Self { prefix, base, ver }
    }

    pub(crate) fn cmp(&self, other: &Self) -> Ordering {
        match self.raw_cmp(other) {
            Ordering::Equal => self.ver.cmp(&other.ver),
            o => o,
        }
    }

    pub(crate) fn raw_cmp(&self, other: &Self) -> Ordering {
        // merge can feed items from different prefix domains, only then use full key compare
        if self.prefix == other.prefix {
            self.base.cmp(other.base)
        } else {
            full_raw_cmp(self.prefix, self.base, other.prefix, other.base)
        }
    }

    pub(crate) fn raw(&self) -> &'a [u8] {
        self.base
    }

    /// the complete user raw key (prefix + base) written into `buf`; `raw()`
    /// returns only the prefix-stripped tail, so operator dispatch and any
    /// user-key comparison must use this instead
    pub(crate) fn full_key<'s>(&self, buf: &'s mut Vec<u8>) -> &'s [u8] {
        buf.clear();
        buf.extend_from_slice(self.prefix);
        buf.extend_from_slice(self.base);
        &buf[..]
    }

    pub(crate) fn txid(&self) -> u64 {
        self.ver.txid
    }

    fn len(&self) -> usize {
        self.prefix.len() + self.base.len() + Ver::len()
    }
}

impl ICodec for LeafSeg<'_> {
    fn packed_size(&self) -> usize {
        self.len() + Varint32::size(self.len())
    }

    fn encode_to(&self, to: &mut [u8]) {
        let (len, data) = to.split_at_mut(Varint32::size(to.len()));
        Varint32::encode(len, self.len() as u32);
        let (ver, k) = data.split_at_mut(Ver::len());
        self.ver.encode_to(ver);
        let (p, b) = k.split_at_mut(self.prefix.len());
        p.copy_from_slice(self.prefix);
        b.copy_from_slice(self.base);
    }
}

impl IDecode for LeafSeg<'_> {
    fn decode_from(_raw: &[u8]) -> Self {
        unimplemented!()
    }
}

impl IKeyCodec for LeafSeg<'_> {
    fn remove_prefix(&self, prefix_len: usize) -> Self {
        hot_true!(self.len() - Ver::len() >= prefix_len);
        if prefix_len >= self.prefix.len() {
            let rest = prefix_len - self.prefix.len();
            Self {
                prefix: &[],
                base: &self.base[rest..],
                ver: self.ver,
            }
        } else {
            Self {
                prefix: &self.prefix[prefix_len..],
                base: self.base,
                ver: self.ver,
            }
        }
    }
}

/// 1. inline data
///```text
/// +-----+-----+----------+------+
/// | HDR | WID | DATA LEN | DATA |
/// +-----+-----+----------+------+
///```
/// 2. inline data with history region
///```text
/// +-----+-----+----------+-----------+------+-------+------+
/// | HDR | WID | DATA LEN | HIST ADDR | SLOT | COUNT | DATA |
/// +-----+-----+----------+-----------+------+-------+------+
///```
/// 3. remote data
///```text
/// +-----+-----+----------+-------------+
/// | HDR | WID | DATA LEN | REMOTE ADDR |
/// +-----+-----+----------+-------------+
///```
/// 4. remote data with history region
///```text
/// +-----+-----+----------+-----------+------+-------+-------------+
/// | HDR | WID | DATA LEN | HIST ADDR | SLOT | COUNT | REMOTE ADDR |
/// +-----+-----+----------+-----------+------+-------+-------------+
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistRef {
    pub page_addr: u64,
    pub slot: u16,
    pub count: u32,
}

impl HistRef {
    pub const fn new(page_addr: u64, slot: u16, count: u32) -> Self {
        Self {
            page_addr,
            slot,
            count,
        }
    }
}

#[derive(Clone, Copy)]
pub struct Val<'a> {
    data: &'a [u8],
}

impl<'a> Val<'a> {
    pub(crate) const DEL_BIT: u8 = 0b0000_0001;
    pub(crate) const MERGE_BIT: u8 = 0b0000_0010;
    pub(crate) const HIST_BIT: u8 = 0b1000_0000;
    pub(crate) const REMOTE_BIT: u8 = 0b0001_0000;
    const HIST_SLOT_LEN: usize = size_of::<u16>();
    const HIST_COUNT_LEN: usize = size_of::<u32>();
    const HIST_LEN: usize = ADDR_LEN + Self::HIST_SLOT_LEN + Self::HIST_COUNT_LEN;
    const DATA_LEN: usize = size_of::<u32>();
    const HDR_LEN: usize = 1;
    const GID_LEN: usize = 1;

    #[inline(always)]
    pub fn is_tombstone(&self) -> bool {
        #[cfg(feature = "extra_check")]
        if self.data[0] & Self::DEL_BIT != 0 {
            assert!(self.data[0] & Self::MERGE_BIT == 0);
        }
        self.data[0] & Self::DEL_BIT != 0
    }

    #[inline(always)]
    pub fn is_merge(&self) -> bool {
        #[cfg(feature = "extra_check")]
        if self.data[0] & Self::MERGE_BIT != 0 {
            assert!(self.data[0] & Self::DEL_BIT == 0);
        }
        self.data[0] & Self::MERGE_BIT != 0
    }

    #[inline(always)]
    pub(crate) fn kind(&self) -> u8 {
        let kind = self.data[0] & (Self::DEL_BIT | Self::MERGE_BIT);
        #[cfg(feature = "extra_check")]
        if kind != 0 {
            assert!(kind == Self::DEL_BIT || kind == Self::MERGE_BIT);
        }
        kind
    }

    pub fn has_hist(&self) -> bool {
        self.data[0] & Self::HIST_BIT != 0
    }

    #[inline(always)]
    fn is_inline(&self) -> bool {
        self.data[0] & Self::REMOTE_BIT == 0
    }

    #[inline(always)]
    fn data_offset(&self) -> usize {
        Self::HDR_LEN + Self::GID_LEN + Self::DATA_LEN + self.has_hist() as usize * Self::HIST_LEN
    }

    pub fn from_raw(data: &'a [u8]) -> Self {
        Self { data }
    }

    #[inline(always)]
    pub fn data_size(&self) -> usize {
        Self::read::<u32>(self.data, Self::HDR_LEN + Self::GID_LEN) as usize
    }

    fn get_record_impl<L: ILoader>(&self, l: &L, cache: bool) -> (Record, Option<BoxRef>) {
        let off = self.data_offset();
        let len = self.data_size();
        let (src, r) = if self.is_inline() {
            (&self.data[off..], None)
        } else {
            let addr = Self::read::<u64>(self.data, off);
            let r = l.load_blob(addr, cache);
            (r.view().as_remote().raw(), Some(r))
        };
        let mut record = Record::decode_from(&src[..len]);
        if self.is_merge() {
            record.set_merge_tag();
        }
        (record, r)
    }

    pub fn get_record<L: ILoader>(&self, l: &L) -> (Record, Option<BoxRef>) {
        self.get_record_impl(l, true)
    }

    pub fn get_record_uncached<L: ILoader>(&self, l: &L) -> (Record, Option<BoxRef>) {
        self.get_record_impl(l, false)
    }

    #[inline(always)]
    pub(crate) fn inline_data(&self) -> Option<&'a [u8]> {
        if !self.is_inline() {
            return None;
        }
        let off = self.data_offset();
        let len = self.data_size();
        Some(&self.data[off + Self::GID_LEN..off + len])
    }

    pub fn get_hist(&self) -> Option<HistRef> {
        if self.has_hist() {
            let off = Self::HDR_LEN + Self::GID_LEN + Self::DATA_LEN;
            Some(HistRef::new(
                Self::read::<u64>(self.data, off),
                Self::read::<u16>(self.data, off + ADDR_LEN),
                Self::read::<u32>(self.data, off + ADDR_LEN + Self::HIST_SLOT_LEN),
            ))
        } else {
            None
        }
    }

    pub fn get_remote(&self) -> u64 {
        if self.is_inline() {
            NULL_ADDR
        } else {
            let off = self.data_offset();
            Self::read::<u64>(self.data, off)
        }
    }

    pub fn calc_size(has_hist: bool, min_blob_size: usize, val_size: usize) -> usize {
        Self::HDR_LEN
            + Self::GID_LEN
            + Self::DATA_LEN
            + if has_hist { Self::HIST_LEN } else { 0 }
            + if min_blob_size <= val_size {
                ADDR_LEN
            } else {
                val_size
            }
    }

    #[inline(always)]
    pub fn encode_inline(dst: &mut [u8], hist: Option<HistRef>, v: &Record) {
        let merge = v.is_merge();
        dst[0] = v.is_tombstone() as u8 | (merge as u8) << 1;
        dst[1] = v.group_id();
        let mut off = Self::HDR_LEN + Self::GID_LEN;
        Self::write::<u32>(dst, off, v.packed_size() as u32);
        off += Self::DATA_LEN;
        if let Some(hist) = hist {
            dst[0] |= Self::HIST_BIT;
            Self::write::<u64>(dst, off, hist.page_addr);
            off += ADDR_LEN;
            Self::write::<u16>(dst, off, hist.slot);
            off += Self::HIST_SLOT_LEN;
            Self::write::<u32>(dst, off, hist.count);
            off += Self::HIST_COUNT_LEN;
        }
        v.encode_to(&mut dst[off..]);
    }

    #[inline(always)]
    pub fn encode_remote(dst: &mut [u8], hist: Option<HistRef>, remote: u64, v: &Record) {
        let merge = v.is_merge();
        dst[0] = v.is_tombstone() as u8 | (merge as u8) << 1;
        dst[0] |= Self::REMOTE_BIT;
        dst[1] = v.group_id();
        let mut off = Self::HDR_LEN + Self::GID_LEN;
        Self::write::<u32>(dst, off, v.packed_size() as u32);
        off += Self::DATA_LEN;
        if let Some(hist) = hist {
            dst[0] |= Self::HIST_BIT;
            Self::write::<u64>(dst, off, hist.page_addr);
            off += ADDR_LEN;
            Self::write::<u16>(dst, off, hist.slot);
            off += Self::HIST_SLOT_LEN;
            Self::write::<u32>(dst, off, hist.count);
            off += Self::HIST_COUNT_LEN;
        }
        Self::write::<u64>(dst, off, remote);
    }

    #[inline(always)]
    pub fn group_id(&self) -> u8 {
        self.data[1]
    }

    fn write<T>(dst: &mut [u8], off: usize, data: T) {
        let p = &mut dst[off..off + size_of::<T>()].as_mut_ptr();
        unsafe { p.cast::<T>().write_unaligned(data) };
    }

    fn read<T>(src: &[u8], off: usize) -> T {
        let p = src[off..off + size_of::<T>()].as_ptr();
        unsafe { p.cast::<T>().read_unaligned() }
    }
}

impl IDecode for Val<'_> {
    fn decode_from(raw: &[u8]) -> Self {
        Self::from_raw(unsafe { std::mem::transmute::<&[u8], &[u8]>(raw) })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Index {
    pub pid: u64,
}

impl Index {
    pub const fn null() -> Self {
        Self { pid: NULL_PID }
    }

    pub const fn new(id: u64) -> Self {
        Self { pid: id }
    }

    pub const fn len() -> usize {
        size_of::<Self>()
    }
}

impl IVal for Index {
    fn is_tombstone(&self) -> bool {
        self.pid == NULL_PID
    }
}

impl ICodec for Index {
    fn packed_size(&self) -> usize {
        Self::len()
    }

    fn encode_to(&self, to: &mut [u8]) {
        hot_true!(to.len() == self.packed_size());
        number_to_slice!(self.pid, to);
    }
}

impl IDecode for Index {
    fn decode_from(raw: &[u8]) -> Self {
        hot_true!(raw.len() >= size_of::<u64>());
        Self {
            pid: slice_to_number!(&raw[0..Self::len()], u64),
        }
    }
}

impl Display for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

#[derive(Clone, PartialEq, Eq, Copy, Debug)]
pub struct Record {
    group_id: u8,
    data: &'static [u8],
}

impl Record {
    /// runtime-only tag carried in the `group_id` high bit; never persisted
    const MERGE_TAG: u8 = 0b1000_0000;

    pub fn normal(group_id: u8, data: &[u8]) -> Self {
        Self {
            group_id,
            data: unsafe { std::mem::transmute::<&[u8], &[u8]>(data) },
        }
    }

    pub fn remove(group_id: u8) -> Self {
        Self {
            group_id,
            data: [].as_slice(),
        }
    }

    pub fn merge(group_id: u8, operand: &[u8]) -> Self {
        Self {
            group_id: group_id | Self::MERGE_TAG,
            data: unsafe { std::mem::transmute::<&[u8], &[u8]>(operand) },
        }
    }

    #[inline(always)]
    pub fn is_merge(&self) -> bool {
        self.group_id & Self::MERGE_TAG != 0
    }

    #[inline(always)]
    pub fn group_id(&self) -> u8 {
        self.group_id & !Self::MERGE_TAG
    }

    pub(crate) fn set_merge_tag(&mut self) {
        self.group_id |= Self::MERGE_TAG;
    }

    #[inline(always)]
    pub fn data(&self) -> &[u8] {
        self.data
    }

    pub fn from_slice(s: &[u8]) -> Self {
        let (l, r) = s.split_at(size_of::<u8>());
        Self {
            group_id: slice_to_number!(l, u8),
            data: unsafe { std::mem::transmute::<&[u8], &[u8]>(r) },
        }
    }

    pub fn as_slice(&self, s: &mut [u8]) {
        let (l, r) = s.split_at_mut(size_of::<u8>());
        // the merge tag is runtime state, the payload stores only the logical group id
        number_to_slice!(self.group_id & !Self::MERGE_TAG, l);
        hot_true!(eq r.len(), self.data.len());
        hot_true!(ne self.data.as_ptr(), r.as_ptr());
        r.copy_from_slice(self.data);
    }

    pub fn size(&self) -> usize {
        size_of::<u8>() + self.data.len()
    }
}

impl ICodec for Record {
    fn packed_size(&self) -> usize {
        self.size()
    }

    fn encode_to(&self, to: &mut [u8]) {
        self.as_slice(to);
    }
}

impl IDecode for Record {
    fn decode_from(raw: &[u8]) -> Self {
        let s = unsafe { std::slice::from_raw_parts(raw.as_ptr(), raw.len()) };
        Self::from_slice(s)
    }
}

impl IVal for Record {
    fn is_tombstone(&self) -> bool {
        self.data.is_empty()
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_tombstone() {
            f.write_fmt(format_args!("<del-{}>", self.group_id))
        } else if self.is_merge() {
            f.write_fmt(format_args!(
                "<merge-{}> {}",
                self.group_id,
                to_str(self.data)
            ))
        } else {
            f.write_fmt(format_args!(
                "<normal-{}> {}",
                self.group_id,
                to_str(self.data)
            ))
        }
    }
}

#[derive(Default, PartialEq, Eq, Clone, Copy, Hash, Debug)]
pub struct Ver {
    pub txid: u64,
    pub cmd: u32,
}

impl Ver {
    pub fn new(txid: u64, cmd: u32) -> Self {
        Self { txid, cmd }
    }

    pub fn len() -> usize {
        size_of::<u64>() + size_of::<u32>()
    }
}

impl PartialOrd for Ver {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ver {
    /// new to old
    fn cmp(&self, other: &Self) -> Ordering {
        match other.txid.cmp(&self.txid) {
            Ordering::Equal => other.cmd.cmp(&self.cmd),
            o => o,
        }
    }
}

pub struct IterItem<'a, L: ILoader> {
    cached_key: Handle<Vec<u8>>,
    prefix: &'a [u8],
    pub(crate) base: &'a [u8],
    txid: u64,
    pub(crate) val: Val<'a>,
    /// reader-side logical value produced by the merge resolver; when set it overrides
    /// the page-backed envelope for all value accessors
    folded: Option<Box<(u8, Vec<u8>)>>,
    // this item is owned by one iterator and never shared while `val` is called;
    // avoid RefCell's borrow flag on the per-item value cache
    val_ref: UnsafeCell<Option<BoxRef>>,
    loader: &'a L,
}

impl<L> Debug for IterItem<'_, L>
where
    L: ILoader,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IterItem")
            .field("prefix", &to_str(self.prefix))
            .field("base", &self.base)
            .finish()
    }
}

impl<'a, L> IterItem<'a, L>
where
    L: ILoader,
{
    pub(crate) fn new(
        cached_key: Handle<Vec<u8>>,
        prefix: &'a [u8],
        base: Key<'a>,
        val: Val<'a>,
        loader: &'a L,
    ) -> Self {
        Self {
            cached_key,
            prefix,
            base: base.raw,
            txid: base.ver.txid,
            val,
            folded: None,
            val_ref: UnsafeCell::new(None),
            loader,
        }
    }

    /// builds an item carrying a resolver-produced logical value
    pub(crate) fn with_folded(mut self, gid: u8, data: Vec<u8>) -> Self {
        self.folded = Some(Box::new((gid, data)));
        self
    }

    #[cfg(test)]
    pub(crate) fn is_folded(&self) -> bool {
        self.folded.is_some()
    }

    #[inline(always)]
    pub(crate) fn cmp_key(&self, other: &[u8]) -> Ordering {
        let pos = self.prefix.len().min(other.len());
        let tmp = self.prefix.cmp(&other[..pos]);
        match tmp {
            Ordering::Equal => self.base.cmp(&other[pos..]),
            _ => tmp,
        }
    }

    pub(crate) fn cmp(&self, other: &Self) -> Ordering {
        // fast path: most scan compares happen within the same prefix domain
        if self.prefix == other.prefix {
            return self.base.cmp(other.base);
        }

        // keep one-side-empty path branch-local to avoid cmp_key + reverse overhead
        if self.prefix.is_empty() {
            return cmp_raw_with_prefixed_tail(self.base, other.prefix, other.base);
        }
        if other.prefix.is_empty() {
            return cmp_raw_with_prefixed_tail(other.base, self.prefix, self.base).reverse();
        }

        // mixed non-empty prefixes need full-key compare for correctness
        full_raw_cmp(self.prefix, self.base, other.prefix, other.base)
    }

    pub(crate) fn txid(&self) -> u64 {
        self.txid
    }

    #[inline(always)]
    pub(crate) fn prefix_identity(&self) -> (*const u8, usize) {
        (self.prefix.as_ptr(), self.prefix.len())
    }

    #[inline(always)]
    pub(crate) fn assembled_key(&self) -> Handle<Vec<u8>> {
        let mut key = self.cached_key;
        key.clear();
        key.extend_from_slice(self.prefix);
        key.extend_from_slice(self.base);
        key
    }

    /// NOTE: the return Slice is valid only in current iteration
    pub fn key(&self) -> &[u8] {
        unsafe { std::mem::transmute(self.cached_key.as_slice()) }
    }

    /// NOTE: the return Slice is valid only in current iteration
    pub fn val(&self) -> &[u8] {
        if let Some(folded) = &self.folded {
            return folded.1.as_slice();
        }
        if let Some(data) = self.val.inline_data() {
            return data;
        }
        let (r, v) = self.val.get_record_uncached(self.loader);
        // SAFETY: an IterItem is only accessed through its owning iterator; callers
        // cannot concurrently mutate the cache through the public API
        unsafe { *self.val_ref.get() = v };
        r.data
    }
}

#[cfg(test)]
mod test {
    use std::{
        cell::RefCell,
        cmp::Ordering,
        collections::HashMap,
        sync::{Arc, atomic::AtomicU64},
    };

    use crate::{
        BucketOptions,
        types::{
            data::{HistRef, IntlKey, Record, Val, Ver},
            refbox::{BoxRef, BoxView, RemoteView},
            traits::{ICodec, IDecode, IFrameAlloc, IHeader, ILoader},
        },
    };

    use super::{Index, Key, LeafSeg};

    #[test]
    fn test_ver() {
        let mut data = vec![
            Ver::new(1, 1),
            Ver::new(1, 2),
            Ver::new(2, 1),
            Ver::new(2, 2),
            Ver::new(3, 1),
        ];

        data.sort();

        fn lower_bound(data: &[Ver], tgt: Ver) -> Option<&Ver> {
            let mut lo = 0;
            let mut hi = data.len();

            while lo < hi {
                let mid = lo + ((hi - lo) >> 1);
                let k = data[mid];
                match k.cmp(&tgt) {
                    Ordering::Less => lo = mid + 1,
                    _ => hi = mid,
                }
            }
            data.get(lo)
        }
        assert_eq!(
            lower_bound(&data, Ver::new(1, u32::MAX)),
            Some(&Ver::new(1, 2))
        );
        assert_eq!(
            lower_bound(&data, Ver::new(2, u32::MAX)),
            Some(&Ver::new(2, 2))
        );
        assert_eq!(
            lower_bound(&data, Ver::new(3, u32::MAX)),
            Some(&Ver::new(3, 1))
        );

        assert_eq!(
            lower_bound(&data, Ver::new(u64::MAX, u32::MAX)),
            Some(&Ver::new(3, 1))
        );
    }

    #[test]
    fn test_key_codec() {
        let key = Key::new("moha".as_bytes(), Ver::new(114514, 0));
        let mut buf = vec![0u8; key.packed_size()];

        key.encode_to(&mut buf);

        let dk = Key::decode_from(buf.as_slice());

        assert_eq!(dk.raw, key.raw);
        assert!(dk.ver == key.ver);

        let ik = IntlKey::new("foo".as_bytes());
        let mut buf = vec![0u8; ik.packed_size()];

        ik.encode_to(&mut buf);
        let dik = IntlKey::decode_from(&buf);
        assert_eq!(dik.raw, ik.raw);
    }

    #[test]
    fn leaf_seg_full_raw_cmp_handles_prefix_boundary() {
        let l = LeafSeg::new("aa".as_bytes(), "z".as_bytes(), Ver::new(3, 1));
        let r = LeafSeg::new("aab".as_bytes(), "x".as_bytes(), Ver::new(2, 1));
        assert!(l.raw_cmp(&r).is_gt());

        let l = LeafSeg::new("ab".as_bytes(), "0".as_bytes(), Ver::new(3, 1));
        let r = LeafSeg::new("ab".as_bytes(), "1".as_bytes(), Ver::new(2, 1));
        assert!(l.raw_cmp(&r).is_lt());

        let l = LeafSeg::new("ab".as_bytes(), "1".as_bytes(), Ver::new(3, 1));
        let r = LeafSeg::new("ab1".as_bytes(), "".as_bytes(), Ver::new(2, 1));
        assert!(l.raw_cmp(&r).is_eq());
    }

    #[test]
    fn full_raw_cmp_handles_one_side_empty_prefix() {
        assert_eq!(
            super::cmp_raw_with_prefixed_tail("ab1".as_bytes(), "ab".as_bytes(), "1".as_bytes()),
            Ordering::Equal
        );
        assert_eq!(
            super::cmp_raw_with_prefixed_tail("ab".as_bytes(), "abc".as_bytes(), "".as_bytes()),
            Ordering::Less
        );
        assert_eq!(
            super::cmp_raw_with_prefixed_tail("abd".as_bytes(), "ab".as_bytes(), "c".as_bytes()),
            Ordering::Greater
        );
    }

    #[test]
    fn test_val_codec() {
        #[derive(Clone)]
        struct L {
            m: RefCell<HashMap<u64, BoxRef>>,
            addr: Arc<AtomicU64>,
        }

        impl L {
            fn new() -> Self {
                Self {
                    m: RefCell::new(HashMap::new()),
                    addr: Arc::new(AtomicU64::new(114)),
                }
            }
        }

        impl IFrameAlloc for L {
            fn alloc(&mut self, size: u32) -> BoxRef {
                let addr = self.addr.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let b = BoxRef::alloc(size, addr);
                self.m.borrow_mut().insert(addr, b.clone());
                b
            }

            fn inline_size(&self) -> usize {
                BucketOptions::MIN_INLINE_SIZE
            }
        }

        impl ILoader for L {
            fn load_pinned(&self, addr: u64) -> BoxView {
                self.m.borrow().get(&addr).map(|x| x.view()).unwrap()
            }

            fn pin(&self, data: BoxRef) {
                self.m.borrow_mut().insert(data.header().addr, data);
            }

            fn copy(&self) -> Self {
                self.clone()
            }

            fn copy_detached(&self) -> Self {
                self.clone()
            }

            fn load_sibling(&self, addr: u64) -> BoxRef {
                self.m.borrow().get(&addr).cloned().unwrap()
            }

            fn load_blob(&self, addr: u64, _cache: bool) -> BoxRef {
                self.m.borrow().get(&addr).cloned().unwrap()
            }
        }

        let put = Record::normal(1, "114514".as_bytes());
        let del = Record::remove(1);
        let merge = Record::merge(1, "+42".as_bytes());
        let sib = Record::normal(1, "1145141919810".as_bytes());

        let mut inline_size = 1 << 20;
        let hist = HistRef::new(192608, 12, 128);

        {
            let l = L::new();
            let mut put_buf = vec![0u8; Val::calc_size(false, inline_size, put.packed_size())];
            let mut del_buf = vec![0u8; Val::calc_size(false, inline_size, del.packed_size())];
            let mut merge_buf = vec![0u8; Val::calc_size(false, inline_size, merge.packed_size())];
            let mut sib_buf = vec![0u8; Val::calc_size(true, inline_size, sib.packed_size())];

            Val::encode_inline(&mut put_buf, None, &put);
            Val::encode_inline(&mut del_buf, None, &del);
            Val::encode_inline(&mut merge_buf, None, &merge);
            Val::encode_inline(&mut sib_buf, Some(hist), &sib);

            let vp = Val::from_raw(&put_buf);
            let vd = Val::from_raw(&del_buf);
            let vm = Val::from_raw(&merge_buf);
            let vs = Val::from_raw(&sib_buf);

            let dp = vp.get_record(&l).0;
            let dd = vd.get_record(&l).0;
            let dm = vm.get_record(&l).0;
            let ds = vs.get_record(&l).0;

            assert!(dp.eq(&put));
            assert!(dd.eq(&del));
            assert!(
                dm.eq(&merge),
                "merge inline round trip must decode with the tag"
            );
            assert!(dm.is_merge());
            assert_eq!(dm.group_id(), 1);
            assert_eq!(dm.data(), b"+42");
            // the persisted envelope carries MERGE_BIT, never the runtime tag
            assert_eq!(merge_buf[0] & Val::DEL_BIT, 0);
            assert_ne!(merge_buf[0] & Val::MERGE_BIT, 0);
            assert!(ds.eq(&sib));

            assert!(vp.get_hist().is_none());
            assert!(vd.get_hist().is_none());
            assert_eq!(vs.get_hist(), Some(hist));
        }

        inline_size = 0;
        {
            let mut l = L::new();
            let mut put_buf = vec![0u8; Val::calc_size(false, inline_size, put.packed_size())];
            let mut del_buf = vec![0u8; Val::calc_size(false, inline_size, del.packed_size())];
            let mut merge_buf = vec![0u8; Val::calc_size(false, inline_size, merge.packed_size())];
            let mut sib_buf = vec![0u8; Val::calc_size(true, inline_size, sib.packed_size())];

            fn encode_to(a: &mut L, x: &Record) -> u64 {
                let b = RemoteView::alloc(a, x.packed_size());
                let mut r = b.view().as_remote();
                x.encode_to(r.raw_mut());
                b.header().addr
            }

            let pa = encode_to(&mut l, &put);
            let da = encode_to(&mut l, &del);
            let ma = encode_to(&mut l, &merge);
            let sa = encode_to(&mut l, &sib);

            Val::encode_remote(&mut put_buf, None, pa, &put);
            Val::encode_remote(&mut del_buf, None, da, &del);
            Val::encode_remote(&mut merge_buf, None, ma, &merge);
            Val::encode_remote(&mut sib_buf, Some(hist), sa, &sib);

            let vp = Val::from_raw(&put_buf);
            let vd = Val::from_raw(&del_buf);
            let vm = Val::from_raw(&merge_buf);
            let vs = Val::from_raw(&sib_buf);

            let (dp, rp) = vp.get_record(&l);
            let (dd, rd) = vd.get_record(&l);
            let (dm, rm) = vm.get_record(&l);
            let (ds, rs) = vs.get_record(&l);

            assert!(dp.eq(&put));
            assert!(dd.eq(&del));
            assert!(
                dm.eq(&merge),
                "merge remote round trip must decode with the tag"
            );
            assert!(dm.is_merge());
            assert_eq!(dm.group_id(), 1);
            assert_eq!(dm.data(), b"+42");
            assert!(ds.eq(&sib));

            let rp = rp.unwrap();
            let rd = rd.unwrap();
            let rm = rm.unwrap();
            let rs = rs.unwrap();
            assert_eq!(rp.header().addr, pa);
            assert_eq!(rd.header().addr, da);
            assert_eq!(rm.header().addr, ma);
            assert_eq!(rs.header().addr, sa);

            assert_eq!(vp.get_remote(), pa);
            assert_eq!(vd.get_remote(), da);
            assert_eq!(vm.get_remote(), ma);
            assert_eq!(vs.get_remote(), sa);

            assert!(vp.get_hist().is_none());
            assert!(vd.get_hist().is_none());
            assert_eq!(vs.get_hist(), Some(hist));
        }
    }

    #[test]
    fn test_index_codec() {
        let idx = Index::new(19268);
        let mut buf = vec![0u8; idx.packed_size()];

        idx.encode_to(&mut buf);

        let di = Index::decode_from(buf.as_slice());

        assert_eq!(di.pid, idx.pid);

        let k = IntlKey::new("233".as_bytes());
        let v = Index::new(1);
        let mut buf = vec![0u8; k.packed_size() + v.packed_size()];
        let s = buf.as_mut_slice();

        let (ks, vs) = s.split_at_mut(k.packed_size());
        k.encode_to(ks);
        v.encode_to(vs);

        let dk = IntlKey::decode_from(s);
        let x = &mut s[k.packed_size()..];
        let dv = Index::decode_from(x);

        assert_eq!(dk.raw, k.raw);
        assert_eq!(dv, v);
    }
}
