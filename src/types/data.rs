use std::{cmp::Ordering, fmt::Display, ops::Deref};

use crate::{
    number_to_slice, slice_to_number,
    types::{
        refbox::RemoteView,
        traits::{ICodec, IDecode, IKey, IKeyCodec, ILoader, IVal},
    },
    utils::{ADDR_LEN, NULL_ADDR, NULL_PID, to_str, varint::Varint32},
};

#[derive(Default, PartialEq, Eq, Clone, Copy)]
pub struct IntlKey<'a> {
    pub raw: &'a [u8],
}

impl<'a> IntlKey<'a> {
    pub(crate) fn new(raw: &'a [u8]) -> Self {
        Self { raw }
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

    fn to_string(&self) -> String {
        format!("raw {}", to_str(self.raw))
    }
}

impl ICodec for IntlKey<'_> {
    fn packed_size(&self) -> usize {
        Varint32::size(self.raw.len()) + self.raw.len()
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        let (l, r) = to.split_at_mut(Varint32::size(to.len()));
        Varint32::encode(l, self.raw.len() as u32);
        r.copy_from_slice(self.raw);
    }
}

impl IDecode for IntlKey<'_> {
    fn decode_from(src: &[u8]) -> Self {
        let (raw_len, n) = Varint32::decode(src).unwrap();
        let raw = &src[n..n + raw_len as usize];
        Self {
            raw: unsafe { std::mem::transmute::<&[u8], &[u8]>(raw) },
        }
    }
}

impl IKeyCodec for IntlKey<'_> {
    fn remove_prefix(&self, prefix_len: usize) -> Self {
        debug_assert!(self.raw.len() >= prefix_len);
        Self {
            raw: &self.raw[prefix_len..],
        }
    }
}

#[derive(Default, PartialEq, Eq, Clone, Copy)]
pub struct Key<'a> {
    pub raw: &'a [u8],
    pub ver: Ver,
}

impl<'a> Key<'a> {
    pub fn new(raw: &'a [u8], ver: Ver) -> Self {
        Self { raw, ver }
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
        let (len, n) = Varint32::decode(data).unwrap();
        let (num, raw) = data[n..n + len as usize].split_at(Ver::len());

        Self {
            raw: unsafe { std::mem::transmute::<&[u8], &[u8]>(raw) },
            ver: Ver::decode_from(num),
        }
    }
}

impl ICodec for Key<'_> {
    fn packed_size(&self) -> usize {
        self.len() + Varint32::size(self.len())
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        let (len, key) = to.split_at_mut(Varint32::size(to.len()));
        Varint32::encode(len, self.len() as u32);
        let (num, raw) = key.split_at_mut(Ver::len());
        self.ver.encode_to(num);
        raw.copy_from_slice(self.raw);
    }
}

impl IKeyCodec for Key<'_> {
    fn remove_prefix(&self, prefix_len: usize) -> Self {
        debug_assert!(self.raw.len() >= prefix_len);
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

    fn to_string(&self) -> String {
        format!("Ver {{ txid: {}, cmd: {} }}", self.txid, self.cmd)
    }
}

impl IKeyCodec for Ver {
    fn remove_prefix(&self, _prefix_len: usize) -> Self {
        *self
    }
}

// TODO: varint encode/decode, space-time trade-off
impl ICodec for Ver {
    fn packed_size(&self) -> usize {
        Self::len()
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), Self::len());
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
        debug_assert!(self.len() >= prefix_len);
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

impl<'a> LeafSeg<'a> {
    pub(crate) fn new(prefix: &'a [u8], base: &'a [u8], ver: Ver) -> Self {
        Self { prefix, base, ver }
    }

    /// compare in same node, they share same prefix
    pub(crate) fn raw_cmp(&self, other: &Self) -> Ordering {
        self.base.cmp(other.base)
    }

    pub(crate) fn cmp(&self, other: &Self) -> Ordering {
        match self.raw_cmp(other) {
            Ordering::Equal => self.ver.cmp(&other.ver),
            o => o,
        }
    }

    pub(crate) fn raw(&self) -> &'a [u8] {
        self.base
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
        debug_assert!(self.len() - Ver::len() >= prefix_len);
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
/// +-----+----------+------+
/// | HDR | DATA LEN | DATA |
/// +-----+----------+------+
///```
/// 2. inline data with sibling
///```text
/// +-----+----------+----------+------+
/// | HDR | DATA LEN | SIB ADDR | DATA |
/// +-----+----------+----------+------+
///```
/// 3. remote data
///```text
/// +-----+----------+-------------+
/// | HDR | DATA LEN | REMOTE ADDR |
/// +-----+----------+-------------+
///```
/// 4. remote data with sibling
///```text
/// +-----+----------+----------+-------------+
/// | HDR | DATA LEN | SIB ADDR | REMOTE ADDR |
/// +-----+----------+----------+-------------+
/// ```
#[derive(Clone, Copy)]
pub struct Val<'a> {
    data: &'a [u8],
}

impl<'a> Val<'a> {
    const DEL_BIT: u8 = 0b0000_0001;
    const SIB_BIT: u8 = 0b1000_0000;
    const REMOTE_BIT: u8 = 0b0001_0000;
    const SIB_LEN: usize = ADDR_LEN;
    const DATA_LEN: usize = size_of::<u32>();
    const HDR_LEN: usize = 1;

    pub fn is_tombstone(&self) -> bool {
        self.data[0] & Self::DEL_BIT != 0
    }

    pub fn is_sibling(&self) -> bool {
        self.data[0] & Self::SIB_BIT != 0
    }

    fn is_inline(&self) -> bool {
        self.data[0] & Self::REMOTE_BIT == 0
    }

    fn data_offset(&self) -> usize {
        Self::HDR_LEN + Self::DATA_LEN + self.is_sibling() as usize * Self::SIB_LEN
    }

    pub fn from_raw(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn data_size(&self) -> usize {
        Self::read::<u32>(self.data, Self::HDR_LEN) as usize
    }

    pub fn get_record<L: ILoader>(&self, l: &L) -> (Record, RemoteView) {
        let off = self.data_offset();
        let len = self.data_size();
        let (src, r) = if self.is_inline() {
            (&self.data[off..], RemoteView::null())
        } else {
            let addr = Self::read::<u64>(self.data, off);
            let r = l.load_remote_unchecked(addr).as_remote();
            (r.raw(), r)
        };
        (Record::decode_from(&src[..len]), r)
    }

    pub fn get_sibling(&self) -> Option<u64> {
        if self.is_sibling() {
            Some(Self::read::<u64>(self.data, Self::HDR_LEN + Self::DATA_LEN))
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

    pub fn calc_size(is_sib: bool, min_blob_size: usize, val_size: usize) -> usize {
        Self::HDR_LEN
            + Self::DATA_LEN
            + if is_sib { Self::SIB_LEN } else { 0 }
            + if min_blob_size < val_size {
                ADDR_LEN
            } else {
                val_size
            }
    }

    pub fn encode_inline<V: IVal>(dst: &mut [u8], sib: u64, v: &V) {
        dst[0] = v.is_tombstone() as u8;
        let mut off = Self::HDR_LEN;
        Self::write::<u32>(dst, off, v.packed_size() as u32);
        off += Self::DATA_LEN;
        if sib != NULL_ADDR {
            dst[0] |= Self::SIB_BIT;
            Self::write::<u64>(dst, off, sib);
            off += Self::SIB_LEN;
        }
        v.encode_to(&mut dst[off..]);
    }

    pub fn encode_remote<V: IVal>(dst: &mut [u8], sib: u64, remote: u64, v: &V) {
        dst[0] = v.is_tombstone() as u8;
        dst[0] |= Self::REMOTE_BIT;
        let mut off = Self::HDR_LEN;
        Self::write::<u32>(dst, off, v.packed_size() as u32);
        off += Self::DATA_LEN;
        if sib != NULL_ADDR {
            dst[0] |= Self::SIB_BIT;
            Self::write::<u64>(dst, off, sib);
            off += Self::SIB_LEN;
        }
        Self::write::<u64>(dst, off, remote);
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
        debug_assert_eq!(to.len(), self.packed_size());
        number_to_slice!(self.pid, to);
    }
}

impl IDecode for Index {
    fn decode_from(raw: &[u8]) -> Self {
        debug_assert!(raw.len() >= size_of::<u64>());
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

#[derive(Clone, PartialEq, Eq, Copy)]
pub struct Record {
    worker_id: u16,
    data: &'static [u8],
}

impl Record {
    pub fn normal(worker_id: u16, data: &[u8]) -> Self {
        Self {
            worker_id,
            data: unsafe { std::mem::transmute::<&[u8], &[u8]>(data) },
        }
    }

    pub fn remove(worker_id: u16) -> Self {
        Self {
            worker_id,
            data: [].as_slice(),
        }
    }

    pub fn worker_id(&self) -> u16 {
        self.worker_id
    }

    pub fn data(&self) -> &[u8] {
        self.data
    }

    pub fn from_slice(s: &[u8]) -> Self {
        let (l, r) = s.split_at(size_of::<u16>());
        Self {
            worker_id: slice_to_number!(l, u16),
            data: unsafe { std::mem::transmute::<&[u8], &[u8]>(r) },
        }
    }

    pub fn as_slice(&self, s: &mut [u8]) {
        let (l, r) = s.split_at_mut(size_of::<u16>());
        number_to_slice!(self.worker_id, l);
        debug_assert_eq!(r.len(), self.data.len());
        debug_assert_ne!(self.data.as_ptr(), r.as_ptr());
        r.copy_from_slice(self.data);
    }

    pub fn size(&self) -> usize {
        size_of::<u16>() + self.data.len()
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
            f.write_fmt(format_args!("<del-{}>", self.worker_id))
        } else {
            f.write_fmt(format_args!(
                "<normal-{}> {}",
                self.worker_id,
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

#[cfg(test)]
mod test {
    use std::{
        cell::RefCell,
        collections::HashMap,
        sync::{Arc, atomic::AtomicU64},
    };

    use crate::{
        Options,
        types::{
            data::{IntlKey, Record, Val, Ver},
            refbox::{BoxRef, RemoteView},
            traits::{IAlloc, IBoxHeader, ICodec, IDecode, IHeader, ILoader},
        },
        utils::NULL_ADDR,
    };

    use super::{Index, Key};

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

        impl IAlloc for L {
            fn allocate(&mut self, size: usize) -> BoxRef {
                let addr = self.addr.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let b = BoxRef::alloc(size as u32, addr);
                self.m.borrow_mut().insert(addr, b.clone());
                b
            }

            fn arena_size(&mut self) -> usize {
                64 << 20
            }

            fn collect(&mut self, _addr: &[u64]) {}

            fn inline_size(&self) -> usize {
                Options::INLINE_SIZE
            }
        }

        impl ILoader for L {
            fn load(&self, addr: u64) -> Option<crate::types::refbox::BoxView> {
                self.m.borrow().get(&addr).map(|x| x.view())
            }

            fn load_unchecked(&self, addr: u64) -> crate::types::refbox::BoxView {
                self.load(addr).unwrap()
            }

            fn pin(&self, data: BoxRef) {
                self.m.borrow_mut().insert(data.header().addr, data);
            }

            fn shallow_copy(&self) -> Self {
                self.clone()
            }

            fn load_remote(&self, addr: u64) -> Option<crate::types::refbox::BoxView> {
                self.load(addr)
            }
        }

        let put = Record::normal(1, "114514".as_bytes());
        let del = Record::remove(1);
        let sib = Record::normal(1, "1145141919810".as_bytes());

        let mut inline_size = 1 << 20;
        let sib_addr = 192608;

        {
            let l = L::new();
            let mut put_buf = vec![0u8; Val::calc_size(false, inline_size, put.packed_size())];
            let mut del_buf = vec![0u8; Val::calc_size(false, inline_size, del.packed_size())];
            let mut sib_buf = vec![0u8; Val::calc_size(true, inline_size, sib.packed_size())];

            Val::encode_inline(&mut put_buf, NULL_ADDR, &put);
            Val::encode_inline(&mut del_buf, NULL_ADDR, &del);
            Val::encode_inline(&mut sib_buf, sib_addr, &sib);

            let vp = Val::from_raw(&put_buf);
            let vd = Val::from_raw(&del_buf);
            let vs = Val::from_raw(&sib_buf);

            let dp = vp.get_record(&l).0;
            let dd = vd.get_record(&l).0;
            let ds = vs.get_record(&l).0;

            assert!(dp.eq(&put));
            assert!(dd.eq(&del));
            assert!(ds.eq(&sib));

            assert!(vp.get_sibling().is_none());
            assert!(vd.get_sibling().is_none());
            assert_eq!(vs.get_sibling(), Some(sib_addr));
        }

        inline_size = 1;
        {
            let mut l = L::new();
            let mut put_buf = vec![0u8; Val::calc_size(false, inline_size, put.packed_size())];
            let mut del_buf = vec![0u8; Val::calc_size(false, inline_size, del.packed_size())];
            let mut sib_buf = vec![0u8; Val::calc_size(true, inline_size, sib.packed_size())];

            fn encode_to(a: &mut L, x: &Record) -> u64 {
                let b = RemoteView::alloc(a, x.packed_size());
                let mut r = b.view().as_remote();
                x.encode_to(r.raw_mut());
                b.header().addr
            }

            let pa = encode_to(&mut l, &put);
            let da = encode_to(&mut l, &del);
            let sa = encode_to(&mut l, &sib);

            Val::encode_remote(&mut put_buf, NULL_ADDR, pa, &put);
            Val::encode_remote(&mut del_buf, NULL_ADDR, da, &del);
            Val::encode_remote(&mut sib_buf, sib_addr, sa, &sib);

            let vp = Val::from_raw(&put_buf);
            let vd = Val::from_raw(&del_buf);
            let vs = Val::from_raw(&sib_buf);

            let (dp, rp) = vp.get_record(&l);
            let (dd, rd) = vd.get_record(&l);
            let (ds, rs) = vs.get_record(&l);

            assert!(dp.eq(&put));
            assert!(dd.eq(&del));
            assert!(ds.eq(&sib));

            assert_eq!(rp.box_header().addr, pa);
            assert_eq!(rd.box_header().addr, da);
            assert_eq!(rs.box_header().addr, sa);

            assert_eq!(vp.get_remote(), pa);
            assert_eq!(vd.get_remote(), da);
            assert_eq!(vs.get_remote(), sa);

            assert!(vp.get_sibling().is_none());
            assert!(vd.get_sibling().is_none());
            assert_eq!(vs.get_sibling(), Some(sib_addr));
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
