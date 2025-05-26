use std::cmp::Ordering;
use std::ops::Deref;

use crate::cc::data::Ver;
use crate::utils::traits::{ICodec, IKey, IVal, IValCodec};
use crate::utils::varint::{Varint32, Varint64};
use crate::{number_to_slice, slice_to_number, utils::bytes::ByteArray};

fn to_str(x: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(x) }
}

#[derive(Default, PartialEq, Eq, Clone, Copy)]
pub struct Key<'a> {
    pub raw: &'a [u8],
    ver: Ver,
}

impl<'a> Key<'a> {
    pub fn new(raw: &'a [u8], txid: u64, cmd: u32) -> Self {
        Self {
            raw,
            ver: Ver::new(txid, cmd),
        }
    }

    pub fn len(&self) -> usize {
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

impl ICodec for Key<'_> {
    fn packed_size(&self) -> usize {
        self.len() + Varint32::size(self.len() as u32)
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        let (len, key) = to.split_at_mut(Varint32::size(to.len() as u32));
        Varint32::encode(len, self.len() as u32);
        let (num, raw) = key.split_at_mut(Ver::len());
        self.ver.encode_to(num);
        raw.copy_from_slice(self.raw);
    }

    fn decode_from(data: ByteArray) -> Self {
        let (len, n) = Varint32::decode(data.as_slice(0, data.len())).unwrap();
        let num = data.sub_array(n, Ver::len());
        let raw = data.as_slice(n + Ver::len(), len as usize - Ver::len());

        Self {
            raw,
            ver: Ver::decode_from(num),
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

// TODO: varint encode/decode, space-time trade-off
impl ICodec for Ver {
    fn packed_size(&self) -> usize {
        Self::len()
    }

    fn encode_to(&self, to: &mut [u8]) {
        let (x, y) = to.split_at_mut(size_of::<u64>());
        number_to_slice!(self.txid, x);
        number_to_slice!(self.cmd, y);
    }

    fn decode_from(raw: ByteArray) -> Self {
        let s = raw.as_slice(0, Self::len());
        let (x, y) = s.split_at(size_of::<u64>());
        Self {
            txid: slice_to_number!(x, u64),
            cmd: slice_to_number!(y, u32),
        }
    }
}

#[derive(Clone, Copy)]
pub struct Sibling<T> {
    addr: u64,
    data: T,
}

impl<T> Sibling<T> {
    pub const ADDR_LEN: usize = size_of::<u64>();
    pub const DEL_BIT: u64 = 1 << 63;

    pub fn put(addr: u64, data: T) -> Self {
        Self { addr, data }
    }

    pub fn del(addr: u64, data: T) -> Self {
        Self {
            addr: addr | Self::DEL_BIT,
            data,
        }
    }

    pub fn addr(&self) -> u64 {
        self.addr & !Self::DEL_BIT
    }

    pub fn is_tombstone(&self) -> bool {
        self.addr & Self::DEL_BIT != 0
    }

    pub fn as_ref(&self) -> &T {
        &self.data
    }
}

impl<T> Sibling<T>
where
    T: IValCodec,
{
    pub fn from(addr: u64, v: &Value<T>) -> Self {
        if v.is_del() {
            Sibling::del(addr, *v.as_ref())
        } else {
            Sibling::put(addr, *v.as_ref())
        }
    }

    pub fn to(&self, x: T) -> Value<T> {
        if self.is_tombstone() {
            Value::Del(x)
        } else {
            Value::Put(x)
        }
    }
}

impl<T> IValCodec for Sibling<T>
where
    T: IValCodec,
{
    fn size(&self) -> usize {
        Self::ADDR_LEN + self.data.size()
    }

    fn encode(&self, to: &mut [u8]) {
        debug_assert_eq!(self.size(), to.len());
        let (addr, data) = to.split_at_mut(Self::ADDR_LEN);
        number_to_slice!(self.addr, addr);
        self.data.encode(data);
    }

    fn decode(from: &[u8]) -> Self {
        let (addr, data) = from.split_at(Self::ADDR_LEN);
        Self {
            addr: slice_to_number!(addr, u64),
            data: T::decode(data),
        }
    }

    fn to_string(&self) -> String {
        format!(
            "({} {}, {})",
            self.addr(),
            self.is_tombstone(),
            self.data.to_string()
        )
    }

    fn data(&self) -> &[u8] {
        self.data.data()
    }
}

#[derive(Default, Clone, Copy)]
pub enum Value<T> {
    Put(T),
    Del(T),
    Sib(Sibling<T>),
    #[default]
    Unused,
}

impl<T> Value<T>
where
    T: IValCodec,
{
    pub fn len(&self) -> usize {
        match self {
            Value::Put(x) => x.size(),
            Value::Del(x) => x.size(),
            Value::Sib(x) => x.size(),
            Value::Unused => unreachable!(),
        }
    }

    pub fn is_del(&self) -> bool {
        match self {
            Value::Del(_) => true,
            Value::Put(_) => false,
            Value::Sib(x) => x.is_tombstone(),
            Value::Unused => unreachable!(),
        }
    }

    pub fn sibling(&self) -> Option<&Sibling<T>> {
        match self {
            Value::Sib(x) => Some(x),
            _ => None,
        }
    }

    pub fn as_ref(&self) -> &T {
        match self {
            Value::Put(x) => x,
            Value::Del(x) => x,
            Value::Sib(x) => x.as_ref(),
            Value::Unused => unreachable!(),
        }
    }
}

const VALUE_PUT_BIT: u8 = 7;
const VALUE_DEL_BIT: u8 = 11;
const VALUE_SIB_BIT: u8 = 17;

impl<T> ICodec for Value<T>
where
    T: IValCodec,
{
    /// using extra 1 byte for Put or Del, since value can be empty(zero length slice)
    fn packed_size(&self) -> usize {
        let len = self.len() + 1;
        Varint32::size(len as u32) + len
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        let (len, payload) = to.split_at_mut(Varint32::size(to.len() as u32));
        Varint32::encode(len, 1 + self.len() as u32);
        let (id, payload) = payload.split_at_mut(1);
        match self {
            Value::Put(x) => {
                id[0] = VALUE_PUT_BIT;
                x.encode(payload);
            }
            Value::Del(x) => {
                id[0] = VALUE_DEL_BIT;
                x.encode(payload);
            }
            Value::Sib(x) => {
                id[0] = VALUE_SIB_BIT;
                x.encode(payload);
            }
            Value::Unused => unreachable!(),
        }
    }

    fn decode_from(raw: ByteArray) -> Self {
        let (len, n) = Varint32::decode(raw.as_slice(0, raw.len())).unwrap();
        let state = raw.as_mut_slice(n, 1)[0];
        let b = raw.as_slice(n + 1, (len - 1) as usize);

        match state {
            VALUE_DEL_BIT => Value::Del(T::decode(b)),
            VALUE_PUT_BIT => Value::Put(T::decode(b)),
            VALUE_SIB_BIT => {
                let s = Sibling::<T>::decode(b);
                Value::Sib(s)
            }
            _ => unreachable!("invalid state {}", state),
        }
    }
}

impl<T> IVal for Value<T>
where
    T: IValCodec,
{
    fn to_string(&self) -> String {
        match self {
            Value::Del(x) => format!("<del, {}>", x.to_string()),
            Value::Put(x) => format!("<put, {}>", x.to_string()),
            Value::Sib(x) => format!("<sib, {}>", x.to_string()),
            Value::Unused => unreachable!(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Index {
    pub pid: u64,
    pub epoch: u64,
}

impl Index {
    pub const fn new(id: u64, epoch: u64) -> Self {
        Self { pid: id, epoch }
    }

    pub const fn len() -> usize {
        size_of::<Self>()
    }
}

impl Ord for Index {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.epoch.cmp(&other.epoch) {
            Ordering::Equal => self.pid.cmp(&other.pid),
            x => x,
        }
    }
}

impl PartialOrd for Index {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl IVal for Index {
    fn to_string(&self) -> String {
        format!("{{ pid: {}, epoch {} }}", self.pid, self.epoch)
    }
}

impl ICodec for Index {
    fn packed_size(&self) -> usize {
        Self::len()
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        let (pid, epoch) = to.split_at_mut(8);
        number_to_slice!(self.pid, pid);
        number_to_slice!(self.epoch, epoch);
    }

    fn decode_from(raw: ByteArray) -> Self {
        debug_assert!(raw.len() >= 16);
        let s = raw.as_mut_slice(0, Self::len());
        let (pid, epoch) = s.split_at(8);

        Self {
            pid: slice_to_number!(pid, u64),
            epoch: slice_to_number!(epoch, u64),
        }
    }
}

impl IKey for &[u8] {
    fn raw(&self) -> &[u8] {
        self
    }

    fn to_string(&self) -> String {
        unsafe { std::str::from_utf8_unchecked(self).into() }
    }
}

impl ICodec for &[u8] {
    fn packed_size(&self) -> usize {
        let len = self.len();
        Varint32::size(len as u32) + len
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        let (len, data) = to.split_at_mut(Varint32::size(to.len() as u32));
        Varint32::encode(len, self.len() as u32);
        data.copy_from_slice(self);
    }

    fn decode_from(raw: ByteArray) -> Self {
        let (len, n) = Varint32::decode(raw.as_slice(0, raw.len())).unwrap();
        raw.as_slice(n, len as usize)
    }
}

impl IValCodec for &[u8] {
    fn size(&self) -> usize {
        self.len()
    }

    fn encode(&self, to: &mut [u8]) {
        to.copy_from_slice(self);
    }

    fn decode(from: &[u8]) -> Self {
        unsafe { std::slice::from_raw_parts(from.as_ptr(), from.len()) }
    }

    fn to_string(&self) -> String {
        unsafe { std::str::from_utf8_unchecked(self).into() }
    }

    fn data(&self) -> &[u8] {
        self
    }
}

pub(crate) struct Slot {
    meta: u64,
}

impl Slot {
    const INDIRECT_BIT: u64 = 1 << 63;
    const MASK: u8 = 0x80; // highest byte mask
    pub const REMOTE_LEN: usize = size_of::<Self>();
    pub const LOCAL_LEN: usize = 1;

    pub(crate) const fn from_remote(addr: u64) -> Self {
        Self {
            meta: addr | Self::INDIRECT_BIT,
        }
    }

    pub(crate) const fn inline() -> Self {
        Self { meta: 0 }
    }

    #[inline]
    pub(crate) const fn is_inline(&self) -> bool {
        self.meta & Self::INDIRECT_BIT == 0
    }

    pub(crate) const fn addr(&self) -> u64 {
        debug_assert!(!self.is_inline());
        self.meta & !Self::INDIRECT_BIT
    }
}

impl ICodec for Slot {
    fn packed_size(&self) -> usize {
        if self.is_inline() {
            let n = Varint64::size(self.meta);
            debug_assert_eq!(n, Self::LOCAL_LEN);
            n
        } else {
            Self::REMOTE_LEN
        }
    }

    fn encode_to(&self, to: &mut [u8]) {
        if self.is_inline() {
            Varint64::encode(to, self.meta);
        } else {
            debug_assert!(to.len() == Self::REMOTE_LEN);
            let be = self.meta.to_be_bytes();
            to.copy_from_slice(&be);
        }
    }

    fn decode_from(raw: ByteArray) -> Self {
        let meta = raw.read::<u8>(0);
        if meta & Self::MASK == 0 {
            Self::inline()
        } else {
            Self {
                meta: <u64>::from_be_bytes(raw.as_slice(0, Self::REMOTE_LEN).try_into().unwrap()),
            }
        }
    }
}

pub(crate) const SLOT_LEN: usize = size_of::<u32>();

#[cfg(test)]
mod test {
    use crate::utils::{
        bytes::ByteArray,
        traits::{ICodec, IValCodec},
    };

    use super::{Index, Key, Sibling, Value};

    #[test]
    fn test_key_codec() {
        let key = Key::new("moha".as_bytes(), 114514, 0);
        let mut buf = vec![0u8; key.packed_size()];

        key.encode_to(&mut buf);

        let b = ByteArray::new(buf.as_mut_ptr(), buf.len());
        let dk = Key::decode_from(b);

        assert_eq!(dk.raw, key.raw);
        assert!(dk.ver == key.ver);
    }

    #[test]
    fn test_val_codec() {
        let put = Value::Put("114514".as_bytes());
        let del = Value::Del("del".as_bytes());
        let sib = Value::Sib(Sibling::put(233, "1145141919810u64".as_bytes()));
        let mut put_buf = vec![0u8; put.packed_size()];
        let mut del_buf = vec![0u8; del.packed_size()];
        let mut sib_buf = vec![0u8; sib.packed_size()];

        put.encode_to(&mut put_buf);
        del.encode_to(&mut del_buf);
        sib.encode_to(&mut sib_buf);

        let pb = ByteArray::new(put_buf.as_mut_ptr(), put_buf.len());
        let db = ByteArray::new(del_buf.as_mut_ptr(), del_buf.len());
        let sb = ByteArray::new(sib_buf.as_mut_ptr(), sib_buf.len());

        let dp = Value::<&[u8]>::decode_from(pb);
        let dd = Value::<&[u8]>::decode_from(db);
        let ds = Value::<&[u8]>::decode_from(sb);

        assert!(dp.as_ref().eq(put.as_ref()));
        assert!(dd.as_ref().eq(del.as_ref()));
        assert!(ds.as_ref().eq(sib.as_ref()));
    }

    #[test]
    fn test_u8_codec() {
        let x = "+1s".as_bytes();
        let mut buf = vec![0u8; x.size()];

        x.encode(&mut buf);

        let xb = buf.as_slice();

        let dx = <&[u8] as IValCodec>::decode(xb);

        assert_eq!(dx, x);
    }

    #[test]
    fn test_index_codec() {
        let idx = Index::new(19268, 233);
        let mut buf = vec![0u8; idx.packed_size()];

        idx.encode_to(&mut buf);

        let ib = ByteArray::new(buf.as_mut_ptr(), buf.len());

        let di = Index::decode_from(ib);

        assert_eq!(di.epoch, idx.epoch);
        assert_eq!(di.pid, idx.pid);

        let k = "233".as_bytes();
        let v = Index::new(1, 0);
        let mut buf = vec![0u8; k.packed_size() + v.packed_size()];
        let s = buf.as_mut_slice();

        let (ks, vs) = s.split_at_mut(k.packed_size());
        k.encode_to(ks);
        v.encode_to(vs);

        let b = ByteArray::new(s.as_mut_ptr(), s.len());

        let dk = <&[u8] as ICodec>::decode_from(b);
        let dv = Index::decode_from(b.add(k.packed_size()));

        assert_eq!(dk, k);
        assert_eq!(dv, v);
    }

    #[test]
    fn sibling() {
        let s = Sibling::put(233, "114514".as_bytes());
        let v = Value::Sib(s);
        let mut buf = vec![0u8; v.packed_size()];

        v.encode_to(&mut buf);

        let raw = ByteArray::new(buf.as_mut_ptr(), buf.len());
        let dv = Value::<&[u8]>::decode_from(raw);

        assert_eq!(dv.sibling().unwrap().addr, 233);
    }
}
