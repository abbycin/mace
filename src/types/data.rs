use std::{cmp::Ordering, ops::Deref};

use crate::{
    number_to_slice, slice_to_number,
    types::traits::{ICodec, IKey, IVal, IValCodec},
    utils::{ADDR_LEN, NULL_PID, to_str, varint::Varint32},
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

    fn remove_prefix(&self, prefix_len: usize) -> Self {
        debug_assert!(self.raw.len() >= prefix_len);
        Self {
            raw: &self.raw[prefix_len..],
        }
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        let (l, r) = to.split_at_mut(Varint32::size(to.len()));
        Varint32::encode(l, self.raw.len() as u32);
        r.copy_from_slice(self.raw);
    }

    fn decode_from(src: &[u8]) -> Self {
        let (raw_len, n) = Varint32::decode(src).unwrap();
        let raw = &src[n..n + raw_len as usize];
        Self {
            raw: unsafe { std::mem::transmute::<&[u8], &[u8]>(raw) },
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

impl ICodec for Key<'_> {
    fn packed_size(&self) -> usize {
        self.len() + Varint32::size(self.len())
    }

    fn remove_prefix(&self, prefix_len: usize) -> Self {
        debug_assert!(self.raw.len() >= prefix_len);
        Self {
            raw: &self.raw[prefix_len..],
            ver: self.ver,
        }
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        let (len, key) = to.split_at_mut(Varint32::size(to.len()));
        Varint32::encode(len, self.len() as u32);
        let (num, raw) = key.split_at_mut(Ver::len());
        self.ver.encode_to(num);
        raw.copy_from_slice(self.raw);
    }

    fn decode_from(data: &[u8]) -> Self {
        let (len, n) = Varint32::decode(data).unwrap();
        let (num, raw) = data[n..n + len as usize].split_at(Ver::len());

        Self {
            raw: unsafe { std::mem::transmute::<&[u8], &[u8]>(raw) },
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

    fn remove_prefix(&self, _prefix_len: usize) -> Self {
        *self
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), Self::len());
        let (x, y) = to.split_at_mut(size_of::<u64>());
        number_to_slice!(self.txid, x);
        number_to_slice!(self.cmd, y);
    }

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

    fn decode_from(_raw: &[u8]) -> Self {
        unimplemented!()
    }

    fn encode_to(&self, to: &mut [u8]) {
        let (len, data) = to.split_at_mut(Varint32::size(self.len()));
        let (p, b) = data.split_at_mut(self.prefix.len());
        Varint32::encode(len, self.len() as u32);
        p.copy_from_slice(self.prefix);
        b.copy_from_slice(self.base);
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

    fn decode_from(_raw: &[u8]) -> Self {
        unimplemented!()
    }

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

#[derive(Clone, Copy)]
pub struct Sibling<T> {
    addr: u64,
    data: T,
}

impl<T> Sibling<T> {
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
}

impl<T> IValCodec for Sibling<T>
where
    T: IValCodec,
{
    fn size(&self) -> usize {
        ADDR_LEN + self.data.size()
    }

    fn encode(&self, to: &mut [u8]) {
        debug_assert_eq!(self.size(), to.len());
        let (addr, data) = to.split_at_mut(ADDR_LEN);
        number_to_slice!(self.addr, addr);
        self.data.encode(data);
    }

    fn decode(from: &[u8]) -> Self {
        let (addr, data) = from.split_at(ADDR_LEN);
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

    pub fn unpack_sibling(&self) -> Self {
        if let Some(s) = self.sibling() {
            if s.is_tombstone() {
                Value::Del(*s.as_ref())
            } else {
                Value::Put(*s.as_ref())
            }
        } else {
            *self
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
        Varint32::size(len) + len
    }

    fn remove_prefix(&self, _prefix_len: usize) -> Self {
        unimplemented!()
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        let (len, payload) = to.split_at_mut(Varint32::size(to.len()));
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

    fn decode_from(raw: &[u8]) -> Self {
        let (len, n) = Varint32::decode(raw).unwrap();
        let state = raw[n];
        let b = &raw[n + 1..n + len as usize];

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

    fn is_tombstone(&self) -> bool {
        self.is_del()
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
    fn to_string(&self) -> String {
        format!("{{ pid: {} }}", self.pid)
    }

    fn is_tombstone(&self) -> bool {
        self.pid == NULL_PID
    }
}

impl ICodec for Index {
    fn packed_size(&self) -> usize {
        Self::len()
    }

    fn remove_prefix(&self, _prefix_len: usize) -> Self {
        unimplemented!()
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        number_to_slice!(self.pid, to);
    }

    fn decode_from(raw: &[u8]) -> Self {
        debug_assert!(raw.len() >= size_of::<u64>());
        Self {
            pid: slice_to_number!(&raw[0..Self::len()], u64),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Copy, Debug)]
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

impl IValCodec for Record {
    fn size(&self) -> usize {
        self.size()
    }

    fn encode(&self, to: &mut [u8]) {
        self.as_slice(to);
    }

    fn decode(raw: &[u8]) -> Self {
        let s = unsafe { std::slice::from_raw_parts(raw.as_ptr(), raw.len()) };
        Self::from_slice(s)
    }

    fn to_string(&self) -> String {
        format!(
            "Record {{ tombstone: {}, worker_id: {}, data: {} }}",
            self.data().is_empty(),
            self.worker_id(),
            to_str(self.data)
        )
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
    use crate::types::{
        data::{IntlKey, Record, Ver},
        traits::ICodec,
    };

    use super::{Index, Key, Sibling, Value};

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
        let put = Value::Put(Record::normal(1, "114514".as_bytes()));
        let del = Value::Del(Record::remove(1));
        let sib = Value::Sib(Sibling::put(
            233,
            Record::normal(1, "1145141919810".as_bytes()),
        ));
        let mut put_buf = vec![0u8; put.packed_size()];
        let mut del_buf = vec![0u8; del.packed_size()];
        let mut sib_buf = vec![0u8; sib.packed_size()];

        put.encode_to(&mut put_buf);
        del.encode_to(&mut del_buf);
        sib.encode_to(&mut sib_buf);

        let pb = put_buf.as_slice();
        let db = del_buf.as_slice();
        let sb = sib_buf.as_slice();

        let dp = Value::<Record>::decode_from(pb);
        let dd = Value::<Record>::decode_from(db);
        let ds = Value::<Record>::decode_from(sb);

        assert!(dp.as_ref().eq(put.as_ref()));
        assert!(dd.as_ref().eq(del.as_ref()));
        assert!(ds.as_ref().eq(sib.as_ref()));
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

    #[test]
    fn sibling() {
        let s = Sibling::put(233, Record::normal(1, "114514".as_bytes()));
        let v = Value::Sib(s);
        let mut buf = vec![0u8; v.packed_size()];

        v.encode_to(&mut buf);

        let dv = Value::<Record>::decode_from(buf.as_slice());

        assert_eq!(dv.sibling().unwrap().addr, 233);
    }
}
