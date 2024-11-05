use std::cmp::Ordering;

use super::traits::{ICodec, IKey, IVal};
use crate::{number_to_slice, slice_to_number, utils::byte_array::ByteArray};

const KEY_VAL_FIXED_BYTES: usize = 4;

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct Key<'a> {
    pub raw: &'a [u8],
    pub lsn: u64, // Txn GSN
}

impl<'a> Key<'a> {
    pub const LSN_LEN: usize = size_of::<u64>();

    pub fn new(raw: &'a [u8], lsn: u64) -> Self {
        Self { raw, lsn }
    }

    pub fn len(&self) -> usize {
        self.raw.len() + Self::LSN_LEN
    }
}

impl PartialOrd for Key<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.raw.cmp(other.raw))
    }
}

impl Ord for Key<'_> {
    /// NOTE: key is in ascending order, while lsn is descending order, since lsn is monotonically
    /// increasing, greater is newer
    fn cmp(&self, other: &Self) -> Ordering {
        match self.raw.cmp(other.raw) {
            Ordering::Equal => other.lsn.cmp(&self.lsn), // descending
            x => x,
        }
    }
}

impl<'a> IKey for &'a [u8] {
    fn raw(&self) -> &[u8] {
        self
    }

    fn separator(&self) -> Self {
        self
    }
}

impl<'a> ICodec for &'a [u8] {
    fn packed_size(&self) -> usize {
        self.len() + KEY_VAL_FIXED_BYTES
    }

    fn encode_to(&self, to: &mut [u8]) {
        let len = self.len();
        let (l, r) = to.split_at_mut(KEY_VAL_FIXED_BYTES);
        number_to_slice!(len as u32, l);
        r.copy_from_slice(self);
    }

    fn decode_from(raw: ByteArray) -> Self {
        let len_raw = raw.to_slice(0, KEY_VAL_FIXED_BYTES);
        let len = slice_to_number!(len_raw, u32);
        raw.add(KEY_VAL_FIXED_BYTES).to_slice(0, len as usize)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Value<'a> {
    Put(&'a [u8]),
    Del,
}

/// NOTE: the real data is a Tuple, which contains worker_id and TxId
impl<'a> Value<'a> {
    pub fn len(&self) -> usize {
        match self {
            Value::Put(x) => x.len(),
            Value::Del => 0,
        }
    }

    pub fn is_del(&self) -> bool {
        match self {
            Self::Del => true,
            _ => false,
        }
    }

    pub fn put(&self) -> Option<&[u8]> {
        match self {
            Value::Put(x) => Some(x),
            _ => None,
        }
    }
}

/// NOTE: [`Index`] is also a subtype of [`Value`] in internal node, we can convert
/// [`Value::Put`] to [`Index`]
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

impl ICodec for Key<'_> {
    // TODO: varint encode
    fn packed_size(&self) -> usize {
        self.len() + KEY_VAL_FIXED_BYTES
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        let (len, key) = to.split_at_mut(KEY_VAL_FIXED_BYTES);
        number_to_slice!(self.len() as u32, len);
        let (lsn, raw) = key.split_at_mut(Self::LSN_LEN);
        number_to_slice!(self.lsn, lsn);
        raw.copy_from_slice(self.raw);
    }

    fn decode_from(data: ByteArray) -> Self {
        let len = data.to_mut_slice(0, KEY_VAL_FIXED_BYTES);
        let len = slice_to_number!(len, u32) as usize;
        let payload = data.add(KEY_VAL_FIXED_BYTES);
        let payload = payload.to_slice(0, len);
        let (lsn, raw) = payload.split_at(Self::LSN_LEN);

        Self {
            raw,
            lsn: slice_to_number!(lsn, u64),
        }
    }
}

const VALUE_PUT_BIT: u8 = 7;
const VALUE_DEL_BIT: u8 = 11;

impl ICodec for Value<'_> {
    /// using extra 1 byte for Put or Del, since value can be empty(zero length slice)
    fn packed_size(&self) -> usize {
        1 + KEY_VAL_FIXED_BYTES + self.len()
    }

    fn encode_to(&self, to: &mut [u8]) {
        debug_assert_eq!(to.len(), self.packed_size());
        let (len, payload) = to.split_at_mut(KEY_VAL_FIXED_BYTES);
        number_to_slice!((self.len()) as u32, len);
        match self {
            Value::Put(x) => {
                let (id, payload) = payload.split_at_mut(1);
                id[0] = VALUE_PUT_BIT;
                payload.copy_from_slice(x);
            }
            Value::Del => {
                payload[0] = VALUE_DEL_BIT;
            }
        }
    }

    fn decode_from(raw: ByteArray) -> Self {
        let len = raw.to_mut_slice(0, KEY_VAL_FIXED_BYTES);
        let state = raw.to_mut_slice(KEY_VAL_FIXED_BYTES as isize, 1);
        let state = slice_to_number!(state, u8);
        let payload_off = KEY_VAL_FIXED_BYTES + 1;

        match state {
            VALUE_DEL_BIT => Value::Del,
            VALUE_PUT_BIT => {
                let len = slice_to_number!(len, u32) as usize;
                Value::Put(unsafe {
                    std::slice::from_raw_parts(raw.offset(payload_off as isize), len)
                })
            }
            _ => unreachable!("invalid state {}", state),
        }
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
        // to.copy_from_slice(&self.ptr.to_le_bytes());
    }

    fn decode_from(raw: ByteArray) -> Self {
        debug_assert!(raw.len() >= 16);
        // let mut bytes = [0u8; size_of::<u64>()];
        // bytes.copy_from_slice(unsafe { std::slice::from_raw_parts(raw.data(), raw.len()) });
        let s = raw.to_mut_slice(0, Self::len());
        let (pid, epoch) = s.split_at(8);

        Self {
            // ptr: u64::from_le_bytes(bytes),
            pid: slice_to_number!(pid, u64),
            epoch: slice_to_number!(epoch, u64),
        }
    }
}

impl IKey for Key<'_> {
    fn raw(&self) -> &[u8] {
        self.raw
    }

    fn separator(&self) -> Self {
        Key::new(self.raw, self.lsn)
    }
}

impl Ord for Value<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        let (l, r) = unsafe {
            let l = self as *const Self as *const u8;
            let r = other as *const Self as *const u8;
            let sz = size_of::<Self>();
            (
                std::slice::from_raw_parts(l, sz),
                std::slice::from_raw_parts(r, sz),
            )
        };
        l.cmp(&r)
    }
}

impl PartialOrd for Value<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl IVal for Value<'_> {
    fn to_string(&self) -> String {
        match self {
            Value::Del => "Del".into(),
            Value::Put(x) => std::str::from_utf8(x).unwrap().into(),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{tree::traits::ICodec, utils::byte_array::ByteArray};

    use super::{Index, Key, Value};

    #[test]
    fn test_key_codec() {
        let key = Key::new("moha".as_bytes(), 114514);
        let mut buf = vec![0u8; key.packed_size()];

        key.encode_to(&mut buf);

        let b = ByteArray::new(buf.as_mut_ptr(), buf.len());
        let dk = Key::decode_from(b);

        assert_eq!(dk.raw, key.raw);
        assert_eq!(dk.lsn, key.lsn);
    }

    #[test]
    fn test_val_codec() {
        let put = Value::Put("114514".as_bytes());
        let del = Value::Del;
        let mut put_buf = vec![0u8; put.packed_size()];
        let mut del_buf = vec![0u8; del.packed_size()];

        put.encode_to(&mut put_buf);
        del.encode_to(&mut del_buf);

        let pb = ByteArray::new(put_buf.as_mut_ptr(), put_buf.len());
        let db = ByteArray::new(del_buf.as_mut_ptr(), del_buf.len());

        let dp = Value::decode_from(pb);
        let dd = Value::decode_from(db);

        assert_eq!(dp, put);
        assert_eq!(dd, del);
    }

    #[test]
    fn test_u8_codec() {
        let x = "+1s".as_bytes();
        let mut buf = vec![0u8; x.packed_size()];

        x.encode_to(&mut buf);

        let xb = ByteArray::new(buf.as_mut_ptr(), buf.len());

        let dx = <&[u8] as ICodec>::decode_from(xb);

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

        let k = [].as_slice();
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
}
