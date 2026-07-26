use crate::{hot_true, must_exist};
use std::{cmp::Ordering, marker::PhantomData};

use crate::{
    types::{
        data::{Index, IntlKey, Key, Val, Ver},
        header::SlotType,
        traits::{IDecode, IKey, IKeyCodec, ILoader},
    },
    utils::{raw_ptr_to_ref, varint::Varint32},
};

use super::header::BaseHeader;

/// the layout of sst:
/// ```text
/// +---------+-------------+---------+--------+----------+
/// | header  | index table | low key | hi key |key-value |
/// +---------+-------------+---------+--------+----------+
/// ```

#[derive(Clone, Copy)]
pub(crate) struct Sst<K> {
    data: *mut BaseHeader,
    _marker: PhantomData<K>,
}

impl<K> Sst<K> {
    pub(crate) fn new(data: *mut BaseHeader) -> Self {
        hot_true!(!data.is_null());
        Self {
            data,
            _marker: PhantomData,
        }
    }

    pub(crate) fn header(&self) -> &BaseHeader {
        raw_ptr_to_ref(self.data)
    }

    fn data_at(&self, pos: usize) -> &[u8] {
        let h = self.header();
        unsafe {
            let off = self.data.add(1).cast::<SlotType>().add(pos).read() as usize;
            let p = self.data.cast::<u8>().add(off);
            std::slice::from_raw_parts(p, h.size as usize - off)
        }
    }
}

impl<K> Sst<K>
where
    K: Ord + IKeyCodec,
{
    /// key is always inline
    pub(crate) fn key_at(&self, pos: usize) -> K {
        let raw = self.data_at(pos);
        K::decode_from(raw)
    }

    pub(crate) fn lower_bound(&self, k: &K) -> Result<usize, usize> {
        let h = self.header();
        let elems = h.elems as usize;
        let rk = k.remove_prefix(h.prefix_len as usize);
        let (mut lo, mut hi) = (0, elems);

        while lo < hi {
            let mid = lo + ((hi - lo) >> 1);
            let key = self.key_at(mid);
            match key.cmp(&rk) {
                Ordering::Less => lo = mid + 1,
                _ => hi = mid,
            }
        }

        if lo == elems { Err(lo) } else { Ok(lo) }
    }
}

impl Sst<IntlKey<'_>> {
    #[inline(always)]
    fn raw_key_at(&self, pos: usize) -> &[u8] {
        let raw = self.data_at(pos);
        let (len, n) = must_exist!(Varint32::decode(raw), "invalid internal key");
        &raw[n..n + len as usize]
    }

    #[inline(always)]
    pub(crate) fn pid_at(&self, pos: usize) -> u64 {
        let raw = self.data_at(pos);
        let (len, n) = must_exist!(Varint32::decode(raw), "invalid internal key");
        Index::decode_from(&raw[n + len as usize..]).pid
    }

    pub(crate) fn floor_pid_by_raw(&self, key: &[u8]) -> Option<(usize, u64)> {
        let h = self.header();
        if h.elems == 0 {
            return None;
        }
        let prefix_len = h.prefix_len as usize;
        let rk = &key[prefix_len..];
        let (mut lo, mut hi) = (0, h.elems as usize);

        while lo < hi {
            let mid = lo + ((hi - lo) >> 1);
            match self.raw_key_at(mid).cmp(rk) {
                Ordering::Equal => return Some((mid, self.pid_at(mid))),
                Ordering::Greater => hi = mid,
                Ordering::Less => lo = mid + 1,
            }
        }

        let pos = lo.max(1) - 1;
        Some((pos, self.pid_at(pos)))
    }
}

impl Sst<Key<'_>> {
    #[inline(always)]
    fn raw_key_at(&self, pos: usize) -> &[u8] {
        let raw = self.data_at(pos);
        let (len, n) = must_exist!(Varint32::decode(raw), "invalid leaf key");
        let key = &raw[n..n + len as usize];
        &key[Ver::len()..]
    }

    #[inline(always)]
    pub(crate) fn ver_val_at(self, pos: usize) -> (Ver, Val<'static>) {
        let raw = self.data_at(pos);
        let (len, n) = must_exist!(Varint32::decode(raw), "invalid leaf key");
        let key = &raw[n..n + len as usize];
        let ver = Ver::decode_from(&key[..Ver::len()]);
        let val = Val::decode_from(&raw[n + len as usize..]);
        (ver, val)
    }

    pub(crate) fn search_ver_val_by_raw(&self, key: &[u8]) -> Option<(Ver, Val<'static>)> {
        let h = self.header();
        if h.elems == 0 {
            return None;
        }
        let prefix_len = h.prefix_len as usize;
        let rk = &key[prefix_len..];
        let (mut lo, mut hi) = (0, h.elems as usize);

        while lo < hi {
            let mid = lo + ((hi - lo) >> 1);
            match self.raw_key_at(mid).cmp(rk) {
                Ordering::Equal => return Some(self.ver_val_at(mid)),
                Ordering::Greater => hi = mid,
                Ordering::Less => lo = mid + 1,
            }
        }

        None
    }
}

impl<K> Sst<K>
where
    K: IKey,
{
    pub fn kv_at<V: IDecode>(&self, pos: usize) -> (K, V) {
        let raw = self.data_at(pos);
        let k = K::decode_from(raw);
        let v = V::decode_from(&raw[k.packed_size()..]);
        (k, v)
    }

    pub(crate) fn show_intl(&self, pid: u64, addr: u64) {
        let elems = self.header().elems as usize;
        log::debug!("---------- show page {pid} {addr} elems {elems} ----------");
        for i in 0..elems {
            let (k, v) = self.kv_at::<Index>(i);
            log::debug!("{} => {}", k.to_string(), v);
        }
        log::debug!("---------- end ----------");
    }

    pub(crate) fn show_leaf<L: ILoader>(&self, l: &L, pid: u64, addr: u64) {
        let elems = self.header().elems as usize;
        log::debug!("---------- show page {pid} {addr} elems {elems} ----------");
        for i in 0..elems {
            let (k, v) = self.kv_at::<Val>(i);
            let (r, _) = v.get_record(l);
            log::debug!("{} => {}", k.to_string(), r);
        }
        log::debug!("---------- end ----------");
    }
}
