use std::{cmp::Ordering, marker::PhantomData};

use crate::{
    types::{
        data::{Index, Val},
        header::SlotType,
        traits::{IDecode, IKey, IKeyCodec, ILoader},
    },
    utils::raw_ptr_to_ref,
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
        debug_assert!(!data.is_null());
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

    pub(crate) fn search_by<F>(&self, key: &K, f: F) -> Result<usize, usize>
    where
        F: Fn(&K, &K) -> Ordering,
    {
        let h = self.header();
        debug_assert!(h.elems > 0);
        let rk = key.remove_prefix(h.prefix_len as usize);
        let (mut lo, mut hi) = (0, h.elems as usize);

        while lo < hi {
            let mid = lo + ((hi - lo) >> 1);
            let k = self.key_at(mid);
            match f(&k, &rk) {
                Ordering::Equal => return Ok(mid),
                Ordering::Greater => hi = mid,
                Ordering::Less => lo = mid + 1,
            }
        }

        Err(lo)
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
