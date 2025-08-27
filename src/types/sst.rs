use std::{cmp::Ordering, marker::PhantomData};

use crate::{
    types::{
        header::{Slot, SlotType},
        refbox::RemoteView,
        traits::{ICodec, IKey, ILoader, IVal},
    },
    utils::{ADDR_LEN, raw_ptr_to_ref, unpack_id},
};

use super::header::BaseHeader;

/// the layout of sst:
/// ```text
/// +---------+-------------+-------------+---------+--------+----------+
/// | header  | remote addr | index table | low key | hi key |key-value |
/// +---------+-------------+-------------+---------+--------+----------+
/// ```
#[derive(Clone, Copy)]
pub(crate) struct Sst<K, V> {
    data: *mut BaseHeader,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Sst<K, V> {
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
        let slot_start = size_of::<BaseHeader>() + h.nr_remote as usize * ADDR_LEN;
        unsafe {
            let off = self
                .data
                .cast::<u8>()
                .add(slot_start)
                .cast::<SlotType>()
                .add(pos)
                .read() as usize;
            let p = self.data.cast::<u8>().add(off);
            std::slice::from_raw_parts(p, h.size as usize - off)
        }
    }
}

impl<K, V> Sst<K, V>
where
    K: Ord + ICodec,
    V: ICodec,
{
    fn get_remote<L>(l: &L, addr: u64) -> (K, V, Option<RemoteView>)
    where
        L: ILoader,
    {
        let mut p = l.pin_load(addr).as_remote();
        let data = p.raw_mut();
        let k = K::decode_from(data);
        let v = V::decode_from(&data[k.packed_size()..]);
        (k, v, Some(p))
    }

    pub(crate) fn key_at<L>(&self, l: &L, pos: usize) -> K
    where
        L: ILoader,
    {
        let raw = self.data_at(pos);
        let s = Slot::decode_from(raw);

        if s.is_inline() {
            K::decode_from(&raw[Slot::LOCAL_LEN..])
        } else {
            let mut p = l.pin_load(s.addr()).as_remote();
            K::decode_from(p.raw_mut())
        }
    }

    pub(crate) fn get_unchecked<L>(&self, l: &L, pos: usize) -> (K, V, Option<RemoteView>)
    where
        L: ILoader,
    {
        let raw = self.data_at(pos);
        let s = Slot::decode_from(raw);

        if s.is_inline() {
            let k = K::decode_from(&raw[Slot::LOCAL_LEN..]);
            let v = V::decode_from(&raw[Slot::LOCAL_LEN + k.packed_size()..]);
            (k, v, None)
        } else {
            Self::get_remote(l, s.addr())
        }
    }

    pub(crate) fn search_by<L, F>(&self, l: &L, key: &K, f: F) -> Result<usize, usize>
    where
        L: ILoader,
        F: Fn(&K, &K) -> Ordering,
    {
        let h = self.header();
        debug_assert!(h.elems > 0);
        let rk = key.remove_prefix(h.prefix_len as usize);
        let (mut lo, mut hi) = (0, h.elems as usize);

        while lo < hi {
            let mid = lo + ((hi - lo) >> 1);
            let k = self.key_at(l, mid);
            match f(&k, &rk) {
                Ordering::Equal => return Ok(mid),
                Ordering::Greater => hi = mid,
                Ordering::Less => lo = mid + 1,
            }
        }

        Err(lo)
    }

    pub(crate) fn lower_bound<L>(&self, l: &L, k: &K) -> Result<usize, usize>
    where
        L: ILoader,
    {
        let h = self.header();
        let elems = h.elems as usize;
        let rk = k.remove_prefix(h.prefix_len as usize);
        let (mut lo, mut hi) = (0, elems);

        while lo < hi {
            let mid = lo + ((hi - lo) >> 1);
            let key = self.key_at(l, mid);
            match key.cmp(&rk) {
                Ordering::Less => lo = mid + 1,
                _ => hi = mid,
            }
        }

        if lo == elems { Err(lo) } else { Ok(lo) }
    }
}

impl<K, V> Sst<K, V>
where
    K: IKey,
    V: IVal,
{
    pub(crate) fn show<L>(&self, l: &L, pid: u64, addr: u64)
    where
        L: ILoader,
    {
        let elems = self.header().elems as usize;
        log::debug!(
            "---------- show page {pid} {:?} elems {elems} ----------",
            unpack_id(addr),
        );
        for i in 0..elems {
            let (k, v, _) = self.get_unchecked(l, i);
            log::debug!("{} => {}", k.to_string(), v.to_string());
        }
        log::debug!("---------- end ----------");
    }
}
