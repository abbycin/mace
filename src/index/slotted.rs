use std::cmp::Ordering;
use std::marker::PhantomData;

use crate::index::data::Value;
use crate::utils::byte_array::ByteArray;
use crate::utils::traits::{ICodec, IKey, IVal, IValCodec};
use crate::utils::OpCode;

use super::data::{Slot, SLOT_LEN};

#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub(crate) struct SlottedHeader {
    pub(crate) link: u64,
    pub(crate) elems: u32,
    page_size: u32,
    slot_offset: u32,
    data_offset: u32,
}

pub const HEADER_LEN: usize = size_of::<SlottedHeader>();

impl SlottedHeader {
    fn init(&mut self, page_size: u32) {
        self.link = 0;
        self.elems = 0;
        self.page_size = page_size;
        self.slot_offset = HEADER_LEN as u32;
        self.data_offset = page_size;
    }

    pub fn link(&self) -> u64 {
        self.link
    }
}

#[derive(Clone, Copy)]
pub struct SlottedPage<K, V> {
    raw: ByteArray,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> SlottedPage<K, V>
where
    K: IKey,
    V: IValCodec,
{
    pub(crate) fn new(raw: ByteArray) -> Self {
        assert!(raw.len() > HEADER_LEN);
        let mut this = Self {
            raw,
            _marker: PhantomData,
        };
        this.header_mut().init(raw.len() as u32);
        this
    }

    pub(crate) fn from(raw: ByteArray) -> Self {
        Self {
            raw,
            _marker: PhantomData,
        }
    }

    pub(crate) fn header_mut(&mut self) -> &mut SlottedHeader {
        unsafe { &mut *self.raw.data().cast::<SlottedHeader>() }
    }

    pub(crate) fn header(&self) -> &SlottedHeader {
        unsafe { &*self.raw.data().cast::<SlottedHeader>() }
    }

    fn build_slot(&self, pos: usize, off: u32, sep: u32, len: u32) -> &Slot {
        let offset = HEADER_LEN + pos * SLOT_LEN;
        let s = unsafe { &mut *self.raw.data().add(offset).cast::<Slot>() };
        s.reset(off, sep, len);
        self.slot(pos)
    }

    fn slot(&self, pos: usize) -> &Slot {
        let offset = HEADER_LEN + pos * SLOT_LEN;
        unsafe { &*self.raw.data().add(offset).cast::<Slot>() }
    }

    fn key_mut(&self, slot: &Slot) -> &mut [u8] {
        self.raw.as_mut_slice(slot.key_off(), slot.key_len())
    }

    fn value_mut(&self, slot: &Slot) -> &mut [u8] {
        self.raw.as_mut_slice(slot.val_off(), slot.val_len())
    }

    pub(crate) fn key_at(&self, pos: usize) -> K {
        let slot = self.slot(pos);
        K::decode_from(self.raw.sub_array(slot.key_off(), slot.key_len()))
    }

    #[cfg(test)]
    pub(crate) fn val_at(&self, pos: usize) -> Value<V> {
        let slot = self.slot(pos);
        Value::<V>::decode_from(self.raw.sub_array(slot.val_off(), slot.val_len()))
    }

    pub(crate) fn get(&self, pos: usize) -> (K, Value<V>) {
        debug_assert!(pos < self.header().elems as usize);
        let slot = self.slot(pos);
        let k = K::decode_from(self.raw.sub_array(slot.key_off(), slot.key_len()));
        let v = Value::<V>::decode_from(self.raw.sub_array(slot.val_off(), slot.val_len()));
        (k, v)
    }

    pub(crate) fn available(&self, size: usize) -> bool {
        let req_size = (SLOT_LEN + size) as u32;
        let h = self.header();
        h.data_offset - h.slot_offset >= req_size
    }

    /// NOTE: no duplication is allowed
    pub(crate) fn lower_bound(&self, key: &K) -> Result<usize, usize> {
        let cnt = self.header().elems as usize;
        let mut lo: usize = 0;
        let mut hi: usize = cnt;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.key_at(mid).cmp(key) {
                Ordering::Less => lo = mid + 1,
                _ => hi = mid,
            }
        }

        if lo == cnt {
            Err(lo)
        } else {
            Ok(lo)
        }
    }

    /// NOTE: key must be unique and ordered
    pub(crate) fn insert(&mut self, key: K, val: Value<V>) -> Result<(), OpCode> {
        let size = (key.packed_size() + val.packed_size()) as u32;
        if !self.available(size as usize) {
            return Err(OpCode::NoSpace);
        }
        let hdr = self.header();
        let pos = hdr.elems as usize;
        let slot = self.build_slot(pos, hdr.data_offset - size, key.packed_size() as u32, size);

        key.encode_to(self.key_mut(slot));
        val.encode_to(self.value_mut(slot));

        // update stats
        let hdr = self.header_mut();
        hdr.elems += 1;
        hdr.data_offset -= size;
        hdr.slot_offset += SLOT_LEN as u32;
        Ok(())
    }

    #[allow(unused)]
    pub(crate) fn show(&self) {
        let h = self.header();

        for i in 0..h.elems as usize {
            let (k, v) = self.get(i);
            log::debug!("{} => {}", k.to_string(), v.to_string());
        }
    }
}

#[cfg(test)]
mod test {

    use crate::{cc::data::Ver, index::data::Value, utils::block::Block};

    use super::SlottedPage;

    #[test]
    fn slotted_page() {
        let b = Block::alloc(128);
        let mut page = SlottedPage::<&[u8], &[u8]>::new(b.view(0, b.len()));

        page.insert("1".as_bytes(), Value::Put("6".as_bytes()))
            .unwrap();
        page.insert("2".as_bytes(), Value::Put("7".as_bytes()))
            .unwrap();
        page.insert("3".as_bytes(), Value::Put("8".as_bytes()))
            .unwrap();
        page.insert("4".as_bytes(), Value::Put("9".as_bytes()))
            .unwrap();
        let r = page.insert("5".as_bytes(), Value::Put("5".as_bytes()));
        assert!(r.is_err());

        assert_eq!(page.header().elems, 4);

        let pos = page.lower_bound(&"1".as_bytes()).unwrap();
        assert_eq!(*page.val_at(pos).as_ref(), "6".as_bytes());

        let x = page.lower_bound(&"5".as_bytes()).unwrap_or_else(|x| x);
        assert_eq!(x, 4);

        let b = Block::alloc(512);
        let mut page = SlottedPage::<Ver, &[u8]>::new(b.view(0, b.len()));

        page.insert(Ver::new(4, 2), Value::Put("42".as_bytes()))
            .unwrap();
        page.insert(Ver::new(4, 1), Value::Put("41".as_bytes()))
            .unwrap();
        page.insert(Ver::new(3, 3), Value::Put("33".as_bytes()))
            .unwrap();
        page.insert(Ver::new(3, 2), Value::Put("32".as_bytes()))
            .unwrap();
        page.insert(Ver::new(3, 1), Value::Put("31".as_bytes()))
            .unwrap();
        page.insert(Ver::new(1, 1), Value::Put("11".as_bytes()))
            .unwrap();

        let x = page
            .lower_bound(&Ver::new(3, u32::MAX))
            .unwrap_or_else(|x| x);
        assert_eq!(x, 2);

        let x = page
            .lower_bound(&Ver::new(2, u32::MAX))
            .unwrap_or_else(|x| x);
        assert_eq!(x, 5);
    }
}
