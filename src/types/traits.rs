use crossbeam_epoch::Guard;

use crate::types::{
    header::BoxHeader,
    refbox::{BoxRef, BoxView},
};

pub trait IInlineSize {
    fn inline_size(&self) -> u32;
}

pub trait ILoader: IInlineSize + Clone {
    fn shallow_copy(&self) -> Self;

    fn pin(&self, data: BoxRef);

    fn pin_load(&self, addr: u64) -> Option<BoxView>;

    fn pin_load_unchecked(&self, addr: u64) -> BoxView {
        self.pin_load(addr).expect("must exist")
    }
}

pub trait IAsBoxRef {
    fn as_box(&self) -> BoxRef;
}

pub trait IBoxHeader {
    fn box_header(&self) -> &BoxHeader;

    fn box_header_mut(&mut self) -> &mut BoxHeader;
}

pub trait IHeader<T> {
    fn header(&self) -> &T;

    fn header_mut(&mut self) -> &mut T;
}

pub trait IAlloc: IInlineSize {
    fn allocate(&mut self, size: usize) -> BoxRef;

    fn collect(&mut self, addr: &[u64]);

    fn arena_size(&mut self) -> u32;
}

pub trait IKey: Default + ICodec + Clone + Copy + Ord {
    fn raw(&self) -> &[u8];

    fn to_string(&self) -> String;
}

pub trait ICodec {
    fn remove_prefix(&self, prefix_len: usize) -> Self;

    fn packed_size(&self) -> usize;

    fn encode_to(&self, to: &mut [u8]);

    fn decode_from(raw: &[u8]) -> Self;
}

pub trait IVal: ICodec + Copy + Clone + Default {
    fn to_string(&self) -> String;

    fn is_tombstone(&self) -> bool;
}

pub trait IValCodec: Copy {
    fn size(&self) -> usize;

    fn encode(&self, to: &mut [u8]);

    fn decode(from: &[u8]) -> Self;

    fn to_string(&self) -> String;
}

pub trait ITree {
    fn put<K, V>(&self, g: &Guard, k: K, v: V)
    where
        K: IKey,
        V: IVal;
}

pub trait IAsSlice: Sized {
    fn as_slice(&self) -> &[u8] {
        let p = self as *const Self;
        unsafe { std::slice::from_raw_parts(p.cast(), size_of::<Self>()) }
    }

    fn from_slice(x: &[u8]) -> Self {
        assert!(x.len() >= size_of::<Self>());
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }
}
