use crossbeam_epoch::Guard;

use crate::types::refbox::BoxRef;

pub trait IInlineSize {
    fn inline_size(&self) -> u32;
}

pub trait IAlloc: IInlineSize {
    fn allocate(&mut self, size: usize) -> BoxRef;

    fn recycle(&mut self, addr: &[u64]);

    fn arena_size(&mut self) -> u32;
}

pub trait IKey: Default + ICodec + Clone + Copy + Ord {
    fn raw(&self) -> &[u8];

    fn to_string(&self) -> String;
}

pub trait ICodec {
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

    fn data(&self) -> &[u8];
}

pub trait ITree {
    fn put<K, V>(&self, g: &Guard, k: K, v: V)
    where
        K: IKey,
        V: IVal;
}

/// NOTE: the type impl [`IAsSlice`] must be `packed(1)`
pub trait IAsSlice: Sized {
    fn as_slice(&self) -> &[u8] {
        let p = self as *const Self;
        unsafe { std::slice::from_raw_parts(p.cast(), size_of::<Self>()) }
    }

    fn from_slice(x: &[u8]) -> Self {
        assert_eq!(x.len(), size_of::<Self>());
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }
}

pub trait IHolder: Iterator {
    fn take_holder(&mut self) -> BoxRef;
}
