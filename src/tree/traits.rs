use std::fmt::Debug;

use crate::utils::byte_array::ByteArray;

pub trait IPageIter: Iterator {
    fn rewind(&mut self);
}

pub trait IKey: ICodec + Clone + Copy + Ord {
    fn raw(&self) -> &[u8];

    fn separator(&self) -> Self;
}

pub trait ICodec {
    fn packed_size(&self) -> usize;

    fn encode_to(&self, to: &mut [u8]);

    fn decode_from(raw: ByteArray) -> Self;
}

pub trait IVal: ICodec + Clone + Copy + Ord + Debug {
    fn to_string(&self) -> String;
}
