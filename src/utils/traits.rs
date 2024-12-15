use crate::utils::byte_array::ByteArray;

pub trait IPageIter: Iterator {
    fn rewind(&mut self);
}

pub trait IKey: ICodec + Clone + Copy + Ord {
    fn raw(&self) -> &[u8];

    fn to_string(&self) -> String;
}

pub trait ICodec {
    fn packed_size(&self) -> usize;

    fn encode_to(&self, to: &mut [u8]);

    fn decode_from(raw: ByteArray) -> Self;
}

pub trait IVal: ICodec + Copy + Clone {
    fn to_string(&self) -> String;
}

pub trait IValCodec: Copy {
    fn size(&self) -> usize;

    fn encode(&self, to: &mut [u8]);

    fn decode(from: &[u8]) -> Self;

    fn to_string(&self) -> String;

    fn data(&self) -> &[u8];
}
