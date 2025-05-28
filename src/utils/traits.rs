use crate::utils::bytes::ByteArray;

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

pub trait IInfer {
    fn infer(&self) -> ByteArray;
}

pub trait IDataLoader: Clone {
    type Out: Clone + IInfer;

    fn load_data(&self, addr: u64) -> Self::Out;
}

pub trait ICollector {
    type Input: Clone + IInfer;

    fn collect(&mut self, x: Self::Input);
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
