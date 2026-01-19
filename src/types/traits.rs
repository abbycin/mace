use crossbeam_epoch::Guard;

use crate::types::{
    header::BoxHeader,
    refbox::{BoxRef, BoxView},
};

pub trait ILoader: Clone {
    fn shallow_copy(&self) -> Self;

    fn pin(&self, data: BoxRef);

    fn load(&self, addr: u64) -> Option<BoxView>;

    fn load_remote(&self, addr: u64) -> Option<BoxRef>;

    fn load_remote_uncached(&self, _addr: u64) -> BoxRef {
        unimplemented!()
    }

    fn load_unchecked(&self, addr: u64) -> BoxView {
        self.load(addr).expect("must exist")
    }

    fn load_remote_unchecked(&self, addr: u64) -> BoxRef {
        self.load_remote(addr).expect("must exist")
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

pub trait IAlloc {
    fn allocate(&mut self, size: usize) -> BoxRef;

    fn collect(&mut self, addr: &[u64]);

    fn arena_size(&mut self) -> usize;

    fn inline_size(&self) -> usize;
}

pub trait IKey: Default + IKeyCodec + Ord {
    fn raw(&self) -> &[u8];

    fn txid(&self) -> u64;

    fn to_string(&self) -> String;
}

pub trait IDecode {
    fn decode_from(raw: &[u8]) -> Self;
}

pub trait ICodec: IDecode + Copy {
    fn packed_size(&self) -> usize;

    fn encode_to(&self, to: &mut [u8]);
}

pub trait IKeyCodec: ICodec {
    fn remove_prefix(&self, prefix_len: usize) -> Self;
}

pub trait IVal: ICodec + Clone {
    fn is_tombstone(&self) -> bool;

    fn worker(&self) -> u8 {
        unimplemented!()
    }
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

    fn len(&self) -> usize {
        size_of::<Self>()
    }
}
