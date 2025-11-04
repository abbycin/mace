use std::ptr::null_mut;

use crate::{
    types::{
        header::{RemoteHeader, TagKind},
        refbox::{BoxRef, RemoteView},
        traits::{IAlloc, IBoxHeader, IHeader},
    },
    utils::NULL_ADDR,
};

impl RemoteView {
    pub fn alloc<A: IAlloc>(a: &mut A, size: usize) -> BoxRef {
        let mut p = a.allocate(size + size_of::<RemoteHeader>());
        p.header_mut().kind = TagKind::Remote;
        p.view().as_remote().header_mut().size = size;
        p
    }

    pub fn null() -> Self {
        Self(null_mut())
    }

    pub fn raw<'a>(&self) -> &'a [u8] {
        let n = self.header().size;
        unsafe { std::slice::from_raw_parts(self.0.add(1).cast::<_>(), n) }
    }

    pub fn raw_mut(&mut self) -> &mut [u8] {
        let n = self.header().size;
        unsafe { std::slice::from_raw_parts_mut(self.0.add(1).cast::<_>(), n) }
    }

    pub fn addr(&self) -> u64 {
        self.map_or(NULL_ADDR, |x| x.box_header().addr)
    }

    #[inline]
    pub fn map_or<F, T>(&self, default: T, other: F) -> T
    where
        F: Fn(&Self) -> T,
    {
        if self.0.is_null() {
            default
        } else {
            other(self)
        }
    }

    #[inline]
    pub fn map_or_else<F1, F2, T>(&self, default: F1, other: F2) -> T
    where
        F1: Fn() -> T,
        F2: Fn(&Self) -> T,
    {
        if self.0.is_null() {
            default()
        } else {
            other(self)
        }
    }
}
