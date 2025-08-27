use crate::types::{
    header::{RemoteHeader, TagKind},
    refbox::{BoxRef, RemoteView},
    traits::{IAlloc, IHeader},
};

impl RemoteView {
    pub fn alloc<A: IAlloc>(a: &mut A, size: usize) -> BoxRef {
        let mut p = a.allocate(size + size_of::<RemoteHeader>());
        p.header_mut().kind = TagKind::Remote;
        p.view().as_remote().header_mut().size = size;
        p
    }

    pub fn raw<'a>(&self) -> &'a [u8] {
        let n = self.header().size;
        unsafe { std::slice::from_raw_parts(self.0.add(1).cast::<_>(), n) }
    }

    pub fn raw_mut(&mut self) -> &mut [u8] {
        let n = self.header().size;
        unsafe { std::slice::from_raw_parts_mut(self.0.add(1).cast::<_>(), n) }
    }
}
