use crate::{
    types::{
        refbox::RemoteView,
        traits::{IBoxHeader, IHeader},
    },
    utils::NULL_ADDR,
};

impl RemoteView {
    const TAG: u64 = 1 << 63;
    #[cfg(test)]
    pub fn alloc<A: crate::types::traits::IAlloc>(
        a: &mut A,
        size: usize,
    ) -> crate::types::refbox::BoxRef {
        use crate::types::header::{RemoteHeader, TagKind};

        let mut p = a.allocate((size + size_of::<RemoteHeader>()) as u32);
        p.header_mut().kind = TagKind::Remote;
        p.view().as_remote().header_mut().size = size;
        p
    }

    pub const fn tag(addr: u64) -> u64 {
        Self::TAG | addr
    }

    pub const fn is_tagged(addr: u64) -> bool {
        Self::TAG & addr != 0
    }

    pub const fn untagged(addr: u64) -> u64 {
        addr & !Self::TAG
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
}
