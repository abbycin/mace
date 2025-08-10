use crate::types::{
    header::{DeltaHeader, TagKind},
    refbox::{BoxRef, DeltaView},
    traits::{IAlloc, ICodec, IHeader},
};

impl DeltaView {
    pub(crate) const HDR_LEN: usize = size_of::<DeltaHeader>();

    /// NOTE: the delta is always allocated togather and no indiraction
    pub(crate) fn from_key_val<A: IAlloc, K: ICodec, V: ICodec>(a: &mut A, k: K, v: V) -> BoxRef {
        let sz = k.packed_size() + v.packed_size() + Self::HDR_LEN;
        let d = a.allocate(sz);
        let mut view = d.view();
        view.header_mut().kind = TagKind::Delta;
        let mut delta = view.as_delta();

        *delta.header_mut() = DeltaHeader {
            klen: k.packed_size() as u32,
            vlen: v.packed_size() as u32,
        };
        k.encode_to(delta.key_mut());
        v.encode_to(delta.val_mut());
        d
    }

    fn data_ptr(&self, off: u64) -> *mut u8 {
        (self.0 as u64 + size_of::<DeltaHeader>() as u64 + off) as *mut u8
    }

    pub(crate) fn key(&self) -> &[u8] {
        let h = self.header();
        unsafe { std::slice::from_raw_parts(self.data_ptr(0), h.klen as usize) }
    }

    pub(crate) fn val(&self) -> &[u8] {
        let h = self.header();
        unsafe { std::slice::from_raw_parts(self.data_ptr(h.klen as u64), h.vlen as usize) }
    }

    pub(crate) fn key_mut(&mut self) -> &mut [u8] {
        let h = self.header();
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr(0), h.klen as usize) }
    }

    pub(crate) fn val_mut(&mut self) -> &mut [u8] {
        let h = self.header();
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr(h.klen as u64), h.vlen as usize) }
    }
}
