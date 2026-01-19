use crate::{
    types::{
        data::Val,
        header::{DeltaHeader, NodeType, TagKind},
        refbox::{BoxRef, DeltaView, RemoteView},
        traits::{IAlloc, IBoxHeader, ICodec, IHeader, IKey, IVal},
    },
    utils::NULL_ADDR,
};

impl DeltaView {
    pub(crate) const HDR_LEN: usize = size_of::<DeltaHeader>();

    pub(crate) fn from_key_val<A: IAlloc, K: IKey, V: IVal>(
        a: &mut A,
        k: &K,
        v: &V,
    ) -> (BoxRef, Option<BoxRef>) {
        let ksz = k.packed_size();
        let vsz = v.packed_size();
        let inline_size = a.inline_size();
        let is_remote = vsz > inline_size;

        let sz = Val::calc_size(false, inline_size, vsz);
        let d = a.allocate(ksz + sz + Self::HDR_LEN);
        let mut view = d.view();
        let h = view.header_mut();
        h.kind = TagKind::Delta;
        h.txid = k.txid();
        let mut delta = view.as_delta();
        *delta.header_mut() = DeltaHeader {
            klen: ksz as u32,
            vlen: sz as u32,
        };
        k.encode_to(delta.key_mut());
        if !is_remote {
            Val::encode_inline(delta.val_mut(), NULL_ADDR, v);
            (d, None)
        } else {
            let r = RemoteView::alloc(a, vsz);
            let mut view = r.view().as_remote();
            v.encode_to(view.raw_mut());
            Val::encode_remote(delta.val_mut(), NULL_ADDR, view.addr(), v);
            (d, Some(r))
        }
    }

    pub(crate) fn from_key_index<A: IAlloc, K: ICodec, V: ICodec>(
        a: &mut A,
        k: K,
        v: V,
        txid: u64,
    ) -> BoxRef {
        let sz = k.packed_size() + v.packed_size() + Self::HDR_LEN;
        let d = a.allocate(sz);
        let mut view = d.view();
        let h = view.header_mut();
        h.kind = TagKind::Delta;
        h.txid = txid;
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

    pub(crate) fn val<'a>(&self) -> Val<'a> {
        debug_assert_eq!(self.box_header().node_type, NodeType::Leaf);
        let h = self.header();
        let p = self.data_ptr(h.klen as u64);
        let v = unsafe { std::slice::from_raw_parts(p, h.vlen as usize) };
        Val::from_raw(v)
    }

    /// for Index only
    pub(crate) fn index(&self) -> &[u8] {
        debug_assert_eq!(self.box_header().node_type, NodeType::Intl);
        let h = self.header();
        unsafe { std::slice::from_raw_parts(self.data_ptr(h.klen as u64), h.vlen as usize) }
    }

    fn key_mut(&mut self) -> &mut [u8] {
        let h = self.header();
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr(0), h.klen as usize) }
    }

    fn val_mut(&mut self) -> &mut [u8] {
        let h = self.header();
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr(h.klen as u64), h.vlen as usize) }
    }
}
