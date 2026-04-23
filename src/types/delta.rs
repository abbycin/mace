use crate::{
    types::{
        data::Val,
        header::{DeltaHeader, NodeType, RemoteHeader, TagKind},
        refbox::{BoxRef, DeltaView},
        traits::{IBoxHeader, ICodec, IFrameAlloc, IHeader, IKey, IVal},
    },
    utils::{NULL_ADDR, data::Position},
};

impl DeltaView {
    pub(crate) const HDR_LEN: usize = size_of::<DeltaHeader>();

    pub(crate) fn from_key_val<A: IFrameAlloc, K: IKey, V: IVal>(
        a: &mut A,
        k: &K,
        v: &V,
        group: u8,
        lsn: Position,
    ) -> (BoxRef, Option<BoxRef>) {
        let ksz = k.packed_size();
        let vsz = v.packed_size();
        let inline_size = a.inline_size();
        let is_remote = vsz > inline_size;

        let sz = Val::calc_size(false, inline_size, vsz);
        let lsn = lsn.max(a.checkpoint_lsn(group));
        let (mut d, remote) = if is_remote {
            // must alloc remote first
            let r = a.alloc((vsz + size_of::<RemoteHeader>()) as u32);
            let d = a.alloc((ksz + sz + Self::HDR_LEN) as u32);
            (d, Some(r))
        } else {
            (a.alloc((ksz + sz + Self::HDR_LEN) as u32), None)
        };
        let mut view = d.view();
        let h = view.header_mut();
        h.kind = TagKind::Delta;
        h.txid = k.txid();
        h.group = group;
        let mut delta = view.as_delta();
        *delta.header_mut() = DeltaHeader {
            klen: ksz as u32,
            vlen: sz as u32,
        };
        k.encode_to(delta.key_mut());
        if !is_remote {
            Val::encode_inline(delta.val_mut(), NULL_ADDR, v);
            d.header_mut().lsn = lsn;
            (d, None)
        } else {
            let mut r = remote.expect("remote allocation must exist");
            let h = r.header_mut();
            h.kind = TagKind::Remote;
            h.lsn = lsn;
            h.group = group;
            let mut view = r.view().as_remote();
            view.header_mut().size = vsz;
            v.encode_to(view.raw_mut());
            Val::encode_remote(delta.val_mut(), NULL_ADDR, view.addr(), v);
            (d, Some(r))
        }
    }

    pub(crate) fn from_key_index<A: IFrameAlloc, K: ICodec, V: ICodec>(
        a: &mut A,
        k: K,
        v: V,
        txid: u64,
        group: u8,
        lsn: Position,
    ) -> BoxRef {
        let sz = k.packed_size() + v.packed_size() + Self::HDR_LEN;
        let d = a.alloc(sz as u32);
        let lsn = lsn.max(a.checkpoint_lsn(group));
        let mut view = d.view();
        let h = view.header_mut();
        h.kind = TagKind::Delta;
        h.txid = txid;
        h.group = group;
        h.lsn = lsn;
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
