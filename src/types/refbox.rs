use crate::hot_true;
use crate::types::header::TagKind;
use crate::utils::data::Position;
use crate::utils::{NULL_ADDR, NULL_PID, OpCode, raw_ptr_to_ref, raw_ptr_to_ref_mut};
use crate::{must_ok, static_assert};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU32;
use std::{
    alloc::{Layout, alloc, dealloc},
    sync::atomic::Ordering::{AcqRel, Relaxed},
};

use super::header::{
    BaseHeader, BoxHeader, DeltaHeader, DiskBaseHeader, DiskBoxHeader, DiskDeltaHeader,
    DiskRemoteHeader, NodeType, PersistedPayloadHeaderV1, RemoteHeader, TagFlag,
};
use super::traits::{IAsBoxRef, IAsSlice, IBoxHeader, IHeader};
static_assert!(BoxRef::HDR_LEN == 64);
static_assert!(align_of::<BoxHeader>() == align_of::<*const ()>());

pub struct BoxRef(*mut BoxHeader);

unsafe impl Send for BoxRef {}
unsafe impl Sync for BoxRef {}

#[derive(Clone, Copy)]
pub struct BoxView(*mut BoxHeader);

#[derive(Clone, Copy)]
pub(crate) struct DeltaView(pub(super) *mut DeltaHeader);

/// both base node and sibling share same layout
#[derive(Clone, Copy)]
pub(crate) struct BaseView(pub(super) *mut BaseHeader);

/// used for large key-val
#[derive(Clone, Copy)]
pub(crate) struct RemoteView(pub(super) *mut RemoteHeader);

impl Deref for BoxView {
    type Target = BoxHeader;

    fn deref(&self) -> &Self::Target {
        raw_ptr_to_ref(self.0)
    }
}

impl DerefMut for BoxView {
    fn deref_mut(&mut self) -> &mut Self::Target {
        raw_ptr_to_ref_mut(self.0)
    }
}

impl BoxView {
    pub(crate) fn inc_ref(&self) {
        raw_ptr_to_ref_mut(self.0).refs.fetch_add(1, Relaxed);
    }

    pub(crate) fn to_box(self) -> BoxRef {
        self.inc_ref();
        BoxRef(self.0)
    }

    fn dec_ref(&self) -> u32 {
        raw_ptr_to_ref_mut(self.0).refs.fetch_sub(1, AcqRel)
    }

    fn cast_to<T>(&self) -> *mut T {
        unsafe { self.0.add(1).cast::<T>() }
    }

    pub(crate) fn as_delta(&self) -> DeltaView {
        match self.header().kind {
            TagKind::Delta => DeltaView(self.cast_to::<_>()),
            _ => unreachable!("invalid kind {:?}", self.header()),
        }
    }

    pub(crate) fn as_base(&self) -> BaseView {
        match self.header().kind {
            TagKind::Base => BaseView(self.cast_to::<_>()),
            _ => unreachable!("invalid kind {:?}", self.header()),
        }
    }

    pub(crate) fn as_remote(&self) -> RemoteView {
        match self.header().kind {
            TagKind::Remote => RemoteView(self.cast_to::<_>()),
            _ => unreachable!("invalid kind {:?}", self.header()),
        }
    }
}

impl BoxRef {
    pub(crate) const HDR_LEN: usize = size_of::<BoxHeader>();
    pub(crate) const DUMP_HDR_LEN: usize = size_of::<BoxHeader>() - size_of::<AtomicU32>();
    pub(crate) const MAX_PERSISTED_HEAD_LEN: usize = Self::DUMP_HDR_LEN + size_of::<BaseHeader>();

    pub(crate) const fn real_size(size: u32) -> u32 {
        Self::HDR_LEN as u32 + size
    }

    pub(crate) fn total_size(&self) -> u32 {
        let h = self.header();
        hot_true!(eq Self::real_size(h.payload_size), h.total_size);
        h.total_size
    }

    pub(crate) fn alloc_exact(size: u32, addr: u64) -> BoxRef {
        let layout = must_ok!(Layout::from_size_align(size as usize, 8));
        let mut this = BoxRef(unsafe { alloc(layout).cast::<_>() });
        #[cfg(feature = "extra_check")]
        assert_eq!(this.0 as usize % 8, 0);
        let h = this.header_mut();
        h.total_size = size;
        h.payload_size = size - Self::HDR_LEN as u32;
        h.flag = TagFlag::Normal;
        h.group = 0;
        h.pid = NULL_PID;
        h.txid = 0;
        h.addr = addr;
        h.link = NULL_ADDR;
        h.lsn = Position::MIN;
        h.refs.store(1, Relaxed);
        this
    }

    /// NOTE: the alignment is hard code to pointer's alignment, and it's true in mace
    pub(crate) fn alloc(size: u32, addr: u64) -> BoxRef {
        Self::alloc_exact(Self::real_size(size), addr)
    }

    pub(crate) fn with_persisted_parts<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&[u8], Option<&[u8]>) -> T,
    {
        let mut head = [0u8; Self::MAX_PERSISTED_HEAD_LEN];
        let head_len = self.encode_persisted_head(&mut head);
        let body = self.persisted_body();
        let body = if body.is_empty() { None } else { Some(body) };
        f(&head[..head_len], body)
    }

    pub(crate) fn real_size_from_dump(size: u32) -> u32 {
        size + size_of::<AtomicU32>() as u32
    }

    pub(crate) fn dump_len(&self) -> usize {
        Self::DUMP_HDR_LEN + self.persisted_payload_len()
    }

    pub(crate) fn load_slice<'a>(&mut self, dump_len: usize) -> &'a mut [u8] {
        let off = size_of::<AtomicU32>();
        let len = dump_len;
        unsafe { std::slice::from_raw_parts_mut(self.0.cast::<u8>().add(off), len) }
    }

    fn persisted_payload_len(&self) -> usize {
        let h = self.header();
        if h.kind == TagKind::Base && h.node_type == NodeType::Leaf {
            let base = self.view().as_base();
            let logical_payload = base.header().size as usize;
            if h.payload_size as usize > logical_payload {
                return logical_payload;
            }
        }
        h.payload_size as usize
    }

    fn encode_box_header(&self, payload_size: u32, out: &mut [u8; Self::DUMP_HDR_LEN]) {
        let src = self.header();
        let node_type = match src.kind {
            TagKind::Remote => NodeType::Leaf,
            _ => src.node_type,
        };
        let flag = match src.kind {
            TagKind::Base => src.flag,
            _ => TagFlag::Normal,
        };
        let hdr = DiskBoxHeader {
            kind: src.kind as u8,
            node_type: node_type as u8,
            flag: flag as u8,
            group: src.group,
            total_size: Self::HDR_LEN as u32 + payload_size,
            payload_size,
            pid: src.pid,
            txid: src.txid,
            addr: src.addr,
            link: src.link,
            lsn: src.lsn,
        };
        out.copy_from_slice(hdr.as_slice());
    }

    fn encode_persisted_head(&self, out: &mut [u8; Self::MAX_PERSISTED_HEAD_LEN]) -> usize {
        let payload_size = self.persisted_payload_len() as u32;
        let mut box_hdr = [0u8; Self::DUMP_HDR_LEN];
        self.encode_box_header(payload_size, &mut box_hdr);
        out[..Self::DUMP_HDR_LEN].copy_from_slice(&box_hdr);

        let payload_hdr_len = self.encode_payload_header(&mut out[Self::DUMP_HDR_LEN..]);
        Self::DUMP_HDR_LEN + payload_hdr_len
    }

    fn encode_payload_header(&self, out: &mut [u8]) -> usize {
        match self.header().kind {
            TagKind::Base => {
                let base = self.view().as_base();
                let base = base.header();
                let payload = DiskBaseHeader {
                    elems: base.elems,
                    split_elems: base.split_elems,
                    size: base.size,
                    right_sibling: base.right_sibling,
                    merging_child: base.merging_child,
                    lo_len: base.lo_len,
                    hi_len: base.hi_len,
                    prefix_len: base.prefix_len,
                    merging: u8::from(base.merging),
                    is_index: u8::from(base.is_index),
                    has_multiple_versions: u8::from(base.has_multiple_versions),
                    padding: base.padding,
                };
                let bytes = payload.as_slice();
                out[..bytes.len()].copy_from_slice(bytes);
                bytes.len()
            }
            TagKind::Delta => {
                let delta = self.view().as_delta();
                let delta = delta.header();
                let payload = DiskDeltaHeader {
                    klen: delta.klen,
                    vlen: delta.vlen,
                };
                let bytes = payload.as_slice();
                out[..bytes.len()].copy_from_slice(bytes);
                bytes.len()
            }
            TagKind::Remote => {
                let remote = self.view().as_remote();
                let remote = remote.header();
                let payload = DiskRemoteHeader { size: remote.size };
                let bytes = payload.as_slice();
                out[..bytes.len()].copy_from_slice(bytes);
                bytes.len()
            }
        }
    }

    fn payload_header_len(&self) -> usize {
        match self.header().kind {
            TagKind::Base => size_of::<BaseHeader>(),
            TagKind::Delta => size_of::<DeltaHeader>(),
            TagKind::Remote => size_of::<RemoteHeader>(),
        }
    }

    fn persisted_body(&self) -> &[u8] {
        let payload_len = self.persisted_payload_len();
        let hdr_len = self.payload_header_len();
        let payload = &self.data_slice::<u8>()[..payload_len];
        &payload[hdr_len..]
    }

    fn parse_persisted_v1(
        raw: &[u8],
        expected_addr: u64,
    ) -> Result<
        (
            DiskBoxHeader,
            TagKind,
            NodeType,
            TagFlag,
            PersistedPayloadHeaderV1,
        ),
        OpCode,
    > {
        if raw.len() < Self::DUMP_HDR_LEN {
            return Err(OpCode::Corruption);
        }
        let box_hdr = DiskBoxHeader::from_slice(raw);
        let kind = match box_hdr.kind {
            x if x == TagKind::Delta as u8 => TagKind::Delta,
            x if x == TagKind::Base as u8 => TagKind::Base,
            x if x == TagKind::Remote as u8 => TagKind::Remote,
            _ => return Err(OpCode::Corruption),
        };
        let node_type = match kind {
            TagKind::Remote if box_hdr.node_type == NodeType::Leaf as u8 => NodeType::Leaf,
            TagKind::Remote => return Err(OpCode::Corruption),
            _ => match box_hdr.node_type {
                x if x == NodeType::Leaf as u8 => NodeType::Leaf,
                x if x == NodeType::Intl as u8 => NodeType::Intl,
                _ => return Err(OpCode::Corruption),
            },
        };
        let flag = match (kind, box_hdr.flag) {
            (_, x) if x == TagFlag::Normal as u8 => TagFlag::Normal,
            (TagKind::Base, x) if x == TagFlag::Sibling as u8 => TagFlag::Sibling,
            _ => return Err(OpCode::Corruption),
        };
        if box_hdr.addr != expected_addr
            || box_hdr.total_size != Self::real_size(box_hdr.payload_size)
            || box_hdr.total_size != Self::real_size_from_dump(raw.len() as u32)
        {
            return Err(OpCode::Corruption);
        }

        let payload = &raw[Self::DUMP_HDR_LEN..];
        if payload.len() != box_hdr.payload_size as usize {
            return Err(OpCode::Corruption);
        }

        let payload_header = match kind {
            TagKind::Base => {
                if payload.len() < size_of::<DiskBaseHeader>() {
                    return Err(OpCode::Corruption);
                }
                let disk = DiskBaseHeader::from_slice(payload);
                if disk.merging > 1 || disk.is_index > 1 || disk.has_multiple_versions > 1 {
                    return Err(OpCode::Corruption);
                }
                PersistedPayloadHeaderV1::Base(disk)
            }
            TagKind::Delta => {
                if payload.len() < size_of::<DiskDeltaHeader>() {
                    return Err(OpCode::Corruption);
                }
                let disk = DiskDeltaHeader::from_slice(payload);
                PersistedPayloadHeaderV1::Delta(disk)
            }
            TagKind::Remote => {
                if payload.len() < size_of::<DiskRemoteHeader>() {
                    return Err(OpCode::Corruption);
                }
                let disk = DiskRemoteHeader::from_slice(payload);
                PersistedPayloadHeaderV1::Remote(disk)
            }
        };

        Ok((box_hdr, kind, node_type, flag, payload_header))
    }

    pub(crate) fn decode_persisted_v1_in_place(
        &mut self,
        expected_addr: u64,
        raw_len: usize,
    ) -> Result<(), OpCode> {
        let (box_hdr, kind, node_type, flag, payload_header) = {
            let raw = self.load_slice(raw_len);
            Self::parse_persisted_v1(raw, expected_addr)?
        };

        {
            let h = self.header_mut();
            h.kind = kind;
            h.node_type = node_type;
            h.flag = flag;
            h.group = box_hdr.group;
            h.total_size = box_hdr.total_size;
            h.payload_size = box_hdr.payload_size;
            h.pid = box_hdr.pid;
            h.txid = box_hdr.txid;
            h.addr = box_hdr.addr;
            h.link = box_hdr.link;
            h.lsn = box_hdr.lsn;
        }

        match payload_header {
            PersistedPayloadHeaderV1::Base(disk) => {
                let mut base = self.view().as_base();
                *base.header_mut() = BaseHeader {
                    elems: disk.elems,
                    split_elems: disk.split_elems,
                    size: disk.size,
                    right_sibling: disk.right_sibling,
                    merging_child: disk.merging_child,
                    lo_len: disk.lo_len,
                    hi_len: disk.hi_len,
                    prefix_len: disk.prefix_len,
                    merging: disk.merging != 0,
                    is_index: disk.is_index != 0,
                    has_multiple_versions: disk.has_multiple_versions != 0,
                    padding: disk.padding,
                };
            }
            PersistedPayloadHeaderV1::Delta(disk) => {
                let mut delta = self.view().as_delta();
                *delta.header_mut() = DeltaHeader {
                    klen: disk.klen,
                    vlen: disk.vlen,
                };
            }
            PersistedPayloadHeaderV1::Remote(disk) => {
                let mut remote = self.view().as_remote();
                *remote.header_mut() = RemoteHeader { size: disk.size };
            }
        }

        let hdr_len = match payload_header {
            PersistedPayloadHeaderV1::Base(_) => size_of::<BaseHeader>(),
            PersistedPayloadHeaderV1::Delta(_) => size_of::<DeltaHeader>(),
            PersistedPayloadHeaderV1::Remote(_) => size_of::<RemoteHeader>(),
        };
        let payload_len = box_hdr.payload_size as usize;
        if payload_len < hdr_len {
            return Err(OpCode::Corruption);
        }
        Ok(())
    }

    /// NOTE: for T is not u8, the caller **MUST** make sure T is aligned to pointer size
    pub(crate) fn data_slice<'a, T>(&self) -> &'a [T] {
        let h = self.header();
        let len = h.total_size as usize - Self::HDR_LEN;
        unsafe { std::slice::from_raw_parts(self.0.add(1).cast::<_>(), len / size_of::<T>()) }
    }

    #[cfg(test)]
    /// NOTE: for T is not u8, the caller **MUST** make sure T is aligned to pointer size
    pub(crate) fn data_slice_mut<'a, T>(&mut self) -> &'a mut [T] {
        let h = self.header();
        let len = h.total_size as usize - Self::HDR_LEN;
        unsafe { std::slice::from_raw_parts_mut(self.0.add(1).cast::<_>(), len / size_of::<T>()) }
    }

    pub(crate) fn view(&self) -> BoxView {
        BoxView(self.0)
    }
}

impl Clone for BoxRef {
    fn clone(&self) -> Self {
        hot_true!(!self.0.is_null());
        self.view().inc_ref();
        Self(self.0)
    }
}

impl Drop for BoxRef {
    fn drop(&mut self) {
        hot_true!(!self.0.is_null());
        let view = self.view();
        if view.dec_ref() == 1 {
            let layout = must_ok!(Layout::from_size_align(self.total_size() as usize, 8));
            let p = self.0 as *mut u8;
            unsafe { dealloc(p, layout) };
        }
    }
}

macro_rules! impl_box {
    ($x: ty) => {
        impl IAsBoxRef for $x {
            fn as_box(&self) -> BoxRef {
                hot_true!(!self.0.is_null());
                let x = BoxView(unsafe { (self.0 as *mut BoxHeader).sub(1) });
                x.to_box()
            }
        }
    };
}

macro_rules! impl_box_header {
    ($x: ty) => {
        impl IBoxHeader for $x {
            fn box_header(&self) -> &BoxHeader {
                unsafe { &*((self.0 as u64 - size_of::<BoxHeader>() as u64) as *const _) }
            }

            fn box_header_mut(&mut self) -> &mut BoxHeader {
                unsafe { &mut *((self.0 as u64 - size_of::<BoxHeader>() as u64) as *mut _) }
            }
        }
    };
}

macro_rules! impl_header {
    ($x: ty, $y: ty) => {
        impl IHeader<$y> for $x {
            fn header(&self) -> &$y {
                raw_ptr_to_ref(self.0)
            }

            fn header_mut(&mut self) -> &mut $y {
                raw_ptr_to_ref_mut(self.0)
            }
        }
    };
}

impl_box!(RemoteView);
impl_box!(DeltaView);
impl_box!(BaseView);

impl_header!(BoxRef, BoxHeader);

impl_header!(RemoteView, RemoteHeader);
impl_header!(BaseView, BaseHeader);
impl_header!(DeltaView, DeltaHeader);
impl_header!(BoxView, BoxHeader);

impl_box_header!(BaseView);
impl_box_header!(DeltaView);
impl_box_header!(RemoteView);

#[cfg(test)]
mod tests {
    use super::*;

    fn persisted_bytes(b: &BoxRef) -> Vec<u8> {
        let mut out = Vec::new();
        b.with_persisted_parts(|head, tail| {
            out.extend_from_slice(head);
            if let Some(body) = tail {
                out.extend_from_slice(body);
            }
        });
        out
    }

    fn prior_persisted_bytes(b: &BoxRef) -> Vec<u8> {
        let payload_len =
            if b.header().kind == TagKind::Base && b.header().node_type == NodeType::Leaf {
                let base = b.view().as_base();
                let logical_payload = base.header().size as usize;
                if b.header().payload_size as usize > logical_payload {
                    Some(logical_payload)
                } else {
                    None
                }
            } else {
                None
            };

        if let Some(payload_len) = payload_len {
            let mut out = Vec::with_capacity(BoxRef::DUMP_HDR_LEN + payload_len);
            let mut hdr = [0u8; BoxRef::DUMP_HDR_LEN];
            b.encode_box_header(payload_len as u32, &mut hdr);
            out.extend_from_slice(&hdr);
            out.extend_from_slice(&b.data_slice::<u8>()[..payload_len]);
            out
        } else {
            let p = b.0.cast::<u8>();
            let off = size_of::<AtomicU32>();
            let len = b.total_size() as usize - off;
            unsafe { std::slice::from_raw_parts(p.add(off), len) }.to_vec()
        }
    }

    fn make_delta(addr: u64, node_type: NodeType) -> BoxRef {
        let mut b = BoxRef::alloc((size_of::<DeltaHeader>() + 12) as u32, addr);
        let h = b.header_mut();
        h.kind = TagKind::Delta;
        h.node_type = node_type;
        let mut delta = b.view().as_delta();
        *delta.header_mut() = DeltaHeader { klen: 4, vlen: 8 };
        b.data_slice_mut::<u8>()[size_of::<DeltaHeader>()..].copy_from_slice(b"abcdefghijkl");
        b
    }

    fn make_remote(addr: u64) -> BoxRef {
        let mut b = BoxRef::alloc((size_of::<RemoteHeader>() + 10) as u32, addr);
        let h = b.header_mut();
        h.kind = TagKind::Remote;
        h.node_type = NodeType::Leaf;
        let mut remote = b.view().as_remote();
        remote.header_mut().size = 10;
        remote.raw_mut().copy_from_slice(b"0123456789");
        b
    }

    fn make_base(addr: u64, node_type: NodeType, persisted_payload: usize, slack: usize) -> BoxRef {
        let mut b = BoxRef::alloc(
            (size_of::<BaseHeader>() + persisted_payload + slack) as u32,
            addr,
        );
        let h = b.header_mut();
        h.kind = TagKind::Base;
        h.node_type = node_type;
        let mut base = b.view().as_base();
        let size = (size_of::<BaseHeader>() + persisted_payload) as u32;
        *base.header_mut() = BaseHeader {
            elems: 2,
            split_elems: 1,
            size,
            right_sibling: 9,
            merging_child: 7,
            lo_len: 3,
            hi_len: 5,
            prefix_len: 2,
            merging: true,
            is_index: node_type == NodeType::Intl,
            has_multiple_versions: node_type == NodeType::Leaf,
            padding: 0,
        };
        let payload = b.data_slice_mut::<u8>();
        let beg = size_of::<BaseHeader>();
        let end = beg + persisted_payload + slack;
        for (idx, byte) in payload[beg..end].iter_mut().enumerate() {
            *byte = (idx as u8).wrapping_add(1);
        }
        b
    }

    #[test]
    fn persisted_parts_match_prior_bytes() {
        let pages = [
            make_delta(1, NodeType::Leaf),
            make_delta(2, NodeType::Intl),
            make_remote(3),
            make_base(4, NodeType::Intl, 24, 0),
            make_base(5, NodeType::Leaf, 24, 16),
        ];

        for page in &pages {
            assert_eq!(persisted_bytes(page), prior_persisted_bytes(page));
        }
    }

    #[test]
    fn persisted_v1_validates_in_place() {
        let pages = [
            make_delta(11, NodeType::Leaf),
            make_delta(12, NodeType::Intl),
            make_remote(13),
            make_base(14, NodeType::Intl, 24, 0),
            make_base(15, NodeType::Leaf, 24, 16),
        ];

        for page in &pages {
            let raw = persisted_bytes(page);
            let addr = page.header().addr;
            let mut loaded =
                BoxRef::alloc_exact(BoxRef::real_size_from_dump(raw.len() as u32), addr);
            loaded.load_slice(raw.len()).copy_from_slice(&raw);
            loaded
                .decode_persisted_v1_in_place(addr, raw.len())
                .expect("persisted v1 bytes must validate in place");
            assert_eq!(persisted_bytes(&loaded), raw);
        }
    }

    #[test]
    fn remote_persisted_v1_is_canonicalized() {
        let mut page = make_remote(21);
        let h = page.header_mut();
        h.node_type = NodeType::Intl;
        h.flag = TagFlag::Sibling;

        let raw = persisted_bytes(&page);
        assert_eq!(raw[1], NodeType::Leaf as u8);
        assert_eq!(raw[2], TagFlag::Normal as u8);

        let mut loaded = BoxRef::alloc_exact(BoxRef::real_size_from_dump(raw.len() as u32), 21);
        loaded.load_slice(raw.len()).copy_from_slice(&raw);
        loaded
            .decode_persisted_v1_in_place(21, raw.len())
            .expect("canonical remote bytes must decode");
        assert_eq!(loaded.header().node_type, NodeType::Leaf);
        assert_eq!(loaded.header().flag, TagFlag::Normal);
    }

    #[test]
    fn remote_persisted_v1_rejects_non_leaf_node_type() {
        let mut raw = persisted_bytes(&make_remote(22));
        raw[1] = NodeType::Intl as u8;

        let mut loaded = BoxRef::alloc_exact(BoxRef::real_size_from_dump(raw.len() as u32), 22);
        loaded.load_slice(raw.len()).copy_from_slice(&raw);
        let err = loaded
            .decode_persisted_v1_in_place(22, raw.len())
            .expect_err("non-canonical remote node_type must fail");
        assert_eq!(err, OpCode::Corruption);
    }
}
