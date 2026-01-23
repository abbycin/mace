use crate::static_assert;
use crate::types::header::TagKind;
use crate::utils::{NULL_ADDR, NULL_PID, raw_ptr_to_ref, raw_ptr_to_ref_mut};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU32;
use std::{
    alloc::{Layout, alloc, dealloc},
    sync::atomic::Ordering::{AcqRel, Relaxed},
};

use super::header::{BaseHeader, BoxHeader, DeltaHeader, RemoteHeader, TagFlag};
use super::traits::{IAsBoxRef, IBoxHeader, IHeader};
static_assert!(BoxRef::HDR_LEN == 48);
static_assert!(align_of::<BoxHeader>() == align_of::<*const ()>());

pub struct BoxRef(*mut BoxHeader);

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
    fn into_owned(self) -> BoxRef {
        BoxRef(self.0)
    }

    pub(crate) fn inc_ref(&self) {
        raw_ptr_to_ref_mut(self.0).refs.fetch_add(1, Relaxed);
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

    pub(crate) const fn real_size(size: u32) -> u32 {
        Self::HDR_LEN as u32 + size
    }

    pub(crate) fn total_size(&self) -> u32 {
        let h = self.header();
        debug_assert_eq!(Self::real_size(h.payload_size), h.total_size);
        h.total_size
    }

    #[inline(always)]
    #[cfg(test)]
    pub(crate) fn alloc(size: u32, addr: u64) -> BoxRef {
        Self::alloc_exact(Self::real_size(size), addr)
    }

    pub(crate) fn init(&mut self, total_size: u32, addr: u64, is_chunk: bool) {
        let h = self.header_mut();
        h.total_size = total_size;
        h.payload_size = total_size - Self::HDR_LEN as u32;
        h.flag = TagFlag::Normal;
        h.pid = NULL_PID;
        h.txid = 0;
        h.addr = addr;
        h.link = NULL_ADDR;
        let refs = if is_chunk { 1 | 0x80000000 } else { 1 };
        h.refs.store(refs, Relaxed);
    }

    pub(crate) fn alloc_exact(size: u32, addr: u64) -> BoxRef {
        let layout = Layout::from_size_align(size as usize, 8)
            .inspect_err(|x| panic!("bad layout {x:?}"))
            .unwrap();
        let mut this = BoxRef(unsafe { alloc(layout).cast::<_>() });
        #[cfg(feature = "extra_check")]
        assert_eq!(this.0 as usize % 8, 0);
        this.init(size, addr, false);
        this
    }

    pub(crate) unsafe fn from_raw(ptr: *mut u8) -> Self {
        Self(ptr as *mut BoxHeader)
    }

    /// because all fields except refcnt are immutable before flush to file, we exclude the refcnt
    /// before write to file
    pub(crate) fn dump_slice(&self) -> &[u8] {
        let p = self.0 as *const u8;
        let off = size_of::<AtomicU32>();
        unsafe { std::slice::from_raw_parts(p.add(off), self.dump_len()) }
    }

    pub(crate) fn real_size_from_dump(size: u32) -> u32 {
        size + size_of::<AtomicU32>() as u32
    }

    pub(crate) fn dump_len(&self) -> usize {
        self.total_size() as usize - size_of::<AtomicU32>()
    }

    /// NOTE: for T is not u8, the caller **MUST** make sure T is aligned to pointer size
    pub(crate) fn data_slice<'a, T>(&self) -> &'a [T] {
        let h = self.header();
        let len = h.total_size as usize - Self::HDR_LEN;
        unsafe { std::slice::from_raw_parts(self.0.add(1).cast::<_>(), len / size_of::<T>()) }
    }

    /// NOTE: for T is not u8, the caller **MUST** make sure T is aligned to pointer size
    pub(crate) fn data_slice_mut<'a, T>(&mut self) -> &'a mut [T] {
        let h = self.header();
        let len = h.total_size as usize - Self::HDR_LEN;
        unsafe { std::slice::from_raw_parts_mut(self.0.add(1).cast::<_>(), len / size_of::<T>()) }
    }

    /// because we don't write refcnt to file, we must not read it too
    pub(crate) fn load_slice(&mut self) -> &mut [u8] {
        let p = self.0 as *mut u8;
        let off = size_of::<AtomicU32>();
        unsafe { std::slice::from_raw_parts_mut(p.add(off), self.dump_len()) }
    }

    pub(crate) fn view(&self) -> BoxView {
        BoxView(self.0)
    }
}

impl Clone for BoxRef {
    fn clone(&self) -> Self {
        debug_assert!(!self.0.is_null());
        self.view().inc_ref();
        Self(self.0)
    }
}

impl Drop for BoxRef {
    fn drop(&mut self) {
        debug_assert!(!self.0.is_null());
        let view = self.view();
        let old_refs = view.dec_ref();

        // mask out the MSB (Chunk Flag) to get actual count
        let count = old_refs & 0x7FFFFFFF;

        if count == 1 {
            if (old_refs & 0x80000000) != 0 {
                // in chunk
                unsafe { crate::map::chunk::dec_ref(self.0 as *mut u8) };
            } else {
                let layout = Layout::from_size_align(self.total_size() as usize, 8).unwrap();
                let p = self.0 as *mut u8;
                unsafe { dealloc(p, layout) };
            }
        }
    }
}

macro_rules! impl_box {
    ($x: ty) => {
        impl IAsBoxRef for $x {
            fn as_box(&self) -> BoxRef {
                debug_assert!(!self.0.is_null());
                let x = BoxView(unsafe { (self.0 as *mut BoxHeader).sub(1) });
                x.inc_ref();
                x.into_owned()
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
