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
static_assert!(BoxRef::HDR_LEN == 40);
static_assert!(align_of::<BoxHeader>() == align_of::<*const ()>());

#[derive(Clone)]
pub(crate) struct KeyRef {
    key: &'static [u8],
    _src: BoxRef,
}

impl KeyRef {
    /// the `key` must be a segment in `x`
    pub(crate) fn new(key: &[u8], x: BoxRef) -> Self {
        Self {
            key: unsafe { std::mem::transmute::<&[u8], &[u8]>(key) },
            _src: x,
        }
    }

    pub(crate) fn build(prefix: &[u8], base: &[u8]) -> Self {
        let len = prefix.len() + base.len();
        let mut b = BoxRef::alloc(len as u32, NULL_ADDR);
        let (dp, db) = b.data_slice_mut()[..len].split_at_mut(prefix.len());
        dp.copy_from_slice(prefix);
        db.copy_from_slice(base);
        let key = unsafe { std::mem::transmute::<&[u8], &[u8]>(&b.data_slice::<u8>()[..len]) };
        Self { key, _src: b }
    }

    pub(crate) fn copy(x: &[u8]) -> Self {
        let mut b = BoxRef::alloc(x.len() as u32, NULL_ADDR);
        b.data_slice_mut()[..x.len()].copy_from_slice(x);
        let key = unsafe { std::mem::transmute::<&[u8], &[u8]>(&b.data_slice::<u8>()[..x.len()]) };
        Self { key, _src: b }
    }

    pub(crate) fn key<'a>(&self) -> &'a [u8] {
        self.key
    }
}

pub(crate) struct BoxRef(*mut BoxHeader);

#[derive(Clone, Copy)]
pub(crate) struct BoxView(*mut BoxHeader);

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

    fn inc_ref(&self) {
        raw_ptr_to_ref_mut(self.0).refs.fetch_add(1, Relaxed);
    }

    fn dec_ref(&self) -> u32 {
        raw_ptr_to_ref_mut(self.0).refs.fetch_sub(1, AcqRel)
    }

    pub(crate) fn refcnt(&self) -> u32 {
        raw_ptr_to_ref_mut(self.0).refs.load(Relaxed)
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

    /// NOTE: the alignment is hard code to pointer's alignment, and it's true in mace
    pub(crate) fn alloc(size: u32, addr: u64) -> BoxRef {
        let real_size = Self::real_size(size);
        let layout = Layout::array::<u8>(real_size as usize)
            .inspect_err(|x| panic!("bad layout {x:?}"))
            .unwrap();
        let mut this = BoxRef(unsafe { alloc(layout).cast::<_>() });
        let h = this.header_mut();
        h.total_size = real_size;
        h.payload_size = size;
        h.flag = TagFlag::Normal;
        h.pid = NULL_PID;
        h.addr = addr;
        h.link = NULL_ADDR;
        h.refs.store(1, Relaxed);
        this
    }

    /// because all fields except refcnt are immutable before flush to file, we exclude the refcnt
    /// before write to file
    pub(crate) fn dump_slice(&self) -> &[u8] {
        let p = self.0 as *const u8;
        let off = size_of::<AtomicU32>();
        unsafe { std::slice::from_raw_parts(p.add(off), self.total_size() as usize - off) }
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
        unsafe { std::slice::from_raw_parts_mut(p.add(off), self.total_size() as usize - off) }
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
        if view.dec_ref() == 1 {
            let layout = Layout::array::<u8>(self.total_size() as usize).unwrap();
            let p = self.0 as *mut u8;
            unsafe { dealloc(p, layout) };
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
