use crate::static_assert;
use crate::types::data::{Index, IntlKey, Key, Record, Sibling, Value, Ver};
use crate::types::header::{NodeType, SLOT_LEN, Slot, SlotType};
use crate::types::traits::{IAlloc, ICodec, IHolder, IInlineSize, IKey, IVal};
use crate::utils::{ADDR_LEN, INIT_ADDR, NULL_ADDR, NULL_PID, raw_ptr_to_ref, raw_ptr_to_ref_mut};
use crate::{Options, number_to_slice};
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::ptr::null_mut;
use std::sync::atomic::AtomicU32;
use std::{
    alloc::{Layout, alloc, dealloc},
    sync::atomic::Ordering::Relaxed,
};

use super::header::{BaseHeader, BoxHeader, DeltaHeader, RemoteHeader, TagFlag, TagKind};
use super::sst::Sst;

static_assert!(BoxRef::HDR_LEN == 40);
static_assert!(align_of::<BoxHeader>() == align_of::<*const ()>());

pub trait ILoader: IInlineSize + Clone {
    fn load(&self, addr: u64) -> BoxRef;
}

pub(crate) struct BoxRef(*mut BoxHeader);
pub(crate) struct BoxView(*mut BoxHeader);

pub(crate) struct DeltaRef(*mut DeltaHeader);
/// both base node and sibling share same layout
pub(crate) struct BaseRef(pub(crate) *mut BaseHeader);
/// used for large key-val
pub(crate) struct RemoteRef(*mut RemoteHeader);

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

    pub(crate) fn copy(x: &[u8]) -> Self {
        let mut b = BoxRef::alloc(x.len() as u32, NULL_ADDR);
        b.data_slice_mut()[..x.len()].copy_from_slice(x);
        let key = unsafe { std::mem::transmute::<&[u8], &[u8]>(&b.data_slice::<u8>()[..x.len()]) };
        Self { key, _src: b }
    }

    pub(crate) fn key(&self) -> &[u8] {
        self.key
    }
}

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
    pub(crate) fn owned(self) -> BoxRef {
        BoxRef(self.0)
    }

    fn inc_ref(&self) {
        raw_ptr_to_ref_mut(self.0).refcnt.fetch_add(1, Relaxed);
    }

    fn dec_ref(&self) -> u32 {
        raw_ptr_to_ref_mut(self.0).refcnt.fetch_sub(1, Relaxed)
    }

    pub(crate) fn refcnt(&self) -> u32 {
        raw_ptr_to_ref_mut(self.0).refcnt.load(Relaxed)
    }
}

impl BoxRef {
    pub(crate) const HDR_LEN: usize = size_of::<BoxHeader>();

    pub(crate) const fn real_size(size: u32) -> u32 {
        Self::HDR_LEN as u32 + size
    }

    pub(crate) fn null() -> Self {
        Self(null_mut())
    }

    pub(crate) fn view(&self) -> BoxView {
        BoxView(self.0)
    }

    /// NOTE: the alignment is hard code to pointer's alignment, and it's true in mace
    pub(crate) fn alloc(size: u32, addr: u64) -> Self {
        let real_size = Self::real_size(size);
        let layout = Layout::array::<u8>(real_size as usize)
            .inspect_err(|x| panic!("bad layout {x:?}"))
            .unwrap();
        let mut this = Self(unsafe { alloc(layout).cast::<_>() });
        let h = this.header_mut();
        h.total_size = real_size;
        h.payload_size = size;
        h.flag = TagFlag::Normal;
        h.pid = NULL_PID;
        h.addr = addr;
        h.refcnt.store(1, Relaxed);
        this
    }

    /// because all fields except refcnt are immutable before flush to file, we exclude the refcnt
    /// before write to file
    pub(crate) fn dump_slice(&self) -> &[u8] {
        let p = self.0 as *const u8;
        let off = size_of::<AtomicU32>();
        unsafe { std::slice::from_raw_parts(p.add(off), self.total_size() as usize - off) }
    }

    /// because we don't write refcnt to file, we must not read it too
    pub(crate) fn load_slice(&mut self) -> &mut [u8] {
        let p = self.0 as *mut u8;
        let off = size_of::<AtomicU32>();
        unsafe { std::slice::from_raw_parts_mut(p.add(off), self.total_size() as usize - off) }
    }

    /// NOTE: for T is not u8, the caller **MUST** make sure T is aligned to pointer size
    pub(crate) fn data_slice<T>(&self) -> &[T] {
        let h = self.header();
        let len = h.total_size as usize - Self::HDR_LEN;
        unsafe { std::slice::from_raw_parts(self.0.add(1).cast::<_>(), len / size_of::<T>()) }
    }

    /// NOTE: for T is not u8, the caller **MUST** make sure T is aligned to pointer size
    pub(crate) fn data_slice_mut<T>(&mut self) -> &mut [T] {
        let h = self.header();
        let len = h.total_size as usize - Self::HDR_LEN;
        unsafe { std::slice::from_raw_parts_mut(self.0.add(1).cast::<_>(), len / size_of::<T>()) }
    }

    pub(crate) fn total_size(&self) -> u32 {
        let h = self.header();
        debug_assert_eq!(Self::real_size(h.payload_size), h.total_size);
        h.total_size
    }

    pub(crate) fn header(&self) -> &BoxHeader {
        raw_ptr_to_ref(self.0)
    }

    pub(crate) fn header_mut(&mut self) -> &mut BoxHeader {
        raw_ptr_to_ref_mut(self.0)
    }

    fn cast_to<T>(&self) -> *mut T {
        unsafe { self.0.add(1).cast::<T>() }
    }

    pub(crate) fn as_delta(&self) -> DeltaRef {
        let h = self.header();
        match h.kind {
            TagKind::Delta => {
                self.view().inc_ref();
                DeltaRef(self.cast_to::<_>())
            }
            _ => unreachable!("invalid kind {:?}", h),
        }
    }

    pub(crate) fn as_base(&self) -> BaseRef {
        let h = self.header();
        match h.kind {
            TagKind::Base => {
                self.view().inc_ref();
                BaseRef(self.cast_to::<_>())
            }
            _ => unreachable!("invalid kind {:?}", h),
        }
    }

    pub(crate) fn as_remote(&self) -> RemoteRef {
        let h = self.header();
        match h.kind {
            TagKind::Remote => {
                self.view().inc_ref();
                RemoteRef(self.cast_to::<_>())
            }
            _ => unreachable!("invalid kind {:?}", h),
        }
    }
}

pub trait IBoxRef {
    fn as_box(&self) -> BoxRef;

    fn box_view(&self) -> BoxView;
}

pub trait IBoxHeader {
    fn box_header(&self) -> &BoxHeader;

    fn box_header_mut(&mut self) -> &mut BoxHeader;
}

macro_rules! impl_as_tag_ptr {
    ($x: ty) => {
        impl IBoxRef for $x {
            fn as_box(&self) -> BoxRef {
                if self.0.is_null() {
                    return BoxRef::null();
                }
                let x = BoxView(unsafe { (self.0 as *mut BoxHeader).sub(1) });
                x.inc_ref();
                x.owned()
            }

            fn box_view(&self) -> BoxView {
                if self.0.is_null() {
                    return BoxRef::null().view();
                }
                BoxView(unsafe { (self.0 as *mut BoxHeader).sub(1) })
            }
        }
    };
}

macro_rules! impl_tag_header {
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

macro_rules! impl_clone_drop_header {
    ($x: ty, $y: ty) => {
        impl Drop for $x {
            fn drop(&mut self) {
                if !self.0.is_null() {
                    drop(self.box_view().owned());
                }
            }
        }

        impl Clone for $x {
            fn clone(&self) -> Self {
                self.box_view().inc_ref();
                Self(self.0)
            }
        }

        impl $x {
            pub(crate) fn header(&self) -> &$y {
                raw_ptr_to_ref(self.0)
            }

            pub(crate) fn header_mut(&mut self) -> &mut $y {
                raw_ptr_to_ref_mut(self.0)
            }
        }
    };
}

impl_as_tag_ptr!(BaseRef);
impl_as_tag_ptr!(DeltaRef);
impl_as_tag_ptr!(RemoteRef);

impl_tag_header!(BaseRef);
impl_tag_header!(DeltaRef);
impl_tag_header!(RemoteRef);

impl_clone_drop_header!(BaseRef, BaseHeader);
impl_clone_drop_header!(DeltaRef, DeltaHeader);
impl_clone_drop_header!(RemoteRef, RemoteHeader);

impl Clone for BoxRef {
    fn clone(&self) -> Self {
        if !self.0.is_null() {
            self.view().inc_ref();
        }
        Self(self.0)
    }
}

impl Drop for BoxRef {
    fn drop(&mut self) {
        if self.0.is_null() {
            return;
        }
        if self.view().dec_ref() == 1 {
            let layout = Layout::array::<u8>(self.total_size() as usize).unwrap();
            let p = self.0 as *mut u8;
            unsafe { dealloc(p, layout) };
        }
    }
}

impl RemoteRef {
    pub fn null() -> Self {
        Self(null_mut())
    }

    /// in case deref to remoteview cause null pointer dereference
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }

    pub fn new<A: IAlloc>(a: &mut A, size: usize) -> Self {
        let mut p = a.allocate(size + size_of::<RemoteHeader>());
        p.header_mut().kind = TagKind::Remote;
        let mut p = p.as_remote();
        p.header_mut().size = size;
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

impl DeltaRef {
    pub(crate) const HDR_LEN: usize = size_of::<DeltaHeader>();

    /// NOTE: the delta is always allocated togather and no indiraction
    pub(crate) fn new<A: IAlloc, K: IKey, V: IVal>(a: &mut A, k: K, v: V) -> Self {
        let sz = k.packed_size() + v.packed_size() + Self::HDR_LEN;
        let mut d = a.allocate(sz);
        d.header_mut().kind = TagKind::Delta;
        let mut d = d.as_delta();

        *d.header_mut() = DeltaHeader {
            klen: k.packed_size() as u32,
            vlen: v.packed_size() as u32,
        };
        k.encode_to(d.key_mut());
        v.encode_to(d.val_mut());
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

impl BaseRef {
    const HDR_LEN: usize = size_of::<BaseHeader>();

    pub(crate) fn new_leaf<'a, A, I, F>(
        a: &mut A,
        lo: &[u8],
        hi: Option<&[u8]>,
        sibling: u64,
        f: F,
    ) -> Self
    where
        A: IAlloc,
        I: IHolder<Item = (Key<'a>, Value<Record>)>,
        F: Fn() -> I,
    {
        let inline_size = a.inline_size() as usize;
        let mut elems = 0;
        let mut hints = VecDeque::new();
        let mut fixed = false;
        let mut last_raw: Option<&[u8]> = None;
        let mut pos = 0;
        let mut payload_sz = 0;
        let mut seekable = SeekableIter::new();
        let mut remote_cnt = 0;
        #[cfg(feature = "extra_check")]
        let mut last_k = None;

        let mut iter = f();
        while let Some((k, v)) = iter.next() {
            #[cfg(feature = "extra_check")]
            {
                if let Some(x) = last_k {
                    assert!(x < k);
                }
                last_k = Some(k);
            }

            seekable.add(k, v, iter.take_holder());
            if let Some(raw) = last_raw {
                if raw == k.raw() {
                    if !fixed {
                        fixed = true;
                        payload_sz += ADDR_LEN; // plus extra addr size
                        remote_cnt += 1; // sibling itself
                        hints.push_back((pos - 1, 0));
                    }
                    let (_, cnt) = hints.back_mut().unwrap();
                    *cnt += 1;
                    pos += 1;
                    if v.packed_size() + Ver::len() > inline_size {
                        remote_cnt += 1; // (ver, value) in sibling
                    }
                    continue;
                }
            }

            let kv_sz = k.packed_size() + v.packed_size();
            pos += 1;
            last_raw = Some(unsafe { std::mem::transmute::<&[u8], &[u8]>(k.raw()) });
            fixed = false;
            if kv_sz > inline_size {
                payload_sz += Slot::REMOTE_LEN;
                remote_cnt += 1; // (key, value) in sst
            } else {
                payload_sz += Slot::LOCAL_LEN;
                payload_sz += kv_sz;
            }
            elems += 1;
        }

        let hdr_sz = elems * SLOT_LEN + Self::HDR_LEN;
        let mut b = Self::init_header::<false, _>(
            a,
            hdr_sz + payload_sz,
            lo,
            hi,
            elems,
            sibling,
            &mut remote_cnt,
        );

        let mut remote = Vec::with_capacity(remote_cnt as usize);
        let mut builder = Builder::from(&mut remote, remote_cnt as usize, b.data_mut(), elems);
        builder.setup_range(a, lo, hi);

        pos = 0;
        loop {
            let Some(&(k, v, ref _b)) = seekable.next() else {
                break;
            };

            let Some((idx, _)) = hints.front() else {
                builder.add(&k, &v, a);
                pos += 1;
                continue;
            };

            if pos == *idx {
                let (_, cnt) = hints.pop_front().unwrap();
                let addr =
                    Self::save_versions(a, &mut builder, cnt as usize, &mut pos, &mut seekable);

                builder.add(&k, &Value::Sib(Sibling::from(addr, &v)), a);
            } else {
                builder.add(&k, &v, a);
            }
            pos += 1;
        }
        b.remote_mut().copy_from_slice(&remote);

        b
    }

    pub(crate) fn new_intl<'a, A, I, F>(
        a: &mut A,
        lo: &[u8],
        hi: Option<&[u8]>,
        sibling: u64,
        f: F,
    ) -> Self
    where
        A: IAlloc,
        I: Iterator<Item = (IntlKey<'a>, Index)>,
        F: Fn() -> I,
    {
        let inline_size = a.inline_size() as usize;
        let mut remote_cnt = 0;
        let mut elems = 0;
        let sz: usize = f()
            .map(|(k, v)| {
                elems += 1;
                let sz = k.packed_size() + v.packed_size();
                if sz > inline_size {
                    remote_cnt += 1;
                    Slot::REMOTE_LEN
                } else {
                    sz + Slot::LOCAL_LEN
                }
            })
            .sum();

        let hdr_sz = elems * SLOT_LEN + Self::HDR_LEN;
        let mut b =
            Self::init_header::<true, _>(a, hdr_sz + sz, lo, hi, elems, sibling, &mut remote_cnt);
        let mut remote = Vec::with_capacity(remote_cnt as usize);

        let mut builder = Builder::from(&mut remote, remote_cnt as usize, b.data_mut(), elems);
        builder.setup_range(a, lo, hi);

        let iter = f();
        for (k, v) in iter {
            builder.add(&k, &v, a);
        }
        b.remote_mut().copy_from_slice(&remote);

        b
    }

    fn save_versions<'a, A: IAlloc>(
        a: &mut A,
        main_builder: &mut Builder,
        mut cnt: usize,
        pos: &mut u32,
        iter: &mut SeekableIter<'a>,
    ) -> u64 {
        let arena_size = a.arena_size() as usize;
        let mut head = None;
        let mut tail: Option<BaseRef> = None;
        let mut beg = iter.curr_pos();

        while cnt > 0 {
            let saved = beg;
            let mut len = Self::HDR_LEN;
            while cnt > 0 {
                let (_, v, _b) = iter.next().unwrap();
                let kv_sz = Ver::len() + v.packed_size();
                let sz = if kv_sz > a.inline_size() as usize {
                    Slot::REMOTE_LEN
                } else {
                    kv_sz + Slot::LOCAL_LEN
                };
                let tmp = len + sz + SLOT_LEN;
                if tmp > arena_size {
                    break;
                }
                len = tmp;
                beg += 1;
                cnt -= 1;
            }
            iter.seek_to(saved);

            // remote_cnt was collected in previous iteration, siblings are already included, so we set to 0
            let mut cnt = 0;
            let mut b =
                Self::init_header::<false, _>(a, len, &[], None, beg - saved, NULL_PID, &mut cnt);
            // NOTE: this is the only place to set flag to Sibling
            b.box_header_mut().flag = TagFlag::Sibling;
            let mut builder = Builder::from(main_builder.remote, 0, b.data_mut(), beg - saved);
            builder.setup_range(a, &[], None);

            for _ in saved..beg {
                let (k, v, _b) = iter.next().unwrap();
                builder.add(k.ver(), v, a);
                *pos += 1;
            }

            let addr = b.box_header().addr;
            main_builder.save_remote(addr);

            if head.is_none() {
                head = Some(addr);
            }

            if let Some(mut last) = tail {
                last.box_header_mut().link = addr;
            }
            tail = Some(b);
        }

        head.unwrap()
    }

    fn init_header<const IS_INDEX: bool, A: IAlloc>(
        a: &mut A,
        mut size: usize,
        lo: &[u8],
        hi: Option<&[u8]>,
        elems: usize,
        sibling: u64,
        remote_cnt: &mut u16,
    ) -> Self {
        let hi_len = hi.map_or(0, |x| x.len());

        let boundary_len = lo.len() + hi_len;
        // we don't use slot to encode lo/hi length, since the length has been saved in header
        size += if boundary_len > a.inline_size() as usize {
            *remote_cnt += 1;
            Slot::REMOTE_LEN
        } else {
            boundary_len
        };

        size += *remote_cnt as usize * ADDR_LEN;

        let mut p = a.allocate(size);
        let h = p.header_mut();
        h.kind = TagKind::Base;
        h.link = 0;
        h.node_type = if IS_INDEX {
            NodeType::Intl
        } else {
            NodeType::Leaf
        };

        let mut b = p.as_base();
        let h = b.header_mut();
        h.elems = elems as u16;
        h.size = size as u32;
        h.right_sibling = sibling;
        h.merging_child = NULL_PID;
        h.lo_len = lo.len() as u32;
        h.hi_len = hi_len as u32;
        h.merging = false;
        h.split_elems = 0;
        h.is_index = IS_INDEX;
        h.nr_remote = *remote_cnt;

        b
    }

    pub(crate) fn null() -> Self {
        Self(null_mut())
    }

    pub(crate) fn is_null(&self) -> bool {
        self.0.is_null()
    }

    pub(crate) fn remote(&self) -> &[u64] {
        let h = self.header();
        unsafe { std::slice::from_raw_parts(self.0.add(1).cast::<u64>(), h.nr_remote as usize) }
    }

    pub(crate) fn remote_mut(&mut self) -> &mut [u64] {
        let h = self.header();
        unsafe { std::slice::from_raw_parts_mut(self.0.add(1).cast::<u64>(), h.nr_remote as usize) }
    }

    pub(crate) fn data(&self) -> &[u8] {
        let h = self.header();
        let remote_len = h.nr_remote as usize * ADDR_LEN;
        unsafe {
            std::slice::from_raw_parts(
                self.0.add(1).cast::<u8>().add(remote_len),
                h.size as usize - Self::HDR_LEN - remote_len,
            )
        }
    }

    /// the date means slot + hi/lo + key-value
    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        let h = self.header();
        let remote_len = h.nr_remote as usize * ADDR_LEN;
        unsafe {
            std::slice::from_raw_parts_mut(
                self.0.add(1).cast::<u8>().add(remote_len),
                h.size as usize - Self::HDR_LEN - remote_len,
            )
        }
    }

    pub(crate) fn get_lo<L: ILoader>(&self, l: &L) -> (BoxRef, &[u8]) {
        let h = self.header();
        if h.lo_len == 0 {
            return (BoxRef::null(), &[]);
        }
        let sz = h.lo_len + h.hi_len;
        let off = h.elems as usize * SLOT_LEN;
        if sz <= l.inline_size() {
            (BoxRef::null(), &self.data()[off..off + h.lo_len as usize])
        } else {
            let s = Slot::decode_from(&self.data()[off..off + Slot::REMOTE_LEN]);
            self.load_remote(l, s.addr(), 0, h.lo_len as usize)
        }
    }

    fn load_remote<L: ILoader>(&self, l: &L, addr: u64, off: usize, len: usize) -> (BoxRef, &[u8]) {
        let p = l.load(addr);
        let r = p.as_remote();
        let s = r.raw();
        (p, &s[off..off + len])
    }

    pub(crate) fn get_hi<L: ILoader>(&self, l: &L) -> Option<(BoxRef, &[u8])> {
        let h = self.header();
        if h.hi_len == 0 {
            return None;
        }
        let off = h.elems as usize * SLOT_LEN;
        let sz = h.lo_len + h.hi_len;
        if sz <= l.inline_size() {
            Some((
                BoxRef::null(),
                &self.data()[off + h.lo_len as usize..off + sz as usize],
            ))
        } else {
            let s = Slot::decode_from(&self.data()[off..off + Slot::REMOTE_LEN]);
            Some(self.load_remote(l, s.addr(), h.lo_len as usize, h.hi_len as usize))
        }
    }

    pub(crate) fn merge<A: IAlloc, L: ILoader>(&self, a: &mut A, l: L, other: Self) -> Self {
        let h = self.header();
        let oh = other.header();
        assert_eq!(h.is_index, oh.is_index);

        let (_p, lo) = self.get_lo(&l);
        let hi_opt = other.get_hi(&l);
        let hi = hi_opt.as_ref().map(|x| x.1);
        let sibling = oh.right_sibling;

        if h.is_index {
            let f = || {
                FuseBaseIter::new(
                    l.clone(),
                    &[
                        (self.sst::<IntlKey, Index>(), h.elems as usize),
                        (other.sst(), oh.elems as usize),
                    ],
                )
            };
            Self::new_intl(a, lo, hi, sibling, f)
        } else {
            let f = || {
                FuseBaseIter::new(
                    l.clone(),
                    &[
                        (self.sst::<Key, Value<Record>>(), h.elems as usize),
                        (other.sst(), oh.elems as usize),
                    ],
                )
            };
            Self::new_leaf(a, lo, hi, sibling, f)
        }
    }

    pub(crate) fn range_iter<L, K, V>(&self, l: L, beg: usize, end: usize) -> BaseIter<L, K, V>
    where
        L: ILoader,
        K: IKey,
        V: IVal,
    {
        if self.is_null() {
            BaseIter::new(l, self.sst(), 0, 0)
        } else {
            BaseIter::new(l, self.sst(), beg, end)
        }
    }

    pub(crate) fn sst<K, V>(&self) -> Sst<K, V> {
        Sst::<K, V>::new(self.0)
    }
}

pub(crate) struct BaseIter<'a, L, K, V>
where
    L: ILoader,
    K: IKey,
    V: IVal,
{
    loader: L,
    sst: Sst<K, V>,
    beg: usize,
    end: usize,
    sib_key: &'a [u8],
    remote: Option<BoxRef>,
    sibling: Option<BaseRef>,
    sibling_pos: usize,
}

impl<'a, L, K, V> BaseIter<'a, L, K, V>
where
    L: ILoader,
    K: IKey,
    V: IVal,
{
    fn new(loader: L, sst: Sst<K, V>, beg: usize, end: usize) -> Self {
        Self {
            loader,
            sst,
            beg,
            end,
            sib_key: &[],
            remote: None,
            sibling: None,
            sibling_pos: 0,
        }
    }
}

impl<'a, L> IHolder for BaseIter<'a, L, Key<'a>, Value<Record>>
where
    L: ILoader,
{
    fn take_holder(&mut self) -> BoxRef {
        self.remote.take().map_or(BoxRef::null(), |x| x)
    }
}

impl<'a, L> Iterator for BaseIter<'a, L, Key<'a>, Value<Record>>
where
    L: ILoader,
{
    type Item = (Key<'a>, Value<Record>);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(p) = self.sibling.as_ref() {
            if self.sibling_pos >= p.header().elems as usize {
                let link = p.box_header().link;
                self.sibling_pos = 0;
                if link != INIT_ADDR {
                    self.sibling = Some(self.loader.load(link).as_base());
                    continue;
                }
                self.sibling.take();
            } else {
                let (ver, v, r) = p
                    .sst::<Ver, Value<Record>>()
                    .get_unchecked(&self.loader, self.sibling_pos);
                self.sibling_pos += 1;
                self.remote = Some(if r.is_null() { p.as_box() } else { r.as_box() });
                let k = Key::new(self.sib_key, ver.txid, ver.cmd);
                return Some((k, v));
            }
        }
        if self.beg < self.end {
            let (k, v, r) = self.sst.get_unchecked(&self.loader, self.beg);
            self.beg += 1;
            self.remote = Some(r.as_box());
            if let Some(s) = v.sibling() {
                self.sib_key = k.raw;
                self.sibling = Some(self.loader.load(s.addr()).as_base());
                self.sibling_pos = 0;
                Some((k, v.unpack_sibling()))
            } else {
                Some((k, v))
            }
        } else {
            None
        }
    }
}

impl<'a, L> Iterator for BaseIter<'a, L, IntlKey<'a>, Index>
where
    L: ILoader,
{
    type Item = (IntlKey<'a>, Index, RemoteRef);

    fn next(&mut self) -> Option<Self::Item> {
        if self.beg < self.end {
            let x = self.sst.get_unchecked(&self.loader, self.beg);
            self.beg += 1;
            Some(x)
        } else {
            None
        }
    }
}

struct FuseBaseIter<'a, L, K, V>
where
    L: ILoader,
    K: IKey,
    V: IVal,
{
    loader: L,
    cur: usize,
    pos: usize,
    data: Vec<FuseDataIterData<K, V>>,
    sib_key: &'a [u8],
    remote: Option<BoxRef>,
    sibling: Option<BaseRef>,
    sibling_pos: usize,
}

impl<'a, L, K, V> FuseBaseIter<'a, L, K, V>
where
    L: ILoader,
    K: IKey,
    V: IVal,
{
    fn new(l: L, ssts: &[(Sst<K, V>, usize)]) -> Self {
        Self {
            loader: l,
            cur: 0,
            pos: 0,
            data: ssts
                .iter()
                .map(|&(sst, end)| FuseDataIterData { sst, end })
                .collect(),
            sib_key: &[],
            remote: None,
            sibling: None,
            sibling_pos: 0,
        }
    }
}

struct FuseDataIterData<K, V>
where
    K: IKey,
    V: IVal,
{
    sst: Sst<K, V>,
    end: usize,
}

impl<'a, L> IHolder for FuseBaseIter<'a, L, Key<'a>, Value<Record>>
where
    L: ILoader,
{
    fn take_holder(&mut self) -> BoxRef {
        self.remote.take().map_or(BoxRef::null(), |x| x)
    }
}

impl<'a, L> Iterator for FuseBaseIter<'a, L, Key<'a>, Value<Record>>
where
    L: ILoader,
{
    type Item = (Key<'a>, Value<Record>);

    fn next(&mut self) -> Option<Self::Item> {
        let s = loop {
            if self.cur == self.data.len() {
                return None;
            }
            let s = &mut self.data[self.cur];
            if self.pos < s.end {
                break s;
            }
            self.pos = 0;
            self.cur += 1;
        };

        while let Some(p) = self.sibling.as_ref() {
            if self.sibling_pos >= p.header().elems as usize {
                self.sibling_pos = 0;
                let link = p.box_header().link;
                if link != 0 {
                    self.sibling = Some(self.loader.load(link).as_base());
                    continue;
                }
                self.sibling = None;
            } else {
                let (ver, v, r) = p
                    .sst::<Ver, Value<Record>>()
                    .get_unchecked(&self.loader, self.sibling_pos);
                self.sibling_pos += 1;
                let b = if r.is_null() { p.as_box() } else { r.as_box() };
                self.remote = Some(b);
                let k = Key::new(self.sib_key, ver.txid, ver.cmd);
                return Some((k, v));
            }
        }
        let (k, v, r) = s.sst.get_unchecked(&self.loader, self.pos);
        self.pos += 1;
        self.remote = Some(r.as_box());
        if let Some(s) = v.sibling() {
            self.sib_key = k.raw;
            self.sibling_pos = 0;
            self.sibling = Some(self.loader.load(s.addr()).as_base());
            return Some((k, v.unpack_sibling()));
        }
        Some((k, v))
    }
}

impl<'a, L> Iterator for FuseBaseIter<'a, L, IntlKey<'a>, Index>
where
    L: ILoader,
{
    type Item = (IntlKey<'a>, Index);

    fn next(&mut self) -> Option<Self::Item> {
        let s = loop {
            if self.cur == self.data.len() {
                return None;
            }
            let s = &mut self.data[self.cur];
            if self.pos < s.end {
                break s;
            }
            self.pos = 0;
            self.cur += 1;
        };

        let (k, v, r) = s.sst.get_unchecked(&self.loader, self.pos);
        self.remote = Some(r.as_box());
        self.pos += 1;
        Some((k, v))
    }
}

/// the layout of sst:
/// ```text
/// +---------+-------------+-------------+---------+--------+----------+
/// | header  | remote addr | index table | low key | hi key |key-value |
/// +---------+-------------+-------------+---------+--------+----------+
/// ```
struct Builder<'a> {
    remote: &'a mut Vec<u64>,
    slots: &'a mut [SlotType],
    payload: &'a mut [u8],
    pos: usize,
    index: usize,
    offset: usize,
}

impl<'a> Builder<'a> {
    /// the `s` is starts from index table
    fn from(remote: &'a mut Vec<u64>, nr_remote: usize, s: &'a mut [u8], elems: usize) -> Self {
        let offset = elems * SLOT_LEN;
        let slots = unsafe {
            let p = s.as_mut_ptr().cast::<SlotType>();
            std::slice::from_raw_parts_mut(p, elems)
        };
        Self {
            remote,
            slots,
            payload: s,
            pos: BaseRef::HDR_LEN + nr_remote * ADDR_LEN,
            index: 0,
            offset,
        }
    }

    fn setup_range<A: IAlloc>(&mut self, a: &mut A, lo: &[u8], hi: Option<&[u8]>) {
        let hi_len = hi.map_or(0, |x| x.len());
        let boundary_len = lo.len() + hi_len;

        if boundary_len > 0 {
            if boundary_len <= a.inline_size() as usize {
                self.payload[self.offset..self.offset + lo.len()].copy_from_slice(lo);
                self.offset += lo.len();
                self.payload[self.offset..self.offset + hi_len]
                    .copy_from_slice(hi.map_or(&[], |x| x));
                self.offset += hi_len;
            } else {
                let mut p = RemoteRef::new(a, boundary_len);
                self.save_remote(p.box_header().addr);
                let dst = p.raw_mut();
                dst[0..lo.len()].copy_from_slice(lo);
                dst[lo.len()..].copy_from_slice(hi.map_or(&[], |x| x));
                number_to_slice!(
                    p.box_header().addr,
                    self.payload[self.offset..self.offset + Slot::REMOTE_LEN]
                );
                self.offset += Slot::REMOTE_LEN;
            }
        }
    }

    fn add_remote<K, V, A: IAlloc>(&mut self, total: usize, k: &K, v: &V, a: &mut A) -> usize
    where
        K: IKey,
        V: IVal,
    {
        // TODO: we can reuse the RemotePtr
        let mut p = RemoteRef::new(a, total);
        let s = Slot::from_remote(p.box_header().addr);
        self.save_remote(s.addr());
        let slot_sz = s.packed_size();
        s.encode_to(self.slice(self.offset, slot_sz));
        let remote_data = p.raw_mut();
        let ksz = k.packed_size();
        k.encode_to(&mut remote_data[..ksz]);
        v.encode_to(&mut remote_data[ksz..]);
        slot_sz
    }

    #[inline]
    fn slice(&mut self, b: usize, len: usize) -> &mut [u8] {
        &mut self.payload[b..b + len]
    }

    fn add<K, V, A: IAlloc>(&mut self, k: &K, v: &V, a: &mut A)
    where
        K: IKey,
        V: IVal,
    {
        let (ksz, vsz) = (k.packed_size(), v.packed_size());
        let mut total_sz = ksz + vsz;

        if total_sz <= a.inline_size() as usize {
            let s = Slot::inline();
            let slot_sz = s.packed_size();
            s.encode_to(self.slice(self.offset, slot_sz));
            k.encode_to(self.slice(self.offset + slot_sz, ksz));
            v.encode_to(self.slice(self.offset + slot_sz + ksz, vsz));
            total_sz += slot_sz;
        } else {
            total_sz = self.add_remote(total_sz, k, v, a);
        }

        self.slots[self.index] = (self.pos + self.offset) as SlotType;
        self.index += 1;
        self.offset += total_sz;
    }

    fn save_remote(&mut self, addr: u64) {
        self.remote.push(addr);
    }
}

struct SeekableIter<'a> {
    data: Vec<(Key<'a>, Value<Record>, BoxRef)>,
    index: usize,
}

impl<'a> SeekableIter<'a> {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(Options::SPLIT_ELEM as usize * 4),
            index: 0,
        }
    }

    fn add(&mut self, k: Key<'a>, v: Value<Record>, b: BoxRef) {
        self.data.push((k, v, b));
    }

    fn seek_to(&mut self, pos: usize) {
        self.index = pos;
    }

    fn curr_pos(&self) -> usize {
        self.index
    }

    fn next(&mut self) -> Option<&(Key<'a>, Value<Record>, BoxRef)> {
        if self.index < self.data.len() {
            let idx = self.index;
            self.index += 1;
            Some(&self.data[idx])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use std::{cell::Cell, collections::HashMap};

    use crate::{
        types::{
            data::{Index, IntlKey, Key, Record, Value},
            header::TagKind,
            refbox::{BaseRef, BoxRef, IBoxHeader, ILoader},
            traits::{IAlloc, IHolder, IInlineSize},
        },
        utils::{MutRef, NULL_PID},
    };

    struct Inner {
        off: Cell<u64>,
        map: HashMap<u64, BoxRef>,
    }

    #[derive(Clone)]
    struct Allocator {
        inner: MutRef<Inner>,
    }

    impl Allocator {
        fn new() -> Self {
            Self {
                inner: MutRef::new(Inner {
                    off: Cell::new(0),
                    map: HashMap::new(),
                }),
            }
        }
    }

    impl IAlloc for Allocator {
        fn allocate(&mut self, size: usize) -> BoxRef {
            let r = &mut self.inner;
            let old = r.off.get();
            r.off.set(old + size as u64);
            let b = BoxRef::alloc(size as u32, old);
            r.map.insert(old, b.clone());
            b
        }

        fn recycle(&mut self, _p: &[u64]) {
            unimplemented!()
        }

        fn arena_size(&mut self) -> u32 {
            1 << 20
        }
    }

    impl ILoader for Allocator {
        fn load(&self, addr: u64) -> BoxRef {
            self.inner.map.get(&addr).unwrap().clone()
        }
    }

    impl IInlineSize for Allocator {
        fn inline_size(&self) -> u32 {
            2048
        }
    }

    #[derive(Clone, Copy)]
    struct LeafData<'a> {
        data: &'a [(Key<'a>, Value<Record>)],
        pos: usize,
    }

    impl<'a> Iterator for LeafData<'a> {
        type Item = (Key<'a>, Value<Record>);
        fn next(&mut self) -> Option<Self::Item> {
            if self.pos == self.data.len() {
                return None;
            }
            let index = self.pos;
            self.pos += 1;
            Some(self.data[index])
        }
    }

    impl IHolder for LeafData<'_> {
        fn take_holder(&mut self) -> BoxRef {
            BoxRef::null()
        }
    }

    #[test]
    fn box_base() {
        let mut a = Allocator::new();
        let kv = [(IntlKey::new("mo".as_bytes()), Index::new(233))];
        let b = BaseRef::new_intl(&mut a, "a".as_bytes(), None, NULL_PID, || {
            kv.iter().map(|&(k, v)| (k, v))
        });

        let th = b.box_header();
        assert_eq!(th.kind, TagKind::Base);

        let empty = LeafData { data: &[], pos: 0 };
        let b = BaseRef::new_leaf(&mut a, [].as_slice(), None, NULL_PID, || empty);
        let th = b.box_header();
        assert_eq!(th.kind, TagKind::Base);

        log::debug!("{th:?}");
    }

    #[test]
    fn builder_leaf() {
        macro_rules! test {
            ($l: expr, $r: expr, $k: expr, $tx: expr, $cmd: expr, $v: expr, $w:expr) => {
                assert_eq!($l.raw, $k.as_bytes());
                assert_eq!($l.txid, $tx);
                assert_eq!($l.cmd, $cmd);
                assert_eq!($r.as_ref().data(), $v.as_bytes());
                assert_eq!($r.as_ref().worker_id(), $w);
            };
        }

        let kv = LeafData {
            data: &[
                (
                    Key::new("foo".as_bytes(), 1, 1),
                    Value::Put(Record::normal(1, "foo".as_bytes())),
                ),
                (
                    Key::new("foo".as_bytes(), 2, 1),
                    Value::Put(Record::normal(1, "bar".as_bytes())),
                ),
                (
                    Key::new("mo".as_bytes(), 3, 1),
                    Value::Put(Record::normal(1, "ha".as_bytes())),
                ),
            ],
            pos: 0,
        };
        let mut a = Allocator::new();
        let l = a.clone();

        let b = BaseRef::new_leaf(&mut a, &[], None, NULL_PID, || kv);
        let mut iter =
            b.range_iter::<Allocator, Key, Value<Record>>(l, 0, b.header().elems as usize);

        let (k, v) = iter.next().unwrap();

        test!(k, v, "foo", 1, 1, "foo", 1);

        assert!(v.sibling().is_none());

        iter.next(); // skip the expanded sibling

        let (k, v) = iter.next().unwrap();
        test!(k, v, "mo", 3, 1, "ha", 1);

        assert!(iter.next().is_none());
    }

    #[test]
    fn builder_intl() {
        macro_rules! test {
            ($l:expr, $r: expr, $k: expr, $v: expr) => {
                assert_eq!($l.raw, $k.as_bytes());
                assert_eq!($r.pid, $v);
            };
        }
        let kv = [
            (IntlKey::new("foo".as_bytes()), Index::new(1)),
            (IntlKey::new("bar".as_bytes()), Index::new(2)),
            (IntlKey::new("baz".as_bytes()), Index::new(3)),
        ];
        let mut a = Allocator::new();
        let l = a.clone();

        let b = BaseRef::new_intl(&mut a, &[], None, NULL_PID, || {
            kv.iter().map(|&(k, v)| (k, v))
        });
        let mut iter = b.range_iter::<Allocator, IntlKey, Index>(l, 0, b.header().elems as usize);

        let (k, v, _) = iter.next().unwrap();
        test!(k, v, "foo", 1);

        let (k, v, _) = iter.next().unwrap();
        test!(k, v, "bar", 2);

        let (k, v, _) = iter.next().unwrap();
        test!(k, v, "baz", 3);

        assert!(iter.next().is_none());
    }
}
