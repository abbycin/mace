use std::{collections::VecDeque, ptr::null_mut};

use crate::{
    Options, number_to_slice,
    types::{
        data::{Index, IntlKey, IntlSeg, Key, LeafSeg, Record, Sibling, Value, Ver},
        header::{BaseHeader, NodeType, SLOT_LEN, Slot, SlotType, TagFlag, TagKind},
        refbox::{BaseView, BoxRef, RemoteView},
        sst::Sst,
        traits::{IAlloc, IBoxHeader, ICodec, IHeader, IKey, ILoader, IVal},
    },
    utils::{ADDR_LEN, INIT_ADDR, NULL_PID},
};

impl BaseView {
    const HDR_LEN: usize = size_of::<BaseHeader>();

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

    pub(crate) fn calc_prefix(lo: &[u8], hi: &Option<&[u8]>) -> usize {
        if let Some(hi) = hi {
            lo.iter()
                .zip(hi.iter())
                .take_while(|&(x, y)| x == y)
                .count()
        } else {
            0
        }
    }

    pub(crate) fn new_leaf<'a, A, I, F>(
        a: &mut A,
        lo: &[u8],
        hi: Option<&[u8]>,
        sibling: u64,
        f: F,
    ) -> BoxRef
    where
        A: IAlloc,
        I: Iterator<Item = (LeafSeg<'a>, Value<Record>)>,
        F: Fn() -> I,
    {
        let inline_size = a.inline_size() as usize;
        let mut elems = 0;
        let mut hints = VecDeque::new();
        let mut fixed = false;
        let mut last_raw: Option<LeafSeg<'a>> = None;
        let mut pos = 0;
        let mut payload_sz = 0;
        let mut seekable = SeekableIter::new();
        let mut remote_cnt = 0;
        let prefix_len = Self::calc_prefix(lo, &hi);
        #[cfg(feature = "extra_check")]
        let mut last_k = None;

        let iter = f();
        for (ks, v) in iter {
            #[cfg(feature = "extra_check")]
            {
                if let Some(x) = last_k {
                    assert!(x.cmp(&ks).is_lt());
                }
                last_k = Some(ks);
            }

            let k = ks.remove_prefix(prefix_len);
            seekable.add(k, v);
            if let Some(ref raw) = last_raw {
                if raw.raw_cmp(&k).is_eq() {
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
            last_raw = Some(k);
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
        let b = Self::alloc::<false, _>(
            a,
            hdr_sz + payload_sz,
            lo,
            hi,
            elems,
            sibling,
            &mut remote_cnt,
            prefix_len,
        );

        let mut base = b.view().as_base();
        let mut remote = Vec::with_capacity(remote_cnt as usize);
        let mut builder = Builder::from(&mut remote, remote_cnt as usize, base.data_mut(), elems);
        builder.setup_range(a, lo, hi);

        pos = 0;
        loop {
            let Some(&(k, v)) = seekable.next() else {
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
        base.remote_mut().copy_from_slice(&remote);
        b
    }

    pub(crate) fn new_intl<'a, A, I, F>(
        a: &mut A,
        lo: &[u8],
        hi: Option<&[u8]>,
        sibling: u64,
        f: F,
    ) -> BoxRef
    where
        A: IAlloc,
        I: Iterator<Item = (IntlSeg<'a>, Index)>,
        F: Fn() -> I,
    {
        let prefix_len = Self::calc_prefix(lo, &hi);
        let inline_size = a.inline_size() as usize;
        let mut remote_cnt = 0;
        let mut elems = 0;
        let sz: usize = f()
            .map(|(k, v)| {
                elems += 1;
                let sz = k.remove_prefix(prefix_len).packed_size() + v.packed_size();
                if sz > inline_size {
                    remote_cnt += 1;
                    Slot::REMOTE_LEN
                } else {
                    sz + Slot::LOCAL_LEN
                }
            })
            .sum();

        let hdr_sz = elems * SLOT_LEN + Self::HDR_LEN;
        let b = Self::alloc::<true, _>(
            a,
            hdr_sz + sz,
            lo,
            hi,
            elems,
            sibling,
            &mut remote_cnt,
            prefix_len,
        );
        let mut remote = Vec::with_capacity(remote_cnt as usize);
        let mut base = b.view().as_base();
        let mut builder = Builder::from(&mut remote, remote_cnt as usize, base.data_mut(), elems);
        builder.setup_range(a, lo, hi);

        let iter = f();
        for (k, v) in iter {
            let k = k.remove_prefix(prefix_len);
            builder.add(&k, &v, a);
        }
        base.remote_mut().copy_from_slice(&remote);

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
        let mut tail: Option<BaseView> = None;
        let mut beg = iter.curr_pos();

        while cnt > 0 {
            let saved = beg;
            let mut len = Self::HDR_LEN;
            while cnt > 0 {
                let (_, v) = iter.next().unwrap();
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
            let b = Self::alloc::<false, _>(a, len, &[], None, beg - saved, NULL_PID, &mut cnt, 0);
            // NOTE: this is the only place to set flag to Sibling
            let mut base = b.view().as_base();
            base.box_header_mut().flag = TagFlag::Sibling;
            let mut builder = Builder::from(main_builder.remote, 0, base.data_mut(), beg - saved);
            builder.setup_range(a, &[], None);

            for _ in saved..beg {
                let (k, v) = iter.next().unwrap();
                builder.add(&k.ver, v, a);
                *pos += 1;
            }

            let addr = base.box_header().addr;
            main_builder.save_remote(addr);

            if head.is_none() {
                head = Some(addr);
            }

            if let Some(mut last) = tail {
                last.box_header_mut().link = addr;
            }
            tail = Some(base);
        }

        head.unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn alloc<const IS_INDEX: bool, A: IAlloc>(
        a: &mut A,
        mut size: usize,
        lo: &[u8],
        hi: Option<&[u8]>,
        elems: usize,
        sibling: u64,
        remote_cnt: &mut u16,
        prefix_len: usize,
    ) -> BoxRef {
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

        let mut b = p.view().as_base();
        let h = b.header_mut();
        h.elems = elems as u16;
        h.size = size as u32;
        h.right_sibling = sibling;
        h.merging_child = NULL_PID;
        h.lo_len = lo.len() as u32;
        h.hi_len = hi_len as u32;
        h.merging = false;
        h.split_elems = 0;
        h.prefix_len = prefix_len as u32;
        h.is_index = IS_INDEX;
        h.nr_remote = *remote_cnt;

        p
    }

    pub(crate) fn merge<A: IAlloc, L: ILoader>(
        &self,
        a: &mut A,
        loader: &L,
        other: Self,
    ) -> BoxRef {
        let hdr1 = self.header();
        let hdr2 = other.header();
        assert_eq!(hdr1.is_index, hdr2.is_index);

        let lo1 = self.lo(loader);
        let lo2 = other.lo(loader);
        let hi = other.hi(loader);
        let sibling = hdr2.right_sibling;
        let elems1 = hdr1.elems as usize;
        let elems2 = hdr2.elems as usize;
        let pl1 = hdr1.prefix_len as usize;
        let pl2 = hdr2.prefix_len as usize;

        if hdr1.is_index {
            let f = || {
                let l = self.range_iter::<L, IntlKey, Index>(loader, 0, elems1);
                let r = other.range_iter::<L, IntlKey, Index>(loader, 0, elems2);
                FuseBaseIter::new(loader, &lo1[..pl1], &lo2[..pl2], l, r)
            };
            Self::new_intl(a, lo1, hi, sibling, f)
        } else {
            let f = || {
                let l = self.range_iter::<L, Key, Value<Record>>(loader, 0, elems1);
                let r = other.range_iter::<L, Key, Value<Record>>(loader, 0, elems2);
                FuseBaseIter::new(loader, &lo1[..pl1], &lo2[..pl2], l, r)
            };
            Self::new_leaf(a, lo1, hi, sibling, f)
        }
    }

    pub(crate) fn remote_mut(&mut self) -> &mut [u64] {
        let h = self.header();
        unsafe { std::slice::from_raw_parts_mut(self.0.add(1).cast::<u64>(), h.nr_remote as usize) }
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

    pub(crate) fn lo<L: ILoader>(&self, l: &L) -> &[u8] {
        let h = self.header();
        if h.lo_len == 0 {
            return &[];
        }
        let sz = h.lo_len + h.hi_len;
        let off = h.elems as usize * SLOT_LEN;
        if sz <= l.inline_size() {
            &self.data()[off..off + h.lo_len as usize]
        } else {
            let s = Slot::decode_from(&self.data()[off..off + Slot::REMOTE_LEN]);
            self.load_remote(l, s.addr(), 0, h.lo_len as usize)
        }
    }

    fn load_remote<L: ILoader>(&self, l: &L, addr: u64, off: usize, len: usize) -> &[u8] {
        let p = l.pin_load(addr);
        let r = p.as_remote();
        let s = r.raw();
        &s[off..off + len]
    }

    pub(crate) fn hi<L: ILoader>(&self, l: &L) -> Option<&[u8]> {
        let h = self.header();
        if h.hi_len == 0 {
            return None;
        }
        let off = h.elems as usize * SLOT_LEN;
        let sz = h.lo_len + h.hi_len;
        if sz <= l.inline_size() {
            Some(&self.data()[off + h.lo_len as usize..off + sz as usize])
        } else {
            let s = Slot::decode_from(&self.data()[off..off + Slot::REMOTE_LEN]);
            Some(self.load_remote(l, s.addr(), h.lo_len as usize, h.hi_len as usize))
        }
    }

    pub(crate) fn range_iter<'a, L, K, V>(
        &'a self,
        l: &'a L,
        beg: usize,
        end: usize,
    ) -> BaseIter<'a, L, K, V>
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
    loader: &'a L,
    sst: Sst<K, V>,
    beg: usize,
    end: usize,
    sib_key: &'a [u8],
    sibling: Option<BaseView>,
    sibling_pos: usize,
}

impl<'a, L, K, V> BaseIter<'a, L, K, V>
where
    L: ILoader,
    K: IKey,
    V: IVal,
{
    fn new(loader: &'a L, sst: Sst<K, V>, beg: usize, end: usize) -> Self {
        Self {
            loader,
            sst,
            beg,
            end,
            sib_key: &[],
            sibling: None,
            sibling_pos: 0,
        }
    }
}

impl<'a, L> Iterator for BaseIter<'a, L, Key<'a>, Value<Record>>
where
    L: ILoader,
{
    type Item = (Key<'a>, Value<Record>);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(p) = self.sibling.as_ref() {
            if self.sibling_pos < p.header().elems as usize {
                let (ver, v, _) = p
                    .sst::<Ver, Value<Record>>()
                    .get_unchecked(self.loader, self.sibling_pos);
                self.sibling_pos += 1;
                let k = Key::new(self.sib_key, ver);
                return Some((k, v));
            } else {
                let link = p.box_header().link;
                self.sibling_pos = 0;
                if link != INIT_ADDR {
                    self.sibling = Some(self.loader.pin_load(link).as_base());
                    continue;
                }
                self.sibling.take();
            }
        }
        if self.beg < self.end {
            let (k, v, _) = self.sst.get_unchecked(self.loader, self.beg);
            self.beg += 1;
            if let Some(s) = v.sibling() {
                self.sib_key = k.raw;
                self.sibling = Some(self.loader.pin_load(s.addr()).as_base());
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
    type Item = (IntlKey<'a>, Index);

    fn next(&mut self) -> Option<Self::Item> {
        if self.beg < self.end {
            let (k, v, _) = self.sst.get_unchecked(self.loader, self.beg);
            self.beg += 1;
            Some((k, v))
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
    first: bool,
    loader: &'a L,
    prefix1: &'a [u8],
    prefix2: &'a [u8],
    iter1: BaseIter<'a, L, K, V>,
    iter2: BaseIter<'a, L, K, V>,
    sib_key: &'a [u8],
    sibling: Option<BaseView>,
    sibling_pos: usize,
}

impl<'a, L, K, V> FuseBaseIter<'a, L, K, V>
where
    L: ILoader,
    K: IKey,
    V: IVal,
{
    fn new(
        loader: &'a L,
        prefix1: &'a [u8],
        prefix2: &'a [u8],
        i1: BaseIter<'a, L, K, V>,
        i2: BaseIter<'a, L, K, V>,
    ) -> Self {
        Self {
            first: true,
            loader,
            prefix1,
            prefix2,
            iter1: i1,
            iter2: i2,
            sib_key: &[],
            sibling: None,
            sibling_pos: 0,
        }
    }

    fn prefix(&self) -> &'a [u8] {
        if self.first {
            self.prefix1
        } else {
            self.prefix2
        }
    }
}

impl<'a, L> Iterator for FuseBaseIter<'a, L, Key<'a>, Value<Record>>
where
    L: ILoader,
{
    type Item = (LeafSeg<'a>, Value<Record>);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(p) = self.sibling.as_ref() {
            if self.sibling_pos >= p.header().elems as usize {
                self.sibling_pos = 0;
                let link = p.box_header().link;
                if link != 0 {
                    self.sibling = Some(self.loader.pin_load(link).as_base());
                    continue;
                }
                self.sibling = None;
            } else {
                let (ver, v, _) = p
                    .sst::<Ver, Value<Record>>()
                    .get_unchecked(self.loader, self.sibling_pos);
                self.sibling_pos += 1;
                let k = LeafSeg::new(self.prefix(), self.sib_key, ver);
                return Some((k, v));
            }
        }
        let (k, v) = {
            let mut next = self.iter1.next();
            if next.is_none() {
                self.first = false;
                next = self.iter2.next();
            }
            next.as_ref()?;
            next.unwrap()
        };

        if let Some(s) = v.sibling() {
            self.sib_key = k.raw;
            self.sibling_pos = 0;
            self.sibling = Some(self.loader.pin_load(s.addr()).as_base());
            return Some((
                LeafSeg::new(self.prefix(), k.raw, k.ver),
                v.unpack_sibling(),
            ));
        }
        Some((LeafSeg::new(self.prefix(), k.raw, k.ver), v))
    }
}

impl<'a, L> Iterator for FuseBaseIter<'a, L, IntlKey<'a>, Index>
where
    L: ILoader,
{
    type Item = (IntlSeg<'a>, Index);

    fn next(&mut self) -> Option<Self::Item> {
        let mut data = self.iter1.next();
        if data.is_none() {
            self.first = false;
            data = self.iter2.next();
            data.as_ref()?;
        }
        data.map(|(k, v)| (IntlSeg::new(self.prefix(), k.raw), v))
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
            pos: BaseView::HDR_LEN + nr_remote * ADDR_LEN,
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
                let p = RemoteView::alloc(a, boundary_len);
                self.save_remote(p.header().addr);
                let mut r = p.view().as_remote();
                let dst = r.raw_mut();
                dst[0..lo.len()].copy_from_slice(lo);
                dst[lo.len()..].copy_from_slice(hi.map_or(&[], |x| x));
                number_to_slice!(
                    p.header().addr,
                    self.payload[self.offset..self.offset + Slot::REMOTE_LEN]
                );
                self.offset += Slot::REMOTE_LEN;
            }
        }
    }

    fn add_remote<K, V, A: IAlloc>(&mut self, total: usize, k: &K, v: &V, a: &mut A) -> usize
    where
        K: ICodec,
        V: ICodec,
    {
        // TODO: we can reuse the RemotePtr
        let p = RemoteView::alloc(a, total);
        let s = Slot::from_remote(p.header().addr);
        self.save_remote(s.addr());
        let slot_sz = s.packed_size();
        s.encode_to(self.slice(self.offset, slot_sz));
        let mut r = p.view().as_remote();
        let remote_data = r.raw_mut();
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
        K: ICodec,
        V: ICodec,
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
    data: Vec<(LeafSeg<'a>, Value<Record>)>,
    index: usize,
}

impl<'a> SeekableIter<'a> {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(Options::SPLIT_ELEM as usize * 4),
            index: 0,
        }
    }

    fn add(&mut self, k: LeafSeg<'a>, v: Value<Record>) {
        self.data.push((k, v));
    }

    fn seek_to(&mut self, pos: usize) {
        self.index = pos;
    }

    fn curr_pos(&self) -> usize {
        self.index
    }

    fn next(&mut self) -> Option<&(LeafSeg<'a>, Value<Record>)> {
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
            data::{Index, IntlKey, IntlSeg, Key, LeafSeg, Record, Value, Ver},
            header::TagKind,
            refbox::{BaseView, BoxRef, BoxView},
            traits::{IAlloc, IHeader, IInlineSize, ILoader},
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
        fn pin_load(&self, addr: u64) -> BoxView {
            self.inner.map.get(&addr).unwrap().view()
        }

        fn pin(&self, data: BoxRef) {
            self.inner.raw().map.insert(data.header().addr, data);
        }

        fn shallow_copy(&self) -> Self {
            self.clone()
        }
    }

    impl IInlineSize for Allocator {
        fn inline_size(&self) -> u32 {
            2048
        }
    }

    #[derive(Clone, Copy)]
    struct LeafData<'a> {
        data: &'a [(LeafSeg<'a>, Value<Record>)],
        pos: usize,
    }

    impl<'a> Iterator for LeafData<'a> {
        type Item = (LeafSeg<'a>, Value<Record>);
        fn next(&mut self) -> Option<Self::Item> {
            if self.pos == self.data.len() {
                return None;
            }
            let index = self.pos;
            self.pos += 1;
            let (k, v) = self.data[index];
            Some((k, v))
        }
    }

    #[test]
    fn box_base() {
        let mut a = Allocator::new();
        let kv = [(IntlKey::new("mo".as_bytes()), Index::new(233))];
        let b = BaseView::new_intl(&mut a, "a".as_bytes(), None, NULL_PID, || {
            kv.iter().map(|&(k, v)| (IntlSeg::new(&[], k.raw), v))
        });

        let th = b.header();
        assert_eq!(th.kind, TagKind::Base);

        let empty = LeafData { data: &[], pos: 0 };
        let b = BaseView::new_leaf(&mut a, [].as_slice(), None, NULL_PID, || empty);
        let th = b.header();
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
                    LeafSeg::new(&[], "foo".as_bytes(), Ver::new(1, 1)),
                    Value::Put(Record::normal(1, "foo".as_bytes())),
                ),
                (
                    LeafSeg::new(&[], "foo".as_bytes(), Ver::new(2, 1)),
                    Value::Put(Record::normal(1, "bar".as_bytes())),
                ),
                (
                    LeafSeg::new(&[], "mo".as_bytes(), Ver::new(3, 1)),
                    Value::Put(Record::normal(1, "ha".as_bytes())),
                ),
            ],
            pos: 0,
        };
        let mut a = Allocator::new();
        let l = a.clone();

        let b = BaseView::new_leaf(&mut a, &[], None, NULL_PID, || kv);
        let base = b.view().as_base();
        let mut iter =
            base.range_iter::<Allocator, Key, Value<Record>>(&l, 0, base.header().elems as usize);

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

        let b = BaseView::new_intl(&mut a, &[], None, NULL_PID, || {
            kv.iter().map(|&(k, v)| (IntlSeg::new(&[], k.raw), v))
        });
        let base = b.view().as_base();
        let mut iter =
            base.range_iter::<Allocator, IntlKey, Index>(&l, 0, base.header().elems as usize);

        let (k, v) = iter.next().unwrap();
        test!(k, v, "foo", 1);

        let (k, v) = iter.next().unwrap();
        test!(k, v, "bar", 2);

        let (k, v) = iter.next().unwrap();
        test!(k, v, "baz", 3);

        assert!(iter.next().is_none());
    }
}
