use std::{cell::Cell, collections::VecDeque, ptr::null_mut};

use crate::{
    Options,
    types::{
        data::{Index, IntlKey, IntlSeg, Key, LeafSeg, Record, Val, Ver},
        header::{BaseHeader, NodeType, SLOT_LEN, SlotType, TagFlag, TagKind},
        refbox::{BaseView, BoxRef},
        sst::Sst,
        traits::{IAlloc, IBoxHeader, ICodec, IHeader, IKey, IKeyCodec, ILoader},
    },
    utils::{NULL_ADDR, NULL_PID},
};

impl BaseView {
    const HDR_LEN: usize = size_of::<BaseHeader>();

    pub(crate) fn null() -> Self {
        Self(null_mut())
    }

    pub(crate) fn is_null(&self) -> bool {
        self.0.is_null()
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

    pub(crate) fn new_leaf<'a, A, L, I>(
        a: &mut A,
        l: &L,
        lo: &[u8],
        hi: Option<&[u8]>,
        right_sibling: u64,
        iter: &mut I,
        txid: u64,
    ) -> BoxRef
    where
        A: IAlloc,
        L: ILoader,
        I: Iterator<Item = (LeafSeg<'a>, Val<'a>)>,
    {
        let inline_size = a.inline_size();
        let arena_size = a.arena_size();
        let mut elems = 0;
        let mut hints = VecDeque::new();
        let mut is_new_sibling = false;
        let mut sibling_frame_len = 0;
        let mut sib_frames = 0;
        let mut sib_real_size = 0;
        let mut last_raw: Option<LeafSeg<'a>> = None;
        let mut pos = 0;
        let mut payload_sz = 0;
        let mut seekable = SeekableIter::new();
        let prefix_len = Self::calc_prefix(lo, &hi);
        #[cfg(feature = "extra_check")]
        let mut last_k: Option<LeafSeg> = None;

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
            if let Some(ref raw) = last_raw
                && raw.raw_cmp(&k).is_eq()
            {
                if !is_new_sibling {
                    is_new_sibling = true;
                    sibling_frame_len = Self::HDR_LEN;
                    payload_sz += Val::calc_size(true, inline_size, v.data_size());
                    hints.push_back((pos - 1, 0));
                }
                let item_len =
                    Ver::len() + Val::calc_size(false, inline_size, v.data_size()) + SLOT_LEN;
                if sibling_frame_len + item_len > arena_size {
                    if sibling_frame_len == Self::HDR_LEN {
                        log::error!(
                            "sibling frame planning failed, arena_size {}, item_len {}",
                            arena_size,
                            item_len
                        );
                        panic!("sibling frame planning failed");
                    }
                    sib_frames += 1;
                    sib_real_size += BoxRef::real_size(sibling_frame_len as u32) as usize;
                    sibling_frame_len = Self::HDR_LEN;
                }
                if sibling_frame_len + item_len > arena_size {
                    log::error!(
                        "sibling frame planning failed after split, arena_size {}, item_len {}",
                        arena_size,
                        item_len
                    );
                    panic!("sibling frame planning failed");
                }
                sibling_frame_len += item_len;

                let (_, cnt) = hints.back_mut().unwrap();
                *cnt += 1;
                pos += 1;
                continue;
            }

            if is_new_sibling {
                if sibling_frame_len <= Self::HDR_LEN {
                    log::error!(
                        "sibling frame planning empty frame, arena_size {}",
                        arena_size
                    );
                    panic!("sibling frame planning failed");
                }
                sib_frames += 1;
                sib_real_size += BoxRef::real_size(sibling_frame_len as u32) as usize;
                sibling_frame_len = 0;
            }

            pos += 1;
            last_raw = Some(k);
            is_new_sibling = false;
            payload_sz += k.packed_size();
            let vsz = v.data_size();
            payload_sz += Val::calc_size(false, inline_size, vsz);
            elems += 1;
        }

        if is_new_sibling {
            if sibling_frame_len <= Self::HDR_LEN {
                log::error!(
                    "sibling frame planning empty tail frame, arena_size {}",
                    arena_size
                );
                panic!("sibling frame planning failed");
            }
            sib_frames += 1;
            sib_real_size += BoxRef::real_size(sibling_frame_len as u32) as usize;
        }

        let hdr_sz = elems * SLOT_LEN + Self::HDR_LEN;
        let boundary = lo.len() + hi.map_or(0, |x| x.len());
        let base_real_size = BoxRef::real_size((hdr_sz + payload_sz + boundary) as u32) as usize;
        let total_frames = 1 + sib_frames;
        let total_real_size = base_real_size + sib_real_size;

        a.begin_packed_alloc(total_real_size as u32, total_frames as u32);

        let b = Self::alloc::<false, _>(
            a,
            hdr_sz + payload_sz,
            lo,
            hi,
            elems,
            right_sibling,
            prefix_len,
            txid,
        );

        let mut base = b.view().as_base();
        let mut has_multiple_versions = false;
        let mut builder = Builder::from(base.data_mut(), elems);
        builder.setup_boundary_keys(lo, hi);

        pos = 0;
        loop {
            let Some((k, v)) = seekable.next() else {
                break;
            };

            let (r, _) = v.get_record(l, true);
            let remote = v.get_remote();
            let Some((idx, _)) = hints.front() else {
                builder.add_leaf(inline_size, k, &r, NULL_ADDR, remote);
                pos += 1;
                continue;
            };

            if pos == *idx {
                let (_, cnt) = hints.pop_front().unwrap();
                let sib_addr = Self::save_versions(a, l, cnt as usize, &mut pos, &seekable, txid);
                has_multiple_versions = true;

                builder.add_leaf(inline_size, k, &r, sib_addr, remote);
            } else {
                builder.add_leaf(inline_size, k, &r, NULL_ADDR, remote);
            }
            pos += 1;
        }
        base.header_mut().has_multiple_versions = has_multiple_versions;
        a.end_packed_alloc();
        b
    }

    pub(crate) fn new_intl<'a, A, I, F>(
        a: &mut A,
        lo: &[u8],
        hi: Option<&[u8]>,
        sibling: u64,
        f: F,
        txid: u64,
    ) -> BoxRef
    where
        A: IAlloc,
        I: Iterator<Item = (IntlSeg<'a>, Index)>,
        F: Fn() -> I,
    {
        let prefix_len = Self::calc_prefix(lo, &hi);
        let mut elems = 0;
        let sz: usize = f()
            .map(|(k, v)| {
                elems += 1;
                let ksz = k.remove_prefix(prefix_len).packed_size();
                let vsz = v.packed_size();
                ksz + vsz
            })
            .sum();

        let hdr_sz = elems * SLOT_LEN + Self::HDR_LEN;
        let b = Self::alloc::<true, _>(a, hdr_sz + sz, lo, hi, elems, sibling, prefix_len, txid);
        let mut base = b.view().as_base();
        let mut builder = Builder::from(base.data_mut(), elems);
        builder.setup_boundary_keys(lo, hi);

        let iter = f();
        for (k, v) in iter {
            let k = k.remove_prefix(prefix_len);
            builder.add_intl(&k, &v);
        }

        b
    }

    fn save_versions<'a, A: IAlloc, L: ILoader>(
        a: &mut A,
        l: &L,
        mut cnt: usize,
        pos: &mut u32,
        iter: &SeekableIter<'a>,
        txid: u64,
    ) -> u64 {
        let inline_size = a.inline_size();
        let arena_size = a.arena_size();
        let mut head = None;
        let mut tail: Option<BaseView> = None;
        let mut beg = iter.curr_pos();

        while cnt > 0 {
            let saved = beg;
            let mut len = Self::HDR_LEN;
            while cnt > 0 {
                let (_, v) = iter.next().unwrap();
                let sz = Ver::len() + Val::calc_size(false, inline_size, v.data_size());
                let tmp = len + sz + SLOT_LEN;
                if tmp > arena_size {
                    break;
                }
                len = tmp;
                beg += 1;
                cnt -= 1;
            }
            iter.seek_to(saved);

            let b = Self::alloc::<false, _>(a, len, &[], None, beg - saved, NULL_PID, 0, txid);
            // NOTE: this is the only place to set flag to Sibling
            let mut base = b.view().as_base();
            base.box_header_mut().flag = TagFlag::Sibling;
            let mut builder = Builder::from(base.data_mut(), beg - saved);
            builder.setup_boundary_keys(&[], None);

            for _ in saved..beg {
                let (k, v) = iter.next().unwrap();
                let (r, _) = v.get_record(l, true);
                builder.add_leaf(inline_size, &k.ver, &r, NULL_ADDR, v.get_remote());
                *pos += 1;
            }

            let addr = base.box_header().addr;

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
        right_sibling: u64,
        prefix_len: usize,
        txid: u64,
    ) -> BoxRef {
        let hi_len = hi.map_or(0, |x| x.len());

        // we don't use slot to encode lo/hi length, since the length has been saved in header
        // and the lo/hi keys are both stored in sst directly discard it's length
        size += lo.len() + hi.map_or(0, |x| x.len());

        let mut p = a.allocate(size as u32);
        let h = p.header_mut();
        h.kind = TagKind::Base;
        h.link = NULL_ADDR;
        h.txid = txid;
        h.node_type = if IS_INDEX {
            NodeType::Intl
        } else {
            NodeType::Leaf
        };

        let mut b = p.view().as_base();
        let h = b.header_mut();
        h.elems = elems as u16;
        h.size = size as u32;
        h.right_sibling = right_sibling;
        h.lo_len = lo.len() as u32;
        h.hi_len = hi_len as u32;
        h.split_elems = 0;
        h.prefix_len = prefix_len as u32;
        h.is_index = IS_INDEX;
        h.padding = 0;

        p
    }

    pub(crate) fn merge<A: IAlloc, L: ILoader>(
        &self,
        a: &mut A,
        loader: &L,
        other: Self,
        txid: u64,
    ) -> BoxRef {
        let hdr1 = self.header();
        let hdr2 = other.header();
        assert_eq!(hdr1.is_index, hdr2.is_index);

        let lo1 = self.lo();
        let lo2 = other.lo();
        let hi = other.hi();
        let sibling = hdr2.right_sibling;
        let elems1 = hdr1.elems as usize;
        let elems2 = hdr2.elems as usize;
        let pl1 = hdr1.prefix_len as usize;
        let pl2 = hdr2.prefix_len as usize;

        if hdr1.is_index {
            let f = || {
                let l = self.range_iter::<L, IntlKey>(loader, 0, elems1);
                let r = other.range_iter::<L, IntlKey>(loader, 0, elems2);
                FuseBaseIter::new(&lo1[..pl1], &lo2[..pl2], l, r)
            };
            Self::new_intl(a, lo1, hi, sibling, f, txid)
        } else {
            let l = self.range_iter::<L, Key>(loader, 0, elems1);
            let r = other.range_iter::<L, Key>(loader, 0, elems2);
            let mut iter = FuseBaseIter::new(&lo1[..pl1], &lo2[..pl2], l, r);
            Self::new_leaf(a, loader, lo1, hi, sibling, &mut iter, txid)
        }
    }

    /// the data means slot + hi/lo + key-value, excluding header and remotes
    fn data_mut(&mut self) -> &mut [u8] {
        let h = self.header();
        unsafe {
            std::slice::from_raw_parts_mut(
                self.0.add(1).cast::<u8>(),
                h.size as usize - Self::HDR_LEN,
            )
        }
    }

    fn data(&self) -> &[u8] {
        let h = self.header();
        unsafe {
            std::slice::from_raw_parts(self.0.add(1).cast::<u8>(), h.size as usize - Self::HDR_LEN)
        }
    }

    pub(crate) fn lo(&self) -> &[u8] {
        let h = self.header();
        if h.lo_len == 0 {
            return &[];
        }
        let off = h.elems as usize * SLOT_LEN;
        &self.data()[off..off + h.lo_len as usize]
    }

    pub(crate) fn hi(&self) -> Option<&[u8]> {
        let h = self.header();
        if h.hi_len == 0 {
            return None;
        }
        let off = h.elems as usize * SLOT_LEN;
        let sz = h.lo_len + h.hi_len;
        Some(&self.data()[off + h.lo_len as usize..off + sz as usize])
    }

    pub(crate) fn range_iter<'a, L, K>(
        &'a self,
        l: &'a L,
        beg: usize,
        end: usize,
    ) -> BaseIter<'a, L, K>
    where
        L: ILoader,
        K: IKey,
    {
        if self.is_null() {
            BaseIter::<L, K>::new(l, self.sst::<K>(), 0, 0)
        } else {
            BaseIter::<L, K>::new(l, self.sst::<K>(), beg, end)
        }
    }

    pub(crate) fn sst<K>(&self) -> Sst<K> {
        Sst::<K>::new(self.0)
    }
}

pub(crate) struct BaseIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    pub loader: &'a L,
    sst: Sst<K>,
    beg: usize,
    end: usize,
    sib_key: &'a [u8],
    sibling: Option<BaseView>,
    sibling_pos: usize,
}

impl<'a, L, K> BaseIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    fn new(loader: &'a L, sst: Sst<K>, beg: usize, end: usize) -> Self {
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

impl<'a, L> Iterator for BaseIter<'a, L, Key<'a>>
where
    L: ILoader,
{
    type Item = (Key<'a>, Val<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(p) = self.sibling.as_ref() {
            if self.sibling_pos < p.header().elems as usize {
                let (ver, val) = p.sst::<Ver>().kv_at(self.sibling_pos);
                self.sibling_pos += 1;
                let k = Key::new(self.sib_key, ver);
                return Some((k, val));
            } else {
                let link = p.box_header().link;
                self.sibling_pos = 0;
                if link != NULL_ADDR {
                    self.sibling = Some(self.loader.load_unchecked(link).as_base());
                    continue;
                }
                self.sibling.take();
            }
        }
        if self.beg < self.end {
            let (k, v) = self.sst.kv_at::<Val>(self.beg);
            self.beg += 1;
            if let Some(addr) = v.get_sibling() {
                self.sib_key = k.raw;
                self.sibling = Some(self.loader.load_unchecked(addr).as_base());
                self.sibling_pos = 0;
                // the sibling is still in `v`
                Some((k, v))
            } else {
                Some((k, v))
            }
        } else {
            None
        }
    }
}

impl<'a, L> Iterator for BaseIter<'a, L, IntlKey<'a>>
where
    L: ILoader,
{
    type Item = (IntlKey<'a>, Index);

    fn next(&mut self) -> Option<Self::Item> {
        if self.beg < self.end {
            let (k, v) = self.sst.kv_at(self.beg);
            self.beg += 1;
            Some((k, v))
        } else {
            None
        }
    }
}

struct FuseBaseIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    prefix1: &'a [u8],
    prefix2: &'a [u8],
    iter1: BaseIter<'a, L, K>,
    iter2: BaseIter<'a, L, K>,
}

impl<'a, L, K> FuseBaseIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    fn new(
        prefix1: &'a [u8],
        prefix2: &'a [u8],
        i1: BaseIter<'a, L, K>,
        i2: BaseIter<'a, L, K>,
    ) -> Self {
        Self {
            prefix1,
            prefix2,
            iter1: i1,
            iter2: i2,
        }
    }
}

impl<'a, L> Iterator for FuseBaseIter<'a, L, Key<'a>>
where
    L: ILoader,
{
    type Item = (LeafSeg<'a>, Val<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.iter1.next() {
            Some((LeafSeg::new(self.prefix1, k.raw, k.ver), v))
        } else {
            self.iter2
                .next()
                .map(|(k, v)| (LeafSeg::new(self.prefix2, k.raw, k.ver), v))
        }
    }
}

impl<'a, L> Iterator for FuseBaseIter<'a, L, IntlKey<'a>>
where
    L: ILoader,
{
    type Item = (IntlSeg<'a>, Index);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.iter1.next() {
            Some((IntlSeg::new(self.prefix1, k.raw), v))
        } else {
            self.iter2
                .next()
                .map(|(k, v)| (IntlSeg::new(self.prefix2, k.raw), v))
        }
    }
}

/// the layout of sst:
/// ```text
/// +---------+-------------+---------+--------+----------+
/// | header  | index table | low key | hi key |key-value |
/// +---------+-------------+---------+--------+----------+
/// ```
struct Builder<'a> {
    slots: &'a mut [SlotType],
    payload: &'a mut [u8],
    pos: usize,
    index: usize,
    offset: usize,
}

impl<'a> Builder<'a> {
    /// the `s` is starts from index table
    fn from(s: &'a mut [u8], elems: usize) -> Self {
        let offset = elems * SLOT_LEN;
        let slots = unsafe {
            let p = s.as_mut_ptr().cast::<SlotType>();
            std::slice::from_raw_parts_mut(p, elems)
        };
        Self {
            slots,
            payload: s,
            pos: BaseView::HDR_LEN,
            index: 0,
            offset,
        }
    }

    fn setup_boundary_keys(&mut self, lo: &[u8], hi: Option<&[u8]>) {
        let hi_len = hi.map_or(0, |x| x.len());
        let boundary_len = lo.len() + hi_len;

        if boundary_len > 0 {
            self.payload[self.offset..self.offset + lo.len()].copy_from_slice(lo);
            self.offset += lo.len();
            self.payload[self.offset..self.offset + hi_len].copy_from_slice(hi.map_or(&[], |x| x));
            self.offset += hi_len;
        }
    }

    #[inline]
    fn slice(&mut self, b: usize, len: usize) -> &mut [u8] {
        &mut self.payload[b..b + len]
    }

    fn add_intl<K>(&mut self, k: &K, v: &Index)
    where
        K: ICodec,
    {
        let (ksz, vsz) = (k.packed_size(), v.packed_size());
        k.encode_to(self.slice(self.offset, ksz));
        v.encode_to(self.slice(self.offset + ksz, vsz));

        self.update_slot(ksz + vsz);
    }

    fn add_leaf<K>(&mut self, limit: usize, k: &K, v: &Record, sib: u64, remote: u64)
    where
        K: ICodec,
    {
        let ksz = k.packed_size();
        let vsz = v.packed_size();
        let val_sz = Val::calc_size(sib != NULL_ADDR, limit, vsz);

        k.encode_to(self.slice(self.offset, ksz));
        if vsz <= limit {
            #[cfg(feature = "extra_check")]
            assert_eq!(remote, NULL_ADDR);
            Val::encode_inline(self.slice(self.offset + ksz, val_sz), sib, v);
        } else {
            Val::encode_remote(self.slice(self.offset + ksz, val_sz), sib, remote, v);
        }

        self.update_slot(ksz + val_sz);
    }

    #[inline(always)]
    fn update_slot(&mut self, total_size: usize) {
        self.slots[self.index] = (self.pos + self.offset) as SlotType;
        self.index += 1;
        self.offset += total_size;
    }
}

struct SeekableIter<'a> {
    data: Vec<(LeafSeg<'a>, Val<'a>)>,
    index: Cell<usize>,
}

impl<'a> SeekableIter<'a> {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(Options::MAX_SPLIT_ELEMS as usize * 2),
            index: Cell::new(0),
        }
    }

    fn add(&mut self, k: LeafSeg<'a>, v: Val<'a>) {
        self.data.push((k, v));
    }

    fn seek_to(&self, pos: usize) {
        self.index.set(pos);
    }

    fn curr_pos(&self) -> usize {
        self.index.get()
    }

    fn next(&self) -> Option<&(LeafSeg<'a>, Val<'a>)> {
        let idx = self.index.get();
        if idx < self.data.len() {
            self.index.set(idx + 1);
            Some(&self.data[idx])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        Options,
        types::{
            data::{Index, IntlKey, IntlSeg, Key, LeafSeg, Record, Val, Ver},
            header::TagKind,
            refbox::{BaseView, BoxRef, BoxView},
            traits::{IAlloc, ICodec, IHeader, ILoader},
        },
        utils::{MutRef, NULL_ADDR, NULL_ORACLE, NULL_PID},
    };
    use std::{cell::Cell, collections::HashMap};

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
        fn allocate(&mut self, size: u32) -> BoxRef {
            let r = &mut self.inner;
            let old = r.off.get();
            r.off.set(old + size as u64);
            let b = BoxRef::alloc(size, old);
            r.map.insert(old, b.clone());
            b
        }

        fn collect(&mut self, _addr: &[u64]) {}

        fn arena_size(&mut self) -> usize {
            1 << 20
        }

        fn inline_size(&self) -> usize {
            Options::INLINE_SIZE
        }
    }

    impl ILoader for Allocator {
        fn load(&self, addr: u64) -> Result<BoxView, crate::OpCode> {
            Ok(self.inner.map.get(&addr).unwrap().view())
        }

        fn pin(&self, data: BoxRef) {
            self.inner.raw_ref().map.insert(data.header().addr, data);
        }

        fn shallow_copy(&self) -> Self {
            self.clone()
        }

        fn deep_copy(&self) -> Self {
            self.clone()
        }

        fn load_remote(&self, addr: u64) -> Result<BoxRef, crate::OpCode> {
            Ok(self.inner.map.get(&addr).unwrap().clone())
        }
    }

    #[derive(Clone, Copy)]
    struct LeafData<'a> {
        data: &'a [(LeafSeg<'a>, Val<'a>)],
        pos: usize,
    }

    impl<'a> Iterator for LeafData<'a> {
        type Item = (LeafSeg<'a>, Val<'a>);
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
        let b = BaseView::new_intl(
            &mut a,
            "a".as_bytes(),
            None,
            NULL_PID,
            || kv.iter().map(|&(k, v)| (IntlSeg::new(&[], k.raw), v)),
            NULL_ORACLE,
        );

        let th = b.header();
        assert_eq!(th.kind, TagKind::Base);

        let mut empty = LeafData { data: &[], pos: 0 };
        let l = a.clone();
        let b = BaseView::new_leaf(
            &mut a,
            &l,
            [].as_slice(),
            None,
            NULL_PID,
            &mut empty,
            NULL_ORACLE,
        );
        let th = b.header();
        assert_eq!(th.kind, TagKind::Base);
    }

    #[test]
    fn builder_leaf() {
        #[allow(clippy::too_many_arguments)]
        fn check(k: &Key, v: &Val, l: &Allocator, ks: &str, txid: u64, cmd: u32, vs: &str, w: u8) {
            assert_eq!(k.raw, ks.as_bytes());
            assert_eq!(k.txid, txid);
            assert_eq!(k.cmd, cmd);
            let (r, _) = v.get_record(l, true);
            assert_eq!(r.data(), vs.as_bytes());
            assert_eq!(r.group_id(), w);
        }

        fn gen_val(a: &mut Allocator, r: Record) -> Val<'static> {
            let sz = Val::calc_size(false, a.inline_size(), r.packed_size());
            let mut b = a.allocate(sz as u32);
            Val::encode_inline(b.data_slice_mut::<u8>(), NULL_ADDR, &r);
            Val::from_raw(unsafe { std::mem::transmute::<&[u8], &[u8]>(b.data_slice::<u8>()) })
        }

        let mut a = Allocator::new();
        // NOTE: the kv should be ordered
        let mut kv = LeafData {
            data: &[
                (
                    LeafSeg::new(&[], "foo".as_bytes(), Ver::new(2, 1)),
                    gen_val(&mut a, Record::normal(1, "bar".as_bytes())),
                ),
                (
                    LeafSeg::new(&[], "foo".as_bytes(), Ver::new(1, 1)),
                    gen_val(&mut a, Record::normal(1, "foo".as_bytes())),
                ),
                (
                    LeafSeg::new(&[], "mo".as_bytes(), Ver::new(3, 1)),
                    gen_val(&mut a, Record::normal(1, "ha".as_bytes())),
                ),
            ],
            pos: 0,
        };
        let l = a.clone();

        let b = BaseView::new_leaf(&mut a, &l, &[], None, NULL_PID, &mut kv, NULL_ORACLE);
        let base = b.view().as_base();
        let mut iter = base.range_iter::<Allocator, Key>(&l, 0, base.header().elems as usize);

        let (k, v) = iter.next().unwrap();

        check(&k, &v, &l, "foo", 2, 1, "bar", 1);

        assert!(v.get_sibling().is_some());

        iter.next(); // skip the expanded sibling

        let (k, v) = iter.next().unwrap();
        check(&k, &v, &l, "mo", 3, 1, "ha", 1);

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

        let b = BaseView::new_intl(
            &mut a,
            &[],
            None,
            NULL_PID,
            || kv.iter().map(|&(k, v)| (IntlSeg::new(&[], k.raw), v)),
            NULL_ORACLE,
        );
        let base = b.view().as_base();
        let mut iter = base.range_iter::<Allocator, IntlKey>(&l, 0, base.header().elems as usize);

        let (k, v) = iter.next().unwrap();
        test!(k, v, "foo", 1);

        let (k, v) = iter.next().unwrap();
        test!(k, v, "bar", 2);

        let (k, v) = iter.next().unwrap();
        test!(k, v, "baz", 3);

        assert!(iter.next().is_none());
    }
}
