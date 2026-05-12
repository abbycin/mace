use std::{cell::Cell, collections::VecDeque, ptr::null_mut};

use crate::{
    Options,
    types::{
        data::{HistRef, Index, IntlKey, IntlSeg, Key, LeafSeg, Record, Val, Ver},
        header::{BaseHeader, NodeType, SLOT_LEN, SlotType, TagFlag, TagKind},
        refbox::{BaseView, BoxRef},
        sst::Sst,
        traits::{IBoxHeader, ICodec, IFrameAlloc, IHeader, IKey, IKeyCodec, ILoader},
    },
    utils::{NULL_ADDR, NULL_PID, OpCode, data::Position},
};

impl BaseView {
    #[cfg(not(test))]
    const HIST_PAGE_BUDGET: usize = 128 * 1024;
    #[cfg(test)]
    const TEST_HIST_PAGE_BUDGET: usize = 256;
    const HDR_LEN: usize = size_of::<BaseHeader>();
    const SIBLING_HINT_CNT_LEN: usize = size_of::<u32>();
    const SIBLING_HINT_ADDR_LEN: usize = size_of::<u64>();
    const REMOTE_HINT_CNT_LEN: usize = size_of::<u32>();
    const REMOTE_HINT_ADDR_LEN: usize = size_of::<u64>();

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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_leaf<'a, A, L, I>(
        a: &mut A,
        l: &L,
        lo: &[u8],
        hi: Option<&[u8]>,
        right_sibling: u64,
        iter: &mut I,
        txid: u64,
        group: u8,
        lsn: Position,
    ) -> BoxRef
    where
        A: IFrameAlloc,
        L: ILoader,
        I: Iterator<Item = (LeafSeg<'a>, Val<'a>)>,
    {
        let lsn = lsn.max(a.checkpoint_lsn(group));
        let inline_size = a.inline_size();
        let mut elems = 0;
        let mut hints: VecDeque<(usize, usize)> = VecDeque::new();
        let mut is_new_sibling = false;
        let mut remote_hint_cnt = 0usize;
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
                    // merge path may interleave segments from different prefixes
                    assert!(x.cmp(&ks).is_lt());
                }
                last_k = Some(ks);
            }

            let k = ks.remove_prefix(prefix_len);
            seekable.add(k, v);
            if v.get_remote() != NULL_ADDR {
                remote_hint_cnt += 1;
            }
            if let Some(ref raw) = last_raw
                && raw.raw_cmp(&k).is_eq()
            {
                if !is_new_sibling {
                    is_new_sibling = true;
                    payload_sz += Val::calc_size(true, inline_size, v.data_size());
                    hints.push_back((pos - 1, 0));
                }

                let (_, cnt) = hints.back_mut().unwrap();
                *cnt += 1;
                pos += 1;
                continue;
            }

            pos += 1;
            last_raw = Some(k);
            is_new_sibling = false;
            payload_sz += k.packed_size();
            let vsz = v.data_size();
            payload_sz += Val::calc_size(false, inline_size, vsz);
            elems += 1;
        }

        let hdr_sz = elems * SLOT_LEN + Self::HDR_LEN;
        // history region refs for all sibling slots in Node
        let mut sibling_refs = VecDeque::with_capacity(hints.len());
        let mut sibling_hint_addrs = Vec::new();
        let mut remote_hint_addrs = Vec::with_capacity(remote_hint_cnt);
        if !hints.is_empty() {
            sibling_refs = Self::save_hist_regions(
                a,
                l,
                &hints,
                &seekable,
                txid,
                group,
                lsn,
                &mut sibling_hint_addrs,
                &mut remote_hint_addrs,
            );
        }
        let hint_sz = Self::leaf_hint_size(sibling_hint_addrs.len(), remote_hint_cnt);
        seekable.seek_to(0);

        let b = Self::try_alloc::<false, _>(
            a,
            hdr_sz + payload_sz,
            hint_sz,
            lo,
            hi,
            elems,
            right_sibling,
            prefix_len,
            txid,
            group,
            lsn,
        );

        let mut base = b.view().as_base();
        let mut has_multiple_versions = false;
        let mut builder = Builder::from(base.data_mut(), elems);
        builder.setup_boundary_keys(lo, hi);

        pos = 0;
        while let Some((k, v)) = seekable.next() {
            let (r, _) = v.get_record(l, true);
            let remote = v.get_remote();
            if remote != NULL_ADDR {
                remote_hint_addrs.push(remote);
            }
            let mut hist = None;
            if let Some((idx, cnt)) = hints.front().copied()
                && pos == idx
            {
                hints.pop_front().expect("must exist");
                hist = Some(sibling_refs.pop_front().expect("must exist"));
                has_multiple_versions = true;
                if cnt != 0 {
                    seekable.seek_to(pos + cnt + 1);
                    pos += cnt;
                }
            }
            builder.add_leaf(inline_size, k, &r, hist, remote);
            pos += 1;
        }
        debug_assert!(hints.is_empty());
        debug_assert!(sibling_refs.is_empty());
        base.header_mut().has_multiple_versions = has_multiple_versions;
        if !sibling_hint_addrs.is_empty() || !remote_hint_addrs.is_empty() {
            base.write_leaf_hints(&sibling_hint_addrs, &remote_hint_addrs);
        }
        b
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_intl<'a, A, I, F>(
        a: &mut A,
        lo: &[u8],
        hi: Option<&[u8]>,
        sibling: u64,
        f: F,
        txid: u64,
        group: u8,
        lsn: Position,
    ) -> BoxRef
    where
        A: IFrameAlloc,
        I: Iterator<Item = (IntlSeg<'a>, Index)>,
        F: Fn() -> I,
    {
        let lsn = lsn.max(a.checkpoint_lsn(group));
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
        let b = Self::try_alloc::<true, _>(
            a,
            hdr_sz + sz,
            0,
            lo,
            hi,
            elems,
            sibling,
            prefix_len,
            txid,
            group,
            lsn,
        );
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

    #[allow(clippy::too_many_arguments)]
    fn save_hist_regions<'a, A: IFrameAlloc, L: ILoader>(
        a: &mut A,
        l: &L,
        hints: &VecDeque<(usize, usize)>,
        iter: &SeekableIter<'a>,
        txid: u64,
        group: u8,
        lsn: Position,
        hint_addrs: &mut Vec<u64>,
        remote_hint_addrs: &mut Vec<u64>,
    ) -> VecDeque<HistRef> {
        struct HistRegion {
            start: usize,
            count: usize,
        }

        if hints.is_empty() {
            return VecDeque::new();
        }

        let mut regions = Vec::with_capacity(hints.len());
        let mut old_vers = Vec::new();
        for &(idx, cnt) in hints {
            debug_assert!(cnt > 0);
            let start = old_vers.len();
            iter.seek_to(idx + 1);
            for _ in 0..cnt {
                let (k, v) = iter.next().expect("must exist");
                old_vers.push((k.ver, *v));
            }
            regions.push(HistRegion { start, count: cnt });
        }
        if old_vers.is_empty() {
            return VecDeque::new();
        }

        let inline_size = a.inline_size();
        let frame_budget = Self::hist_page_budget();
        let mut page_ranges = Vec::new();
        let mut beg = 0usize;

        while beg < old_vers.len() {
            let saved = beg;
            let mut len = Self::HDR_LEN;
            while beg < old_vers.len() {
                let (_, v) = old_vers[beg];
                let sz = Ver::len() + Val::calc_size(false, inline_size, v.data_size());
                let tmp = len + sz + SLOT_LEN;
                if tmp > frame_budget {
                    break;
                }
                len = tmp;
                beg += 1;
            }
            if saved == beg {
                beg += 1;
            }
            page_ranges.push((saved, beg));
        }

        let mut entry_loc = vec![(0usize, 0u16); old_vers.len()];
        for (pid, &(start, end)) in page_ranges.iter().enumerate() {
            for (slot, i) in (start..end).enumerate() {
                entry_loc[i] = (pid, slot as u16);
            }
        }

        let mut refs_idx = VecDeque::with_capacity(regions.len());
        for region in &regions {
            let (pid, slot) = entry_loc[region.start];
            refs_idx.push_back((pid, slot, region.count as u32));
        }

        let mut tail: Option<BaseView> = None;
        let mut page_addrs = Vec::with_capacity(page_ranges.len());
        for &(start, end) in &page_ranges {
            let mut len = Self::HDR_LEN;
            for (_, v) in old_vers.iter().take(end).skip(start) {
                let sz = Ver::len() + Val::calc_size(false, inline_size, v.data_size());
                len += sz + SLOT_LEN;
            }
            let b = Self::try_alloc::<false, _>(
                a,
                len,
                0,
                &[],
                None,
                end - start,
                NULL_PID,
                0,
                txid,
                group,
                lsn,
            );
            // NOTE: this is the only place to set flag to Sibling
            let mut base = b.view().as_base();
            base.box_header_mut().flag = TagFlag::Sibling;
            let mut builder = Builder::from(base.data_mut(), end - start);
            builder.setup_boundary_keys(&[], None);

            for (k, v) in old_vers[start..end].iter() {
                let (r, _) = v.get_record(l, true);
                let remote = v.get_remote();
                if remote != NULL_ADDR {
                    remote_hint_addrs.push(remote);
                }
                builder.add_leaf(inline_size, k, &r, None, remote);
            }

            let addr = base.box_header().addr;
            hint_addrs.push(addr);
            page_addrs.push(addr);

            if let Some(mut last) = tail {
                last.box_header_mut().link = addr;
            }
            tail = Some(base);
        }
        refs_idx
            .into_iter()
            .map(|(pid, slot, count)| HistRef::new(page_addrs[pid], slot, count))
            .collect()
    }

    #[inline]
    const fn hist_page_budget() -> usize {
        #[cfg(test)]
        {
            Self::TEST_HIST_PAGE_BUDGET
        }
        #[cfg(not(test))]
        {
            Self::HIST_PAGE_BUDGET
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn try_alloc<const IS_INDEX: bool, A: IFrameAlloc>(
        a: &mut A,
        mut size: usize,
        extra_payload: usize,
        lo: &[u8],
        hi: Option<&[u8]>,
        elems: usize,
        right_sibling: u64,
        prefix_len: usize,
        txid: u64,
        group: u8,
        lsn: Position,
    ) -> BoxRef {
        let hi_len = hi.map_or(0, |x| x.len());

        // we don't use slot to encode lo/hi length, since the length has been saved in header
        // and the lo/hi keys are both stored in sst directly discard it's length
        size += lo.len() + hi.map_or(0, |x| x.len());

        let mut p = a.alloc((size + extra_payload) as u32);
        let h = p.header_mut();
        h.kind = TagKind::Base;
        h.link = NULL_ADDR;
        h.txid = txid;
        h.group = group;
        h.lsn = lsn;
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
        h.merging_child = NULL_ADDR;
        h.merging = false;
        h.prefix_len = prefix_len as u32;
        h.is_index = IS_INDEX;
        h.padding = 0;

        p
    }

    const fn sibling_hint_size(nr_heads: usize) -> usize {
        Self::SIBLING_HINT_CNT_LEN + nr_heads * Self::SIBLING_HINT_ADDR_LEN
    }

    const fn leaf_hint_size(nr_heads: usize, nr_remotes: usize) -> usize {
        if nr_heads == 0 && nr_remotes == 0 {
            0
        } else {
            Self::sibling_hint_size(nr_heads)
                + Self::REMOTE_HINT_CNT_LEN
                + nr_remotes * Self::REMOTE_HINT_ADDR_LEN
        }
    }

    fn write_leaf_hints(&mut self, heads: &[u64], remotes: &[u64]) {
        debug_assert!(!self.header().is_index);
        let payload = self.box_header().payload_size as usize;
        let off = self.header().size as usize;
        let need = off + Self::leaf_hint_size(heads.len(), remotes.len());
        assert!(payload >= need);

        let p = self.0.cast::<u8>();
        unsafe {
            p.add(off).cast::<u32>().write_unaligned(heads.len() as u32);
            let sibling_off = off + Self::SIBLING_HINT_CNT_LEN;
            if !heads.is_empty() {
                let src = heads.as_ptr().cast::<u8>();
                let dst = p.add(sibling_off);
                std::ptr::copy_nonoverlapping(src, dst, heads.len() * Self::SIBLING_HINT_ADDR_LEN);
            }
            let remote_cnt_off = sibling_off + heads.len() * Self::SIBLING_HINT_ADDR_LEN;
            p.add(remote_cnt_off)
                .cast::<u32>()
                .write_unaligned(remotes.len() as u32);
            if !remotes.is_empty() {
                let src = remotes.as_ptr().cast::<u8>();
                let dst = p.add(remote_cnt_off + Self::REMOTE_HINT_CNT_LEN);
                std::ptr::copy_nonoverlapping(src, dst, remotes.len() * Self::REMOTE_HINT_ADDR_LEN);
            }
        }
    }

    fn leaf_hint_layout(&self) -> Option<(usize, usize, usize)> {
        debug_assert!(!self.header().is_index);
        let payload = self.box_header().payload_size as usize;
        let off = self.header().size as usize;
        if payload == off {
            return None;
        }
        assert!(payload >= off + Self::SIBLING_HINT_CNT_LEN);
        let p = self.0.cast::<u8>();
        let cnt = unsafe { p.add(off).cast::<u32>().read_unaligned() as usize };
        let remote_cnt_off = off + Self::sibling_hint_size(cnt);
        assert!(payload >= remote_cnt_off + Self::REMOTE_HINT_CNT_LEN);
        let remote_cnt = unsafe { p.add(remote_cnt_off).cast::<u32>().read_unaligned() as usize };
        let need =
            remote_cnt_off + Self::REMOTE_HINT_CNT_LEN + remote_cnt * Self::REMOTE_HINT_ADDR_LEN;
        assert!(payload >= need);
        Some((off, cnt, remote_cnt))
    }

    pub(crate) fn load_sibling_heads_hint(&self, out: &mut Vec<u64>) -> bool {
        let Some((off, cnt, _)) = self.leaf_hint_layout() else {
            return false;
        };
        if cnt == 0 {
            return false;
        }
        let p = self.0.cast::<u8>();
        let start = out.len();
        out.resize(start + cnt, 0);
        unsafe {
            let src = p.add(off + Self::SIBLING_HINT_CNT_LEN).cast::<u8>();
            let dst = out.as_mut_ptr().add(start).cast::<u8>();
            std::ptr::copy_nonoverlapping(src, dst, cnt * Self::SIBLING_HINT_ADDR_LEN);
        }
        true
    }

    pub(crate) fn load_remote_hints(&self, out: &mut Vec<u64>) -> bool {
        let Some((off, sibling_cnt, remote_cnt)) = self.leaf_hint_layout() else {
            return false;
        };
        if remote_cnt == 0 {
            return false;
        }
        let p = self.0.cast::<u8>();
        let remote_off = off + Self::sibling_hint_size(sibling_cnt) + Self::REMOTE_HINT_CNT_LEN;
        let start = out.len();
        out.resize(start + remote_cnt, 0);
        unsafe {
            let src = p.add(remote_off).cast::<u8>();
            let dst = out.as_mut_ptr().add(start).cast::<u8>();
            std::ptr::copy_nonoverlapping(src, dst, remote_cnt * Self::REMOTE_HINT_ADDR_LEN);
        }
        true
    }

    pub(crate) fn merge<A: IFrameAlloc, L: ILoader>(
        &self,
        a: &mut A,
        loader: &L,
        other: Self,
        txid: u64,
        group: u8,
        lsn: Position,
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
            Self::new_intl(a, lo1, hi, sibling, f, txid, group, lsn)
        } else {
            let l = self.range_iter::<L, Key>(loader, 0, elems1);
            let r = other.range_iter::<L, Key>(loader, 0, elems2);
            let mut iter = FuseBaseIter::new(&lo1[..pl1], &lo2[..pl2], l, r);
            Self::new_leaf(a, loader, lo1, hi, sibling, &mut iter, txid, group, lsn)
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
    hist: Option<HistIter<'a>>,
}

pub(crate) struct BaseRevIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    pub loader: &'a L,
    sst: Sst<K>,
    cur: isize,
    end: isize,
    hist: Option<HistIter<'a>>,
}

struct HistIter<'a> {
    key_raw: &'a [u8],
    page: BaseView,
    slot: usize,
    remaining: usize,
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
            hist: None,
        }
    }
}

impl<'a, L, K> BaseRevIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    pub(crate) fn new(loader: &'a L, sst: Sst<K>, start: isize, end: isize) -> Self {
        Self {
            loader,
            sst,
            cur: start,
            end,
            hist: None,
        }
    }
}

impl<'a, L> Iterator for BaseIter<'a, L, Key<'a>>
where
    L: ILoader,
{
    type Item = (Key<'a>, Val<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_with_sibling(|_| {})
    }
}

impl<'a, L> BaseIter<'a, L, Key<'a>>
where
    L: ILoader,
{
    pub(crate) fn try_next_with_sibling<F>(
        &mut self,
        mut on_sibling: F,
    ) -> Result<Option<(Key<'a>, Val<'a>)>, OpCode>
    where
        F: FnMut(u64),
    {
        while let Some(state) = self.hist.as_mut() {
            if state.remaining == 0 {
                self.hist = None;
                continue;
            }

            let elems = state.page.header().elems as usize;
            if state.slot >= elems {
                let link = state.page.box_header().link;
                if link == NULL_ADDR {
                    self.hist = None;
                    continue;
                }
                on_sibling(link);
                state.page = self.loader.load(link)?.as_base();
                state.slot = 0;
                continue;
            }

            let (ver, val) = state.page.sst::<Ver>().kv_at(state.slot);
            state.slot += 1;
            state.remaining -= 1;
            let k = Key::new(state.key_raw, ver);
            return Ok(Some((k, val)));
        }
        if self.beg < self.end {
            let (k, v) = self.sst.kv_at::<Val>(self.beg);
            self.beg += 1;
            if let Some(hist) = v.get_hist() {
                on_sibling(hist.page_addr);
                self.hist = Some(HistIter {
                    key_raw: k.raw,
                    page: self.loader.load(hist.page_addr)?.as_base(),
                    slot: hist.slot as usize,
                    remaining: hist.count as usize,
                });
                // the sibling is still in `v`
                Ok(Some((k, v)))
            } else {
                Ok(Some((k, v)))
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) fn next_with_sibling<F>(&mut self, mut on_sibling: F) -> Option<(Key<'a>, Val<'a>)>
    where
        F: FnMut(u64),
    {
        self.try_next_with_sibling(&mut on_sibling)
            .expect("must exist")
    }
}

impl<'a, L> BaseRevIter<'a, L, Key<'a>>
where
    L: ILoader,
{
    pub(crate) fn try_next_back_with_sibling<F>(
        &mut self,
        mut on_sibling: F,
    ) -> Result<Option<(Key<'a>, Val<'a>)>, OpCode>
    where
        F: FnMut(u64),
    {
        while let Some(state) = self.hist.as_mut() {
            if state.remaining == 0 {
                self.hist = None;
                continue;
            }

            let elems = state.page.header().elems as usize;
            if state.slot >= elems {
                let link = state.page.box_header().link;
                if link == NULL_ADDR {
                    self.hist = None;
                    continue;
                }
                on_sibling(link);
                state.page = self.loader.load(link)?.as_base();
                state.slot = 0;
                continue;
            }

            let (ver, val) = state.page.sst::<Ver>().kv_at(state.slot);
            state.slot += 1;
            state.remaining -= 1;
            let k = Key::new(state.key_raw, ver);
            return Ok(Some((k, val)));
        }

        if self.cur >= self.end {
            let pos = self.cur as usize;
            let (k, v) = self.sst.kv_at::<Val>(pos);
            self.cur -= 1;
            if let Some(hist) = v.get_hist() {
                on_sibling(hist.page_addr);
                self.hist = Some(HistIter {
                    key_raw: k.raw,
                    page: self.loader.load(hist.page_addr)?.as_base(),
                    slot: hist.slot as usize,
                    remaining: hist.count as usize,
                });
            }
            Ok(Some((k, v)))
        } else {
            Ok(None)
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

    fn add_leaf<K>(&mut self, limit: usize, k: &K, v: &Record, hist: Option<HistRef>, remote: u64)
    where
        K: ICodec,
    {
        let ksz = k.packed_size();
        let vsz = v.packed_size();
        let val_sz = Val::calc_size(hist.is_some(), limit, vsz);

        k.encode_to(self.slice(self.offset, ksz));
        if vsz <= limit {
            #[cfg(feature = "extra_check")]
            assert_eq!(remote, NULL_ADDR);
            Val::encode_inline(self.slice(self.offset + ksz, val_sz), hist, v);
        } else {
            Val::encode_remote(self.slice(self.offset + ksz, val_sz), hist, remote, v);
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
            traits::{IBoxHeader, ICodec, IFrameAlloc, IHeader, ILoader},
        },
        utils::{MutRef, NULL_ADDR, NULL_ORACLE, NULL_PID, data::Position},
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

    impl IFrameAlloc for Allocator {
        fn alloc(&mut self, size: u32) -> BoxRef {
            let r = &mut self.inner;
            let old = r.off.get();
            r.off.set(old + size as u64);
            let b = BoxRef::alloc(size, old);
            r.map.insert(old, b.clone());
            b
        }

        fn inline_size(&self) -> usize {
            Options::MIN_INLINE_SIZE
        }
    }

    impl ILoader for Allocator {
        fn load(&self, addr: u64) -> Result<BoxView, crate::OpCode> {
            Ok(self.inner.map.get(&addr).unwrap().view())
        }

        fn pin(&self, data: BoxRef) {
            self.inner.raw_ref().map.insert(data.header().addr, data);
        }

        fn copy_with_pin(&self) -> Self {
            self.clone()
        }

        fn copy_without_pin(&self) -> Self {
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
            0,
            Position::MIN,
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
            0,
            Position::MIN,
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
            let mut b = a.alloc(sz as u32);
            Val::encode_inline(b.data_slice_mut::<u8>(), None, &r);
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

        let b = BaseView::new_leaf(
            &mut a,
            &l,
            &[],
            None,
            NULL_PID,
            &mut kv,
            NULL_ORACLE,
            0,
            Position::MIN,
        );
        let base = b.view().as_base();
        let mut iter = base.range_iter::<Allocator, Key>(&l, 0, base.header().elems as usize);

        let (k, v) = iter.next().unwrap();

        check(&k, &v, &l, "foo", 2, 1, "bar", 1);

        let hist = v.get_hist().expect("must have history");
        assert!(hist.page_addr < b.header().addr);
        assert_eq!(hist.slot, 0);
        assert_eq!(hist.count, 1);

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
            0,
            Position::MIN,
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

    #[test]
    fn merge_leaf_with_different_prefix_lengths_keeps_distinct_raw_keys() {
        fn gen_val(a: &mut Allocator, r: Record) -> Val<'static> {
            let sz = Val::calc_size(false, a.inline_size(), r.packed_size());
            let mut b = a.alloc(sz as u32);
            Val::encode_inline(b.data_slice_mut::<u8>(), None, &r);
            Val::from_raw(unsafe { std::mem::transmute::<&[u8], &[u8]>(b.data_slice::<u8>()) })
        }

        let mut a = Allocator::new();
        let l = a.clone();

        let mut left_data = LeafData {
            data: &[(
                LeafSeg::new(&[], "aa1".as_bytes(), Ver::new(3, 1)),
                gen_val(&mut a, Record::normal(1, "left".as_bytes())),
            )],
            pos: 0,
        };
        let left = BaseView::new_leaf(
            &mut a,
            &l,
            "aa0".as_bytes(),
            Some("ab0".as_bytes()),
            NULL_PID,
            &mut left_data,
            NULL_ORACLE,
            0,
            Position::MIN,
        );

        let mut right_data = LeafData {
            data: &[(
                LeafSeg::new(&[], "aba1".as_bytes(), Ver::new(2, 1)),
                gen_val(&mut a, Record::normal(1, "right".as_bytes())),
            )],
            pos: 0,
        };
        let right = BaseView::new_leaf(
            &mut a,
            &l,
            "ab0".as_bytes(),
            Some("abz".as_bytes()),
            NULL_PID,
            &mut right_data,
            NULL_ORACLE,
            0,
            Position::MIN,
        );

        let merged = left.view().as_base().merge(
            &mut a,
            &l,
            right.view().as_base(),
            NULL_ORACLE,
            0,
            Position::MIN,
        );
        let base = merged.view().as_base();
        let mut iter = base.range_iter::<Allocator, Key>(&l, 0, base.header().elems as usize);

        let (k1, _) = iter.next().expect("must have first key");
        assert_eq!(k1.raw, "a1".as_bytes());
        let (k2, _) = iter.next().expect("must have second key");
        assert_eq!(k2.raw, "ba1".as_bytes());
        assert!(iter.next().is_none());
    }

    #[test]
    fn builder_leaf_history_region_can_cross_page() {
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
            let mut b = a.alloc(sz as u32);
            Val::encode_inline(b.data_slice_mut::<u8>(), None, &r);
            Val::from_raw(unsafe { std::mem::transmute::<&[u8], &[u8]>(b.data_slice::<u8>()) })
        }

        let mut a = Allocator::new();
        let mut rows = Vec::new();
        rows.push((
            LeafSeg::new(&[], "foo".as_bytes(), Ver::new(20, 1)),
            gen_val(&mut a, Record::normal(1, "v20".as_bytes())),
        ));
        for txid in (1..20).rev() {
            let val = format!("v{txid}");
            rows.push((
                LeafSeg::new(&[], "foo".as_bytes(), Ver::new(txid, 1)),
                gen_val(&mut a, Record::normal(1, val.as_bytes())),
            ));
        }

        rows.push((
            LeafSeg::new(&[], "z".as_bytes(), Ver::new(30, 1)),
            gen_val(&mut a, Record::normal(1, "vz".as_bytes())),
        ));

        let mut kv = LeafData {
            data: rows.as_slice(),
            pos: 0,
        };
        let l = a.clone();
        let b = BaseView::new_leaf(
            &mut a,
            &l,
            &[],
            None,
            NULL_PID,
            &mut kv,
            NULL_ORACLE,
            0,
            Position::MIN,
        );
        let base = b.view().as_base();
        let mut iter = base.range_iter::<Allocator, Key>(&l, 0, base.header().elems as usize);

        let (head_k, head_v) = iter.next().expect("must have head");
        check(&head_k, &head_v, &l, "foo", 20, 1, "v20", 1);
        let hist = head_v.get_hist().expect("must have history");
        assert_eq!(hist.slot, 0);
        assert_eq!(hist.count, 19);
        let head_page = l.load(hist.page_addr).expect("must load").as_base();
        assert_ne!(head_page.box_header().link, NULL_ADDR);

        for txid in (1..20).rev() {
            let (k, v) = iter.next().expect("must have old version");
            let expected = format!("v{txid}");
            check(&k, &v, &l, "foo", txid, 1, expected.as_str(), 1);
        }

        let (tail_k, tail_v) = iter.next().expect("must have tail key");
        check(&tail_k, &tail_v, &l, "z", 30, 1, "vz", 1);
        assert!(tail_v.get_hist().is_none());
        assert!(iter.next().is_none());
    }
}
