use super::builder::{Builder, SeekableIter};
use super::*;

impl BaseView {
    #[cfg(not(test))]
    const HIST_PAGE_BUDGET: usize = 128 * 1024;
    #[cfg(test)]
    const TEST_HIST_PAGE_BUDGET: usize = 256;
    const SIBLING_HINT_CNT_LEN: usize = size_of::<u32>();
    const SIBLING_HINT_ADDR_LEN: usize = size_of::<u64>();
    const REMOTE_HINT_CNT_LEN: usize = size_of::<u32>();
    const REMOTE_HINT_ADDR_LEN: usize = size_of::<u64>();
    #[allow(clippy::too_many_arguments)]
    pub(super) fn save_hist_regions<'a, A: IFrameAlloc, L: ILoader>(
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
        let mut old_vers: Vec<(Ver, CompactVal)> = Vec::new();
        for &(idx, cnt) in hints {
            hot_true!(cnt > 0);
            let start = old_vers.len();
            iter.seek_to(idx + 1);
            for _ in 0..cnt {
                let (k, v) = iter.next().expect("must exist");
                old_vers.push((k.ver, v.clone()));
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
                let (_, v) = &old_vers[beg];
                let sz = Ver::len() + Val::calc_size(false, inline_size, v.data_size());
                let tmp = len + sz + SLOT_LEN;
                if tmp > frame_budget {
                    break;
                }
                len = tmp;
                beg += 1;
            }
            if saved == beg {
                let (_, v) = &old_vers[beg];
                len += Ver::len() + Val::calc_size(false, inline_size, v.data_size()) + SLOT_LEN;
                beg += 1;
            }
            page_ranges.push((saved, beg, len));
        }

        let mut refs_idx = VecDeque::with_capacity(regions.len());
        let mut page_cursor = 0;
        for region in &regions {
            while page_cursor + 1 < page_ranges.len()
                && page_ranges[page_cursor + 1].0 <= region.start
            {
                page_cursor += 1;
            }
            let (start, _, _) = page_ranges[page_cursor];
            refs_idx.push_back((
                page_cursor,
                (region.start - start) as u16,
                region.count as u32,
            ));
        }

        let mut tail: Option<BaseView> = None;
        let mut page_addrs = Vec::with_capacity(page_ranges.len());
        for &(start, end, len) in &page_ranges {
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
                // blob-destined fold rows allocate their remote box here, exactly
                // like the main second pass; get_remote() alone would encode a
                // dangling address for them
                let (r, remote) = match v.materialize_blob(a, group, lsn) {
                    Some(x) => x,
                    None => v.record_with_remote(a, l, group, lsn),
                };
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
    pub(super) const fn sibling_hint_size(nr_heads: usize) -> usize {
        Self::SIBLING_HINT_CNT_LEN + nr_heads * Self::SIBLING_HINT_ADDR_LEN
    }

    pub(super) const fn leaf_hint_size(nr_heads: usize, nr_remotes: usize) -> usize {
        if nr_heads == 0 && nr_remotes == 0 {
            0
        } else {
            Self::sibling_hint_size(nr_heads)
                + Self::REMOTE_HINT_CNT_LEN
                + nr_remotes * Self::REMOTE_HINT_ADDR_LEN
        }
    }

    pub(super) fn write_leaf_hints(&mut self, heads: &[u64], remotes: &[u64]) {
        hot_true!(!self.header().is_index);
        let off = self.header().size as usize;
        hot_true!(
            self.box_header().payload_size as usize
                >= off + Self::leaf_hint_size(heads.len(), remotes.len())
        );

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
        hot_true!(!self.header().is_index);
        let payload = self.box_header().payload_size as usize;
        let off = self.header().size as usize;
        if payload == off {
            return None;
        }
        hot_true!(payload >= off + Self::SIBLING_HINT_CNT_LEN);
        let p = self.0.cast::<u8>();
        let cnt = unsafe { p.add(off).cast::<u32>().read_unaligned() as usize };
        let remote_cnt_off = off + Self::sibling_hint_size(cnt);
        hot_true!(payload >= remote_cnt_off + Self::REMOTE_HINT_CNT_LEN);
        let remote_cnt = unsafe { p.add(remote_cnt_off).cast::<u32>().read_unaligned() as usize };
        hot_true!(
            payload
                >= remote_cnt_off
                    + Self::REMOTE_HINT_CNT_LEN
                    + remote_cnt * Self::REMOTE_HINT_ADDR_LEN
        );
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
}
