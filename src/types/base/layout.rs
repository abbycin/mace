use super::builder::{Builder, SeekableIter};
use super::*;

impl BaseView {
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
        I: LeafCompactSource<'a> + ?Sized,
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
        let mut fold_scratch: Vec<u8> = Vec::new();
        let prefix_len = Self::calc_prefix(lo, &hi);
        #[cfg(feature = "extra_check")]
        let mut last_k: Option<LeafSeg> = None;

        while let Some((ks, cv)) = iter.next_compact(&mut fold_scratch) {
            #[cfg(feature = "extra_check")]
            {
                if let Some(x) = last_k {
                    // merge path may interleave segments from different prefixes
                    assert!(x.cmp(&ks).is_lt());
                }
                last_k = Some(ks);
            }

            let k = ks.remove_prefix(prefix_len);
            // rows whose record reaches the inline boundary end up as remote
            // envelopes in the rebuilt leaf; that includes stored inline rows
            // above a lowered bucket threshold, which materialize in pass 2
            if cv.is_remote() || cv.data_size() >= inline_size {
                remote_hint_cnt += 1;
            }
            let vsz = cv.data_size();
            let same_as_last = matches!(
                (&last_raw, &k),
                (Some(raw), kk) if raw.raw_cmp(kk).is_eq()
            );

            if same_as_last {
                if !is_new_sibling {
                    is_new_sibling = true;
                    payload_sz += Val::calc_size(true, inline_size, vsz);
                    hints.push_back((pos - 1, 0));
                }

                let (_, cnt) = must_exist!(hints.back_mut());
                *cnt += 1;
                pos += 1;
                seekable.add(k, cv);
                continue;
            }

            pos += 1;
            last_raw = Some(k);
            is_new_sibling = false;
            payload_sz += k.packed_size();
            payload_sz += Val::calc_size(false, inline_size, vsz);
            elems += 1;
            seekable.add(k, cv);
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
            // blob-destined fold results allocate their remote box here, where the
            // frame allocator is available; the box enters the allocator map like
            // any large-value write (lsn/group aligned for GC retirement)
            let (r, remote) = match v.materialize_blob(a, group, lsn) {
                Some(x) => x,
                None => v.record_with_remote(a, l, group, lsn),
            };
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
        hot_true!(hints.is_empty());
        hot_true!(sibling_refs.is_empty());
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
    pub(super) fn try_alloc<const IS_INDEX: bool, A: IFrameAlloc>(
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
        h.has_multiple_versions = false;
        h.padding = 0;

        p
    }
}
