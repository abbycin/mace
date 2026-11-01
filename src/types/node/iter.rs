use super::compact::{CompactVal, LeafCompactSource};
use super::filter::{FilterOut, LeafFilter, PlainLeafFilter};
use super::*;

impl<L> Node<L>
where
    L: ILoader,
{
    #[allow(clippy::iter_skip_zero)]
    pub(crate) fn intl_iter(&'_ self) -> IntlIter<'_, L> {
        hot_true!(eq self.box_header().node_type, NodeType::Intl);
        let len = self.header().prefix_len as usize;
        let lo = self.lo();
        IntlIter {
            prefix: &lo[..len],
            next_l: None,
            next_r: None,
            sst_iter: self
                .inner
                .range_iter(&self.loader, 0, self.inner.header().elems as usize),
            delta_iter: IterAdaptor::Iter(self.state.read().delta.iter().skip(0)),
        }
    }

    #[allow(clippy::iter_skip_zero)]
    pub(super) fn plain_leaf_iter<'b>(
        &'b self,
        j: &'b mut Junk,
        safe_txid: u64,
        abort_ctx: Option<Handle<Context>>,
    ) -> PlainLeafIter<'b, L> {
        hot_true!(eq self.box_header().node_type, NodeType::Leaf);
        let len = self.header().prefix_len as usize;
        let lo = self.lo();
        PlainLeafIter {
            cursor: LeafCursor::new(
                &lo[..len],
                self.inner
                    .range_iter(&self.loader, 0, self.inner.header().elems as usize),
                IterAdaptor::Iter(self.state.read().delta.iter().skip(0)),
            ),
            filter: PlainLeafFilter::new(safe_txid, j, abort_ctx),
        }
    }

    #[allow(clippy::iter_skip_zero)]
    pub(super) fn leaf_iter<'b>(
        &'b self,
        j: &'b mut Junk,
        safe_txid: u64,
        inline_size: usize,
        op: Option<&'b dyn crate::MergeOperator>,
        abort_ctx: Option<Handle<Context>>,
    ) -> LeafIter<'b, L> {
        hot_true!(eq self.box_header().node_type, NodeType::Leaf);
        let len = self.header().prefix_len as usize;
        let lo = self.lo();
        LeafIter {
            pending: None,
            key_buf: Vec::new(),
            cursor: LeafCursor::new(
                &lo[..len],
                self.inner
                    .range_iter(&self.loader, 0, self.inner.header().elems as usize),
                IterAdaptor::Iter(self.state.read().delta.iter().skip(0)),
            ),
            filter: LeafFilter::new(safe_txid, &self.loader, j, op, inline_size, abort_ctx),
        }
    }

    #[allow(clippy::iter_skip_zero)]
    pub(super) fn leaf_iter_drop_aborted<'b>(
        &'b self,
        j: &'b mut Junk,
        safe_txid: u64,
        ctx: Handle<Context>,
    ) -> LeafIter<'b, L> {
        hot_true!(eq self.box_header().node_type, NodeType::Leaf);
        let len = self.header().prefix_len as usize;
        let lo = self.lo();
        LeafIter {
            pending: None,
            key_buf: Vec::new(),
            cursor: LeafCursor::new(
                &lo[..len],
                self.inner
                    .range_iter(&self.loader, 0, self.inner.header().elems as usize),
                IterAdaptor::Iter(self.state.read().delta.iter().skip(0)),
            ),
            filter: LeafFilter::with_drop_aborted(safe_txid, &self.loader, j, ctx),
        }
    }
}

pub(crate) enum IterAdaptor<'a, T> {
    Iter(std::iter::Skip<Iter<'a, DeltaView>>),
    Range(std::iter::Skip<RangeIter<'a, DeltaView, T>>),
}

impl<'a, T> Iterator for IterAdaptor<'a, T> {
    type Item = DeltaView;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IterAdaptor::Iter(i) => i.next(),
            IterAdaptor::Range(r) => r.next(),
        }
    }
}

pub(crate) enum IterAdaptorRev<'a> {
    IterRev {
        iter: Iter<'a, DeltaView>,
        excluded: Option<&'a [u8]>,
        pending: Option<DeltaView>,
        group: Vec<DeltaView>,
    },
    RangeRev {
        iter: RangeIter<'a, DeltaView, &'a [u8]>,
        excluded: Option<&'a [u8]>,
        pending: Option<DeltaView>,
        group: Vec<DeltaView>,
    },
}

impl IterAdaptorRev<'_> {
    /// emit one raw-key group newest-to-older while preserving the pending boundary
    fn group_next<I>(
        iter: &mut I,
        excluded: Option<&[u8]>,
        pending: &mut Option<DeltaView>,
        group: &mut Vec<DeltaView>,
    ) -> Option<DeltaView>
    where
        I: DoubleEndedIterator<Item = DeltaView>,
    {
        if let Some(x) = group.pop() {
            return Some(x);
        }

        let first = loop {
            let x = pending.take().or_else(|| iter.next_back())?;
            if excluded.is_none_or(|b| Key::decode_from(x.key()).raw != b) {
                break x;
            }
        };
        let raw = Key::decode_from(first.key()).raw;
        group.push(first);
        while let Some(x) = iter.next_back() {
            let k = Key::decode_from(x.key());
            if excluded.is_some_and(|b| k.raw == b) {
                continue;
            }
            if k.raw == raw {
                // reverse walk yields older->newer, keep full group then emit newest->older
                group.push(x);
                continue;
            }
            *pending = Some(x);
            break;
        }
        group.pop()
    }

    fn next(&mut self) -> Option<DeltaView> {
        match self {
            IterAdaptorRev::IterRev {
                iter,
                excluded,
                pending,
                group,
            } => Self::group_next(iter, *excluded, pending, group),
            IterAdaptorRev::RangeRev {
                iter,
                excluded,
                pending,
                group,
            } => Self::group_next(iter, *excluded, pending, group),
        }
    }
}

pub(crate) struct IntlIter<'a, L>
where
    L: ILoader,
{
    prefix: &'a [u8],
    next_l: Option<(IntlSeg<'a>, Index)>,
    next_r: Option<(IntlSeg<'a>, Index)>,
    sst_iter: BaseIter<'a, L, IntlKey<'a>>,
    delta_iter: IterAdaptor<'a, &'a [u8]>,
}

/// two-way merge over the base SST and the delta tree used by leaf consolidation
struct LeafCursor<'a, L>
where
    L: ILoader,
{
    prefix: &'a [u8],
    next_l: Option<(LeafSeg<'a>, Val<'a>)>,
    next_r: Option<(LeafSeg<'a>, Val<'a>)>,
    sst_iter: BaseIter<'a, L, Key<'a>>,
    delta_iter: IterAdaptor<'a, &'a [u8]>,
}

impl<'a, L: ILoader> LeafCursor<'a, L> {
    #[inline]
    fn new(
        prefix: &'a [u8],
        sst_iter: BaseIter<'a, L, Key<'a>>,
        delta_iter: IterAdaptor<'a, &'a [u8]>,
    ) -> Self {
        Self {
            prefix,
            next_l: None,
            next_r: None,
            sst_iter,
            delta_iter,
        }
    }

    #[inline(always)]
    fn next<F: FnMut(u64)>(&mut self, on_sibling: F) -> Option<(LeafSeg<'a>, Val<'a>)> {
        if self.next_r.is_none()
            && let Some((k, v)) = self.sst_iter.next_with_sibling(on_sibling)
        {
            self.next_r = Some((LeafSeg::new(self.prefix, k.raw, k.ver), v));
        }
        if self.next_l.is_none()
            && let Some(x) = self.delta_iter.next()
        {
            let k = Key::decode_from(x.key());
            self.next_l = Some((
                LeafSeg::new(self.prefix, &k.raw[self.prefix.len()..], k.ver),
                x.val(),
            ));
        }
        match (self.next_l.take(), self.next_r.take()) {
            (None, None) => None,
            (None, Some(r)) => Some(r),
            (Some(l), None) => Some(l),
            (Some(l), Some(r)) => match l.0.cmp(&r.0) {
                Less => {
                    self.next_r = Some(r);
                    Some(l)
                }
                Greater => {
                    self.next_l = Some(l);
                    Some(r)
                }
                Equal => unreachable!("base and delta versions must be disjoint"),
            },
        }
    }
}

pub(crate) struct PlainLeafIter<'a, L>
where
    L: ILoader,
{
    cursor: LeafCursor<'a, L>,
    filter: PlainLeafFilter<'a>,
}

impl<'a, L: ILoader> LeafCompactSource<'a> for PlainLeafIter<'a, L> {
    #[inline(always)]
    fn next_compact<'s>(
        &mut self,
        _scratch: &'s mut Vec<u8>,
    ) -> Option<(LeafSeg<'a>, CompactVal<'a>)> {
        loop {
            let (cursor, filter) = (&mut self.cursor, &mut self.filter);
            let item = cursor.next(|addr| filter.junks.push(addr))?;
            if filter.check(&item.0, &item.1) {
                return Some((item.0, CompactVal::Page(item.1)));
            }
        }
    }
}

pub(crate) struct LeafIter<'a, L>
where
    L: ILoader,
{
    /// next candidate held while the current key is being drained
    pending: Option<(LeafSeg<'a>, Val<'a>)>,
    /// contiguous versions of the key currently being compacted
    key_buf: Vec<(LeafSeg<'a>, Val<'a>)>,
    cursor: LeafCursor<'a, L>,
    pub(super) filter: LeafFilter<'a, L>,
}

impl<'a, L: ILoader> LeafCompactSource<'a> for LeafIter<'a, L> {
    #[inline(always)]
    fn next_compact<'s>(
        &mut self,
        scratch: &'s mut Vec<u8>,
    ) -> Option<(LeafSeg<'a>, CompactVal<'a>)> {
        if self.filter.operator.is_some() {
            return self.next_compact_key(scratch);
        }

        loop {
            // pending fold outcomes (a synthesized boundary row or a raw-preserving
            // re-emission) always precede the next input item
            if let Some(item) = self.filter.pop_emit() {
                return Some(item);
            }

            let item = self.next_candidate()?;

            match self.filter.check(&item) {
                FilterOut::Keep => return Some((item.0, CompactVal::Page(item.1))),
                FilterOut::Drop => {}
            }
        }
    }
}

impl<'a, L: ILoader> LeafIter<'a, L> {
    #[inline(always)]
    fn next_candidate(&mut self) -> Option<(LeafSeg<'a>, Val<'a>)> {
        if let Some(item) = self.pending.take() {
            return Some(item);
        }
        let (cursor, filter) = (&mut self.cursor, &mut self.filter);
        cursor.next(|addr| filter.junks.push(addr))
    }

    fn next_compact_key<'s>(
        &mut self,
        scratch: &'s mut Vec<u8>,
    ) -> Option<(LeafSeg<'a>, CompactVal<'a>)> {
        loop {
            if let Some(item) = self.filter.pop_emit() {
                return Some(item);
            }
            if self.key_buf.is_empty() {
                let first = self.next_candidate()?;
                let raw = first.0;
                self.key_buf.push(first);
                while let Some(item) = self.next_candidate() {
                    if item.0.raw_cmp(&raw).is_eq() {
                        self.key_buf.push(item);
                    } else {
                        self.pending = Some(item);
                        break;
                    }
                }
                self.filter.process_key(&self.key_buf, scratch);
                self.key_buf.clear();
            }
        }
    }
}

pub(crate) trait RawSst<'a, L: ILoader> {
    fn try_step<F: FnMut(u64)>(&mut self, f: F) -> Result<Option<(Key<'a>, Val<'a>)>, OpCode>;

    fn loader(&self) -> &'a L;
}

impl<'a, L: ILoader> RawSst<'a, L> for BaseIter<'a, L, Key<'a>> {
    fn try_step<F: FnMut(u64)>(&mut self, f: F) -> Result<Option<(Key<'a>, Val<'a>)>, OpCode> {
        self.try_next_with_sibling(f)
    }

    fn loader(&self) -> &'a L {
        self.loader
    }
}

impl<'a, L: ILoader> RawSst<'a, L> for BaseRevIter<'a, L, Key<'a>> {
    fn try_step<F: FnMut(u64)>(&mut self, f: F) -> Result<Option<(Key<'a>, Val<'a>)>, OpCode> {
        self.try_next_back_with_sibling(f)
    }

    fn loader(&self) -> &'a L {
        self.loader
    }
}

pub(crate) trait RawDelta<'a> {
    fn next_dir(&mut self) -> Option<DeltaView>;
}

impl<'a> RawDelta<'a> for IterAdaptor<'a, &'a [u8]> {
    fn next_dir(&mut self) -> Option<DeltaView> {
        self.next()
    }
}

impl<'a> RawDelta<'a> for IterAdaptorRev<'a> {
    fn next_dir(&mut self) -> Option<DeltaView> {
        self.next()
    }
}

pub(crate) struct RawLeafIterCore<'a, L, S, D, const REV: bool>
where
    L: ILoader,
{
    pub(super) cached_key: Handle<Vec<u8>>,
    pub(super) prefix: &'a [u8],
    pub(super) next_l: Option<IterItem<'a, L>>,
    pub(super) next_r: Option<IterItem<'a, L>>,
    pub(super) sst_iter: S,
    pub(super) delta_iter: D,
}

pub(crate) type RawLeafIter<'a, L> =
    RawLeafIterCore<'a, L, BaseIter<'a, L, Key<'a>>, IterAdaptor<'a, &'a [u8]>, false>;
pub(crate) type RawLeafRevIter<'a, L> =
    RawLeafIterCore<'a, L, BaseRevIter<'a, L, Key<'a>>, IterAdaptorRev<'a>, true>;

impl<'a, L> Iterator for IntlIter<'a, L>
where
    L: ILoader,
{
    type Item = (IntlSeg<'a>, Index);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.next_l.is_none()
                && let Some(x) = self.delta_iter.next()
            {
                let k = IntlKey::decode_from(x.key());
                // NOTE: split raw into two parts which simplify comparation
                self.next_l = Some((
                    IntlSeg::new(self.prefix, &k.raw[self.prefix.len()..]),
                    Index::decode_from(x.index()),
                ));
            }

            if self.next_r.is_none()
                && let Some((k, v)) = self.sst_iter.next()
            {
                self.next_r = Some((IntlSeg::new(self.prefix, k.raw), v));
            }

            match (self.next_l, self.next_r) {
                (None, None) => return None,
                (None, Some(x)) => {
                    self.next_r = None;
                    return Some(x);
                }
                (Some(x), None) => {
                    self.next_l = None;
                    hot_true!(!x.1.is_tombstone());
                    return Some(x);
                }
                (Some(l), Some(r)) => match l.0.raw_cmp(&r.0) {
                    Equal => {
                        self.next_l = None;
                        self.next_r = None;
                        // when the latest one is marked as tombstone, skip all same `raw`s
                        // NOTE: there are at most same `raw` one in delta another in sst
                        if l.1.is_tombstone() {
                            continue;
                        }
                        return Some(l);
                    }
                    Greater => {
                        self.next_r = None;
                        return Some(r);
                    }
                    Less => {
                        self.next_l = None;
                        return Some(l);
                    }
                },
            }
        }
    }
}

impl<'a, L> Iterator for RawLeafIter<'a, L>
where
    L: ILoader,
{
    type Item = IterItem<'a, L>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().expect("must exist")
    }
}

impl<'a, L, S, D, const REV: bool> RawLeafIterCore<'a, L, S, D, REV>
where
    L: ILoader,
    S: RawSst<'a, L>,
    D: RawDelta<'a>,
{
    fn try_step(&mut self) -> Result<Option<IterItem<'a, L>>, OpCode> {
        if self.next_l.is_none()
            && let Some(x) = self.delta_iter.next_dir()
        {
            let k = Key::decode_from(x.key());
            self.next_l = Some(IterItem::new(
                self.cached_key,
                &[],
                k,
                x.val(),
                self.sst_iter.loader(),
            ));
        }

        if self.next_r.is_none()
            && let Some((k, val)) = self.sst_iter.try_step(|_| {})?
        {
            self.next_r = Some(IterItem::new(
                self.cached_key,
                self.prefix,
                k,
                val,
                self.sst_iter.loader(),
            ));
        }

        Ok(match (self.next_l.take(), self.next_r.take()) {
            (None, None) => None,
            (None, Some(r)) => Some(r),
            (Some(l), None) => Some(l),
            (Some(l), Some(r)) => {
                let ord = if self.prefix.is_empty() {
                    l.cmp(&r)
                } else {
                    cmp_raw_with_prefixed_tail(l.base, self.prefix, r.base)
                };
                // forward returns the smaller side, reverse the larger; equal keeps the
                // delta (latest) row in both directions
                let return_left = match ord {
                    Less => !REV,
                    Greater => REV,
                    Equal => true,
                };
                if return_left {
                    self.next_r = Some(r);
                    Some(l)
                } else {
                    self.next_l = Some(l);
                    Some(r)
                }
            }
        })
    }
}

impl<'a, L, S, D> RawLeafIterCore<'a, L, S, D, false>
where
    L: ILoader,
    S: RawSst<'a, L>,
    D: RawDelta<'a>,
{
    pub(crate) fn try_next(&mut self) -> Result<Option<IterItem<'a, L>>, OpCode> {
        self.try_step()
    }
}

impl<'a, L, S, D> RawLeafIterCore<'a, L, S, D, true>
where
    L: ILoader,
    S: RawSst<'a, L>,
    D: RawDelta<'a>,
{
    pub(crate) fn try_next_back(&mut self) -> Result<Option<IterItem<'a, L>>, OpCode> {
        self.try_step()
    }
}
