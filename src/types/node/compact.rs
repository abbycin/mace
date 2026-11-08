use super::*;

impl<L> Node<L>
where
    L: ILoader,
{
    pub(crate) fn compact<A: IFrameAlloc>(
        &self,
        a: &mut A,
        safe_txid: u64,
        op: Option<&dyn crate::MergeOperator>,
        block: &mut dyn FnMut(&[u8]),
        abort_ctx: Option<Handle<Context>>,
    ) -> (Node<L>, Junk) {
        let mut junks = Junk::new();
        let b = self.merge_to_base(a, &mut junks, safe_txid, op, block, abort_ctx);
        let mut base = b.view().as_base();
        let h = base.header_mut();
        let old = self.header();
        h.merging = old.merging;
        h.merging_child = old.merging_child;
        let (group, lsn) = {
            let h = b.header();
            (h.group, h.lsn)
        };
        (Self::new(self.loader.copy_detached(), b, group, lsn), junks)
    }

    /// retained-abort versions visible to the page-wide cleanup; cheap pre-scan so a page
    /// with nothing to remove does not build and discard a whole new leaf
    pub(crate) fn has_retained_abort(&self, ctx: Handle<Context>) -> bool {
        hot_true!(!self.header().is_index);
        let mut sst_iter =
            self.inner
                .range_iter(&self.loader, 0, self.inner.header().elems as usize);
        while let Some((k, v)) = sst_iter.next_with_sibling(|_| {}) {
            if is_retained_abort(&ctx, v.group_id(), k.ver.txid) {
                return true;
            }
        }
        let state = self.state.read();
        state.delta.iter().any(|x| {
            let k = Key::decode_from(x.key());
            is_retained_abort(&ctx, x.val().group_id(), k.ver.txid)
        })
    }

    pub(crate) fn remove_aborted<A: IFrameAlloc>(
        &self,
        a: &mut A,
        safe_txid: u64,
        ctx: Handle<Context>,
    ) -> (Node<L>, Junk, bool) {
        let mut junks = Junk::new();
        let (b, removed) = self.merge_to_base_drop_aborted(a, &mut junks, safe_txid, ctx);
        let mut base = b.view().as_base();
        let h = base.header_mut();
        let old = self.header();
        h.merging = old.merging;
        h.merging_child = old.merging_child;
        let (group, lsn) = {
            let h = b.header();
            (h.group, h.lsn)
        };
        (
            Self::new(self.loader.copy_detached(), b, group, lsn),
            junks,
            removed,
        )
    }

    pub(super) fn merge_to_base<A: IFrameAlloc>(
        &self,
        a: &mut A,
        j: &mut Junk,
        safe_txid: u64,
        op: Option<&dyn crate::MergeOperator>,
        block: &mut dyn FnMut(&[u8]),
        abort_ctx: Option<Handle<Context>>,
    ) -> BoxRef {
        let h = self.header();
        let lo = self.lo();
        let hi = self.hi();

        if h.is_index {
            let (group, lsn) = self.get_group_lsn();
            BaseView::new_intl(
                a,
                lo,
                hi,
                h.right_sibling,
                || self.intl_iter(),
                safe_txid,
                group,
                lsn,
            )
        } else {
            let (group, lsn) = self.get_group_lsn();
            if op.is_none() {
                let mut iter = self.plain_leaf_iter(j, safe_txid, abort_ctx);
                return self.build_leaf(a, &mut iter, safe_txid, group, lsn);
            }
            let mut iter = self.leaf_iter(j, safe_txid, a.inline_size(), op, abort_ctx);
            let base = self.build_leaf(a, &mut iter, safe_txid, group, lsn);
            // record keys whose fold violated the merge contract
            for k in iter.filter.blocked_hits.drain(..) {
                block(&k);
            }
            base
        }
    }

    fn build_leaf<'b, A, I>(
        &'b self,
        a: &mut A,
        iter: &mut I,
        safe_txid: u64,
        group: u8,
        lsn: Position,
    ) -> BoxRef
    where
        A: IFrameAlloc,
        I: LeafCompactSource<'b> + ?Sized,
    {
        let h = self.header();
        BaseView::new_leaf(
            a,
            &self.loader,
            self.lo(),
            self.hi(),
            h.right_sibling,
            iter,
            safe_txid,
            group,
            lsn,
        )
    }

    fn merge_to_base_drop_aborted<A: IFrameAlloc>(
        &self,
        a: &mut A,
        j: &mut Junk,
        safe_txid: u64,
        ctx: Handle<Context>,
    ) -> (BoxRef, bool) {
        hot_true!(!self.header().is_index);
        let mut iter = self.leaf_iter_drop_aborted(j, safe_txid, ctx);
        let (group, lsn) = self.get_group_lsn();
        let base = self.build_leaf(a, &mut iter, safe_txid, group, lsn);
        (base, iter.filter.removed)
    }

    pub(crate) fn process_merge<A: IFrameAlloc>(
        &self,
        a: &mut A,
        op: MergeOp,
        safe_txid: u64,
        abort_ctx: Handle<Context>,
    ) -> (Node<L>, Junk) {
        match op {
            MergeOp::Merged => {
                hot_true!(eq self.box_header().node_type, NodeType::Intl);
                let mut key = None;
                let h = self.header();
                let merging_child = h.merging_child;
                for (k, v) in self.intl_iter() {
                    if v.pid == merging_child {
                        key = Some(k);
                        break;
                    }
                }
                let (group, lsn) = self.get_group_lsn();
                let b = DeltaView::from_key_index(
                    a,
                    must_exist!(key),
                    Index::null(),
                    safe_txid,
                    group,
                    lsn,
                );
                let tmp = self.insert(b);
                let mut no_block = |_: &[u8]| {};
                let (mut p, j) = tmp.compact(a, safe_txid, None, &mut no_block, None);
                p.header_mut().merging_child = NULL_PID;
                p.header_mut().split_elems = h.split_elems + 1;
                (p, j)
            }
            MergeOp::MarkParent(pid) => {
                hot_true!(eq self.box_header().node_type, NodeType::Intl);
                let mut no_block = |_: &[u8]| {};
                let (mut p, j) = self.compact(a, safe_txid, None, &mut no_block, None);
                p.header_mut().merging_child = pid;
                (p, j)
            }
            MergeOp::MarkChild => {
                hot_true!(eq self.box_header().node_type, NodeType::Leaf);
                let mut no_block = |_: &[u8]| {};
                let (mut p, j) = self.compact(a, safe_txid, None, &mut no_block, Some(abort_ctx));
                p.header_mut().merging = true;
                (p, j)
            }
        }
    }
}

/// payload length field of an encoded synthetic envelope
fn synth_data_size(env: &[u8]) -> usize {
    let off = 1 + 1; // flags + gid, then the u32 length field
    u32::from_le_bytes([env[off], env[off + 1], env[off + 2], env[off + 3]]) as usize
}

/// inline record bytes ([gid][payload]) of an encoded synthetic envelope
fn synth_inline_bytes(env: &[u8]) -> &[u8] {
    let off = 1 + 1 + 4;
    let len = synth_data_size(env);
    &env[off..off + len]
}

/// a candidate version flowing into consolidation: either a page-backed value or a
/// synthesized fold result owned by the consolidation scratch buffer
#[derive(Clone)]
/// classification of a folded value against the persisted size limits
pub(crate) enum FoldClass {
    /// record fits inline
    Inline,
    /// record must materialize through the remote/blob path
    Blob,
    /// record exceeds MAX_KV_SIZE: the fold must not materialize
    TooLarge,
}

/// classify a folded record by its persisted size: [gid][data] is inline below `inline_size`,
/// blob-backed up to `MAX_KV_SIZE`, and too large beyond that
pub(crate) fn classify_fold_size(len: usize, inline_size: usize) -> FoldClass {
    let record_len = size_of::<u8>() + len;
    if record_len < inline_size {
        FoldClass::Inline
    } else if record_len <= Options::MAX_KV_SIZE {
        FoldClass::Blob
    } else {
        FoldClass::TooLarge
    }
}

#[derive(Clone)]
pub(crate) enum CompactVal<'a> {
    Page(Val<'a>),
    /// fully encoded synthetic envelope ([flags][gid][len][gid][payload] inline, or
    /// [flags|REMOTE][gid][len][addr] remote with a provisional address slot), owned
    /// so it can outlive the consolidation scratch buffer across iterator steps;
    /// `blob` carries the encoded record bytes ([gid][data]) when the fold result
    /// must materialize through the remote/blob path (allocated in the second pass
    /// of BaseView::new_leaf, which holds the frame allocator)
    Synth {
        env: Vec<u8>,
        blob: Option<Vec<u8>>,
    },
}

impl<'a> CompactVal<'a> {
    pub(crate) fn data_size(&self) -> usize {
        match self {
            CompactVal::Page(v) => v.data_size(),
            CompactVal::Synth { env, .. } => synth_data_size(env),
        }
    }

    pub(crate) fn is_remote(&self) -> bool {
        match self {
            CompactVal::Page(v) => v.get_remote() != NULL_ADDR,
            // a fold result destined for a blob counts toward the remote hint budget
            CompactVal::Synth { blob, .. } => blob.is_some(),
        }
    }

    pub(crate) fn get_remote(&self) -> u64 {
        match self {
            CompactVal::Page(v) => v.get_remote(),
            // blob-destined rows allocate their address during the second pass
            CompactVal::Synth { .. } => NULL_ADDR,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_tombstone(&self) -> bool {
        match self {
            CompactVal::Page(v) => v.is_tombstone(),
            CompactVal::Synth { env, .. } => env[0] & Val::DEL_BIT != 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_merge(&self) -> bool {
        match self {
            CompactVal::Page(v) => v.is_merge(),
            CompactVal::Synth { env, .. } => env[0] & Val::MERGE_BIT != 0,
        }
    }

    /// materializes the record behind this version (loader only needed for page values)
    pub(crate) fn get_record<L: ILoader>(&self, l: &L) -> (Record, Option<BoxRef>) {
        match self {
            CompactVal::Page(v) => v.get_record(l),
            // synthesized envelopes are always inline: payload region is [gid][data];
            // blob-destined rows decode from their record bytes directly
            CompactVal::Synth { env, blob } => {
                let rec = if let Some(data) = blob {
                    Record::from_slice(data)
                } else {
                    Record::from_slice(synth_inline_bytes(env))
                };
                let mut rec = rec;
                if env[0] & Val::MERGE_BIT != 0 {
                    rec.set_merge_tag();
                }
                (rec, None)
            }
        }
    }

    /// second pass: allocates the remote box for a blob-destined fold result and
    /// returns (record, remote address); page-backed and inline rows return None
    pub(crate) fn materialize_blob<A: IFrameAlloc>(
        &self,
        a: &mut A,
        group: u8,
        lsn: Position,
    ) -> Option<(Record, u64)> {
        match self {
            CompactVal::Synth {
                blob: Some(data), ..
            } => {
                let mut r = a.alloc((data.len() + size_of::<RemoteHeader>()) as u32);
                let h = r.header_mut();
                h.kind = TagKind::Remote;
                h.node_type = NodeType::Leaf;
                h.lsn = lsn;
                h.group = group;
                let mut view = r.view().as_remote();
                view.header_mut().size = data.len();
                view.raw_mut().copy_from_slice(data);
                Some((Record::from_slice(data), view.addr()))
            }
            _ => None,
        }
    }

    /// second pass for page-backed rows: returns (record, remote addr). a
    /// stored inline row whose record now sits at or above the inline
    /// boundary materializes a real remote box here, because a bucket
    /// threshold can be lowered on an existing bucket and rebuilding the row
    /// otherwise encodes a remote envelope with a null address (the box is
    /// what `builder.add_leaf` needs when `vsz >= limit`)
    pub(crate) fn record_with_remote<A: IFrameAlloc, L: ILoader>(
        &self,
        a: &mut A,
        l: &L,
        group: u8,
        lsn: Position,
    ) -> (Record, u64) {
        let (r, _) = self.get_record(l);
        let mut remote = self.get_remote();
        if remote == NULL_ADDR && r.packed_size() >= a.inline_size() {
            let mut b = a.alloc((r.packed_size() + size_of::<RemoteHeader>()) as u32);
            let h = b.header_mut();
            h.kind = TagKind::Remote;
            h.node_type = NodeType::Leaf;
            h.lsn = lsn;
            h.group = group;
            let mut view = b.view().as_remote();
            view.header_mut().size = r.packed_size();
            r.encode_to(view.raw_mut());
            remote = b.header().addr;
        }
        (r, remote)
    }
}

/// adapts a plain page-backed version stream; folding never triggers for these callers
pub(crate) struct PlainValSource<'a, I> {
    iter: I,
    _pd: std::marker::PhantomData<&'a ()>,
}

impl<'a, I> PlainValSource<'a, I>
where
    I: Iterator<Item = (LeafSeg<'a>, Val<'a>)>,
{
    pub(crate) fn new(iter: I) -> Self {
        Self {
            iter,
            _pd: std::marker::PhantomData,
        }
    }
}

impl<'a, I> LeafCompactSource<'a> for PlainValSource<'a, I>
where
    I: Iterator<Item = (LeafSeg<'a>, Val<'a>)>,
{
    #[inline(always)]
    fn next_compact<'s>(
        &mut self,
        _scratch: &'s mut Vec<u8>,
    ) -> Option<(LeafSeg<'a>, CompactVal<'a>)> {
        self.iter.next().map(|(k, v)| (k, CompactVal::Page(v)))
    }
}

/// source of versions for consolidation with optional materialization
pub(crate) trait LeafCompactSource<'a> {
    fn next_compact<'s>(
        &mut self,
        scratch: &'s mut Vec<u8>,
    ) -> Option<(LeafSeg<'a>, CompactVal<'a>)>;
}
