use super::compact::{CompactVal, FoldClass, classify_fold_size};
use super::*;

/// outcome of the operator-free streaming filter for one candidate version
pub(super) enum FilterOut {
    Keep,
    /// the version is consumed; its remote payload (if any) is junked
    Drop,
}

/// shared non-merge version rule; the bool reports a new raw key so the caller can
/// reset its merge-shadow state
#[inline(always)]
fn check_plain_version<'a>(
    txid: u64,
    last: &mut Option<&'a [u8]>,
    skip_dup: &mut bool,
    k: &LeafSeg<'a>,
    v: &Val<'a>,
) -> (FilterOut, bool) {
    if let Some(l) = *last
        && l == k.raw()
    {
        if *skip_dup {
            return (FilterOut::Drop, false);
        }
        if k.txid() > txid {
            return (FilterOut::Keep, false);
        }
        *skip_dup = true;
        return if v.is_tombstone() {
            (FilterOut::Drop, false)
        } else {
            (FilterOut::Keep, false)
        };
    }
    *last = Some(k.raw());
    *skip_dup = k.txid() <= txid;
    let out = if v.is_tombstone() && *skip_dup {
        FilterOut::Drop
    } else {
        FilterOut::Keep
    };
    (out, true)
}

/// baseline compaction filter for pages that cannot materialize merge operands
pub(super) struct PlainLeafFilter<'a> {
    txid: u64,
    last: Option<&'a [u8]>,
    pub(super) junks: &'a mut Vec<u64>,
    skip_dup: bool,
    /// a safe tombstone shadows all older versions, including raw merge operands
    drop_merges: bool,
    abort_ctx: Option<Handle<Context>>,
}

impl<'a> PlainLeafFilter<'a> {
    pub(super) fn new(
        txid: u64,
        junks: &'a mut Vec<u64>,
        abort_ctx: Option<Handle<Context>>,
    ) -> Self {
        Self {
            txid,
            last: None,
            junks,
            skip_dup: false,
            drop_merges: false,
            abort_ctx,
        }
    }

    #[inline(always)]
    pub(super) fn check(&mut self, k: &LeafSeg<'a>, v: &Val<'a>) -> bool {
        if k.txid() <= self.txid
            && self
                .abort_ctx
                .is_some_and(|ctx| is_retained_abort(&ctx, v.group_id(), k.txid()))
        {
            self.collect(v);
            return false;
        }
        if v.is_merge() {
            if self.drop_merges && self.last == Some(k.raw()) {
                self.collect(v);
                return false;
            }
            // raw operands remain available when a plain base is retained; a
            // safe tombstone is the only version barrier that shadows them
            return true;
        }
        let (out, new_raw) =
            check_plain_version(self.txid, &mut self.last, &mut self.skip_dup, k, v);
        if new_raw {
            self.drop_merges = false;
        }
        match out {
            FilterOut::Keep => true,
            FilterOut::Drop => {
                if v.is_tombstone() && self.skip_dup {
                    self.drop_merges = true;
                }
                self.collect(v);
                false
            }
        }
    }

    #[inline(always)]
    fn collect(&mut self, v: &Val<'a>) {
        let remote = v.get_remote();
        if remote != NULL_ADDR {
            self.junks.push(RemoteView::tag(remote));
        }
    }
}

pub(crate) struct LeafFilter<'a, L> {
    txid: u64,
    loader: &'a L,
    last: Option<&'a [u8]>,
    pub(super) junks: &'a mut Vec<u64>,
    skip_dup: bool,
    /// when present, merge runs at/below the safe boundary materialize into one
    /// synthesized base row; when absent the chain stays raw-preserving
    pub(super) operator: Option<&'a dyn crate::MergeOperator>,
    /// absorbed below-safe merge run, newest first: (version, envelope) copies
    pub(super) run_ops: Vec<(LeafSeg<'a>, Val<'a>)>,
    /// pending folded outcomes, popped newest-first by the iterator
    emit: Vec<(LeafSeg<'a>, CompactVal<'a>)>,
    /// scratch for the full user raw key (prefix + base) of the folded run:
    /// LeafSeg::raw() returns the stripped tail, which would dispatch the wrong
    /// algebra on a compressed page
    fold_key: Vec<u8>,
    /// keys whose fold hit an engine-detected contract violation during this
    /// consolidation; drained by merge_to_base and handed to the bucket block set
    pub(super) blocked_hits: Vec<Vec<u8>>,
    inline_size: usize,
    drop_aborted_ctx: Option<Handle<Context>>,
    /// retained-abort predicate source for materializing compaction
    abort_ctx: Option<Handle<Context>>,
    pub(super) removed: bool,
}

impl<'a, L: ILoader> LeafFilter<'a, L> {
    pub(super) fn new(
        txid: u64,
        loader: &'a L,
        junks: &'a mut Vec<u64>,
        op: Option<&'a dyn crate::MergeOperator>,
        a_inline_size: usize,
        abort_ctx: Option<Handle<Context>>,
    ) -> Self {
        Self {
            txid,
            loader,
            last: None,
            junks,
            skip_dup: false,
            operator: op,
            run_ops: Vec::new(),
            emit: Vec::new(),
            fold_key: Vec::new(),
            blocked_hits: Vec::new(),
            inline_size: a_inline_size,
            drop_aborted_ctx: None,
            abort_ctx,
            removed: false,
        }
    }

    pub(super) fn with_drop_aborted(
        txid: u64,
        loader: &'a L,
        junks: &'a mut Vec<u64>,
        ctx: Handle<Context>,
    ) -> Self {
        Self {
            txid,
            loader,
            last: None,
            junks,
            skip_dup: false,
            operator: None,
            run_ops: Vec::new(),
            emit: Vec::new(),
            fold_key: Vec::new(),
            blocked_hits: Vec::new(),
            inline_size: 0,
            drop_aborted_ctx: Some(ctx),
            abort_ctx: None,
            removed: false,
        }
    }

    #[inline(always)]
    pub(super) fn pop_emit(&mut self) -> Option<(LeafSeg<'a>, CompactVal<'a>)> {
        self.emit.pop()
    }

    /// process one complete version chain; keeping the chain local makes the fold
    /// decision a single pass over contiguous memory and delays junking merge
    /// operands until a fold has succeeded
    pub(super) fn process_key(&mut self, versions: &[(LeafSeg<'a>, Val<'a>)], synth: &mut Vec<u8>) {
        let mut out = std::mem::take(&mut self.emit);
        out.clear();
        if out.capacity() < versions.len() {
            out.reserve(versions.len() - out.capacity());
        }
        let mut i = 0;
        let mut raw_fallback = false;

        while i < versions.len() {
            let (k, v) = versions[i];
            if self.drops_retained_abort(&k, &v) {
                self.collect(&v);
                i += 1;
                continue;
            }

            if raw_fallback {
                out.push((k, CompactVal::Page(v)));
                i += 1;
                continue;
            }

            if !self.run_ops.is_empty() {
                if v.is_merge() && k.txid() <= self.txid {
                    self.run_ops.push((k, v));
                    i += 1;
                    continue;
                }

                if v.is_merge() {
                    out.push((k, CompactVal::Page(v)));
                    i += 1;
                    continue;
                }

                let base_rec = if v.is_tombstone() {
                    None
                } else {
                    let (rec, _r) = v.get_record(self.loader);
                    Some(rec)
                };
                let base = base_rec.as_ref().map(|rec| rec.data());
                let boundary = self.run_ops[0].0;
                match self.fold_run(self.operator.expect("run requires operator"), base, synth) {
                    Ok(blob) => {
                        out.push((
                            boundary,
                            CompactVal::Synth {
                                env: synth.clone(),
                                blob,
                            },
                        ));
                        for (_, old) in std::mem::take(&mut self.run_ops) {
                            self.collect(&old);
                        }
                        self.collect(&v);
                        for &(_, old) in &versions[i + 1..] {
                            self.collect(&old);
                        }
                        break;
                    }
                    Err(_) => {
                        out.extend(
                            self.run_ops
                                .drain(..)
                                .map(|(seg, val)| (seg, CompactVal::Page(val))),
                        );
                        out.push((k, CompactVal::Page(v)));
                        raw_fallback = true;
                        i += 1;
                    }
                }
                continue;
            }

            if v.is_merge() {
                if k.txid() <= self.txid {
                    self.run_ops.push((k, v));
                } else {
                    out.push((k, CompactVal::Page(v)));
                }
                i += 1;
                continue;
            }

            if k.txid() <= self.txid {
                if !v.is_tombstone() {
                    out.push((k, CompactVal::Page(v)));
                } else {
                    self.collect(&v);
                }
                self.collect_remaining(versions, i + 1);
                break;
            }

            out.push((k, CompactVal::Page(v)));
            i += 1;
        }

        if !self.run_ops.is_empty() {
            let boundary = self.run_ops[0].0;
            match self.fold_run(self.operator.expect("run requires operator"), None, synth) {
                Ok(blob) => {
                    out.push((
                        boundary,
                        CompactVal::Synth {
                            env: synth.clone(),
                            blob,
                        },
                    ));
                    for (_, old) in std::mem::take(&mut self.run_ops) {
                        self.collect(&old);
                    }
                }
                Err(_) => {
                    out.extend(
                        self.run_ops
                            .drain(..)
                            .map(|(seg, val)| (seg, CompactVal::Page(val))),
                    );
                }
            }
        }

        out.reverse();
        self.emit = out;
    }

    #[inline(always)]
    fn drops_retained_abort(&self, k: &LeafSeg<'a>, v: &Val<'a>) -> bool {
        k.txid() <= self.txid
            && self
                .abort_ctx
                .is_some_and(|ctx| is_retained_abort(&ctx, v.group_id(), k.txid()))
    }

    fn collect_remaining(&mut self, versions: &[(LeafSeg<'a>, Val<'a>)], start: usize) {
        for &(_, v) in &versions[start..] {
            self.collect(&v);
        }
    }

    /// outcome for the operator-free streaming path
    fn check_out(&mut self, k: &LeafSeg<'a>, v: &Val<'a>) -> FilterOut {
        if let Some(ctx) = self.drop_aborted_ctx
            && is_retained_abort(&ctx, v.group_id(), k.txid())
        {
            self.removed = true;
            return FilterOut::Drop;
        }
        if self.drops_retained_abort(k, v) {
            return FilterOut::Drop;
        }
        if self.operator.is_none() {
            return self.check_plain_fast(k, v);
        }
        self.plain(k, v)
    }

    #[inline(always)]
    fn check_plain_fast(&mut self, k: &LeafSeg<'a>, v: &Val<'a>) -> FilterOut {
        if v.is_merge() && self.last == Some(k.raw()) && self.skip_dup {
            FilterOut::Drop
        } else if v.is_merge() {
            FilterOut::Keep
        } else {
            self.plain(k, v)
        }
    }

    /// plain (non-merge) version handling with the existing safe-txid trimming
    fn plain(&mut self, k: &LeafSeg<'a>, v: &Val<'a>) -> FilterOut {
        check_plain_version(self.txid, &mut self.last, &mut self.skip_dup, k, v).0
    }

    /// folds the collected run (newest first) onto an optional base and encodes the
    /// synthesized envelope into `synth` as [flags][gid][len u32][gid][payload];
    /// the row carries the safe boundary version's logical group
    pub(super) fn fold_run(
        &mut self,
        op: &dyn crate::MergeOperator,
        base: Option<&[u8]>,
        synth: &mut Vec<u8>,
    ) -> Result<Option<Vec<u8>>, OpCode> {
        let (b_seg, b_val) = self.run_ops[0];
        // the stable user raw key of the folded run: every run member shares it,
        // and it must be the FULL key (prefix + base) — LeafSeg::raw() returns
        // the stripped tail, which would dispatch the wrong algebra on a
        // compressed page
        let key = b_seg.full_key(&mut self.fold_key);
        let (b_rec, _r) = b_val.get_record(self.loader);
        let gid = b_rec.group_id();
        let mut acc: Option<Vec<u8>> = None;
        for &(_, val) in self.run_ops.iter().rev() {
            let (rec, _r) = val.get_record(self.loader);
            let data = rec.data();
            acc = Some(match acc {
                None => data.to_vec(),
                Some(prev) => {
                    // the operator is total; empty or oversized combine output is
                    // an engine-detected contract violation, not an operator error
                    let combined = op.combine_operands(key, &prev, data);
                    if combined.is_empty() {
                        self.blocked_hits.push(key.to_vec());
                        return Err(OpCode::MergeContractViolation);
                    }
                    if size_of::<u8>() + combined.len() > Options::MAX_KV_SIZE {
                        self.blocked_hits.push(key.to_vec());
                        return Err(OpCode::MergeContractViolation);
                    }
                    combined
                }
            });
        }
        let acc = acc.expect("run non-empty");
        let applied = match &base {
            None => op.apply(key, None, &acc),
            Some(b) => op.apply(key, Some(b), &acc),
        };
        let (flags, data) = match applied {
            None => (Val::DEL_BIT, Vec::new()), // apply -> None: tombstone-shaped fold
            Some(v) if v.is_empty() => {
                self.blocked_hits.push(key.to_vec());
                return Err(OpCode::MergeContractViolation);
            }
            Some(v) => (0u8, v),
        };
        let payload_len = size_of::<u8>() + data.len();
        match classify_fold_size(data.len(), self.inline_size) {
            FoldClass::TooLarge => {
                // the folded base cannot be persisted: keep the chain raw-preserving
                self.blocked_hits.push(key.to_vec());
                return Err(OpCode::MergeContractViolation);
            }
            FoldClass::Blob => {
                // remote envelope [flags|REMOTE][gid][len u32][addr u64]; the
                // address slot is a provisional zero filled by the second pass
                synth.clear();
                synth.push(flags | Val::REMOTE_BIT);
                synth.push(gid);
                synth.extend_from_slice(&(payload_len as u32).to_le_bytes());
                synth.extend_from_slice(&[0u8; 8]);
                let mut blob = Vec::with_capacity(payload_len);
                blob.push(gid);
                blob.extend_from_slice(&data);
                return Ok(Some(blob));
            }
            FoldClass::Inline => {}
        }
        synth.clear();
        synth.push(flags);
        synth.push(gid);
        synth.extend_from_slice(&(payload_len as u32).to_le_bytes());
        synth.push(gid);
        synth.extend_from_slice(&data);
        Ok(None)
    }

    #[inline(always)]
    pub(super) fn check(&mut self, x: &(LeafSeg<'a>, Val<'a>)) -> FilterOut {
        let (k, v) = x;
        match self.check_out(k, v) {
            FilterOut::Drop => {
                self.collect(v);
                FilterOut::Drop
            }
            other => other,
        }
    }

    fn collect(&mut self, v: &Val) {
        let remote = v.get_remote();
        if remote != NULL_ADDR {
            self.junks.push(RemoteView::tag(remote));
        }
    }
}
