use super::*;

fn get_impl<K: AsRef<[u8]>>(
    ctx: &Context,
    tree: &Tree,
    snapshot: SnapshotStamp,
    k: K,
) -> Result<ValRef, OpCode> {
    #[cfg(feature = "extra_check")]
    assert!(!k.as_ref().is_empty(), "key must be non-empty");

    let g = crossbeam_epoch::pin();
    let key = Key::new(k.as_ref(), Ver::new(snapshot.start_ts, NULL_CMD));
    let r = tree.traverse(&g, key, |txid, record_gid| {
        is_visible_to(ctx, snapshot, record_gid, txid)
    })?;
    Ok(r)
}

fn seek_impl<'a, K>(tree: &'a Tree, snapshot: SnapshotStamp, prefix: K) -> Iter<'a>
where
    K: AsRef<[u8]>,
{
    let b = prefix.as_ref();
    #[cfg(feature = "extra_check")]
    assert!(!b.is_empty(), "prefix can't be empty");

    let upper = prefix_upper_exclusive(b);
    if let Some(upper) = upper {
        tree.range(b..upper.as_slice(), snapshot)
    } else {
        tree.range(b.., snapshot)
    }
}

fn range_impl<'a, K, R>(tree: &'a Tree, snapshot: SnapshotStamp, range: R) -> Iter<'a>
where
    K: AsRef<[u8]>,
    R: RangeBounds<K>,
{
    tree.range(range, snapshot)
}

pub(super) fn prefix_upper_exclusive(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for i in (0..upper.len()).rev() {
        if upper[i] != u8::MAX {
            upper[i] += 1;
            upper.truncate(i + 1);
            return Some(upper);
        }
    }
    None
}

impl<'a> TxnView<'a> {
    pub(crate) fn new(ctx: &'a Context, tree: &'a Tree) -> Result<Self, OpCode> {
        let pin = ctx.alloc_view_pin();
        Ok(Self { ctx, pin, tree })
    }

    /// Gets the value associated with a key in this view.
    /// **key must be non-empty**.
    ///
    /// Merge operands fold through the bucket's registered merge operator. If the
    /// bucket was opened without one while a key's visible version chain still
    /// holds merge operands (written by an earlier process that had the operator
    /// registered), the chain cannot be interpreted and this returns
    /// [`OpCode::Invalid`] instead of a value — no operand or base is surfaced
    /// as a guessed value.
    #[inline]
    pub fn get<K: AsRef<[u8]>>(&self, k: K) -> Result<ValRef, OpCode> {
        get_impl(
            self.ctx,
            self.tree,
            SnapshotStamp::view(self.pin.start_ts()),
            k,
        )
    }

    /// Seeks an iterator to a key prefix in this view.
    /// prefix can't be empty and the [`Iter::Item`] is only valid in current iteration.
    ///
    /// **NOTE:** [`Iter`] will save a clone of the resource, so do not save [`Iter`] to avoid
    /// resource shortage.
    #[inline]
    pub fn seek<K>(&self, prefix: K) -> Iter<'_>
    where
        K: AsRef<[u8]>,
    {
        seek_impl(self.tree, SnapshotStamp::view(self.pin.start_ts()), prefix)
    }

    #[inline]
    pub fn range<K, R>(&self, range: R) -> Iter<'_>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        range_impl(self.tree, SnapshotStamp::view(self.pin.start_ts()), range)
    }

    #[inline]
    #[cfg(feature = "extra_check")]
    pub(crate) fn testing_start_ts(&self) -> u64 {
        self.pin.start_ts()
    }
}

impl<'a> TxnKV<'a> {
    /// Gets the value associated with a key.
    /// **key must be non-empty**.
    ///
    /// Merge operands fold through the bucket's registered merge operator. If the
    /// bucket was opened without one while a key's visible version chain still
    /// holds merge operands (written by an earlier process that had the operator
    /// registered), the chain cannot be interpreted and this returns
    /// [`OpCode::Invalid`] instead of a value — no operand or base is surfaced
    /// as a guessed value.
    #[inline]
    pub fn get<K>(&self, k: K) -> Result<ValRef, OpCode>
    where
        K: AsRef<[u8]>,
    {
        let state = self.state_ref();
        get_impl(self.ctx, self.tree, Self::snapshot(state), k)
    }

    /// Seeks an iterator to a key prefix.
    /// prefix can't be empty and the [`Iter::Item`] is only valid in current iteration.
    ///
    /// **NOTE:** [`Iter`] will save a clone of the resource, so do not save [`Iter`] to avoid
    /// resource shortage.
    #[inline]
    pub fn seek<K>(&self, prefix: K) -> Iter<'_>
    where
        K: AsRef<[u8]>,
    {
        let state = self.state_ref();
        seek_impl(self.tree, Self::snapshot(state), prefix)
    }

    #[inline]
    pub fn range<K, R>(&self, range: R) -> Iter<'_>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let state = self.state_ref();
        range_impl(self.tree, Self::snapshot(state), range)
    }
}

impl Drop for TxnView<'_> {
    fn drop(&mut self) {
        self.ctx.free_view_pin(self.pin);
    }
}
