use super::{ValRef, tree::LatestValMeta};
use crate::{
    OpCode,
    cc::{
        SnapshotStamp,
        context::{CCNode, Context},
        group::{TxnState, WriterGroup},
        is_visible_to,
        wal::{WalDel, WalPut, WalReplace},
    },
    index::tree::{Iter, Tree},
    map::flow::ForegroundWritePermit,
    must_ok,
    types::data::{Key, Record, Ver},
    utils::{
        Handle, NULL_CMD,
        observe::{
            CounterMetric, EventKind, HistogramMetric, LATENCY_SAMPLE_SHIFT, ObserveEvent,
            observe_elapsed, sampled_instant,
        },
    },
};
use crossbeam_epoch::Guard;
use std::cell::{Cell, UnsafeCell};
use std::ops::RangeBounds;
use std::sync::atomic::Ordering::Relaxed;

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
    if let Some(ref upper) = upper {
        tree.range(b..upper.as_slice(), move |ctx, txid, record_gid| {
            is_visible_to(ctx, snapshot, record_gid, txid)
        })
    } else {
        tree.range(b.., move |ctx, txid, record_gid| {
            is_visible_to(ctx, snapshot, record_gid, txid)
        })
    }
}

fn range_impl<'a, K, R>(tree: &'a Tree, snapshot: SnapshotStamp, range: R) -> Iter<'a>
where
    K: AsRef<[u8]>,
    R: RangeBounds<K>,
{
    tree.range(range, move |ctx, txid, record_gid| {
        is_visible_to(ctx, snapshot, record_gid, txid)
    })
}

fn prefix_upper_exclusive(prefix: &[u8]) -> Option<Vec<u8>> {
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

/// A read-write transaction.
pub struct TxnKV<'a> {
    ctx: &'a Context,
    state: UnsafeCell<TxnState>,
    tree: &'a Tree,
    bucket_id: u64,
    is_end: Cell<bool>,
    limit: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailCause {
    Aborted,
    Conflict,
}

struct RegGuard<'a> {
    group: &'a WriterGroup,
    finished: bool,
}

impl<'a> RegGuard<'a> {
    fn new(group: &'a WriterGroup) -> Self {
        Self {
            group,
            finished: false,
        }
    }

    fn finish(&mut self) {
        self.finished = true;
    }
}

impl Drop for RegGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.group.reg_abot();
        }
    }
}

impl<'a> TxnKV<'a> {
    pub(crate) fn new(ctx: &'a Context, tree: &'a Tree) -> Result<Self, OpCode> {
        let gid = ctx.next_group_id();
        let g = ctx.group(gid);
        let start_ckpt = g.ckpt_cnt.load(Relaxed);
        let mut state = TxnState::new(gid as u8, 0, start_ckpt);
        let bucket_id = tree.bucket_id();
        let max_ckpt_per_txn = tree.store.opt.max_ckpt_per_txn;

        tree.bucket.state.inc_txn_ref();

        {
            let mut log = g.logging.lock();
            let mut begin_guard = RegGuard::new(g);
            g.start_reg();
            let start_ts = ctx.alloc_oracle();
            state.start_ts = start_ts;
            g.reg_start_ts(start_ts);
            #[cfg(feature = "extra_check")]
            crate::testing::fire_txn_begin_sync_point(
                crate::testing::TxnBeginSyncPoint::AfterBeginTimestampBeforeFactPublish,
                start_ts,
            );
            match log.record_begin(start_ts) {
                Ok(lsn) => {
                    state.begin_lsn = lsn;
                    state.prev_lsn = lsn;
                    g.active_fact(start_ts, lsn);
                    g.reg_end();
                    begin_guard.finish();
                }
                Err(e) => {
                    g.leave_inflight();
                    tree.bucket.state.dec_txn_ref();
                    return Err(e);
                }
            }
        }
        ctx.opt.observer.counter(CounterMetric::TxnBegin, 1);

        Ok(Self {
            ctx,
            state: UnsafeCell::new(state),
            tree,
            bucket_id,
            is_end: Cell::new(false),
            limit: max_ckpt_per_txn,
        })
    }

    fn should_abort(&self) -> Result<(), OpCode> {
        let state = self.state_ref();
        let g = self.ctx.group(state.group());

        if self.is_end.get() || g.ckpt_cnt.load(Relaxed) - state.start_ckpt >= self.limit {
            return Err(OpCode::AbortTx);
        }
        Ok(())
    }

    #[inline]
    fn state_ref(&self) -> &TxnState {
        unsafe { &*self.state.get() }
    }

    #[inline]
    #[cfg(feature = "extra_check")]
    pub(crate) fn testing_start_ts(&self) -> u64 {
        self.state_ref().start_ts
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn state_mut(&self) -> &mut TxnState {
        unsafe { &mut *self.state.get() }
    }

    #[inline]
    fn observe_counter(&self, metric: CounterMetric, delta: u64) {
        self.ctx.opt.observer.counter(metric, delta);
    }

    #[inline]
    fn observe_event(&self, event: ObserveEvent) {
        self.ctx.opt.observer.event(event);
    }

    #[inline]
    fn before_write_budget(&self, estimated_bytes: usize) -> ForegroundWritePermit {
        self.tree
            .bucket
            .before_foreground_write(estimated_bytes as u64)
    }

    #[inline]
    fn conflict_abort(&self, txid: u64) -> OpCode {
        self.observe_counter(CounterMetric::TxnConflictAbort, 1);
        self.observe_event(ObserveEvent {
            kind: EventKind::TxnConflictAbort,
            bucket_id: self.bucket_id,
            txid,
            file_id: 0,
            value: 0,
        });
        OpCode::AbortTx
    }

    #[inline]
    fn write_abort(&self, start_ts: u64, cause: FailCause) -> OpCode {
        match cause {
            FailCause::Aborted => OpCode::AbortTx,
            FailCause::Conflict => self.conflict_abort(start_ts),
        }
    }

    #[inline]
    fn is_visible_for_write(&self, snapshot: SnapshotStamp, txid: u64, record_gid: u8) -> bool {
        is_visible_to(self.ctx, snapshot, record_gid, txid)
    }

    #[inline]
    fn snapshot(state: &TxnState) -> SnapshotStamp {
        SnapshotStamp::txn(state.group() as u8, state.start_ts)
    }

    fn resolve_latest_meta_for_write(
        &self,
        opt: &Option<LatestValMeta>,
        state: &TxnState,
    ) -> Result<Option<LatestValMeta>, FailCause> {
        let Some(rv) = opt else {
            return Ok(None);
        };
        let snapshot = Self::snapshot(state);
        if self.is_visible_for_write(snapshot, rv.ver.txid, rv.group_id) {
            return Ok(Some(*rv));
        }
        if self
            .ctx
            .group(rv.group_id as usize)
            .is_retained_abort(rv.ver.txid)
        {
            return Err(FailCause::Aborted);
        }
        Err(FailCause::Conflict)
    }

    fn clean_aborted(&self, g: &Guard, raw: &[u8]) -> Result<bool, OpCode> {
        let latest = match self
            .tree
            .get(g, Key::new(raw, Ver::new(u64::MAX, u32::MAX)))
        {
            Ok((k, v)) => Some((k, v)),
            Err(OpCode::NotFound | OpCode::Again) => None,
            Err(e) => return Err(e),
        };
        let Some((k, v)) = latest else {
            return Ok(false);
        };
        if !self
            .ctx
            .group(v.group_id() as usize)
            .is_retained_abort(k.ver().txid)
        {
            return Ok(false);
        }

        match self.tree.remove_aborted_head(g, raw, k.ver().txid) {
            Ok(true) => {
                g.flush();
                Ok(true)
            }
            Ok(false) => Ok(false),
            Err(OpCode::Again) => {
                g.flush();
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    fn put_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<(), OpCode> {
        let estimated = k.len().saturating_add(v.len());
        #[cfg(feature = "extra_check")]
        assert!(!k.is_empty(), "key must be non-empty");

        loop {
            self.should_abort()?;
            let g = crossbeam_epoch::pin();
            let state = self.state_mut();
            let start_ts = state.start_ts;
            let gid = state.group();

            let cmd_id_val = state.cmd_id;
            state.cmd_id += 1;
            let key = Key::new(k, Ver::new(start_ts, cmd_id_val));
            let val = Record::normal(gid as u8, v);
            let _write_permit = self.before_write_budget(estimated);
            let mut abort_cause = FailCause::Conflict;

            let res = self.tree.update(&g, key, val, |opt| {
                let g = self.ctx.group(gid);

                let current = match self.resolve_latest_meta_for_write(opt, state) {
                    Ok(current) => current,
                    Err(cause) => {
                        abort_cause = cause;
                        return Err(self.write_abort(state.start_ts, cause));
                    }
                };
                let r = match current {
                    None => Ok(()),
                    Some(current) => {
                        if !current.is_del {
                            Err(OpCode::Exist)
                        } else {
                            Ok(())
                        }
                    }
                };

                if r.is_ok() && !*logged {
                    *logged = true;
                    state.modified = true;
                    let mut log = g.logging.lock();
                    let new_pos = log.record_update(
                        &Key::new(k, key.ver().to_owned()),
                        WalPut::new(v.len()),
                        v,
                        state.prev_lsn,
                        self.bucket_id,
                    )?;
                    state.prev_lsn = new_pos;
                }
                r.map(|_| (gid as u8, state.prev_lsn))
            });

            match res {
                Err(OpCode::AbortTx) if abort_cause == FailCause::Aborted => {
                    let _ = self.clean_aborted(&g, k)?;
                    continue;
                }
                Ok(_) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    fn update_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<(), OpCode> {
        let estimated = k.len().saturating_add(v.len().saturating_mul(2));
        #[cfg(feature = "extra_check")]
        assert!(!k.is_empty(), "key must be non-empty");

        loop {
            self.should_abort()?;
            let g = crossbeam_epoch::pin();
            let state = self.state_mut();
            let start_ts = state.start_ts;
            let gid = state.group();

            let cmd_id_val = state.cmd_id;
            state.cmd_id += 1;
            let key = Key::new(k, Ver::new(start_ts, cmd_id_val));
            let val = Record::normal(gid as u8, v);
            let _write_permit = self.before_write_budget(estimated);
            let mut abort_cause = FailCause::Conflict;

            let res = self.tree.update(&g, key, val, |opt| {
                let g = self.ctx.group(gid);
                let current = match self.resolve_latest_meta_for_write(opt, state) {
                    Ok(current) => current,
                    Err(cause) => {
                        abort_cause = cause;
                        return Err(self.write_abort(state.start_ts, cause));
                    }
                };
                let Some(current) = current else {
                    return Err(OpCode::NotFound);
                };
                if current.is_del {
                    return Err(OpCode::NotFound);
                }

                if !*logged {
                    state.modified = true;
                    *logged = true;
                    let mut log = g.logging.lock();
                    let new_pos = log.record_update(
                        &Key::new(k, key.ver().to_owned()),
                        WalReplace::new(v.len()),
                        v,
                        state.prev_lsn,
                        self.bucket_id,
                    )?;
                    state.prev_lsn = new_pos;
                }
                Ok((gid as u8, state.prev_lsn))
            });

            match res {
                Err(OpCode::AbortTx) if abort_cause == FailCause::Aborted => {
                    let _ = self.clean_aborted(&g, k)?;
                    continue;
                }
                Ok(_) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    /// Puts a key-value pair into the bucket.
    /// **key must be non-empty**.
    pub fn put<K, V>(&self, k: K, v: V) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut logged = false;
        self.put_impl(k.as_ref(), v.as_ref(), &mut logged)
    }

    /// Updates existing key-value pair in the bucket.
    /// **key must be non-empty**.
    pub fn update<K, V>(&self, k: K, v: V) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut logged = false;
        self.update_impl(k.as_ref(), v.as_ref(), &mut logged)
    }

    /// Upserts a key-value pair into the bucket.
    /// **key must be non-empty**.
    pub fn upsert<K, V>(&self, k: K, v: V) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut logged = false;
        let (k, v) = (k.as_ref(), v.as_ref());
        let estimated = k.len().saturating_add(v.len().saturating_mul(2));
        #[cfg(feature = "extra_check")]
        assert!(!k.is_empty(), "key must be non-empty");

        loop {
            self.should_abort()?;
            let g = crossbeam_epoch::pin();
            let state = self.state_mut();
            let start_ts = state.start_ts;
            let gid = state.group();

            let cmd_id_val = state.cmd_id;
            state.cmd_id += 1;
            let key = Key::new(k, Ver::new(start_ts, cmd_id_val));
            let val = Record::normal(gid as u8, v);
            let _write_permit = self.before_write_budget(estimated);
            let mut abort_cause = FailCause::Conflict;

            let res = self.tree.update(&g, key, val, |opt| {
                let g = self.ctx.group(gid);

                let current = match self.resolve_latest_meta_for_write(opt, state) {
                    Ok(current) => current,
                    Err(cause) => {
                        abort_cause = cause;
                        return Err(self.write_abort(state.start_ts, cause));
                    }
                };

                if !logged {
                    logged = true;
                    state.modified = true;
                    let mut log = g.logging.lock();
                    let new_pos = match current {
                        None => log.record_update(
                            &Key::new(k, key.ver().to_owned()),
                            WalPut::new(v.len()),
                            v,
                            state.prev_lsn,
                            self.bucket_id,
                        )?,
                        Some(_) => log.record_update(
                            &Key::new(k, key.ver().to_owned()),
                            WalReplace::new(v.len()),
                            v,
                            state.prev_lsn,
                            self.bucket_id,
                        )?,
                    };
                    state.prev_lsn = new_pos;
                }
                Ok((gid as u8, state.prev_lsn))
            });

            match res {
                Err(OpCode::AbortTx) if abort_cause == FailCause::Aborted => {
                    let _ = self.clean_aborted(&g, k)?;
                    continue;
                }
                Ok(_) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    /// Deletes a key-value pair from the bucket.
    /// **key must be non-empty**.
    pub fn del<T>(&self, k: T) -> Result<(), OpCode>
    where
        T: AsRef<[u8]>,
    {
        let mut logged = false;
        let k = k.as_ref();
        #[cfg(feature = "extra_check")]
        assert!(!k.is_empty(), "key must be non-empty");

        loop {
            self.should_abort()?;
            let g = crossbeam_epoch::pin();
            let state = self.state_mut();
            let start_ts = state.start_ts;
            let gid = state.group();
            let cmd_id_val = state.cmd_id;
            state.cmd_id += 1;

            let key = Key::new(k, Ver::new(start_ts, cmd_id_val));
            let val = Record::remove(gid as u8);
            let _write_permit = self.before_write_budget(key.raw.len());
            let mut abort_cause = FailCause::Conflict;

            let res = self.tree.update(&g, key, val, |opt| {
                let g = self.ctx.group(gid);
                let current = match self.resolve_latest_meta_for_write(opt, state) {
                    Ok(current) => current,
                    Err(cause) => {
                        abort_cause = cause;
                        return Err(self.write_abort(state.start_ts, cause));
                    }
                };
                let Some(current) = current else {
                    return Err(OpCode::NotFound);
                };
                if current.is_del {
                    return Err(OpCode::NotFound);
                }

                if !logged {
                    logged = true;
                    state.modified = true;
                    let mut log = g.logging.lock();
                    let new_pos = log.record_update(
                        &key,
                        WalDel::new(),
                        [].as_slice(),
                        state.prev_lsn,
                        self.bucket_id,
                    )?;
                    state.prev_lsn = new_pos;
                }
                Ok((gid as u8, state.prev_lsn))
            });

            match res {
                Err(OpCode::AbortTx) if abort_cause == FailCause::Aborted => {
                    let _ = self.clean_aborted(&g, k)?;
                    continue;
                }
                Ok(_) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    /// Commits the transaction.
    pub fn commit(self) -> Result<(), OpCode> {
        self.should_abort()?;
        let state = self.state_ref();
        let commit_started = sampled_instant(state.start_ts, LATENCY_SAMPLE_SHIFT);
        let g = self.ctx.group(state.group());

        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check("mace_txn_commit_begin")?;

        if !state.modified {
            {
                let mut log = g.logging.lock();
                log.record_commit(state.start_ts)?;
                g.remove_fact(state.start_ts);
            }
            self.is_end.set(true);
            self.observe_counter(CounterMetric::TxnCommit, 1);
            observe_elapsed(
                self.ctx.opt.observer.as_ref(),
                HistogramMetric::TxnCommitMicros,
                commit_started,
            );
            return Ok(());
        }

        let mut log = g.logging.lock();
        log.record_commit(state.start_ts)?;
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check("mace_txn_commit_after_record_commit")?;
        log.sync(false)?;
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check("mace_txn_commit_after_wal_sync")?;
        g.commit_fact(state.start_ts, || self.ctx.alloc_oracle());

        self.is_end.set(true);
        self.observe_counter(CounterMetric::TxnCommit, 1);
        observe_elapsed(
            self.ctx.opt.observer.as_ref(),
            HistogramMetric::TxnCommitMicros,
            commit_started,
        );
        Ok(())
    }

    /// Gets the value associated with a key.
    /// **key must be non-empty**.
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

impl Drop for TxnKV<'_> {
    fn drop(&mut self) {
        let group_id = self.state_ref().group();
        if !self.is_end.get() {
            let state = self.state_ref();
            let g = self.ctx.group(state.group());
            let modified = state.modified;

            let mut log = g.logging.lock();
            must_ok!(log.record_abort(state.start_ts));
            if modified {
                must_ok!(log.sync(false));
            }
            let abort_clean_task = modified.then(|| {
                self.ctx.build_abort_clean_task(
                    state.start_ts,
                    self.bucket_id,
                    state.group() as u8,
                    state.prev_lsn,
                    state.begin_lsn.file_id,
                )
            });
            if modified {
                g.abort_fact(state.start_ts);
            } else {
                g.remove_fact(state.start_ts);
            }
            if let Some(task) = abort_clean_task {
                self.ctx.enqueue_abort_clean_task(task);
                #[cfg(feature = "extra_check")]
                crate::testing::fire_txn_abort_sync_point(
                    crate::testing::TxnAbortSyncPoint::AfterAbortCleanEnqueueBeforeLoggingRelease,
                    state.start_ts,
                );
            }
            drop(log);
            self.observe_counter(CounterMetric::TxnAbort, 1);
            self.is_end.set(true);
        }
        self.ctx.group(group_id).leave_inflight();
        self.tree.bucket.state.dec_txn_ref();
    }
}

/// A read-only transaction (consistent view).
pub struct TxnView<'a> {
    ctx: &'a Context,
    pin: Handle<CCNode>,
    tree: &'a Tree,
}

impl<'a> TxnView<'a> {
    pub(crate) fn new(ctx: &'a Context, tree: &'a Tree) -> Result<Self, OpCode> {
        let pin = ctx.alloc_view_pin();
        Ok(Self { ctx, pin, tree })
    }

    /// Gets the value associated with a key in this view.
    /// **key must be non-empty**.
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

impl Drop for TxnView<'_> {
    fn drop(&mut self) {
        self.ctx.free_view_pin(self.pin);
    }
}

#[cfg(test)]
mod test {
    use super::prefix_upper_exclusive;
    use crate::{BucketOptions, Mace, OpCode, Options, RandomPath};

    #[test]
    fn txnkv() {
        txnkv_impl().unwrap();
    }

    #[test]
    fn prefix_upper_exclusive_handles_carry() {
        assert_eq!(
            prefix_upper_exclusive(&[0x61, 0x62, 0x63]),
            Some(vec![0x61, 0x62, 0x64])
        );
        assert_eq!(
            prefix_upper_exclusive(&[0x61, 0xff, 0xff]),
            Some(vec![0x62])
        );
        assert_eq!(prefix_upper_exclusive(&[0xff]), None);
        assert_eq!(prefix_upper_exclusive(&[0xff, 0xff]), None);
    }

    fn txnkv_impl() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let _ = std::fs::remove_dir_all(&*path);
        let opt = Options::new(&*path).validate().unwrap();
        let mace = Mace::new(opt)?;
        let (k1, k2) = ("beast".as_bytes(), "senpai".as_bytes());
        let (v1, v2) = ("114514".as_bytes(), "1919810".as_bytes());
        let db = mace.new_bucket("default", BucketOptions::default())?;

        let kv = db.begin()?;
        kv.put(k1, v1).expect("can't put");
        kv.put(k2, v2).expect("can't put");

        kv.del(k1).expect("can't del");
        kv.commit()?;

        let kv = db.begin()?;
        let r = kv.get(k1);
        assert!(r.is_err());

        let r = kv.get(k2).expect("can't get");
        assert_eq!(r.slice(), v2);

        kv.del(k2).expect("can't del");
        drop(kv);

        let kv = db.begin()?;
        let r = kv.get(k1);
        assert!(r.is_err());
        kv.del(k2).expect("can't del");
        let r = kv.del(k2);
        assert!(r.is_err());

        kv.commit()?;

        let kv = db.begin()?;
        let r = kv.get(k1);
        assert!(r.is_err());
        let r = kv.get(k2);
        assert!(r.is_err());

        kv.commit()?;

        {
            let kv = db.begin()?;
            kv.put("1", "10")?;
            kv.commit()?;

            let kv = db.begin()?;
            kv.update("1", "11").expect("can't replace");
            drop(kv);

            let view = db.view()?;
            let x = view.get("1").expect("can't get");
            assert_eq!(x.slice(), "10".as_bytes());
        }

        {
            let kv = db.begin()?;
            kv.put("2", "20")?;
            kv.update("2", "21")?;
            let r = kv.get("2").unwrap();
            assert_eq!(r.slice(), "21".as_bytes());
            kv.del("2")?;
            drop(kv);

            let view = db.view()?;
            let x = view.get("2");
            assert!(x.is_err());
        }

        {
            let kv = db.begin()?;
            kv.put("11", "10")?;
            kv.commit()?;

            let kv = db.begin()?;
            kv.upsert("11", "11").expect("can't replace");
            drop(kv);

            let view = db.view()?;
            let x = view.get("11").expect("can't get");
            assert_eq!(x.slice(), "10".as_bytes());
        }

        {
            let kv = db.begin()?;
            kv.put("22", "20")?;
            kv.upsert("22", "21")?;
            let r = kv.get("22").unwrap();
            assert_eq!(r.slice(), "21".as_bytes());
            kv.del("22")?;
            drop(kv);

            let view = db.view()?;
            let x = view.get("22");
            assert!(x.is_err());
        }

        {
            let kv = db.begin()?;
            kv.put("elder", "+1s")?;
            kv.del("elder")?;
            kv.commit()?;
            let kv = db.begin()?;
            let r = kv.update("elder", "mo");
            // a remove key can't be update again
            assert!(r.is_err());
            // but can be upsert
            kv.upsert("elder", "mo")?;
            kv.commit()?;
            let view = db.view()?;
            assert_eq!(view.get("elder").unwrap().slice(), "mo".as_bytes());
        }

        {
            let kv = db.begin()?;
            kv.put("fast", "v0")?;
            kv.commit()?;

            let kv = db.begin()?;
            kv.update("fast", "v1")?;
            kv.commit()?;

            let view = db.view()?;
            assert_eq!(view.get("fast")?.slice(), b"v1");
        }

        {
            let kv = db.begin()?;
            let r = kv.update("missing", "v1");
            assert!(matches!(r, Err(OpCode::NotFound)));
        }
        drop(db);
        drop(mace);
        Ok(())
    }

    #[test]
    fn cross_long_txn() {
        cross_long_txn_impl().unwrap();
    }

    fn cross_long_txn_impl() -> Result<(), OpCode> {
        let path = RandomPath::new();
        let mut opt = Options::new(&*path);
        let consolidate_threshold = 256;
        opt.tmp_store = true;
        let mace = Mace::new(opt.validate().unwrap())?;
        let db = mace.new_bucket(
            "default",
            BucketOptions {
                split_elems: consolidate_threshold * 2,
                consolidate_threshold,
                ..BucketOptions::default()
            },
        )?;

        let kv = db.begin()?;
        kv.put("foo", "bar")?;
        kv.commit()?;

        let view = db.view()?;
        let kv = db.begin()?;

        kv.update("foo", "bar1")?;
        kv.update("foo", "bar2")?;

        // trigger consolidate
        for i in 0..consolidate_threshold {
            let x = format!("key_{i}");
            kv.put(&x, &x)?;
        }

        let r = kv.get("foo")?;
        assert_eq!(r.slice(), "bar2".as_bytes());
        kv.commit()?;

        let v = view.get("foo")?;
        assert_eq!(v.slice(), "bar".as_bytes());

        drop(view);
        drop(db);
        drop(mace);
        Ok(())
    }
}
