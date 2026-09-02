use super::{ValRef, tree::LatestValMeta};
use crate::{
    OpCode, Options,
    cc::{
        SnapshotStamp,
        context::{CCNode, Context},
        group::{TxnState, WriterGroup},
        is_visible_to,
        wal::{WalDel, WalMerge, WalPut, WalReplace},
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
            let mut log = ctx.group(gid).logging.lock();
            let mut begin_guard = RegGuard::new(g);
            g.start_reg();
            let start_ts = ctx.alloc_begin_oracle();
            state.start_ts = start_ts;
            g.reg_start_ts(start_ts);
            #[cfg(feature = "extra_check")]
            crate::testing::fire_txn_begin_sync_point(
                crate::testing::TxnBeginSyncPoint::AfterBeginTimestampBeforeFactPublish,
                start_ts,
            );
            match log.record_begin(gid, start_ts) {
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

    /// admission resolution for merge writes: a concurrent head is admitted only when it is
    /// itself a merge operand and the bucket has a runtime operator; every other invisible
    /// head keeps first-writer-wins
    fn resolve_latest_meta_for_merge(
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
        // merge/merge coexists under the same runtime operator; every other concurrent
        // invisible head keeps first-writer-wins
        if rv.is_merge && self.tree.bucket.merge_operator().is_some() {
            return Ok(Some(*rv));
        }
        Err(FailCause::Conflict)
    }

    /// merge admission with a bounded retry budget for cleaning aborted heads.
    /// Concurrent merge/merge writes may stack operands on one key. When those transactions abort,
    /// a later merge removes the retained-aborted heads one at a time before retrying its update.
    /// A sustained abort stream could otherwise make one call retry indefinitely, so the configured
    /// writer-concurrency budget returns `OpCode::Again` and lets the caller retry with a fresh
    /// transaction.
    fn merge_impl(&self, k: &[u8], operand: &[u8]) -> Result<(), OpCode> {
        let has_operator = self.tree.bucket.merge_operator().is_some();
        // an empty operand would decode as a tombstone, so it never admits
        if !has_operator || operand.is_empty() {
            return Err(OpCode::Invalid);
        }
        // raw operands use the normal inline/remote storage policy and must fit MAX_KV_SIZE
        if k.len() + size_of::<u32>() + operand.len() > Options::MAX_KV_SIZE {
            return Err(OpCode::TooLarge);
        }
        // contract-violating keys stay blocked until a committed delete clears them
        if self.tree.bucket.merge_blocked_keys.contains(k) {
            return Err(OpCode::MergeContractViolation);
        }
        self.tree.bucket.mark_merge();

        let mut logged = false;
        let estimated = k.len().saturating_add(operand.len());

        // bounded internal retry budget for relocations, splits, and lock contention
        let budget = self.ctx.opt.concurrent_write as u32;
        let mut attempts = 0u32;
        loop {
            self.should_abort()?;
            attempts += 1;
            if attempts > budget {
                return Err(OpCode::Again);
            }

            let g = crossbeam_epoch::pin();
            let state = self.state_mut();
            let start_ts = state.start_ts;
            let gid = state.group();

            let cmd_id_val = state.cmd_id;
            state.cmd_id += 1;
            let key = Key::new(k, Ver::new(start_ts, cmd_id_val));
            let val = Record::merge(gid as u8, operand);
            let _write_permit = self.before_write_budget(estimated);
            let mut abort_cause = FailCause::Conflict;

            let res = self.tree.update(&g, key, val, |opt| {
                match self.resolve_latest_meta_for_merge(opt, state) {
                    Ok(_) => {}
                    Err(cause) => {
                        abort_cause = cause;
                        return Err(self.write_abort(state.start_ts, cause));
                    }
                };

                if !logged {
                    logged = true;
                    state.modified = true;
                    let mut log = self.ctx.group(gid).logging.lock();
                    let new_pos = log.record_update(
                        gid as u8,
                        &Key::new(k, key.ver().to_owned()),
                        WalMerge::new(operand.len()),
                        operand,
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

    /// retry-time cleanup of an aborted head blocking this transaction's write.
    /// uses head metadata only: no value materialization, no operator involvement
    fn clean_aborted(&self, g: &Guard, raw: &[u8]) -> Result<bool, OpCode> {
        let latest = self.tree.latest_head_meta(g, raw)?;
        let Some(m) = latest else {
            return Ok(false);
        };
        if !self
            .ctx
            .group(m.group_id as usize)
            .is_retained_abort(m.ver.txid)
        {
            return Ok(false);
        }

        match self.tree.remove_aborted_head(g, raw, m.ver.txid) {
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
                    let mut log = self.ctx.group(gid).logging.lock();
                    let new_pos = log.record_update(
                        gid as u8,
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
                    let mut log = self.ctx.group(gid).logging.lock();
                    let new_pos = log.record_update(
                        gid as u8,
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
                    let mut log = self.ctx.group(gid).logging.lock();
                    let new_pos = match current {
                        None => log.record_update(
                            gid as u8,
                            &Key::new(k, key.ver().to_owned()),
                            WalPut::new(v.len()),
                            v,
                            state.prev_lsn,
                            self.bucket_id,
                        )?,
                        Some(_) => log.record_update(
                            gid as u8,
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
                    // a committed del clears the key's merge-blocked status
                    state.deleted_keys.push(key.raw.to_vec());
                    let mut log = self.ctx.group(gid).logging.lock();
                    let new_pos = log.record_update(
                        gid as u8,
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

    /// Deletes the key and, once this transaction commits, clears its
    /// merge-blocked status; an aborted transaction keeps the blocked state.
    /// Use after `merge` returned `OpCode::MergeContractViolation` to reset the
    /// violating chain. Idempotent: a key that is already absent still records
    /// the commit-gated unblock.
    ///
    /// `reset_merge` is an ordinary delete. To reset and immediately add a new operand, perform
    /// both operations in the same transaction or serialize separate transactions.
    pub fn reset_merge<T>(&self, k: T) -> Result<(), OpCode>
    where
        T: AsRef<[u8]>,
    {
        match self.del(k.as_ref()) {
            Ok(()) => Ok(()),
            // already deleted: no tombstone needed, but the commit must still
            // clear the blocked status
            Err(OpCode::NotFound) => {
                self.state_mut().deleted_keys.push(k.as_ref().to_vec());
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Appends a merge operand for a key under the bucket's runtime merge operator.
    ///
    /// The operand becomes visible to other snapshots only after [`TxnKV::commit`] succeeds.
    /// `merge` does not return a logical value.
    ///
    /// Returns [`OpCode::Invalid`] when the bucket has no runtime merge operator, when the
    /// operand is empty, or when the operator output would be rejected. Returns
    /// [`OpCode::TooLarge`] when key plus operand exceed [`Options::MAX_KV_SIZE`]. Operands
    /// larger than the bucket inline boundary use the normal remote/blob storage path.
    /// Concurrent merge/merge writes may coexist; other overlapping writes use first-writer-wins.
    ///
    /// **key must be non-empty**.
    pub fn merge<K, O>(&self, k: K, operand: O) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        O: AsRef<[u8]>,
    {
        let k = k.as_ref();
        #[cfg(feature = "extra_check")]
        assert!(!k.is_empty(), "key must be non-empty");

        self.merge_impl(k, operand.as_ref())
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
                let mut log = self.ctx.group(state.group()).logging.lock();
                log.record_commit(state.group(), state.start_ts)?;
                g.remove_fact(state.start_ts);
            }
            self.is_end.set(true);
            self.unblock_deleted_keys();
            self.observe_counter(CounterMetric::TxnCommit, 1);
            observe_elapsed(
                self.ctx.opt.observer.as_ref(),
                HistogramMetric::TxnCommitMicros,
                commit_started,
            );
            return Ok(());
        }

        let ticket = {
            let mut log = self.ctx.lock_wal_logging(state.group(), state.start_ts);
            log.record_commit(state.group(), state.start_ts)?;
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::check("mace_txn_commit_after_record_commit")?;
            if self.ctx.opt.sync_on_write {
                let target = log.current_pos();
                Some(log.register_sync(target)?)
            } else {
                log.sync(false)?;
                None
            }
        };
        // publish facts only after the durable generation completes
        if let Some(ticket) = ticket {
            self.ctx.drive_sync(&ticket)?;
        }
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check("mace_txn_commit_after_wal_sync")?;
        g.commit_fact(state.start_ts, || self.ctx.alloc_oracle());
        // test-determinism wakeup (extra_check only; production relies on the
        // collector's own polling cadence)
        #[cfg(feature = "extra_check")]
        self.ctx.request_collect();

        self.is_end.set(true);
        self.unblock_deleted_keys();
        self.observe_counter(CounterMetric::TxnCommit, 1);
        observe_elapsed(
            self.ctx.opt.observer.as_ref(),
            HistogramMetric::TxnCommitMicros,
            commit_started,
        );
        Ok(())
    }

    /// clears the merge-blocked status of every key deleted by this transaction;
    /// only called on committed transactions (abort keeps the blocked state)
    fn unblock_deleted_keys(&self) {
        let state = self.state_ref();
        if state.deleted_keys.is_empty() {
            return;
        }
        for k in &state.deleted_keys {
            self.tree.bucket.merge_blocked_keys.remove(k);
        }
    }

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

impl Drop for TxnKV<'_> {
    fn drop(&mut self) {
        let group_id = self.state_ref().group();
        if !self.is_end.get() {
            let state = self.state_ref();
            let g = self.ctx.group(state.group());
            let modified = state.modified;
            let durable = self.ctx.opt.sync_on_write;

            if modified && durable {
                // publish the abort fact and cleanup pin atomically to GC
                let ticket = {
                    let mut log = self.ctx.lock_wal_logging(state.group(), state.start_ts);
                    must_ok!(log.record_abort(state.group(), state.start_ts));
                    let target = log.current_pos();
                    must_ok!(log.register_sync(target))
                };
                must_ok!(self.ctx.drive_sync(&ticket));
                // recovery reconstructs durable aborts not yet published in memory
                #[cfg(feature = "failpoints")]
                crate::utils::failpoint::crash("mace_txn_abort_after_wal_sync");
                let log = self.ctx.lock_wal_logging(state.group(), state.start_ts);
                let physical_wal_id = log.wal_id;
                let abort_clean_task = self.ctx.build_abort_clean_task(
                    state.start_ts,
                    self.bucket_id,
                    state.group() as u8,
                    physical_wal_id,
                    state.prev_lsn,
                    state.begin_lsn.file_id,
                );
                g.abort_fact(state.start_ts);
                // test-determinism wakeup (extra_check only)
                #[cfg(feature = "extra_check")]
                self.ctx.request_collect();
                #[cfg(feature = "extra_check")]
                crate::testing::fire_txn_abort_sync_point(
                    crate::testing::TxnAbortSyncPoint::AfterAbortFactBeforeAbortCleanEnqueue,
                    state.start_ts,
                );
                self.ctx.enqueue_abort_clean_task(abort_clean_task);
                #[cfg(feature = "extra_check")]
                crate::testing::fire_txn_abort_sync_point(
                    crate::testing::TxnAbortSyncPoint::AfterAbortCleanEnqueueBeforeLoggingRelease,
                    state.start_ts,
                );
                drop(log);
            } else {
                let mut log = self.ctx.lock_wal_logging(state.group(), state.start_ts);
                must_ok!(log.record_abort(state.group(), state.start_ts));
                if modified {
                    must_ok!(log.sync(false));
                }
                let physical_wal_id = log.wal_id;
                let abort_clean_task = modified.then(|| {
                    self.ctx.build_abort_clean_task(
                        state.start_ts,
                        self.bucket_id,
                        state.group() as u8,
                        physical_wal_id,
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
            }
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
