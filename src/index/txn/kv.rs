use super::*;

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
        #[cfg(feature = "metrics")]
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

    pub(super) fn should_abort(&self) -> Result<(), OpCode> {
        let state = self.state_ref();
        let g = self.ctx.group(state.group());

        if self.is_end.get() || g.ckpt_cnt.load(Relaxed) - state.start_ckpt >= self.limit {
            return Err(OpCode::AbortTx);
        }
        Ok(())
    }

    #[inline]
    pub(super) fn state_ref(&self) -> &TxnState {
        unsafe { &*self.state.get() }
    }

    #[inline]
    #[cfg(feature = "extra_check")]
    pub(crate) fn testing_start_ts(&self) -> u64 {
        self.state_ref().start_ts
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub(super) fn state_mut(&self) -> &mut TxnState {
        unsafe { &mut *self.state.get() }
    }

    #[cfg(feature = "metrics")]
    #[inline]
    pub(super) fn observe_counter(&self, metric: CounterMetric, delta: u64) {
        self.ctx.opt.observer.counter(metric, delta);
    }

    #[cfg(feature = "metrics")]
    #[inline]
    fn observe_event(&self, event: ObserveEvent) {
        self.ctx.opt.observer.event(event);
    }

    /// Takes the foreground admission permit before a mutation enters the tree.
    ///
    /// The permit is what bounds dirty memory: a writer that would exceed the ceiling
    /// waits here instead of inside `tree.update`, so a blocked writer never holds a
    /// partial mutation.
    #[inline]
    pub(super) fn before_write_budget(&self, estimated_bytes: usize) -> ForegroundWritePermit {
        self.tree
            .bucket
            .before_foreground_write(estimated_bytes as u64)
    }

    #[inline]
    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    fn conflict_abort(&self, txid: u64) -> OpCode {
        #[cfg(feature = "metrics")]
        {
            self.observe_counter(CounterMetric::TxnConflictAbort, 1);
            self.observe_event(ObserveEvent {
                kind: EventKind::TxnConflictAbort,
                bucket_id: self.bucket_id,
                txid,
                file_id: 0,
                value: 0,
            });
        }
        OpCode::AbortTx
    }

    #[inline]
    pub(super) fn write_abort(&self, start_ts: u64, cause: FailCause) -> OpCode {
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
    pub(super) fn snapshot(state: &TxnState) -> SnapshotStamp {
        SnapshotStamp::txn(state.group() as u8, state.start_ts)
    }

    pub(super) fn resolve_latest_meta_for_write(
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
        if is_retained_abort(self.ctx, rv.group_id, rv.ver.txid) {
            return Err(FailCause::Aborted);
        }
        Err(FailCause::Conflict)
    }

    /// admission resolution for merge writes: a concurrent head is admitted only when it is
    /// itself a merge operand and the bucket has a runtime operator; every other invisible
    /// head keeps first-writer-wins
    pub(super) fn resolve_latest_meta_for_merge(
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
        if is_retained_abort(self.ctx, rv.group_id, rv.ver.txid) {
            return Err(FailCause::Aborted);
        }
        // merge/merge coexists under the same runtime operator; every other concurrent
        // invisible head keeps first-writer-wins
        if rv.is_merge && self.tree.bucket.merge_operator().is_some() {
            return Ok(Some(*rv));
        }
        Err(FailCause::Conflict)
    }

    /// Commits the transaction.
    pub fn commit(self) -> Result<(), OpCode> {
        self.should_abort()?;
        let state = self.state_ref();
        #[cfg(feature = "metrics")]
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
            #[cfg(feature = "metrics")]
            {
                self.observe_counter(CounterMetric::TxnCommit, 1);
                observe_elapsed(
                    self.ctx.opt.observer.as_ref(),
                    HistogramMetric::TxnCommitMicros,
                    commit_started,
                );
            }
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
        #[cfg(feature = "metrics")]
        {
            self.observe_counter(CounterMetric::TxnCommit, 1);
            observe_elapsed(
                self.ctx.opt.observer.as_ref(),
                HistogramMetric::TxnCommitMicros,
                commit_started,
            );
        }
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
            #[cfg(feature = "metrics")]
            self.observe_counter(CounterMetric::TxnAbort, 1);
            self.is_end.set(true);
        }
        self.ctx.group(group_id).leave_inflight();
        self.tree.bucket.state.dec_txn_ref();
    }
}
