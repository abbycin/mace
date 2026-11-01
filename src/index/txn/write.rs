use super::*;
use crate::cc::wal::{IWalCodec, IWalPayload};

#[derive(Clone, Copy)]
enum WriteOp<'a> {
    Put(&'a [u8], &'a [u8]),
    Update(&'a [u8], &'a [u8]),
    Upsert(&'a [u8], &'a [u8]),
    Del(&'a [u8]),
}

impl<'a> WriteOp<'a> {
    fn key(self) -> &'a [u8] {
        match self {
            Self::Put(k, _) | Self::Update(k, _) | Self::Upsert(k, _) | Self::Del(k) => k,
        }
    }

    fn record(self, gid: u8) -> Record {
        match self {
            Self::Put(_, v) | Self::Update(_, v) | Self::Upsert(_, v) => Record::normal(gid, v),
            Self::Del(_) => Record::remove(gid),
        }
    }

    fn estimate(self, key: &Key<'_>) -> usize {
        match self {
            Self::Put(k, v) => k.len().saturating_add(v.len()),
            Self::Update(k, v) | Self::Upsert(k, v) => {
                k.len().saturating_add(v.len().saturating_mul(2))
            }
            Self::Del(_) => key.raw.len(),
        }
    }

    fn admit(self, current: Option<LatestValMeta>) -> Result<(), OpCode> {
        match self {
            Self::Put(..) => match current {
                None => Ok(()),
                Some(current) => {
                    if !current.is_del {
                        Err(OpCode::Exist)
                    } else {
                        Ok(())
                    }
                }
            },
            Self::Update(..) | Self::Del(..) => match current {
                None => Err(OpCode::NotFound),
                Some(current) => {
                    if current.is_del {
                        Err(OpCode::NotFound)
                    } else {
                        Ok(())
                    }
                }
            },
            Self::Upsert(..) => Ok(()),
        }
    }
}

impl<'a> TxnKV<'a> {
    /// appends this transaction's WAL record for `key` and advances its previous lsn
    fn log_write<T>(
        &self,
        gid: usize,
        key: &Key<'_>,
        w: T,
        nv: &[u8],
        state: &mut TxnState,
    ) -> Result<(), OpCode>
    where
        T: IWalCodec + IWalPayload,
    {
        let mut log = self.ctx.group(gid).logging.lock();
        let new_pos = log.record_update(gid as u8, key, w, nv, state.prev_lsn, self.bucket_id)?;
        state.prev_lsn = new_pos;
        Ok(())
    }

    /// appends the admitted write's WAL record and applies its admission side effects
    fn log_admitted_write(
        &self,
        gid: usize,
        key: &Key<'_>,
        op: WriteOp<'_>,
        current: Option<LatestValMeta>,
        state: &mut TxnState,
    ) -> Result<(), OpCode> {
        match op {
            WriteOp::Put(_, v) => self.log_write(gid, key, WalPut::new(v.len()), v, state),
            WriteOp::Update(_, v) => self.log_write(gid, key, WalReplace::new(v.len()), v, state),
            WriteOp::Upsert(_, v) => match current {
                None => self.log_write(gid, key, WalPut::new(v.len()), v, state),
                Some(_) => self.log_write(gid, key, WalReplace::new(v.len()), v, state),
            },
            WriteOp::Del(_) => {
                // a committed del clears the key's merge-blocked status
                state.deleted_keys.push(key.raw.to_vec());
                self.log_write(gid, key, WalDel::new(), [].as_slice(), state)
            }
        }
    }

    fn write_impl(&self, op: WriteOp<'_>) -> Result<(), OpCode> {
        let k = op.key();
        #[cfg(feature = "extra_check")]
        assert!(!k.is_empty(), "key must be non-empty");

        let mut logged = false;
        loop {
            self.should_abort()?;
            let g = crossbeam_epoch::pin();
            let state = self.state_mut();
            let start_ts = state.start_ts;
            let gid = state.group();

            let cmd_id_val = state.cmd_id;
            state.cmd_id += 1;
            let key = Key::new(k, Ver::new(start_ts, cmd_id_val));
            let val = op.record(gid as u8);
            let estimated = op.estimate(&key);
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
                let r = op.admit(current);

                if r.is_ok() && !logged {
                    logged = true;
                    state.modified = true;
                    self.log_admitted_write(
                        gid,
                        &Key::new(k, key.ver().to_owned()),
                        op,
                        current,
                        state,
                    )?;
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
                    self.log_write(
                        gid,
                        &Key::new(k, key.ver().to_owned()),
                        WalMerge::new(operand.len()),
                        operand,
                        state,
                    )?;
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
        if !is_retained_abort(self.ctx, m.group_id, m.ver.txid) {
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

    /// Puts a key-value pair into the bucket.
    /// **key must be non-empty**.
    pub fn put<K, V>(&self, k: K, v: V) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.write_impl(WriteOp::Put(k.as_ref(), v.as_ref()))
    }

    /// Updates existing key-value pair in the bucket.
    /// **key must be non-empty**.
    pub fn update<K, V>(&self, k: K, v: V) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.write_impl(WriteOp::Update(k.as_ref(), v.as_ref()))
    }

    /// Upserts a key-value pair into the bucket.
    /// **key must be non-empty**.
    pub fn upsert<K, V>(&self, k: K, v: V) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.write_impl(WriteOp::Upsert(k.as_ref(), v.as_ref()))
    }

    /// Deletes a key-value pair from the bucket.
    /// **key must be non-empty**.
    pub fn del<T>(&self, k: T) -> Result<(), OpCode>
    where
        T: AsRef<[u8]>,
    {
        self.write_impl(WriteOp::Del(k.as_ref()))
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
    /// [`OpCode::TooLarge`] when key plus operand exceed the maximum key/value size. Operands
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
}
