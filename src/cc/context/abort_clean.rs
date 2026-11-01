use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AbortCleanTask {
    pub txid: u64,
    pub bucket_id: u64,
    /// physical wal stream containing the abort-clean chain
    pub physical_wal_id: u8,
    /// logical group owning the retained-abort fact
    pub logical_group_id: u8,
    pub tail_lsn: Position,
    pub pin_file_id: u64,
    pub state: AbortCleanState,
    pub quiesced: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AbortCleanState {
    Pending,
    WaitingQuiesce,
}

impl Context {
    #[inline]
    fn update_pending_abort_clean_floor(&self, txid: u64) {
        let mut floor = self.pending_abort_clean_floor.load(Relaxed);
        while txid < floor {
            match self
                .pending_abort_clean_floor
                .compare_exchange_weak(floor, txid, Relaxed, Relaxed)
            {
                Ok(_) => return,
                Err(actual) => floor = actual,
            }
        }
    }

    fn recompute_pending_abort_clean_floor(&self) {
        let mut floor = u64::MAX;
        for shard in &self.pending_abort_clean {
            if let Some((txid, _)) = shard.read().first_key_value() {
                floor = floor.min(*txid);
            }
        }
        self.pending_abort_clean_floor.store(floor, Relaxed);
    }

    #[inline]
    pub(crate) fn build_abort_clean_task(
        &self,
        txid: u64,
        bucket_id: u64,
        logical_group_id: u8,
        physical_wal_id: u8,
        tail_lsn: Position,
        pin_file_id: u64,
    ) -> AbortCleanTask {
        AbortCleanTask {
            txid,
            bucket_id,
            physical_wal_id,
            logical_group_id,
            tail_lsn,
            pin_file_id: pin_file_id.min(tail_lsn.file_id),
            state: AbortCleanState::Pending,
            quiesced: false,
        }
    }

    #[inline]
    pub(crate) fn enqueue_abort_clean_task(&self, task: AbortCleanTask) {
        let _seq = self.pending_abort_clean_seqlock.write_lock();
        let shard = Self::shard_of(task.txid);
        let old = self.pending_abort_clean[shard]
            .write()
            .insert(task.txid, task);
        if old.is_none() {
            self.pending_abort_clean_nr.fetch_add(1, Relaxed);
            self.update_pending_abort_clean_floor(task.txid);
            let bucket_shard = Self::shard_of(task.bucket_id);
            let mut buckets = self.pending_abort_clean_buckets[bucket_shard].write();
            *buckets.entry(task.bucket_id).or_insert(0) += 1;
        }
    }

    #[inline]
    pub(crate) fn enqueue_abort_clean(
        &self,
        txid: u64,
        bucket_id: u64,
        logical_group_id: u8,
        physical_wal_id: u8,
        tail_lsn: Position,
        pin_file_id: u64,
    ) {
        self.enqueue_abort_clean_task(self.build_abort_clean_task(
            txid,
            bucket_id,
            logical_group_id,
            physical_wal_id,
            tail_lsn,
            pin_file_id,
        ));
    }

    pub(crate) fn remove_abort_clean(&self, txid: u64) {
        let old = {
            let _seq = self.pending_abort_clean_seqlock.write_lock();
            let shard = Self::shard_of(txid);
            let old = self.pending_abort_clean[shard].write().remove(&txid);
            if let Some(task) = old {
                self.pending_abort_clean_nr.fetch_sub(1, Relaxed);
                if self.pending_abort_clean_nr.load(Relaxed) == 0 {
                    self.pending_abort_clean_floor.store(u64::MAX, Relaxed);
                } else if txid <= self.pending_abort_clean_floor.load(Relaxed) {
                    self.recompute_pending_abort_clean_floor();
                }
                let bucket_shard = Self::shard_of(task.bucket_id);
                let mut buckets = self.pending_abort_clean_buckets[bucket_shard].write();
                if let Some(cnt) = buckets.get_mut(&task.bucket_id) {
                    *cnt -= 1;
                    if *cnt == 0 {
                        buckets.remove(&task.bucket_id);
                    }
                }
                Some(task)
            } else {
                None
            }
        };
        if let Some(task) = old {
            self.groups[task.logical_group_id as usize].retire_aborted_fact(txid);
        }
    }

    pub(crate) fn abort_clean_tasks(&self) -> Vec<AbortCleanTask> {
        let mut tasks = Vec::new();
        for shard in &self.pending_abort_clean {
            tasks.extend(shard.read().values().copied());
        }
        tasks
    }

    pub(crate) fn mark_abort_clean_wait_quiesce(&self, txid: u64) {
        let shard = Self::shard_of(txid);
        if let Some(task) = self.pending_abort_clean[shard].write().get_mut(&txid)
            && task.state == AbortCleanState::Pending
        {
            task.state = AbortCleanState::WaitingQuiesce;
            task.quiesced = false;
        }
    }

    pub(crate) fn mark_abort_clean_quiesced(&self, txid: u64) {
        let shard = Self::shard_of(txid);
        if let Some(task) = self.pending_abort_clean[shard].write().get_mut(&txid) {
            task.quiesced = true;
        }
    }

    pub(crate) fn abort_clean_event_sink(&self) -> Arc<Mutex<Vec<u64>>> {
        self.abort_clean_events.clone()
    }

    pub(crate) fn drain_abort_clean_events(&self) -> Vec<u64> {
        std::mem::take(&mut *self.abort_clean_events.lock())
    }

    pub(crate) fn min_abort_clean_file_id(&self, physical_wal_id: u8) -> Option<u64> {
        let mut min_id = None;
        for shard in &self.pending_abort_clean {
            let candidate = shard
                .read()
                .values()
                .filter(|x| x.physical_wal_id == physical_wal_id)
                .map(|x| x.pin_file_id)
                .min();
            if let Some(candidate) = candidate {
                min_id = Some(min_id.map_or(candidate, |v: u64| v.min(candidate)));
            }
        }
        min_id
    }

    /// keep a durable checkpoint floor (and the wal analysis hint written from it)
    /// behind every pending abort-clean task of that physical stream
    ///
    /// the hint tells recovery where record analysis may start, and everything before it is
    /// assumed to be folded into durable pages. that is true for redo, which is gated by the
    /// bucket frontier, but not for transaction outcomes: an aborted transaction's only durable
    /// trace is its `Abort` record, so a hint past it would skip the abort, leave the aborted
    /// versions looking committed and drop the abort-clean task. a task's pin is the earliest
    /// record it still needs, so the floor may not pass it
    pub(crate) fn clamp_floor_to_abort_clean(
        &self,
        pos: Position,
        physical_wal_id: u8,
    ) -> Position {
        match self.min_abort_clean_file_id(physical_wal_id) {
            // whole-file granularity is enough: recovery scans from the hint file onwards
            Some(pin) => pos.min(Position::new(pin, 0)),
            None => pos,
        }
    }

    #[inline]
    pub(crate) fn has_pending_abort_clean_bucket(&self, bucket_id: u64) -> bool {
        let shard = Self::shard_of(bucket_id);
        self.pending_abort_clean_buckets[shard]
            .read()
            .contains_key(&bucket_id)
    }
}
