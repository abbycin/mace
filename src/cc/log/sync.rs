use super::*;

/// completion published once by the generation leader under `SyncStateCore.inner`
pub(super) struct Completion {
    inner: UnsafeCell<CompletionInner>,
}

struct CompletionInner {
    result: Option<Result<(), OpCode>>,
    /// consumed by SyncTicket::sealed_cut under extra_check diagnostics
    #[cfg_attr(not(feature = "extra_check"), allow(dead_code))]
    cut: Position,
}

// safety: every access to `inner` happens under `SyncStateCore.inner` (write by
// the leader, read by waiters after the mutex is re-acquired on wake) or on the
// leader's own thread after publication; the Logging itself is already Sync
unsafe impl Sync for Completion {}

impl Completion {
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(CompletionInner {
                result: None,
                cut: Position::MIN,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SyncPhase {
    /// no generation in flight; the next register becomes a leader
    Open,
    /// a generation is registered; followers may join until the leader seals
    Syncing,
}

pub(super) struct SyncStateInner {
    pub(super) phase: SyncPhase,
    /// completion held by the leader and every participant
    pub(super) current: Option<Arc<Completion>>,
    /// followers that joined the in-flight generation
    followers: usize,
}

pub(super) struct SyncStateCore {
    pub(super) inner: Mutex<SyncStateInner>,
    cv: Condvar,
    /// lock-free mirror for merge-window polling
    has_follower: AtomicBool,
}

impl SyncStateCore {
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(SyncStateInner {
                phase: SyncPhase::Open,
                current: None,
                followers: 0,
            }),
            cv: Condvar::new(),
            has_follower: AtomicBool::new(false),
        }
    }
}

/// one participant in a sync generation
pub(crate) struct SyncTicket {
    is_leader: bool,
    target: Position,
    core: Option<Arc<SyncStateCore>>,
    completion: Option<Arc<Completion>>,
}

impl SyncTicket {
    fn completed(target: Position) -> Self {
        Self {
            is_leader: false,
            target,
            core: None,
            completion: None,
        }
    }

    /// whether registration found the target already durable
    #[inline]
    pub(crate) fn is_completed(&self) -> bool {
        self.completion.is_none()
    }

    #[inline]
    pub(crate) fn is_leader(&self) -> bool {
        self.is_leader
    }

    #[inline]
    pub(crate) fn target(&self) -> Position {
        self.target
    }

    /// sealed cut, available after completion
    #[cfg_attr(not(feature = "extra_check"), allow(dead_code))]
    pub(crate) fn sealed_cut(&self) -> Option<Position> {
        self.completion
            .as_ref()
            .map(|completion| unsafe { (*completion.inner.get()).cut })
    }

    /// whether a follower joined, read without taking the sync-state mutex
    pub(crate) fn has_followers(&self) -> bool {
        let Some(core) = &self.core else {
            return false;
        };
        core.has_follower.load(Relaxed)
    }

    /// wait without holding the shared logging mutex
    pub(crate) fn wait(&self) {
        let (Some(core), Some(completion)) = (&self.core, &self.completion) else {
            return;
        };
        let mut inner = core.inner.lock();
        // safety: the result is written under `core.inner` by the leader's
        // complete; the lock re-acquisition on wake is the happens-before edge
        while unsafe { (*completion.inner.get()).result.is_none() } {
            core.cv.wait(&mut inner);
        }
    }

    /// result after `wait`, or after leader completion
    pub(crate) fn check_result(&self) -> Result<(), OpCode> {
        match &self.completion {
            None => Ok(()),
            Some(completion) => {
                // safety: wait() synchronized with the leader's complete and the
                // leader observes its own publication; immutable after publish
                unsafe { (*completion.inner.get()).result }.unwrap_or(Err(OpCode::IoError))
            }
        }
    }
}

impl Drop for SyncTicket {
    fn drop(&mut self) {
        if self.is_leader {
            // unblock followers if the leader exits before publication
            let (Some(core), Some(completion)) = (&self.core, &self.completion) else {
                return;
            };
            let mut inner = core.inner.lock();
            let is_current = inner
                .current
                .as_ref()
                .is_some_and(|c| Arc::ptr_eq(c, completion));
            // safety: the write is under the sync state mutex; waiters
            // re-acquire it on wake, the leader's own thread is unwinding
            let unpublished = unsafe { (*completion.inner.get()).result.is_none() };
            if is_current && unpublished {
                unsafe {
                    *completion.inner.get() = CompletionInner {
                        result: Some(Err(OpCode::IoError)),
                        cut: Position::MIN,
                    };
                }
                inner.phase = SyncPhase::Open;
                inner.current = None;
                inner.followers = 0;
                core.has_follower.store(false, Relaxed);
                drop(inner);
                core.cv.notify_all();
            }
        }
    }
}

impl Logging {
    pub fn sync_checkpoint_barrier(&mut self) -> Result<(), OpCode> {
        // durable barriers use the generation protocol
        assert!(
            !self.opt.sync_on_write,
            "durable route must sync through the generation protocol"
        );
        if self.durable_pos >= self.log_pos && !self.log_dir_dirty {
            return Ok(());
        }

        self.flush_ring_to_writer();

        #[cfg(feature = "metrics")]
        let sync_started = sampled_instant(
            self.log_pos.file_id ^ self.log_pos.offset,
            LATENCY_SAMPLE_SHIFT,
        );
        self.sync_writer_data_and_dir();
        self.durable_pos = self.flushed_pos;
        #[cfg(feature = "metrics")]
        {
            self.opt.observer.counter(CounterMetric::WalSync, 1);
            observe_elapsed(
                self.opt.observer.as_ref(),
                HistogramMetric::WalSyncMicros,
                sync_started,
            );
        }
        Ok(())
    }

    #[inline]
    fn sync_writer_data_and_dir(&mut self) {
        must_exist!(self.writer.as_mut(), "wal writer").sync_data();
        must_ok!(self.sync_log_dir_if_dirty());
    }

    fn sync_log_dir_if_dirty(&mut self) -> Result<(), OpCode> {
        if self.log_dir_dirty {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_after_file_sync_before_dir_sync");
            self.opt
                .fs
                .as_ref()
                .sync_dir(&self.opt.log_root())
                .map_err(OpCode::from)?;
            self.log_dir_dirty = false;
        }
        Ok(())
    }

    /// sync rotated writers, current writer, then a newly-created directory entry
    fn try_sync_generation_files(&mut self) -> Result<(), OpCode> {
        #[cfg(feature = "extra_check")]
        {
            let prev = self
                .sync_fault
                .try_update(Relaxed, Relaxed, |n| (n > 0).then(|| n - 1));
            if let Ok(failing) = prev
                && failing > 0
            {
                return Err(OpCode::IoError);
            }
        }
        for w in self.pending_writers.iter_mut() {
            w.try_sync()?;
        }
        self.pending_writers.clear();
        must_exist!(self.writer.as_mut(), "wal writer").try_sync()?;
        self.sync_log_dir_if_dirty()?;
        Ok(())
    }

    #[cfg(feature = "extra_check")]
    pub(crate) fn fail_next_syncs(&self, n: usize) {
        self.sync_fault.store(n, Relaxed);
    }

    /// register a sync target into the shared-stream generation protocol; the
    /// caller must hold the logging mutex. durable-route only.
    pub(crate) fn register_sync(&mut self, target: Position) -> Result<SyncTicket, OpCode> {
        debug_assert!(
            self.opt.sync_on_write,
            "generation sync is a durable-route mechanism"
        );
        let mut inner = self.sync_state.inner.lock();
        if self.durable_pos >= target {
            // completed-ticket short-circuit: the cut covering target is already
            // durable; no generation, no leader, no wait, no sync count
            return Ok(SyncTicket::completed(target));
        }
        match inner.phase {
            SyncPhase::Syncing => {
                // the leader has registered but not sealed yet: seal..complete
                // hold the logging mutex we hold now, so joining here is always
                // pre-seal and the leader's cut will cover this target
                let completion = inner
                    .current
                    .clone()
                    .expect("syncing phase must hold a completion");
                inner.followers += 1;
                self.sync_state.has_follower.store(true, Relaxed);
                Ok(SyncTicket {
                    is_leader: false,
                    target,
                    core: Some(self.sync_state.clone()),
                    completion: Some(completion),
                })
            }
            SyncPhase::Open => {
                inner.phase = SyncPhase::Syncing;
                let completion = Arc::new(Completion::new());
                inner.current = Some(completion.clone());
                Ok(SyncTicket {
                    is_leader: true,
                    target,
                    core: Some(self.sync_state.clone()),
                    completion: Some(completion),
                })
            }
        }
    }

    /// force-barrier registration: flush the ring, then register the current
    /// stream position; the caller must hold the logging mutex.
    /// durable-route only.
    pub(crate) fn barrier_register(&mut self) -> Result<SyncTicket, OpCode> {
        debug_assert!(
            self.opt.sync_on_write,
            "generation barrier is a durable-route mechanism"
        );
        self.flush_ring_to_writer();
        let target = self.current_pos();
        self.register_sync(target)
    }

    /// generation leader: seal the flushed cut, sync every writer detached by
    /// rotation plus the current writer and the log directory, advance
    /// durable_pos to the sealed cut on success, then publish the completion
    /// and wake every waiter. the caller must hold the logging mutex, and this
    /// is the only place a generation's completion is published so no register
    /// can observe a sealed-but-uncompleted generation.
    pub(crate) fn leader_seal_sync_and_complete(&mut self, ticket: &SyncTicket) {
        debug_assert!(ticket.is_leader);
        self.flush_ring_to_writer();
        let cut = self.flushed_pos;
        {
            let inner = self.sync_state.inner.lock();
            debug_assert_eq!(inner.phase, SyncPhase::Syncing);
            // the lock discipline guarantees inner.current is this leader's
            // completion here: only complete clears it, only the leader
            // completes, and the orphan guard runs only after the leader
            // ticket drops
        }
        #[cfg(feature = "extra_check")]
        crate::testing::fire_wal_sync_point(crate::testing::WalSyncPoint::AfterSealBeforeFileSync);
        // crash window: the sealed cut is flushed but the generation files are
        // not yet synced; recovery must reinterpret the flushed tail
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_wal_generation_before_file_sync");
        // crash window: a torn tail record (garbage appended after the last
        // valid record by the test child); recovery must truncate at the first
        // malformed record start and discard everything after it
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_wal_tail_corrupt");

        #[cfg(feature = "metrics")]
        let sync_started = sampled_instant(cut.file_id ^ cut.offset, LATENCY_SAMPLE_SHIFT);
        let result = self.try_sync_generation_files();
        #[cfg(feature = "metrics")]
        {
            observe_elapsed(
                self.opt.observer.as_ref(),
                HistogramMetric::WalSyncMicros,
                sync_started,
            );
            self.opt.observer.counter(CounterMetric::WalSync, 1);
        }
        if result.is_ok() {
            self.durable_pos = cut;
        }
        // crash window: the generation files are synced and durable_pos reached
        // the sealed cut, but the completion is not yet published, so no
        // participant published a fact; recovery must reconstruct the outcomes
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_wal_generation_after_file_sync_before_complete");

        let mut inner = self.sync_state.inner.lock();
        #[cfg(feature = "metrics")]
        let followers = inner.followers;
        // the lock discipline makes this unconditional publish sound: the
        // leader holds the logging mutex, phase == Syncing means inner.current
        // is the completion this leader installed at register (only complete
        // clears it, and only the leader completes; the orphan guard runs only
        // after the leader ticket drops, so a completing leader's completion
        // is always current). per-generation isolation comes from each
        // generation installing its own completion Arc at register, not from
        // any generation counter
        if let Some(completion) = inner.current.as_ref() {
            // safety: the completion result is guarded by the sync state mutex
            unsafe {
                *completion.inner.get() = CompletionInner {
                    result: Some(result),
                    cut,
                };
            }
        }
        inner.phase = SyncPhase::Open;
        inner.current = None;
        inner.followers = 0;
        self.sync_state.has_follower.store(false, Relaxed);
        drop(inner);

        #[cfg(feature = "metrics")]
        {
            self.opt.observer.counter(CounterMetric::WalGeneration, 1);
            self.opt
                .observer
                .counter(CounterMetric::WalGenerationLeader, 1);
            if followers > 0 {
                self.opt
                    .observer
                    .counter(CounterMetric::WalGenerationFollower, followers as u64);
            }
            self.opt
                .observer
                .histogram(HistogramMetric::WalGenerationBatch, (followers + 1) as u64);
            if result.is_err() {
                self.opt
                    .observer
                    .counter(CounterMetric::WalGenerationError, 1);
            }
        }
        self.sync_state.cv.notify_all();
        #[cfg(feature = "extra_check")]
        crate::testing::fire_wal_sync_point(crate::testing::WalSyncPoint::AfterGenerationComplete);
    }

    #[inline]
    pub(super) fn flush_ring_to_writer(&mut self) {
        let len = self.ring.distance();
        if len != 0 {
            // a non-empty ring means committed bytes are queued for this file
            let writer = must_exist!(self.writer.as_mut(), "wal writer");
            let head = self.ring.head();
            writer.write(self.ring.slice(head, len));
            self.ring.cons(len);

            self.flushed_pos = self.log_pos;
        }
    }

    pub fn sync(&mut self, force: bool) -> Result<(), OpCode> {
        // durable route must sync through the generation protocol; a direct
        // sync here would bypass B-SYNC-4's flush-only non-generation rule
        assert!(
            !self.opt.sync_on_write,
            "durable route must sync through the generation protocol"
        );
        self.flush_ring_to_writer();
        if force {
            #[cfg(feature = "metrics")]
            let sync_started = sampled_instant(
                self.log_pos.file_id ^ self.log_pos.offset,
                LATENCY_SAMPLE_SHIFT,
            );
            self.sync_writer_data_and_dir();
            self.durable_pos = self.flushed_pos;
            #[cfg(feature = "metrics")]
            {
                self.opt.observer.counter(CounterMetric::WalSync, 1);
                observe_elapsed(
                    self.opt.observer.as_ref(),
                    HistogramMetric::WalSyncMicros,
                    sync_started,
                );
            }
        }
        Ok(())
    }
}
