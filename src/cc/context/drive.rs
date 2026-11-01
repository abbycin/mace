use super::*;

impl Context {
    /// publish shared-stream floors before runtime checkpointing or GC
    pub(crate) fn publish_shared_logical_checkpoint_floors(&self, floors: &[Position]) {
        if let Some(shared) = &self.shared_logging {
            // recovery drains every reconstructed abort-clean task before this point, so the
            // pin is normally absent here; clamp anyway so a task that somehow outlived the
            // drain can never let recovery's own floors skip its records
            let pin = self.min_abort_clean_file_id(Options::SHARED_ID);
            let clamped: Vec<Position> = floors
                .iter()
                .enumerate()
                .map(|(slot, pos)| match pin {
                    Some(pin) => {
                        let pin_pos = Position::new(pin, 0);
                        if *pos > pin_pos {
                            log::warn!(
                                "abort-clean pin clamped shared floor[{:?}] from {:?} to {:?} (pin={:?})",
                                slot,
                                pos,
                                pin_pos,
                                pin
                            );
                        }
                        (*pos).min(pin_pos)
                    }
                    None => *pos,
                })
                .collect();
            shared.lock().set_logical_checkpoint_floors(&clamped);
        }
    }

    /// deactivate shared-stream floors after an era wipe
    pub(crate) fn reset_shared_logical_checkpoint_floors(&self) {
        if let Some(shared) = &self.shared_logging {
            shared.lock().reset_logical_checkpoint_floors();
        }
    }

    /// rebase writers after recovery truncation
    pub(crate) fn rebase_logging_positions_to_physical_eof(&self) {
        if let Some(shared) = &self.shared_logging {
            shared.lock().rebase_positions_to_physical_eof();
        } else {
            for group in self.groups.iter() {
                group.logging.lock().rebase_positions_to_physical_eof();
            }
        }
    }

    /// witness for the era handoff's order: the wipe removes the old era's files, so it may
    /// only run once control of them has left every logger
    #[cfg(feature = "extra_check")]
    pub(crate) fn assert_writers_detached(&self) {
        for logging in self.writer_loggings() {
            must_true!(
                logging.lock().writer.is_none(),
                "the era wipe must not run while a recovery writer is still open"
            );
        }
    }

    /// close every recovery writer before the era wipe.
    ///
    /// the precheck runs over every logger first: a relaxed route must not close
    /// some streams' files and then discover another logger is not detachable
    pub(crate) fn detach_recovery_writers(&self, epoch_start: u64) -> Result<(), OpCode> {
        // a foreground writer exists only after `init_safe_exclusive`, so the recovery
        // window is what makes "no append in flight" true rather than merely likely
        if !self.recovering() {
            log::error!("refusing to detach recovery writers outside the recovery window");
            return Err(OpCode::IoError);
        }
        let loggings = self.writer_loggings();
        for logging in loggings.iter() {
            logging.lock().check_detachable(epoch_start)?;
        }
        self.recovery_abort_only.store(true, Release);
        for logging in loggings.iter() {
            logging.lock().detach_writer(epoch_start)?;
        }
        Ok(())
    }

    /// publish the new era's writer into every logger of a same-width rebuild.
    ///
    /// the whole install must succeed before the context may serve the runtime, so
    /// a partial install leaves `recovery_abort_only` set for the caller's abort
    pub(crate) fn install_epoch_writers(&self, epoch_start: u64) -> Result<(), OpCode> {
        must_true!(
            self.recovery_abort_only(),
            "epoch writers install outside a rebuild's abort window"
        );
        for logging in self.writer_loggings() {
            logging.lock().install_epoch_writer(epoch_start)?;
        }
        self.recovery_abort_only.store(false, Release);
        Ok(())
    }

    /// recovery-open abort cleanup: close a partially built context without the
    /// runtime WAL barrier.
    ///
    /// the barrier assumes installed era writers and admitted foreground writers;
    /// a failed rebuild open can have detached loggers or a partially installed
    /// era, and it has no foreground write whose durability the barrier would
    /// establish
    pub(crate) fn abort_recovery_open(&self) {
        let _ = self.tx.send(CollectorSignal::Quit);
        if let Some(handle) = self.collector.lock().take() {
            let _ = handle.join();
        }
    }

    /// bounded merge window without holding the logging mutex
    fn merge_window(&self, ticket: &SyncTicket) {
        if !self.opt.sync_on_write {
            return;
        }
        let cap_us = self.opt.sync_merge_window_us;
        if cap_us == 0 {
            return;
        }
        let cap = Duration::from_micros(cap_us);
        let quiet = cap.min(Duration::from_micros(40));
        let started = Instant::now();
        while started.elapsed() < cap {
            if !ticket.has_followers() && started.elapsed() > quiet {
                break;
            }
            std::thread::sleep(Duration::from_micros(1));
        }
    }

    /// lock a logical group's WAL logger
    #[inline]
    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    pub(crate) fn lock_wal_logging(&self, gid: usize, seed: u64) -> MutexGuard<'_, Logging> {
        let logging = &self.groups[gid].logging;
        if self.opt.sync_on_write {
            let guard = logging.lock();
            #[cfg(feature = "metrics")]
            {
                let started = sampled_instant(seed, LATENCY_SAMPLE_SHIFT);
                observe_elapsed(
                    self.opt.observer.as_ref(),
                    HistogramMetric::WalLockWaitMicros,
                    started,
                );
            }
            guard
        } else {
            logging.lock()
        }
    }

    #[inline]
    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    pub(crate) fn lock_shared_logging(&self, seed: u64) -> MutexGuard<'_, Logging> {
        let shared = self
            .shared_logging()
            .expect("durable mode must construct the shared logging");
        let guard = shared.lock();
        #[cfg(feature = "metrics")]
        {
            let started = sampled_instant(seed, LATENCY_SAMPLE_SHIFT);
            observe_elapsed(
                self.opt.observer.as_ref(),
                HistogramMetric::WalLockWaitMicros,
                started,
            );
        }
        guard
    }

    /// complete a leader ticket or wait for a follower ticket
    pub(crate) fn drive_sync(&self, ticket: &SyncTicket) -> Result<(), OpCode> {
        if ticket.is_completed() {
            return Ok(());
        }
        #[cfg(feature = "metrics")]
        let wait_started = sampled_instant(
            ticket.target().file_id ^ ticket.target().offset,
            LATENCY_SAMPLE_SHIFT,
        );
        #[cfg(feature = "extra_check")]
        crate::testing::fire_wal_sync_point(if ticket.is_leader() {
            crate::testing::WalSyncPoint::AfterLeaderRegisterBeforeSeal
        } else {
            crate::testing::WalSyncPoint::AfterFollowerRegister
        });
        if ticket.is_leader() {
            self.merge_window(ticket);
            let mut log =
                self.lock_shared_logging(ticket.target().file_id ^ ticket.target().offset);
            log.leader_seal_sync_and_complete(ticket);
        } else {
            ticket.wait();
        }
        #[cfg(feature = "metrics")]
        observe_elapsed(
            self.opt.observer.as_ref(),
            HistogramMetric::WalGenerationWaitMicros,
            wait_started,
        );
        let result = ticket.check_result();
        #[cfg(feature = "extra_check")]
        if result.is_ok() {
            let log = self.lock_shared_logging(0);
            must_true!(
                log.durable_pos() >= ticket.target(),
                "generation ticket must be covered by durable_pos"
            );
            if let Some(cut) = ticket.sealed_cut() {
                must_true!(
                    log.durable_pos() >= cut,
                    "durable_pos must reach the sealed generation cut"
                );
            }
        }
        result
    }

    fn sync_runtime_logs(&self) -> Result<(), OpCode> {
        if self.opt.sync_on_write {
            let ticket = {
                let mut log = self.lock_shared_logging(0);
                log.barrier_register()?
            };
            self.drive_sync(&ticket)
        } else {
            for group in self.groups.iter() {
                let mut log = group.logging.lock();
                log.sync(true)?;
            }
            Ok(())
        }
    }

    pub fn sync(&self) -> Result<(), OpCode> {
        // same window as `quit`: no runtime sync exists before the new era is fully
        // installed
        must_true!(
            !self.recovery_abort_only(),
            "sync on a recovery-open context whose era writers are not installed"
        );
        self.sync_runtime_logs()
    }

    pub(crate) fn quit(&self) {
        // a rebuild open installs the full new era before it may serve the runtime;
        // reaching a barrier with detached or partially installed loggers means the
        // open published a context it never finished, and a barrier would either
        // abort on the missing writer or make a half-installed era look durable
        must_true!(
            !self.recovery_abort_only(),
            "quit on a recovery-open context whose era writers are not installed"
        );
        self.drain_inflight();
        must_ok!(self.sync_runtime_logs());
        must_ok!(self.tx.send(CollectorSignal::Quit));

        if let Some(h) = self.collector.lock().take() {
            let _ = h.join();
        }
    }
}
