use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

/// instance-level deletion suspension shared by checkpoints and every runtime unlink site
///
/// a checkpoint holds a token for the whole copy window. while at least one token is
/// alive, an unlink site skips its deletion phase entirely instead of deferring per
/// file, so no `*DeleteDone` record and no wal recycle phase advances. a site that
/// already started keeps its work slot until it finishes, which is what
/// [`DeletionGuard::wait_quiescent`] waits for.
#[derive(Default)]
pub(crate) struct DeletionGuard {
    state: Mutex<GuardState>,
    idle: Condvar,
}

#[derive(Default)]
struct GuardState {
    /// live suspension tokens
    holders: usize,
    /// deletion phases currently running
    in_flight: usize,
}

impl DeletionGuard {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GuardState::default()),
            idle: Condvar::new(),
        })
    }

    /// suspends deletions until the returned token is dropped
    pub(crate) fn suspend(self: &Arc<Self>) -> DeletionToken {
        self.state.lock().holders += 1;
        DeletionToken {
            guard: Arc::clone(self),
            #[cfg(feature = "metrics")]
            since: std::time::Instant::now(),
        }
    }

    /// claims one deletion phase; `None` means deletions are suspended and the
    /// caller must skip its whole phase without touching durable state
    pub(crate) fn begin(&self) -> Option<DeletionWork<'_>> {
        let mut state = self.state.lock();
        if state.holders > 0 {
            return None;
        }
        state.in_flight += 1;
        Some(DeletionWork { guard: self })
    }

    /// waits until no deletion phase is running; callers must hold a token, otherwise
    /// a new phase may start while this returns
    pub(crate) fn wait_quiescent(&self) {
        let mut state = self.state.lock();
        debug_assert!(
            state.holders > 0,
            "quiescence wait requires a suspension token"
        );
        while state.in_flight > 0 {
            self.idle.wait(&mut state);
        }
    }

    #[cfg(test)]
    pub(crate) fn holders(&self) -> usize {
        self.state.lock().holders
    }

    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        self.state.lock().in_flight
    }
}

/// RAII suspension token; dropping it lets the next deletion phase run again
pub(crate) struct DeletionToken {
    guard: Arc<DeletionGuard>,
    #[cfg(feature = "metrics")]
    since: std::time::Instant,
}

impl DeletionToken {
    /// how long deletions have been suspended, for the checkpoint's hold-time metric
    #[cfg(feature = "metrics")]
    pub(crate) fn held_micros(&self) -> u64 {
        self.since.elapsed().as_micros() as u64
    }
}

impl Drop for DeletionToken {
    fn drop(&mut self) {
        self.guard.state.lock().holders -= 1;
    }
}

/// RAII marker for one running deletion phase
pub(crate) struct DeletionWork<'a> {
    guard: &'a DeletionGuard,
}

impl Drop for DeletionWork<'_> {
    fn drop(&mut self) {
        let mut state = self.guard.state.lock();
        state.in_flight -= 1;
        if state.in_flight == 0 {
            self.guard.idle.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DeletionGuard;
    use std::sync::Arc;
    use std::sync::mpsc::channel;
    use std::time::Duration;

    #[test]
    fn suspension_blocks_new_work_and_release_restores_it() {
        let guard = DeletionGuard::new();
        let token = guard.suspend();
        assert!(guard.begin().is_none(), "suspended guard must refuse work");
        drop(token);
        let work = guard.begin().expect("released guard must admit work");
        assert_eq!(guard.in_flight(), 1);
        drop(work);
        assert_eq!(guard.in_flight(), 0);
    }

    #[test]
    fn quiescence_waits_for_running_work() {
        let guard = Arc::new(DeletionGuard::new());
        let work = guard.begin().expect("unsuspended guard must admit work");
        let (tx, rx) = channel();
        let waiter = {
            let guard = Arc::clone(&guard);
            let token = guard.suspend();
            std::thread::spawn(move || {
                guard.wait_quiescent();
                let _ = tx.send(());
                drop(token);
            })
        };

        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "quiescence must wait while a deletion phase is running"
        );
        drop(work);
        rx.recv_timeout(Duration::from_secs(5))
            .expect("quiescence must return once the running phase ends");
        waiter.join().expect("waiter must finish");
        assert_eq!(guard.holders(), 0);
    }
}
