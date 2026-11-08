//! foreground admission and the dirty memory ceiling
//!
//! the ceiling is `pool_capacity` plus one bounded drain burst: a writer may exceed it by
//! exactly that much to let a checkpoint make progress, and is then blocked until it
//! drops. Exceeding the ceiling throttles the foreground; it never discards work.
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

#[cfg(feature = "metrics")]
use crate::utils::observe::{CounterMetric, HistogramMetric, Observer};
use crate::utils::options::{BucketOptions, ParsedOptions};

#[derive(Default)]
struct WriteAdmissionState {
    reserved_bytes: u64,
    wake_progress_bytes: u64,
    waiting_writers: usize,
}

enum ProgressWake {
    None,
    One(usize),
    All,
}

#[must_use = "foreground write permit must be held until the write completes"]
pub(crate) struct ForegroundWritePermit {
    flow: Option<Arc<FlowController>>,
    reserved_bytes: u64,
}

impl ForegroundWritePermit {
    pub(crate) fn noop() -> Self {
        Self {
            flow: None,
            reserved_bytes: 0,
        }
    }

    fn new(flow: Arc<FlowController>, reserved_bytes: u64) -> Self {
        Self {
            flow: Some(flow),
            reserved_bytes,
        }
    }
}

impl Drop for ForegroundWritePermit {
    fn drop(&mut self) {
        if let Some(flow) = self.flow.take() {
            flow.release_foreground_reservation(self.reserved_bytes);
        }
    }
}

pub(crate) struct CheckpointFlow {
    controller: Arc<FlowController>,
}

impl CheckpointFlow {
    fn new(controller: Arc<FlowController>) -> Self {
        Self { controller }
    }

    pub(crate) fn mark_progress(&self, bytes: u64) {
        self.controller.on_checkpoint_progress(bytes);
    }

    pub(crate) fn finish(self) {
        self.controller.on_checkpoint_finish();
    }
}

pub(crate) struct FlowController {
    #[cfg(feature = "metrics")]
    observer: Arc<dyn Observer>,
    backpressure_enabled: bool,
    flush_unit_bytes: u64,
    pool_capacity_bytes: u64,
    admission_state: Mutex<WriteAdmissionState>,
    admission_cv: Condvar,
}

impl FlowController {
    const ADMISSION_WAIT_RECHECK_MS: u64 = 10;
    const ADMISSION_BURST_POOL_DIVISOR: u64 = 8;
    const ADMISSION_BURST_MIN_BYTES: u64 = 1 << 20;
    const ADMISSION_WAKE_BURST_DIVISOR: u64 = 8;
    const ADMISSION_WAKE_MIN_BYTES: u64 = 1 << 20;

    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    pub(crate) fn new(global: &ParsedOptions, opt: &BucketOptions) -> Self {
        let flush_unit_bytes = (opt.checkpoint_size as u64).max(1);
        Self {
            #[cfg(feature = "metrics")]
            observer: global.observer.clone(),
            backpressure_enabled: opt.enable_backpressure,
            flush_unit_bytes,
            pool_capacity_bytes: (opt.pool_capacity as u64).max(flush_unit_bytes),
            admission_state: Mutex::new(WriteAdmissionState::default()),
            admission_cv: Condvar::new(),
        }
    }

    fn admission_burst_bytes(&self) -> u64 {
        let limit = self.pool_capacity_bytes.max(1);
        let floor = (limit / 64).max(Self::ADMISSION_BURST_MIN_BYTES).min(limit);
        self.flush_unit_bytes
            .min(limit / Self::ADMISSION_BURST_POOL_DIVISOR)
            .max(floor)
            .min(limit)
    }

    /// bounded drain headroom granted to foreground writers while a
    /// checkpoint round is in flight and dirty is at the pool limit.
    /// this is a separate credit from [`Self::admission_burst_bytes`]: the
    /// wake threshold keeps a 1MiB floor for notification pacing, but the
    /// admission credit must stay proportional to the pool so small pools
    /// (tests, tiny buckets) never double their hard ceiling.
    /// credit = min(checkpoint_size, floor(pool_capacity / 8)), at least 1
    /// byte and never more than the pool; with defaults (pool 1GiB,
    /// checkpoint 256MiB) this is 128MiB, unrelated to the wake threshold.
    fn admission_credit_burst_bytes(&self) -> u64 {
        let limit = self.pool_capacity_bytes.max(1);
        self.flush_unit_bytes
            .min(limit / Self::ADMISSION_BURST_POOL_DIVISOR)
            .max(1)
            .min(limit)
    }

    fn admission_wake_bytes(&self) -> u64 {
        let burst = self.admission_burst_bytes().max(1);
        (burst / Self::ADMISSION_WAKE_BURST_DIVISOR)
            .max(Self::ADMISSION_WAKE_MIN_BYTES)
            .min(burst)
    }

    fn release_foreground_reservation(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut state = self.admission_state.lock();
        state.reserved_bytes = state.reserved_bytes.saturating_sub(bytes);
        let should_notify = state.waiting_writers != 0;
        drop(state);
        if should_notify {
            self.admission_cv.notify_one();
        }
    }

    fn on_checkpoint_progress(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        match self.admission_wake(bytes) {
            ProgressWake::None => {}
            ProgressWake::One(count) => {
                for _ in 0..count {
                    self.admission_cv.notify_one();
                }
            }
            ProgressWake::All => {
                self.admission_cv.notify_all();
            }
        }
    }

    fn admission_wake(&self, bytes: u64) -> ProgressWake {
        let mut state = self.admission_state.lock();
        if state.waiting_writers == 0 {
            return ProgressWake::None;
        }
        state.wake_progress_bytes = state.wake_progress_bytes.saturating_add(bytes);
        let wake_bytes = self.admission_wake_bytes();
        let wake_count = (state.wake_progress_bytes / wake_bytes) as usize;
        if wake_count == 0 {
            return ProgressWake::None;
        }
        state.wake_progress_bytes %= wake_bytes;
        if wake_count >= state.waiting_writers {
            state.wake_progress_bytes = 0;
            ProgressWake::All
        } else {
            ProgressWake::One(wake_count)
        }
    }

    pub(crate) fn begin_checkpoint(self: &Arc<Self>) -> CheckpointFlow {
        let mut state = self.admission_state.lock();
        state.wake_progress_bytes = 0;
        drop(state);
        CheckpointFlow::new(self.clone())
    }

    fn on_checkpoint_finish(&self) {
        let mut state = self.admission_state.lock();
        state.wake_progress_bytes = 0;
        drop(state);
        self.admission_cv.notify_all();
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.backpressure_enabled
    }

    pub(crate) fn noop(&self) -> ForegroundWritePermit {
        ForegroundWritePermit::noop()
    }

    pub(crate) fn acquire_foreground_permit<S, T>(
        self: &Arc<Self>,
        write_bytes: u64,
        snapshot: S,
        mut trigger_checkpoint: T,
    ) -> ForegroundWritePermit
    where
        S: Fn() -> (usize, usize, bool),
        T: FnMut(),
    {
        if !self.backpressure_enabled || write_bytes == 0 {
            return ForegroundWritePermit::noop();
        }

        let limit = self.pool_capacity_bytes.max(1);
        let burst = self.admission_credit_burst_bytes();
        let reserved = write_bytes.min(limit).max(1);
        let mut wait_started: Option<Instant> = None;
        loop {
            let mut state = self.admission_state.lock();
            let (hot_bytes, dirty_bytes, checkpoint_inflight) = snapshot();
            let dirty = dirty_bytes as u64;
            // NOTE: dirty (hot + remaining sealed) already excludes persisted
            // bytes: release_persisted_pages decrements sealed_bytes for every
            // page flushed, so a checkpoint round's drain is reflected directly
            // in dirty. subtracting progress again (the same persisted bytes
            // accumulated via mark_checkpoint_progress) would double-deduct and
            // let the hot generation grow unbounded while a round drains.
            let effective_dirty = dirty;
            if ((hot_bytes as u64) >= self.flush_unit_bytes || dirty >= limit)
                && !checkpoint_inflight
            {
                drop(state);
                trigger_checkpoint();
                continue;
            }
            // the durable gate on dirty (hot + remaining sealed) is
            // pool_capacity: dirty excludes persisted bytes, so checkpoint
            // progress is already reflected and must not be granted twice.
            // while a checkpoint round drains, admission caps projected dirty
            // (dirty + reservations + this write) at limit + credit_burst
            // (<= pool/8, <= flush_unit) so the foreground keeps making
            // progress instead of stalling until the drain finishes; actual
            // dirty can overshoot the projection between snapshots via per-op
            // estimate gaps and unadmitted eviction growth, and the next
            // snapshot re-bounds it. no progress-based credit: those bytes
            // are already gone from the dirty footprint above.
            let admission_limit = if dirty >= limit {
                limit.saturating_add(burst)
            } else {
                limit
            };
            let projected = effective_dirty
                .saturating_add(state.reserved_bytes)
                .saturating_add(reserved);
            if projected <= admission_limit {
                state.reserved_bytes = state.reserved_bytes.saturating_add(reserved);
                drop(state);
                #[cfg(feature = "metrics")]
                if let Some(started) = wait_started.take() {
                    self.observer.counter(CounterMetric::FlowFgAdmissionWait, 1);
                    self.observer.histogram(
                        HistogramMetric::FlowFgAdmissionWaitMicros,
                        started.elapsed().as_micros() as u64,
                    );
                }
                return ForegroundWritePermit::new(self.clone(), reserved);
            }
            if dirty_bytes != 0 && !checkpoint_inflight {
                drop(state);
                trigger_checkpoint();
                continue;
            }
            if wait_started.is_none() {
                wait_started = Some(Instant::now());
            }
            state.waiting_writers = state.waiting_writers.saturating_add(1);
            // periodically recheck because snapshot-backed predicates can change
            // outside admission_state and are not all paired with condvar signals
            let _ = self.admission_cv.wait_for(
                &mut state,
                Duration::from_millis(Self::ADMISSION_WAIT_RECHECK_MS),
            );
            state.waiting_writers = state.waiting_writers.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::options::{BucketOptions, Options};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// scripted (hot, dirty, inflight) state + trigger counter, shared with
    /// the admission thread through an Arc
    struct Script {
        hot: AtomicUsize,
        dirty: AtomicUsize,
        inflight: AtomicBool,
        trigger_calls: AtomicUsize,
    }

    impl Script {
        fn new(dirty: usize, inflight: bool) -> Arc<Self> {
            Arc::new(Self {
                hot: AtomicUsize::new(0),
                dirty: AtomicUsize::new(dirty),
                inflight: AtomicBool::new(inflight),
                trigger_calls: AtomicUsize::new(0),
            })
        }
        fn snapshot(&self) -> (usize, usize, bool) {
            (
                self.hot.load(Ordering::Relaxed),
                self.dirty.load(Ordering::Relaxed),
                self.inflight.load(Ordering::Relaxed),
            )
        }
    }

    fn test_flow(pool: usize, checkpoint: usize) -> Arc<FlowController> {
        use crate::utils::RandomPath;
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.sync_on_write = false;
        let global = opt.validate().expect("options validate");
        let bopt = BucketOptions {
            pool_capacity: pool,
            checkpoint_size: checkpoint,
            enable_backpressure: true,
            ..BucketOptions::default()
        };
        Arc::new(FlowController::new(&global, &bopt))
    }

    /// run admission on a helper thread; returns (permit_result, trigger_count)
    fn admit_in_thread(
        flow: &Arc<FlowController>,
        script: &Arc<Script>,
        write_bytes: u64,
    ) -> std::thread::JoinHandle<Result<ForegroundWritePermit, ()>> {
        let flow = flow.clone();
        let script = script.clone();
        std::thread::spawn(move || {
            let trigger = || {
                script.trigger_calls.fetch_add(1, Ordering::Relaxed);
            };
            let snap = || script.snapshot();
            let permit = flow.acquire_foreground_permit(write_bytes, snap, trigger);
            Ok(permit)
        })
    }

    fn wait_until(cond: impl Fn() -> bool, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if cond() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        cond()
    }

    #[test]
    fn admission_triggers_checkpoint_when_dirty_at_limit_and_not_inflight() {
        let flow = test_flow(8 << 20, 1 << 20);
        let script = Script::new(8 << 20, false); // dirty == limit, idle
        let handle = admit_in_thread(&flow, &script, 4096);
        // the very first loop iteration must trigger a checkpoint instead of
        // admitting: dirty >= limit && !inflight => trigger, then re-check
        let triggered = wait_until(
            || script.trigger_calls.load(Ordering::Relaxed) >= 1,
            Duration::from_secs(2),
        );
        assert!(
            triggered,
            "dirty>=limit && !inflight must trigger a checkpoint"
        );
        // with inflight still false and dirty still at limit the loop keeps
        // triggering; flip inflight to model the cut, then admission may grant
        // under the limit+burst ceiling
        script.inflight.store(true, Ordering::Relaxed);
        let admitted = wait_until(|| handle.is_finished(), Duration::from_secs(2));
        assert!(
            admitted,
            "admission must complete after inflight cut at dirty==limit"
        );
        let _permit = handle.join().expect("admission completes").expect("permit");
        // no assertion on reserved beyond sanity: write_bytes < limit so it
        // fits both ceilings
    }

    #[test]
    fn admission_denied_beyond_limit_plus_burst_while_inflight() {
        let flow = test_flow(8 << 20, 1 << 20);
        // burst = min(checkpoint 1MB, pool/8 = 1MB) = 1MB; ceiling = 9MB
        let script = Script::new((8 << 20) + (1 << 20), true); // dirty = limit+burst
        let handle = admit_in_thread(&flow, &script, 4096);
        // admission must NOT complete while dirty is pinned at limit+burst and
        // inflight: projected = dirty + reserved(4096) > ceiling => waits
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !handle.is_finished(),
            "admission must wait when projected exceeds limit+burst while inflight"
        );
        // drain completes: dirty drops below limit, inflight clears
        script.dirty.store(0, Ordering::Relaxed);
        script.inflight.store(false, Ordering::Relaxed);
        let _permit = handle.join().expect("admission completes").expect("permit");
    }

    #[test]
    fn admission_grants_within_burst_window_while_inflight() {
        let flow = test_flow(8 << 20, 1 << 20);
        // dirty sits inside the drain headroom: (limit, limit+burst]. a fixed
        // credit (limit+burst ceiling) admits here; a pure pool-limit gate
        // (the pre-burst regression shape) would block until drain completes
        let script = Script::new((8 << 20) + (1 << 20) - 4096, true);
        let handle = admit_in_thread(&flow, &script, 4096);
        let admitted = wait_until(|| handle.is_finished(), Duration::from_millis(500));
        assert!(
            admitted,
            "admission must grant inside the bounded drain burst while inflight"
        );
        let _permit = handle.join().expect("admission completes").expect("permit");
        assert_eq!(
            script.trigger_calls.load(Ordering::Relaxed),
            0,
            "inflight round must not trigger another checkpoint"
        );
    }

    #[test]
    fn admission_uses_pool_limit_when_dirty_below_limit() {
        let flow = test_flow(8 << 20, 1 << 20);
        let script = Script::new(4 << 20, false); // dirty < limit
        let handle = admit_in_thread(&flow, &script, 4096);
        let permit = handle
            .join()
            .expect("admission completes")
            .expect("permit without checkpoint");
        assert_eq!(
            script.trigger_calls.load(Ordering::Relaxed),
            0,
            "no checkpoint needed when dirty below limit"
        );
        assert!(permit.reserved_bytes > 0);
    }

    #[test]
    fn admission_grants_while_drain_releases_bytes_inflight() {
        let flow = test_flow(8 << 20, 1 << 20);
        // burst = 1MB, ceiling = 9MB; dirty pinned at 9MB denies a 4096B write
        let script = Script::new((8 << 20) + (1 << 20), true);
        let handle = admit_in_thread(&flow, &script, 4096);
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !handle.is_finished(),
            "pinned at the ceiling must deny while the round is inflight"
        );
        // the round drains half its sealed bytes but stays inflight: dirty
        // drops inside the headroom, so admission must grant before drain end
        script
            .dirty
            .store((8 << 20) + (512 << 10), Ordering::Relaxed);
        let admitted = wait_until(|| handle.is_finished(), Duration::from_secs(2));
        assert!(
            admitted,
            "admission must grant once drain releases bytes below the burst ceiling"
        );
        let _permit = handle.join().expect("admission completes").expect("permit");
        assert_eq!(
            script.trigger_calls.load(Ordering::Relaxed),
            0,
            "an inflight round must never trigger another checkpoint"
        );
    }

    #[test]
    fn admission_waiters_share_ceiling_while_inflight() {
        let flow = test_flow(8 << 20, 1 << 20);
        // two 3MB writers against dirty = 3MB, inflight: the first reservation
        // fits under the pool limit, the second projects past it only because
        // the first reservation is already charged, so it must wait
        let script = Script::new(3 << 20, true);
        let handle_a = admit_in_thread(&flow, &script, 3 << 20);
        let handle_b = admit_in_thread(&flow, &script, 3 << 20);
        let first = wait_until(
            || handle_a.is_finished() || handle_b.is_finished(),
            Duration::from_secs(2),
        );
        assert!(first, "at least one writer must fit under the pool limit");
        std::thread::sleep(Duration::from_millis(200));
        let (a_done, b_done) = (handle_a.is_finished(), handle_b.is_finished());
        assert!(
            a_done != b_done,
            "the second writer must wait on the first's reservation"
        );
        // drain until both reservations fit under the limit
        script.dirty.store(1 << 20, Ordering::Relaxed);
        let admitted = if a_done {
            wait_until(|| handle_b.is_finished(), Duration::from_secs(2))
        } else {
            wait_until(|| handle_a.is_finished(), Duration::from_secs(2))
        };
        assert!(
            admitted,
            "drain must release the waiter once both reservations fit"
        );
        let _first_permit = handle_a
            .join()
            .expect("admission completes")
            .expect("permit");
        let _second_permit = handle_b
            .join()
            .expect("admission completes")
            .expect("permit");
    }

    #[test]
    fn admission_credit_never_zero_for_tiny_pools() {
        // limit = max(pool, flush_unit) = 1 byte and limit/8 floors to zero:
        // the .max(1) floor keeps the ceiling honest, so a 1-byte reservation
        // at dirty == limit must fit exactly at limit + credit while inflight
        let flow = test_flow(1, 1);
        let script = Script::new(1, true); // dirty == limit, inflight
        let handle = admit_in_thread(&flow, &script, 1);
        let admitted = wait_until(|| handle.is_finished(), Duration::from_millis(500));
        assert!(
            admitted,
            "the 1-byte credit floor must admit at the tiny ceiling"
        );
        let _permit = handle.join().expect("admission completes").expect("permit");
        assert_eq!(
            script.trigger_calls.load(Ordering::Relaxed),
            0,
            "inflight round must not trigger another checkpoint"
        );
    }
}
