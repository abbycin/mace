use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::utils::observe::{CounterMetric, HistogramMetric, Observer};
use crate::utils::options::ParsedOptions;

pub(crate) struct FlowTask {
    enqueue_at: Instant,
    est_bytes: u64,
    actual_bytes: AtomicU64,
    flushed: AtomicBool,
}

impl FlowTask {
    fn new(est_bytes: u64) -> Self {
        Self {
            enqueue_at: Instant::now(),
            est_bytes,
            actual_bytes: AtomicU64::new(0),
            flushed: AtomicBool::new(false),
        }
    }

    pub(crate) fn mark_io_built(&self, bytes: u64) {
        self.actual_bytes.store(bytes, Relaxed);
        self.flushed.store(true, Relaxed);
    }
}

#[derive(Default)]
struct WriteAdmissionState {
    reserved_bytes: u64,
    checkpoint_progress_bytes: u64,
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
    task: Arc<FlowTask>,
}

impl CheckpointFlow {
    fn new(controller: Arc<FlowController>, task: Arc<FlowTask>) -> Self {
        Self { controller, task }
    }

    pub(crate) fn mark_io_built(&self, bytes: u64, elapsed: Duration) {
        self.controller
            .on_io_built(self.task.as_ref(), bytes, elapsed);
    }

    pub(crate) fn mark_progress(&self, bytes: u64) {
        self.controller.on_checkpoint_progress(bytes);
    }

    pub(crate) fn finish(self) {
        self.controller.on_checkpoint_finish(self.task.as_ref());
    }
}

pub(crate) struct FlowController {
    observer: Arc<dyn Observer>,
    backpressure_enabled: bool,
    flush_unit_bytes: u64,
    pool_capacity_bytes: u64,
    idle_reset: Duration,
    debt_bytes: AtomicU64,
    /// EWMA (Exponentially Weighted Moving Average)
    /// pure IO throughput (disk write time only)
    io_bps_ewma: AtomicU64,
    /// end-to-end throughput (enqueue to completion, includes scheduling/queuing)
    /// we track both to distinguish device bottleneck (io_bps low) from scheduling bottleneck (e2e low)
    e2e_bps_ewma: AtomicU64,
    io_sample_count: AtomicU64,
    idle_since: Mutex<Option<Instant>>,
    admission_state: Mutex<WriteAdmissionState>,
    admission_cv: Condvar,
}

impl FlowController {
    const EWMA_ALPHA_RISE_NUM: u64 = 20;
    const EWMA_ALPHA_FALL_NUM: u64 = 5;
    const EWMA_ALPHA_DEN: u64 = 100;
    const DEFAULT_IDLE_RESET_MS: u64 = 5_000;
    const ADMISSION_BURST_POOL_DIVISOR: u64 = 8;
    const ADMISSION_BURST_MIN_BYTES: u64 = 1 << 20;
    const ADMISSION_PROGRESS_EXTRA_BURST_CAP_MULTIPLIER: u64 = 3;
    const ADMISSION_WAKE_BURST_DIVISOR: u64 = 8;
    const ADMISSION_WAKE_MIN_BYTES: u64 = 1 << 20;

    pub(crate) fn new(opt: &ParsedOptions) -> Self {
        let flush_unit_bytes = (opt.checkpoint_size as u64).max(1);
        Self {
            observer: opt.observer.clone(),
            backpressure_enabled: opt.enable_backpressure,
            flush_unit_bytes,
            pool_capacity_bytes: (opt.pool_capacity as u64).max(flush_unit_bytes),
            idle_reset: Duration::from_millis(Self::DEFAULT_IDLE_RESET_MS),
            debt_bytes: AtomicU64::new(0),
            io_bps_ewma: AtomicU64::new(0),
            e2e_bps_ewma: AtomicU64::new(0),
            io_sample_count: AtomicU64::new(0),
            idle_since: Mutex::new(Some(Instant::now())),
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

    fn admission_progress_extra_burst(&self, progress_bytes: u64) -> u64 {
        let burst = self.admission_burst_bytes();
        progress_bytes
            .min(burst.saturating_mul(Self::ADMISSION_PROGRESS_EXTRA_BURST_CAP_MULTIPLIER))
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
        state.checkpoint_progress_bytes = state.checkpoint_progress_bytes.saturating_add(bytes);
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

    pub(crate) fn begin_checkpoint(self: &Arc<Self>, est_bytes: u64) -> CheckpointFlow {
        let mut state = self.admission_state.lock();
        state.checkpoint_progress_bytes = 0;
        state.wake_progress_bytes = 0;
        CheckpointFlow::new(self.clone(), self.on_enqueue_est(est_bytes))
    }

    fn on_checkpoint_finish(&self, task: &FlowTask) {
        self.on_mark_release(task);
        self.on_mark_done(task);
        let mut state = self.admission_state.lock();
        state.checkpoint_progress_bytes = 0;
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
        let burst = self.admission_burst_bytes();
        let reserved = write_bytes.min(limit).max(1);
        let mut wait_started: Option<Instant> = None;
        loop {
            let mut state = self.admission_state.lock();
            let (hot_bytes, dirty_bytes, checkpoint_inflight) = snapshot();
            let dirty = dirty_bytes as u64;
            let progress = state.checkpoint_progress_bytes;
            let effective_dirty = dirty.saturating_sub(progress);
            if ((hot_bytes as u64) >= self.flush_unit_bytes || dirty >= limit)
                && !checkpoint_inflight
            {
                drop(state);
                trigger_checkpoint();
                continue;
            }
            let admission_limit = if dirty >= limit {
                limit
                    .saturating_add(burst)
                    .saturating_add(self.admission_progress_extra_burst(progress))
            } else {
                limit
            };
            let projected = effective_dirty
                .saturating_add(state.reserved_bytes)
                .saturating_add(reserved);
            if projected <= admission_limit {
                state.reserved_bytes = state.reserved_bytes.saturating_add(reserved);
                drop(state);
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
            self.admission_cv.wait(&mut state);
            state.waiting_writers = state.waiting_writers.saturating_sub(1);
        }
    }

    fn on_enqueue_est(&self, est_bytes: u64) -> Arc<FlowTask> {
        self.on_maybe_idle_enqueue();
        self.debt_bytes.fetch_add(est_bytes, Relaxed);
        Arc::new(FlowTask::new(est_bytes))
    }

    fn on_mark_release(&self, task: &FlowTask) {
        let actual = task.actual_bytes.load(Relaxed);
        let settle = if actual != 0 { actual } else { task.est_bytes };
        let debt_after = self.settle_debt(settle);
        if debt_after == 0 {
            self.mark_idle();
        }
    }

    fn on_mark_done(&self, task: &FlowTask) {
        let actual = task.actual_bytes.load(Relaxed);
        let settle = if actual != 0 { actual } else { task.est_bytes };
        if !task.flushed.load(Relaxed) || settle == 0 {
            return;
        }

        let e2e_micros = task.enqueue_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
        if e2e_micros != 0 {
            let sample = ((settle as u128) * 1_000_000u128 / (e2e_micros as u128))
                .min(u64::MAX as u128) as u64;
            self.update_ewma(&self.e2e_bps_ewma, sample);
        }
    }

    fn on_io_built(&self, task: &FlowTask, bytes: u64, elapsed: Duration) {
        let est = task.est_bytes;
        if bytes > est {
            self.debt_bytes.fetch_add(bytes - est, Relaxed);
        } else if est > bytes {
            self.settle_debt(est - bytes);
        }
        let io_micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        if bytes != 0 && io_micros != 0 {
            let sample = ((bytes as u128) * 1_000_000u128 / (io_micros as u128))
                .min(u64::MAX as u128) as u64;
            self.update_ewma(&self.io_bps_ewma, sample);
            self.io_sample_count.fetch_add(1, Relaxed);
        }
        task.mark_io_built(bytes);
    }

    fn update_ewma(&self, slot: &AtomicU64, sample: u64) {
        if sample == 0 {
            return;
        }
        let old = slot.load(Relaxed);
        if old == 0 {
            slot.store(sample, Relaxed);
            return;
        }
        let alpha_num = if sample >= old {
            Self::EWMA_ALPHA_RISE_NUM
        } else {
            Self::EWMA_ALPHA_FALL_NUM
        };
        let keep = Self::EWMA_ALPHA_DEN.saturating_sub(alpha_num);
        let next = ((old as u128 * keep as u128) + (sample as u128 * alpha_num as u128))
            / Self::EWMA_ALPHA_DEN as u128;
        slot.store(next.min(u64::MAX as u128) as u64, Relaxed);
    }

    fn settle_debt(&self, settle: u64) -> u64 {
        let mut cur = self.debt_bytes.load(Relaxed);
        loop {
            let next = cur.saturating_sub(settle);
            match self
                .debt_bytes
                .compare_exchange_weak(cur, next, Relaxed, Relaxed)
            {
                Ok(_) => return next,
                Err(actual) => cur = actual,
            }
        }
    }

    fn on_maybe_idle_enqueue(&self) {
        let mut idle_since = self.idle_since.lock();
        if let Some(at) = *idle_since
            && at.elapsed() >= self.idle_reset
        {
            self.reset_idle_samples();
        }
        *idle_since = None;
    }

    fn mark_idle(&self) {
        let mut idle_since = self.idle_since.lock();
        if idle_since.is_none() {
            *idle_since = Some(Instant::now());
        }
    }

    fn reset_idle_samples(&self) {
        self.io_bps_ewma.store(0, Relaxed);
        self.e2e_bps_ewma.store(0, Relaxed);
        self.io_sample_count.store(0, Relaxed);
    }
}
