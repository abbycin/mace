use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use crate::utils::observe::{CounterMetric, HistogramMetric, Observer};
use crate::utils::options::ParsedOptions;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum FlowOutcome {
    Unknown = 0,
    Flushed = 1,
    Skip = 2,
    Empty = 3,
    MetaOnly = 4,
}

impl FlowOutcome {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Flushed,
            2 => Self::Skip,
            3 => Self::Empty,
            4 => Self::MetaOnly,
            _ => Self::Unknown,
        }
    }
}

pub(crate) struct FlowTask {
    enqueue_at: Instant,
    est_bytes: u64,
    actual_bytes: AtomicU64,
    io_micros: AtomicU64,
    outcome: AtomicU8,
    force_bypass: AtomicBool,
}

impl FlowTask {
    fn new(est_bytes: u64) -> Self {
        Self {
            enqueue_at: Instant::now(),
            est_bytes,
            actual_bytes: AtomicU64::new(0),
            io_micros: AtomicU64::new(0),
            outcome: AtomicU8::new(FlowOutcome::Unknown as u8),
            force_bypass: AtomicBool::new(false),
        }
    }

    pub(crate) fn mark_outcome(&self, outcome: FlowOutcome) {
        self.outcome.store(outcome as u8, Relaxed);
    }

    pub(crate) fn mark_io_built(&self, bytes: u64, elapsed: Duration) {
        self.actual_bytes.store(bytes, Relaxed);
        self.io_micros
            .store(elapsed.as_micros().min(u64::MAX as u128) as u64, Relaxed);
        self.mark_outcome(FlowOutcome::Flushed);
    }

    pub(crate) fn mark_force_bypass(&self) {
        self.force_bypass.store(true, Relaxed);
    }

    fn force_bypass(&self) -> bool {
        self.force_bypass.load(Relaxed)
    }
}

pub(crate) struct FlowController {
    observer: Arc<dyn Observer>,
    backpressure_enabled: bool,
    flush_pacing_enabled: bool,
    soft_debt_units: u64,
    hard_debt_units: u64,
    stop_debt_units: u64,
    cold_start_fail_safe_units: u64,
    pacing_soft_debt_units: u64,
    flush_unit_bytes: u64,
    min_ratio_pct: u64,
    floor_bps: u64,
    warmup_min_samples: u64,
    publish_guard_min_ratio_pct: u64,
    idle_reset: Duration,
    max_delay_us: u64,
    debt_bytes: AtomicU64,
    /// EWMA (Exponentially Weighted Moving Average)
    /// pure IO throughput (disk write time only)
    io_bps_ewma: AtomicU64,
    /// end-to-end throughput (enqueue to completion, includes scheduling/queuing)
    /// we track both to distinguish device bottleneck (io_bps low) from scheduling bottleneck (e2e low)
    e2e_bps_ewma: AtomicU64,
    io_sample_count: AtomicU64,
    adaptive_floor_bps: AtomicU64,
    teardown_bypass_scopes: AtomicU64,
    arena_starving_scopes: AtomicU64,
    idle_since: Mutex<Option<Instant>>,
    next_flush_at: Mutex<Option<Instant>>,
}

impl FlowController {
    const EWMA_ALPHA_NUM: u64 = 20;
    const EWMA_ALPHA_DEN: u64 = 100;

    pub(crate) fn new(opt: &ParsedOptions) -> Self {
        Self {
            observer: opt.observer.clone(),
            backpressure_enabled: opt.enable_backpressure,
            flush_pacing_enabled: opt.enable_flush_pacing,
            soft_debt_units: opt.bp_soft_debt_units as u64,
            hard_debt_units: opt.bp_hard_debt_units as u64,
            stop_debt_units: opt.bp_stop_debt_units as u64,
            cold_start_fail_safe_units: opt.bp_cold_start_fail_safe_units as u64,
            pacing_soft_debt_units: opt.bp_pacing_soft_debt_units as u64,
            flush_unit_bytes: opt.bp_flush_unit_bytes.max(1),
            min_ratio_pct: opt.bp_min_ratio_pct as u64,
            floor_bps: opt.bp_floor_bps,
            warmup_min_samples: opt.bp_warmup_min_samples as u64,
            publish_guard_min_ratio_pct: opt.bp_publish_guard_min_ratio_pct as u64,
            idle_reset: Duration::from_millis(opt.bp_idle_reset_ms),
            max_delay_us: opt.bp_max_delay_us,
            debt_bytes: AtomicU64::new(0),
            io_bps_ewma: AtomicU64::new(0),
            e2e_bps_ewma: AtomicU64::new(0),
            io_sample_count: AtomicU64::new(0),
            adaptive_floor_bps: AtomicU64::new(opt.bp_floor_bps),
            teardown_bypass_scopes: AtomicU64::new(0),
            arena_starving_scopes: AtomicU64::new(0),
            idle_since: Mutex::new(Some(Instant::now())),
            next_flush_at: Mutex::new(None),
        }
    }

    pub(crate) fn on_enqueue_est(&self, est_bytes: u64) -> Arc<FlowTask> {
        self.on_maybe_idle_enqueue();
        self.debt_bytes.fetch_add(est_bytes, Relaxed);
        Arc::new(FlowTask::new(est_bytes))
    }

    pub(crate) fn on_mark_done(&self, task: &FlowTask) {
        let outcome = FlowOutcome::from_u8(task.outcome.load(Relaxed));
        let actual = task.actual_bytes.load(Relaxed);
        let settle = if actual != 0 { actual } else { task.est_bytes };

        let debt_after = self.settle_debt(settle);
        if debt_after == 0 {
            self.mark_idle();
        }

        if outcome != FlowOutcome::Flushed || settle == 0 {
            return;
        }

        let e2e_micros = task.enqueue_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
        if e2e_micros != 0 {
            let sample = ((settle as u128) * 1_000_000u128 / (e2e_micros as u128))
                .min(u64::MAX as u128) as u64;
            self.update_ewma(&self.e2e_bps_ewma, sample);
        }
    }

    pub(crate) fn on_io_built(&self, task: &FlowTask, bytes: u64, elapsed: Duration) {
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
        task.mark_io_built(bytes, elapsed);
    }

    pub(crate) fn before_foreground_write(&self, write_bytes: u64) {
        if !self.backpressure_enabled || write_bytes == 0 {
            return;
        }
        let debt = self.debt_bytes.load(Relaxed);
        if debt == 0 {
            return;
        }

        let debt_units = self.debt_milli_units(debt);
        let soft = self.soft_debt_milli_units();
        let hard = self.hard_debt_milli_units();
        let stop = self.stop_debt_milli_units();
        let cold_fail_safe = self.cold_start_fail_safe_milli_units();
        let cold_start = !self.cold_start_ready();

        if cold_start && debt_units < cold_fail_safe {
            return;
        }
        if !cold_start && debt_units <= soft {
            return;
        }

        let base_units = if cold_start { cold_fail_safe } else { soft };
        let base_bps = if cold_start {
            self.effective_floor_bps()
        } else {
            self.disk_bps()
        };
        if base_bps == 0 {
            return;
        }

        let delay_us = if debt_units >= stop {
            self.max_delay_us
        } else {
            let ratio_pct =
                ((base_units as u128) * 100u128 / (debt_units as u128)).min(100u128) as u64;
            let ratio_pct = ratio_pct.max(self.min_ratio_pct).min(100);
            let mut target_bps = base_bps
                .saturating_mul(ratio_pct)
                .saturating_div(100)
                .max(1);
            if !cold_start {
                target_bps = target_bps
                    .saturating_mul(self.publish_guard_ratio_pct())
                    .saturating_div(100)
                    .max(1);
            }
            let mut delay = ((write_bytes as u128) * 1_000_000u128 / (target_bps as u128))
                .min(u64::MAX as u128) as u64;
            if debt_units >= hard {
                delay = delay.saturating_mul(2);
            }
            delay.min(self.max_delay_us)
        };

        if delay_us != 0 {
            std::thread::sleep(Duration::from_micros(delay_us));
            self.observer.counter(CounterMetric::FlowFgDelay, 1);
            self.observer
                .histogram(HistogramMetric::FlowFgDelayMicros, delay_us);
        }
    }

    pub(crate) fn before_flush(&self, task: &FlowTask) {
        if !self.flush_pacing_enabled {
            return;
        }
        if task.force_bypass() || self.teardown_bypass_scopes.load(Relaxed) != 0 {
            self.observer
                .counter(CounterMetric::FlowFlushPacingBypassTeardown, 1);
            return;
        }
        if self.arena_starving_scopes.load(Relaxed) != 0 {
            self.observer
                .counter(CounterMetric::FlowFlushPacingBypassArenaStarvation, 1);
            return;
        }
        let debt = self.debt_bytes.load(Relaxed);
        if debt == 0 {
            return;
        }
        if !self.cold_start_ready() {
            return;
        }
        let debt_units = self.debt_milli_units(debt);
        if debt_units > self.pacing_soft_debt_milli_units() {
            self.observer
                .counter(CounterMetric::FlowFlushPacingBypassHighDebt, 1);
            return;
        }
        let disk_bps = self.disk_bps();
        if disk_bps == 0 {
            return;
        }
        let bytes = task.est_bytes.max(1);
        let interval_us = ((bytes as u128) * 1_000_000u128 / (disk_bps as u128))
            .min(self.max_delay_us as u128)
            .max(1) as u64;
        let mut next = self
            .next_flush_at
            .lock()
            .expect("flow pacing mutex poisoned");
        let now = Instant::now();
        if let Some(deadline) = *next
            && deadline > now
        {
            let mut sleep = deadline.duration_since(now);
            let max_sleep = Duration::from_micros(self.max_delay_us);
            if sleep > max_sleep {
                sleep = max_sleep;
            }
            if !sleep.is_zero() {
                std::thread::sleep(sleep);
                let slept_us = sleep.as_micros().min(u64::MAX as u128) as u64;
                self.observer
                    .counter(CounterMetric::FlowFlushPacingSleep, 1);
                self.observer
                    .histogram(HistogramMetric::FlowFlushPacingSleepMicros, slept_us);
            }
        }
        let now = Instant::now();
        let base = next.map_or(now, |d| d.max(now));
        *next = Some(base + Duration::from_micros(interval_us));
    }

    pub(crate) fn enter_teardown_bypass(&self) {
        self.teardown_bypass_scopes.fetch_add(1, Relaxed);
    }

    pub(crate) fn leave_teardown_bypass(&self) {
        self.dec_scope(&self.teardown_bypass_scopes);
    }

    pub(crate) fn enter_arena_starving(&self) {
        self.arena_starving_scopes.fetch_add(1, Relaxed);
    }

    pub(crate) fn leave_arena_starving(&self) {
        self.dec_scope(&self.arena_starving_scopes);
    }

    fn cold_start_ready(&self) -> bool {
        self.io_sample_count.load(Relaxed) >= self.warmup_min_samples
    }

    fn debt_milli_units(&self, debt_bytes: u64) -> u64 {
        if debt_bytes == 0 {
            return 0;
        }
        (((debt_bytes as u128) * 1_000u128) / (self.flush_unit_bytes as u128))
            .max(1)
            .min(u64::MAX as u128) as u64
    }

    fn soft_debt_milli_units(&self) -> u64 {
        self.soft_debt_units.saturating_mul(1_000)
    }

    fn hard_debt_milli_units(&self) -> u64 {
        self.hard_debt_units.saturating_mul(1_000)
    }

    fn stop_debt_milli_units(&self) -> u64 {
        self.stop_debt_units.saturating_mul(1_000)
    }

    fn cold_start_fail_safe_milli_units(&self) -> u64 {
        self.cold_start_fail_safe_units.saturating_mul(1_000)
    }

    fn pacing_soft_debt_milli_units(&self) -> u64 {
        self.pacing_soft_debt_units.saturating_mul(1_000)
    }

    fn disk_bps(&self) -> u64 {
        if !self.cold_start_ready() {
            return 0;
        }
        self.io_bps_ewma.load(Relaxed).max(1)
    }

    fn publish_guard_ratio_pct(&self) -> u64 {
        let io = self.io_bps_ewma.load(Relaxed);
        let e2e = self.e2e_bps_ewma.load(Relaxed);
        if io == 0 || e2e == 0 || e2e >= io {
            return 100;
        }
        (((e2e as u128) * 100u128) / (io as u128))
            .min(100u128)
            .max(self.publish_guard_min_ratio_pct as u128) as u64
    }

    fn update_ewma(&self, slot: &AtomicU64, sample: u64) {
        if sample == 0 {
            return;
        }
        let old = slot.load(Relaxed);
        if old == 0 {
            slot.store(sample, Relaxed);
            self.raise_adaptive_floor(sample);
            return;
        }
        let keep = Self::EWMA_ALPHA_DEN.saturating_sub(Self::EWMA_ALPHA_NUM);
        let next = ((old as u128 * keep as u128) + (sample as u128 * Self::EWMA_ALPHA_NUM as u128))
            / Self::EWMA_ALPHA_DEN as u128;
        slot.store(next.min(u64::MAX as u128) as u64, Relaxed);
        self.raise_adaptive_floor(sample);
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
        let mut idle_since = self.idle_since.lock().expect("flow idle mutex poisoned");
        if let Some(at) = *idle_since
            && at.elapsed() >= self.idle_reset
        {
            self.reset_idle_samples();
        }
        *idle_since = None;
    }

    fn mark_idle(&self) {
        let mut idle_since = self.idle_since.lock().expect("flow idle mutex poisoned");
        if idle_since.is_none() {
            *idle_since = Some(Instant::now());
        }
        let mut next = self
            .next_flush_at
            .lock()
            .expect("flow pacing mutex poisoned");
        *next = None;
    }

    fn reset_idle_samples(&self) {
        self.io_bps_ewma.store(0, Relaxed);
        self.e2e_bps_ewma.store(0, Relaxed);
        self.io_sample_count.store(0, Relaxed);
        self.adaptive_floor_bps.store(self.floor_bps, Relaxed);
    }

    fn effective_floor_bps(&self) -> u64 {
        self.floor_bps.max(self.adaptive_floor_bps.load(Relaxed))
    }

    fn raise_adaptive_floor(&self, sample_bps: u64) {
        let candidate = sample_bps.saturating_div(8).max(self.floor_bps);
        self.adaptive_floor_bps
            .fetch_update(Relaxed, Relaxed, |x| Some(x.max(candidate)))
            .ok();
    }

    fn dec_scope(&self, scope: &AtomicU64) {
        let mut cur = scope.load(Relaxed);
        loop {
            if cur == 0 {
                return;
            }
            match scope.compare_exchange_weak(cur, cur - 1, Relaxed, Relaxed) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    use crate::utils::options::Options;

    use super::*;

    fn make_flow(configure: impl FnOnce(&mut Options)) -> FlowController {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "mace_flow_test_{}_{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Relaxed)
        ));
        let mut opt = Options::new(root.clone());
        opt.enable_backpressure = true;
        opt.enable_flush_pacing = true;
        configure(&mut opt);
        let parsed = opt.validate().expect("options validate must succeed");
        let flow = FlowController::new(&parsed);
        let _ = std::fs::remove_dir_all(root);
        flow
    }

    fn prime_io_sample(flow: &FlowController, bytes: u64, micros: u64) {
        let t = flow.on_enqueue_est(bytes);
        flow.on_io_built(t.as_ref(), bytes, Duration::from_micros(micros));
        flow.on_mark_done(t.as_ref());
    }

    #[test]
    fn foreground_backpressure_skips_cold_start_before_fail_safe_backlog() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
            opt.bp_flush_unit_bytes = 1_000;
            opt.bp_soft_debt_units = 2;
            opt.bp_hard_debt_units = 6;
            opt.bp_stop_debt_units = 12;
            opt.bp_cold_start_fail_safe_units = 4;
            opt.bp_max_delay_us = 20_000;
        });

        let _t = flow.on_enqueue_est(3_000);

        let start = Instant::now();
        flow.before_foreground_write(1);
        assert!(
            start.elapsed() < Duration::from_millis(5),
            "cold start should collect io samples before throttling below fail-safe backlog"
        );
    }

    #[test]
    fn foreground_backpressure_sleeps_when_steady_state_debt_exceeds_stop() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
            opt.bp_flush_unit_bytes = 1_000;
            opt.bp_soft_debt_units = 1;
            opt.bp_hard_debt_units = 2;
            opt.bp_stop_debt_units = 3;
            opt.bp_warmup_min_samples = 1;
            opt.bp_publish_guard_min_ratio_pct = 100;
            opt.bp_max_delay_us = 20_000;
        });

        prime_io_sample(&flow, 1_000, 1_000);
        let _t = flow.on_enqueue_est(10_000);

        let start = Instant::now();
        flow.before_foreground_write(1);
        assert!(
            start.elapsed() >= Duration::from_micros(15_000),
            "steady-state backpressure should apply visible sleep under stop debt"
        );
    }

    #[test]
    fn flush_pacing_sleeps_under_low_debt() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
            opt.bp_flush_unit_bytes = 1_000_000;
            opt.bp_soft_debt_units = 1;
            opt.bp_pacing_soft_debt_units = 1;
            opt.bp_hard_debt_units = 2;
            opt.bp_stop_debt_units = 3;
            opt.bp_warmup_min_samples = 1;
            opt.bp_max_delay_us = 50_000;
        });

        prime_io_sample(&flow, 10_000, 10_000);
        let t1 = flow.on_enqueue_est(10_000);
        let t2 = flow.on_enqueue_est(10_000);

        flow.before_flush(t1.as_ref());
        let start = Instant::now();
        flow.before_flush(t2.as_ref());
        assert!(
            start.elapsed() >= Duration::from_micros(5_000),
            "second low-debt flush should be paced"
        );
    }

    #[test]
    fn flush_pacing_bypasses_under_high_debt() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
            opt.bp_flush_unit_bytes = 10_000;
            opt.bp_soft_debt_units = 1;
            opt.bp_pacing_soft_debt_units = 1;
            opt.bp_hard_debt_units = 2;
            opt.bp_stop_debt_units = 3;
            opt.bp_warmup_min_samples = 1;
            opt.bp_max_delay_us = 50_000;
        });

        prime_io_sample(&flow, 10_000, 10_000);
        let t = flow.on_enqueue_est(20_000);
        flow.before_flush(t.as_ref());
        let next = flow
            .next_flush_at
            .lock()
            .expect("flow pacing mutex poisoned")
            .to_owned();
        assert!(
            next.is_none(),
            "high debt should bypass pacing schedule setup"
        );
    }

    #[test]
    fn flush_pacing_bypasses_on_teardown_flag() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
            opt.bp_flush_unit_bytes = 1_000_000;
            opt.bp_soft_debt_units = 10;
            opt.bp_pacing_soft_debt_units = 1;
            opt.bp_hard_debt_units = 20;
            opt.bp_stop_debt_units = 40;
            opt.bp_warmup_min_samples = 1;
            opt.bp_max_delay_us = 50_000;
        });

        prime_io_sample(&flow, 10_000, 10_000);
        let t = flow.on_enqueue_est(10_000);
        t.mark_force_bypass();
        flow.before_flush(t.as_ref());
        let next = flow
            .next_flush_at
            .lock()
            .expect("flow pacing mutex poisoned")
            .to_owned();
        assert!(
            next.is_none(),
            "teardown flag should bypass pacing schedule setup"
        );
    }

    #[test]
    fn idle_reset_clears_stale_samples() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
            opt.bp_flush_unit_bytes = 1_000_000;
            opt.bp_soft_debt_units = 10;
            opt.bp_hard_debt_units = 20;
            opt.bp_stop_debt_units = 40;
            opt.bp_idle_reset_ms = 1;
        });

        let t = flow.on_enqueue_est(20_000);
        flow.on_io_built(t.as_ref(), 20_000, Duration::from_millis(1));
        flow.on_mark_done(t.as_ref());
        assert!(
            flow.io_bps_ewma.load(Relaxed) > 0 || flow.e2e_bps_ewma.load(Relaxed) > 0,
            "flush sample should update throughput ewma"
        );

        std::thread::sleep(Duration::from_millis(5));
        let _next = flow.on_enqueue_est(1);

        assert_eq!(
            flow.io_bps_ewma.load(Relaxed),
            0,
            "idle reset should clear io ewma"
        );
        assert_eq!(
            flow.e2e_bps_ewma.load(Relaxed),
            0,
            "idle reset should clear e2e ewma"
        );
        assert_eq!(
            flow.io_sample_count.load(Relaxed),
            0,
            "idle reset should clear io sample count"
        );
        assert_eq!(
            flow.adaptive_floor_bps.load(Relaxed),
            flow.floor_bps,
            "idle reset should restore adaptive floor to configured floor"
        );
    }

    #[test]
    fn debt_reconciles_when_actual_exceeds_estimate() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
        });

        let t = flow.on_enqueue_est(64);
        assert_eq!(flow.debt_bytes.load(Relaxed), 64);

        flow.on_io_built(t.as_ref(), 96, Duration::from_micros(10));
        assert_eq!(flow.debt_bytes.load(Relaxed), 96);

        flow.on_mark_done(t.as_ref());
        assert_eq!(flow.debt_bytes.load(Relaxed), 0);
    }

    #[test]
    fn debt_reconciles_when_actual_below_estimate() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
        });

        let t = flow.on_enqueue_est(128);
        assert_eq!(flow.debt_bytes.load(Relaxed), 128);

        flow.on_io_built(t.as_ref(), 32, Duration::from_micros(10));
        assert_eq!(flow.debt_bytes.load(Relaxed), 32);

        flow.on_mark_done(t.as_ref());
        assert_eq!(flow.debt_bytes.load(Relaxed), 0);
    }

    #[test]
    fn metadata_only_mark_done_settles_once_without_ewma_sample() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
        });

        let t = flow.on_enqueue_est(128);
        assert_eq!(flow.debt_bytes.load(Relaxed), 128);

        t.mark_outcome(FlowOutcome::MetaOnly);
        flow.on_mark_done(t.as_ref());

        assert_eq!(flow.debt_bytes.load(Relaxed), 0);
        assert_eq!(flow.io_bps_ewma.load(Relaxed), 0);
        assert_eq!(flow.e2e_bps_ewma.load(Relaxed), 0);
    }

    #[test]
    fn teardown_scope_bypasses_pacing_for_normal_tasks() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
            opt.bp_flush_unit_bytes = 1_000_000;
            opt.bp_soft_debt_units = 10;
            opt.bp_pacing_soft_debt_units = 1;
            opt.bp_hard_debt_units = 12;
            opt.bp_stop_debt_units = 15;
            opt.bp_warmup_min_samples = 1;
            opt.bp_max_delay_us = 50_000;
        });

        prime_io_sample(&flow, 10_000, 10_000);
        let t = flow.on_enqueue_est(10_000);
        flow.enter_teardown_bypass();
        flow.before_flush(t.as_ref());
        flow.leave_teardown_bypass();

        let next = flow
            .next_flush_at
            .lock()
            .expect("flow pacing mutex poisoned")
            .to_owned();
        assert!(
            next.is_none(),
            "global teardown scope should bypass pacing for normal tasks"
        );
    }

    #[test]
    fn arena_starving_scope_is_reference_counted() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
            opt.bp_flush_unit_bytes = 1_000_000;
            opt.bp_soft_debt_units = 10;
            opt.bp_pacing_soft_debt_units = 1;
            opt.bp_hard_debt_units = 12;
            opt.bp_stop_debt_units = 15;
            opt.bp_warmup_min_samples = 1;
            opt.bp_max_delay_us = 50_000;
        });

        prime_io_sample(&flow, 10_000, 10_000);
        let t = flow.on_enqueue_est(10_000);
        flow.enter_arena_starving();
        flow.enter_arena_starving();
        flow.leave_arena_starving();
        flow.before_flush(t.as_ref());

        let next = flow
            .next_flush_at
            .lock()
            .expect("flow pacing mutex poisoned")
            .to_owned();
        assert!(
            next.is_none(),
            "remaining starving scope should keep pacing bypass enabled"
        );

        flow.leave_arena_starving();
        flow.before_flush(t.as_ref());
        let next_after_clear = flow
            .next_flush_at
            .lock()
            .expect("flow pacing mutex poisoned")
            .to_owned();
        assert!(
            next_after_clear.is_some(),
            "pacing should resume when starving scopes are all cleared"
        );
    }

    #[test]
    fn flush_pacing_uses_dedicated_threshold_instead_of_foreground_soft_limit() {
        let flow = make_flow(|opt| {
            opt.bp_floor_bps = 1_000_000;
            opt.bp_flush_unit_bytes = 10_000;
            opt.bp_soft_debt_units = 8;
            opt.bp_pacing_soft_debt_units = 1;
            opt.bp_hard_debt_units = 24;
            opt.bp_stop_debt_units = 48;
            opt.bp_warmup_min_samples = 1;
            opt.bp_max_delay_us = 50_000;
        });

        prime_io_sample(&flow, 10_000, 10_000);
        let t = flow.on_enqueue_est(20_000);
        flow.before_flush(t.as_ref());

        let next = flow
            .next_flush_at
            .lock()
            .expect("flow pacing mutex poisoned")
            .to_owned();
        assert!(
            next.is_none(),
            "pacing should bypass once backlog exceeds pacing threshold even if foreground soft limit is larger"
        );
    }
}
