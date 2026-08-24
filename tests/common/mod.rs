#![allow(dead_code)]

use mace::{Mace, OpCode, Options, RandomPath};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

pub struct TestEnv {
    root: RandomPath,
}

impl Default for TestEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl TestEnv {
    pub fn new() -> Self {
        Self {
            root: RandomPath::tmp(),
        }
    }

    pub fn path(&self) -> &Path {
        self.root.as_path()
    }

    pub fn options(&self) -> Options {
        Options::new(&*self.root)
    }

    pub fn open_default(&self) -> Result<Mace, OpCode> {
        let opt = self.options();
        Mace::new(opt.validate()?)
    }

    pub fn open_with<F>(&self, tune: F) -> Result<Mace, OpCode>
    where
        F: FnOnce(&mut Options),
    {
        let mut opt = self.options();
        tune(&mut opt);
        Mace::new(opt.validate()?)
    }
}

pub fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

pub fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

pub fn is_retryable_txn_err(err: OpCode) -> bool {
    matches!(err, OpCode::AbortTx | OpCode::Again)
}

fn parse_runner(value: &str) -> Vec<String> {
    value
        .split_whitespace()
        .map(str::to_owned)
        .collect::<Vec<_>>()
}

fn detect_cargo_runner() -> Option<Vec<String>> {
    let arch = std::env::consts::ARCH
        .to_ascii_uppercase()
        .replace('-', "_");
    let mut preferred: Vec<(String, String)> = Vec::new();
    let mut all: Vec<(String, String)> = Vec::new();

    for (key, value) in std::env::vars() {
        if !key.starts_with("CARGO_TARGET_") || !key.ends_with("_RUNNER") {
            continue;
        }

        if value.trim().is_empty() {
            continue;
        }

        all.push((key.clone(), value.clone()));

        if key.contains(&format!("_{arch}_")) {
            preferred.push((key, value));
        }
    }

    preferred.sort_by(|a, b| a.0.cmp(&b.0));
    all.sort_by(|a, b| a.0.cmp(&b.0));

    if let Some((_, value)) = preferred.into_iter().next() {
        return Some(parse_runner(&value)).filter(|parts| !parts.is_empty());
    }

    if all.len() == 1 {
        return Some(parse_runner(&all[0].1)).filter(|parts| !parts.is_empty());
    }

    None
}

pub fn child_test_command(exe: &Path) -> Command {
    match detect_cargo_runner() {
        Some(parts) => {
            let mut it = parts.into_iter();
            let mut cmd = Command::new(it.next().expect("runner must not be empty"));
            cmd.args(it);
            cmd.arg(exe);
            cmd
        }
        None => Command::new(exe),
    }
}

/// arm the deterministic-test gc mode: under extra_check every round is
/// explicitly driven, so no background timer may fire (gc_timeout=0). other
/// builds keep the default timer — a bare zero degenerates into a busy poll
/// there and would run continuous background rounds (see registry
/// test.timeout_zero_disables_background_trigger).
pub fn deterministic_gc(options: &mut Options) {
    #[cfg(feature = "extra_check")]
    {
        options.gc_timeout = 0;
    }
    #[cfg(not(feature = "extra_check"))]
    {
        let _ = options;
    }
}

/// drive collector cycles until `cond` holds; each round wakes the collector
/// and consumes its cycle-completed signal. **caller must hold hooks_lock**
/// (the si/generation suite_lock aliases already provide it) so parallel
/// tests cannot steal the signal. no wall-clock sleep is involved.
#[cfg(feature = "extra_check")]
pub fn collector_rounds_until(
    bucket: &mace::Bucket,
    max_rounds: u32,
    mut cond: impl FnMut() -> bool,
) -> bool {
    use mace::testing;
    for _ in 0..max_rounds {
        if cond() {
            return true;
        }
        let (tx, rx) = std::sync::mpsc::channel();
        testing::set_collector_completed_hook(Some(std::sync::Arc::new(move || {
            let _ = tx.send(());
        })));
        testing::wake_cc_collector(bucket);
        let completed = rx.recv_timeout(Duration::from_secs(10)).is_ok();
        testing::clear_collector_completed_hook();
        if cond() {
            return true;
        }
        if !completed {
            // a cycle was requested but never finished: further rounds would
            // spin on a stuck collector, surface it via the caller's assert
            return cond();
        }
    }
    cond()
}
/// run one explicit gc round and verify engine-reported completion.
///
/// `start_gc` blocks until the collector finishes its full `run()` (the
/// semaphore posts after completion), so this returns only after a complete
/// engine round. under `extra_check` the round must additionally fire the
/// gc-completed signal, which this call consumes under `hooks_lock` so
/// parallel tests cannot steal or inject signals; a timeout panics with the
/// engine snapshot attached for attribution.
pub fn gc_round(mace: &Mace, timeout: Duration) {
    #[cfg(not(feature = "extra_check"))]
    let _ = timeout;
    #[cfg(feature = "extra_check")]
    let (tx, rx) = std::sync::mpsc::channel();
    #[cfg(feature = "extra_check")]
    let _hooks = mace_testing_lock();
    #[cfg(feature = "extra_check")]
    mace_testing_set_gc_completed(move || {
        // infallible: a stale fire after this call cleared the hook must not
        // panic an unrelated engine thread
        let _ = tx.send(());
    });

    mace.start_gc();

    #[cfg(feature = "extra_check")]
    {
        mace_testing_clear_gc_completed();
        drop(_hooks);
        if rx.recv_timeout(timeout).is_err() {
            panic!(
                "gc round did not report completion within {timeout:?}; {}",
                mace_testing_debug_snapshot(mace)
            );
        }
    }
}

/// drive up to `max_rounds` synchronous gc rounds until `cond` holds; no
/// wall-clock sleep is involved (each round is a full synchronous engine
/// point). returns the final `cond()` value so callers can assert with their
/// own diagnostics.
pub fn gc_rounds_until(mace: &Mace, max_rounds: u32, mut cond: impl FnMut() -> bool) -> bool {
    for _ in 0..max_rounds {
        // route through gc_round so the completion signal stays consumed under
        // hooks_lock; bare start_gc here would let parallel callers fire into
        // someone else's armed window and mask a missing own fire
        gc_round(mace, Duration::from_secs(30));
        if cond() {
            return true;
        }
    }
    cond()
}

/// `gc_rounds_until` with an inter-round settle delay: for preconditions that
/// depend on the engine's designed aging window (stat up2 must fall behind
/// the current tick before the decline-rate selector will pick a file), which
/// no number of back-to-back synchronous rounds can cross. still bounded:
/// fixed round budget, per-round synchronous engine point.
pub fn gc_rounds_until_with_settle(
    mace: &Mace,
    max_rounds: u32,
    settle: Duration,
    mut cond: impl FnMut() -> bool,
) -> bool {
    for _ in 0..max_rounds {
        mace.start_gc();
        if cond() {
            return true;
        }
        std::thread::sleep(settle);
    }
    mace.start_gc();
    cond()
}

#[cfg(feature = "extra_check")]
fn mace_testing_lock() -> parking_lot::MutexGuard<'static, ()> {
    mace::testing::hooks_lock()
}

#[cfg(feature = "extra_check")]
fn mace_testing_set_gc_completed(hook: impl Fn() + Send + Sync + 'static) {
    mace::testing::set_gc_completed_hook(Some(std::sync::Arc::new(hook)));
}

#[cfg(feature = "extra_check")]
fn mace_testing_clear_gc_completed() {
    mace::testing::clear_gc_completed_hook();
}

#[cfg(feature = "extra_check")]
fn mace_testing_debug_snapshot(mace: &Mace) -> String {
    mace::testing::debug_snapshot(mace)
}

/// engine snapshot text for assertion diagnostics; the rich form exists only
/// under extra_check (Observer trait is write-only, counters live test-side)
pub fn mace_snapshot_text(mace: &Mace) -> String {
    #[cfg(feature = "extra_check")]
    return mace::testing::debug_snapshot(mace);
    #[cfg(not(feature = "extra_check"))]
    {
        let _ = mace;
        String::from("(engine snapshot requires extra_check)")
    }
}
