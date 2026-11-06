//! dependency-free diagnostics: timestamp, os thread id, level, file:line.
//!
//! there is no registration step and no global logger object. the level filter is one
//! atomic, applied by `Options::validate`, and the sink is stderr, so a report cannot be
//! lost because the embedder forgot to install something. a disabled level costs one
//! atomic load and never formats its arguments.
//!
//! the fatal invariant path in `crate::error` does not route through here: that path
//! aborts the process and must keep working when this module itself is the broken thing,
//! so it writes its own report and only borrows [`timestamp`] and [`thread_tag`].

use std::cell::Cell;
use std::io::Write;
#[cfg(target_os = "windows")]
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU8;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum DiagLevel {
    Off = 0,
    Error = 1,
    #[default]
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl DiagLevel {
    fn label(self) -> &'static str {
        match self {
            DiagLevel::Off => "OFF",
            DiagLevel::Error => "ERROR",
            DiagLevel::Warn => "WARN",
            DiagLevel::Info => "INFO",
            DiagLevel::Debug => "DEBUG",
            DiagLevel::Trace => "TRACE",
        }
    }
}

// warn by default: the engine's error paths are genuine anomalies, so they reach stderr
// without any configuration, while info/trace stay free until an operator asks for them
static MAX: AtomicU8 = AtomicU8::new(DiagLevel::Warn as u8);

/// installs the filter an open requested, replacing any previous one
pub(crate) fn set_diag_level(level: DiagLevel) {
    MAX.store(level as u8, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub(crate) fn enabled(level: DiagLevel) -> bool {
    level as u8 <= MAX.load(std::sync::atomic::Ordering::Relaxed)
}

/// os thread id, cached per thread
///
/// this is a process-global constant for a thread, not per-instance state: every `Mace`
/// instance in the process observes the same value, so the cache carries no isolation
/// requirement. the syscall is not free and a per-record call would be paid on the hot
/// levels, hence the cache.
fn tid() -> i64 {
    thread_local! {
        static TID: Cell<i64> = const { Cell::new(0) };
    }
    TID.with(|cell| {
        let cached = cell.get();
        if cached != 0 {
            return cached;
        }
        let fresh = raw_tid();
        cell.set(fresh);
        fresh
    })
}

fn raw_tid() -> i64 {
    #[cfg(target_os = "linux")]
    {
        i64::from(unsafe { libc::gettid() })
    }
    #[cfg(target_os = "macos")]
    {
        let mut id = 0_u64;
        let result = unsafe { libc::pthread_threadid_np(libc::pthread_self(), &mut id) };
        debug_assert_eq!(result, 0);
        id as i64
    }
    #[cfg(target_os = "freebsd")]
    {
        i64::from(unsafe { libc::pthread_getthreadid_np() })
    }
    #[cfg(target_os = "windows")]
    {
        static NEXT_TID: AtomicI64 = AtomicI64::new(1);
        NEXT_TID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

/// `tid/name` when the thread was named, bare `tid` otherwise
///
/// the engine names its own threads, so a log line points at the subsystem that produced
/// it; unnamed pool threads still get the id
pub(crate) fn thread_tag() -> String {
    match std::thread::current().name() {
        Some(name) => format!("{}/{}", tid(), name),
        None => tid().to_string(),
    }
}

#[cold]
#[inline(never)]
pub(crate) fn emit(level: DiagLevel, file: &str, line: u32, args: std::fmt::Arguments<'_>) {
    let record = format_record(
        level,
        file,
        line,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default(),
        thread_tag(),
        &args.to_string(),
    );
    // one writer at a time: stderr is unbuffered but a terminal write is not atomic, and a
    // torn line is worse than a slightly later one
    static WRITE: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    let _guard = WRITE.lock();
    let _ = std::io::stderr().write_all(record.as_bytes());
}

/// `YYYY-MM-DD HH:MM:SS.ffffff tid/name [LEVEL] file:line message`
///
/// utc, so reports from different hosts order without a timezone database and without
/// the `chrono` dependency a local-time formatter would need
fn format_record(
    level: DiagLevel,
    file: &str,
    line: u32,
    now: Duration,
    thread: String,
    message: &str,
) -> String {
    format!(
        "{} {thread} [{}] {file}:{line} {message}\n",
        timestamp(now),
        level.label()
    )
}

pub(crate) fn timestamp(now: Duration) -> String {
    let (year, month, day) = civil((now.as_secs() / 86_400) as i64);
    let sod = now.as_secs() % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{:06}",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60,
        now.subsec_micros()
    )
}

/// days since the unix epoch to a civil date, howard hinnant's `civil_from_days`
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe as i64 + era * 400;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// builds the report line and writes it, without evaluating any argument when disabled
macro_rules! record {
    ($level:expr, $($arg:tt)*) => {{
        if $crate::utils::diag::enabled($level) {
            $crate::utils::diag::emit($level, file!(), line!(), format_args!($($arg)*));
        }
    }};
}

macro_rules! error {
    ($($arg:tt)*) => { $crate::utils::diag::record!($crate::utils::diag::DiagLevel::Error, $($arg)*) };
}

// `warn` cannot be the macro's own identifier: an unqualified use of it is ambiguous
// with the builtin attribute, so the macro is defined as `warn_` and re-exported
macro_rules! warn_ {
    ($($arg:tt)*) => { $crate::utils::diag::record!($crate::utils::diag::DiagLevel::Warn, $($arg)*) };
}

macro_rules! info {
    ($($arg:tt)*) => { $crate::utils::diag::record!($crate::utils::diag::DiagLevel::Info, $($arg)*) };
}

macro_rules! trace {
    ($($arg:tt)*) => { $crate::utils::diag::record!($crate::utils::diag::DiagLevel::Trace, $($arg)*) };
}

pub(crate) use warn_ as warn;
pub(crate) use {error, info, record, trace};

#[cfg(test)]
mod tests {
    use super::*;

    /// the four fields an operator needs from a background thread must survive without a
    /// logging facade: timestamp, thread, file:line and level
    #[test]
    fn a_record_carries_timestamp_thread_location_and_level() {
        let now = Duration::from_secs(1_777_003_723) + Duration::from_micros(123_456);
        let line = format_record(
            DiagLevel::Error,
            "src/store/gc.rs",
            71,
            now,
            "4123/garbage-collector".to_owned(),
            "wal gap detected after recycled prefix",
        );
        assert_eq!(
            line,
            "2026-04-24 04:08:43.123456 4123/garbage-collector [ERROR] src/store/gc.rs:71 \
             wal gap detected after recycled prefix\n"
        );
    }

    /// an unnamed thread still identifies itself
    #[test]
    fn an_unnamed_thread_reports_a_bare_id() {
        let line = format_record(
            DiagLevel::Warn,
            "src/meta/manifest/files.rs",
            192,
            Duration::ZERO,
            "77".to_owned(),
            "obsolete stat remove failed",
        );
        assert_eq!(
            line,
            "1970-01-01 00:00:00.000000 77 [WARN] src/meta/manifest/files.rs:192 \
             obsolete stat remove failed\n"
        );
    }

    /// dates on both sides of the leap-year boundary, where a naive day counter breaks
    #[test]
    fn civil_dates_handle_leap_years_and_epoch() {
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(59), (1970, 3, 1));
        assert_eq!(civil(11_016), (2000, 2, 29));
        assert_eq!(civil(19_782), (2024, 2, 29));
        assert_eq!(civil(-1), (1969, 12, 31));
    }

    /// the filter compares by severity, and off silences everything
    #[test]
    fn the_filter_is_ordered_by_severity() {
        set_diag_level(DiagLevel::Warn);
        assert!(enabled(DiagLevel::Error));
        assert!(enabled(DiagLevel::Warn));
        assert!(!enabled(DiagLevel::Info));
        assert!(!enabled(DiagLevel::Trace));

        set_diag_level(DiagLevel::Off);
        assert!(!enabled(DiagLevel::Error));

        set_diag_level(DiagLevel::Trace);
        assert!(enabled(DiagLevel::Trace));
        set_diag_level(DiagLevel::Warn);
    }

    /// the thread cache must return the same value to every caller on a thread
    #[test]
    fn a_thread_id_is_stable_across_calls() {
        let first = tid();
        assert_ne!(first, 0);
        assert_eq!(first, tid());
    }

    /// the option default is warn: unconfigured opens still report anomalies
    #[test]
    fn the_default_level_is_warn() {
        assert_eq!(DiagLevel::default(), DiagLevel::Warn);
    }
}
