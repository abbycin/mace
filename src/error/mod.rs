//! fail-stop invariant checking.
//!
//! `must_ok!`, `must_exist!` and `must_true!` (with `hot_true!` as the debug/`extra_check`
//! wrapper) report the failed invariant, its caller, the captured arguments and a backtrace
//! on stderr, flush every stream, and then abort the process.
//!
//! an invariant failure is not recoverable: unwinding would run destructors and hand control
//! back to code that assumes the invariant held, so the failure is fail-stop. the report is
//! emitted before the abort so an operator keeps the evidence, and the release profile
//! already aborts on panic (`panic = "abort"`), which this makes uniform across profiles.

use backtrace::Backtrace;
use std::fmt::Display;
use std::io::Write;
use std::panic::Location;

#[cold]
#[inline(never)]
#[track_caller]
pub fn must_ok_failed<E>(target: &str, message: &str, args: &[String], err: &E) -> !
where
    E: Display,
{
    abort_after_report("must_ok", target, message, args, Some(err))
}

#[cold]
#[inline(never)]
#[track_caller]
pub fn must_exist_failed(target: &str, message: &str, args: &[String]) -> ! {
    abort_after_report::<std::convert::Infallible>("must_exist", target, message, args, None)
}

#[cold]
#[inline(never)]
#[track_caller]
pub fn must_true_failed(target: &str, message: &str, args: &[String]) -> ! {
    abort_after_report::<std::convert::Infallible>("must_true", target, message, args, None)
}

#[cold]
#[inline(never)]
#[track_caller]
fn emit_failure<E>(kind: &str, target: &str, message: &str, args: &[String], err: Option<&E>)
where
    E: Display,
{
    let caller = Location::caller();
    eprintln!("fatal invariant failure");
    eprintln!("check={kind}");
    eprintln!("target={target}");
    eprintln!("caller={}:{}", caller.file(), caller.line());
    eprintln!("context={message}");
    for arg in args {
        eprintln!("arg={arg}");
    }
    if let Some(err) = err {
        eprintln!("error={err}");
    }
    // the process aborts instead of unwinding, so no panic hook runs: the report must
    // carry its own backtrace
    emit_backtrace(caller);
}

/// reports the failed invariant and then aborts the process
///
/// an invariant failure is not recoverable: unwinding would run user destructors and could
/// hand control back to code that assumes the invariant held. the report - the failed
/// check, its caller, the message, the captured arguments, the error and a backtrace - is
/// printed first, and the failure line names the check and its message, because no panic
/// hook runs on this path
#[cold]
#[inline(never)]
#[track_caller]
fn abort_after_report<E>(
    kind: &str,
    target: &str,
    message: &str,
    args: &[String],
    err: Option<&E>,
) -> !
where
    E: Display,
{
    emit_failure(kind, target, message, args, err);
    // the message must survive the abort even when the backtrace could not be symbolized,
    // so it is also the last line of the report
    eprintln!("fatal: {kind} failed: {message}");
    // stderr writes are unbuffered, but stdout is line buffered, and the process is about
    // to die without unwinding: flush both so the report and the program's own output
    // survive the abort
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    std::process::abort()
}

fn emit_backtrace(caller: &Location<'_>) {
    let bt = Backtrace::new();
    let mut frames = Vec::new();

    for frame in bt.frames() {
        for symbol in frame.symbols() {
            let Some(name) = symbol.name() else {
                continue;
            };
            let name = name.to_string();
            if frames.is_empty() && is_internal_backtrace_symbol(&name) {
                continue;
            }
            frames.push((
                name,
                symbol.filename().map(|x| x.display().to_string()),
                symbol.lineno(),
                symbol.colno(),
            ));
            break;
        }
    }

    if frames.is_empty() {
        return;
    }

    eprintln!("backtrace:");
    eprintln!("   0: {}", frames[0].0);
    eprintln!(
        "             at {}:{}:{}",
        caller.file(),
        caller.line(),
        caller.column()
    );
    for (idx, (name, file, line, col)) in frames.into_iter().enumerate().skip(1) {
        eprintln!("   {idx}: {name}");
        if let (Some(file), Some(line)) = (file, line) {
            if let Some(col) = col {
                eprintln!("             at {file}:{line}:{col}");
            } else {
                eprintln!("             at {file}:{line}");
            }
        }
    }
}

fn is_internal_backtrace_symbol(name: &str) -> bool {
    name.contains("::emit_backtrace")
        || name.contains("::emit_failure")
        || name.contains("::abort_after_report")
        || name.contains("::must_true_failed")
        || name.contains("::must_exist_failed")
        || name.contains("::must_ok_failed")
        || name.starts_with("backtrace::")
}

#[macro_export]
macro_rules! must_ok {
    ($expr:expr $(,)?) => {{
        match $expr {
            Ok(value) => value,
            Err(err) => $crate::error::must_ok_failed("statement", stringify!($expr), &[], &err),
        }
    }};
    ($expr:expr, $fmt:literal $(,)?) => {{
        match $expr {
            Ok(value) => value,
            Err(err) => $crate::error::must_ok_failed("statement", &format!($fmt), &[], &err),
        }
    }};
    ($expr:expr, $fmt:literal, $($arg:expr),+ $(,)?) => {{
        match $expr {
            Ok(value) => value,
            Err(err) => $crate::error::must_ok_failed(
                "statement",
                &format!($fmt, $($arg),+),
                &[$(format!("{}={:?}", stringify!($arg), $arg)),+],
                &err,
            ),
        }
    }};
}

#[macro_export]
macro_rules! must_exist {
    ($expr:expr $(,)?) => {{
        match $expr {
            Some(value) => value,
            None => $crate::error::must_exist_failed("statement", stringify!($expr), &[]),
        }
    }};
    ($expr:expr, $fmt:literal $(,)?) => {{
        match $expr {
            Some(value) => value,
            None => $crate::error::must_exist_failed("statement", &format!($fmt), &[]),
        }
    }};
    ($expr:expr, $fmt:literal, $($arg:expr),+ $(,)?) => {{
        match $expr {
            Some(value) => value,
            None => $crate::error::must_exist_failed(
                "statement",
                &format!($fmt, $($arg),+),
                &[$(format!("{}={:?}", stringify!($arg), $arg)),+],
            ),
        }
    }};
}

#[macro_export]
macro_rules! hot_true {
    ($($tt:tt)*) => {{
        #[cfg(any(debug_assertions, feature = "extra_check"))]
        {
            $crate::must_true!($($tt)*);
        }
    }};
}

#[macro_export]
macro_rules! must_true {
    (eq $left:expr, $right:expr $(,)?) => {{
        match (&$left, &$right) {
            (left_val, right_val) => {
                if *left_val != *right_val {
                    $crate::error::must_true_failed(
                        "statement",
                        "assert_eq failed",
                        &[
                            format!("left={:?}", left_val),
                            format!("right={:?}", right_val),
                        ],
                    );
                }
            }
        }
    }};
    (eq $left:expr, $right:expr, $fmt:literal $(,)?) => {{
        match (&$left, &$right) {
            (left_val, right_val) => {
                if *left_val != *right_val {
                    $crate::error::must_true_failed(
                        "statement",
                        &format!($fmt),
                        &[
                            format!("left={:?}", left_val),
                            format!("right={:?}", right_val),
                        ],
                    );
                }
            }
        }
    }};
    (eq $left:expr, $right:expr, $fmt:literal, $($arg:expr),+ $(,)?) => {{
        match (&$left, &$right) {
            (left_val, right_val) => {
                if *left_val != *right_val {
                    $crate::error::must_true_failed(
                        "statement",
                        &format!($fmt, $($arg),+),
                        &[
                            $(format!("{}={:?}", stringify!($arg), $arg)),+,
                            format!("left={:?}", left_val),
                            format!("right={:?}", right_val),
                        ],
                    );
                }
            }
        }
    }};
    (ne $left:expr, $right:expr $(,)?) => {{
        match (&$left, &$right) {
            (left_val, right_val) => {
                if *left_val == *right_val {
                    $crate::error::must_true_failed(
                        "statement",
                        "assert_ne failed",
                        &[
                            format!("left={:?}", left_val),
                            format!("right={:?}", right_val),
                        ],
                    );
                }
            }
        }
    }};
    (ne $left:expr, $right:expr, $fmt:literal $(,)?) => {{
        match (&$left, &$right) {
            (left_val, right_val) => {
                if *left_val == *right_val {
                    $crate::error::must_true_failed(
                        "statement",
                        &format!($fmt),
                        &[
                            format!("left={:?}", left_val),
                            format!("right={:?}", right_val),
                        ],
                    );
                }
            }
        }
    }};
    (ne $left:expr, $right:expr, $fmt:literal, $($arg:expr),+ $(,)?) => {{
        match (&$left, &$right) {
            (left_val, right_val) => {
                if *left_val == *right_val {
                    $crate::error::must_true_failed(
                        "statement",
                        &format!($fmt, $($arg),+),
                        &[
                            $(format!("{}={:?}", stringify!($arg), $arg)),+,
                            format!("left={:?}", left_val),
                            format!("right={:?}", right_val),
                        ],
                    );
                }
            }
        }
    }};
    ($expr:expr $(,)?) => {{
        match $expr {
            true => (),
            false => $crate::error::must_true_failed("statement", stringify!($expr), &[]),
        }
    }};
    ($expr:expr, $fmt:literal $(,)?) => {{
        match $expr {
            true => (),
            false => $crate::error::must_true_failed("statement", &format!($fmt), &[]),
        }
    }};
    ($expr:expr, $fmt:literal, $($arg:expr),+ $(,)?) => {{
        match $expr {
            true => (),
            false => $crate::error::must_true_failed(
                "statement",
                &format!($fmt, $($arg),+),
                &[$(format!("{}={:?}", stringify!($arg), $arg)),+],
            ),
        }
    }};
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::process::{Command, Output};

    /// set for the child process of an abort test
    pub(crate) const ENV_CHILD: &str = "MACE_ABORT_TEST_CHILD";

    /// whether this process is the child of an abort test
    pub(crate) fn is_abort_child() -> bool {
        std::env::var(ENV_CHILD).ok().as_deref() == Some("1")
    }

    /// runs one test of this binary in a child process and returns its output
    ///
    /// a fatal invariant failure aborts the process, so a test that pins that behavior
    /// cannot observe the outcome in-process. the abort also skips the child's `Drop`, so
    /// the child is given a temp root this parent process removes once the child has exited
    pub(crate) fn run_child_test(name: &str) -> Output {
        let child_root = crate::RandomPath::tmp();
        std::fs::create_dir_all(&*child_root).expect("child temp root must be created");
        let exe = std::env::current_exe().expect("current exe");
        let mut cmd = match cargo_runner() {
            Some(parts) => {
                let mut it = parts.into_iter();
                let mut cmd = Command::new(it.next().expect("runner must not be empty"));
                cmd.args(it);
                cmd.arg(exe);
                cmd
            }
            None => Command::new(exe),
        };
        cmd.arg("--exact")
            .arg(name)
            .arg("--nocapture")
            .env(ENV_CHILD, "1")
            .env(crate::RandomPath::TMP_ROOT_ENV, &*child_root)
            .output()
            .expect("child test must run")
    }

    /// the runner configured for a cross-compiled test target, if any
    fn cargo_runner() -> Option<Vec<String>> {
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
            if key.contains(&format!("_{arch}_")) {
                preferred.push((key.clone(), value.clone()));
            }
            all.push((key, value));
        }
        preferred.sort_by(|a, b| a.0.cmp(&b.0));
        all.sort_by(|a, b| a.0.cmp(&b.0));
        let chosen = preferred.into_iter().next().or_else(|| {
            if all.len() == 1 {
                all.into_iter().next()
            } else {
                None
            }
        })?;
        let parts: Vec<String> = chosen.1.split_whitespace().map(str::to_string).collect();
        (!parts.is_empty()).then_some(parts)
    }

    /// asserts that the child aborted and printed the expected report
    pub(crate) fn assert_aborted(output: &Output, expected: &[&str]) {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                output.status.signal(),
                Some(6),
                "the child must abort, got {:?}",
                output.status
            );
        }
        #[cfg(not(unix))]
        assert!(
            !output.status.success(),
            "the child must fail, got {:?}",
            output.status
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        for needle in expected {
            assert!(
                stderr.contains(needle),
                "the report must contain {needle:?} before the abort, got:\n{stderr}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{assert_aborted, is_abort_child, run_child_test};

    /// a fatal invariant failure prints the report and then aborts the process, so the
    /// report is what an operator sees and no destructor runs afterwards
    #[test]
    fn a_fatal_failure_aborts_the_process() {
        let output = run_child_test("error::tests::fatal_failure_child");
        assert_aborted(
            &output,
            &[
                "fatal invariant failure",
                "check=must_true",
                "context=child invariant",
                "backtrace:",
                // the failure line carries the message and is printed by the abort path
                "fatal: must_true failed: child invariant",
            ],
        );
    }

    /// the aborting side of the test above: it runs only in the child process
    #[test]
    fn fatal_failure_child() {
        if !is_abort_child() {
            return;
        }
        crate::must_true!(false, "child invariant");
    }
}
