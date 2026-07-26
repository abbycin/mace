use backtrace::Backtrace;
use std::fmt::Display;
use std::panic::Location;

#[cold]
#[inline(never)]
#[track_caller]
pub fn must_ok_failed<E>(target: &str, message: &str, args: &[String], err: &E) -> !
where
    E: Display,
{
    emit_failure("must_ok", target, message, args, Some(err));
    panic!("must_ok failed: {message}")
}

#[cold]
#[inline(never)]
#[track_caller]
pub fn must_exist_failed(target: &str, message: &str, args: &[String]) -> ! {
    emit_failure::<std::convert::Infallible>("must_exist", target, message, args, None);
    panic!("must_exist failed: {message}")
}

#[cold]
#[inline(never)]
#[track_caller]
pub fn must_true_failed(target: &str, message: &str, args: &[String]) -> ! {
    emit_failure::<std::convert::Infallible>("must_true", target, message, args, None);
    panic!("must_true failed: {message}")
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
    if !panic_hook_prints_backtrace() {
        emit_backtrace(caller);
    }
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
        || name.contains("::must_true_failed")
        || name.contains("::must_exist_failed")
        || name.contains("::must_ok_failed")
        || name.starts_with("backtrace::")
}

fn panic_hook_prints_backtrace() -> bool {
    backtrace_env_enabled(std::env::var("RUST_BACKTRACE").ok().as_deref())
}

fn backtrace_env_enabled(val: Option<&str>) -> bool {
    match val.map(str::trim) {
        None | Some("") | Some("0") => false,
        Some(_) => true,
    }
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
mod tests {
    use super::backtrace_env_enabled;

    #[test]
    fn parses_rust_backtrace_values() {
        assert!(!backtrace_env_enabled(None));
        assert!(!backtrace_env_enabled(Some("")));
        assert!(!backtrace_env_enabled(Some("0")));
        assert!(!backtrace_env_enabled(Some(" 0 ")));
        assert!(backtrace_env_enabled(Some("1")));
        assert!(backtrace_env_enabled(Some("full")));
    }
}
