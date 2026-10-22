//! helpers shared by the CLI acceptance tests
//!
//! this is a deliberately small copy of the engine test suite's child-process
//! helper: the CLI is a separate package, so it cannot reuse `tests/common`
//! from the workspace root.

use std::path::Path;
use std::process::Command;

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

/// spawn a child test binary through the target runner when cross-testing
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
