#![allow(dead_code)]

use mace::{Mace, OpCode, Options, RandomPath};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

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

pub fn wait_until<F>(timeout: Duration, step: Duration, mut predicate: F) -> bool
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while start.elapsed() <= timeout {
        if predicate() {
            return true;
        }
        std::thread::sleep(step);
    }
    false
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
