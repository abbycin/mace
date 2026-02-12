#![allow(dead_code)]

use mace::{Mace, OpCode, Options, RandomPath};
use std::path::Path;
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
