use parking_lot::{Condvar, Mutex};
use std::{cmp::min, time::Duration};

pub struct Countblock {
    lock: Mutex<isize>,
    cond: Condvar,
}

impl Countblock {
    pub fn new(count: usize) -> Self {
        Self {
            lock: Mutex::new(min(count, isize::MAX as usize) as isize),
            cond: Condvar::new(),
        }
    }

    pub fn post(&self) {
        let mut c = self.lock.lock();
        *c += 1;
        self.cond.notify_one();
    }

    pub fn wait(&self) {
        let mut c = self.lock.lock();
        *c -= 1;
        while *c == 0 {
            self.cond.wait_for(&mut c, Duration::from_millis(1));
        }
    }
}
