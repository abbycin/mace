use std::{
    cmp::min,
    sync::{Condvar, Mutex},
    time::Duration,
};

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
        let mut c = self.lock.lock().expect("can't lock");
        *c += 1;
        self.cond.notify_one();
    }

    pub fn wait(&self) {
        let mut c = self.lock.lock().expect("can't lock");
        *c -= 1;
        while *c == 0 {
            c = self
                .cond
                .wait_timeout(c, Duration::from_millis(1))
                .expect("can't wait")
                .0;
        }
    }
}
