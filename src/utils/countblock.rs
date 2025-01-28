use std::{
    cmp::min,
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Condvar, Mutex,
    },
    time::Duration,
};

pub struct Countblock {
    lock: Mutex<isize>,
    cond: Condvar,
    flag: AtomicBool,
}

impl Countblock {
    pub fn new(count: usize) -> Self {
        Self {
            lock: Mutex::new(min(count, isize::MAX as usize) as isize),
            cond: Condvar::new(),
            flag: AtomicBool::new(false),
        }
    }

    pub fn post(&self) {
        if !self.flag.load(Relaxed) {
            let mut c = self.lock.lock().expect("can't lock");
            *c += 1;
            self.cond.notify_one();
        }
    }

    pub fn wait(&self) {
        if !self.flag.load(Relaxed) {
            let mut c = self.lock.lock().expect("can't lock");
            while *c <= 0 && !self.flag.load(Relaxed) {
                c = self
                    .cond
                    .wait_timeout(c, Duration::from_millis(1))
                    .expect("can't wait")
                    .0;
            }
            *c -= 1;
        }
    }

    pub fn quit(&self) {
        self.flag.store(true, Relaxed);
    }
}
