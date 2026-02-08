use parking_lot::{Condvar, Mutex};

pub struct Countblock {
    lock: Mutex<usize>,
    cond: Condvar,
}

impl Countblock {
    pub fn new(count: usize) -> Self {
        Self {
            lock: Mutex::new(count),
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
        while *c == 0 {
            self.cond.wait(&mut c);
        }
        *c -= 1;
    }
}
