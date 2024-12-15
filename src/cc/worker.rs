use std::{
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicU64, Arc},
};

use super::{
    cc::{ConcurrencyControl, Transaction},
    log::{GroupCommitter, Logging},
};

pub struct Worker {
    pub cc: ConcurrencyControl,
    pub txn: Transaction,
    pub logging: Logging,
    /// a snapshot of [`Transaction::start_ts`] which will be used across threads
    pub tx_id: AtomicU64,
    pub id: u16,
}

impl Worker {
    fn new(ctx: Arc<GroupCommitter>, id: u16, buffer_size: usize) -> Self {
        Self {
            cc: ConcurrencyControl::new(),
            txn: Transaction::new(),
            logging: Logging::new(ctx, buffer_size, id),
            tx_id: AtomicU64::new(0),
            id,
        }
    }
}

#[derive(Clone, Copy)]
pub struct SyncWorker {
    w: *mut Worker,
}

impl SyncWorker {
    pub fn new(ctx: Arc<GroupCommitter>, id: u16, buffer_size: usize) -> Self {
        let w = Box::new(Worker::new(ctx, id, buffer_size));
        Self {
            w: Box::into_raw(w),
        }
    }

    pub fn reclaim(&self) {
        let b = unsafe { Box::from_raw(self.w) };
        drop(b);
    }
}

unsafe impl Sync for SyncWorker {}
unsafe impl Send for SyncWorker {}

impl Deref for SyncWorker {
    type Target = Worker;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.w }
    }
}

impl DerefMut for SyncWorker {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.w }
    }
}
