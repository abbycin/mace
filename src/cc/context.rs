use crate::Options;

use super::log::GroupCommitter;
use super::worker::SyncWorker;
use super::INIT_ORACLE;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

pub struct Context {
    pub(crate) opt: Arc<Options>,
    /// maybe a bottleneck
    oracle: AtomicU64,
    workers: Arc<Vec<SyncWorker>>,
    /// oldest, txid less than or equal to it can be purged
    pub wmk_oldest: AtomicU64,
    pub commiter: Arc<GroupCommitter>,
}

fn group_commit_thread(gc: Arc<GroupCommitter>, worker: Arc<Vec<SyncWorker>>) {
    std::thread::Builder::new()
        .name("group_commiter".into())
        .spawn(move || {
            log::debug!("start group commiter");
            gc.run(worker);
            log::debug!("stop group commiter");
        })
        .expect("can't spawn group commit thread");
}

impl Context {
    pub fn new(opt: Arc<Options>) -> Self {
        let cores = coreid::cores_online();
        let gc = Arc::new(GroupCommitter::new(opt.clone()));
        let mut w = Vec::with_capacity(cores);
        for i in 0..cores {
            w.push(SyncWorker::new(gc.clone(), i as u16, opt.wal_buffer_size));
        }
        let worker = Arc::new(w);
        group_commit_thread(gc.clone(), worker.clone());

        Self {
            opt: opt.clone(),
            oracle: AtomicU64::new(INIT_ORACLE),
            workers: worker,
            wmk_oldest: AtomicU64::new(0),
            commiter: gc.clone(),
        }
    }

    pub fn worker(&self, wid: usize) -> SyncWorker {
        self.workers[wid]
    }

    pub fn workers(&self) -> Arc<Vec<SyncWorker>> {
        self.workers.clone()
    }

    pub(crate) fn load_oracle(&self) -> u64 {
        self.oracle.load(Relaxed)
    }

    pub(crate) fn alloc_oracle(&self) -> u64 {
        self.oracle.fetch_add(1, Relaxed)
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        self.commiter.signal_stop();
        self.commiter.wait();
        for w in self.workers.iter() {
            w.reclaim()
        }
    }
}
