use crate::map::buffer::Buffers;
use crate::utils::countblock::Countblock;
use crate::utils::data::Meta;
use crate::utils::next_power_of_2;
use crate::utils::queue::Queue;
use crate::{OpCode, Options};

use super::log::{CState, GroupCommitter};
use super::worker::SyncWorker;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::sync::Arc;

pub struct Context {
    pub(crate) opt: Arc<Options>,
    /// maybe a bottleneck
    /// contains oldest, txid less than or equal to it can be purged
    pub(crate) meta: Arc<Meta>,
    workers: Arc<Vec<SyncWorker>>,
    active_workers: Queue<SyncWorker>,
    ctrl: Arc<CState>,
    sem: Arc<Countblock>,
}

fn group_commit_thread(
    mut gc: GroupCommitter,
    ctrl: Arc<CState>,
    buffer: Arc<Buffers>,
    worker: Arc<Vec<SyncWorker>>,
    sem: Arc<Countblock>,
) {
    std::thread::Builder::new()
        .name("group_commiter".into())
        .spawn(move || {
            log::debug!("start group commiter");
            gc.run(ctrl, buffer, worker, sem);
            log::debug!("stop group commiter");
        })
        .expect("can't spawn group commit thread");
}

impl Context {
    pub fn new(
        opt: Arc<Options>,
        sem: Arc<Countblock>,
        buffer: Arc<Buffers>,
        meta: Arc<Meta>,
    ) -> Arc<Self> {
        let cores = opt.workers;
        let gc = GroupCommitter::new(opt.clone(), meta.clone());
        let mut w = Vec::with_capacity(cores);
        let ctrl = CState::new();
        let fsn = Arc::new(AtomicU64::new(0));
        let active_workers = Queue::new(next_power_of_2(cores) as u32, None);

        for i in 0..cores {
            let x = SyncWorker::new(fsn.clone(), i as u16, opt.clone(), sem.clone());
            active_workers.push(x).unwrap();
            w.push(x);
        }
        let workers = Arc::new(w);
        group_commit_thread(gc, ctrl.clone(), buffer, workers.clone(), sem.clone());

        let this = Self {
            opt: opt.clone(),
            meta,
            workers,
            active_workers,
            ctrl,
            sem,
        };

        Arc::new(this)
    }

    pub fn alloc_worker(&self) -> Result<SyncWorker, OpCode> {
        self.active_workers.pop()
    }

    pub fn free_worker(&self, w: SyncWorker) {
        self.active_workers.push(w).expect("no space");
    }

    #[inline]
    pub fn worker(&self, wid: usize) -> SyncWorker {
        self.workers[wid]
    }

    pub fn workers(&self) -> &Vec<SyncWorker> {
        &self.workers
    }

    #[inline]
    pub(crate) fn wmk_oldest(&self) -> u64 {
        self.meta.wmk_oldest.load(Relaxed)
    }

    #[inline]
    pub(crate) fn update_wmk(&self, x: u64) {
        self.meta.wmk_oldest.store(x, Release);
    }

    #[inline]
    pub(crate) fn load_oracle(&self) -> u64 {
        self.meta.oracle.load(Relaxed)
    }

    #[inline]
    pub(crate) fn alloc_oracle(&self) -> u64 {
        self.meta.oracle.fetch_add(1, Relaxed)
    }

    pub(crate) fn start(&self) {
        self.ctrl.mark_working();
    }

    pub(crate) fn quit(&self) {
        self.sem.quit();
        self.workers.iter().for_each(|x| x.logging.wait_flush());
        self.ctrl.mark_stop();
        self.ctrl.wait();
        self.workers.iter().for_each(|x| x.reclaim());
    }
}
