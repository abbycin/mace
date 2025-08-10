use crate::OpCode;
use crate::map::buffer::Buffers;
use crate::utils::Handle;
use crate::utils::data::{Meta, WalDescHandle};
use crate::utils::options::ParsedOptions;
use crate::utils::queue::Queue;

use super::worker::SyncWorker;
use std::sync::Arc;
use std::sync::atomic::Ordering::{Relaxed, Release};

pub struct Context {
    pub(crate) opt: Arc<ParsedOptions>,
    /// maybe a bottleneck
    /// contains oldest, txid less than or equal to it can be purged
    pub(crate) meta: Arc<Meta>,
    workers: Arc<Vec<SyncWorker>>,
    active_workers: Queue<SyncWorker>,
}

impl Context {
    pub fn new(
        opt: Arc<ParsedOptions>,
        buffer: Handle<Buffers>,
        meta: Arc<Meta>,
        desc: &[WalDescHandle],
    ) -> Self {
        let cores = opt.workers;
        // NOTE: the elements of desc were ordered by worker id
        assert_eq!(cores, desc.len());
        let mut w = Vec::with_capacity(cores);
        // queue elems must > 1, when worker is 1, the next_power_of_two is 1 too, so we plus 1 here
        let active_workers = Queue::new((cores + 1).next_power_of_two());
        for i in desc.iter() {
            let x = SyncWorker::new(i.clone(), meta.clone(), opt.clone(), buffer);
            w.push(x);
        }

        for x in w.iter().rev() {
            active_workers.push(*x).unwrap();
        }

        Self {
            opt: opt.clone(),
            meta,
            workers: Arc::new(w),
            active_workers,
        }
    }

    pub fn alloc_worker(&self) -> Result<SyncWorker, OpCode> {
        self.active_workers.pop().ok_or(OpCode::Again)
    }

    pub fn free_worker(&self, w: SyncWorker) {
        self.active_workers.push(w).unwrap();
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
        self.workers
            .iter()
            .for_each(|w| w.logging.enable_checkpoint())
    }

    pub(crate) fn quit(&self) {
        self.workers.iter().for_each(|x| {
            x.logging.sync_desc();
            x.reclaim()
        });
    }
}
