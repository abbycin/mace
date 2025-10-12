use crate::OpCode;
use crate::meta::{Manifest, Numerics};
use crate::utils::data::WalDescHandle;
use crate::utils::options::ParsedOptions;
use crate::utils::queue::Queue;

use super::worker::SyncWorker;
use std::sync::Arc;
use std::sync::atomic::Ordering::{Relaxed, Release};

pub struct Context {
    pub(crate) opt: Arc<ParsedOptions>,
    /// maybe a bottleneck
    /// contains oldest, txid less than or equal to it can be purged
    pub(crate) numerics: Arc<Numerics>,
    pub(crate) manifest: Manifest,
    workers: Arc<Vec<SyncWorker>>,
    active_workers: Queue<SyncWorker>,
}

impl Context {
    pub fn new(opt: Arc<ParsedOptions>, manifest: Manifest, desc: &[WalDescHandle]) -> Self {
        let cores = opt.workers as usize;
        // NOTE: the elements of desc were ordered by worker id
        assert_eq!(cores, desc.len());
        let mut w = Vec::with_capacity(cores);
        // queue elems must > 1, when worker is 1, the next_power_of_two is 1 too, so we plus 1 here
        let active_workers = Queue::new((cores + 1).next_power_of_two());
        let numerics = manifest.numerics.clone();
        for i in desc.iter() {
            let x = SyncWorker::new(i.clone(), numerics.clone(), opt.clone());
            w.push(x);
        }

        for x in w.iter().rev() {
            active_workers.push(*x).unwrap();
        }
        let workers = Arc::new(w);

        Self {
            opt: opt.clone(),
            numerics,
            workers,
            active_workers,
            manifest,
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
        self.numerics.wmk_oldest.load(Relaxed)
    }

    #[inline]
    pub(crate) fn update_wmk(&self, x: u64) {
        self.numerics.wmk_oldest.store(x, Release);
    }

    #[inline]
    pub(crate) fn load_oracle(&self) -> u64 {
        self.numerics.oracle.load(Relaxed)
    }

    #[inline]
    pub(crate) fn alloc_oracle(&self) -> u64 {
        self.numerics.oracle.fetch_add(1, Relaxed)
    }

    pub(crate) fn start(&self) {
        self.workers
            .iter()
            .for_each(|w| w.logging.enable_checkpoint())
    }

    pub(crate) fn quit(&self) {
        self.workers.iter().for_each(|x| {
            // we are the last one, so use mutable copy is ok
            let mut h = *x;
            h.logging.stabilize();
            x.reclaim()
        });
    }
}
