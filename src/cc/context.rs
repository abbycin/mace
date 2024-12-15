use crate::Options;

use super::log::GroupCommitter;
use super::worker::SyncWorker;
use super::INIT_ORACLE;
use std::collections::HashMap;
use std::sync::atomic::Ordering::Release;
use std::sync::{
    atomic::{AtomicU64, Ordering::Relaxed},
    Mutex,
};
use std::sync::{Arc, RwLock, RwLockReadGuard};

#[derive(Default)]
pub struct WmkInfo {
    /// oldest, txid less than or equal to it can be purged
    pub wmk_odlest: AtomicU64,
    /// active oldest txid, excluding txid in commit tree, be used to find wmk_of_all in commit tree
    pub oldest_tx: AtomicU64,
    pub mutex: Mutex<()>,
}

impl WmkInfo {
    pub fn new() -> Self {
        Self {
            wmk_odlest: AtomicU64::new(0),
            oldest_tx: AtomicU64::new(0),
            mutex: Mutex::new(()),
        }
    }

    pub fn update_wmk(&self, old: u64) {
        self.wmk_odlest.store(old, Release);
    }
}

pub struct Context {
    pub(crate) opt: Arc<Options>,
    /// maybe a bottleneck
    oracle: AtomicU64,
    workers: Arc<RwLock<HashMap<usize, SyncWorker>>>,
    pub wmk_info: WmkInfo,
    pub commiter: Arc<GroupCommitter>,
}

fn group_commit_thread(gc: Arc<GroupCommitter>, worker: Arc<RwLock<HashMap<usize, SyncWorker>>>) {
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
        let gc = Arc::new(GroupCommitter::new(opt.clone()));
        let worker = Arc::new(RwLock::new(HashMap::new()));
        group_commit_thread(gc.clone(), worker.clone());

        Self {
            opt: opt.clone(),
            oracle: AtomicU64::new(INIT_ORACLE),
            workers: worker,
            wmk_info: WmkInfo::new(),
            commiter: gc.clone(),
        }
    }

    pub fn get_worker(&self) -> SyncWorker {
        let core = coreid::current_core();

        if let Some(w) = self.workers.read().expect("can't lock read").get(&core) {
            return *w;
        }
        // dead lock will not happen
        let mut lk = self.workers.write().expect("can't lock write");
        let w = SyncWorker::new(self.commiter.clone(), core as u16, self.opt.wal_buffer_size);
        lk.insert(core, w);
        let cap = core + 1; // + 1 since core is 0 based
        lk.iter_mut()
            .map(|(_, x)| x.cc.commit_tree.update_cap(cap))
            .count();
        w
    }

    pub fn worker(&self, wid: usize) -> SyncWorker {
        let lk = self.workers.read().expect("can't lock read");
        *lk.get(&wid).expect("user should ensure worker is valid")
    }

    pub fn workers(&self) -> RwLockReadGuard<HashMap<usize, SyncWorker>> {
        self.workers.read().expect("can't lock read")
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
        for (_, w) in self.workers.read().expect("can't lock read").iter() {
            w.reclaim();
        }
    }
}
