use parking_lot::RwLock;

use crate::OpCode;
use crate::cc::cc::ConcurrencyControl;
use crate::meta::{Manifest, Numerics};
use crate::utils::data::WalDescHandle;
use crate::utils::options::ParsedOptions;
use crate::utils::queue::Queue;
use crate::utils::{CachePad, Handle, INIT_WMK, NULL_ORACLE, rand_range};

use super::worker::SyncWorker;
use std::ops::{Deref, DerefMut};
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::time::Duration;

pub struct Context {
    pub(crate) opt: Arc<ParsedOptions>,
    /// maybe a bottleneck
    /// contains oldest, txid less than or equal to it can be purged
    pub(crate) numerics: Arc<Numerics>,
    pub(crate) manifest: Manifest,
    pool: Arc<CCPool>,
    workers: Arc<Vec<SyncWorker>>,
    active_workers: Queue<SyncWorker>,
    nr_view: CachePad<AtomicUsize>,
    min_view_txid: Arc<AtomicU64>,
    tx: Sender<()>,
}

impl Context {
    pub fn new(opt: Arc<ParsedOptions>, manifest: Manifest, desc: &[WalDescHandle]) -> Self {
        let cores = opt.concurrent_write as usize;
        assert_eq!(cores, desc.len());
        let mut w = Vec::with_capacity(cores);
        let active_workers = Queue::new(cores);
        let numerics = manifest.numerics.clone();
        for d in desc.iter() {
            let x = SyncWorker::new(d.clone(), numerics.clone(), opt.clone());
            active_workers.push(x).unwrap();
            w.push(x);
        }

        let pool = Arc::new(CCPool::new(cores));
        let workers = Arc::new(w);
        let min_view_txid = Arc::new(AtomicU64::new(INIT_WMK));
        let (tx, rx) = channel();
        collect_thread(rx, min_view_txid.clone(), pool.clone());

        Self {
            opt: opt.clone(),
            numerics,
            manifest,
            pool,
            workers,
            active_workers,
            nr_view: CachePad::default(),
            min_view_txid,
            tx,
        }
    }

    pub fn oldest_view_txid(&self) -> Option<u64> {
        if self.nr_view.load(Relaxed) > 0 {
            Some(self.min_view_txid.load(Relaxed))
        } else {
            None
        }
    }

    pub fn alloc_worker(&self) -> Result<SyncWorker, OpCode> {
        self.active_workers.pop().ok_or(OpCode::Again)
    }

    pub fn free_worker(&self, w: SyncWorker) {
        self.active_workers.push(w).unwrap();
    }

    pub fn alloc_cc(&self) -> Handle<CCNode> {
        let start_ts = self.load_oracle();
        self.nr_view.fetch_add(1, Relaxed);
        // it's necessary for CommitTree's log compaction, before collect thread works
        let _ = self
            .min_view_txid
            .compare_exchange(INIT_WMK, start_ts, Relaxed, Relaxed);
        self.pool.alloc(start_ts, self.safe_txid())
    }

    pub fn free_cc(&self, cc: Handle<CCNode>) {
        self.nr_view.fetch_sub(1, Relaxed);
        self.pool.free(cc);
    }

    #[inline]
    pub fn worker(&self, wid: usize) -> SyncWorker {
        self.workers[wid]
    }

    pub fn workers(&self) -> &Vec<SyncWorker> {
        &self.workers
    }

    #[inline]
    pub(crate) fn safe_txid(&self) -> u64 {
        self.numerics.safe_tixd()
    }

    #[inline]
    pub(crate) fn update_wmk(&self, x: u64) {
        let oldest = if let Some(view) = self.oldest_view_txid() {
            view.min(x)
        } else {
            x
        };
        self.numerics.wmk_oldest.store(oldest, Relaxed);
    }

    #[inline]
    pub(crate) fn load_oracle(&self) -> u64 {
        self.numerics.oracle.load(Relaxed)
    }

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
        self.tx
            .send(())
            .expect("notify collector thread quit failed");
    }
}

fn collect_thread(reader: Receiver<()>, min_view_txid: Arc<AtomicU64>, pool: Arc<CCPool>) {
    std::thread::Builder::new()
        .name("collector".into())
        .spawn(move || {
            loop {
                let r = reader.recv_timeout(Duration::from_millis(10));
                match r {
                    Err(RecvTimeoutError::Timeout) => {
                        let mut min = NULL_ORACLE;
                        let r = pool.registry.read();
                        for h in r.iter() {
                            min = min.min(h.start_ts);
                        }
                        drop(r);
                        if min != NULL_ORACLE {
                            min_view_txid.store(min, Relaxed);
                        }
                    }
                    _ => break,
                }
            }
        })
        .expect("can't start collector thread");
}

const CCPOOL_SHARD: usize = 32;
const CCPOOL_SHARD_MASK: usize = CCPOOL_SHARD - 1;

pub(crate) struct CCNode {
    cc: ConcurrencyControl,
    next: AtomicPtr<CCNode>,
    shard_index: usize,
    registry_index: usize,
}

impl CCNode {
    fn new(concurrent_write: usize) -> Self {
        Self {
            cc: ConcurrencyControl::new(concurrent_write),
            next: AtomicPtr::new(null_mut()),
            shard_index: 0,
            registry_index: 0,
        }
    }
}

impl Deref for CCNode {
    type Target = ConcurrencyControl;

    fn deref(&self) -> &Self::Target {
        &self.cc
    }
}

impl DerefMut for CCNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cc
    }
}

struct CCPool {
    shards: [AtomicPtr<CCNode>; CCPOOL_SHARD],
    shard_index: CachePad<AtomicUsize>,
    // TODO: maybe change to seqlock ?
    registry: RwLock<Vec<Handle<CCNode>>>,
    concurrent_write: usize,
}

impl CCPool {
    fn new(concurrent_write: usize) -> Self {
        let mut registry = Vec::with_capacity(CCPOOL_SHARD);
        let shards = std::array::from_fn(|_| {
            let mut h = Handle::new(CCNode::new(concurrent_write));
            h.registry_index = registry.len();
            registry.push(h);
            AtomicPtr::new(h.inner())
        });
        Self {
            shards,
            shard_index: CachePad::default(),
            registry: RwLock::new(registry),
            concurrent_write,
        }
    }

    fn get_shard_idx(&self) -> usize {
        self.shard_index.fetch_add(1, Relaxed) & CCPOOL_SHARD_MASK
    }

    fn alloc(&self, start_ts: u64, safe_txid: u64) -> Handle<CCNode> {
        let mut shard = self.get_shard_idx();

        let mut h = 'outer: loop {
            let head = self.shards[shard].load(Acquire);
            if !head.is_null() {
                let next = unsafe { (*head).next.load(Acquire) };
                if self.shards[shard]
                    .compare_exchange_weak(head, next, AcqRel, Relaxed)
                    .is_ok()
                {
                    let mut h = Handle::from(head);
                    h.shard_index = shard;
                    break h;
                }
            }

            for i in 0..CCPOOL_SHARD {
                let idx = (shard + i) & CCPOOL_SHARD_MASK;
                let head = self.shards[idx].load(Acquire);
                if !head.is_null() {
                    shard = idx;
                    continue 'outer;
                }
            }

            let mut cc = Handle::new(CCNode::new(self.concurrent_write));
            cc.shard_index = shard;
            let mut r = self.registry.write();
            cc.registry_index = r.len();
            r.push(cc);
            break cc;
        };
        h.start_ts = start_ts;
        h.global_wmk_tx = safe_txid;
        h
    }

    fn free(&self, mut cc: Handle<CCNode>) {
        if self.registry.read().len() > CCPOOL_SHARD
            && rand_range(0..CCPOOL_SHARD) == 0
            && let Some(mut r) = self.registry.try_write()
            && r.len() > CCPOOL_SHARD
        {
            let last = r.swap_remove(cc.registry_index);
            assert_eq!(last.inner(), cc.inner());
            if cc.registry_index < r.len() {
                r[cc.registry_index].registry_index = cc.registry_index;
            }
            cc.reclaim();
            return;
        }

        cc.start_ts = NULL_ORACLE;
        let ptr = cc.inner();
        let index = cc.shard_index;
        loop {
            let head = self.shards[index].load(Acquire);
            unsafe { (*ptr).next.store(head, Release) };
            if self.shards[index]
                .compare_exchange_weak(head, ptr, AcqRel, Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
}

impl Drop for CCPool {
    fn drop(&mut self) {
        let mut r = self.registry.write();
        while let Some(x) = r.pop() {
            x.reclaim();
        }
    }
}
