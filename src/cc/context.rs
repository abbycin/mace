use crossbeam_epoch::Guard;
use parking_lot::{Mutex, RwLock};

use crate::OpCode;
use crate::cc::cc::ConcurrencyControl;
use crate::meta::Numerics;
use crate::utils::data::Position;
use crate::utils::options::ParsedOptions;
use crate::utils::{CachePad, Handle, INIT_WMK, NULL_ORACLE};

use super::group::WriterGroup;
use std::ops::{Deref, DerefMut};
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub(crate) struct GroupBoot {
    pub oldest_id: u64,
    pub latest_id: u64,
    pub checkpoint: Position,
}

pub struct Context {
    pub(crate) opt: Arc<ParsedOptions>,
    /// maybe a bottleneck
    /// contains oldest, txid less than or equal to it can be purged
    pub(crate) numerics: Arc<Numerics>,
    pool: Arc<CCPool>,
    groups: Arc<Vec<WriterGroup>>,
    nr_view: CachePad<AtomicUsize>,
    group_rr: CachePad<AtomicUsize>,
    /// this value is advanced by collect_thread based on active view snapshots
    min_view_txid: Arc<AtomicU64>,
    tx: Sender<()>,
    collector: Mutex<Option<JoinHandle<()>>>,
}

impl Context {
    pub fn new(opt: Arc<ParsedOptions>, numerics: Arc<Numerics>, group_boot: &[GroupBoot]) -> Self {
        let cores = opt.concurrent_write as usize;
        let mut groups = Vec::with_capacity(cores);
        for i in 0..cores {
            let boot = group_boot.get(i).copied().unwrap_or(GroupBoot {
                oldest_id: 0,
                latest_id: 0,
                checkpoint: Position::default(),
            });
            let g = WriterGroup::new(
                i,
                boot.checkpoint,
                boot.latest_id,
                boot.oldest_id,
                numerics.clone(),
                opt.clone(),
            );
            groups.push(g);
        }

        let pool = Arc::new(CCPool::new(cores));
        let groups = Arc::new(groups);
        let min_view_txid = Arc::new(AtomicU64::new(INIT_WMK));
        let (tx, rx) = channel();
        let collector = collect_thread(rx, min_view_txid.clone(), pool.clone());

        Self {
            opt: opt.clone(),
            numerics,
            pool,
            groups,
            nr_view: CachePad::default(),
            group_rr: CachePad::default(),
            min_view_txid,
            tx,
            collector: Mutex::new(Some(collector)),
        }
    }

    pub fn oldest_view_txid(&self) -> Option<u64> {
        if self.nr_view.load(Relaxed) > 0 {
            Some(self.min_view_txid.load(Relaxed))
        } else {
            None
        }
    }

    pub fn alloc_cc(&self) -> Handle<CCNode> {
        let start_ts = self.load_oracle();
        self.nr_view.fetch_add(1, Relaxed);
        // it's necessary for CommitTree's log compaction, before collect thread works
        let _ = self
            .min_view_txid
            .compare_exchange(INIT_WMK, start_ts, Relaxed, Relaxed);
        self.pool.alloc(start_ts)
    }

    pub fn free_cc(&self, cc: Handle<CCNode>) {
        self.nr_view.fetch_sub(1, Relaxed);
        self.pool.free(cc);
    }

    #[inline]
    pub fn group(&self, gid: usize) -> &WriterGroup {
        &self.groups[gid]
    }

    pub fn groups(&self) -> &Vec<WriterGroup> {
        &self.groups
    }

    pub(crate) fn next_group_id(&self) -> usize {
        let nr = self.groups.len();
        const SPIN_RETRY: usize = 32;
        let mut best_gid = 0;
        let mut best_load = usize::MAX;
        for attempt in 0..=SPIN_RETRY {
            let start = self.group_rr.fetch_add(1, Relaxed) % nr;
            for i in 0..nr {
                let gid = (start + i) % nr;
                if self.groups[gid].try_enter_inflight_if_free() {
                    return gid;
                }
                let load = self.groups[gid].inflight();
                if load < best_load {
                    best_load = load;
                    best_gid = gid;
                }
            }
            if best_load <= 1 && attempt < SPIN_RETRY {
                std::thread::yield_now();
                best_load = usize::MAX;
                continue;
            }
            self.groups[best_gid].enter_inflight();
            return best_gid;
        }
        unreachable!()
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
        self.groups.iter().for_each(|w| {
            w.logging.lock().enable_checkpoint();
        })
    }

    pub(crate) fn quit(&self) {
        self.groups.iter().for_each(|x| {
            let r = {
                let mut log = x.logging.lock();
                log.sync(true)
            };
            r.inspect_err(|e| {
                log::error!("can't stabilize WAL, {:?}", e);
            })
            .expect("can't fail");
        });
        self.tx
            .send(())
            .expect("notify collector thread quit failed");

        if let Some(h) = self.collector.lock().take() {
            let _ = h.join();
        }
    }

    pub fn sync(&self) -> Result<(), OpCode> {
        for group in self.groups.iter() {
            let mut log = group.logging.lock();
            log.sync(true)?;
        }
        Ok(())
    }
}

fn collect_thread(
    reader: Receiver<()>,
    min_view_txid: Arc<AtomicU64>,
    pool: Arc<CCPool>,
) -> JoinHandle<()> {
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
                        pool.maybe_shrink();
                    }
                    _ => break,
                }
            }
        })
        .expect("can't start collector thread")
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
    registry_len: CachePad<AtomicUsize>,
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
            registry_len: CachePad::from(AtomicUsize::new(CCPOOL_SHARD)),
            concurrent_write,
        }
    }

    fn get_shard_idx(&self) -> usize {
        self.shard_index.fetch_add(1, Relaxed) & CCPOOL_SHARD_MASK
    }

    fn try_pop_shard(&self, index: usize, _guard: &Guard) -> Option<Handle<CCNode>> {
        loop {
            let head = self.shards[index].load(Acquire);
            if head.is_null() {
                return None;
            }
            let next = unsafe { (*head).next.load(Acquire) };
            if self.shards[index]
                .compare_exchange_weak(head, next, AcqRel, Relaxed)
                .is_ok()
            {
                let mut h = Handle::from(head);
                h.shard_index = index;
                return Some(h);
            }
        }
    }

    fn push_shard(&self, index: usize, mut cc: Handle<CCNode>) {
        cc.start_ts = NULL_ORACLE;
        let ptr = cc.inner();
        loop {
            let head = self.shards[index].load(Acquire);
            unsafe { (*ptr).next.store(head, Release) };
            if self.shards[index]
                .compare_exchange_weak(head, ptr, AcqRel, Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    fn alloc(&self, start_ts: u64) -> Handle<CCNode> {
        let shard = self.get_shard_idx();
        let guard = crossbeam_epoch::pin();

        let mut h = if let Some(x) = self.try_pop_shard(shard, &guard) {
            x
        } else {
            let mut popped = None;
            for i in 0..CCPOOL_SHARD {
                let idx = (shard + i) & CCPOOL_SHARD_MASK;
                if let Some(x) = self.try_pop_shard(idx, &guard) {
                    popped = Some(x);
                    break;
                }
            }

            if let Some(x) = popped {
                x
            } else {
                let mut cc = Handle::new(CCNode::new(self.concurrent_write));
                cc.shard_index = shard;
                cc.start_ts = start_ts;
                let mut r = self.registry.write();
                cc.registry_index = r.len();
                r.push(cc);
                self.registry_len.store(r.len(), Release);
                cc
            }
        };
        h.start_ts = start_ts;
        h
    }

    fn free(&self, cc: Handle<CCNode>) {
        self.push_shard(cc.shard_index, cc);
    }

    fn maybe_shrink_one(&self, start: usize, guard: &Guard) -> bool {
        let mut victim = None;
        for i in 0..CCPOOL_SHARD {
            let idx = (start + i) & CCPOOL_SHARD_MASK;
            if let Some(cc) = self.try_pop_shard(idx, guard) {
                victim = Some((idx, cc));
                break;
            }
        }
        let Some((idx, cc)) = victim else {
            return false;
        };
        let Some(mut r) = self.registry.try_write() else {
            self.push_shard(idx, cc);
            return false;
        };
        if r.len() <= CCPOOL_SHARD {
            drop(r);
            self.push_shard(idx, cc);
            return false;
        }

        let last = r.swap_remove(cc.registry_index);
        assert_eq!(last.inner(), cc.inner());
        if cc.registry_index < r.len() {
            r[cc.registry_index].registry_index = cc.registry_index;
        }
        self.registry_len.store(r.len(), Release);
        drop(r);
        guard.defer(move || cc.reclaim());
        true
    }

    fn maybe_shrink(&self) {
        let len = self.registry_len.load(Acquire);
        if len <= CCPOOL_SHARD {
            return;
        }
        let backlog = len - CCPOOL_SHARD;
        let mut batch = backlog / 8;
        if !backlog.is_multiple_of(8) {
            batch += 1;
        }
        batch = batch.clamp(1, 16);

        let guard = crossbeam_epoch::pin();
        let deadline = Instant::now() + Duration::from_micros(200);
        let mut start = self.get_shard_idx();
        let mut done = 0;
        while done < batch
            && self.registry_len.load(Acquire) > CCPOOL_SHARD
            && Instant::now() < deadline
        {
            if !self.maybe_shrink_one(start, &guard) {
                break;
            }
            done += 1;
            start = (start + 1) & CCPOOL_SHARD_MASK;
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

#[cfg(test)]
mod tests {
    use super::{CCPOOL_SHARD, CCPool};
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn ccpool_shrink_reclaims_idle_nodes() {
        let pool = CCPool::new(4);
        let total = CCPOOL_SHARD + 8;
        let mut handles = Vec::with_capacity(total);
        for i in 0..total {
            handles.push(pool.alloc(i as u64 + 1));
        }
        assert!(pool.registry_len.load(Relaxed) >= total);

        for h in handles {
            pool.free(h);
        }
        for _ in 0..(total * 4) {
            pool.maybe_shrink();
            if pool.registry_len.load(Relaxed) == CCPOOL_SHARD {
                break;
            }
        }
        assert_eq!(pool.registry_len.load(Relaxed), CCPOOL_SHARD);
    }

    #[test]
    fn ccpool_alloc_free_fast_path_keeps_registry_len() {
        let pool = CCPool::new(4);
        let base_len = pool.registry_len.load(Relaxed);
        let h = pool.alloc(10);
        pool.free(h);
        assert_eq!(pool.registry_len.load(Relaxed), base_len);

        let h2 = pool.alloc(11);
        assert_eq!(h2.start_ts, 11);
        pool.free(h2);
    }
}
