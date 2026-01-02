use parking_lot::RwLock;
use std::cell::Cell;
use std::cmp;
use std::cmp::min;
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use crate::utils::{NULL_ORACLE, rand_range};

use super::context::Context;

#[derive(Debug)]
pub struct ConcurrencyControl {
    pub(crate) commit_tree: CommitTree,
    cached_sts: Vec<Cell<u64>>,
    cached_cts: Vec<Cell<u64>>,
    /// shared local water mark, avoid performing LCB in read-only txn
    wmk_oldest_tx: AtomicU64,
    /// latest commit ts, update everytime a txn was commited
    pub(crate) latest_cts: AtomicU64,
    /// snapshot of latest_cts in gc
    pub(crate) last_latest_cts: AtomicU64,
    // snapshot of global water mark, for short circuit visibility checking
    pub(crate) global_wmk_tx: u64,
    pub(crate) start_ts: u64,
}

unsafe impl Send for ConcurrencyControl {}
unsafe impl Sync for ConcurrencyControl {}

impl ConcurrencyControl {
    pub(crate) fn new(workers: usize) -> Self {
        Self {
            commit_tree: CommitTree::new(workers),
            cached_sts: (0..workers).map(|_| Cell::new(0)).collect(),
            cached_cts: (0..workers).map(|_| Cell::new(0)).collect(),
            wmk_oldest_tx: AtomicU64::new(0),
            latest_cts: AtomicU64::new(0),
            last_latest_cts: AtomicU64::new(0),
            global_wmk_tx: 0,
            start_ts: NULL_ORACLE, // it's required for CommitTree's log compaction
        }
    }

    // NOTE: it's thread-safe
    /// check `txid` is visible to worker `wid` with txn with `start_ts`
    pub fn is_visible_to(
        &self,
        ctx: &Context,
        self_wid: u8,
        record_wid: u8,
        start_ts: u64,
        txid: u64,
    ) -> bool {
        // if txid was created by same worker, it's visible to later txn
        if self_wid == record_wid {
            return true;
        }

        let wid = record_wid as usize;

        // NOTE: The following applies only to SI
        if txid > start_ts {
            return false;
        }

        if self.global_wmk_tx > txid {
            return true;
        }

        // short circuit
        if self.cached_sts[wid].get() == start_ts {
            return self.cached_cts[wid].get() >= txid;
        }

        if self.cached_cts[wid].get() >= txid {
            return true;
        }

        // slow path
        let lcb = ctx.worker(wid).cc.commit_tree.lcb(start_ts);
        if lcb != 0 {
            self.cached_sts[wid].set(start_ts);
            self.cached_cts[wid].set(lcb);
            return lcb >= txid;
        }

        false
    }

    /// collect water mark for safe consolidation, currently only [`WmkInfo::wmk_of_old`] is used
    pub fn collect_wmk(&self, ctx: &Context) {
        // 1/n probability, balance overhead
        let workers = ctx.workers();

        if rand_range(0..workers.len()) != 0 {
            return;
        }

        let mut oldest_tx = u64::MAX;

        for w in workers.iter() {
            let cur_tx = w.tx_id.load(Acquire);
            if cur_tx == 0 {
                continue;
            }

            oldest_tx = min(cur_tx, oldest_tx);
        }

        let mut g_old = u64::MAX;

        for w in workers.iter() {
            let cc = &w.cc;

            // no gc happened before
            if cc.last_latest_cts.load(Relaxed) == cc.latest_cts.load(Relaxed) {
                let old = cc.wmk_oldest_tx.load(Acquire);
                if old > 0 {
                    g_old = min(g_old, old);
                }
                continue;
            }

            let local_wmk_old = cc.commit_tree.lcb(oldest_tx);

            cc.wmk_oldest_tx.store(local_wmk_old, Release);

            cc.last_latest_cts
                .store(cc.latest_cts.load(Relaxed), Relaxed);

            if local_wmk_old > 0 {
                g_old = min(g_old, local_wmk_old);
            }
        }

        if g_old != u64::MAX {
            ctx.update_wmk(g_old);
        }
    }

    #[allow(dead_code)]
    pub fn show(&self) {
        log::debug!("------------ cache ----------");
        log::debug!(
            "wmk_oldest_tx {} global_wmk_tx {}",
            self.wmk_oldest_tx.load(Relaxed),
            self.global_wmk_tx
        );
        for i in 0..self.cached_cts.len() {
            let (s, c) = (self.cached_sts[i].get(), self.cached_cts[i].get());
            log::debug!("start {} commit {}", s, c);
        }
        log::debug!("-------------- lcb -----------");
        self.commit_tree.show();
    }
}

#[derive(Debug)]
pub struct CommitTree {
    /// <commitTs, startTs>
    log: RwLock<Vec<(u64, u64)>>,
    cap: usize,
}
unsafe impl Send for CommitTree {}
unsafe impl Sync for CommitTree {}

impl CommitTree {
    pub fn new(workers: usize) -> Self {
        Self {
            log: RwLock::new(Vec::new()),
            cap: workers + 1, // plus 1 for read-only txn
        }
    }

    fn lcb_impl(log: &[(u64, u64)], start_ts: u64) -> Option<usize> {
        let mut b = 0;
        let mut e = log.len();

        while b < e {
            let mid = b + (e - b) / 2;
            match log[mid].0.cmp(&start_ts) {
                cmp::Ordering::Equal | cmp::Ordering::Greater => e = mid,
                cmp::Ordering::Less => b = mid + 1,
            }
        }
        // LCB(w, ts) > vts, excluding `=`
        if b > 0 {
            Some(b - 1)
        } else if !log.is_empty() && log[b].0 < start_ts {
            Some(b)
        } else {
            None
        }
    }

    /// return last commited `start_ts` before given `start_ts`
    pub fn lcb(&self, start_ts: u64) -> u64 {
        let log = self.log.read();
        if let Some(pos) = Self::lcb_impl(&log, start_ts) {
            log[pos].1
        } else {
            0
        }
    }

    #[allow(dead_code)]
    fn show(&self) {
        let log = self.log.read();
        for (c, s) in log.iter() {
            log::debug!("start {} commit {}", *s, *c);
        }
    }

    pub fn append(&self, start: u64, commit: u64) {
        let mut log = self.log.write();
        log.push((commit, start));
    }

    pub fn compact(&self, ctx: &Context, this_worker: u8) {
        let log = self.log.write();
        if log.len() < self.cap {
            return;
        }
        let mut set = HashSet::new();

        set.insert(log[log.len() - 1]);
        // at least keep the oldest read-only txn's LCB
        if let Some(view) = ctx.oldest_view_txid()
            && let Some(c) = Self::lcb_impl(&log, view)
        {
            set.insert(log[c]);
        }
        drop(log);

        for w in ctx.workers().iter() {
            if this_worker == w.logging.worker {
                continue;
            }

            let txid = w.tx_id.load(Relaxed);
            if txid == 0 {
                // already commited
                continue;
            }

            let log = self.log.read();
            if let Some(c) = Self::lcb_impl(&log, txid) {
                set.insert(log[c]);
            }
            drop(log);
        }

        let mut log = self.log.write();
        log.clear();
        for p in set {
            log.push(p);
        }
        log.sort_unstable();
    }
}

#[cfg(test)]
mod test {
    use super::CommitTree;

    #[test]
    fn commit_tree() {
        let t = CommitTree::new(10);

        t.append(1, 2);
        t.append(3, 4);
        t.append(5, 6);
        t.append(7, 8);
        t.append(9, 10);

        assert_eq!(t.lcb(6), 3);
        assert_eq!(t.lcb(9), 7);
        assert_eq!(t.lcb(11), 9);
        assert_eq!(t.lcb(0), 0);
        assert_eq!(t.lcb(2), 0);
    }
}
