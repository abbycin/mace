use std::alloc::{Layout, alloc_zeroed};
use std::cmp;
use std::cmp::min;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::{collections::HashSet, sync::RwLock};

use crate::utils::rand_range;

use super::context::Context;

#[derive(Default, Clone, Copy)]
#[repr(align(64))]
struct Ts(u64);

pub struct ConcurrencyControl {
    pub(crate) commit_tree: CommitTree,
    cached_sts: Vec<Ts>,
    cached_cts: Vec<Ts>,
    /// shared local water mark, avoid performing LCB in read-only txn
    wmk_oldest_tx: AtomicU64,
    /// latest commit ts, update everytime a txn was commited
    pub(crate) latest_cts: AtomicU64,
    /// snapshot of latest_cts in gc
    pub(crate) last_latest_cts: AtomicU64,
    /// snapshot of global water mark, for short circuit visibility checking
    pub(crate) global_wmk_tx: AtomicU64,
}

impl ConcurrencyControl {
    fn alloc(count: usize) -> Vec<Ts> {
        unsafe {
            let layout = Layout::from_size_align(count * size_of::<Ts>(), align_of::<Ts>())
                .expect("bad layout");
            let raw = alloc_zeroed(layout).cast::<Ts>();
            Vec::from_raw_parts(raw, count, count)
        }
    }
    pub(crate) fn new(workers: usize) -> Self {
        Self {
            commit_tree: CommitTree::new(workers),
            cached_sts: Self::alloc(workers),
            cached_cts: Self::alloc(workers),
            wmk_oldest_tx: AtomicU64::new(0),
            latest_cts: AtomicU64::new(0),
            last_latest_cts: AtomicU64::new(0),
            global_wmk_tx: AtomicU64::new(0),
        }
    }

    // NOTE: it's thread-safe
    /// check `txid` is visible to worker `wid` with txn with `start_ts`
    pub fn is_visible_to(
        &mut self,
        ctx: &Context,
        self_wid: u16,
        other_wid: u16,
        start_ts: u64,
        txid: u64,
    ) -> bool {
        // if txid was created on same worker, it's visible to later txn
        if self_wid == other_wid {
            return true;
        }

        let wid = other_wid as usize;

        // NOTE: The following applies only to SI
        if txid > start_ts {
            return false;
        }

        if self.global_wmk_tx.load(Relaxed) > txid {
            return true;
        }

        // short circuit
        if self.cached_sts[wid].0 == start_ts {
            return self.cached_cts[wid].0 >= txid;
        }

        if self.cached_cts[wid].0 >= txid {
            return true;
        }

        // slow path
        let lcb = ctx.worker(wid).cc.commit_tree.lcb(start_ts);
        if lcb != 0 {
            self.cached_sts[wid].0 = start_ts;
            self.cached_cts[wid].0 = lcb;
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
            self.global_wmk_tx.load(Relaxed)
        );
        for i in 0..self.cached_cts.len() {
            let (s, c) = (self.cached_sts[i], self.cached_cts[i]);
            log::debug!("start {} commit {}", s.0, c.0);
        }
        log::debug!("-------------- lcb -----------");
        self.commit_tree.show();
    }
}

pub struct CommitTree {
    /// <commitTs, startTs>
    log: Vec<(u64, u64)>,
    cap: usize,
    lk: RwLock<()>,
}

impl CommitTree {
    pub fn new(workers: usize) -> Self {
        Self {
            log: Vec::new(),
            cap: workers,
            lk: RwLock::new(()),
        }
    }

    pub fn lcb_impl(log: &[(u64, u64)], start_ts: u64) -> Option<usize> {
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
        let _lk = self.lk.read().expect("can't lock read");
        if let Some(pos) = Self::lcb_impl(&self.log, start_ts) {
            self.log[pos].1
        } else {
            0
        }
    }

    #[allow(dead_code)]
    fn show(&self) {
        for (c, s) in &self.log {
            log::debug!("start {} commit {}", *s, *c);
        }
    }

    pub fn append(&mut self, start: u64, commit: u64) {
        let _lk = self.lk.write().expect("can't lock write");
        self.log.push((commit, start));
    }

    pub fn compact(&mut self, ctx: &Context, this_worker: u16) {
        let rlk = self.lk.read().expect("can't lock read");
        if self.log.len() < self.cap {
            return;
        }
        let mut set = HashSet::new();

        set.insert(self.log[self.log.len() - 1]);
        drop(rlk);

        for w in ctx.workers().iter() {
            if this_worker == w.logging.worker {
                continue;
            }

            let txid = w.tx_id.load(Relaxed);
            if txid == 0 {
                // already commited
                continue;
            }

            let lk = self.lk.read().expect("can't lock read");
            if let Some(c) = Self::lcb_impl(&self.log, txid) {
                set.insert(self.log[c]);
            }
            drop(lk);
        }

        let _wlk = self.lk.write().expect("can't lock write");
        self.log.clear();
        for p in set {
            self.log.push(p);
        }
        self.log.sort_unstable();
    }
}

#[cfg(test)]
mod test {
    use super::CommitTree;

    #[test]
    fn commit_tree() {
        let mut t = CommitTree::new(10);

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
