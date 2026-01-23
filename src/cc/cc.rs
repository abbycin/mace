use parking_lot::RwLock;
use std::cmp;
use std::cmp::min;
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use crate::utils::{NULL_ORACLE, rand_range};

use super::context::Context;

#[derive(Debug)]
struct CacheEntry {
    seq: AtomicU64,
    sts: AtomicU64,
    cts: AtomicU64,
}

impl CacheEntry {
    fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
            sts: AtomicU64::new(0),
            cts: AtomicU64::new(0),
        }
    }

    fn load(&self) -> (u64, u64) {
        loop {
            let v1 = self.seq.load(Acquire);
            if v1 & 1 == 1 {
                continue;
            }
            let s = self.sts.load(Relaxed);
            let c = self.cts.load(Relaxed);
            let v2 = self.seq.load(Acquire);
            if v1 == v2 {
                return (s, c);
            }
        }
    }

    fn store(&self, s: u64, c: u64) {
        self.seq.fetch_add(1, Release);
        self.sts.store(s, Relaxed);
        self.cts.store(c, Relaxed);
        self.seq.fetch_add(1, Release);
    }
}

#[derive(Debug)]
pub struct ConcurrencyControl {
    pub(crate) commit_tree: CommitTree,
    cached: Vec<CacheEntry>,
    /// shared local water mark, avoid performing LCB in read-only txn
    wmk_oldest_tx: AtomicU64,
    /// latest commit ts, update everytime a txn was commited
    pub(crate) latest_cts: AtomicU64,
    /// snapshot of latest_cts in gc
    pub(crate) last_latest_cts: AtomicU64,
    pub(crate) start_ts: u64,
}

unsafe impl Send for ConcurrencyControl {}
unsafe impl Sync for ConcurrencyControl {}

impl ConcurrencyControl {
    pub(crate) fn new(groups: usize) -> Self {
        Self {
            commit_tree: CommitTree::new(groups),
            cached: (0..groups).map(|_| CacheEntry::new()).collect(),
            wmk_oldest_tx: AtomicU64::new(0),
            latest_cts: AtomicU64::new(0),
            last_latest_cts: AtomicU64::new(0),
            start_ts: NULL_ORACLE, // it's required for CommitTree's log compaction
        }
    }

    // NOTE: it's thread-safe
    pub fn is_visible_to(
        &self,
        ctx: &Context,
        self_gid: u8,
        record_gid: u8,
        start_ts: u64,
        record_txid: u64,
    ) -> bool {
        let gid = record_gid as usize;

        // NOTE: The following applies only to SI
        if record_txid == start_ts {
            return self_gid == record_gid;
        }

        if record_txid > start_ts {
            return false;
        }

        if ctx.safe_txid() > record_txid {
            return true;
        }

        // short circuit
        let (s, c) = self.cached[gid].load();
        if s <= start_ts && c >= record_txid {
            return true;
        }

        // slow path
        let lcb = ctx.group(gid).cc.commit_tree.lcb(start_ts);
        if lcb != 0 {
            self.cached[gid].store(start_ts, lcb);
            return lcb >= record_txid;
        }

        false
    }

    /// collect water mark for safe consolidation, currently only [`WmkInfo::wmk_of_old`] is used
    pub fn collect_wmk(&self, ctx: &Context) {
        // 1/n probability, balance overhead
        let groups = ctx.groups();

        if rand_range(0..groups.len()) != 0 {
            return;
        }

        let mut oldest_tx = u64::MAX;

        for g in groups.iter() {
            if let Some(min_tx) = g.active_txns.min_txid() {
                oldest_tx = min(min_tx, oldest_tx);
            }
        }

        let mut g_old = u64::MAX;

        for w in groups.iter() {
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
        log::debug!("wmk_oldest_tx {} ", self.wmk_oldest_tx.load(Relaxed),);
        for i in 0..self.cached.len() {
            let (s, c) = self.cached[i].load();
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
    pub fn new(groups: usize) -> Self {
        Self {
            log: RwLock::new(Vec::new()),
            cap: groups + 1, // plus 1 for read-only txn
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

    pub fn compact(&self, ctx: &Context, _this_group: u8) {
        if self.log.read().len() < self.cap {
            return;
        }
        let log_read = self.log.read();
        if log_read.len() < self.cap {
            return;
        }
        let mut set = HashSet::new();

        set.insert(log_read[log_read.len() - 1]);
        // at least keep the oldest read-only txn's LCB
        if let Some(view) = ctx.oldest_view_txid()
            && let Some(c) = Self::lcb_impl(&log_read, view)
        {
            set.insert(log_read[c]);
        }

        for w in ctx.groups().iter() {
            if w.active_txns.is_empty() {
                continue;
            }
            w.active_txns.for_each_txid(|txid| {
                if let Some(c) = Self::lcb_impl(&log_read, txid) {
                    set.insert(log_read[c]);
                }
            });
        }
        drop(log_read);

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
