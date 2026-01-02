use dashmap::{DashMap, Entry};
use std::collections::HashSet;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};
use std::sync::atomic::{AtomicIsize, AtomicU32};

use crate::utils::{ROOT_PID, rand_range};

#[derive(PartialEq, Eq, Copy, Clone, PartialOrd, Ord, Debug)]
#[repr(u32)]
pub(crate) enum CacheState {
    Hot = 3,
    Warm = 2,
    Cool = 1,
}

impl From<u32> for CacheState {
    fn from(value: u32) -> Self {
        unsafe { std::mem::transmute::<u32, CacheState>(value) }
    }
}

struct CacheItem {
    state: AtomicU32,
    size: isize,
}

impl CacheItem {
    fn warm(&self) {
        let cur = self.state.load(Relaxed);
        let _ = self.state.compare_exchange(
            cur,
            (cur + 1).min(CacheState::Hot as u32),
            Relaxed,
            Relaxed,
        );
    }

    fn cool(&self) -> CacheState {
        let cur = self.state.load(Relaxed);
        self.state
            .compare_exchange(cur, cur.saturating_sub(1), Relaxed, Relaxed)
            .unwrap_or_else(|x| x)
            .into()
    }

    fn status(&self) -> CacheState {
        self.state.load(Relaxed).into()
    }
}
pub(crate) struct NodeCache {
    map: DashMap<u64, CacheItem>,
    used: AtomicIsize,
    cap: isize,
    pct: isize,
}

impl NodeCache {
    pub(crate) fn new(cap: usize, pct: usize) -> Self {
        Self {
            map: DashMap::new(),
            used: AtomicIsize::new(0),
            cap: cap as isize,
            pct: pct as isize,
        }
    }

    pub(crate) fn full(&self) -> bool {
        self.used.load(Acquire) >= self.cap
    }

    pub(crate) fn almost_full(&self) -> bool {
        self.used.load(Acquire) >= self.cap * 100 / 80
    }

    pub(crate) fn cap(&self) -> usize {
        self.cap as usize
    }

    pub(crate) fn put(&self, pid: u64, state: u32, size: isize) {
        let e = self.map.entry(pid);
        match e {
            Entry::Occupied(mut o) => {
                let old = o.get_mut();
                self.used.fetch_add(size - old.size, AcqRel);
                old.size = size;
                old.warm();
            }
            Entry::Vacant(v) => {
                self.used.fetch_add(size, AcqRel);
                v.insert(CacheItem {
                    state: AtomicU32::new(state),
                    size,
                });
            }
        }
    }

    /// it's possible that evictor has removed the pid from cache, but not replace page table yet
    /// and other threads finished replace that cause eviction fail, so we add it back to cache with
    /// Cool state
    pub(crate) fn warm(&self, pid: u64, size: usize) {
        self.put(pid, CacheState::Cool as u32, size as isize);
    }

    pub(crate) fn evict_one(&self, pid: u64) {
        if let Some((_, i)) = self.map.remove(&pid) {
            self.used.fetch_sub(i.size, AcqRel);
        }
    }

    pub(crate) fn evict(&self) -> Vec<u64> {
        let mut cnt = self.pct as usize * self.map.len() / 100;
        let mut pids = HashSet::new();
        let mut iter = self.map.iter();

        while cnt > 0
            && let Some(i) = iter.next()
        {
            let (&pid, v) = (i.key(), i.value());
            if pid != ROOT_PID
                && rand_range(0..100) > self.pct as usize
                && v.cool() > CacheState::Cool
            {
                pids.insert(pid);
            }
            cnt -= 1;
        }
        pids.into_iter().collect()
    }

    pub(crate) fn compact(&self) -> Vec<u64> {
        let mut pids = Vec::new();
        let mut cnt = self.pct as usize * self.map.len() / 100;

        for i in self.map.iter() {
            if cnt == 0 {
                break;
            }
            cnt -= 1;
            if rand_range(0..100) > self.pct as usize && i.status() > CacheState::Cool {
                pids.push(*i.key());
            }
        }
        pids
    }
}
