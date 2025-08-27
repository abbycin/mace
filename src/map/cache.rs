use dashmap::{DashMap, Entry};
use std::sync::atomic::Ordering::{AcqRel, Relaxed};
use std::sync::atomic::{AtomicIsize, AtomicU32};

use crate::utils::rand_range;

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

    fn cool(&mut self) -> CacheState {
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
        self.used.load(Relaxed) >= self.cap
    }

    pub(crate) fn almost_full(&self) -> bool {
        self.used.load(Relaxed) >= self.cap * 100 / 80
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

    pub(crate) fn warm(&self, pid: u64) {
        if let Some(v) = self.map.get(&pid) {
            v.warm();
        }
    }

    pub(crate) fn evict_one(&self, pid: u64) {
        if let Some((_, i)) = self.map.remove(&pid) {
            self.used.fetch_sub(i.size, AcqRel);
        }
    }

    pub(crate) fn evict(&self) -> Vec<u64> {
        let tgt = self.pct * self.cap / 100;
        let mut pids = Vec::new();

        while self.cap - self.used.load(Relaxed) < tgt {
            self.map.retain(|&pid, v| {
                if rand_range(0..100) > self.pct as usize || v.cool() > CacheState::Cool {
                    return true;
                }
                self.used.fetch_sub(v.size, Relaxed);
                pids.push(pid);
                false
            });
        }
        pids
    }

    pub(crate) fn compact(&self) -> Vec<u64> {
        let mut pids = Vec::new();
        let mut count = self.pct * self.cap / 100;

        for i in self.map.iter() {
            if count == 0 {
                break;
            }
            count -= 1;
            if rand_range(0..100) > self.pct as usize && i.status() > CacheState::Cool {
                pids.push(*i.key());
            }
        }
        pids
    }

    pub(crate) fn reclaim<F>(&self, mut f: F)
    where
        F: FnMut(u64),
    {
        self.map.iter().for_each(|x| f(*x.key()));
    }
}
