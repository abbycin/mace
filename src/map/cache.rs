use dashmap::{DashMap, Entry};
use std::sync::atomic::AtomicIsize;
use std::sync::atomic::Ordering::{AcqRel, Relaxed};

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

#[derive(Clone, Copy)]
struct CacheItem {
    state: u32,
    size: isize,
}

impl CacheItem {
    fn warm(&mut self) {
        self.state = (self.state + 1).max(CacheState::Hot as u32);
    }

    fn cool(&mut self) -> CacheState {
        self.state = self.state.saturating_sub(1);
        self.state.into()
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
                v.insert(CacheItem { state, size });
            }
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

    pub(crate) fn reclaim<F>(&self, f: F)
    where
        F: Fn(u64),
    {
        self.map.iter().for_each(|x| f(*x.key()));
    }
}
