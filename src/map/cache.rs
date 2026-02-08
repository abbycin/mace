use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering::{AcqRel, Relaxed};
use std::sync::atomic::{AtomicIsize, AtomicU32, AtomicU64, AtomicUsize};

#[derive(PartialEq, Eq, Copy, Clone, PartialOrd, Ord, Debug)]
#[repr(u32)]
pub(crate) enum CacheState {
    Hot = 3,
    Warm = 2,
    Cold = 1,
}

impl CacheState {
    fn cool(self) -> CacheState {
        match self {
            CacheState::Hot => CacheState::Warm,
            CacheState::Warm => CacheState::Cold,
            CacheState::Cold => CacheState::Cold,
        }
    }

    fn warm(self) -> CacheState {
        match self {
            CacheState::Cold => CacheState::Warm,
            CacheState::Warm => CacheState::Hot,
            CacheState::Hot => CacheState::Hot,
        }
    }
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
    fn warm(&self) -> CacheState {
        let mut cur = self.state.load(Relaxed);
        loop {
            let next = CacheState::from(cur).warm() as u32;
            match self.state.compare_exchange(cur, next, Relaxed, Relaxed) {
                Ok(_) => return next.into(),
                Err(v) => cur = v,
            }
        }
    }

    fn cool(&self) -> CacheState {
        let mut cur = self.state.load(Relaxed);
        loop {
            let next = CacheState::from(cur).cool() as u32;
            match self.state.compare_exchange(cur, next, Relaxed, Relaxed) {
                Ok(_) => return next.into(),
                Err(v) => cur = v,
            }
        }
    }
}

pub(crate) struct NodeCache {
    map: DashMap<u64, CacheItem>,
    used: Arc<AtomicIsize>,
}

impl NodeCache {
    pub(crate) fn new(used: Arc<AtomicIsize>) -> Self {
        Self {
            map: DashMap::new(),
            used,
        }
    }

    pub(crate) fn insert(&self, pid: u64, state: CacheState, size: isize) {
        let e = self.map.entry(pid);
        match e {
            dashmap::Entry::Occupied(mut o) => {
                let old = o.get_mut();
                self.used.fetch_add(size - old.size, AcqRel);
                old.size = size;
                old.state.store(state as u32, Relaxed);
            }
            dashmap::Entry::Vacant(v) => {
                self.used.fetch_add(size, AcqRel);
                v.insert(CacheItem {
                    state: AtomicU32::new(state as u32),
                    size,
                });
            }
        }
    }

    pub(crate) fn touch(&self, pid: u64, size: isize) {
        let e = self.map.entry(pid);
        match e {
            dashmap::Entry::Occupied(mut o) => {
                let old = o.get_mut();
                self.used.fetch_add(size - old.size, AcqRel);
                old.size = size;
                old.warm();
            }
            dashmap::Entry::Vacant(v) => {
                self.used.fetch_add(size, AcqRel);
                v.insert(CacheItem {
                    state: AtomicU32::new(CacheState::Warm as u32),
                    size,
                });
            }
        }
    }

    pub(crate) fn cool(&self, pid: u64) -> Option<CacheState> {
        let e = self.map.get(&pid)?;
        Some(e.cool())
    }

    pub(crate) fn state(&self, pid: u64) -> Option<CacheState> {
        let e = self.map.get(&pid)?;
        Some(CacheState::from(e.state.load(Relaxed)))
    }

    pub(crate) fn evict(&self, pid: u64) -> bool {
        if let Some((_, i)) = self.map.remove(&pid) {
            self.used.fetch_sub(i.size, AcqRel);
            true
        } else {
            false
        }
    }

    pub(crate) fn update_size(&self, pid: u64, size: isize) -> bool {
        if let Some(mut item) = self.map.get_mut(&pid) {
            self.used.fetch_add(size - item.size, AcqRel);
            item.size = size;
            true
        } else {
            false
        }
    }

    pub(crate) fn used(&self) -> isize {
        self.used.load(Relaxed)
    }
}

pub(crate) struct CandidateRing {
    buf: Vec<AtomicU64>,
    idx: AtomicUsize,
}

pub(crate) const CANDIDATE_RING_SIZE: usize = 1024;
pub(crate) const CANDIDATE_SAMPLE_RATE: u64 = 4;
pub(crate) const EVICT_SAMPLE_MAX_ROUNDS: usize = 4;

impl CandidateRing {
    pub(crate) fn new(cap: usize) -> Self {
        let mut buf = Vec::with_capacity(cap);
        for _ in 0..cap {
            buf.push(AtomicU64::new(0));
        }
        Self {
            buf,
            idx: AtomicUsize::new(0),
        }
    }

    pub(crate) fn push(&self, pid: u64) {
        let len = self.buf.len();
        if len == 0 {
            return;
        }
        let pos = self.idx.fetch_add(1, Relaxed) % len;
        self.buf[pos].store(pid + 1, Relaxed);
    }

    pub(crate) fn snapshot(&self) -> Vec<u64> {
        let mut out = Vec::new();
        for slot in &self.buf {
            let v = slot.load(Relaxed);
            if v > 0 {
                out.push(v - 1);
            }
        }
        out
    }
}
