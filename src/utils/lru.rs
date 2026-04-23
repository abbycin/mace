use parking_lot::{Mutex, MutexGuard};
use rustc_hash::FxHasher;
use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hash},
    ops::Deref,
    ptr::{self},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use super::spooky::{spooky_hash, spooky_hash_pair};

type FastHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

fn init_head<K, V>() -> *mut Node<K, V> {
    let p = Box::into_raw(Box::new(Node::default()));
    unsafe {
        (*p).next = p;
        (*p).prev = p;
    }
    p
}

pub(crate) struct Node<K, V> {
    key: Option<K>,
    val: Option<V>,
    prev: *mut Node<K, V>,
    next: *mut Node<K, V>,
}

unsafe impl<K, V> Send for Node<K, V> {}
unsafe impl<K, V> Sync for Node<K, V> {}

impl<K, V> Node<K, V> {
    fn new(k: K, v: V) -> Self {
        Self {
            key: Some(k),
            val: Some(v),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }

    fn set_val(&mut self, v: V) {
        self.val.replace(v);
    }

    fn push_back(head: *mut Self, other: *mut Self) {
        unsafe {
            (*other).next = (*head).next;
            (*(*head).next).prev = other;
            (*other).prev = head;
            (*head).next = other;
        }
    }

    fn remove(other: *mut Self) {
        unsafe {
            let prev = (*other).prev;
            let next = (*other).next;
            (*prev).next = next;
            (*next).prev = prev;
        }
    }

    fn move_back(head: *mut Self, other: *mut Self) {
        Self::remove(other);
        Self::push_back(head, other);
    }

    fn front(head: *mut Self) -> *mut Self {
        unsafe { (*head).prev }
    }
}

impl<K, V> Default for Node<K, V> {
    fn default() -> Self {
        Self {
            key: None,
            val: None,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

pub(crate) struct LruGuard<'a, K, V, N> {
    data: &'a V,
    _guard: MutexGuard<'a, FastHashMap<K, N>>,
}

impl<K, V, N> Deref for LruGuard<'_, K, V, N> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.data
    }
}

pub(crate) struct LruShardGuard<'a, K, V> {
    cap: usize,
    k: K,
    lru: &'a Lru<K, V>,
    map: MutexGuard<'a, FastHashMap<K, *mut Node<K, V>>>,
}

impl<'a, K, V> LruShardGuard<'a, K, V>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn add_if_missing<F, E>(mut self, f: F) -> Result<(), E>
    where
        F: Fn() -> Result<V, E>,
    {
        if !self.map.contains_key(&self.k) {
            let v = f()?;
            self.lru.add_unlocked(&mut self.map, self.cap, self.k, v);
        }
        Ok(())
    }
}

pub(crate) struct Lru<K, V> {
    head: *mut Node<K, V>,
    map: Mutex<FastHashMap<K, *mut Node<K, V>>>,
}

// FxHasher drops RandomState storage, so the non-macOS layout is smaller.
#[cfg(not(target_os = "macos"))]
crate::static_assert!(size_of::<Lru<u32, crate::io::File>>() == 48);

unsafe impl<K, V> Send for Lru<K, V> {}
unsafe impl<K, V> Sync for Lru<K, V> {}

impl<K, V> Lru<K, V>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn new() -> Self {
        Self {
            head: init_head(),
            map: Mutex::new(FastHashMap::default()),
        }
    }

    pub(crate) fn lock_shard<'a>(&'a self, cap: usize, k: K) -> LruShardGuard<'a, K, V> {
        LruShardGuard {
            cap,
            k,
            lru: self,
            map: self.map.lock(),
        }
    }

    pub(crate) fn add(&self, cap: usize, k: K, v: V) {
        let mut map = self.map.lock();
        self.add_unlocked(&mut map, cap, k, v);
    }

    pub(crate) fn add_with_evict(&self, cap: usize, k: K, v: V) -> Option<(K, V)> {
        let mut map = self.map.lock();
        if let Some(e) = map.get(&k) {
            unsafe { (*(*e)).set_val(v) };
            Node::move_back(self.head, *e);
            return None;
        }

        let node = Box::new(Node::new(k.clone(), v));
        let p = Box::into_raw(node);
        map.insert(k, p);
        Node::push_back(self.head, p);

        if map.len() > cap {
            let node = Node::front(self.head);
            unsafe {
                let key = (*node).key.take().unwrap();
                let val = (*node).val.take().unwrap();
                map.remove(&key);
                Node::remove(node);
                let _ = Box::from_raw(node);
                return Some((key, val));
            }
        }
        None
    }

    fn add_unlocked(
        &self,
        map: &mut MutexGuard<'_, FastHashMap<K, *mut Node<K, V>>>,
        cap: usize,
        k: K,
        v: V,
    ) {
        let e = map.get(&k);
        if let Some(e) = e {
            unsafe { (*(*e)).set_val(v) };
            Node::move_back(self.head, *e);
        } else {
            let node = Box::new(Node::new(k.clone(), v));
            let p = Box::into_raw(node);
            map.insert(k, p);
            Node::push_back(self.head, p);
        }

        if map.len() > cap {
            let node = Node::front(self.head);
            unsafe {
                let key = (*node).key.take().unwrap();
                map.remove(&key);
                Node::remove(node);
                let _ = Box::from_raw(node);
            }
        }
    }

    pub(crate) fn get<'a>(&'a self, k: &K) -> Option<LruGuard<'a, K, V, *mut Node<K, V>>> {
        let map = self.map.lock();
        if let Some(x) = map.get(k) {
            Node::move_back(self.head, *x);
            Some(LruGuard {
                data: unsafe { (*(*x)).val.as_ref().unwrap() },
                _guard: map,
            })
        } else {
            None
        }
    }

    pub(crate) fn del(&self, k: &K) {
        let mut map = self.map.lock();
        if let Some(node) = map.remove(k) {
            Node::remove(node);
            unsafe {
                let _ = Box::from_raw(node);
            }
        }
    }
}

impl<K, V> Drop for Lru<K, V> {
    fn drop(&mut self) {
        unsafe {
            let mut p = (*self.head).next;
            while p != self.head {
                let next = (*p).next;
                drop(Box::from_raw(p));
                p = next;
            }
            drop(Box::from_raw(self.head));
        }
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum CachePriority {
    Low,
    High,
}

pub(crate) struct PriorityValue<V> {
    val: V,
    prio: CachePriority,
    weight: usize,
}

impl<V> PriorityValue<V> {
    fn new(val: V, prio: CachePriority, weight: usize) -> Self {
        Self { val, prio, weight }
    }
}

type PriorityNode<V> = Node<u128, PriorityValue<V>>;

struct EvictOutcome {
    prio: usize,
    weight: usize,
}

struct PriorityShard<V> {
    queue: [*mut PriorityNode<V>; 2],
    map: Mutex<FastHashMap<u128, *mut PriorityNode<V>>>,
}

impl<V> PriorityShard<V> {
    fn new() -> Self {
        Self {
            queue: [init_head(), init_head()],
            map: Mutex::new(FastHashMap::default()),
        }
    }

    fn atomic_saturating_sub(x: &AtomicUsize, delta: usize) {
        let mut old = x.load(Ordering::Relaxed);
        loop {
            let new = old.saturating_sub(delta);
            match x.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return,
                Err(cur) => old = cur,
            }
        }
    }

    fn account_add(
        used_bytes: &[AtomicUsize; 2],
        used_entries: &AtomicUsize,
        old_prio: Option<usize>,
        old_weight: usize,
        new_prio: usize,
        new_weight: usize,
    ) {
        if let Some(old_prio) = old_prio {
            Self::atomic_saturating_sub(&used_bytes[old_prio], old_weight);
        } else {
            used_entries.fetch_add(1, Ordering::AcqRel);
        }
        used_bytes[new_prio].fetch_add(new_weight, Ordering::AcqRel);
    }

    fn add_and_account(
        &self,
        prio: CachePriority,
        weight: usize,
        key: u128,
        val: V,
        used_bytes: &[AtomicUsize; 2],
        used_entries: &AtomicUsize,
    ) {
        let mut lk = self.map.lock();
        let weight = weight.max(1);
        let new_prio = prio as usize;
        let new_head = self.queue[new_prio];

        if let Some(node) = lk.get(&key).copied() {
            let (old_prio, old_weight) = unsafe {
                let old = (*node).val.as_ref().expect("priority lru value must exist");
                (old.prio as usize, old.weight)
            };
            unsafe { (*node).set_val(PriorityValue::new(val, prio, weight)) };
            if old_prio == new_prio {
                Node::move_back(new_head, node);
            } else {
                Node::remove(node);
                Node::push_back(new_head, node);
            }
            Self::account_add(
                used_bytes,
                used_entries,
                Some(old_prio),
                old_weight,
                new_prio,
                weight,
            );
        } else {
            let node = Box::new(Node::new(key, PriorityValue::new(val, prio, weight)));
            let ptr = Box::into_raw(node);
            lk.insert(key, ptr);
            Node::push_back(new_head, ptr);
            Self::account_add(used_bytes, used_entries, None, 0, new_prio, weight);
        }
    }

    fn pop_one_locked(
        &self,
        lk: &mut MutexGuard<'_, FastHashMap<u128, *mut PriorityNode<V>>>,
        prio: usize,
    ) -> Option<EvictOutcome> {
        let head = self.queue[prio];
        let node = Node::front(head);
        if node == head {
            return None;
        }
        unsafe {
            let key = (*node).key.take().expect("priority lru key must exist");
            let weight = (*node).val.as_ref().map(|x| x.weight.max(1)).unwrap_or(1);
            lk.remove(&key);
            Node::remove(node);
            let _ = Box::from_raw(node);
            Some(EvictOutcome { prio, weight })
        }
    }

    fn evict_one(&self, prefer: Option<CachePriority>) -> Option<EvictOutcome> {
        let mut lk = self.map.lock();
        if let Some(prio) = prefer {
            self.pop_one_locked(&mut lk, prio as usize)
        } else {
            self.pop_one_locked(&mut lk, CachePriority::Low as usize)
                .or_else(|| self.pop_one_locked(&mut lk, CachePriority::High as usize))
        }
    }

    fn get<'a>(&'a self, key: &u128) -> Option<LruGuard<'a, u128, V, *mut PriorityNode<V>>> {
        let lk = self.map.lock();
        if let Some(node) = lk.get(key).copied() {
            let val = unsafe { (*node).val.as_ref().expect("priority lru value must exist") };
            Node::move_back(self.queue[val.prio as usize], node);
            Some(LruGuard {
                data: &val.val,
                _guard: lk,
            })
        } else {
            None
        }
    }
}

impl<V> Drop for PriorityShard<V> {
    fn drop(&mut self) {
        unsafe {
            for head in self.queue {
                let mut p = (*head).next;
                while p != head {
                    let next = (*p).next;
                    drop(Box::from_raw(p));
                    p = next;
                }
                drop(Box::from_raw(head));
            }
        }
    }
}

pub const LRU_SHARD: usize = 32;
const LRU_SHARD_MASK: usize = LRU_SHARD - 1;

pub(crate) struct ShardLru<V> {
    shard: [Lru<u64, V>; LRU_SHARD],
    cap: usize,
}

impl<V> ShardLru<V> {
    pub(crate) fn new(cap: usize) -> Self {
        let cap = cap / LRU_SHARD;
        Self {
            shard: std::array::from_fn(|_| Lru::new()),
            cap,
        }
    }

    #[inline(always)]
    fn get_shard(k: u64) -> usize {
        spooky_hash(k) as usize & LRU_SHARD_MASK
    }

    #[allow(unused)]
    pub(crate) fn add(&self, k: u64, v: V) {
        self.shard[Self::get_shard(k)].add(self.cap, k, v);
    }

    pub(crate) fn get<'a>(&'a self, k: u64) -> Option<LruGuard<'a, u64, V, *mut Node<u64, V>>> {
        self.shard[Self::get_shard(k)].get(&k)
    }

    pub(crate) fn del(&self, k: u64) {
        self.shard[Self::get_shard(k)].del(&k);
    }

    pub(crate) fn lock_shard<'a>(&'a self, k: u64) -> LruShardGuard<'a, u64, V> {
        self.shard[Self::get_shard(k)].lock_shard(self.cap, k)
    }
}

pub(crate) struct ShardPriorityLru<V> {
    shard: [PriorityShard<V>; LRU_SHARD],
    used_bytes: [AtomicUsize; 2],
    used_entries: AtomicUsize,
    byte_cap: [usize; 2],
    entry_cap: usize,
    cursor: AtomicUsize,
    trim_running: AtomicBool,
    trim_requested: AtomicBool,
}

impl<V> ShardPriorityLru<V> {
    const MAX_TRIM_EVICT_PER_ROUND: usize = 16;

    pub(crate) fn new(cap: usize, high_ratio: usize, max_entries: usize) -> Self {
        let high_ratio = high_ratio.min(100);
        let hi_cap = cap.saturating_mul(high_ratio) / 100;
        let lo_cap = cap.saturating_sub(hi_cap);
        Self {
            shard: std::array::from_fn(|_| PriorityShard::new()),
            used_bytes: std::array::from_fn(|_| AtomicUsize::new(0)),
            used_entries: AtomicUsize::new(0),
            byte_cap: [lo_cap, hi_cap],
            entry_cap: max_entries,
            cursor: AtomicUsize::new(0),
            trim_running: AtomicBool::new(false),
            trim_requested: AtomicBool::new(false),
        }
    }

    #[inline(always)]
    fn get_shard(k: u128) -> usize {
        let hi = (k >> 64) as u64;
        let lo = k as u64;
        spooky_hash_pair(hi, lo) as usize & LRU_SHARD_MASK
    }

    #[inline]
    fn over_bytes(&self, prio: CachePriority) -> bool {
        let idx = prio as usize;
        self.used_bytes[idx].load(Ordering::Acquire) > self.byte_cap[idx]
    }

    #[inline]
    fn over_entry_limit(&self) -> bool {
        self.entry_cap != 0 && self.used_entries.load(Ordering::Acquire) > self.entry_cap
    }

    #[inline]
    fn maybe_over_limit(&self) -> bool {
        self.over_bytes(CachePriority::Low)
            || self.over_bytes(CachePriority::High)
            || self.over_entry_limit()
    }

    fn account_eviction(&self, prio: usize, weight: usize) {
        PriorityShard::<V>::atomic_saturating_sub(&self.used_bytes[prio], weight);
        PriorityShard::<V>::atomic_saturating_sub(&self.used_entries, 1);
    }

    fn evict_round_robin(&self, prefer: Option<CachePriority>) -> bool {
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) & LRU_SHARD_MASK;
        for step in 0..LRU_SHARD {
            let idx = (start + step) & LRU_SHARD_MASK;
            if let Some(evicted) = self.shard[idx].evict_one(prefer) {
                self.account_eviction(evicted.prio, evicted.weight);
                self.cursor
                    .store((idx + 1) & LRU_SHARD_MASK, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    fn trim(&self) {
        let mut evicted = 0;
        while evicted < Self::MAX_TRIM_EVICT_PER_ROUND && self.maybe_over_limit() {
            let prefer = if self.over_bytes(CachePriority::Low) {
                Some(CachePriority::Low)
            } else if self.over_bytes(CachePriority::High) {
                Some(CachePriority::High)
            } else {
                None
            };
            let ok = self.evict_round_robin(prefer);
            if !ok {
                break;
            }
            evicted += 1;
        }
    }

    fn try_trim(&self) {
        if self
            .trim_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        self.trim_requested.store(false, Ordering::Release);
        self.trim();
        self.trim_running.store(false, Ordering::Release);

        // One call processes at most MAX_TRIM_EVICT_PER_ROUND evictions.
        // If pressure remains, keep the request bit set for later callers.
        if self.trim_requested.swap(false, Ordering::AcqRel) || self.maybe_over_limit() {
            self.trim_requested.store(true, Ordering::Release);
        }
    }

    pub(crate) fn add(&self, prio: CachePriority, k: u128, weight: usize, v: V) {
        let shard_idx = Self::get_shard(k);
        self.shard[shard_idx].add_and_account(
            prio,
            weight,
            k,
            v,
            &self.used_bytes,
            &self.used_entries,
        );
        if self.maybe_over_limit() {
            self.trim_requested.store(true, Ordering::Release);
            self.try_trim();
        }
    }

    pub(crate) fn get<'a>(
        &'a self,
        k: u128,
    ) -> Option<LruGuard<'a, u128, V, *mut Node<u128, PriorityValue<V>>>> {
        self.shard[Self::get_shard(k)].get(&k)
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use crate::utils::lru::{CachePriority, Lru, ShardPriorityLru};

    #[test]
    fn lru() {
        let m = Lru::new();
        let cap = 3;

        m.add(cap, 1, 1);
        m.add(cap, 1, 2);

        assert_eq!(m.get(&1).unwrap().deref(), &2);

        m.add(cap, 2, 2);
        m.add(cap, 3, 3);
        m.add(cap, 4, 4);

        assert!(m.get(&1).is_none());
        assert_eq!(m.get(&2).unwrap().deref(), &2);
        assert_eq!(m.get(&3).unwrap().deref(), &3);
        assert_eq!(m.get(&4).unwrap().deref(), &4);
    }

    #[test]
    fn priority_lru_keeps_distinct_u128_keys() {
        let m = ShardPriorityLru::new(64, 50, 0);
        let k1 = (1_u128 << 64) | 7;
        let k2 = (2_u128 << 64) | 7;

        m.add(CachePriority::High, k1, 1, 11_u8);
        m.add(CachePriority::High, k2, 1, 22_u8);

        assert_eq!(m.get(k1).unwrap().deref(), &11);
        assert_eq!(m.get(k2).unwrap().deref(), &22);
    }
}
