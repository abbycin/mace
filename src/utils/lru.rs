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

impl CachePriority {
    fn other(self) -> Self {
        match self {
            CachePriority::Low => CachePriority::High,
            CachePriority::High => CachePriority::Low,
        }
    }
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
    queue: *mut PriorityNode<V>,
    map: Mutex<FastHashMap<u128, *mut PriorityNode<V>>>,
}

impl<V> PriorityShard<V> {
    fn new() -> Self {
        Self {
            queue: init_head(),
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
        used_total_bytes: &AtomicUsize,
        used_entries: &AtomicUsize,
        old_prio: Option<usize>,
        old_weight: usize,
        new_prio: usize,
        new_weight: usize,
    ) {
        if let Some(old_prio) = old_prio {
            Self::atomic_saturating_sub(&used_bytes[old_prio], old_weight);
            Self::atomic_saturating_sub(used_total_bytes, old_weight);
        } else {
            used_entries.fetch_add(1, Ordering::AcqRel);
        }
        used_bytes[new_prio].fetch_add(new_weight, Ordering::AcqRel);
        used_total_bytes.fetch_add(new_weight, Ordering::AcqRel);
    }

    #[allow(clippy::too_many_arguments)]
    fn add_and_account(
        &self,
        prio: CachePriority,
        weight: usize,
        key: u128,
        val: V,
        used_bytes: &[AtomicUsize; 2],
        used_total_bytes: &AtomicUsize,
        used_entries: &AtomicUsize,
    ) {
        let mut lk = self.map.lock();
        let weight = weight.max(1);
        let new_prio = prio as usize;

        if let Some(node) = lk.get(&key).copied() {
            let (old_prio, old_weight) = unsafe {
                let old = (*node).val.as_ref().expect("priority lru value must exist");
                (old.prio as usize, old.weight)
            };
            unsafe { (*node).set_val(PriorityValue::new(val, prio, weight)) };
            Node::move_back(self.queue, node);
            Self::account_add(
                used_bytes,
                used_total_bytes,
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
            Node::push_back(self.queue, ptr);
            Self::account_add(
                used_bytes,
                used_total_bytes,
                used_entries,
                None,
                0,
                new_prio,
                weight,
            );
        }
    }

    fn pop_one_locked(
        &self,
        lk: &mut MutexGuard<'_, FastHashMap<u128, *mut PriorityNode<V>>>,
        prio: Option<usize>,
    ) -> Option<EvictOutcome> {
        let mut node = Node::front(self.queue);
        while node != self.queue {
            let cur = node;
            node = unsafe { (*cur).prev };
            let cur_prio = unsafe {
                (*cur)
                    .val
                    .as_ref()
                    .expect("priority lru value must exist")
                    .prio as usize
            };
            if prio.is_some_and(|idx| idx != cur_prio) {
                continue;
            }
            unsafe {
                let key = (*cur).key.take().expect("priority lru key must exist");
                let val = (*cur).val.take().expect("priority lru value must exist");
                lk.remove(&key);
                Node::remove(cur);
                let _ = Box::from_raw(cur);
                return Some(EvictOutcome {
                    prio: val.prio as usize,
                    weight: val.weight,
                });
            }
        }
        None
    }

    fn evict_one(&self, prefer: Option<CachePriority>) -> Option<EvictOutcome> {
        let mut lk = self.map.lock();
        self.pop_one_locked(&mut lk, prefer.map(|x| x as usize))
    }

    fn get<'a>(&'a self, key: &u128) -> Option<LruGuard<'a, u128, V, *mut PriorityNode<V>>> {
        let lk = self.map.lock();
        if let Some(node) = lk.get(key).copied() {
            let val = unsafe { (*node).val.as_ref().expect("priority lru value must exist") };
            Node::move_back(self.queue, node);
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
            let mut p = (*self.queue).next;
            while p != self.queue {
                let next = (*p).next;
                drop(Box::from_raw(p));
                p = next;
            }
            drop(Box::from_raw(self.queue));
        }
    }
}

pub(crate) struct ShardLru<V, const LRU_SHARD: usize = 8> {
    shard: [Lru<u64, V>; LRU_SHARD],
    cap: usize,
}

impl<V, const LRU_SHARD: usize> ShardLru<V, LRU_SHARD> {
    const LRU_SHARD_MASK: usize = LRU_SHARD - 1;

    pub(crate) fn new(cap: usize) -> Self {
        assert!(LRU_SHARD > 0);
        assert!(LRU_SHARD.is_power_of_two());
        let cap = cap / LRU_SHARD;
        Self {
            shard: std::array::from_fn(|_| Lru::new()),
            cap,
        }
    }

    #[inline(always)]
    fn get_shard(k: u64) -> usize {
        spooky_hash(k) as usize & Self::LRU_SHARD_MASK
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

pub(crate) struct ShardPriorityLru<V, const LRU_SHARD: usize = 32> {
    shard: [PriorityShard<V>; LRU_SHARD],
    used_bytes: [AtomicUsize; 2],
    used_total_bytes: AtomicUsize,
    used_entries: AtomicUsize,
    byte_cap: usize,
    cursor: AtomicUsize,
    evict_bias: AtomicUsize,
    trim_running: AtomicBool,
    trim_requested: AtomicBool,
}

impl<V, const LRU_SHARD: usize> ShardPriorityLru<V, LRU_SHARD> {
    const MAX_TRIM_EVICT_PER_ROUND: usize = 16;
    const LRU_SHARD_MASK: usize = LRU_SHARD - 1;

    pub(crate) fn new(cap: usize) -> Self {
        assert!(LRU_SHARD > 0);
        assert!(LRU_SHARD.is_power_of_two());
        Self {
            shard: std::array::from_fn(|_| PriorityShard::new()),
            used_bytes: std::array::from_fn(|_| AtomicUsize::new(0)),
            used_total_bytes: AtomicUsize::new(0),
            used_entries: AtomicUsize::new(0),
            byte_cap: cap,
            cursor: AtomicUsize::new(0),
            evict_bias: AtomicUsize::new(0),
            trim_running: AtomicBool::new(false),
            trim_requested: AtomicBool::new(false),
        }
    }

    #[inline(always)]
    fn get_shard(k: u128) -> usize {
        let hi = (k >> 64) as u64;
        let lo = k as u64;
        spooky_hash_pair(hi, lo) as usize & Self::LRU_SHARD_MASK
    }

    #[inline]
    fn maybe_over_limit(&self) -> bool {
        self.used_total_bytes.load(Ordering::Acquire) > self.byte_cap
    }

    fn account_eviction(&self, prio: usize, weight: usize) {
        PriorityShard::<V>::atomic_saturating_sub(&self.used_bytes[prio], weight);
        PriorityShard::<V>::atomic_saturating_sub(&self.used_total_bytes, weight);
        PriorityShard::<V>::atomic_saturating_sub(&self.used_entries, 1);
    }

    fn evict_round_robin(&self, prefer: CachePriority) -> bool {
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) & Self::LRU_SHARD_MASK;
        for step in 0..LRU_SHARD {
            let idx = (start + step) & Self::LRU_SHARD_MASK;
            if let Some(evicted) = self.shard[idx].evict_one(Some(prefer)) {
                self.account_eviction(evicted.prio, evicted.weight);
                self.cursor
                    .store((idx + 1) & Self::LRU_SHARD_MASK, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    fn next_evict_prio(&self) -> Option<CachePriority> {
        let low = self.used_bytes[CachePriority::Low as usize].load(Ordering::Acquire) != 0;
        let high = self.used_bytes[CachePriority::High as usize].load(Ordering::Acquire) != 0;
        match (low, high) {
            (false, false) => None,
            (true, false) => Some(CachePriority::Low),
            (false, true) => Some(CachePriority::High),
            (true, true) => {
                let bias = self.evict_bias.fetch_add(1, Ordering::Relaxed) % 3;
                if bias < 2 {
                    Some(CachePriority::Low)
                } else {
                    Some(CachePriority::High)
                }
            }
        }
    }

    fn trim(&self) {
        let mut evicted = 0;
        while evicted < Self::MAX_TRIM_EVICT_PER_ROUND && self.maybe_over_limit() {
            let Some(prefer) = self.next_evict_prio() else {
                break;
            };
            let ok = self.evict_round_robin(prefer) || self.evict_round_robin(prefer.other());
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
            &self.used_total_bytes,
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
        let m = ShardPriorityLru::<_, 32>::new(64);
        let k1 = (1_u128 << 64) | 7;
        let k2 = (2_u128 << 64) | 7;

        m.add(CachePriority::High, k1, 1, 11_u8);
        m.add(CachePriority::High, k2, 1, 22_u8);

        assert_eq!(m.get(k1).unwrap().deref(), &11);
        assert_eq!(m.get(k2).unwrap().deref(), &22);
    }

    #[test]
    fn priority_lru_prefers_low_victims() {
        let m = ShardPriorityLru::<_, 1>::new(3);
        let k1 = 1_u128;
        let k2 = 2_u128;
        let k3 = 3_u128;
        let k4 = 4_u128;

        m.add(CachePriority::High, k1, 1, 11_u8);
        m.add(CachePriority::Low, k2, 1, 22_u8);
        m.add(CachePriority::Low, k3, 1, 33_u8);
        m.add(CachePriority::High, k4, 1, 44_u8);

        assert_eq!(m.get(k1).unwrap().deref(), &11);
        assert!(m.get(k2).is_none());
        assert_eq!(m.get(k3).unwrap().deref(), &33);
        assert_eq!(m.get(k4).unwrap().deref(), &44);
    }
}
