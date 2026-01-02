use parking_lot::{Mutex, MutexGuard};
use std::{
    cell::Cell,
    collections::HashMap,
    hash::Hash,
    ops::Deref,
    ptr::{self},
};

use super::spooky::spooky_hash;

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
    _guard: MutexGuard<'a, HashMap<K, N>>,
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
    map: MutexGuard<'a, HashMap<K, *mut Node<K, V>>>,
}

impl<'a, K, V> LruShardGuard<'a, K, V>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn add_if_missing<F>(mut self, f: F) -> Option<()>
    where
        F: Fn() -> Option<V>,
    {
        if !self.map.contains_key(&self.k) {
            let v = f()?;
            self.lru.add_unlocked(&mut self.map, self.cap, self.k, v);
        }
        Some(())
    }
}

pub(crate) struct Lru<K, V> {
    head: *mut Node<K, V>,
    map: Mutex<HashMap<K, *mut Node<K, V>>>,
}

// it's larger than 64 on macOS
#[cfg(not(target_os = "macos"))]
crate::static_assert!(size_of::<Lru<u32, crate::io::File>>() == 64);

unsafe impl<K, V> Send for Lru<K, V> {}
unsafe impl<K, V> Sync for Lru<K, V> {}

impl<K, V> Lru<K, V>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn new() -> Self {
        Self {
            head: init_head(),
            map: Mutex::new(HashMap::new()),
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

    fn add_unlocked(
        &self,
        map: &mut MutexGuard<'_, HashMap<K, *mut Node<K, V>>>,
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
            (*self.head).next = ptr::null_mut();
            while !p.is_null() {
                let next = (*p).next;
                drop(Box::from_raw(p));
                p = next;
            }
        }
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum CachePriority {
    Low,
    High,
}

struct Cap {
    curr: Cell<usize>,
    limit: usize,
}

impl Cap {
    const fn new(limit: usize) -> Self {
        Self {
            curr: Cell::new(0),
            limit,
        }
    }

    fn inc(&self) {
        self.curr.set(self.curr.get() + 1);
    }

    fn dec(&self) {
        self.curr.set(self.curr.get() - 1);
    }

    fn is_full(&self) -> bool {
        self.curr.get() >= self.limit
    }
}

struct PriorityLru<K, V> {
    cap: [Cap; 2],
    queue: [*mut Node<K, (V, CachePriority)>; 2],
    map: Mutex<HashMap<K, *mut Node<K, (V, CachePriority)>>>,
}

impl<K, V> PriorityLru<K, V>
where
    K: Eq + Hash + Clone,
{
    fn new(cap: usize, high_ratio: usize) -> Self {
        let hi_cap = cap * high_ratio / 100;
        let lo_cap = cap - hi_cap;

        Self {
            cap: [Cap::new(lo_cap), Cap::new(hi_cap)],
            queue: [init_head(), init_head()],
            map: Mutex::new(HashMap::new()),
        }
    }

    fn add(&self, prio: CachePriority, k: K, v: V) {
        let mut lk = self.map.lock();
        let index = prio as usize;
        let head = self.queue[index];
        let cap = &self.cap[index];

        if let Some(e) = lk.get(&k) {
            unsafe { (*(*e)).set_val((v, prio)) };
            Node::move_back(head, *e);
        } else {
            let node = Box::new(Node::new(k.clone(), (v, prio)));
            let p = Box::into_raw(node);
            lk.insert(k, p);
            Node::push_back(head, p);
            cap.inc();
        }

        if cap.is_full() {
            let node = Node::front(head);
            unsafe {
                let key = (*node).key.take().unwrap();
                lk.remove(&key);
                Node::remove(node);
                let _ = Box::from_raw(node);
            }
            cap.dec();
        }
    }

    fn get<'a>(&'a self, k: &K) -> Option<LruGuard<'a, K, V, *mut Node<K, (V, CachePriority)>>> {
        let lk = self.map.lock();
        if let Some(e) = lk.get(k) {
            let v = unsafe { (*(*e)).val.as_ref().unwrap() };
            Node::move_back(self.queue[v.1 as usize], *e);
            Some(LruGuard {
                data: &v.0,
                _guard: lk,
            })
        } else {
            None
        }
    }
}

impl<K, V> Drop for PriorityLru<K, V> {
    fn drop(&mut self) {
        unsafe {
            for head in self.queue {
                let mut p = (*head).next;
                (*head).next = ptr::null_mut();
                while !p.is_null() {
                    let next = (*p).next;
                    drop(Box::from_raw(p));
                    p = next;
                }
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
    shard: [PriorityLru<u64, V>; LRU_SHARD],
}

impl<V> ShardPriorityLru<V> {
    pub(crate) fn new(cap: usize, high_ratio: usize) -> Self {
        let shard_cap = cap / LRU_SHARD;
        Self {
            shard: std::array::from_fn(|_| PriorityLru::new(shard_cap, high_ratio)),
        }
    }

    #[inline(always)]
    fn get_shard(k: u64) -> usize {
        spooky_hash(k) as usize & LRU_SHARD_MASK
    }

    pub(crate) fn add(&self, prio: CachePriority, k: u64, v: V) {
        self.shard[Self::get_shard(k)].add(prio, k, v);
    }

    pub(crate) fn get<'a>(
        &'a self,
        k: u64,
    ) -> Option<LruGuard<'a, u64, V, *mut Node<u64, (V, CachePriority)>>> {
        self.shard[Self::get_shard(k)].get(&k)
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use crate::utils::lru::Lru;

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
}
