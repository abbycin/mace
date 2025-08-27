use std::{
    collections::HashMap,
    hash::Hash,
    ops::Deref,
    ptr::{self},
    sync::{Mutex, MutexGuard},
};

use super::spooky::spooky_hash;

struct Node<K, V> {
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
pub(crate) struct LruGuard<'a, K, V> {
    data: &'a V,
    _guard: MutexGuard<'a, HashMap<K, *mut Node<K, V>>>,
}

impl<K, V> Deref for LruGuard<'_, K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.data
    }
}

#[repr(align(64))]
pub(crate) struct LruInner<K, V> {
    head: *mut Node<K, V>,
    map: Mutex<HashMap<K, *mut Node<K, V>>>,
}

// it's larger than 64 on macOS
#[cfg(not(target_os = "macos"))]
crate::static_assert!(size_of::<LruInner<u32, io::File>>() == 64);

unsafe impl<K, V> Send for LruInner<K, V> {}
unsafe impl<K, V> Sync for LruInner<K, V> {}

impl<K, V> LruInner<K, V>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn new() -> Self {
        let p = Box::into_raw(Box::new(Node::default()));
        unsafe {
            (*p).next = p;
            (*p).prev = p;
        }
        Self {
            head: p,
            map: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn add(&self, cap: usize, k: K, v: V) {
        let mut map = self.map.lock().unwrap();
        let e = map.get(&k);
        if let Some(e) = e {
            unsafe { (*(*e)).set_val(v) };
            self.move_back(*e);
        } else {
            let node = Box::new(Node::new(k.clone(), v));
            let p = Box::into_raw(node);
            map.insert(k, p);
            self.push_back(p);
        }

        if map.len() > cap {
            let node = self.front();
            unsafe {
                let key = (*node).key.take().unwrap();
                map.remove(&key);
                self.remove_node(node);
                let _ = Box::from_raw(node);
            }
        }
    }

    pub(crate) fn get<'a>(&'a self, k: &K) -> Option<LruGuard<'a, K, V>> {
        let map = self.map.lock().unwrap();
        if let Some(x) = map.get(k) {
            self.move_back(*x);
            Some(LruGuard {
                data: unsafe { (*(*x)).val.as_ref().unwrap() },
                _guard: map,
            })
        } else {
            None
        }
    }

    pub(crate) fn del(&self, k: &K) {
        let mut map = self.map.lock().unwrap();
        if let Some(node) = map.remove(k) {
            self.remove_node(node);
            unsafe {
                let _ = Box::from_raw(node);
            }
        }
    }

    fn push_back(&self, node: *mut Node<K, V>) {
        unsafe {
            (*node).next = (*self.head).next;
            (*(*self.head).next).prev = node;
            (*node).prev = self.head;
            (*self.head).next = node;
        }
    }

    fn move_back(&self, node: *mut Node<K, V>) {
        self.remove_node(node);
        self.push_back(node);
    }

    fn remove_node(&self, node: *mut Node<K, V>) {
        unsafe {
            let prev = (*node).prev;
            let next = (*node).next;
            (*prev).next = next;
            (*next).prev = prev;
        }
    }

    fn front(&self) -> *mut Node<K, V> {
        unsafe { (*self.head).prev }
    }
}

impl<K, V> Drop for LruInner<K, V> {
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

pub const LRU_SHARD: usize = 32;
const LRU_SHARD_MASK: usize = LRU_SHARD - 1;

pub(crate) struct Lru<V> {
    shard: [LruInner<u64, V>; LRU_SHARD],
    cap: usize,
}

impl<V> Lru<V> {
    pub(crate) fn new(cap: usize) -> Self {
        let cap = cap / LRU_SHARD;
        Self {
            shard: std::array::from_fn(|_| LruInner::new()),
            cap,
        }
    }

    #[inline(always)]
    fn get_shard(k: u64) -> usize {
        spooky_hash(k) as usize & LRU_SHARD_MASK
    }

    pub(crate) fn add(&self, k: u64, v: V) {
        self.shard[Self::get_shard(k)].add(self.cap, k, v);
    }

    pub(crate) fn get<'a>(&'a self, k: u64) -> Option<LruGuard<'a, u64, V>> {
        self.shard[Self::get_shard(k)].get(&k)
    }

    pub(crate) fn del(&self, k: u64) {
        self.shard[Self::get_shard(k)].del(&k);
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use crate::utils::lru::LruInner;

    #[test]
    fn lru() {
        let m = LruInner::new();
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
