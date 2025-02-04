use std::{
    collections::HashMap,
    hash::Hash,
    ptr::{self},
};

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

pub(crate) struct Lru<K, V> {
    head: *mut Node<K, V>,
    map: HashMap<K, *mut Node<K, V>>,
    cap: usize,
    size: usize,
}

unsafe impl<K, V> Send for Lru<K, V> {}
unsafe impl<K, V> Sync for Lru<K, V> {}

impl<K, V> Lru<K, V>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn new(cap: usize) -> Self {
        let p = Box::into_raw(Box::new(Node::default()));
        unsafe {
            (*p).next = p;
            (*p).prev = p;
        }
        Self {
            head: p,
            map: HashMap::new(),
            cap,
            size: 0,
        }
    }

    pub(crate) fn add(&mut self, k: K, v: V) -> Option<&mut V> {
        let e = self.map.get(&k);
        let r = if e.is_none() {
            let node = Box::new(Node::new(k.clone(), v));
            let p = Box::into_raw(node);
            self.map.insert(k, p);
            self.push_back(p);
            self.size += 1;
            unsafe { (*p).val.as_mut() }
        } else {
            let e = e.unwrap();
            unsafe {
                (*(*e)).set_val(v);
            }
            self.move_back(*e);
            unsafe { (*(*e)).val.as_mut() }
        };

        if self.size > self.cap {
            let node = self.front();
            unsafe {
                self.size -= 1;
                let key = (*node).key.take().unwrap();
                self.map.remove(&key);
                self.remove_node(node);
                let _ = Box::from_raw(node);
            }
        }
        r
    }

    pub fn get(&self, k: &K) -> Option<&V> {
        self.map.get(k).map(|x| {
            self.move_back(*x);
            unsafe { (*(*x)).val.as_ref().unwrap() }
        })
    }

    pub fn get_mut(&self, k: &K) -> Option<&mut V> {
        self.map.get(k).map(|x| {
            self.move_back(*x);
            unsafe { (*(*x)).val.as_mut().unwrap() }
        })
    }

    #[allow(unused)]
    pub fn del(&mut self, k: &K) {
        if let Some(node) = self.map.remove(k) {
            self.remove_node(node);
            unsafe {
                let _ = Box::from_raw(node);
            }
            self.size -= 1;
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

impl<K, V> Drop for Lru<K, V> {
    fn drop(&mut self) {
        unsafe {
            let mut p = (*self.head).next;
            (*self.head).next = ptr::null_mut();

            while !p.is_null() {
                let prev = (*p).next;
                drop(Box::from_raw(p));
                p = prev;
            }
        }
    }
}

#[test]
fn lru() {
    let mut m = Lru::new(3);

    m.add(1, 1);
    m.add(1, 2);

    assert_eq!(m.get(&1).unwrap(), &2);

    m.add(2, 2);
    m.add(3, 3);
    m.add(4, 4);

    assert_eq!(m.get(&1), None);
    assert_eq!(m.get(&2).unwrap(), &2);
    assert_eq!(m.get(&3).unwrap(), &3);
    assert_eq!(m.get(&4).unwrap(), &4);
}
