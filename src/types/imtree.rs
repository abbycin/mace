use std::{
    cmp::Ordering::{self, Greater, Less},
    mem::MaybeUninit,
    num::NonZeroUsize,
    ops::{Deref, DerefMut, Index, IndexMut},
    slice::SliceIndex,
    sync::Arc,
};

use crate::types::Comparator;

pub(crate) const NODE_SIZE: usize = 64;
const CHILDREN_SIZE: usize = NODE_SIZE + 1;
const MEDIAN: usize = NODE_SIZE / 2;

pub(crate) struct ImTree<K> {
    root: Option<Node<K>>,
    c: Comparator<K>,
    size: usize,
}

impl<K> ImTree<K>
where
    K: Copy,
{
    fn insert(&mut self, k: K) -> Option<K> {
        let root = self.root.get_or_insert_with(Node::default);
        match root.put(k, self.c) {
            Update::Replaced(old) => return Some(old),
            Update::Inserted => (),
            Update::Split(sep, rhs) => {
                let lhs = std::mem::take(root);
                *root = Node::from_split(lhs, sep, rhs);
            }
        }
        self.size += 1;
        None
    }

    pub(crate) fn new(c: Comparator<K>) -> Self {
        Self {
            root: None,
            c,
            size: 0,
        }
    }

    pub(crate) fn set_comparator(&mut self, c: Comparator<K>) {
        self.c = c;
    }

    pub(crate) fn update(&self, k: K) -> Self {
        let mut tmp = self.clone();
        tmp.insert(k);
        tmp
    }

    pub(crate) fn put(&mut self, k: K) {
        self.insert(k);
    }

    pub(crate) fn len(&self) -> usize {
        self.size
    }

    pub(crate) fn find<T, F>(&self, k: &T, f: F) -> Option<&K>
    where
        F: Fn(&K, &T) -> Ordering,
    {
        self.root.as_ref().and_then(|node| node.find_by(k, f))
    }

    pub(crate) fn iter(&self) -> Iter<'_, K> {
        Iter::new(self.root.as_ref())
    }

    pub(crate) fn range_from<T>(
        &self,
        k: T,
        cmp: fn(&K, &T) -> Ordering,
        equal: fn(&K, &T) -> bool,
    ) -> RangeIter<'_, K, T> {
        RangeIter::new(self.root.as_ref(), k, cmp, equal)
    }
}

enum Node<K> {
    Intl(Arc<Intl<K>>),
    Leaf(Arc<Leaf<K>>),
}

struct Intl<K> {
    keys: Chunk<K, NODE_SIZE>,
    children: Children<K>,
}

struct Leaf<K> {
    keys: PodChunk<K, NODE_SIZE>,
}

enum Children<K> {
    Intl {
        intl: Chunk<Arc<Intl<K>>, CHILDREN_SIZE>,
        level: NonZeroUsize,
    },
    Leaf {
        leaf: Chunk<Arc<Leaf<K>>, CHILDREN_SIZE>,
    },
}

impl<K: Copy> Intl<K> {
    fn put<F>(&mut self, k: K, cmp: F) -> Update<K>
    where
        F: Fn(&K, &K) -> Ordering,
    {
        let i = self
            .keys
            .binary_search_by(|x| cmp(x, &k))
            .map(|x| x + 1) // next child
            .unwrap_or_else(|x| x);
        let res = match &mut self.children {
            Children::Intl { intl, .. } => Arc::make_mut(&mut intl[i]).put(k, cmp),
            Children::Leaf { leaf } => Arc::make_mut(&mut leaf[i]).put(k, cmp),
        };

        match res {
            Update::Split(key, node) if self.keys.len() >= NODE_SIZE => {
                self.split_branch_put(i, key, node)
            }
            Update::Split(sep, node) => {
                self.keys.insert(i, sep);
                self.children.insert(i + 1, node);
                Update::Inserted
            }
            x => x,
        }
    }

    fn split_branch_put(&mut self, pos: usize, k: K, node: Node<K>) -> Update<K> {
        let key_split = MEDIAN + (pos > MEDIAN) as usize;
        let mut rk = self.keys.split_at(key_split);
        let index_split = MEDIAN + (pos >= MEDIAN) as usize;
        let mut rc = self.children.split_at(index_split);

        let sep = if pos == MEDIAN {
            rc.insert(0, node.clone());
            k
        } else {
            if pos < MEDIAN {
                self.keys.insert(pos, k);
                self.children.insert(pos + 1, node);
            } else {
                rk.insert(pos - (MEDIAN + 1), k);
                rc.insert(pos - (MEDIAN + 1) + 1, node);
            }
            self.keys.pop_back()
        };

        Update::Split(
            sep,
            Node::Intl(Arc::new(Intl {
                keys: rk,
                children: rc,
            })),
        )
    }
}

impl<K> Intl<K> {
    fn find_by<T, F>(&self, k: &T, f: F) -> Option<&K>
    where
        F: Fn(&K, &T) -> Ordering,
    {
        let mut node = self;
        loop {
            let pos = node
                .keys
                .binary_search_by(|x| f(x, k))
                .map(|pos| pos + 1) // next child
                .unwrap_or_else(|pos| pos);
            match &node.children {
                Children::Intl { intl, .. } => node = &intl[pos],
                Children::Leaf { leaf } => return leaf[pos].find_by(k, f),
            }
        }
    }
}

impl<K> Intl<K> {
    fn level(&self) -> usize {
        match &self.children {
            Children::Intl { level, .. } => level.get(),
            Children::Leaf { .. } => 1,
        }
    }
}

impl<K: Clone> Leaf<K> {
    fn put<F>(&mut self, k: K, cmp: F) -> Update<K>
    where
        F: Fn(&K, &K) -> Ordering,
    {
        match self.keys.binary_search_by(|x| cmp(x, &k)) {
            Ok(pos) => {
                let old = std::mem::replace(&mut self.keys[pos], k);
                Update::Replaced(old)
            }
            Err(pos) if self.keys.len() >= NODE_SIZE => self.split_leaf_put(pos, k),
            Err(pos) => {
                self.keys.insert(pos, k);
                Update::Inserted
            }
        }
    }

    fn split_leaf_put(&mut self, pos: usize, k: K) -> Update<K> {
        let mut rk = self.keys.split_at(MEDIAN);
        if pos < MEDIAN {
            self.keys.insert(pos, k);
        } else {
            rk.insert(pos - MEDIAN, k);
        }
        Update::Split(
            rk.first().unwrap().clone(),
            Node::Leaf(Arc::new(Leaf { keys: rk })),
        )
    }
}

impl<K> Leaf<K> {
    fn find_by<T, F>(&self, k: &T, f: F) -> Option<&K>
    where
        F: Fn(&K, &T) -> Ordering,
    {
        // lower bound
        let keys = &self.keys;
        let pos = keys
            .binary_search_by(|x| match f(x, k) {
                Less => Less,
                _ => Greater,
            })
            .unwrap_or_else(|x| x);
        keys.get(pos)
    }
}

impl<K> Children<K> {
    fn insert(&mut self, pos: usize, node: Node<K>) {
        match (self, node) {
            (Children::Intl { intl, .. }, Node::Intl(node)) => intl.insert(pos, node),
            (Children::Leaf { leaf }, Node::Leaf(node)) => leaf.insert(pos, node),
            _ => unreachable!("invalid insert"),
        }
    }

    fn split_at(&mut self, pos: usize) -> Self {
        match self {
            Children::Intl { intl, level } => Children::Intl {
                intl: intl.split_at(pos),
                level: *level,
            },
            Children::Leaf { leaf } => Children::Leaf {
                leaf: leaf.split_at(pos),
            },
        }
    }

    fn len(&self) -> usize {
        match self {
            Children::Intl { intl, .. } => intl.len(),
            Children::Leaf { leaf } => leaf.len(),
        }
    }
}

impl<K> Node<K>
where
    K: Copy,
{
    fn put(&mut self, k: K, c: Comparator<K>) -> Update<K> {
        match self {
            Node::Intl(intl) => Arc::make_mut(intl).put(k, c),
            Node::Leaf(leaf) => Arc::make_mut(leaf).put(k, c),
        }
    }

    fn find_by<T, F>(&self, k: &T, f: F) -> Option<&K>
    where
        F: Fn(&K, &T) -> Ordering,
    {
        match self {
            Node::Intl(intl) => intl.find_by(k, f),
            Node::Leaf(leaf) => leaf.find_by(k, f),
        }
    }

    fn from_split(lhs: Self, sep: K, rhs: Self) -> Self {
        Node::Intl(Arc::new(Intl {
            keys: Chunk::with_data(sep),
            children: match (lhs, rhs) {
                (Node::Intl(l), Node::Intl(r)) => Children::Intl {
                    level: NonZeroUsize::new(l.level() + 1).unwrap(),
                    intl: Chunk::from_iter([l, r]),
                },
                (Node::Leaf(l), Node::Leaf(r)) => Children::Leaf {
                    leaf: Chunk::from_iter([l, r]),
                },
                _ => unreachable!("invalid node"),
            },
        }))
    }
}

impl<K> Node<K> {
    fn level(&self) -> usize {
        match self {
            Node::Intl(intl) => intl.level(),
            Node::Leaf(_) => 0,
        }
    }
}

enum Update<K> {
    Replaced(K),
    Inserted,
    Split(K, Node<K>),
}

struct Chunk<T, const N: usize> {
    data: MaybeUninit<[T; N]>,
    size: usize,
}

impl<T, const N: usize> Chunk<T, N> {
    fn new() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            size: 0,
        }
    }

    fn with_data(data: T) -> Self {
        let mut tmp = Self::new();
        tmp.size = 1;
        unsafe { tmp.ptr_at_mut(0).write(data) };
        tmp
    }

    fn from_iter<I: IntoIterator<Item = T>>(x: I) -> Self {
        let mut tmp = Self::new();
        for i in x {
            tmp.push_back(i);
        }
        tmp
    }

    fn len(&self) -> usize {
        self.size
    }

    fn insert(&mut self, pos: usize, data: T) {
        assert!(!self.is_full());
        assert!(pos <= self.len());
        self.move_to(pos, pos + 1, self.len() - pos);
        unsafe { self.ptr_at_mut(pos).write(data) };
        self.size += 1;
    }

    fn split_at(&mut self, pos: usize) -> Self {
        assert!(pos <= self.len());

        let mut tmp = Self::new();
        if pos == self.len() {
            return tmp;
        }
        let len = self.len() - pos;
        self.copy_to(pos, 0, len, &mut tmp);
        tmp.size = len;
        self.size = pos;
        tmp
    }

    fn push_back(&mut self, x: T) {
        assert!(self.size < N);
        unsafe { self.ptr_at_mut(self.size).write(x) };
        self.size += 1;
    }

    fn pop_back(&mut self) -> T {
        assert!(!self.is_empty());
        self.size -= 1;
        unsafe { self.ptr_at(self.size).read() }
    }

    fn ptr_at(&self, pos: usize) -> *const T {
        unsafe { (&self.data as *const _ as *const T).add(pos) }
    }

    fn ptr_at_mut(&mut self, pos: usize) -> *mut T {
        unsafe { (&mut self.data as *mut _ as *mut T).add(pos) }
    }

    fn copy_to(&self, from: usize, to: usize, count: usize, dst: &mut Self) {
        if count > 0 {
            unsafe { std::ptr::copy_nonoverlapping(self.ptr_at(from), dst.ptr_at_mut(to), count) };
        }
    }

    fn move_to(&mut self, from: usize, to: usize, count: usize) {
        if count > 0 {
            let src = self.ptr_at_mut(from);
            let dst = self.ptr_at_mut(to);
            unsafe { std::ptr::copy(src, dst, count) };
        }
    }

    fn as_slice(&self) -> &[T] {
        unsafe {
            std::slice::from_raw_parts(
                &self.data as *const MaybeUninit<[T; N]> as *const T,
                self.len(),
            )
        }
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe {
            std::slice::from_raw_parts_mut(
                &mut self.data as *mut MaybeUninit<[T; N]> as *mut T,
                self.len(),
            )
        }
    }

    fn is_empty(&self) -> bool {
        self.size == 0
    }

    fn is_full(&self) -> bool {
        self.size == N
    }
}

/// TODO: remove when stable rust support `specialization`
struct PodChunk<T, const N: usize> {
    data: MaybeUninit<[T; N]>,
    size: usize,
}

impl<T, const N: usize> PodChunk<T, N> {
    fn new() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            size: 0,
        }
    }

    fn len(&self) -> usize {
        self.size
    }

    fn insert(&mut self, pos: usize, data: T) {
        assert!(!self.is_full());
        assert!(pos <= self.len());
        self.move_to(pos, pos + 1, self.len() - pos);
        unsafe { self.ptr_at_mut(pos).write(data) };
        self.size += 1;
    }

    fn split_at(&mut self, pos: usize) -> Self {
        assert!(pos <= self.len());

        let mut tmp = Self::new();
        if pos == self.len() {
            return tmp;
        }
        let len = self.len() - pos;
        self.copy_to(pos, 0, len, &mut tmp);
        tmp.size = len;
        self.size = pos;
        tmp
    }

    fn ptr_at(&self, pos: usize) -> *const T {
        unsafe { (&self.data as *const _ as *const T).add(pos) }
    }

    fn ptr_at_mut(&mut self, pos: usize) -> *mut T {
        unsafe { (&mut self.data as *mut _ as *mut T).add(pos) }
    }

    fn copy_to(&self, from: usize, to: usize, count: usize, dst: &mut Self) {
        if count > 0 {
            unsafe { std::ptr::copy_nonoverlapping(self.ptr_at(from), dst.ptr_at_mut(to), count) };
        }
    }

    fn move_to(&mut self, from: usize, to: usize, count: usize) {
        if count > 0 {
            let src = self.ptr_at_mut(from);
            let dst = self.ptr_at_mut(to);
            unsafe { std::ptr::copy(src, dst, count) };
        }
    }

    fn as_slice(&self) -> &[T] {
        unsafe {
            std::slice::from_raw_parts(
                &self.data as *const MaybeUninit<[T; N]> as *const T,
                self.len(),
            )
        }
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe {
            std::slice::from_raw_parts_mut(
                &mut self.data as *mut MaybeUninit<[T; N]> as *mut T,
                self.len(),
            )
        }
    }

    fn is_full(&self) -> bool {
        self.size == N
    }
}

pub(crate) struct Iter<'a, K> {
    cursor: Cursor<'a, K>,
    runout: bool,
    is_yield: bool,
}

impl<'a, K> Iter<'a, K> {
    fn new(node: Option<&'a Node<K>>) -> Self {
        let mut cursor = Cursor::new(node);
        cursor.seek_to_first();
        Self {
            cursor,
            runout: false,
            is_yield: false,
        }
    }
}

pub(crate) struct RangeIter<'a, K, T> {
    cursor: Cursor<'a, K>,
    key: T,
    equal: fn(&K, &T) -> bool,
    runout: bool,
    is_yield: bool,
}

impl<'a, K, T> RangeIter<'a, K, T> {
    fn new(
        node: Option<&'a Node<K>>,
        key: T,
        cmp: fn(&K, &T) -> Ordering,
        equal: fn(&K, &T) -> bool,
    ) -> Self {
        let mut cursor = Cursor::new(node);
        let runout = cursor.seek_to_key(&key, cmp);

        Self {
            cursor,
            key,
            equal,
            runout,
            is_yield: false,
        }
    }
}

struct Cursor<'a, K> {
    // (visited_child, path)
    path: Vec<(usize, &'a Intl<K>)>,
    // (visited_data, node)
    leaf: Option<(usize, &'a Leaf<K>)>,
}

impl<'a, K> Cursor<'a, K> {
    fn new(node: Option<&'a Node<K>>) -> Self {
        let mut this = Cursor {
            path: Vec::new(),
            leaf: None,
        };

        if let Some(node) = node {
            this.path.reserve_exact(node.level());
            match node {
                Node::Intl(intl) => this.path.push((0, intl)),
                Node::Leaf(leaf) => this.leaf = Some((0, leaf)),
            }
        }
        this
    }

    fn seek_to_key<T, C>(&mut self, k: &T, cmp: C) -> bool
    where
        C: Fn(&K, &T) -> Ordering,
    {
        loop {
            if let Some((pos, leaf)) = &mut self.leaf {
                // lower bound
                let r = leaf.keys.binary_search_by(|x| match cmp(x, k) {
                    Less => Less,
                    _ => Greater,
                });
                *pos = r.unwrap_or_else(|x| x);

                if r == Err(leaf.keys.len()) {
                    self.next();
                }
                return r.is_ok();
            }

            let Some((pos, intl)) = self.path.last_mut() else {
                return false;
            };
            *pos = intl
                .keys
                .binary_search_by(|x| cmp(x, k))
                .map(|pos| pos + 1)
                .unwrap_or_else(|pos| pos);
            let (pos, intl) = (*pos, *intl);
            self.push_child(pos, intl);
        }
    }

    fn push_child(&mut self, pos: usize, intl: &'a Intl<K>) {
        match &intl.children {
            Children::Intl { intl, .. } => self.path.push((0, &intl[pos])),
            Children::Leaf { leaf } => self.leaf = Some((0, &leaf[pos])),
        }
    }

    fn next(&mut self) -> Option<&'a K> {
        loop {
            if let Some((pos, leaf)) = &mut self.leaf {
                if *pos + 1 < leaf.keys.len() {
                    *pos += 1;
                    return leaf.keys.get(*pos);
                }
                self.leaf = None;
            }
            let Some((pos, intl)) = self.path.last_mut() else {
                break;
            };
            if *pos + 1 < intl.children.len() {
                *pos += 1;
                let (pos, intl) = (*pos, *intl);
                self.push_child(pos, intl);
                break;
            }
            self.path.pop();
        }
        self.seek_to_first()
    }

    fn peek(&mut self) -> Option<&'a K> {
        if let Some((i, leaf)) = &self.leaf {
            leaf.keys.get(*i)
        } else {
            None
        }
    }

    fn seek_to_first(&mut self) -> Option<&'a K> {
        loop {
            if let Some((pos, leaf)) = &self.leaf {
                debug_assert_eq!(*pos, 0);
                return leaf.keys.get(*pos);
            }
            let (pos, intl) = self.path.last()?;
            debug_assert_eq!(*pos, 0);
            self.push_child(*pos, *intl);
        }
    }
}

// traits implementations

impl<K: Copy> Clone for ImTree<K> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            c: self.c,
            size: self.size,
        }
    }
}

impl<T: Clone, const N: usize> Clone for Chunk<T, N> {
    fn clone(&self) -> Self {
        let mut tmp = Self::new();
        for i in 0..self.size {
            tmp.push_back(unsafe { (*self.ptr_at(i)).clone() });
        }
        tmp
    }
}

impl<T, const N: usize> Drop for Chunk<T, N> {
    fn drop(&mut self) {
        unsafe { std::ptr::drop_in_place(self.as_mut_slice()) };
    }
}

impl<T: Copy, const N: usize> Clone for PodChunk<T, N> {
    fn clone(&self) -> Self {
        let mut tmp = Self::new();
        unsafe {
            tmp.ptr_at_mut(0)
                .copy_from_nonoverlapping(self.ptr_at(0), self.size);
        }
        tmp.size = self.size;
        tmp
    }
}

impl<K: Copy> Clone for Intl<K> {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            children: self.children.clone(),
        }
    }
}

impl<K: Copy> Clone for Leaf<K> {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
        }
    }
}

impl<K: Copy> Clone for Children<K> {
    fn clone(&self) -> Self {
        match self {
            Children::Intl { intl, level } => Children::Intl {
                intl: intl.clone(),
                level: *level,
            },
            Children::Leaf { leaf } => Children::Leaf { leaf: leaf.clone() },
        }
    }
}

impl<K: Copy> Clone for Node<K> {
    fn clone(&self) -> Self {
        match self {
            Node::Intl(intl) => Node::Intl(intl.clone()),
            Node::Leaf(leaf) => Node::Leaf(leaf.clone()),
        }
    }
}

impl<K> Default for Node<K> {
    fn default() -> Self {
        Node::Leaf(Arc::new(Leaf {
            keys: PodChunk::new(),
        }))
    }
}

impl<T, I, const N: usize> Index<I> for Chunk<T, N>
where
    I: SliceIndex<[T]>,
{
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.as_slice().index(index)
    }
}

impl<T, I, const N: usize> IndexMut<I> for Chunk<T, N>
where
    I: SliceIndex<[T]>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.as_mut_slice().index_mut(index)
    }
}

impl<T, const N: usize> Deref for Chunk<T, N> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T, const N: usize> DerefMut for Chunk<T, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<T, I, const N: usize> Index<I> for PodChunk<T, N>
where
    I: SliceIndex<[T]>,
{
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.as_slice().index(index)
    }
}

impl<T, I, const N: usize> IndexMut<I> for PodChunk<T, N>
where
    I: SliceIndex<[T]>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.as_mut_slice().index_mut(index)
    }
}

impl<T, const N: usize> Deref for PodChunk<T, N> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T, const N: usize> DerefMut for PodChunk<T, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<'a, K> Iterator for Iter<'a, K> {
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        if self.runout {
            return None;
        }

        if self.is_yield {
            self.cursor.next()
        } else {
            self.is_yield = true;
            self.cursor.peek()
        }
    }
}

impl<'a, K, T> Iterator for RangeIter<'a, K, T> {
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        if self.runout {
            return None;
        }

        let next = if self.is_yield {
            self.cursor.next()
        } else {
            self.is_yield = true;
            self.cursor.peek()
        };

        if let Some(x) = next.as_ref() {
            if !(self.equal)(x, &self.key) {
                self.runout = true;
                return None;
            }
        }

        next
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::{cmp::Ordering::Equal, sync::atomic::AtomicU64};

    use rand::seq::SliceRandom;

    use crate::types::data::{Key, Record, Value, Ver};
    use crate::types::imtree::{ImTree, NODE_SIZE};
    use crate::types::refbox::{BoxRef, DeltaView};
    use crate::types::traits::{IAlloc, IInlineSize};

    static G_VER: AtomicUsize = AtomicUsize::new(1);

    struct Allocator;

    static G_OFF: AtomicU64 = AtomicU64::new(0);

    impl IAlloc for Allocator {
        fn allocate(&mut self, size: usize) -> BoxRef {
            let addr = G_OFF.fetch_add(size as u64, std::sync::atomic::Ordering::Relaxed);
            BoxRef::alloc(size as u32, addr)
        }

        fn recycle(&mut self, _p: &[u64]) {
            unimplemented!()
        }

        fn arena_size(&mut self) -> u32 {
            1 << 20
        }
    }
    impl IInlineSize for Allocator {
        fn inline_size(&self) -> u32 {
            2048
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    struct Data {
        key: &'static [u8],
        val: &'static [u8],
        ver: usize,
    }

    impl Data {
        fn kv(k: &'static str, v: &'static str) -> Self {
            Self {
                key: k.as_bytes(),
                val: v.as_bytes(),
                ver: G_VER.fetch_add(1, Relaxed),
            }
        }

        fn key(x: &'static str) -> Self {
            Self {
                key: x.as_bytes(),
                val: &[],
                ver: usize::MAX,
            }
        }
    }

    impl Ord for Data {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            match self.key.cmp(other.key) {
                Equal => other.ver.cmp(&self.ver),
                o => o,
            }
        }
    }

    impl PartialOrd for Data {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    fn to_str(x: &[u8]) -> &str {
        unsafe { std::str::from_utf8_unchecked(x) }
    }

    impl Debug for Data {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Data")
                .field("key", &to_str(self.key))
                .field("val", &to_str(self.val))
                .field("ver", &self.ver)
                .finish()
        }
    }

    #[test]
    fn simple() {
        let mut m = ImTree::<Data>::new(|x, y| x.cmp(y));

        let mut x = m.update(Data::kv("foo", "mo"));
        m = x.update(Data::kv("foo", "ha"));
        assert_eq!(x.len(), 1);
        assert_eq!(m.len(), 2);
        x = m.update(Data::kv("elder", "+1s"));
        m = x.update(Data::kv("young", "naive"));

        assert_eq!(x.len(), 3);
        assert_eq!(m.len(), 4);

        // lower_bound
        let r = m.find(&Data::key("foo"), |x, y| x.cmp(y));
        assert!(r.is_some());
        let Data { key: _, val, ver } = r.unwrap();
        assert_eq!(val, &"ha".as_bytes());
        assert_eq!(ver, &2);

        let mut it = m.range_from(
            Data::key("foo"),
            |x, y| x.cmp(y),
            |x, y| x.key.cmp(y.key).is_eq(),
        );

        assert_eq!(it.next().unwrap().ver, 2);
        assert_eq!(it.next().unwrap().ver, 1);
        assert_eq!(it.next(), None);

        assert_eq!(m.iter().count(), m.len());
    }

    #[test]
    fn ordered() {
        let mut m = ImTree::<i32>::new(|x, y| x.cmp(y));
        let mut v: Vec<i32> = (0..NODE_SIZE as i32 * 2).collect();
        let mut rng = rand::thread_rng();
        v.shuffle(&mut rng);

        for &i in &v {
            m = m.update(i);
        }

        for i in &v {
            let x = m.find(i, |x, y| x.cmp(y));
            assert!(x.is_some());
            assert_eq!(x.unwrap(), i);
        }

        v.sort();
        let c: Vec<i32> = m.iter().copied().collect();
        assert_eq!(v, c);
    }

    #[test]
    fn leak() {
        let mut im = ImTree::<DeltaView>::new(|x, y| x.key().cmp(y.key()));
        let mut a = Allocator;

        let delta = DeltaView::from_key_val(
            &mut a,
            Key::new("foo".as_bytes(), Ver::new(1, 0)),
            Value::Put(Record::normal(1, "bar".as_bytes())),
        );
        im = im.update(delta.view().as_delta());

        assert_eq!(im.len(), 1);
    }
}
