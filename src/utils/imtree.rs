use std::{
    cmp::Ordering::{self, Greater, Less},
    marker::PhantomData,
    mem::MaybeUninit,
    num::NonZeroUsize,
    ops::{Deref, DerefMut, Index, IndexMut},
    slice::SliceIndex,
    sync::Arc,
};

use crate::utils::Comparator;

/// tune this value when it's necessary
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

    pub(crate) fn iter(&self) -> Iter<'static, K> {
        Iter::new(self.root.clone())
    }

    pub(crate) fn range_from<T>(
        &self,
        k: T,
        cmp: fn(&K, &T) -> Ordering,
        equal: fn(&K, &T) -> bool,
    ) -> RangeIter<'static, K, T> {
        RangeIter::new(self.root.clone(), k, cmp, equal)
    }

    pub(crate) fn visit_from<T, F>(&self, k: &T, cmp: fn(&K, &T) -> Ordering, f: &mut F) -> bool
    where
        F: FnMut(K) -> bool,
    {
        if let Some(root) = &self.root {
            root.visit_from(k, cmp, f)
        } else {
            false
        }
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

impl<K> Intl<K>
where
    K: Copy,
{
    fn level(&self) -> usize {
        match &self.children {
            Children::Intl { level, .. } => level.get(),
            Children::Leaf { .. } => 1,
        }
    }

    fn visit_from<T, F>(&self, k: &T, cmp: fn(&K, &T) -> Ordering, f: &mut F) -> bool
    where
        F: FnMut(K) -> bool,
    {
        let pos = match self.keys.binary_search_by(|x| cmp(x, k)) {
            Ok(pos) => pos + 1,
            Err(pos) => pos,
        };

        match &self.children {
            Children::Intl { intl, .. } => {
                for i in pos..intl.len() {
                    if intl[i].visit_from(k, cmp, f) {
                        return true;
                    }
                }
            }
            Children::Leaf { leaf } => {
                for i in pos..leaf.len() {
                    if leaf[i].visit_from(k, cmp, f) {
                        return true;
                    }
                }
            }
        }
        false
    }
}

impl<K> Leaf<K>
where
    K: Copy,
{
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
            *rk.first().unwrap(),
            Node::Leaf(Arc::new(Leaf { keys: rk })),
        )
    }

    fn visit_from<T, F>(&self, k: &T, cmp: fn(&K, &T) -> Ordering, f: &mut F) -> bool
    where
        F: FnMut(K) -> bool,
    {
        let pos = match self.keys.binary_search_by(|x| cmp(x, k)) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        for i in pos..self.keys.len() {
            if f(self.keys[i]) {
                return true;
            }
        }
        false
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

    fn visit_from<T, F>(&self, k: &T, cmp: fn(&K, &T) -> Ordering, f: &mut F) -> bool
    where
        F: FnMut(K) -> bool,
    {
        match self {
            Node::Intl(intl) => intl.visit_from(k, cmp, f),
            Node::Leaf(leaf) => leaf.visit_from(k, cmp, f),
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

impl<K> Node<K>
where
    K: Copy,
{
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

pub(crate) struct Iter<'a, K: Copy> {
    fwd: Cursor<K>,
    bwd: Cursor<K>,
    fwd_yielded: bool,
    bwd_yielded: bool,
    exhausted: bool,
    _marker: PhantomData<&'a K>,
}

impl<'a, K: Copy> Iter<'a, K> {
    fn new(node: Option<Node<K>>) -> Self {
        let mut fwd = Cursor::new(node.clone());
        let mut bwd = Cursor::new(node);
        let has_fwd = fwd.seek_to_first().is_some();
        let has_bwd = bwd.seek_to_last().is_some();

        Self {
            fwd,
            bwd,
            fwd_yielded: false,
            bwd_yielded: false,
            exhausted: !(has_fwd && has_bwd),
            _marker: PhantomData,
        }
    }

    #[inline]
    fn update_exhausted(&mut self, has_next: bool, other_side_yielded: bool) -> bool {
        let done = should_exhaust(
            has_next,
            other_side_yielded,
            self.fwd.leaf_pos(),
            self.bwd.leaf_pos(),
        );
        if done {
            self.exhausted = true;
        }
        done
    }
}

pub(crate) struct RangeIter<'a, K: Copy, T> {
    fwd: Cursor<K>,
    bwd: Cursor<K>,
    key: T,
    cmp: fn(&K, &T) -> Ordering,
    equal: fn(&K, &T) -> bool,
    exhausted: bool,
    fwd_yielded: bool,
    bwd_yielded: bool,
    _marker: PhantomData<&'a K>,
}

impl<'a, K: Copy, T> RangeIter<'a, K, T> {
    fn new(
        node: Option<Node<K>>,
        key: T,
        cmp: fn(&K, &T) -> Ordering,
        equal: fn(&K, &T) -> bool,
    ) -> Self {
        let mut fwd = Cursor::new(node.clone());
        let mut bwd = Cursor::new(node);
        let has_fwd = fwd.seek_to_key(&key, cmp).is_some();
        let has_bwd = bwd.seek_to_last().is_some();

        let mut this = Self {
            fwd,
            bwd,
            key,
            cmp,
            equal,
            exhausted: !(has_fwd && has_bwd),
            fwd_yielded: false,
            bwd_yielded: false,
            _marker: std::marker::PhantomData,
        };

        if !this.exhausted
            && let Some(k) = this.fwd.peek()
            && !this.in_range(&k)
        {
            this.exhausted = true;
        }

        this
    }

    pub(crate) fn peek(&self) -> Option<K> {
        if self.exhausted {
            None
        } else {
            self.fwd.peek()
        }
    }

    #[inline]
    fn in_range(&self, k: &K) -> bool {
        (self.cmp)(k, &self.key) != Less && (self.equal)(k, &self.key)
    }

    #[inline]
    fn update_exhausted(&mut self, has_next: bool, other_side_yielded: bool) -> bool {
        let done = should_exhaust(
            has_next,
            other_side_yielded,
            self.fwd.leaf_pos(),
            self.bwd.leaf_pos(),
        );
        if done {
            self.exhausted = true;
        }
        done
    }
}

#[inline]
fn should_exhaust<K>(
    has_next: bool,
    other_side_yielded: bool,
    fwd: Option<(usize, *const Leaf<K>)>,
    bwd: Option<(usize, *const Leaf<K>)>,
) -> bool {
    if !has_next {
        return true;
    }

    if let (Some((fi, f)), Some((bi, b))) = (fwd, bwd)
        && std::ptr::eq(f, b)
        && fi >= bi
    {
        return fi != bi || other_side_yielded;
    }

    false
}

struct Cursor<K: Copy> {
    // (visited_child, path)
    path: Vec<(usize, Arc<Intl<K>>)>,
    // (visited_data, node)
    leaf: Option<(usize, Arc<Leaf<K>>)>,
}

impl<K: Copy> Cursor<K> {
    fn new(node: Option<Node<K>>) -> Self {
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

    fn seek_to_key<T, C>(&mut self, k: &T, cmp: C) -> Option<K>
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
                    return self.next();
                }
                return self.peek();
            }

            let (pos, intl) = self.path.last_mut()?;
            *pos = intl
                .keys
                .binary_search_by(|x| cmp(x, k))
                .map(|pos| pos + 1)
                .unwrap_or_else(|pos| pos);
            let (pos, intl) = (*pos, intl.clone());
            self.push_child(pos, intl);
        }
    }

    fn push_child(&mut self, pos: usize, intl: Arc<Intl<K>>) {
        match &intl.children {
            Children::Intl { intl, .. } => self.path.push((0, intl[pos].clone())),
            Children::Leaf { leaf } => self.leaf = Some((0, leaf[pos].clone())),
        }
    }

    fn next(&mut self) -> Option<K> {
        loop {
            if let Some((pos, leaf)) = &mut self.leaf {
                if *pos + 1 < leaf.keys.len() {
                    *pos += 1;
                    return leaf.keys.get(*pos).copied();
                }
                self.leaf = None;
            }
            let Some((pos, intl)) = self.path.last_mut() else {
                break;
            };
            if *pos + 1 < intl.children.len() {
                *pos += 1;
                let (pos, intl) = (*pos, intl.clone());
                self.push_child(pos, intl);
                break;
            }
            self.path.pop();
        }
        self.seek_to_first()
    }

    fn prev(&mut self) -> Option<K> {
        loop {
            if let Some((pos, leaf)) = &mut self.leaf {
                if *pos > 0 {
                    *pos -= 1;
                    return leaf.keys.get(*pos).copied();
                }
                self.leaf = None;
            }
            let Some((pos, intl)) = self.path.last_mut() else {
                break;
            };
            if *pos > 0 {
                *pos -= 1;
                let (pos, intl) = (*pos, intl.clone());
                self.push_child(pos, intl);
                break;
            }
            self.path.pop();
        }
        self.seek_to_last()
    }

    fn peek(&self) -> Option<K> {
        if let Some((i, leaf)) = &self.leaf {
            leaf.keys.get(*i).copied()
        } else {
            None
        }
    }

    fn seek_to_first(&mut self) -> Option<K> {
        loop {
            if let Some((pos, leaf)) = &self.leaf {
                debug_assert_eq!(*pos, 0);
                return leaf.keys.get(*pos).copied();
            }
            let (pos, intl) = self.path.last()?.clone();
            debug_assert_eq!(pos, 0);
            self.push_child(pos, intl);
        }
    }

    fn seek_to_last(&mut self) -> Option<K> {
        loop {
            if let Some((pos, leaf)) = &mut self.leaf {
                *pos = leaf.keys.len().saturating_sub(1);
                return leaf.keys.get(*pos).copied();
            }
            let (pos, intl) = self.path.last_mut()?;
            *pos = intl.children.len().saturating_sub(1);
            let (pos, intl) = (*pos, intl.clone());
            self.push_child(pos, intl);
        }
    }

    fn leaf_pos(&self) -> Option<(usize, *const Leaf<K>)> {
        self.leaf
            .as_ref()
            .map(|(idx, leaf)| (*idx, Arc::as_ptr(leaf)))
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

impl<'a, K: Copy> Iterator for Iter<'a, K> {
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        let next = if self.fwd_yielded {
            self.fwd.next()
        } else {
            self.fwd_yielded = true;
            self.fwd.peek()
        };
        if self.update_exhausted(next.is_some(), self.bwd_yielded) {
            return None;
        }
        next
    }
}

impl<'a, K: Copy, T> Iterator for RangeIter<'a, K, T> {
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        let next = if self.fwd_yielded {
            self.fwd.next()
        } else {
            self.fwd_yielded = true;
            self.fwd.peek()
        };

        let Some(k) = next else {
            self.exhausted = true;
            return None;
        };

        if !self.in_range(&k) {
            self.exhausted = true;
            return None;
        }

        if self.update_exhausted(true, self.bwd_yielded) {
            return None;
        }

        Some(k)
    }
}

impl<'a, K: Copy> DoubleEndedIterator for Iter<'a, K> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        let next = if self.bwd_yielded {
            self.bwd.prev()
        } else {
            self.bwd_yielded = true;
            self.bwd.peek()
        };
        if self.update_exhausted(next.is_some(), self.fwd_yielded) {
            return None;
        }
        next
    }
}

impl<'a, K: Copy, T> DoubleEndedIterator for RangeIter<'a, K, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        loop {
            let next = if self.bwd_yielded {
                self.bwd.prev()
            } else {
                self.bwd_yielded = true;
                self.bwd.peek()
            };
            let Some(k) = next else {
                self.exhausted = true;
                return None;
            };

            if !self.in_range(&k) {
                if (self.cmp)(&k, &self.key) == Less {
                    self.exhausted = true;
                    return None;
                }
                continue;
            }

            if self.update_exhausted(true, self.fwd_yielded) {
                return None;
            }

            return Some(k);
        }
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;
    use std::sync::atomic::Ordering::Relaxed;
    use std::{cmp::Ordering::Equal, sync::atomic::AtomicU64};

    use crate::Options;
    use crate::types::data::{Key, Record, Ver};
    use crate::types::refbox::{BoxRef, DeltaView};
    use crate::types::traits::IFrameAlloc;
    use crate::utils::data::Position;
    use crate::utils::imtree::{ImTree, NODE_SIZE};

    struct Allocator;

    static G_OFF: AtomicU64 = AtomicU64::new(0);

    impl IFrameAlloc for Allocator {
        fn alloc(&mut self, size: u32) -> BoxRef {
            let addr = G_OFF.fetch_add(size as u64, Relaxed);
            BoxRef::alloc(size, addr)
        }

        fn inline_size(&self) -> usize {
            Options::MIN_INLINE_SIZE
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    struct Data {
        key: &'static [u8],
        val: &'static [u8],
        ver: usize,
    }

    impl Data {
        fn kv(k: &'static str, v: &'static str, ver: usize) -> Self {
            Self {
                key: k.as_bytes(),
                val: v.as_bytes(),
                ver,
            }
        }

        fn key(x: &'static str, ver: usize) -> Self {
            Self {
                key: x.as_bytes(),
                val: &[],
                ver,
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

    fn alloc_key(owned: &mut Vec<Box<[u8]>>, key: String) -> &'static [u8] {
        let boxed = key.into_bytes().into_boxed_slice();
        let key = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(boxed.as_ref()) };
        owned.push(boxed);
        key
    }

    #[test]
    fn simple() {
        let mut m = ImTree::<Data>::new(|x, y| x.cmp(y));

        let mut x = m.update(Data::kv("foo", "mo", 1));
        m = x.update(Data::kv("foo", "ha", 2));
        assert_eq!(x.len(), 1);
        assert_eq!(m.len(), 2);
        x = m.update(Data::kv("elder", "+1s", 3));
        m = x.update(Data::kv("young", "naive", 4));

        assert_eq!(x.len(), 3);
        assert_eq!(m.len(), 4);

        assert_eq!(m.iter().count(), m.len());

        let mut it = m.range_from(
            Data::key("foo", usize::MAX), // start from the latest one (ie. the smallest one)
            |x, y| x.cmp(y),
            |x, y| x.key.cmp(y.key).is_eq(),
        );

        assert_eq!(it.next().unwrap().ver, 2);
        assert_eq!(it.next().unwrap().ver, 1);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn leak() {
        let mut im = ImTree::<DeltaView>::new(|x, y| x.key().cmp(y.key()));
        let mut a = Allocator;

        let (delta, _r) = DeltaView::from_key_val(
            &mut a,
            &Key::new("foo".as_bytes(), Ver::new(1, 0)),
            &Record::normal(1, "bar".as_bytes()),
            0,
            Position::default(),
        );
        im = im.update(delta.view().as_delta());

        assert_eq!(im.len(), 1);
    }

    #[test]
    fn simple_rev_iter() {
        let mut m = ImTree::<Data>::new(|x, y| x.cmp(y));
        let mut owned = Vec::new();
        for i in 0..(NODE_SIZE * 3) {
            let k = format!("k{:03}", i);
            let key = alloc_key(&mut owned, k);
            m = m.update(Data {
                key,
                val: &[],
                ver: 1,
            });
        }

        let fwd: Vec<Data> = m.iter().collect();
        let mut it = m.iter();
        let mut rev = Vec::new();
        while let Some(x) = it.next_back() {
            rev.push(x);
        }
        let expected: Vec<Data> = fwd.iter().copied().rev().collect();
        assert_eq!(rev, expected);
    }

    #[test]
    fn iter_shrinking_window() {
        use std::collections::HashSet;

        let mut m = ImTree::<Data>::new(|x, y| x.cmp(y));
        let mut owned = Vec::new();
        let total = NODE_SIZE * 10 - 11;
        for i in 0..total {
            let k = format!("k{:03}", i);
            let key = alloc_key(&mut owned, k);
            m = m.update(Data {
                key,
                val: &[],
                ver: 1,
            });
        }

        let sorted: Vec<String> = m.iter().map(|x| to_str(x.key).to_string()).collect();
        assert_eq!(sorted.len(), total);
        let first = sorted.first().cloned().expect("non-empty");
        let second = sorted.get(1).cloned().expect("at least two items");
        let last = sorted.last().cloned().expect("non-empty");
        let last_1 = sorted
            .get(sorted.len().saturating_sub(2))
            .cloned()
            .expect("at least two items");

        let mut it = m.iter();
        let mut seen = HashSet::new();
        let k = to_str(it.next().unwrap().key).to_string();
        assert_eq!(k, first);
        seen.insert(k);
        let k = to_str(it.next_back().unwrap().key).to_string();
        assert_eq!(k, last);
        seen.insert(k);
        let k = to_str(it.next().unwrap().key).to_string();
        assert_eq!(k, second);
        seen.insert(k);
        let k = to_str(it.next_back().unwrap().key).to_string();
        assert_eq!(k, last_1);
        seen.insert(k);

        loop {
            let l = it.next();
            let r = it.next_back();
            match (l, r) {
                (None, None) => break,
                (Some(x), None) => {
                    seen.insert(to_str(x.key).to_string());
                }
                (None, Some(x)) => {
                    seen.insert(to_str(x.key).to_string());
                }
                (Some(x), Some(y)) => {
                    seen.insert(to_str(x.key).to_string());
                    seen.insert(to_str(y.key).to_string());
                }
            }
        }

        assert_eq!(seen.len(), total);
        for i in 0..total {
            let k = format!("k{:03}", i);
            assert!(seen.contains(&k), "missing key {k}");
        }
        assert!(it.next_back().is_none());
        assert!(it.next().is_none());
    }

    #[test]
    fn next_back_keeps_same_raw_versions() {
        let total = NODE_SIZE * 11 - 3;
        let mut m = ImTree::<Data>::new(|x, y| x.cmp(y));
        let mut owned = Vec::new();
        for i in 0..total {
            let k = format!("a{:03}", i);
            let key = alloc_key(&mut owned, k);
            m = m.update(Data {
                key,
                val: &[],
                ver: 1,
            });
        }
        m = m.update(Data::key("foo", 3));
        m = m.update(Data::key("foo", 2));
        m = m.update(Data::key("foo", 1));
        for i in 0..total {
            let k = format!("z{:03}", i);
            let key = alloc_key(&mut owned, k);
            m = m.update(Data {
                key,
                val: &[],
                ver: 1,
            });
        }

        let mut it = m.range_from(
            Data::key("foo", usize::MAX),
            |x, y| x.cmp(y),
            |x, y| x.key == y.key,
        );
        assert_eq!(it.next_back().unwrap().ver, 1);
        assert_eq!(it.next_back().unwrap().ver, 2);
        assert_eq!(it.next_back().unwrap().ver, 3);
        assert!(it.next_back().is_none());
    }

    #[test]
    fn next_back_on_absent_start_is_empty() {
        let mut m = ImTree::<Data>::new(|x, y| x.cmp(y));
        let mut owned = Vec::new();
        for i in 0..(NODE_SIZE * 2 - 7) {
            let k = format!("k{:03}", i);
            let key = alloc_key(&mut owned, k);
            m = m.update(Data {
                key,
                val: &[],
                ver: 1,
            });
        }

        let mut it = m.range_from(
            Data::key("m999", usize::MAX),
            |x, y| x.cmp(y),
            |x, y| x.key == y.key,
        );
        assert!(it.next_back().is_none());
        assert!(it.next().is_none());
    }
}
