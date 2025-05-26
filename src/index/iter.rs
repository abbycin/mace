use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use crate::utils::traits::{IInfer, IKey, IPageIter, IVal};

pub struct ItemIter<T> {
    item: Option<T>,
    next: Option<T>,
}

impl<T> Default for ItemIter<T> {
    fn default() -> Self {
        Self {
            item: None,
            next: None,
        }
    }
}

impl<T: Clone> ItemIter<T> {
    pub fn new(item: T) -> Self {
        Some(item).into()
    }
}

impl<T: Clone> From<Option<T>> for ItemIter<T> {
    fn from(value: Option<T>) -> Self {
        Self {
            next: value.clone(),
            item: value,
        }
    }
}

impl<T> Iterator for ItemIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next.take()
    }
}

/// [`super::page::Delta`] requires [`IPageIter`]
impl<T: Clone> IPageIter for ItemIter<T> {
    fn rewind(&mut self) {
        self.next = self.item.clone()
    }
}

pub struct SliceIter<'a, K, V>
where
    K: IKey,
    V: IVal,
{
    data: &'a [(K, V)],
    index: usize,
}

impl<'a, K, V> SliceIter<'a, K, V>
where
    K: IKey,
    V: IVal,
{
    pub fn new(s: &'a [(K, V)]) -> Self {
        Self { data: s, index: 0 }
    }
}

impl<K, V> Iterator for SliceIter<'_, K, V>
where
    K: IKey,
    V: IVal,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        self.index += 1;
        if index < self.data.len() {
            Some(self.data[index])
        } else {
            None
        }
    }
}

impl<K, V> IPageIter for SliceIter<'_, K, V>
where
    K: IKey,
    V: IVal,
{
    fn rewind(&mut self) {
        self.index = 0;
    }
}

pub struct SortedIter<I>
where
    I: Iterator,
{
    iter: I,
    rank: usize,
    next: Option<I::Item>,
}

impl<I> SortedIter<I>
where
    I: Iterator,
{
    pub fn new(iter: I, rank: usize) -> Self {
        Self {
            iter,
            rank,
            next: None,
        }
    }

    fn init(&mut self) {
        self.next = self.iter.next();
    }
}

impl<I, K, V, O> Eq for SortedIter<I>
where
    I: Iterator<Item = (K, V, Option<O>)>,
    K: Ord,
    O: Clone + IInfer,
{
}

impl<I, K, V, O> PartialEq for SortedIter<I>
where
    I: Iterator<Item = (K, V, Option<O>)>,
    K: Ord,
    O: Clone + IInfer,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I, K, V, O> Ord for SortedIter<I>
where
    I: Iterator<Item = (K, V, Option<O>)>,
    K: Ord,
    O: Clone + IInfer,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let o = match (&self.next, &other.next) {
            (Some(l), Some(r)) => l.0.cmp(&r.0),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };

        if o == Ordering::Equal {
            self.rank.cmp(&other.rank)
        } else {
            o
        }
    }
}

impl<I, K, V, O> PartialOrd for SortedIter<I>
where
    I: Iterator<Item = (K, V, Option<O>)>,
    K: Ord,
    O: Clone + IInfer,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> Iterator for SortedIter<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.next.take();
        self.next = self.iter.next();
        next
    }
}

impl<I> IPageIter for SortedIter<I>
where
    I: IPageIter,
{
    fn rewind(&mut self) {
        self.iter.rewind();
        self.next = self.iter.next();
    }
}

pub struct MergeIterBuilder<I>
where
    I: Iterator,
{
    iters: Vec<Reverse<SortedIter<I>>>,
}

impl<I, K, V, O> MergeIterBuilder<I>
where
    I: Iterator<Item = (K, V, Option<O>)>,
    K: Ord,
    O: Clone + IInfer,
{
    pub fn new(cap: usize) -> Self {
        Self {
            iters: Vec::with_capacity(cap),
        }
    }

    pub fn add(&mut self, i: I) {
        let rank = self.iters.len(); // the sequence of delta chain: new to old
        self.iters.push(Reverse(SortedIter::new(i, rank)));
    }

    pub fn build(self) -> MergeIter<I> {
        MergeIter::new(self.iters)
    }
}

pub struct MergeIter<I>
where
    I: Iterator,
    SortedIter<I>: Iterator + Ord,
{
    // small heap
    heap: BinaryHeap<Reverse<SortedIter<I>>>,
}

impl<I> MergeIter<I>
where
    I: Iterator,
    SortedIter<I>: Iterator + Ord,
{
    fn new(mut x: Vec<Reverse<SortedIter<I>>>) -> Self {
        for i in x.iter_mut() {
            i.0.init();
        }
        Self { heap: x.into() }
    }
}

impl<I> Iterator for MergeIter<I>
where
    I: Iterator,
    SortedIter<I>: Iterator<Item = I::Item> + Ord,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut i) = self.heap.peek_mut() {
            i.0.next()
        } else {
            None
        }
    }
}

impl<I> IPageIter for MergeIter<I>
where
    I: Iterator,
    SortedIter<I>: IPageIter<Item = I::Item> + Ord,
{
    fn rewind(&mut self) {
        let mut v = std::mem::take(&mut self.heap).into_vec();
        for i in v.iter_mut() {
            i.0.rewind();
        }
        let mut h = BinaryHeap::from(v);
        std::mem::swap(&mut self.heap, &mut h);
    }
}
