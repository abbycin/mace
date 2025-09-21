use std::ops::{Deref, DerefMut};

use crate::{
    types::traits::{IHeader, ILoader},
    utils::Handle,
};

use super::node::Node;

pub(crate) struct Page<L: ILoader> {
    inner: Handle<Node<L>>,
}

impl<L: ILoader> Copy for Page<L> {}
impl<L: ILoader> Clone for Page<L> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<L> Page<L>
where
    L: ILoader,
{
    pub(crate) fn new(node: Node<L>) -> Self {
        let inner = Handle::new(node);
        Self { inner }
    }

    pub(crate) fn from_swip(swip: u64) -> Self {
        Self {
            inner: Handle::from(swip as *mut Node<L>),
        }
    }

    pub(crate) fn load(l: L, addr: u64) -> Option<Self> {
        let node = Node::<L>::load(addr, l)?;
        Some(Self {
            inner: Handle::new(node),
        })
    }

    pub(crate) fn swip(&self) -> u64 {
        self.inner.inner() as u64
    }

    pub(crate) fn clone_node(&self) -> Node<L> {
        self.inner.deref().clone()
    }

    pub(crate) fn is_intl(&self) -> bool {
        self.header().is_index
    }

    pub(crate) fn reclaim(&self) {
        self.inner.reclaim();
    }
}

impl<L: ILoader> Deref for Page<L> {
    type Target = Node<L>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<L: ILoader> DerefMut for Page<L> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
