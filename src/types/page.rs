use std::ops::{Deref, DerefMut};

use crate::{types::refbox::ILoader, utils::Handle};

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

    pub(crate) fn load(l: L, addr: u64) -> Self {
        let node = Node::<L>::load(addr, l);
        Self {
            inner: Handle::new(node),
        }
    }

    pub(crate) fn swip(&self) -> u64 {
        self.inner.inner() as u64
    }

    pub(crate) fn pid(&self) -> u64 {
        self.inner.box_header().pid
    }

    pub(crate) fn clone_node(&self) -> Node<L> {
        self.inner.deref().clone()
    }

    pub(crate) fn set_pid(&mut self, pid: u64) {
        self.inner.box_header_mut().pid = pid;
    }

    pub(crate) fn size(&self) -> usize {
        self.inner.size()
    }

    pub(crate) fn latest_addr(&self) -> u64 {
        self.inner.addr
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
