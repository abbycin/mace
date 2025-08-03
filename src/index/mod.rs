mod systxn;
pub(crate) mod tree;
pub use tree::ValRef;
pub(crate) mod txn;

use crate::map::buffer::Loader;

type Node = crate::types::node::Node<Loader>;
type Page = crate::types::page::Page<Loader>;
