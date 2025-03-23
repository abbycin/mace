pub(crate) mod data;
mod iter;
pub(crate) mod page;
mod systxn;
pub(crate) mod tree;
pub use data::Key;
pub use tree::ValRef;
mod builder;
pub(crate) mod registry;
mod slotted;
pub(crate) mod txn;

use crate::{map::data::FrameRef, OpCode};

trait IAlloc {
    fn allocate(&mut self, size: usize) -> Result<FrameRef, OpCode>;

    fn page_size(&self) -> usize;
}
