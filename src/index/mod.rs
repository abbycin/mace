pub(crate) mod data;
mod iter;
pub(crate) mod page;
mod systxn;
pub(crate) mod tree;
pub use data::Key;
pub use tree::Val;
mod builder;
mod slotted;
pub(crate) mod txn;

#[cfg(test)]
pub(crate) use builder::Delta;

use crate::{map::data::FrameOwner, OpCode};

trait IAlloc {
    fn allocate(&mut self, size: usize) -> Result<(u64, FrameOwner), OpCode>;

    fn page_size(&self) -> usize;
}
