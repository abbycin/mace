pub(crate) mod data;
mod iter;
pub(crate) mod page;
mod systxn;
pub(crate) mod tree;
pub use data::Key;
pub use tree::ValRef;
mod builder;
pub(crate) mod txn;

use crate::{OpCode, map::data::FrameRef};

pub trait IAlloc {
    fn allocate(&mut self, size: usize) -> Result<FrameRef, OpCode>;

    fn page_size(&self) -> u32;

    fn limit_size(&self) -> u32;
}
