use crate::cc::context::Context;
use crate::map::buffer::Buffers;
use crate::map::table::PageMap;
use crate::utils::data::Meta;
use crate::utils::NULL_PID;
use crate::{OpCode, Options};
use std::sync::Arc;

pub struct Store {
    pub(crate) page: PageMap,
    pub(crate) buffer: Arc<Buffers>,
    pub(crate) context: Arc<Context>,
    pub(crate) opt: Arc<Options>,
}

impl Store {
    /// recover from exist database from given path or create a new instance
    pub fn new(page: PageMap, opt: Arc<Options>, meta: Arc<Meta>) -> Result<Self, OpCode> {
        let buffer = Arc::new(Buffers::new(opt.clone(), meta.clone())?);
        Ok(Self {
            page,
            buffer: buffer.clone(),
            context: Context::new(opt.clone(), buffer, meta),
            opt,
        })
    }

    // since NEXT_ID starts from 1, the ROOT's addr can't be 0 when it's not first run
    pub(crate) fn is_fresh(&self, root_pid: u64) -> bool {
        self.page.get(root_pid) == NULL_PID
    }

    pub(crate) fn start(&self) {
        self.context.start();
        self.buffer.start();
    }

    pub(crate) fn quit(&self) {
        self.context.quit(); // flush log first
        self.buffer.quit();
    }
}
