use crate::cc::context::Context;
use crate::map::buffer::Buffers;
use crate::map::table::PageMap;
use crate::map::{Mapping, create_buffer};
use crate::utils::data::{Meta, WalDescHandle};
use crate::utils::options::ParsedOptions;
use crate::utils::{Handle, NULL_PID};
use crate::{OpCode, ROOT_PID};
use std::sync::Arc;

pub struct Store {
    pub(crate) context: Handle<Context>,
    pub(crate) buffer: Handle<Buffers>,
    pub(crate) page: Arc<PageMap>,
    pub(crate) opt: Arc<ParsedOptions>,
}

impl Store {
    pub fn new(
        page: PageMap,
        opt: Arc<ParsedOptions>,
        meta: Arc<Meta>,
        mapping: Mapping,
        desc: &[WalDescHandle],
    ) -> Result<Self, OpCode> {
        let page = Arc::new(page);
        let ctx = Handle::new(Context::new(opt.clone(), meta.clone(), desc));
        Ok(Self {
            buffer: create_buffer(page.clone(), ctx, opt.clone(), meta.clone(), mapping)?,
            context: ctx,
            page,
            opt,
        })
    }

    // since NEXT_ID starts from 1, the ROOT's addr can't be 0 when it's not first run
    pub(crate) fn is_fresh(&self) -> bool {
        self.page.get(ROOT_PID) == NULL_PID
    }

    pub(crate) fn start(&self) {
        self.context.start();
    }

    pub(crate) fn quit(&self) {
        self.buffer.quit();
        self.context.quit();
        self.buffer.reclaim();
        self.context.reclaim();
    }
}
