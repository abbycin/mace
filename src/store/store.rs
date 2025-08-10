use crate::cc::context::Context;
use crate::map::Mapping;
use crate::map::buffer::Buffers;
use crate::map::table::PageMap;
use crate::utils::data::{Meta, WalDescHandle};
use crate::utils::options::ParsedOptions;
use crate::utils::{Handle, NULL_PID};
use crate::{OpCode, ROOT_PID};
use std::sync::Arc;

pub struct Store {
    pub(crate) buffer: Handle<Buffers>,
    pub(crate) context: Context,
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
        let buffer = Handle::new(Buffers::new(
            page.clone(),
            opt.clone(),
            meta.clone(),
            mapping,
        )?);
        Ok(Self {
            buffer,
            context: Context::new(opt.clone(), buffer, meta, desc),
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
        self.context.quit(); // flush log first
        self.buffer.quit();
        self.buffer.reclaim();
    }
}
