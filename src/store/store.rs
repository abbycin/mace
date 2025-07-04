use crate::cc::context::Context;
use crate::map::Mapping;
use crate::map::buffer::Buffers;
use crate::map::table::PageMap;
use crate::utils::countblock::Countblock;
use crate::utils::data::{Meta, WalDescHandle};
use crate::utils::options::ParsedOptions;
use crate::utils::{AMutRef, NULL_PID};
use crate::{OpCode, ROOT_PID};
use std::sync::Arc;

pub struct Store {
    pub(crate) page: PageMap,
    pub(crate) buffer: AMutRef<Buffers>,
    pub(crate) context: Arc<Context>,
    pub(crate) opt: Arc<ParsedOptions>,
}

impl Store {
    /// recover from exist database from given path or create a new instance
    pub fn new(
        page: PageMap,
        opt: Arc<ParsedOptions>,
        meta: Arc<Meta>,
        mapping: Mapping,
        desc: &[WalDescHandle],
    ) -> Result<Self, OpCode> {
        let cores = opt.workers;
        let sem = Arc::new(Countblock::new(cores));
        let buffer = AMutRef::new(Buffers::new(
            opt.clone(),
            sem.clone(),
            meta.clone(),
            mapping,
        )?);
        Ok(Self {
            page,
            buffer: buffer.clone(),
            context: Context::new(opt.clone(), buffer, meta, desc),
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
    }
}
