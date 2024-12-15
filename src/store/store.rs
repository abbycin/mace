use crate::cc::context::Context;
use crate::map::buffer::Buffers;
use crate::map::page_map::PageMap;
use crate::utils::NULL_PID;
use crate::{OpCode, Options};
use std::sync::Arc;

pub struct Store {
    pub(crate) page: PageMap,
    pub(crate) buffer: Buffers,
    pub(crate) context: Arc<Context>,
    pub(crate) opt: Arc<Options>,
}

impl Store {
    /// recover from exist database from given path or create a new instance
    pub fn new(opt: Options) -> Result<Self, OpCode> {
        let opt = Arc::new(opt);

        if !opt.db_root.exists() {
            let _ = std::fs::create_dir_all(&opt.db_root);
        }

        Ok(Self {
            page: PageMap::new(opt.clone())?,
            buffer: Buffers::new(opt.clone())?,
            context: Arc::new(Context::new(opt.clone())),
            opt,
        })
    }

    // since NEXT_ID starts from 1, the ROOT's addr can't be 0 when it's not first run
    pub(crate) fn is_fresh(&self, root_pid: u64) -> bool {
        self.page.get(root_pid) == NULL_PID
    }
}
