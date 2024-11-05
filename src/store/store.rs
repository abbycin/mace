use crate::map::buffer::Buffers;
use crate::map::page_map::PageMap;
use crate::utils::ROOT_PID;
use crate::{OpCode, Options};
use std::sync::Arc;

pub struct Store {
    pub(crate) page: PageMap,
    pub(crate) buffer: Buffers,
    pub(crate) opt: Arc<Options>,
}

impl Store {
    /// recover from exist database from given path or create a new instance
    pub fn new(opt: Options) -> Result<Self, OpCode> {
        let opt = Arc::new(opt);

        let _ = std::fs::create_dir_all(&opt.db_path);

        Ok(Self {
            page: PageMap::new(opt.clone())?,
            buffer: Buffers::new(opt.clone())?,
            opt,
        })
    }

    // since NEXT_ID starts from 1, the ROOT's addr can't be 0 when it's not first run
    pub(crate) fn is_fresh(&self) -> bool {
        self.page.get(ROOT_PID) == 0
    }
}
