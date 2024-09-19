use std::path::Path;

mod cc;
mod log;
mod map;
mod store;
mod utils;

pub(crate) use map::{buffer_map::BufferMap, page_map::PageMap};
pub(crate) use utils::{options::Options, OpCode};

const ROOT_PID: u64 = 0;

pub struct Store {
    tab: PageMap,
    buffer: BufferMap,
}

impl Store {
    fn new(opt: Options) -> Self {
        todo!()
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> OpCode {
        // Physical Page ID
        let ppid = self.tab.get(ROOT_PID);
        // NOTE: we use SWIP introduced by LeanStore, it's unnecessary to lookup in a buffer manager
        todo!()
    }

    fn del(&mut self, key: &[u8]) -> OpCode {
        todo!()
    }

    fn get(&self, key: &[u8]) -> &[u8] {
        todo!()
    }

    fn replace(&mut self, key: &[u8], val: &[u8]) -> OpCode {
        todo!()
    }

    fn scan(&self, key_start: &[u8], key_end: &[u8]) {
        todo!()
    }
}
