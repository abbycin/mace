use super::page::Page;
use super::traits::IKey;
use super::traits::IVal;
use crate::map::data::FrameOwner;
use crate::OpCode;
use crate::Store;
use std::collections::HashMap;
use std::sync::Arc;

/// a system transaction works like user transaction, which ensure ACID, commit
/// on success, rollback on abort and undo changes
/// [`SysTxn`] works on a block of buffer, it never undo, instead it mark the dirty
/// data as tombstone, which will be cleaned by Garbage Collector later
pub struct SysTxn<'a> {
    store: &'a Store,
    buffers: HashMap<u64, FrameOwner>,
    read_only_buffer: Vec<Arc<FrameOwner>>,
    page_ids: Vec<(u64, u64)>,
    /// page file list
    buffer_id: HashMap<u32, u32>,
}

impl<'a> SysTxn<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            store,
            buffers: HashMap::new(),
            read_only_buffer: Vec::new(),
            page_ids: Vec::new(),
            buffer_id: HashMap::new(),
        }
    }

    fn commit(&mut self) {
        self.page_ids.clear();
        self.buffers.clear();
        for (id, cnt) in &self.buffer_id {
            for _ in 0..*cnt {
                self.store.buffer.release_buffer(*id);
            }
        }
        self.buffer_id.clear();
    }

    pub fn pin_frame(&mut self, f: &Arc<FrameOwner>) {
        self.read_only_buffer.push(f.clone());
    }

    pub fn unpin_all(&mut self) {
        self.read_only_buffer.clear();
    }

    pub fn alloc<K, V>(&mut self, size: usize) -> Result<(u64, Page<K, V>), OpCode>
    where
        K: IKey,
        V: IVal,
    {
        let (addr, buff_id, frame) = self.store.buffer.alloc(size as u32, true)?;

        let payload = frame.payload();
        self.buffers.insert(addr, frame);
        self.inc_buffer_use_cnt(buff_id);
        Ok((addr, Page::from(payload)))
    }

    pub fn map(&mut self, addr: u64) -> u64 {
        let frame = self.buffers.get_mut(&addr).expect("invalid page addr");
        if frame.is_tombstone() {
            panic!("bad insert");
        }

        let pid = self.store.page.map(addr).expect("no page slot");
        log::info!("mapping pid {} addr {}", pid, addr);
        frame.set_pid(pid);
        self.page_ids.push((pid, addr));
        pid
    }

    pub fn update(&mut self, pid: u64, old: u64, new: u64) -> Result<(), u64> {
        // single file, linear increment
        assert!(new > old);
        if let Err(cur) = self.store.page.cas(pid, old, new) {
            return Err(cur);
        }

        let frame = self.buffers.get_mut(&new).expect("invalid addr");
        frame.set_pid(pid);
        self.commit();
        Ok(())
    }

    pub fn replace(&mut self, pid: u64, old: u64, new: u64, junks: &[u64]) -> Result<(), OpCode> {
        // single file, linear increment
        assert!(new > old);

        let addr = self.apply_junks(junks);
        self.update(pid, old, new).map_err(|_| {
            let frame = self.buffers.get_mut(&addr).expect("invalid addr");
            frame.set_tombstone();
            OpCode::Again
        })?;
        Ok(())
    }

    fn apply_junks(&mut self, junks: &[u64]) -> u64 {
        let size = junks.len() * size_of::<u64>();
        let (addr, buff_id, mut frame) = self
            .store
            .buffer
            .alloc(size as u32, true)
            .expect("memory run out");

        // required
        frame.set_delete();

        let mut frameref = frame.load_data().expect("invalid frame");
        let a = frameref.deleted_entries();
        assert_eq!(a.len(), junks.len());
        a.copy_from_slice(junks);

        self.buffers.insert(addr, frame);
        self.inc_buffer_use_cnt(buff_id);
        addr
    }

    fn inc_buffer_use_cnt(&mut self, buff_id: u32) {
        if let Some(cnt) = self.buffer_id.get_mut(&buff_id) {
            *cnt += 1;
        } else {
            self.buffer_id.insert(buff_id, 1);
        }
    }
}

impl Drop for SysTxn<'_> {
    fn drop(&mut self) {
        for (pid, addr) in &self.page_ids {
            self.store.page.unmap(*pid, *addr);
        }
        self.read_only_buffer.clear();

        for (_, frame) in self.buffers.iter_mut() {
            frame.set_tombstone()
        }

        for (id, cnt) in &self.buffer_id {
            for _ in 0..*cnt {
                self.store.buffer.release_buffer(*id);
            }
        }
    }
}
