use super::IAlloc;
use crate::OpCode;
use crate::Store;
use crate::map::data::{FrameFlag, FrameOwner, FrameRef};
use crate::utils::data::JUNK_LEN;
use crate::utils::traits::ICollector;
use std::sync::atomic::Ordering::Relaxed;

pub struct SysTxn<'a> {
    pub store: &'a Store,
    /// arena list
    buffers: Vec<(usize, FrameRef)>,
    junks: Vec<FrameOwner>,
    page_ids: Vec<(u64, u64)>,
}

impl<'a> SysTxn<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            store,
            buffers: Vec::new(),
            junks: Vec::new(),
            page_ids: Vec::new(),
        }
    }

    pub fn transmute<'b>(&mut self) -> &'b mut Self {
        unsafe { &mut *(self as *mut _) }
    }

    fn commit(&mut self) {
        self.page_ids.clear();
        // NOTE: some frames allocated but failed in cas are also cleand here, since they were
        // marked as tombstone on cas failed (if we don't do that, we have to traverse buffers to
        // determine which id should be delayed for releasing buffer and be marked tombstone here,
        // it's a little bit more overhead and complicate)
        for (id, _) in &self.buffers {
            self.store.buffer.release_buffer(*id);
        }
        self.buffers.clear();
    }

    pub fn gc(&mut self, f: FrameOwner) {
        self.junks.push(f);
    }

    pub fn alloc(&mut self, size: usize) -> Result<FrameRef, OpCode> {
        let (buff_id, frame) = self.store.buffer.alloc(size as u32).inspect_err(|e| {
            log::error!("alloc memory fail, {:?}", e);
        })?;

        self.buffers.push((buff_id, frame));
        Ok(frame)
    }

    pub fn map(&mut self, frame: &mut FrameRef) -> u64 {
        if matches!(frame.flag(), FrameFlag::TombStone) {
            panic!("bad insert");
        }
        let addr = frame.addr();

        let pid = self.store.page.map(addr).expect("no page slot");
        frame.set_pid(pid);
        self.page_ids.push((pid, addr));
        pid
    }

    pub fn update(&mut self, pid: u64, old: u64, frame: &mut FrameRef) -> Result<(), u64> {
        let new = frame.addr();
        self.store.page.cas(pid, old, new).inspect_err(|_| {
            // NOTE: if retry ok, it will be set to non-tombstone in Frame::set_pid
            frame.set_tombstone();
            self.junks.clear();
        })?;

        Self::apply_junks(self);
        frame.set_pid(pid);
        self.commit();
        Ok(())
    }

    fn apply_junks(&mut self) {
        // the junks maybe from multiple arenas
        let junks: Vec<u64> = self.junks.iter().map(|x| x.addr()).collect();

        if !junks.is_empty() {
            let sz = junks.len() * JUNK_LEN;

            let (id, mut junk) = self.store.buffer.alloc(sz as u32).expect("no memory");
            junk.fill_junk(&junks);
            self.store.buffer.release_buffer(id);
        }
        self.junks.clear();
    }

    pub fn update_unchecked(&mut self, pid: u64, frame: &mut FrameRef) {
        self.store.page.index(pid).store(frame.addr(), Relaxed);
        frame.set_pid(pid);
        self.commit();
    }
}

impl Drop for SysTxn<'_> {
    fn drop(&mut self) {
        for (pid, addr) in &self.page_ids {
            self.store.page.unmap(*pid, *addr).expect("can't go wrong");
        }
        self.junks.clear();

        for (id, f) in self.buffers.iter_mut() {
            f.set_tombstone();
            self.store.buffer.release_buffer(*id);
        }
    }
}

impl IAlloc for SysTxn<'_> {
    fn allocate(&mut self, size: usize) -> Result<FrameRef, OpCode> {
        self.alloc(size)
    }

    fn page_size(&self) -> u32 {
        self.store.opt.page_size
    }

    fn limit_size(&self) -> u32 {
        self.store.opt.inline_size
    }
}

impl ICollector for SysTxn<'_> {
    type Input = FrameOwner;

    fn collect(&mut self, x: Self::Input) {
        self.gc(x);
    }
}
