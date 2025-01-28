use super::IAlloc;
use crate::map::data::{Frame, FrameFlag, FrameOwner, FrameView};
use crate::OpCode;
use crate::Store;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

pub struct SysTxn<'a> {
    pub store: &'a Store,
    /// arena list
    buffers: Vec<(usize, FrameView)>,
    read_only_buffer: Vec<Arc<FrameOwner>>,
    page_ids: Vec<(u64, u64)>,
}

impl<'a> SysTxn<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            store,
            buffers: Vec::new(),
            read_only_buffer: Vec::new(),
            page_ids: Vec::new(),
        }
    }

    fn commit(&mut self) {
        self.page_ids.clear();
        // NOTE: some frames allocated but failed in cas are also cleand here, since they were
        // marked as tombstone on cas failed (if we don't do that, we have to traverse buffers to
        // determine which id should be delayed for releasing buffer and be marked tombstone here,
        // it's a little bit more overhead and complicate)
        for (id, f) in &self.buffers {
            let r = f.set_state(Frame::STATE_ACTIVE, Frame::STATE_INACTIVE);
            debug_assert_eq!(r, Frame::STATE_ACTIVE);
            self.store.buffer.release_buffer(*id, f.addr());
        }
        self.buffers.clear();
    }

    pub fn pin_frame(&mut self, f: &Arc<FrameOwner>) {
        self.read_only_buffer.push(f.clone());
    }

    pub fn unpin_all(&mut self) {
        self.read_only_buffer.clear();
    }

    pub fn alloc(&mut self, size: usize) -> Result<FrameView, OpCode> {
        let (buff_id, frame) = self.store.buffer.alloc(size as u32)?;

        self.buffers.push((buff_id, frame));
        Ok(frame)
    }

    pub fn map(&mut self, frame: &mut FrameView) -> u64 {
        if matches!(frame.flag(), FrameFlag::TombStone) {
            panic!("bad insert");
        }
        let addr = frame.addr();

        let pid = self.store.page.map(addr).expect("no page slot");
        frame.set_pid(pid);
        self.page_ids.push((pid, addr));
        pid
    }

    pub fn update(&mut self, pid: u64, old: u64, frame: &mut FrameView) -> Result<(), u64> {
        let new = frame.addr();
        self.store.page.cas(pid, old, new).inspect_err(|_| {
            // NOTE: if retry ok, it will be set to non-tombstone
            frame.set_tombstone();
        })?;

        frame.set_pid(pid);
        self.commit();
        Ok(())
    }

    pub fn update_unchecked(&mut self, pid: u64, frame: &mut FrameView) {
        self.store.page.index(pid).store(frame.addr(), Relaxed);
        frame.set_pid(pid);
        self.commit();
    }
}

impl Drop for SysTxn<'_> {
    fn drop(&mut self) {
        for (pid, addr) in &self.page_ids {
            self.store.page.unmap(*pid, *addr);
        }
        self.read_only_buffer.clear();

        for (id, f) in self.buffers.iter_mut() {
            f.set_tombstone();
            let r = f.set_state(Frame::STATE_ACTIVE, Frame::STATE_INACTIVE);
            debug_assert_eq!(r, Frame::STATE_ACTIVE);
            self.store.buffer.release_buffer(*id, f.addr());
        }
    }
}

impl IAlloc for SysTxn<'_> {
    fn allocate(&mut self, size: usize) -> Result<FrameView, OpCode> {
        self.alloc(size)
    }

    fn page_size(&self) -> usize {
        self.store.opt.page_size
    }
}
