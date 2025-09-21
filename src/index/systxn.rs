pub(crate) use crate::OpCode;
use crate::index::Node;
use crate::map::data::Arena;
use crate::types::header::TagFlag;
use crate::types::refbox::{BoxRef, BoxView};
use crate::types::traits::{IAlloc, IHeader, IInlineSize};
use crate::utils::Handle;
use crate::utils::data::JUNK_LEN;
use crate::{Store, index::Page};
use crossbeam_epoch::Guard;
#[cfg(feature = "metric")]
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

pub struct SysTxn<'a> {
    pub store: &'a Store,
    g: &'a Guard,
    pinned: Vec<Page>,
    maps: Vec<(u64, u64)>,
    /// garbage and allocs may overlap, garbage will be set to Junk on success, while allocs will be
    /// set to TombStone on failure
    garbage: Vec<u64>,
    allocs: Vec<(Handle<Arena>, BoxView)>,
}

impl<'a> SysTxn<'a> {
    pub fn new(store: &'a Store, g: &'a Guard) -> Self {
        Self {
            store,
            g,
            pinned: Vec::new(),
            maps: Vec::new(),
            garbage: Vec::new(),
            allocs: Vec::new(),
        }
    }

    pub fn record_and_commit(&mut self, worker_id: usize, seq: u64) {
        self.store.buffer.record_lsn(worker_id, seq);
        self.commit();
    }

    pub fn commit(&mut self) {
        self.maps.clear();
        while let Some((a, _)) = self.allocs.pop() {
            a.dec_ref();
        }

        let mut garbage: Vec<u64> = Vec::new();
        std::mem::swap(&mut garbage, &mut self.garbage);
        self.recycle(&garbage);
        if !self.garbage.is_empty() {
            self.apply_junk();
        }

        self.pinned.clear();
    }

    pub fn pin(&mut self, p: Page) {
        self.pinned.push(p);
    }

    pub fn alloc(&mut self, size: usize) -> BoxRef {
        #[cfg(feature = "metric")]
        {
            G_ALLOC_STATUS.total_alloc_size.fetch_add(size, Relaxed);
            G_ALLOC_STATUS.total_allocs.fetch_add(1, Relaxed);
        }
        let (h, t) = self.store.buffer.alloc(size as u32).expect("never happen");
        self.allocs.push((h, t.view()));
        t
    }

    /// map pid to page table and assign pid to page
    pub fn map(&mut self, p: &mut Page) -> u64 {
        let pid = self.store.page.map(p.swip()).expect("no page slot");
        p.set_pid(pid);
        self.maps.push((pid, p.swip()));
        pid
    }

    /// unmap pid from page table then recycle space
    pub fn unmap(&mut self, p: Page) -> Result<(), OpCode> {
        // allocate only TagHeader
        let mut unmap = self.alloc(0);
        let h = unmap.header_mut();
        let pid = p.pid();
        h.flag = TagFlag::Unmap;
        h.pid = pid;

        self.store.page.unmap(pid, p.swip()).map(|_| {
            p.garbage_collect(self);
            self.store.buffer.evict(pid);
            self.g.defer(move || p.reclaim());
        })
    }

    fn apply_junk(&mut self) {
        #[cfg(feature = "extra_check")]
        {
            let old = self.garbage.len();
            self.garbage.sort();
            self.garbage.dedup();
            assert_eq!(old, self.garbage.len());
        }
        let sz = self.garbage.len() * JUNK_LEN;
        let (h, mut junk) = self.store.buffer.alloc(sz as u32).unwrap();
        junk.header_mut().flag = TagFlag::Junk;
        let dst = junk.data_slice_mut::<u64>();
        dst.copy_from_slice(&self.garbage);
        self.garbage.clear();
        h.dec_ref();
    }

    /// return new page on success,the old page will be reclaimed
    pub fn replace(&mut self, old: Page, node: Node) -> Result<Page, OpCode> {
        let pid = old.pid();
        let mut new = Page::new(node);
        new.set_pid(pid);
        self.store
            .page
            .cas(pid, old.swip(), new.swip())
            .map(|_| {
                old.garbage_collect(self);
                self.store.buffer.warm(pid, new.size());
                self.g.defer(move || old.reclaim());
                new
            })
            .map_err(|_| {
                new.reclaim();
                OpCode::Again
            })
    }

    pub fn update(&mut self, old: Page, new: &mut Page) -> Result<(), OpCode> {
        let pid = old.pid();
        new.set_pid(pid);
        self.store
            .page
            .cas(pid, old.swip(), new.swip())
            .map(|_| {
                self.store.buffer.warm(pid, new.size());
                self.g.defer(move || old.reclaim())
            })
            .map_err(|_| OpCode::Again)
    }

    /// a small optimization, when address is currently in an active arena, we can mark it as TombStone
    /// so that it will NOT be flushed to data file, or else the address will be used by GC to reclaim
    /// the unused space in old data file
    ///
    /// this can significantly reduce data file size
    fn recycle(&mut self, addr: &[u64]) {
        self.store.buffer.tombstone_active(addr, |addr| {
            self.garbage.push(addr);
        });
    }
}

impl Drop for SysTxn<'_> {
    fn drop(&mut self) {
        while let Some((pid, swip)) = self.maps.pop() {
            // the pid is not publish yet, so the unmap here will always succeed
            self.store.page.unmap(pid, swip).expect("never happen");
        }

        while let Some((a, b)) = self.allocs.pop() {
            let h = b.header();
            #[cfg(feature = "metric")]
            {
                G_ALLOC_STATUS
                    .dealloc_size
                    .fetch_add(h.total_size as usize, Relaxed);
                G_ALLOC_STATUS.deallocs.fetch_add(1, Relaxed);
            }
            a.dealloc(h.addr, h.total_size as usize);
            a.dec_ref();
        }
        while let Some(p) = self.pinned.pop() {
            p.reclaim();
        }
    }
}

impl IAlloc for SysTxn<'_> {
    fn allocate(&mut self, size: usize) -> BoxRef {
        self.alloc(size)
    }

    fn collect(&mut self, addr: &[u64]) {
        self.garbage.extend_from_slice(addr);
    }

    fn arena_size(&mut self) -> u32 {
        self.store.opt.data_file_size
    }
}

impl IInlineSize for SysTxn<'_> {
    fn inline_size(&self) -> u32 {
        self.store.opt.max_inline_size
    }
}

#[cfg(feature = "metric")]
#[derive(Debug)]
pub struct AllocStatus {
    total_alloc_size: AtomicUsize,
    total_allocs: AtomicUsize,
    dealloc_size: AtomicUsize,
    deallocs: AtomicUsize,
}

#[cfg(feature = "metric")]
static G_ALLOC_STATUS: AllocStatus = AllocStatus {
    total_alloc_size: AtomicUsize::new(0),
    total_allocs: AtomicUsize::new(0),
    dealloc_size: AtomicUsize::new(0),
    deallocs: AtomicUsize::new(0),
};

#[cfg(feature = "metric")]
pub fn g_alloc_status() -> &'static AllocStatus {
    &G_ALLOC_STATUS
}
