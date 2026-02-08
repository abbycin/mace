use crate::OpCode;
use crate::index::Node;
use crate::index::Page;
use crate::map::buffer::BucketContext;
use crate::map::data::Arena;
use crate::map::table::PageMap;
use crate::types::header::TagFlag;
use crate::types::refbox::{BoxRef, BoxView};
use crate::types::traits::{IAlloc, IHeader};
use crate::utils::Handle;
use crate::utils::data::{JUNK_LEN, Position};
use crate::utils::options::ParsedOptions;
use crossbeam_epoch::Guard;
#[cfg(feature = "metric")]
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

pub struct SysTxn<'a> {
    pub table: &'a PageMap,
    pub(crate) buffer: &'a BucketContext,
    opt: &'a ParsedOptions,
    g: &'a Guard,
    maps: Vec<(u64, u64)>,
    /// garbage and allocs may overlap, garbage will be set to Junk on success, while allocs will be
    /// set to TombStone on failure
    garbage: Vec<u64>,
    allocs: Vec<(Handle<Arena>, BoxView)>,
}

impl<'a> SysTxn<'a> {
    pub fn new(
        table: &'a PageMap,
        opt: &'a ParsedOptions,
        g: &'a Guard,
        buffer: &'a BucketContext,
    ) -> Self {
        Self {
            table,
            buffer,
            opt,
            g,
            maps: Vec::new(),
            garbage: Vec::new(),
            allocs: Vec::new(),
        }
    }

    pub fn record_and_commit(&mut self, group_id: usize, pos: Position) {
        self.buffer.record_lsn(group_id, pos);
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
    }

    pub fn alloc(&mut self, size: usize) -> BoxRef {
        #[cfg(feature = "metric")]
        {
            G_ALLOC_STATUS.total_alloc_size.fetch_add(size, Relaxed);
            G_ALLOC_STATUS.total_allocs.fetch_add(1, Relaxed);
        }
        let (h, t) = self.buffer.alloc(size as u32).expect("never happen");
        let view: BoxView = t.view();
        self.allocs.push((h, view));
        t
    }

    /// map pid to page table and assign pid to page
    pub fn map(&mut self, p: &mut Page) -> u64 {
        let pid = self.table.map(p.swip()).expect("no page slot");
        p.set_pid(pid);
        self.maps.push((pid, p.swip()));
        pid
    }

    pub fn map_to(&mut self, p: &mut Page, pid: u64) {
        p.set_pid(pid);
        self.table.map_to(pid, p.swip());
        self.maps.push((pid, p.swip()));
    }

    /// unmap pid from page table then recycle space
    pub fn unmap(&mut self, p: Page, old_junks: &[u64]) -> Result<(), OpCode> {
        // allocate only TagHeader
        let mut unmap = self.alloc(0);
        let h = unmap.header_mut();
        let pid = p.pid();
        h.flag = TagFlag::Unmap;
        h.pid = pid;

        self.table
            .unmap(pid, p.swip())
            .map(|_| {
                p.garbage_collect(self, old_junks);
                self.g.defer(move || p.reclaim());
            })
            .map_err(|_| OpCode::Again)
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
        let (h, mut junk) = self.buffer.alloc(sz as u32).unwrap();
        junk.header_mut().flag = TagFlag::Junk;
        let dst = junk.data_slice_mut::<u64>();
        dst.copy_from_slice(&self.garbage);
        self.garbage.clear();
        h.dec_ref();
    }

    /// return new page on success, the old page will be reclaimed
    pub fn replace(&mut self, old: Page, node: Node, old_junks: &[u64]) -> Result<Page, OpCode> {
        let pid = old.pid();
        let mut new = Page::new(node);
        new.set_pid(pid);
        self.table
            .cas(pid, old.swip(), new.swip())
            .map(|_| {
                old.garbage_collect(self, old_junks);
                self.buffer.warm(pid, new.size());
                self.g.defer(move || old.reclaim());
                new
            })
            .map_err(|_| {
                new.reclaim();
                OpCode::Again
            })
    }

    #[allow(dead_code)]
    pub fn update(&mut self, old: Page, new: &mut Page) -> Result<(), OpCode> {
        let pid = old.pid();
        new.set_pid(pid);
        self.table
            .cas(pid, old.swip(), new.swip())
            .map(|_| {
                self.buffer.warm(pid, new.size());
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
        self.buffer.recycle(addr, |addr| {
            self.garbage.push(addr);
        });
    }
}

impl Drop for SysTxn<'_> {
    fn drop(&mut self) {
        while let Some((pid, swip)) = self.maps.pop() {
            // rollback: pid should still point to the swip we installed; failure indicates fatal corruption
            self.table
                .unmap(pid, swip)
                .unwrap_or_else(|_| panic!("page map slot changed before rollback, pid {pid}"));
            let p = Page::from_swip(swip);
            p.reclaim();
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
    }
}

impl IAlloc for SysTxn<'_> {
    fn allocate(&mut self, size: usize) -> BoxRef {
        self.alloc(size)
    }

    fn collect(&mut self, addr: &[u64]) {
        self.garbage.extend_from_slice(addr);
    }

    fn arena_size(&mut self) -> usize {
        self.opt.data_file_size
    }

    fn inline_size(&self) -> usize {
        self.opt.inline_size
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
