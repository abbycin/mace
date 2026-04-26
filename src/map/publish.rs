use crate::map::buffer::{BucketContext, WriteEpoch};
use crate::map::table::{PageMap, Swip};
use crate::map::{Node, Page};
use crate::types::refbox::BoxRef;
use crate::types::traits::IFrameAlloc;
use crate::utils::NULL_ADDR;
use crate::utils::options::ParsedOptions;
use crossbeam_epoch::Guard;
use rustc_hash::FxHashMap;

pub struct Publish<'a> {
    table: &'a PageMap,
    pub(crate) bucket: &'a BucketContext,
    epoch: WriteEpoch,
    g: &'a Guard,
    dirty_roots: FxHashMap<u64, u64>,
    cache_pages: Vec<Page>,
}

impl<'a> Publish<'a> {
    pub(crate) fn commit(mut self) {
        for (pid, addr) in self.dirty_roots {
            self.epoch.dirty_roots.mark(pid, addr);
        }

        for p in self.cache_pages.drain(..) {
            self.bucket.cache(p);
        }
        self.bucket.pool.try_checkpoint();
    }

    fn touch_pid(&mut self, pid: u64, addr: u64) {
        #[cfg(feature = "extra_check")]
        assert_ne!(pid, crate::utils::NULL_PID);
        self.dirty_roots
            .entry(pid)
            .and_modify(|cur| *cur = (*cur).max(addr))
            .or_insert(addr);
    }

    pub(crate) fn cache_after_commit(&mut self, p: Page) {
        self.cache_pages.push(p);
    }

    pub(crate) fn map(&mut self, p: &mut Page) -> u64 {
        let pid = self.table.map(p.swip());
        p.set_pid(pid);
        self.touch_pid(pid, p.latest_addr());
        pid
    }

    pub(crate) fn map_to(&mut self, p: &mut Page, pid: u64) {
        p.set_pid(pid);
        self.table.map_to(pid, p.swip());
        self.touch_pid(pid, p.latest_addr());
    }

    pub(crate) fn evict_simple(&mut self, pid: u64, old: Page, addr: u64) {
        self.table
            .cas(pid, old.swip(), addr)
            .expect("must success, becuase it has been protected by Mutex");
        self.bucket.evict_cache(pid);
        self.touch_pid(pid, Swip::new(addr).untagged());
        self.g.defer(move || old.reclaim());
    }

    pub(crate) fn evict(&mut self, old: Page, mut node: Node, junks: Vec<u64>, addr: u64) {
        let pid = old.pid();
        let old_base = old.base_addr();
        // compact/merge may pass extra junks, but old base/delta must always be retired
        let mut structural_junks = Vec::new();
        old.collect_junk(|x| structural_junks.push(x));
        node.set_pid(pid);
        let new_base = node.base_addr();
        debug_assert_eq!(Swip::new(addr).untagged(), node.latest_addr());
        self.evict_simple(pid, old, addr);
        self.bucket.pool.transfer_junks(
            &self.epoch,
            self.g,
            old_base,
            new_base,
            structural_junks,
            junks,
        );
    }

    pub(crate) fn replace(&mut self, old: Page, mut node: Node, junks: Vec<u64>) -> Page {
        let pid = old.pid();
        let old_base = old.base_addr();
        let new_base = node.base_addr();
        let mut structural_junks = Vec::new();
        old.collect_junk(|x| structural_junks.push(x));
        node.set_pid(pid);
        let new = Page::new(node);
        if self.table.cas(pid, old.swip(), new.swip()).is_ok() {
            self.bucket.pool.transfer_junks(
                &self.epoch,
                self.g,
                old_base,
                new_base,
                structural_junks,
                junks,
            );
            self.bucket.warm(pid, new.size());
            self.touch_pid(pid, new_base);
            self.g.defer(move || old.reclaim());
            return new;
        }
        unreachable!(
            "replace has been protected by Mutex and caller must check if mapping has been changed"
        );
    }
}

pub(crate) struct AllocGuard<'a> {
    opt: &'a ParsedOptions,
    bucket: &'a BucketContext,
    epoch: WriteEpoch,
}

impl<'a> AllocGuard<'a> {
    pub(crate) fn new(opt: &'a ParsedOptions, bucket: &'a BucketContext) -> Self {
        let epoch = bucket.pool.capture_epoch();
        Self { opt, bucket, epoch }
    }

    pub(crate) fn reserve_pid(&self) -> u64 {
        self.bucket.table.reserve_pid()
    }

    pub(crate) fn into_publish(self, g: &'a Guard) -> Publish<'a> {
        Publish {
            table: &self.bucket.table,
            bucket: self.bucket,
            epoch: self.epoch,
            g,
            dirty_roots: FxHashMap::default(),
            cache_pages: Vec::new(),
        }
    }

    pub(crate) fn mark_dirty(&self, pid: u64, addr: u64) {
        self.epoch.dirty_roots.mark(pid, addr);
    }

    pub(crate) fn mark_unmap(&self, pid: u64, old: u64) {
        self.bucket
            .table
            .cas(pid, old, NULL_ADDR)
            .expect("unmapped pid must still be mapped before recycle");
        self.epoch.unmap_pid.mark(pid);
    }

    pub(crate) fn collect_retired(&self, base_addr: u64, dst: &mut Vec<u64>) {
        self.bucket
            .pool
            .collect_junks(&self.epoch.retired, base_addr, dst);
    }
}

impl IFrameAlloc for AllocGuard<'_> {
    fn alloc(&mut self, size: u32) -> BoxRef {
        self.bucket
            .pool
            .alloc_in(&self.epoch.pages, &self.epoch.bytes, size)
    }

    fn frame_budget(&mut self) -> usize {
        self.opt.data_file_size
    }

    fn inline_size(&self) -> usize {
        self.opt.inline_size
    }

    fn checkpoint_lsn(&self, group: u8) -> crate::utils::data::Position {
        self.bucket.pool.checkpoint_lsn(group)
    }
}
