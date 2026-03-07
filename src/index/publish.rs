use std::collections::HashMap;

use crate::OpCode;
use crate::index::Node;
use crate::index::Page;
use crate::map::buffer::BucketContext;
use crate::map::data::Arena;
use crate::map::table::{PageMap, PidLease};
use crate::types::header::TagFlag;
use crate::types::refbox::{BoxRef, BoxView, RemoteView};
use crate::types::traits::{IFrameAlloc, IHeader, IRetireSink};
use crate::utils::Handle;
use crate::utils::data::Position;
use crate::utils::options::ParsedOptions;
use crossbeam_epoch::Guard;

pub(crate) struct PendingUnmap {
    tag: BoxRef,
    retire_arena: u64,
}

pub struct TreeBuildCtx<'a> {
    table: &'a PageMap,
    pub(crate) buffer: &'a BucketContext,
    opt: &'a ParsedOptions,
    allocs: Vec<(Handle<Arena>, BoxView)>,
}

pub struct TreePublishCtx<'a> {
    table: &'a PageMap,
    pub(crate) buffer: &'a BucketContext,
    g: &'a Guard,
    maps: Vec<(u64, u64)>,
    retire_by_arena: HashMap<u64, Vec<u64>>,
    retire_target_arena: Option<u64>,
    allocs: Vec<(Handle<Arena>, BoxView)>,
}

impl<'a> TreeBuildCtx<'a> {
    pub fn new(table: &'a PageMap, opt: &'a ParsedOptions, buffer: &'a BucketContext) -> Self {
        Self {
            table,
            buffer,
            opt,
            allocs: Vec::new(),
        }
    }

    pub(crate) fn reserve_pid(&self) -> Result<PidLease<'a>, OpCode> {
        self.table.reserve_pid().ok_or(OpCode::Again)
    }

    pub(crate) fn prepare_unmap(&mut self, pid: u64) -> Result<PendingUnmap, OpCode> {
        let mut unmap = self.alloc(0)?;
        let retire_arena = self.arena_id_of(unmap.header().addr);
        let h = unmap.header_mut();
        h.flag = TagFlag::Unmap;
        h.pid = pid;
        Ok(PendingUnmap {
            tag: unmap,
            retire_arena,
        })
    }

    pub(crate) fn into_publish(mut self, g: &'a Guard) -> TreePublishCtx<'a> {
        let mut allocs = Vec::new();
        std::mem::swap(&mut allocs, &mut self.allocs);
        TreePublishCtx {
            table: self.table,
            buffer: self.buffer,
            g,
            maps: Vec::new(),
            retire_by_arena: HashMap::new(),
            retire_target_arena: None,
            allocs,
        }
    }

    pub(crate) fn alloc(&mut self, size: u32) -> Result<BoxRef, OpCode> {
        let (h, t) = self.buffer.alloc(size)?;
        let view: BoxView = t.view();
        self.allocs.push((h, view));
        Ok(t)
    }

    fn alloc_pair_internal(&mut self, size1: u32, size2: u32) -> Result<(BoxRef, BoxRef), OpCode> {
        let ((h1, b1), (h2, b2)) = self.buffer.alloc_pair(size1, size2)?;
        self.allocs.push((h1, b1.view()));
        self.allocs.push((h2, b2.view()));
        Ok((b1, b2))
    }

    fn arena_id_of(&self, addr: u64) -> u64 {
        self.buffer
            .arena_id_of(addr)
            .unwrap_or_else(|| panic!("failed to resolve arena id for addr {addr}"))
    }
}

impl Drop for TreeBuildCtx<'_> {
    fn drop(&mut self) {
        while let Some((a, b)) = self.allocs.pop() {
            let h = b.header();
            a.dealloc(h.addr, h.total_size as usize);
            a.dec_ref();
        }
    }
}

impl IFrameAlloc for TreeBuildCtx<'_> {
    fn try_alloc(&mut self, size: u32) -> Result<BoxRef, OpCode> {
        self.alloc(size)
    }

    fn try_alloc_pair(&mut self, size1: u32, size2: u32) -> Result<(BoxRef, BoxRef), OpCode> {
        self.alloc_pair_internal(size1, size2)
    }

    fn arena_size(&mut self) -> usize {
        self.opt.data_file_size
    }

    fn inline_size(&self) -> usize {
        self.opt.inline_size
    }
}

impl<'a> TreePublishCtx<'a> {
    pub(crate) fn record_lsn_and_finish(mut self, group_id: usize, pos: Position) {
        self.buffer.record_lsn(group_id, pos);
        self.finish_inner();
    }

    pub(crate) fn finish(mut self) {
        self.finish_inner();
    }

    fn finish_inner(&mut self) {
        self.maps.clear();
        let mut retire_by_arena = HashMap::new();
        std::mem::swap(&mut retire_by_arena, &mut self.retire_by_arena);
        for (arena_id, addrs) in retire_by_arena {
            if addrs.is_empty() {
                continue;
            }
            #[cfg(feature = "extra_check")]
            let addrs = {
                let mut addrs = addrs;
                let old = addrs.len();
                addrs.sort_unstable();
                addrs.dedup();
                assert_eq!(old, addrs.len());
                addrs
            };
            self.recycle(arena_id, &addrs);
        }

        while let Some((a, _)) = self.allocs.pop() {
            a.dec_ref();
        }
    }

    pub(crate) fn map(&mut self, p: &mut Page) -> u64 {
        let pid = self.table.map(p.swip()).expect("no page slot");
        p.set_pid(pid);
        self.maps.push((pid, p.swip()));
        pid
    }

    pub(crate) fn map_to(&mut self, p: &mut Page, pid: u64) {
        p.set_pid(pid);
        self.table.map_to(pid, p.swip());
        self.maps.push((pid, p.swip()));
    }

    pub(crate) fn map_reserved(&mut self, p: &mut Page, lease: &mut PidLease<'_>) -> u64 {
        let pid = lease.pid();
        p.set_pid(pid);
        self.table.publish_reserved(lease, p.swip());
        self.maps.push((pid, p.swip()));
        pid
    }

    pub(crate) fn unmap(
        &mut self,
        p: Page,
        mut pending: PendingUnmap,
        old_junks: &[u64],
    ) -> Result<(), OpCode> {
        let h = pending.tag.header_mut();
        h.flag = TagFlag::Unmap;
        h.pid = p.pid();
        let pid = p.pid();
        if self.table.unmap(pid, p.swip()).is_err() {
            return Err(OpCode::Again);
        }
        self.collect_in_arena(pending.retire_arena, |publish| {
            p.garbage_collect(publish, old_junks);
        });
        self.g.defer(move || p.reclaim());
        Ok(())
    }

    pub(crate) fn replace(
        &mut self,
        old: Page,
        node: Node,
        old_junks: &[u64],
    ) -> Result<Page, OpCode> {
        let pid = old.pid();
        let mut new = Page::new(node);
        new.set_pid(pid);
        let retire_arena = self.arena_id_of(new.latest_addr());
        match self.table.cas(pid, old.swip(), new.swip()) {
            Ok(_) => {
                self.collect_in_arena(retire_arena, |publish| {
                    old.garbage_collect(publish, old_junks);
                });
                self.buffer.warm(pid, new.size());
                self.g.defer(move || old.reclaim());
                Ok(new)
            }
            Err(_) => {
                new.reclaim();
                Err(OpCode::Again)
            }
        }
    }

    pub(crate) fn collect_garbage(&mut self, arena_addr: u64, page: Page, junks: &[u64]) {
        let arena_id = self.arena_id_of(arena_addr);
        self.collect_in_arena(arena_id, |publish| {
            page.garbage_collect(publish, junks);
        });
    }

    fn recycle(&mut self, arena_id: u64, addr: &[u64]) {
        let mut data = Vec::new();
        let mut blob = Vec::new();
        self.buffer.recycle(addr, |addr| {
            if RemoteView::is_tagged(addr) {
                blob.push(RemoteView::untagged(addr));
            } else {
                data.push(addr);
            }
        });
        if !data.is_empty() || !blob.is_empty() {
            self.buffer.stage_retire(arena_id, &data, &blob);
        }
    }

    fn collect_in_arena<F>(&mut self, arena_id: u64, f: F)
    where
        F: FnOnce(&mut Self),
    {
        let prev = self.retire_target_arena.replace(arena_id);
        f(self);
        self.retire_target_arena = prev;
    }

    fn arena_id_of(&self, addr: u64) -> u64 {
        self.buffer
            .arena_id_of(addr)
            .unwrap_or_else(|| panic!("failed to resolve arena id for addr {addr}"))
    }
}

impl Drop for TreePublishCtx<'_> {
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
            a.dealloc(h.addr, h.total_size as usize);
            a.dec_ref();
        }
    }
}

impl IRetireSink for TreePublishCtx<'_> {
    fn collect(&mut self, addr: &[u64]) {
        let arena_id = self
            .retire_target_arena
            .expect("retire collect must run with arena target");
        self.retire_by_arena
            .entry(arena_id)
            .or_default()
            .extend_from_slice(addr);
    }
}
