use std::{
    sync::{
        Arc,
        atomic::{
            AtomicIsize,
            Ordering::{Acquire, Relaxed},
        },
        mpsc::{Receiver, RecvTimeoutError, Sender},
    },
    time::Duration,
};

use crossbeam_epoch::Guard;

use crate::{
    map::cache::{CacheState, EVICT_SAMPLE_MAX_ROUNDS},
    types::node::Junks,
};
use crate::{
    map::{
        SharedState,
        buffer::{BucketContext, BucketMgr, Loader, Pool},
        data::Arena,
        table::Swip,
    },
    meta::Numerics,
    types::{
        page::Page,
        refbox::{BoxRef, BoxView, RemoteView},
        traits::{IFrameAlloc, IHeader, IRetireSink},
    },
    utils::{Handle, OpCode, options::ParsedOptions},
};
use rand::seq::SliceRandom;

struct EvictBuildCtx<'a> {
    opt: &'a ParsedOptions,
    pool: Handle<Pool>,
    frames: Vec<(Handle<Arena>, BoxView)>,
}

struct EvictPublishCtx {
    pool: Handle<Pool>,
    frames: Vec<(Handle<Arena>, BoxView)>,
    retire_target_arena: Option<u64>,
    garbage: Vec<u64>,
}

pub(crate) struct Evictor {
    opt: Arc<ParsedOptions>,
    buckets: Handle<BucketMgr>,
    numerics: Arc<Numerics>,
    used: Arc<AtomicIsize>,
    rx: Receiver<SharedState>,
    tx: Sender<()>,
}

impl Drop for Evictor {
    fn drop(&mut self) {}
}

unsafe impl Send for Evictor {}

impl<'a> EvictBuildCtx<'a> {
    fn new(opt: &'a ParsedOptions, pool: Handle<Pool>) -> Self {
        Self {
            opt,
            pool,
            frames: vec![],
        }
    }

    fn into_publish(mut self) -> EvictPublishCtx {
        let mut frames = Vec::new();
        std::mem::swap(&mut frames, &mut self.frames);
        EvictPublishCtx {
            pool: self.pool,
            frames,
            retire_target_arena: None,
            garbage: vec![],
        }
    }
}

impl Drop for EvictBuildCtx<'_> {
    fn drop(&mut self) {
        while let Some((a, b)) = self.frames.pop() {
            let h = b.header();
            a.dealloc(h.addr, h.total_size as usize);
            a.dec_ref();
        }
    }
}

impl IFrameAlloc for EvictBuildCtx<'_> {
    fn try_alloc(&mut self, size: u32) -> Result<BoxRef, OpCode> {
        let (h, b) = self.pool.alloc(size)?;
        self.frames.push((h, b.view()));
        Ok(b)
    }

    fn try_alloc_pair(&mut self, size1: u32, size2: u32) -> Result<(BoxRef, BoxRef), OpCode> {
        let ((h1, b1), (h2, b2)) = self.pool.alloc_pair(size1, size2)?;
        self.frames.push((h1, b1.view()));
        self.frames.push((h2, b2.view()));
        Ok((b1, b2))
    }

    fn arena_size(&mut self) -> usize {
        self.opt.data_file_size
    }

    fn inline_size(&self) -> usize {
        self.opt.inline_size
    }
}

impl EvictPublishCtx {
    fn set_retire_target_addr(&mut self, addr: u64) {
        self.retire_target_arena = Some(
            self.pool
                .arena_id_of(addr)
                .unwrap_or_else(|| panic!("failed to resolve evict retire arena for addr {addr}")),
        );
    }

    fn finish(mut self) {
        let mut garbage: Vec<u64> = Vec::new();
        std::mem::swap(&mut garbage, &mut self.garbage);
        if !garbage.is_empty() {
            let arena_id = self
                .retire_target_arena
                .take()
                .expect("evictor retire target must be set before finish");
            let mut data = Vec::new();
            let mut blob = Vec::new();
            self.pool.recycle(&garbage, |addr| {
                if RemoteView::is_tagged(addr) {
                    blob.push(RemoteView::untagged(addr));
                } else {
                    data.push(addr);
                }
            });
            if !data.is_empty() || !blob.is_empty() {
                #[cfg(feature = "extra_check")]
                {
                    let old_data = data.len();
                    data.sort_unstable();
                    data.dedup();
                    assert_eq!(old_data, data.len());
                    let old_blob = blob.len();
                    blob.sort_unstable();
                    blob.dedup();
                    assert_eq!(old_blob, blob.len());
                }
                self.pool.stage_retire(arena_id, &data, &blob);
            }
        } else {
            self.retire_target_arena = None;
        }

        while let Some((a, _)) = self.frames.pop() {
            a.dec_ref();
        }
    }
}

impl Drop for EvictPublishCtx {
    fn drop(&mut self) {
        while let Some((a, b)) = self.frames.pop() {
            let h = b.header();
            a.dealloc(h.addr, h.total_size as usize);
            a.dec_ref();
        }
        self.retire_target_arena = None;
        self.garbage.clear();
    }
}

impl IRetireSink for EvictPublishCtx {
    fn collect(&mut self, addr: &[u64]) {
        assert!(
            self.retire_target_arena.is_some(),
            "evictor collect must run with arena target"
        );
        self.garbage.extend_from_slice(addr);
    }
}

impl Evictor {
    pub(crate) fn new(
        opt: Arc<ParsedOptions>,
        buckets: Handle<BucketMgr>,
        numerics: Arc<Numerics>,
        used: Arc<AtomicIsize>,
        rx: Receiver<SharedState>,
        tx: Sender<()>,
    ) -> Self {
        Self {
            opt,
            buckets,
            numerics,
            used,
            rx,
            tx,
        }
    }

    fn almose_full(&self) -> bool {
        let threshold = self.opt.cache_capacity as isize * 80 / 100;
        self.used.load(Acquire) > threshold
    }

    fn evict_once(&mut self, g: &Guard, limit: usize, safe_txid: u64) {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_evictor_before_evict_once");

        let bucket_ctxs = self.buckets.active_contexts();
        let mut candidates: Vec<(Arc<BucketContext>, u64)> = Vec::new();

        for bucket_ctx in bucket_ctxs {
            let snapshot = bucket_ctx.candidate_snapshot();
            for pid in snapshot {
                candidates.push((bucket_ctx.clone(), pid));
            }
        }

        if candidates.is_empty() {
            return;
        }

        let pct = self.opt.cache_evict_pct.min(100);
        if pct == 0 {
            return;
        }
        let target = (candidates.len() * pct / 100).max(1).min(candidates.len());
        let mut rng = rand::rng();

        for _ in 0..EVICT_SAMPLE_MAX_ROUNDS {
            candidates.shuffle(&mut rng);
            let mut evicted = false;

            for (bucket_ctx, pid) in candidates.iter().take(target) {
                if bucket_ctx.state.is_deleting() || bucket_ctx.state.is_drop() {
                    continue;
                }
                let Some(state) = bucket_ctx.cool(*pid) else {
                    continue;
                };
                if state != CacheState::Cold {
                    continue;
                }

                loop {
                    let swip = Swip::new(bucket_ctx.table.get(*pid));

                    // it's passible when a node was unmapped, but not removed from cache yet (concurrently)
                    if swip.is_null() {
                        break;
                    }
                    // tagged means the page has been evicted or was replaced while candidate ring is stale
                    if swip.is_tagged() {
                        break;
                    }
                    let old = Page::<Loader>::from_swip(swip.untagged());
                    let Some(_lk) = old.try_lock() else {
                        continue;
                    };
                    let mut publish_opt = None;
                    let (addr, junks) = if old.delta_len() > limit {
                        let mut build = EvictBuildCtx::new(self.opt.as_ref(), old.loader.pool);
                        let Ok((mut node, j)) = old.try_compact(&mut build, safe_txid) else {
                            break;
                        };
                        node.set_pid(old.pid());
                        let addr = node.latest_addr();
                        assert_eq!(addr, node.base_addr());
                        publish_opt = Some(build.into_publish());
                        (addr, j)
                    } else {
                        (old.latest_addr(), Junks::new())
                    };

                    match bucket_ctx.table.cas(*pid, old.swip(), Swip::tagged(addr)) {
                        Ok(_) => {
                            if bucket_ctx.evict_cache(*pid) {
                                evicted = true;
                            }
                            if let Some(mut publish) = publish_opt {
                                publish.set_retire_target_addr(addr);
                                old.garbage_collect(&mut publish, &junks);
                                publish.finish();
                            }
                            // must guarded by EBR, other threads may still use the old page
                            g.defer(move || old.reclaim());
                        }
                        Err(_) => {
                            // it has been replaced by other thread, in this case, we do NOT retry, it will
                            // be evicted next time
                        }
                    }
                }
            }

            if evicted {
                return;
            }
        }
    }

    fn compact_once(&mut self, g: &Guard, limit: usize, safe_txid: u64) {
        if self.numerics.oracle.load(Relaxed) != safe_txid {
            return;
        }
        let bucket_ctxs = self.buckets.active_contexts();
        let mut candidates: Vec<(Arc<BucketContext>, u64)> = Vec::new();

        for bucket_ctx in bucket_ctxs {
            let snapshot = bucket_ctx.candidate_snapshot();
            for pid in snapshot {
                candidates.push((bucket_ctx.clone(), pid));
            }
        }

        if candidates.is_empty() {
            return;
        }

        let pct = self.opt.cache_evict_pct.min(100);
        if pct == 0 {
            return;
        }
        let target = (candidates.len() * pct / 100).max(1).min(candidates.len());
        let mut rng = rand::rng();
        let mut compacted = false;

        for _ in 0..EVICT_SAMPLE_MAX_ROUNDS {
            candidates.shuffle(&mut rng);
            for (bucket_ctx, pid) in candidates.iter().take(target) {
                if bucket_ctx.state.is_deleting() || bucket_ctx.state.is_drop() {
                    continue;
                }
                let Some(state) = bucket_ctx.cache_state(*pid) else {
                    continue;
                };
                if state == CacheState::Cold {
                    continue;
                }

                let swip = Swip::new(bucket_ctx.table.get(*pid));
                // it's passible when a node was unmapped, but not removed from cache yet (concurrently)
                if swip.is_null() {
                    continue;
                }
                // tagged means the page has been evicted or replaced already
                if swip.is_tagged() {
                    continue;
                }
                let old = Page::<Loader>::from_swip(swip.untagged());
                if old.delta_len() > limit {
                    let Some(_lk) = old.try_lock() else {
                        continue;
                    };

                    let mut build = EvictBuildCtx::new(self.opt.as_ref(), old.loader.pool);
                    let Ok((mut node, junks)) = old.try_compact(&mut build, safe_txid) else {
                        continue;
                    };
                    node.set_pid(old.pid());
                    let new = Page::new(node);
                    let mut publish = build.into_publish();

                    // never retry, probability of successful CAS > 70%
                    // NOTE: other threads may perform cas in the same time
                    if bucket_ctx.table.cas(*pid, old.swip(), new.swip()).is_ok() {
                        bucket_ctx.update_cache_size(*pid, new.size());
                        publish.set_retire_target_addr(new.latest_addr());
                        old.garbage_collect(&mut publish, &junks);
                        g.defer(move || old.reclaim());
                        publish.finish();
                        compacted = true;
                    } else {
                        new.reclaim();
                    }
                }
            }

            if compacted {
                return;
            }
        }
    }

    pub(crate) fn start(self) {
        std::thread::Builder::new()
            .name("evictor".into())
            .spawn(move || {
                evictor_loop(self);
            })
            .expect("can't start evictor thread");
    }
}

fn evictor_loop(mut e: Evictor) {
    const TMO_MS: u64 = 200;
    const COMPACT_TMO: u64 = 5 * TMO_MS;
    let mut compact_cnt = 0;
    let limit = e.opt.max_delta_len();
    loop {
        let r = e.rx.recv_timeout(Duration::from_millis(TMO_MS));
        let g = crossbeam_epoch::pin();
        let safe_txid = e.numerics.safe_tixd();

        match r {
            Ok(s) => match s {
                SharedState::Quit => {
                    break;
                }
                SharedState::Evict => {
                    e.evict_once(&g, limit, safe_txid);
                }
            },
            Err(RecvTimeoutError::Timeout) => {
                compact_cnt += TMO_MS;
                if e.almose_full() {
                    e.evict_once(&g, limit, safe_txid);
                } else if compact_cnt >= COMPACT_TMO {
                    compact_cnt = 0;
                    e.compact_once(&g, limit, safe_txid);
                }
            }
            Err(_) => break,
        }
    }

    let _ = e.tx.send(());
    log::info!("evictor thread exit");
}
