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
        buffer::{BucketContext, BucketMgr, Loader, PackedAllocCtx, Pool},
        data::Arena,
        table::Swip,
    },
    meta::Numerics,
    types::{
        header::TagFlag,
        page::Page,
        refbox::{BoxRef, BoxView},
        traits::{IAlloc, IHeader},
    },
    utils::{Handle, NULL_PID, data::JUNK_LEN, options::ParsedOptions},
};
use rand::seq::SliceRandom;

pub(crate) struct Evictor {
    opt: Arc<ParsedOptions>,
    buckets: Handle<BucketMgr>,
    numerics: Arc<Numerics>,
    used: Arc<AtomicIsize>,
    rx: Receiver<SharedState>,
    tx: Sender<()>,
    current_pool: Option<Handle<Pool>>,
    current_bucket_id: u64,
    frames: Vec<(Handle<Arena>, BoxView)>,
    garbage: Vec<u64>,
    packed_ctx: Option<PackedAllocCtx>,
    packed_frame_start: Option<usize>,
}

impl Drop for Evictor {
    fn drop(&mut self) {}
}

unsafe impl Send for Evictor {}

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
            current_pool: None,
            current_bucket_id: NULL_PID,
            frames: vec![],
            garbage: vec![],
            packed_ctx: None,
            packed_frame_start: None,
        }
    }

    fn finish_packed_if_any(&mut self) {
        if let Some(ctx) = self.packed_ctx.take() {
            ctx.finish();
            self.packed_frame_start = None;
        }
    }

    fn cancel_packed_if_any(&mut self) {
        if let Some(ctx) = self.packed_ctx.take() {
            ctx.cancel();
            if let Some(start) = self.packed_frame_start.take() {
                self.frames.truncate(start);
            }
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
                    assert!(!swip.is_tagged());
                    let old = Page::<Loader>::from_swip(swip.untagged());
                    self.current_pool = Some(old.loader.pool);
                    self.current_bucket_id = bucket_ctx.bucket_id;

                    let mut compacted = false;
                    let old = Page::<Loader>::from_swip(swip.untagged());
                    let Some(_lk) = old.try_lock() else {
                        continue;
                    };
                    let (addr, junks) = if old.delta_len() > limit {
                        let (mut node, j) = old.compact(self, safe_txid);
                        node.set_pid(old.pid());
                        let addr = node.latest_addr();
                        assert_eq!(addr, node.base_addr());
                        compacted = true;
                        (addr, j)
                    } else {
                        (old.latest_addr(), Junks::new())
                    };

                    match bucket_ctx.table.cas(*pid, old.swip(), Swip::tagged(addr)) {
                        Ok(_) => {
                            if bucket_ctx.evict_cache(*pid) {
                                evicted = true;
                            }
                            if compacted {
                                old.garbage_collect(self, &junks);
                            }
                            // must guarded by EBR, other threads may still use the old page
                            g.defer(move || old.reclaim());
                            self.commit();
                        }
                        Err(_) => {
                            // it has been replaced by other thread, in this case, we do NOT retry, it will
                            // be evicted next time
                            self.rollback();
                        }
                    }
                }
            }

            if evicted {
                return;
            }
        }
    }

    fn commit(&mut self) {
        self.finish_packed_if_any();

        while let Some((a, _)) = self.frames.pop() {
            a.dec_ref();
        }

        let mut garbage: Vec<u64> = Vec::new();
        std::mem::swap(&mut garbage, &mut self.garbage);
        let pool = self.current_pool.as_ref().unwrap();
        pool.recycle(&garbage, |addr| {
            self.garbage.push(addr);
        });

        if !self.garbage.is_empty() {
            #[cfg(feature = "extra_check")]
            {
                let old = self.garbage.len();
                self.garbage.sort();
                self.garbage.dedup();
                assert_eq!(old, self.garbage.len());
            }
            let sz = self.garbage.len() * JUNK_LEN;
            let (h, mut junk) = pool.alloc(sz as u32).expect("never happen");
            junk.header_mut().flag = TagFlag::Junk;
            let dst = junk.data_slice_mut::<u64>();
            dst.copy_from_slice(&self.garbage);
            self.garbage.clear();
            h.dec_ref();
        }
    }

    fn rollback(&mut self) {
        self.cancel_packed_if_any();

        while let Some((a, b)) = self.frames.pop() {
            let h = b.header();
            a.dealloc(h.addr, h.total_size as usize);
            a.dec_ref();
        }
        self.garbage.clear();
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

                assert!(!swip.is_tagged());
                let old = Page::<Loader>::from_swip(swip.untagged());
                if old.delta_len() > limit {
                    let Some(_lk) = old.try_lock() else {
                        continue;
                    };

                    self.current_pool = Some(old.loader.pool);
                    self.current_bucket_id = bucket_ctx.bucket_id;

                    let (mut node, junks) = old.compact(self, safe_txid);
                    node.set_pid(old.pid());
                    let new = Page::new(node);

                    // never retry, probability of successful CAS > 70%
                    // NOTE: other threads may perform cas in the same time
                    if bucket_ctx.table.cas(*pid, old.swip(), new.swip()).is_ok() {
                        bucket_ctx.update_cache_size(*pid, new.size());
                        old.garbage_collect(self, &junks);
                        g.defer(move || old.reclaim());
                        self.commit();
                        compacted = true;
                    } else {
                        new.reclaim();
                        self.rollback();
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

impl IAlloc for Evictor {
    fn allocate(&mut self, size: u32) -> BoxRef {
        let (h, b) = if let Some(ctx) = self.packed_ctx.as_mut() {
            ctx.alloc(size)
        } else {
            let pool = self.current_pool.as_ref().expect("pool must be set");
            pool.alloc(size).expect("never happen")
        };
        self.frames.push((h, b.view()));
        b
    }

    fn allocate_pair(&mut self, size1: u32, size2: u32) -> (BoxRef, BoxRef) {
        assert!(
            self.packed_ctx.is_none(),
            "allocate_pair in packed evictor allocation"
        );
        let pool = self.current_pool.as_ref().expect("pool must be set");
        let ((h1, b1), (h2, b2)) = pool.alloc_pair(size1, size2).expect("never happen");
        self.frames.push((h1, b1.view()));
        self.frames.push((h2, b2.view()));
        (b1, b2)
    }

    fn begin_packed_alloc(&mut self, total_real_size: u32, nr_frames: u32) {
        if self.packed_ctx.is_some() {
            log::error!("nested packed allocation in evictor");
            panic!("nested packed allocation in evictor");
        }

        let pool = self.current_pool.as_ref().expect("pool must be set");
        let ctx = pool
            .begin_packed_ctx(total_real_size, nr_frames)
            .expect("begin packed allocation failed");
        self.packed_frame_start = Some(self.frames.len());
        self.packed_ctx = Some(ctx);
    }

    fn end_packed_alloc(&mut self) {
        let Some(ctx) = self.packed_ctx.take() else {
            log::error!("end packed allocation without begin in evictor");
            panic!("end packed allocation without begin in evictor");
        };
        ctx.finish();
        self.packed_frame_start = None;
    }

    fn arena_size(&mut self) -> usize {
        self.opt.data_file_size
    }

    fn collect(&mut self, addr: &[u64]) {
        self.garbage.extend_from_slice(addr);
    }

    fn inline_size(&self) -> usize {
        self.opt.inline_size
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
