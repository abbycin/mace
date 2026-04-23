use std::{
    sync::{
        Arc,
        atomic::{
            AtomicIsize,
            Ordering::{Acquire, Relaxed},
        },
        mpsc::{Receiver, RecvTimeoutError, Sender},
    },
    time::{Duration, Instant},
};

use crossbeam_epoch::Guard;

use crate::map::{
    Page,
    cache::{CacheState, EVICT_SAMPLE_MAX_ROUNDS},
    publish::AllocGuard,
};
use crate::{
    map::{
        SharedState,
        buffer::{BucketContext, BucketMgr},
        table::Swip,
    },
    meta::Numerics,
    utils::{Handle, options::ParsedOptions},
};
use rand::seq::SliceRandom;

pub(crate) struct Evictor {
    opt: Arc<ParsedOptions>,
    buckets: Handle<BucketMgr>,
    numerics: Arc<Numerics>,
    used: Arc<AtomicIsize>,
    rx: Receiver<SharedState>,
    tx: Sender<()>,
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
        }
    }

    fn begin_build<'a, 'b: 'a>(&'a self, bucket: &'b BucketContext) -> AllocGuard<'a> {
        AllocGuard::new(&self.opt, bucket)
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
                let pid = *pid;
                let Some(state) = bucket_ctx.cool(pid) else {
                    continue;
                };
                if state != CacheState::Cold {
                    continue;
                }

                loop {
                    let swip = Swip::new(bucket_ctx.table.get(pid));

                    // it's passible when a node was unmapped, but not removed from cache yet (concurrently)
                    if swip.is_null() {
                        break;
                    }
                    // tagged means the page has been evicted or was replaced while candidate ring is stale
                    if swip.is_tagged() {
                        break;
                    }
                    let old = Page::from_swip(swip.untagged());
                    let Some(_lk) = old.try_lock() else {
                        continue;
                    };
                    if bucket_ctx.table.get(pid) != old.swip() {
                        // mapping changed after snapshot/read; it become warm again, so break
                        break;
                    }

                    evicted = true;

                    let mut build = self.begin_build(bucket_ctx);
                    if old.delta_len() > limit {
                        let (node, junk) = old.compact(&mut build, safe_txid);
                        let addr = node.latest_addr();
                        assert_eq!(addr, node.base_addr());
                        let mut publish = build.into_publish(g);
                        publish.evict(old, node, junk, Swip::tagged(addr));
                        publish.commit();
                    } else {
                        let mut publish = build.into_publish(g);
                        publish.evict_simple(pid, old, Swip::tagged(old.latest_addr()));
                        publish.commit();
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
                let old = Page::from_swip(swip.untagged());
                if old.delta_len() > limit {
                    let Some(_lk) = old.try_lock() else {
                        continue;
                    };
                    if bucket_ctx.table.get(*pid) != old.swip() {
                        continue;
                    }
                    let mut build = self.begin_build(bucket_ctx);
                    let (node, junk) = old.compact(&mut build, safe_txid);
                    let mut publish = build.into_publish(g);

                    publish.replace(old, node, junk);
                    publish.commit();
                    compacted = true;
                }
            }

            if compacted {
                return;
            }
        }
    }

    fn nudge_stale_checkpoints(&self, interval_ms: u64) {
        for bucket_ctx in self.buckets.active_contexts() {
            bucket_ctx.nudge_checkpoint(interval_ms);
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
    const SCAN_MS: u64 = 10 * 1000;
    let mut compact_cnt = 0;
    let chkpt_ivl = e.opt.checkpoint_nudge_ms;
    let mut last_nudge_scan = Instant::now();
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

        if chkpt_ivl > 0 && last_nudge_scan.elapsed() >= Duration::from_millis(SCAN_MS) {
            last_nudge_scan = Instant::now();
            e.nudge_stale_checkpoints(chkpt_ivl);
        }
    }

    let _ = e.tx.send(());
    log::info!("evictor thread exit");
}
