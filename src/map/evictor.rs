use crate::must_true;
use std::{
    sync::{
        Arc,
        mpsc::{Receiver, RecvTimeoutError, Sender},
    },
    time::{Duration, Instant},
};

use crossbeam_epoch::Guard;
use rand::seq::SliceRandom;

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
    utils::{Handle, options::ParsedOptions},
};

pub(crate) struct Evictor {
    opt: Arc<ParsedOptions>,
    buckets: Handle<BucketMgr>,
    rx: Receiver<SharedState>,
    tx: Sender<()>,
}

unsafe impl Send for Evictor {}

impl Evictor {
    pub(crate) fn new(
        opt: Arc<ParsedOptions>,
        buckets: Handle<BucketMgr>,
        rx: Receiver<SharedState>,
        tx: Sender<()>,
    ) -> Self {
        Self {
            opt,
            buckets,
            rx,
            tx,
        }
    }

    fn begin_build<'a>(&self, bucket: &'a BucketContext) -> AllocGuard<'a> {
        AllocGuard::new(bucket)
    }

    fn eviction_buckets(&self) -> Vec<Arc<BucketContext>> {
        self.buckets
            .active_contexts()
            .into_iter()
            .filter(|bucket| bucket.almost_full())
            .collect()
    }

    fn target_bucket(&self, bucket_id: u64) -> Option<Arc<BucketContext>> {
        self.buckets.buckets.get(&bucket_id).and_then(|entry| {
            let bucket = entry.value().clone();
            if bucket.state.is_deleting() || bucket.state.is_drop() {
                None
            } else {
                Some(bucket)
            }
        })
    }

    fn evict_bucket(&mut self, g: &Guard, safe_txid: u64, bucket_ctx: Arc<BucketContext>) -> bool {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_evictor_before_evict_once");

        let mut rng = rand::rng();
        if !bucket_ctx.begin_eviction() && !bucket_ctx.almost_full() {
            return false;
        }
        if !bucket_ctx.almost_full() {
            return false;
        }

        let mut candidates = bucket_ctx.candidate_snapshot();
        let target = bucket_ctx.evict_sample_target(candidates.len());
        if target == 0 {
            return false;
        }

        for _ in 0..EVICT_SAMPLE_MAX_ROUNDS {
            candidates.shuffle(&mut rng);

            for pid in candidates.iter().take(target) {
                let pid = *pid;
                let Some(state) = bucket_ctx.cool(pid) else {
                    continue;
                };
                if state != CacheState::Cold {
                    continue;
                }

                loop {
                    let swip = Swip::new(bucket_ctx.table.get(pid));

                    // it's possible when a node was unmapped, but not removed from cache yet (concurrently)
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
                    if old.take_recent() {
                        bucket_ctx.warm_state(pid);
                        break;
                    }
                    if bucket_ctx.table.get(pid) != old.swip() {
                        // mapping changed after snapshot/read; it become warm again, so break
                        break;
                    }
                    if bucket_ctx.cache_state(pid) != Some(CacheState::Cold) {
                        break;
                    }

                    let mut build = self.begin_build(&bucket_ctx);
                    if !old.is_intl() && old.delta_len() > 0 {
                        let (node, junk) = old.compact(&mut build, safe_txid);
                        let addr = node.latest_addr();
                        must_true!(eq addr, node.base_addr());
                        let mut publish = build.into_publish(g);
                        publish.evict(old, node, junk, Swip::tagged(addr));
                        publish.commit();
                    } else {
                        let mut publish = build.into_publish(g);
                        publish.evict_simple(pid, old, Swip::tagged(old.latest_addr()));
                        publish.commit();
                    }

                    return true;
                }
            }
        }

        false
    }

    fn evict_once(&mut self, g: &Guard, safe_txid: u64, bucket_ctxs: Vec<Arc<BucketContext>>) {
        for bucket_ctx in bucket_ctxs {
            if self.evict_bucket(g, safe_txid, bucket_ctx) {
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
    const SCAN_MS: u64 = 10 * 1000;
    let chkpt_ivl = e.opt.checkpoint_nudge_ms;
    let mut last_nudge_scan = Instant::now();
    loop {
        let r = e.rx.recv_timeout(Duration::from_millis(TMO_MS));
        let g = crossbeam_epoch::pin();
        // always use compact-safe watermark so consolidation does not prune committed history
        // while there are pending aborted heads waiting for physical cleanup
        let safe_txid = e.buckets.ctx.compact_safe_txid();

        match r {
            Ok(SharedState::Quit) => break,
            Ok(SharedState::Evict(bucket_id)) => {
                if let Some(bucket_ctx) = e.target_bucket(bucket_id) {
                    let _ = e.evict_bucket(&g, safe_txid, bucket_ctx);
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                let bucket_ctxs = e.eviction_buckets();
                if !bucket_ctxs.is_empty() {
                    e.evict_once(&g, safe_txid, bucket_ctxs);
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
