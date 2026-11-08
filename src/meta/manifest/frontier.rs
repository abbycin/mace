use super::*;

impl Manifest {
    pub(crate) fn current_group_checkpoints(&self) -> GroupPositions {
        let mut out = init_group_pos();
        let ctx = self.buckets.ctx;
        let groups = ctx.groups();
        let n = groups.len().min(Options::MAX_CONCURRENT_WRITE as usize);
        if let Some(shared) = ctx.shared_logging() {
            // shared WAL floors remain group-specific
            let lk = shared.lock();
            for (i, slot) in out.iter_mut().enumerate().take(n) {
                *slot = lk.logical_checkpoint_at(i);
            }
        } else {
            for (i, slot) in out.iter_mut().enumerate().take(n) {
                *slot = groups[i].logging.lock().last_ckpt();
            }
        }
        out
    }

    pub(crate) fn durable_frontier_lsn(&self, bucket_id: u64, group: u8) -> Position {
        let idx = group as usize;
        must_true!(idx < Options::MAX_CONCURRENT_WRITE as usize);
        self.bucket_frontier
            .get(&bucket_id)
            .map(|x| x[idx])
            .unwrap_or(Position::MIN)
    }

    pub(crate) fn merge_bucket_frontier(
        &self,
        bucket_id: u64,
        delta: &GroupPositions,
    ) -> BucketDurableFrontier {
        let mut merged = self
            .bucket_frontier
            .get(&bucket_id)
            .map(|x| *x.value())
            .unwrap_or(init_group_pos());
        for i in 0..merged.len() {
            merged[i] = merged[i].max(delta[i]);
        }
        self.bucket_frontier.insert(bucket_id, merged);
        BucketDurableFrontier::new(bucket_id, merged)
    }

    pub(crate) fn global_frontier_lower_bound(&self, groups: usize) -> GroupPositions {
        let mut lower = init_group_pos();
        let mut init = [false; Options::MAX_CONCURRENT_WRITE as usize];
        let groups = groups.min(Options::MAX_CONCURRENT_WRITE as usize);
        let fallback = self.current_group_checkpoints();
        for bucket_id in self.catalog.ids() {
            let bucket_ctx = self.buckets.context(bucket_id);
            let has_pending_flush = bucket_ctx
                .as_ref()
                .is_some_and(|ctx| ctx.has_pending_flush_data());
            let frontier = self.bucket_frontier.get(&bucket_id);
            for i in 0..groups {
                // clean/read-only buckets must not pin global frontier with stale bucket frontier.
                // only buckets that still have pending flush data participate via durable frontier.
                let pos = if has_pending_flush {
                    frontier.as_ref().map_or(fallback[i], |x| x[i])
                } else {
                    fallback[i]
                };
                if !init[i] || pos < lower[i] {
                    lower[i] = pos;
                    init[i] = true;
                }
            }
        }
        for i in 0..groups {
            if !init[i] {
                lower[i] = fallback[i];
            }
        }
        lower
    }
}
