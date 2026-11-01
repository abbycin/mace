use parking_lot::Mutex;

use crate::cc::context::Context;
use crate::map::buffer::ReclaimMode;
use crate::map::flush::{FlushDirective, FlushResult};
use crate::meta::{FileKind, IntervalPair, Manifest, MemStat};
use crate::must_exist;
use crate::utils::Handle;
use std::sync::Arc;

pub(crate) struct ManifestCheckpointObserver {
    pub(super) manifest: Handle<Manifest>,
    pub(super) ctx: Handle<Context>,
    on_finish: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl ManifestCheckpointObserver {
    pub(crate) fn new(manifest: Handle<Manifest>, ctx: Handle<Context>) -> Self {
        Self {
            manifest,
            ctx,
            on_finish: Mutex::new(None),
        }
    }

    pub(crate) fn set_finish_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        self.on_finish.lock().replace(hook);
    }

    /// what this flush round does with the bucket's sealed batch
    ///
    /// the mode is written by the reclaim site; the lifecycle state is never used to infer it. A
    /// queued batch for a bucket without a runtime is an invariant failure, not a skip: the
    /// reclaim paths keep the runtime alive until the batch was drained.
    pub(crate) fn flush_directive(&self, bucket_id: u64) -> FlushDirective {
        let runtime = must_exist!(
            self.manifest.buckets.runtime(bucket_id),
            "bucket checkpoint task requires a live runtime"
        );
        match runtime.reclaim_mode() {
            Some(ReclaimMode::Discard) => FlushDirective::Skip,
            Some(ReclaimMode::FinalCheckpoint { .. }) | None => FlushDirective::Normal,
        }
    }

    pub(crate) fn next_update_epoch(&self, bucket_id: u64, kind: FileKind) -> u64 {
        self.manifest
            .buckets
            .get_runtime(bucket_id)
            .next_update_epoch(kind)
    }

    pub(crate) fn stage_unsynced_file(&self, kind: FileKind, file_id: u64) {
        self.manifest.stage_unsynced_file(kind, file_id);
    }

    pub(crate) fn stage_orphan_file(&self, kind: FileKind, file_id: u64) {
        self.manifest.stage_orphan_file(kind, file_id);
    }

    pub(crate) fn update_mem_interval_stat(
        &self,
        kind: FileKind,
        ivl: IntervalPair,
        stat: MemStat,
    ) {
        self.manifest.add_stat(kind, stat, ivl);
    }

    pub(crate) fn on_checkpoint(&self, result: FlushResult) {
        super::publish::checkpoint_publish(self, result)
    }

    pub(crate) fn finish_checkpoint(&self) {
        if let Some(hook) = self.on_finish.lock().as_ref().cloned() {
            hook();
        }
    }
}
