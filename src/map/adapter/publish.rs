use std::collections::BTreeMap;
use std::ops::Deref;

#[cfg(feature = "failpoints")]
use crate::OpCode;
use crate::map::flush::FlushResult;
use crate::meta::{
    BucketDurableFrontier, FileKind, JunkPermits, MetaKind, PersistStat, RetiredStatFlushSnapshot,
    Txn,
};
use crate::must_ok;
use crate::utils::data::{GroupPositions, init_group_pos};
use crate::utils::options::Options;

use super::observer::ManifestCheckpointObserver;

#[derive(Default)]
struct FlushStatDeltaKind {
    old_stats: Vec<PersistStat>,
    new_stats: Vec<PersistStat>,
}

pub(super) struct FlushStatDelta {
    kinds: [FlushStatDeltaKind; 2],
}

impl FlushStatDelta {
    fn kind(&self, kind: FileKind) -> &FlushStatDeltaKind {
        &self.kinds[kind.slot()]
    }

    fn kind_mut(&mut self, kind: FileKind) -> &mut FlushStatDeltaKind {
        &mut self.kinds[kind.slot()]
    }
}

pub(super) fn checkpoint_publish(observer: &ManifestCheckpointObserver, mut result: FlushResult) {
    let has_new_files = result.has_new_files();
    let bucket_id = result.bucket_id;
    let retired_stat_snapshot = observer.manifest.snapshot_retired_stat_keys(bucket_id);
    let frontier_delta = *result.latest_chkpoint_lsn.deref();
    let previous_frontier = observer
        .manifest
        .bucket_frontier
        .get(&bucket_id)
        .map(|x| *x.value())
        .unwrap_or_else(init_group_pos);

    sync_barrier(
        observer,
        &mut result,
        has_new_files,
        &frontier_delta,
        &previous_frontier,
    );

    let (junk_permits, bucket_frontier) = acquire_junk_permits(observer, &result, &frontier_delta);
    let stat_delta = apply_stat_delta(observer, &mut result, &junk_permits);
    let mut txn = observer.manifest.begin();

    commit_metadata(
        observer,
        &mut txn,
        &result,
        &stat_delta,
        &bucket_frontier,
        retired_stat_snapshot,
        junk_permits,
    );
    advance_floors(observer);
}

pub(super) fn sync_barrier(
    observer: &ManifestCheckpointObserver,
    result: &mut FlushResult,
    has_new_files: bool,
    frontier_delta: &GroupPositions,
    previous_frontier: &GroupPositions,
) {
    let groups = observer.ctx.groups();
    let durable = observer.ctx.opt.sync_on_write;

    // page checkpoint can fold uncleaned txn versions into durable pages, recovery still needs
    // the corresponding WAL tail to rebuild tx outcomes before safe_txid can expose them
    if durable {
        if frontier_delta
            .iter()
            .zip(previous_frontier.iter())
            .take(groups.len())
            .any(|(pos, prev)| *pos > *prev)
        {
            let ticket = {
                let mut log = observer.ctx.lock_shared_logging(0);
                must_ok!(log.barrier_register())
            };
            must_ok!(observer.ctx.drive_sync(&ticket));
        }
    } else {
        for (i, g) in groups.iter().enumerate() {
            if i < frontier_delta.len() && frontier_delta[i] > previous_frontier[i] {
                let mut log = g.logging.lock();
                must_ok!(log.sync_checkpoint_barrier());
            }
        }
    }

    result.sync();
    #[cfg(feature = "extra_check")]
    crate::testing::fire_checkpoint_sync_point(
        crate::testing::CheckpointSyncPoint::AfterResultSync,
        result.bucket_id,
    );
    if has_new_files {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_flush_after_data_dir_sync");
    }
}

pub(super) fn acquire_junk_permits<'a>(
    observer: &'a ManifestCheckpointObserver,
    result: &FlushResult,
    frontier_delta: &GroupPositions,
) -> (JunkPermits<'a>, BucketDurableFrontier) {
    let needed: Vec<FileKind> = FileKind::ALL
        .into_iter()
        .filter(|&kind| !result.kind(kind).junk.is_empty())
        .collect();
    // the registry owns the all-or-nothing wait: no permit loop here
    let junk_permits = observer.manifest.junk().acquire_permits(&needed);
    let bucket_frontier = observer
        .manifest
        .merge_bucket_frontier(result.bucket_id, frontier_delta);
    (junk_permits, bucket_frontier)
}

pub(super) fn apply_stat_delta(
    observer: &ManifestCheckpointObserver,
    result: &mut FlushResult,
    junk_permits: &JunkPermits<'_>,
) -> FlushStatDelta {
    let bucket_id = result.bucket_id;
    let mut delta = FlushStatDelta {
        kinds: [FlushStatDeltaKind::default(), FlushStatDeltaKind::default()],
    };

    for kind in FileKind::ALL {
        let tick = result
            .kind(kind)
            .ivls
            .first()
            .map(|x| x.file_id)
            .map(|_| {
                observer
                    .manifest
                    .buckets
                    .get_runtime(bucket_id)
                    .next_update_epoch(kind)
            })
            .unwrap_or_else(|| next_tick(observer, bucket_id, kind));
        let mut by_file = BTreeMap::<u64, PersistStat>::new();
        let stats = if result.kind(kind).junk.is_empty() {
            Vec::new()
        } else {
            must_ok!(observer.manifest.apply_junks(
                kind,
                junk_permits.permit_required(kind),
                bucket_id,
                tick,
                &result.kind(kind).junk,
            ))
        };
        for stat in stats {
            by_file.insert(stat.file_id, stat);
        }
        delta.kind_mut(kind).old_stats = by_file.into_values().collect();
    }

    #[cfg(feature = "failpoints")]
    if FileKind::ALL
        .iter()
        .any(|&kind| !delta.kind(kind).old_stats.is_empty())
    {
        crate::utils::failpoint::crash("mace_flush_after_old_stat_delta");
    }

    for kind in FileKind::ALL {
        #[cfg(feature = "extra_check")]
        assert_eq!(result.kind(kind).stats.len(), result.kind(kind).ivls.len());

        delta.kind_mut(kind).new_stats = std::mem::take(&mut result.kind_mut(kind).stats);
    }
    delta
}

pub(super) fn commit_metadata(
    observer: &ManifestCheckpointObserver,
    txn: &mut Txn<'_>,
    result: &FlushResult,
    stat_delta: &FlushStatDelta,
    bucket_frontier: &BucketDurableFrontier,
    retired_stat_snapshot: RetiredStatFlushSnapshot,
    junk_permits: JunkPermits<'_>,
) {
    for kind in FileKind::ALL {
        for ivl in &result.kind(kind).ivls {
            observer.manifest.clear_orphan_file(kind, txn, ivl.file_id);
        }
    }
    for kind in FileKind::ALL {
        for ivl in &result.kind(kind).ivls {
            txn.record(interval_meta_kind(kind), ivl);
        }
        stat_delta
            .kind(kind)
            .old_stats
            .iter()
            .filter(|x| {
                !observer
                    .manifest
                    .is_retired_stat(kind, result.bucket_id, x.file_id)
            })
            .for_each(|x| record_stat_update(txn, kind, x));
        stat_delta
            .kind(kind)
            .new_stats
            .iter()
            .for_each(|x| txn.record(stat_meta_kind(kind), x));
    }

    txn.record(MetaKind::BucketFrontier, bucket_frontier);
    txn.record(MetaKind::Map, &result.map_table);
    txn.record(MetaKind::Sequences, observer.manifest.sequences.deref());

    #[cfg(feature = "failpoints")]
    if let Err(e) = crate::utils::failpoint::check("mace_flush_before_manifest_commit") {
        abort_flush_publish("before manifest commit", e);
    }
    #[cfg(feature = "extra_check")]
    crate::testing::fire_gc_stat_sync_point(
        crate::testing::GcStatSyncPoint::CheckpointBeforeManifestCommit,
        result.bucket_id,
        &observer.ctx.opt.db_root,
    );
    txn.commit();
    observer
        .manifest
        .clear_retired_stat_keys(retired_stat_snapshot);
    for kind in FileKind::ALL {
        observer.manifest.clear_synced_files(kind);
    }
    drop(junk_permits);

    #[cfg(feature = "failpoints")]
    if let Err(e) = crate::utils::failpoint::check("mace_flush_after_manifest_commit") {
        abort_flush_publish("after manifest commit", e);
    }
}

pub(super) fn advance_floors(observer: &ManifestCheckpointObserver) {
    let groups = observer.ctx.groups();
    let durable = observer.ctx.opt.sync_on_write;
    let global_frontier = observer.manifest.global_frontier_lower_bound(groups.len());

    if durable {
        let mut lk = observer.ctx.lock_shared_logging(0);
        for (i, g) in groups.iter().enumerate() {
            let mut pos = global_frontier[i];
            if let Some(min) = g.min_active_lsn(&mut lk) {
                pos = pos.min(min);
            }
            pos = observer
                .ctx
                .clamp_floor_to_abort_clean(pos, Options::SHARED_ID);
            lk.update_checkpoint_for(pos, i);
        }
        // durable checkpoints only flush the hint
    } else {
        // relaxed checkpoints update only the owning group's floor
        for (i, g) in groups.iter().enumerate() {
            let mut lk = g.logging.lock();
            let mut pos = global_frontier[i];
            if let Some(min) = g.min_active_lsn(&mut lk)
                && min < pos
            {
                pos = min;
            }
            pos = observer.ctx.clamp_floor_to_abort_clean(pos, g.id as u8);
            lk.update_checkpoint_for(pos, i);
        }
    }
}

fn next_tick(observer: &ManifestCheckpointObserver, bucket_id: u64, kind: FileKind) -> u64 {
    observer
        .manifest
        .buckets
        .get_runtime(bucket_id)
        .load_update_epoch(kind)
}

fn interval_meta_kind(kind: FileKind) -> MetaKind {
    match kind {
        FileKind::Data => MetaKind::DataInterval,
        FileKind::Blob => MetaKind::BlobInterval,
    }
}

fn stat_meta_kind(kind: FileKind) -> MetaKind {
    match kind {
        FileKind::Data => MetaKind::DataStat,
        FileKind::Blob => MetaKind::BlobStat,
    }
}

fn record_stat_update(txn: &mut Txn<'_>, kind: FileKind, stat: &PersistStat) {
    match kind {
        FileKind::Data => txn.record_data_stat_update(stat),
        FileKind::Blob => txn.record_blob_stat_update(stat),
    }
}

#[cfg(feature = "failpoints")]
#[cold]
fn abort_flush_publish(stage: &str, err: OpCode) -> ! {
    // the process is about to abort, so the report is not filtered: a configured level
    // must not be able to hide why the engine died
    crate::utils::diag::emit(
        crate::utils::diag::DiagLevel::Error,
        file!(),
        line!(),
        format_args!("flush publish {stage} failed: {err:?}"),
    );
    std::process::abort()
}
