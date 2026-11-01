use std::collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;

use super::GarbageCollector;
#[cfg(feature = "metrics")]
use crate::utils::observe::CounterMetric;
use crate::{
    OpCode,
    map::data::MetaReader,
    meta::{DelInterval, Delete, FileKind, MetaKind},
    must_ok, must_true,
};

impl GarbageCollector {
    pub(super) fn delete_meta_kind(kind: FileKind) -> MetaKind {
        kind_select!(kind, data => MetaKind::DataDelete, blob => MetaKind::BlobDelete)
    }

    pub(super) fn delete_interval_meta_kind(kind: FileKind) -> MetaKind {
        kind_select!(kind, data => MetaKind::DataDelInterval, blob => MetaKind::BlobDelInterval)
    }

    #[cfg(feature = "metrics")]
    fn obsolete_counter(kind: FileKind) -> CounterMetric {
        kind_select!(kind, data => CounterMetric::GcDataObsoleteFile, blob => CounterMetric::GcBlobObsoleteFile)
    }

    #[cfg(feature = "failpoints")]
    fn obsolete_after_meta_commit_failpoint(kind: FileKind) -> &'static str {
        kind_select!(kind, data => "mace_gc_data_obsolete_after_meta_commit", blob => "mace_gc_blob_obsolete_after_meta_commit")
    }

    #[cfg(feature = "failpoints")]
    fn obsolete_after_retired_mark_failpoint(kind: FileKind) -> &'static str {
        kind_select!(kind, data => "mace_gc_data_obsolete_after_retired_mark", blob => "mace_gc_blob_obsolete_after_retired_mark")
    }

    #[cfg(feature = "failpoints")]
    fn obsolete_after_remove_stat_failpoint(kind: FileKind) -> &'static str {
        kind_select!(kind, data => "mace_gc_data_obsolete_after_remove_stat", blob => "mace_gc_blob_obsolete_after_remove_stat")
    }

    pub(super) fn process_obsoleted_files(
        &self,
        kind: FileKind,
        obsoleted: &[u64],
        bucket_id: u64,
    ) {
        self.process_obsoleted_groups(kind, std::iter::once((bucket_id, obsoleted)));
    }

    pub(super) fn process_obsoleted_batches(
        &self,
        kind: FileKind,
        grouped: &HashMap<u64, Vec<u64>>,
    ) {
        self.process_obsoleted_groups(
            kind,
            grouped
                .iter()
                .map(|(&bucket_id, ids)| (bucket_id, ids.as_slice())),
        );
    }

    /// collect reclaim candidates before taking the junk gate
    fn collect_obsolete_intervals<'a, I>(
        &self,
        kind: FileKind,
        groups: I,
    ) -> Vec<(u64, u64, Vec<u64>)>
    where
        I: IntoIterator<Item = (u64, &'a [u64])>,
    {
        let mut file_intervals = Vec::new();
        for (bucket_id, obsoleted) in groups {
            for &file_id in obsoleted {
                if self.store.manifest.is_unsynced_file(kind, file_id) {
                    continue;
                }
                let path = match kind {
                    FileKind::Data => self.store.opt.data_file(file_id),
                    FileKind::Blob => self.store.opt.blob_file(file_id),
                };
                let mut loader = MetaReader::new(self.store.opt.fs.as_ref(), path);
                let ivls = loader.get_interval();
                let mut lows = Vec::new();
                for i in ivls {
                    if kind == FileKind::Data || i.lo <= i.hi {
                        // keep deleting old sentinel keys from earlier empty data files
                        lows.push(i.lo);
                    }
                }
                file_intervals.push((bucket_id, file_id, lows));
            }
        }
        file_intervals
    }

    fn process_obsoleted_groups<'a, I>(&self, kind: FileKind, groups: I)
    where
        I: IntoIterator<Item = (u64, &'a [u64])>,
    {
        let file_intervals = self.collect_obsolete_intervals(kind, groups);
        if file_intervals.is_empty() {
            return;
        }

        let gate = self.store.manifest.junk().close_gate(kind);
        gate.drain();
        let kept = self.recheck_obsolete_stats(kind, &file_intervals);
        self.commit_obsolete_metadata(kind, &kept, file_intervals[0].0);
        self.retire_obsolete_stats(kind, &kept);
        drop(gate);
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::obsolete_after_remove_stat_failpoint(kind));
        #[cfg(feature = "extra_check")]
        crate::testing::fire_gc_stat_sync_point(
            match kind {
                FileKind::Data => crate::testing::GcStatSyncPoint::DataObsoleteAfterRuntimeRemove,
                FileKind::Blob => crate::testing::GcStatSyncPoint::BlobObsoleteAfterRuntimeRemove,
            },
            file_intervals[0].0,
            &self.store.opt.db_root,
        );
        self.delete_obsolete_files(kind, kept);
    }

    /// re-read durable stats after the gate has drained; only inactive files survive
    fn recheck_obsolete_stats(
        &self,
        kind: FileKind,
        file_intervals: &[(u64, u64, Vec<u64>)],
    ) -> HashMap<u64, (Delete, Vec<u64>)> {
        let mut kept = HashMap::<u64, (Delete, Vec<u64>)>::new();
        for &(bucket_id, file_id, ref lows) in file_intervals {
            match self
                .store
                .manifest
                .file_state(kind)
                .stat
                .load_persisted_stat_inner(file_id, &self.store.manifest.btree)
            {
                Ok(stat) => {
                    must_true!(
                        stat.active_elems == 0 && stat.active_size == 0,
                        "reclaim candidate still active in durable stats"
                    );
                    let entry = kept
                        .entry(bucket_id)
                        .or_insert_with(|| (Delete::default(), Vec::new()));
                    entry.0.push(file_id);
                    entry.1.extend(lows);
                }
                Err(OpCode::NotFound) => {}
                Err(err) => must_ok!(Result::<(), OpCode>::Err(err)),
            }
        }
        kept
    }

    /// publish one batched delete txn before touching runtime stats or files
    #[cfg_attr(not(feature = "extra_check"), allow(unused_variables))]
    fn commit_obsolete_metadata(
        &self,
        kind: FileKind,
        kept: &HashMap<u64, (Delete, Vec<u64>)>,
        sync_bucket_id: u64,
    ) {
        let mut txn = self.store.manifest.begin();
        for (&bucket_id, (unlinked, lows)) in kept {
            txn.record(Self::delete_meta_kind(kind), unlinked);
            txn.record(
                Self::delete_interval_meta_kind(kind),
                &DelInterval {
                    lo: lows.clone(),
                    bucket_id,
                },
            );
        }
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(Self::obsolete_after_meta_commit_failpoint(kind));
        #[cfg(feature = "extra_check")]
        crate::testing::fire_gc_stat_sync_point(
            match kind {
                FileKind::Data => crate::testing::GcStatSyncPoint::DataObsoleteAfterMetaCommit,
                FileKind::Blob => crate::testing::GcStatSyncPoint::BlobObsoleteAfterMetaCommit,
            },
            sync_bucket_id,
            &self.store.opt.db_root,
        );
    }

    /// retire runtime stat intervals for the files committed above
    fn retire_obsolete_stats(&self, kind: FileKind, kept: &HashMap<u64, (Delete, Vec<u64>)>) {
        for (&bucket_id, (unlinked, lows)) in kept {
            self.store
                .manifest
                .mark_retired_stats(kind, bucket_id, unlinked);
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash(Self::obsolete_after_retired_mark_failpoint(kind));
            #[cfg(feature = "extra_check")]
            crate::testing::fire_gc_stat_sync_point(
                match kind {
                    FileKind::Data => crate::testing::GcStatSyncPoint::DataObsoleteAfterRetiredMark,
                    FileKind::Blob => crate::testing::GcStatSyncPoint::BlobObsoleteAfterRetiredMark,
                },
                bucket_id,
                &self.store.opt.db_root,
            );
            self.store
                .manifest
                .remove_retired_stat_intervals(kind, bucket_id, unlinked, lows);
        }
    }

    /// physically remove files after the runtime gate is dropped
    fn delete_obsolete_files(&self, kind: FileKind, kept: HashMap<u64, (Delete, Vec<u64>)>) {
        #[cfg(feature = "metrics")]
        let mut total = 0;
        for (_, (unlinked, _)) in kept {
            #[cfg(feature = "metrics")]
            {
                total += unlinked.len();
            }
            self.store.manifest.save_obsolete_files(kind, &unlinked);
            must_ok!(self.store.manifest.delete_files());
        }
        #[cfg(feature = "metrics")]
        self.store
            .opt
            .observer
            .counter(Self::obsolete_counter(kind), total as u64);
        self.file_runs(kind).fetch_add(1, Relaxed);
    }
}
