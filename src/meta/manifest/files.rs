use super::*;
use crate::utils::diag;

impl Manifest {
    pub(crate) fn mark_retired_stats(&self, kind: FileKind, bucket_id: u64, file_ids: &[u64]) {
        if file_ids.is_empty() {
            return;
        }
        self.retired_stat_keys.mark(kind, bucket_id, file_ids);
        self.observe_retired_stat_keys();
    }

    pub(crate) fn is_retired_stat(&self, kind: FileKind, bucket_id: u64, file_id: u64) -> bool {
        self.retired_stat_keys.contains(kind, bucket_id, file_id)
    }

    pub(crate) fn snapshot_retired_stat_keys(&self, bucket_id: u64) -> RetiredStatFlushSnapshot {
        self.retired_stat_keys.snapshot_bucket(bucket_id)
    }

    pub(crate) fn clear_retired_stat_keys(&self, snapshot: RetiredStatFlushSnapshot) {
        self.retired_stat_keys.clear_snapshot(snapshot);
        self.observe_retired_stat_keys();
    }

    fn observe_retired_stat_keys(&self) {
        #[cfg(feature = "metrics")]
        self.opt.observer.gauge(
            GaugeMetric::RetiredStatKeysCurrent,
            self.retired_stat_keys.len() as i64,
        );
    }

    pub(crate) fn stage_orphan_file(&self, kind: FileKind, file_id: u64) {
        self.stage_orphan_marker(self.orphan_marker_key(kind, file_id), kind, file_id);
        #[cfg(feature = "metrics")]
        {
            self.opt.observer.counter(self.orphan_stage_metric(kind), 1);
            self.opt.observer.event(ObserveEvent {
                kind: self.orphan_stage_event(kind),
                bucket_id: 0,
                txid: 0,
                file_id,
                value: 0,
            });
        }
    }

    pub(crate) fn clear_orphan_file(&self, kind: FileKind, txn: &mut Txn<'_>, file_id: u64) {
        txn.del(BUCKET_MISC, self.orphan_marker_key(kind, file_id));
        #[cfg(feature = "metrics")]
        {
            self.opt.observer.counter(self.orphan_clear_metric(kind), 1);
            self.opt.observer.event(ObserveEvent {
                kind: self.orphan_clear_event(kind),
                bucket_id: 0,
                txid: 0,
                file_id,
                value: 0,
            });
        }
    }

    pub(crate) fn stage_unsynced_file(&self, kind: FileKind, file_id: u64) {
        self.unsynced_set(kind).write().insert(file_id);
    }

    pub(crate) fn clear_synced_files(&self, kind: FileKind) {
        self.unsynced_set(kind).write().clear();
    }

    pub(crate) fn is_unsynced_file(&self, kind: FileKind, file_id: u64) -> bool {
        self.unsynced_set(kind).read().contains(&file_id)
    }

    fn stage_orphan_marker(&self, key: Vec<u8>, kind: FileKind, file_id: u64) {
        let v = [];
        self.btree
            .exec(BUCKET_MISC, |txn| txn.put(&key, v))
            .unwrap_or_else(|e| {
                panic!(
                    "failed to stage {} orphan marker for file {}: {:?}",
                    self.kind_name(kind),
                    file_id,
                    e
                )
            });
    }

    fn orphan_marker_key(&self, kind: FileKind, file_id: u64) -> Vec<u8> {
        match kind {
            FileKind::Data => orphan_data_marker_key(file_id),
            FileKind::Blob => orphan_blob_marker_key(file_id),
        }
    }

    #[cfg(feature = "metrics")]
    fn orphan_stage_metric(&self, kind: FileKind) -> CounterMetric {
        match kind {
            FileKind::Data => CounterMetric::FlushOrphanDataStaged,
            FileKind::Blob => CounterMetric::FlushOrphanBlobStaged,
        }
    }

    #[cfg(feature = "metrics")]
    fn orphan_clear_metric(&self, kind: FileKind) -> CounterMetric {
        match kind {
            FileKind::Data => CounterMetric::FlushOrphanDataCleared,
            FileKind::Blob => CounterMetric::FlushOrphanBlobCleared,
        }
    }

    #[cfg(feature = "metrics")]
    fn orphan_stage_event(&self, kind: FileKind) -> EventKind {
        match kind {
            FileKind::Data => EventKind::FlushOrphanDataStaged,
            FileKind::Blob => EventKind::FlushOrphanBlobStaged,
        }
    }

    #[cfg(feature = "metrics")]
    fn orphan_clear_event(&self, kind: FileKind) -> EventKind {
        match kind {
            FileKind::Data => EventKind::FlushOrphanDataCleared,
            FileKind::Blob => EventKind::FlushOrphanBlobCleared,
        }
    }

    fn unsynced_set(&self, kind: FileKind) -> &RwLock<HashSet<u64>> {
        &self.file_state(kind).unsync
    }

    fn kind_name(&self, kind: FileKind) -> &'static str {
        match kind {
            FileKind::Data => "data",
            FileKind::Blob => "blob",
        }
    }

    fn load_file(&self, kind: FileKind, bucket_id: u64, addr: u64) -> BoxRef {
        let ctx = self.get_bucket_context_must_exist(bucket_id);
        self.file_state(kind).stat.load(addr, &ctx)
    }

    pub(crate) fn load_data<C>(&self, bucket_id: u64, addr: u64, cache: C) -> BoxRef
    where
        C: Fn(BoxRef),
    {
        let b = self.load_file(FileKind::Data, bucket_id, addr);
        cache(b.clone());
        b
    }

    pub(crate) fn load_blob<C>(&self, bucket_id: u64, addr: u64, cache: C) -> BoxRef
    where
        C: Fn(BoxView),
    {
        let b = self.load_file(FileKind::Blob, bucket_id, addr);
        cache(b.view());
        b
    }

    pub(crate) fn save_obsolete_files(&self, kind: FileKind, id: &[u64]) {
        if !id.is_empty() {
            let mut lk = self.file_state(kind).obsolete.lock();
            lk.extend_from_slice(id);
        }
    }

    /// remove the obsolete files recorded in the manifest.
    ///
    /// the open path propagates a failed directory sync (the caller can still fail the open);
    /// runtime callers keep their fail-stop by wrapping the result
    pub(crate) fn delete_files(&self) -> Result<(), OpCode> {
        // a running checkpoint suspends deletions: skip the whole phase so that no
        // file is unlinked and no `*DeleteDone` record advances. the obsolete ids stay
        // in memory (and their durable `Delete` records stay in the manifest), so the
        // next call retries exactly this batch.
        let Some(_work) = self.deletions.begin() else {
            return Ok(());
        };
        let mut deleted = [Vec::new(), Vec::new()];
        for kind in FileKind::ALL {
            let mut lk = self.file_state(kind).obsolete.lock();
            lk.retain(|&id| {
                let path = stat_file_path(&self.opt, kind, id);
                match self.opt.fs.remove_file_if_exists(&path) {
                    Ok(()) => {
                        deleted[kind.slot()].push(id);
                        false
                    }
                    Err(err) => {
                        diag::warn!("obsolete stat remove failed, path={path:?}, error={err:?}");
                        true
                    }
                }
            });
        }

        if FileKind::ALL
            .iter()
            .any(|&kind| !deleted[kind.slot()].is_empty())
        {
            self.opt.try_sync_data_dir()?;
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_delete_files_after_dir_sync_before_meta_commit");

            let mut txn = self.begin();
            for kind in FileKind::ALL {
                let ids = &mut deleted[kind.slot()];
                if ids.is_empty() {
                    continue;
                }
                txn.record(
                    delete_done_meta_kind(kind),
                    &Delete::from(std::mem::take(ids)),
                );
            }
            txn.commit();
        }
        Ok(())
    }

    pub(crate) fn add_stat(&self, kind: FileKind, stat: MemStat, ivl: IntervalPair) {
        if let Some(ctx) = self.buckets.context(stat.bucket_id) {
            stat_intervals(kind, &ctx)
                .write()
                .insert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
        }
        self.file_state(kind).stat.add_stat_mem(stat);
    }

    pub(crate) fn prepare_stat_intervals(
        &self,
        kind: FileKind,
        fstats: Vec<MemStat>,
        relocs: HashMap<u64, (u64, LenSeq)>,
        cut: &PublishCut<'_>,
    ) -> (Vec<MemStat>, Vec<PersistStat>) {
        self.file_state(kind)
            .stat
            .prepare_stat_intervals(fstats, relocs, cut)
    }

    pub(crate) fn publish_stat_intervals(
        &self,
        kind: FileKind,
        fstats: Vec<MemStat>,
        obsoleted: &[u64],
        del_intervals: &[u64],
        remap_intervals: &[IntervalPair],
    ) {
        if let Some(ctx) = self.buckets.context(fstats[0].bucket_id) {
            let mut lk = stat_intervals(kind, &ctx).write();
            for &lo in del_intervals {
                lk.remove(lo);
            }
            // split rewrite replaces each old interval with one or more output sub-intervals,
            // so these lower bounds are new keys rather than existing entries to update
            for i in remap_intervals {
                lk.insert(i.lo_addr, i.hi_addr, i.file_id);
            }
        }
        self.file_state(kind)
            .stat
            .publish_stat_intervals(fstats, obsoleted);
    }

    pub(crate) fn remove_retired_stat_intervals(
        &self,
        kind: FileKind,
        bucket_id: u64,
        retired: &[u64],
        del_intervals: &[u64],
    ) {
        if let Some(ctx) = self.buckets.context(bucket_id) {
            let mut lk = stat_intervals(kind, &ctx).write();
            for &lo in del_intervals {
                lk.remove(lo);
            }
        }
        self.file_state(kind).stat.remove_stat_interval(retired);
    }

    pub(crate) fn apply_junks(
        &self,
        kind: FileKind,
        permit: &crate::meta::JunkPermit<'_>,
        bucket_id: u64,
        tick: u64,
        junks: &[u64],
    ) -> Result<Vec<PersistStat>, OpCode> {
        if junks.is_empty() {
            return Ok(Vec::new());
        }
        let ctx = self.get_bucket_context_must_exist(bucket_id);
        let is_retired = |file_id: u64| {
            let retired = self.is_retired_stat(kind, bucket_id, file_id);
            #[cfg(feature = "metrics")]
            if retired {
                self.opt.observer.counter(self.skip_retired_metric(kind), 1);
            }
            retired
        };
        JunkApplier::new(
            &self.file_state(kind).stat,
            &self.junk,
            &ctx,
            &self.btree,
            &is_retired,
        )
        .apply(permit, tick, junks)
    }

    #[cfg(feature = "metrics")]
    fn skip_retired_metric(&self, kind: FileKind) -> CounterMetric {
        match kind {
            FileKind::Data => CounterMetric::FlushSkipRetiredDataStat,
            FileKind::Blob => CounterMetric::FlushSkipRetiredBlobStat,
        }
    }
}

pub(crate) struct FileState {
    pub(crate) stat: StatCtx,
    pub(crate) obsolete: Mutex<Vec<u64>>,
    pub(crate) unsync: RwLock<HashSet<u64>>,
}

impl FileState {
    pub(crate) fn new(opt: Arc<ParsedOptions>, kind: FileKind) -> Self {
        Self {
            stat: StatCtx::new(opt, kind),
            obsolete: Mutex::new(Vec::new()),
            unsync: RwLock::new(HashSet::new()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RetiredStatKind {
    Data,
    Blob,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct RetiredStatKey {
    kind: RetiredStatKind,
    bucket_id: u64,
    file_id: u64,
}

#[derive(Default)]
pub(crate) struct RetiredStatKeys {
    keys: DashSet<RetiredStatKey>,
}

pub(crate) struct RetiredStatFlushSnapshot {
    keys: Vec<RetiredStatKey>,
}

impl RetiredStatKeys {
    pub(crate) fn mark(&self, kind: FileKind, bucket_id: u64, file_ids: &[u64]) -> usize {
        let mut inserted = 0;
        let kind = Self::kind(kind);
        for &file_id in file_ids {
            if self.keys.insert(RetiredStatKey {
                kind,
                bucket_id,
                file_id,
            }) {
                inserted += 1;
            }
        }
        inserted
    }

    pub(crate) fn contains(&self, kind: FileKind, bucket_id: u64, file_id: u64) -> bool {
        self.keys.contains(&RetiredStatKey {
            kind: Self::kind(kind),
            bucket_id,
            file_id,
        })
    }

    pub(crate) fn snapshot_bucket(&self, bucket_id: u64) -> RetiredStatFlushSnapshot {
        let keys = self
            .keys
            .iter()
            .filter_map(|entry| {
                let key = *entry.key();
                (key.bucket_id == bucket_id).then_some(key)
            })
            .collect();
        RetiredStatFlushSnapshot { keys }
    }

    pub(crate) fn clear_snapshot(&self, snapshot: RetiredStatFlushSnapshot) -> usize {
        let mut removed = 0;
        for key in snapshot.keys {
            if self.keys.remove(&key).is_some() {
                removed += 1;
            }
        }
        removed
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    fn kind(kind: FileKind) -> RetiredStatKind {
        match kind {
            FileKind::Data => RetiredStatKind::Data,
            FileKind::Blob => RetiredStatKind::Blob,
        }
    }
}
