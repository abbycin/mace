use btree_store::BTree;
use dashmap::{DashMap, DashSet};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{
            AtomicU32, AtomicU64,
            Ordering::{AcqRel, Acquire, Relaxed},
        },
        mpsc::{Receiver, Sender},
    },
};

use crate::{
    Options,
    cc::context::Context,
    map::{
        IDataReader, SharedState,
        buffer::{BucketContext, BucketMgr},
        flush::CheckpointObserver,
        table::{BucketState, PageMap},
    },
    meta::{
        BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_OBSOLETE_BLOB, BUCKET_OBSOLETE_DATA, Sequences,
        entry::{IMetaCodec, MetaOp},
    },
    must_exist, must_ok, must_true,
    types::refbox::{BoxRef, BoxView},
    utils::{
        Handle, INIT_ADDR, MutRef, OpCode,
        compress::CompressorPool,
        data::{GroupPositions, LenSeq, Position, init_group_pos},
        observe::{CounterMetric, EventKind, GaugeMetric, ObserveEvent},
        options::{BucketOptions, ParsedOptions, PersistedOptions},
    },
};

use super::{
    BUCKET_FRONTIER, BUCKET_METAS, BUCKET_MISC, BUCKET_PENDING_DEL, BUCKET_VERSION,
    BucketDurableFrontier, BucketMeta, CURRENT_VERSION, Delete, FileKind, IntervalPair,
    MAX_BUCKETS, MemStat, MetaKind, OPTIONS_KEY, PersistStat, VERSION_KEY, WalRecycleIntent,
    WalRecycleState, delete_done_meta_kind, delete_meta_kind, interval_bucket_name,
    orphan_blob_marker_key, orphan_data_marker_key, stat::StatCtx, stat_file_path, stat_intervals,
    txn::Txn, wal_recycle_key,
};

pub(crate) struct Manifest {
    pub(crate) sequences: Arc<Sequences>,
    files: [FileState; 2],
    pub(crate) buckets: Handle<BucketMgr>,
    pub(crate) bucket_metas: DashMap<String, Arc<BucketMeta>>,
    pub(crate) bucket_metas_by_id: DashMap<u64, Arc<BucketMeta>>,
    pub(crate) bucket_frontier: DashMap<u64, GroupPositions>,
    pub(crate) bucket_runtimes: DashMap<u64, Arc<BucketRuntime>>,
    pub(crate) structural_lock: Mutex<()>,
    /// total bucket count including both active/pending_del
    pub(crate) nr_buckets: AtomicU64,
    retired_stat_keys: RetiredStatKeys,
    pub(crate) opt: Arc<ParsedOptions>,
    pub(crate) btree: BTree,
}

enum BucketRemoveMode {
    Drop,
    Delete,
}

impl Drop for Manifest {
    fn drop(&mut self) {
        self.buckets.abort();
        self.buckets.reclaim();
    }
}

impl Manifest {
    pub(crate) fn new(opt: Arc<ParsedOptions>, tx: Sender<SharedState>, rx: Receiver<()>) -> Self {
        let path = opt.manifest();
        let btree = must_ok!(BTree::open(path), "can't open btree-store");
        let buckets_to_ensure = [
            BUCKET_MISC,
            BUCKET_DATA_STAT,
            BUCKET_BLOB_STAT,
            BUCKET_OBSOLETE_DATA,
            BUCKET_OBSOLETE_BLOB,
            BUCKET_METAS,
            BUCKET_PENDING_DEL,
            BUCKET_FRONTIER,
            BUCKET_VERSION,
        ];

        for name in buckets_to_ensure {
            must_ok!(btree.exec(name, |_| Ok(())), "can't ensure bucket exists");
        }

        Self {
            sequences: Arc::new(Sequences::default()),
            files: [
                FileState::new(opt.clone(), FileKind::Data),
                FileState::new(opt.clone(), FileKind::Blob),
            ],
            buckets: Handle::new(BucketMgr::new(
                opt.clone(),
                Handle::from(std::ptr::null_mut()),
                tx,
                rx,
            )),
            bucket_metas: DashMap::new(),
            bucket_metas_by_id: DashMap::new(),
            bucket_frontier: DashMap::new(),
            bucket_runtimes: DashMap::new(),
            structural_lock: Mutex::new(()),
            retired_stat_keys: RetiredStatKeys::default(),
            nr_buckets: AtomicU64::new(0),
            opt,
            btree,
        }
    }

    pub(crate) fn load_persisted_options_if_present(
        &self,
    ) -> Result<Option<PersistedOptions>, OpCode> {
        match self.btree.view(BUCKET_MISC, |txn| txn.get(OPTIONS_KEY)) {
            Ok(raw) => PersistedOptions::from_json(&raw).map(Some),
            Err(btree_store::Error::NotFound) => Ok(None),
            Err(other) => Err(OpCode::from(other)),
        }
    }

    pub(crate) fn store_persisted_options(
        &self,
        persisted: &PersistedOptions,
    ) -> Result<(), OpCode> {
        let raw = persisted.to_json()?;
        self.btree
            .exec(BUCKET_MISC, |txn| txn.put(OPTIONS_KEY, raw.as_slice()))
            .map_err(OpCode::from)
    }

    pub(crate) fn bootstrap_persisted_options(
        &self,
        persisted: &PersistedOptions,
    ) -> Result<(), OpCode> {
        let raw = persisted.to_json()?;
        self.btree
            .exec_multi(|multi| {
                multi.exec(BUCKET_VERSION, |txn| {
                    txn.put(VERSION_KEY, CURRENT_VERSION.to_le_bytes())
                })?;
                multi.exec(BUCKET_MISC, |txn| txn.put(OPTIONS_KEY, raw.as_slice()))?;
                Ok(())
            })
            .map_err(OpCode::from)
    }

    pub(crate) fn set_context(
        &self,
        ctx: Handle<Context>,
        reader: Arc<dyn IDataReader>,
        observer: Arc<dyn CheckpointObserver>,
    ) {
        unsafe {
            let mgr = &mut *self.buckets.inner();
            mgr.start(ctx, reader, observer);
        }
    }

    pub(crate) fn abort(&mut self) {
        self.buckets.abort();
    }

    fn bucket_is_empty(&self, bucket: &str) -> Result<bool, OpCode> {
        self.btree
            .view(bucket, |txn| {
                let mut iter = txn.iter_uncached();
                let mut k = Vec::new();
                let mut v = Vec::new();
                Ok(!iter.next_ref(&mut k, &mut v))
            })
            .map_err(OpCode::from)
    }

    pub(crate) fn is_pristine_uninitialized(&self) -> Result<bool, OpCode> {
        for bucket in [
            BUCKET_MISC,
            BUCKET_DATA_STAT,
            BUCKET_BLOB_STAT,
            BUCKET_OBSOLETE_DATA,
            BUCKET_OBSOLETE_BLOB,
            BUCKET_METAS,
            BUCKET_PENDING_DEL,
            BUCKET_FRONTIER,
            BUCKET_VERSION,
        ] {
            if !self.bucket_is_empty(bucket)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn get_bucket_context_must_exist(&self, bucket_id: u64) -> Arc<BucketContext> {
        must_exist!(self.buckets.buckets.get(&bucket_id), "must exist")
            .value()
            .clone()
    }

    pub(crate) fn get_bucket_runtime(&self, bucket_id: u64) -> Arc<BucketRuntime> {
        must_exist!(
            self.bucket_runtimes
                .get(&bucket_id)
                .map(|runtime| runtime.value().clone()),
            "must exist"
        )
    }

    // bucket runtime remains alive across load/unload so background GC can read lifecycle state
    // and reuse compression contexts without recreating the full bucket context
    pub(crate) fn ensure_bucket_runtime(&self, bucket_id: u64) -> Arc<BucketRuntime> {
        self.bucket_runtimes
            .entry(bucket_id)
            .or_insert_with(|| Arc::new(BucketRuntime::new()))
            .value()
            .clone()
    }

    pub(crate) fn current_group_checkpoints(&self) -> GroupPositions {
        let mut out = init_group_pos();
        let groups = self.buckets.ctx.groups();
        let n = groups.len().min(Options::MAX_CONCURRENT_WRITE as usize);
        for i in 0..n {
            out[i] = groups[i].logging.lock().last_ckpt();
        }
        out
    }

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
        self.opt.observer.gauge(
            GaugeMetric::RetiredStatKeysCurrent,
            self.retired_stat_keys.len() as i64,
        );
    }

    pub(crate) fn stat_ctx(&self, kind: FileKind) -> &StatCtx {
        &self.file_state(kind).stat
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
        for meta in self.bucket_metas_by_id.iter() {
            let bucket_id = *meta.key();
            let bucket_ctx = self.buckets.buckets.get(&bucket_id);
            let has_pending_flush = bucket_ctx
                .as_ref()
                .is_some_and(|ctx| ctx.value().has_pending_flush_data());
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

    pub(crate) fn begin(&self) -> Txn<'_> {
        Txn::new(self)
    }

    pub(crate) fn file_state(&self, kind: FileKind) -> &FileState {
        &self.files[kind.slot()]
    }

    pub(crate) fn vacuum_meta(
        &self,
        target_bytes: u64,
    ) -> Result<btree_store::CompactStats, OpCode> {
        self.btree.compact(target_bytes).map_err(OpCode::from)
    }

    fn load_bucket_meta_locked(&self, name: &str) -> Result<Arc<BucketMeta>, OpCode> {
        if let Some(meta) = self.bucket_metas.get(name) {
            return Ok(meta.clone());
        }

        let mut meta = None;
        let _ = self.btree.view(BUCKET_METAS, |txn| {
            if let Ok(v) = txn.get(name.as_bytes()) {
                meta = Some(Arc::new(BucketMeta::decode(&v)));
            }
            Ok(())
        });

        let meta = meta.ok_or(OpCode::NotFound)?;
        self.bucket_metas.insert(name.to_string(), meta.clone());
        self.bucket_metas_by_id.insert(meta.id, meta.clone());
        self.ensure_bucket_runtime(meta.id);
        Ok(meta)
    }

    pub(crate) fn load_bucket_meta(&self, name: &str) -> Result<Arc<BucketMeta>, OpCode> {
        if let Some(meta) = self.bucket_metas.get(name) {
            return Ok(meta.clone());
        }

        let _lock = self.structural_lock.lock();
        self.load_bucket_meta_locked(name)
    }

    pub(crate) fn create_bucket(
        &self,
        name: &str,
        opt: BucketOptions,
    ) -> Result<(Arc<BucketMeta>, Arc<BucketContext>), OpCode> {
        let _lock = self.structural_lock.lock();

        match self.load_bucket_meta_locked(name) {
            Ok(_) => return Err(OpCode::Exist),
            Err(OpCode::NotFound) => {}
            Err(e) => return Err(e),
        }

        if self.nr_buckets.load(Relaxed) >= MAX_BUCKETS {
            return Err(OpCode::NoSpace);
        }

        let bucket_id = self.sequences.next_bucket_id.fetch_add(1, Relaxed);
        self.nr_buckets.fetch_add(1, Relaxed);

        let meta = Arc::new(BucketMeta {
            id: bucket_id,
            options: opt,
        });
        // publish meta early so context creation sees it
        self.bucket_metas.insert(name.to_string(), meta.clone());
        self.bucket_metas_by_id.insert(bucket_id, meta.clone());

        // ensure state and pagemap are initialized
        let bucket_ctx = self.load_bucket_context_locked(bucket_id);
        let frontier = self.current_group_checkpoints();
        self.bucket_frontier.insert(bucket_id, frontier);
        let frontier_rec = BucketDurableFrontier::new(bucket_id, frontier);

        let mut buf = vec![0u8; meta.as_ref().packed_size()];
        let mut txn = self.begin();
        txn.record(MetaKind::Sequences, self.sequences.as_ref());
        txn.record(MetaKind::BucketFrontier, &frontier_rec);
        meta.as_ref().encode(&mut buf);
        txn.ops_mut()
            .entry(BUCKET_METAS.to_string())
            .or_default()
            .push(MetaOp::Put(name.as_bytes().to_vec(), buf));
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_create_before_manifest_commit");
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_create_after_manifest_commit");

        Ok((meta, bucket_ctx))
    }

    fn bucket_option_conflicts(old: BucketOptions, new: BucketOptions) -> Vec<&'static str> {
        let mut conflicts = Vec::new();
        if old.inline_size != new.inline_size {
            conflicts.push("inline_size");
        }
        if old.split_elems != new.split_elems {
            conflicts.push("split_elems");
        }
        conflicts
    }

    pub(crate) fn update_bucket_options(
        &self,
        name: &str,
        opt: BucketOptions,
    ) -> Result<(), OpCode> {
        let _lock = self.structural_lock.lock();
        let meta = self.load_bucket_meta_locked(name)?;
        let bucket_id = meta.id;
        let runtime = self.get_bucket_runtime(bucket_id);

        if self.buckets.buckets.contains_key(&bucket_id) || runtime.has_rewrite() {
            log::info!(
                "bucket {}({}) is busy, reject bucket option update",
                name,
                bucket_id
            );
            return Err(OpCode::Again);
        }

        if meta.options == opt {
            return Ok(());
        }

        let conflicts = Self::bucket_option_conflicts(meta.options, opt);
        if !conflicts.is_empty() {
            log::error!(
                "bucket {}({}) option update conflicts on [{}], old: {:?}, new: {:?}",
                name,
                bucket_id,
                conflicts.join(", "),
                meta.options,
                opt
            );
            return Err(OpCode::Invalid);
        }

        let new_meta = Arc::new(BucketMeta {
            id: bucket_id,
            options: opt,
        });
        let mut buf = vec![0u8; new_meta.as_ref().packed_size()];
        new_meta.as_ref().encode(&mut buf);

        let mut txn = self.begin();
        txn.ops_mut()
            .entry(BUCKET_METAS.to_string())
            .or_default()
            .push(MetaOp::Put(name.as_bytes().to_vec(), buf));
        txn.commit();

        self.bucket_metas.insert(name.to_string(), new_meta.clone());
        self.bucket_metas_by_id.insert(bucket_id, new_meta);
        Ok(())
    }

    pub(crate) fn load_bucket_context(&self, bucket_id: u64) -> Result<Arc<BucketContext>, OpCode> {
        if let Some(ctx) = self.buckets.buckets.get(&bucket_id) {
            return Ok(ctx.value().clone());
        }

        let _lock = self.structural_lock.lock();
        if let Some(ctx) = self.buckets.buckets.get(&bucket_id) {
            return Ok(ctx.value().clone());
        }
        if !self.bucket_metas_by_id.contains_key(&bucket_id) {
            return Err(OpCode::NotFound);
        }
        Ok(self.load_bucket_context_locked(bucket_id))
    }

    pub(crate) fn try_acquire_rewrite(&self, bucket_id: u64) -> Option<BucketRewritePermit> {
        let _lock = self.structural_lock.lock();
        let meta = self.bucket_metas_by_id.get(&bucket_id)?;
        let runtime = self.get_bucket_runtime(bucket_id);
        if runtime.state.is_deleting() || runtime.state.is_drop() {
            return None;
        }
        let enable_compression = meta.options.enable_compression;
        Some(BucketRewritePermit {
            enable_compression,
            compressors: runtime.compressors.clone(),
            _guard: runtime.begin_rewrite(),
        })
    }

    fn load_bucket_context_locked(&self, bucket_id: u64) -> Arc<BucketContext> {
        // double check
        if let Some(ctx) = self.buckets.buckets.get(&bucket_id) {
            return ctx.value().clone();
        }

        if !self.bucket_metas_by_id.contains_key(&bucket_id) {
            log::error!("bucket {} missing metadata when loading context", bucket_id);
            panic!("bucket {bucket_id} not found");
        }

        let runtime = self.ensure_bucket_runtime(bucket_id);
        let state = runtime.state.clone();

        // PageMap lazy load
        let table = {
            let table = MutRef::new(PageMap::default());
            if let Some(_meta) = self.bucket_metas_by_id.get(&bucket_id) {
                table.recover(bucket_id, Some(&self.btree));
            }
            table
        };

        let flush = must_exist!(self.buckets.flush.as_ref(), "flusher started").clone();
        let meta = must_exist!(
            self.bucket_metas_by_id.get(&bucket_id),
            "bucket meta must exist"
        )
        .clone();
        let ctx = Arc::new(BucketContext::new(
            self.buckets.ctx,
            &self.opt,
            Arc::new(meta.options),
            state,
            bucket_id,
            table,
            flush,
            self.buckets.lru,
            self.buckets.reader.clone(),
            self.buckets.tx.clone(),
            runtime.compressors.clone(),
        ));

        self.recover_intervals(bucket_id, &ctx);

        self.buckets.buckets.insert(bucket_id, ctx.clone());
        ctx
    }

    fn begin_bucket_remove_locked(
        &self,
        name: &str,
        mode: BucketRemoveMode,
    ) -> Result<u64, OpCode> {
        let meta = self.load_bucket_meta_locked(name)?;
        let bucket_id = meta.id;

        // remove from maps (unpublish)
        self.bucket_metas.remove(name);
        if matches!(mode, BucketRemoveMode::Delete) {
            self.bucket_metas_by_id.remove(&bucket_id);
        }

        let runtime = self.get_bucket_runtime(bucket_id);
        let state = runtime.state.clone();
        let holder_baseline = match mode {
            BucketRemoveMode::Drop => 2,
            BucketRemoveMode::Delete => 1,
        };
        let mut busy = Arc::strong_count(&meta) > holder_baseline || state.is_busy();
        if matches!(mode, BucketRemoveMode::Delete) && state.is_drop() {
            busy = true;
        }
        if busy {
            self.bucket_metas.insert(name.to_string(), meta.clone());
            if matches!(mode, BucketRemoveMode::Delete) {
                self.bucket_metas_by_id.insert(bucket_id, meta);
            }
            return Err(OpCode::Again);
        }

        match mode {
            BucketRemoveMode::Drop => state.set_drop(),
            BucketRemoveMode::Delete => state.set_deleting(),
        }

        if matches!(mode, BucketRemoveMode::Delete) && runtime.has_rewrite() {
            state.clear_deleting();
            self.bucket_metas.insert(name.to_string(), meta.clone());
            self.bucket_metas_by_id.insert(bucket_id, meta);
            return Err(OpCode::Again);
        }

        Ok(bucket_id)
    }

    pub(crate) fn unload_bucket(&self, name: &str) -> Result<(), OpCode> {
        // serialize deletions and creations
        let _lock = self.structural_lock.lock();
        let bucket_id = {
            let meta = self.load_bucket_meta_locked(name)?;
            meta.id
        };
        if self.buckets.ctx.has_pending_abort_clean_bucket(bucket_id) {
            return Err(OpCode::Again);
        }

        // remove from maps (unpublish)
        let bucket_id = self.begin_bucket_remove_locked(name, BucketRemoveMode::Drop)?;

        if let Some(ctx) = self
            .buckets
            .buckets
            .get(&bucket_id)
            .map(|x| x.value().clone())
        {
            ctx.checkpoint_before_reclaim();
        }
        let _ = self.buckets.buckets.remove(&bucket_id);

        self.get_bucket_runtime(bucket_id).state.clear_drop();
        Ok(())
    }

    pub(crate) fn delete_bucket(&self, name: &str) -> Result<(), OpCode> {
        // serialize deletions and creations
        let _lock = self.structural_lock.lock();

        // remove from maps (unpublish)
        let bucket_id = self.begin_bucket_remove_locked(name, BucketRemoveMode::Delete)?;

        // cleanup page maps and resources via manager
        self.buckets.del_bucket(bucket_id);

        // collect and record obsolete files
        let mut files = [Vec::new(), Vec::new()];
        for kind in FileKind::ALL {
            files[kind.slot()] = self
                .stat_ctx(kind)
                .bucket_files()
                .get(&bucket_id)
                .map(|x| x.clone())
                .unwrap_or_default();
        }

        // commit transaction (logical delete)
        let mut txn = self.begin();

        // remove from BUCKET_METAS
        txn.ops_mut()
            .entry(BUCKET_METAS.to_string())
            .or_default()
            .push(MetaOp::Del(name.as_bytes().to_vec()));

        // record for physical cleanup in background
        txn.ops_mut()
            .entry(BUCKET_PENDING_DEL.to_string())
            .or_default()
            .push(MetaOp::Put(bucket_id.to_le_bytes().to_vec(), vec![]));
        if self.bucket_frontier.contains_key(&bucket_id) {
            txn.ops_mut()
                .entry(BUCKET_FRONTIER.to_string())
                .or_default()
                .push(MetaOp::Del(bucket_id.to_le_bytes().to_vec()));
        }

        // record obsolete files
        for kind in FileKind::ALL {
            if files[kind.slot()].is_empty() {
                continue;
            }
            // bucket delete is already unpublished and must not publish retired keys
            txn.record(
                delete_meta_kind(kind),
                &Delete::from(files[kind.slot()].clone()),
            );
        }

        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_delete_before_manifest_commit");
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_delete_after_manifest_commit");

        // in-memory cleanup for stats
        // once a bucket enters pending-delete, its file metadata must no longer participate in
        // GC victim selection; otherwise GC may try to update interval buckets that were already
        // physically dropped by pending-bucket cleanup.
        for kind in FileKind::ALL {
            let ids = &files[kind.slot()];
            self.stat_ctx(kind).remove_stat_interval(ids);
            for &id in ids {
                self.stat_ctx(kind).remove_cache(id);
            }
            self.save_obsolete_files(kind, ids);
        }
        self.bucket_frontier.remove(&bucket_id);
        self.delete_files();

        Ok(())
    }

    pub(crate) fn loaded_bucket_names(&self) -> Vec<String> {
        self.bucket_metas.iter().map(|x| x.key().clone()).collect()
    }

    pub(crate) fn recover_intervals(&self, bucket_id: u64, ctx: &BucketContext) {
        let mut max_addr = INIT_ADDR;

        for kind in FileKind::ALL {
            let interval_table = interval_bucket_name(kind, bucket_id);
            let _ = self.btree.view(&interval_table, |txn| {
                let mut iter = txn.iter_uncached();
                let mut k = Vec::new();
                let mut v = Vec::new();
                let mut map = stat_intervals(kind, ctx).write();
                while iter.next_ref(&mut k, &mut v) {
                    let ivl = IntervalPair::decode(&v);
                    max_addr = max_addr.max(ivl.hi_addr);
                    map.upsert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
                }
                Ok(())
            });
        }

        ctx.state.next_addr.fetch_max(max_addr + 1, Relaxed);
    }

    pub(crate) fn stage_orphan_file(&self, kind: FileKind, file_id: u64) {
        self.stage_orphan_marker(self.orphan_marker_key(kind, file_id), kind, file_id);
        self.opt.observer.counter(self.orphan_stage_metric(kind), 1);
        self.opt.observer.event(ObserveEvent {
            kind: self.orphan_stage_event(kind),
            bucket_id: 0,
            txid: 0,
            file_id,
            value: 0,
        });
    }

    pub(crate) fn clear_orphan_file(&self, kind: FileKind, txn: &mut Txn<'_>, file_id: u64) {
        txn.ops_mut()
            .entry(BUCKET_MISC.to_string())
            .or_default()
            .push(MetaOp::Del(self.orphan_marker_key(kind, file_id)));
        self.opt.observer.counter(self.orphan_clear_metric(kind), 1);
        self.opt.observer.event(ObserveEvent {
            kind: self.orphan_clear_event(kind),
            bucket_id: 0,
            txid: 0,
            file_id,
            value: 0,
        });
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

    pub(crate) fn load_wal_recycle_state(&self, group_id: u8) -> WalRecycleState {
        let key = wal_recycle_key(group_id);
        match self.btree.view(BUCKET_MISC, |txn| txn.get(&key)) {
            Ok(val) => WalRecycleState::decode(&val),
            Err(btree_store::Error::NotFound) => WalRecycleState::none(group_id),
            Err(err) => panic!("load wal recycle state failed for group {group_id}: {err:?}"),
        }
    }

    pub(crate) fn record_wal_recycle_state(&self, txn: &mut Txn<'_>, state: WalRecycleState) {
        let mut buf = vec![0u8; state.packed_size()];
        state.encode(&mut buf);
        txn.ops_mut()
            .entry(BUCKET_MISC.to_string())
            .or_default()
            .push(MetaOp::Put(wal_recycle_key(state.group_id), buf));
    }

    pub(crate) fn commit_wal_recycle_intent(&self, intent: WalRecycleIntent) {
        let mut txn = self.begin();
        self.record_wal_recycle_state(
            &mut txn,
            WalRecycleState::intent(intent.group_id, intent.from_file_id, intent.to_file_id),
        );
        txn.commit();
    }

    pub(crate) fn commit_wal_recycle_done(&self, intent: WalRecycleIntent) {
        let mut txn = self.begin();
        self.record_wal_recycle_state(
            &mut txn,
            WalRecycleState::done(intent.group_id, intent.from_file_id, intent.to_file_id),
        );
        txn.commit();
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

    fn orphan_stage_metric(&self, kind: FileKind) -> CounterMetric {
        match kind {
            FileKind::Data => CounterMetric::FlushOrphanDataStaged,
            FileKind::Blob => CounterMetric::FlushOrphanBlobStaged,
        }
    }

    fn orphan_clear_metric(&self, kind: FileKind) -> CounterMetric {
        match kind {
            FileKind::Data => CounterMetric::FlushOrphanDataCleared,
            FileKind::Blob => CounterMetric::FlushOrphanBlobCleared,
        }
    }

    fn orphan_stage_event(&self, kind: FileKind) -> EventKind {
        match kind {
            FileKind::Data => EventKind::FlushOrphanDataStaged,
            FileKind::Blob => EventKind::FlushOrphanBlobStaged,
        }
    }

    fn orphan_clear_event(&self, kind: FileKind) -> EventKind {
        match kind {
            FileKind::Data => EventKind::FlushOrphanDataCleared,
            FileKind::Blob => EventKind::FlushOrphanBlobCleared,
        }
    }

    fn unsynced_set(&self, kind: FileKind) -> &parking_lot::RwLock<std::collections::HashSet<u64>> {
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
        self.stat_ctx(kind).load(addr, &ctx)
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

    pub(crate) fn delete_files(&self) {
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
                        log::warn!("obsolete stat remove failed, path={path:?}, error={err:?}");
                        true
                    }
                }
            });
        }

        if FileKind::ALL
            .iter()
            .any(|&kind| !deleted[kind.slot()].is_empty())
        {
            self.opt.sync_data_dir();
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
    }

    pub(crate) fn add_stat(&self, kind: FileKind, stat: MemStat, ivl: IntervalPair) {
        if let Some(ctx) = self.buckets.buckets.get(&stat.bucket_id) {
            stat_intervals(kind, ctx.value())
                .write()
                .insert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
        }
        self.stat_ctx(kind).add_stat_mem(stat);
    }

    pub(crate) fn update_stat_interval(
        &self,
        kind: FileKind,
        fstat: MemStat,
        relocs: HashMap<u64, LenSeq>,
        obsoleted: &[u64],
        del_intervals: &[u64],
        remap_intervals: &[IntervalPair],
    ) -> PersistStat {
        let bucket_id = fstat.bucket_id;
        if let Some(ctx) = self.buckets.buckets.get(&bucket_id) {
            let mut lk = stat_intervals(kind, ctx.value()).write();
            for &lo in del_intervals {
                lk.remove(lo);
            }
            for i in remap_intervals {
                lk.update(i.lo_addr, i.hi_addr, i.file_id);
            }
        }

        self.stat_ctx(kind)
            .update_stat_interval(fstat, relocs, obsoleted)
    }

    pub(crate) fn apply_junks(
        &self,
        kind: FileKind,
        bucket_id: u64,
        tick: u64,
        junks: &[u64],
    ) -> Vec<PersistStat> {
        if junks.is_empty() {
            return Vec::new();
        }
        let ctx = self.get_bucket_context_must_exist(bucket_id);
        self.stat_ctx(kind)
            .apply_junks(tick, junks, &ctx, &self.btree, |file_id| {
                let retired = self.is_retired_stat(kind, bucket_id, file_id);
                if retired {
                    self.opt.observer.counter(self.skip_retired_metric(kind), 1);
                }
                retired
            })
    }

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

pub(crate) struct BucketRuntime {
    pub(crate) state: MutRef<BucketState>,
    pub(crate) compressors: Arc<CompressorPool>,
    rewrite_inflight: AtomicU32,
    update_epochs: [AtomicU64; 2],
}

impl BucketRuntime {
    pub(crate) fn new() -> Self {
        Self {
            state: MutRef::new(BucketState::new()),
            compressors: CompressorPool::new(),
            rewrite_inflight: AtomicU32::new(0),
            update_epochs: [AtomicU64::new(1), AtomicU64::new(1)],
        }
    }

    pub(crate) fn begin_rewrite(self: &Arc<Self>) -> BucketRewriteGuard {
        self.rewrite_inflight.fetch_add(1, AcqRel);
        BucketRewriteGuard {
            runtime: self.clone(),
        }
    }

    pub(crate) fn has_rewrite(&self) -> bool {
        self.rewrite_inflight.load(Acquire) != 0
    }

    pub(crate) fn observe_stat_epoch(&self, kind: FileKind, up1: u64, up2: u64) {
        let epoch = up1.max(up2).saturating_add(1).max(1);
        self.update_epochs[kind.slot()].fetch_max(epoch, Relaxed);
    }

    pub(crate) fn load_update_epoch(&self, kind: FileKind) -> u64 {
        self.update_epochs[kind.slot()].load(Relaxed)
    }

    pub(crate) fn next_update_epoch(&self, kind: FileKind) -> u64 {
        self.update_epochs[kind.slot()].fetch_add(1, Relaxed)
    }
}

pub(crate) struct BucketRewriteGuard {
    runtime: Arc<BucketRuntime>,
}

impl Drop for BucketRewriteGuard {
    fn drop(&mut self) {
        self.runtime.rewrite_inflight.fetch_sub(1, AcqRel);
    }
}

pub(crate) struct BucketRewritePermit {
    pub(crate) enable_compression: bool,
    pub(crate) compressors: Arc<CompressorPool>,
    pub(crate) _guard: BucketRewriteGuard,
}
