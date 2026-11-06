use std::sync::atomic::Ordering::Release;

use super::*;
use crate::utils::diag;

/// how an in-place lifecycle transition takes a bucket out of service
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BucketRemoveMode {
    /// manual unload: the bucket stays named and becomes active again
    Unload,
    /// logical delete: the bucket leaves the catalog and waits for physical cleanup
    Delete,
}

/// the single owner of a bucket's durable identity
///
/// `names` is the active-name catalog, `options` the persisted options per id, `pending` the ids
/// whose logical delete committed but whose page/interval cleanup has not finished, and
/// `nr_buckets` the combined count. No bucket context or runtime state is stored here; the
/// structural lock coordinates lifecycle transitions on runtimes owned by the bucket manager.
pub(crate) struct BucketCatalog {
    structural_lock: Mutex<()>,
    names: DashMap<String, u64>,
    options: DashMap<u64, PersistedBucketOptions>,
    pending: DashSet<u64>,
    nr_buckets: AtomicU64,
}

impl BucketCatalog {
    pub(crate) fn new() -> Self {
        Self {
            structural_lock: Mutex::new(()),
            names: DashMap::new(),
            options: DashMap::new(),
            pending: DashSet::new(),
            nr_buckets: AtomicU64::new(0),
        }
    }

    /// loads and validates the persisted catalog, returning it with the highest stored bucket id
    ///
    /// duplicate ids, undecodable metadata and malformed pending keys reject the open: the same
    /// identity may never be handed out twice
    pub(crate) fn load(btree: &BTree) -> Result<(Self, Option<u64>), OpCode> {
        let catalog = Self::new();
        let mut error = None;
        let mut max_id: Option<u64> = None;
        btree
            .view(BUCKET_METAS, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let meta = match BucketMeta::try_decode(&v) {
                        Ok(meta) => meta,
                        Err(err) => {
                            error = Some(err);
                            return Ok(());
                        }
                    };
                    let name = match std::str::from_utf8(&k) {
                        Ok(name) => name,
                        Err(_) => {
                            error = Some(OpCode::Corruption);
                            return Ok(());
                        }
                    };
                    if catalog.options.insert(meta.id, meta.options).is_some() {
                        error = Some(OpCode::Corruption);
                        return Ok(());
                    }
                    catalog.names.insert(name.to_string(), meta.id);
                    max_id = Some(max_id.map_or(meta.id, |id| id.max(meta.id)));
                }
                Ok(())
            })
            .map_err(OpCode::from)?;
        if let Some(err) = error {
            return Err(err);
        }

        btree
            .view(BUCKET_PENDING_DEL, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let key_id: [u8; 8] = match k.as_slice().try_into() {
                        Ok(bytes) => bytes,
                        Err(_) => {
                            error = Some(OpCode::Corruption);
                            return Ok(());
                        }
                    };
                    if !catalog.pending.insert(u64::from_le_bytes(key_id)) {
                        error = Some(OpCode::Corruption);
                        return Ok(());
                    }
                }
                Ok(())
            })
            .map_err(OpCode::from)?;
        if let Some(err) = error {
            return Err(err);
        }

        catalog.nr_buckets.store(
            (catalog.names.len() + catalog.pending.len()) as u64,
            Release,
        );
        Ok((catalog, max_id))
    }

    pub(crate) fn lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.structural_lock.lock()
    }

    pub(crate) fn nr_buckets(&self) -> u64 {
        let _lock = self.structural_lock.lock();
        self.nr_buckets.load(Acquire)
    }

    /// the active bucket names: an unloaded bucket stays listed, a logically deleted one does not
    pub(crate) fn active_names(&self) -> Vec<String> {
        let _lock = self.structural_lock.lock();
        self.names.iter().map(|x| x.key().clone()).collect()
    }

    pub(crate) fn ids(&self) -> Vec<u64> {
        self.options.iter().map(|x| *x.key()).collect()
    }

    pub(crate) fn id(&self, name: &str) -> Option<u64> {
        self.names.get(name).map(|x| *x.value())
    }

    pub(crate) fn contains(&self, bucket_id: u64) -> bool {
        self.options.contains_key(&bucket_id)
    }

    pub(crate) fn options(&self, bucket_id: u64) -> Option<PersistedBucketOptions> {
        self.options.get(&bucket_id).map(|x| *x.value())
    }

    pub(crate) fn meta(&self, bucket_id: u64) -> Option<BucketMeta> {
        Some(BucketMeta {
            id: bucket_id,
            options: self.options(bucket_id)?,
        })
    }

    /// resolve a name to an active bucket and take its lease
    ///
    /// the whole resolution - name to id, lifecycle check, lease acquisition - runs inside the
    /// structural lock that also serializes unload and delete, so a lease can never be handed out
    /// for a bucket that is already leaving. The lock is released before the caller loads the
    /// context: context loading re-enters `load_bucket_context` and must not nest the lock.
    pub(crate) fn load_for_open(
        &self,
        manager: &BucketManager,
        name: &str,
    ) -> Result<BucketOpen, OpCode> {
        let _lock = self.structural_lock.lock();
        let bucket_id = self.id(name).ok_or(OpCode::NotFound)?;
        let runtime = manager.runtime(bucket_id).ok_or(OpCode::NotFound)?;
        if !runtime.state.is_active() {
            return Err(OpCode::NotFound);
        }
        let options = self.options(bucket_id).ok_or(OpCode::NotFound)?;
        let lease = runtime.acquire_lease();
        Ok(BucketOpen {
            meta: BucketMeta {
                id: bucket_id,
                options,
            },
            lease,
        })
    }

    /// reserve an id and the three physical btree buckets for a new bucket
    ///
    /// nothing is published to the catalog or the bucket manager here. The guard owns the
    /// structural lock and the local runtime/lease until [`CreateGuard::commit`] publishes the
    /// fully initialized bucket after the metadata transaction committed. Creation is serialized
    /// against open, option updates, unload and delete by that lock; a rollback deletes only the
    /// three empty btree buckets.
    pub(crate) fn begin_create<'a>(
        &'a self,
        btree: &'a BTree,
        next_bucket_id: &AtomicU64,
        name: &str,
        opt: &BucketOptions,
    ) -> Result<CreateGuard<'a>, OpCode> {
        let lock = self.structural_lock.lock();
        if self.names.contains_key(name) {
            return Err(OpCode::Exist);
        }
        if self.nr_buckets.load(Acquire) >= MAX_BUCKETS {
            return Err(OpCode::NoSpace);
        }

        let bucket_id = next_bucket_id.fetch_add(1, Relaxed);
        for btree_bucket in [
            super::super::page_table_name(bucket_id),
            super::super::data_interval_name(bucket_id),
            super::super::blob_interval_name(bucket_id),
        ] {
            Manifest::ensure_btree_bucket(btree, &btree_bucket);
        }
        let runtime = Arc::new(BucketRuntime::new());
        let lease = runtime.acquire_lease();

        Ok(CreateGuard {
            catalog: self,
            btree,
            _lock: lock,
            name: name.to_string(),
            bucket_id,
            options: PersistedBucketOptions::from(opt),
            runtime,
            lease: Some(lease),
            durable: false,
            committed: false,
        })
    }

    fn rollback_create(&self, btree: &BTree, bucket_id: u64) {
        for btree_bucket in [
            super::super::page_table_name(bucket_id),
            super::super::data_interval_name(bucket_id),
            super::super::blob_interval_name(bucket_id),
        ] {
            match btree.del_bucket(&btree_bucket) {
                Ok(()) | Err(BTreeError::BucketNotFound) => {}
                // nothing referenced this id in the catalog; a retry recreates it from empty
                // metadata under a new id
                Err(err) => diag::error!(
                    "can't roll back btree-store bucket {btree_bucket} of bucket {bucket_id}: {err:?}"
                ),
            }
        }
    }

    /// mark a bucket busy and record the reclaim mode before its runtime is released
    ///
    /// leases, active transactions, rewrites and pending abort-clean work all refuse the
    /// transition. The structural lock keeps a lease from being granted after the busy check, and
    /// the nudge barrier keeps the evictor from enqueuing a checkpoint task after the lifecycle
    /// changed, so the checkpoint thread always finds a runtime with a mode to read.
    fn begin_reclaim(
        &self,
        manager: &BucketManager,
        name: &str,
        mode: BucketRemoveMode,
    ) -> Result<u64, OpCode> {
        let _lock = self.structural_lock.lock();
        let bucket_id = self.id(name).ok_or(OpCode::NotFound)?;
        let runtime = manager.runtime(bucket_id).ok_or(OpCode::NotFound)?;
        let busy = !runtime.state.is_active()
            || runtime.lease_count() != 0
            || runtime.state.is_busy()
            || runtime.has_rewrite()
            || manager.ctx.has_pending_abort_clean_bucket(bucket_id);
        if busy {
            return Err(OpCode::Again);
        }

        match mode {
            BucketRemoveMode::Unload => {
                runtime.state.set_dropping();
                runtime.begin_reclaim(ReclaimMode::FinalCheckpoint { force_fsync: false });
            }
            BucketRemoveMode::Delete => {
                runtime.state.set_deleting();
                runtime.begin_reclaim(ReclaimMode::Discard);
            }
        }
        runtime.state.wait_nudge_quiescent();

        if matches!(mode, BucketRemoveMode::Delete) {
            // logical delete: the name leaves the catalog now, the count keeps the pending bucket
            self.names.remove(name);
            self.options.remove(&bucket_id);
            self.pending.insert(bucket_id);
        }
        Ok(bucket_id)
    }

    fn end_unload(&self, manager: &BucketManager, bucket_id: u64) {
        let runtime = manager.get_runtime(bucket_id);
        runtime.clear_reclaim();
        runtime.state.set_active();
    }

    /// the next pending-delete id, read from the durable pending table
    ///
    /// a read failure other than the modeled absence of a page-table/interval btree is returned:
    /// the caller keeps the pending record and the bucket count
    pub(crate) fn next_pending(&self, btree: &BTree) -> Result<Option<u64>, OpCode> {
        let mut bucket_id = None;
        let mut key_error = None;
        btree
            .view(BUCKET_PENDING_DEL, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                if iter.next_ref(&mut k, &mut v) {
                    match k.as_slice().try_into() {
                        Ok(raw) => bucket_id = Some(u64::from_le_bytes(raw)),
                        Err(_) => key_error = Some(OpCode::Corruption),
                    }
                }
                Ok(())
            })
            .map_err(OpCode::from)?;
        if let Some(err) = key_error {
            return Err(err);
        }
        Ok(bucket_id)
    }

    /// retire a pending-delete record after its btree sweep committed
    ///
    /// this is the only place the bucket count decreases, and it runs after the durable pending
    /// removal, never before the physical cleanup finished
    pub(crate) fn finish_pending(&self, manager: &BucketManager, bucket_id: u64) {
        // the count follows the same record the cleanup retired, so a retry can never decrement
        // twice for one pending id
        if self.pending.remove(&bucket_id).is_some() {
            self.nr_buckets.fetch_sub(1, AcqRel);
        }
        manager.remove_runtime(bucket_id);
    }
}

/// one resolved open: the caller holds the lease and may now load the context
pub(crate) struct BucketOpen {
    pub(crate) meta: BucketMeta,
    pub(crate) lease: BucketLease,
}

/// reservation returned by [`BucketCatalog::begin_create`]
///
/// it owns the catalog's structural lock for its whole life. Nothing is visible until `commit`
/// publishes the runtime, context, frontier, options, name and count after the metadata
/// transaction committed. A pre-commit drop deletes only the three empty btree buckets; a
/// post-commit drop never deletes them, because the committed metadata references them.
pub(crate) struct CreateGuard<'a> {
    catalog: &'a BucketCatalog,
    btree: &'a BTree,
    /// released after the rollback/publish: struct fields drop after `Drop::drop`
    _lock: parking_lot::MutexGuard<'a, ()>,
    name: String,
    bucket_id: u64,
    options: PersistedBucketOptions,
    runtime: Arc<BucketRuntime>,
    lease: Option<BucketLease>,
    durable: bool,
    committed: bool,
}

impl CreateGuard<'_> {
    pub(crate) fn bucket_id(&self) -> u64 {
        self.bucket_id
    }

    pub(crate) fn runtime(&self) -> &Arc<BucketRuntime> {
        &self.runtime
    }

    /// the metadata transaction returned: the three btree tables are now referenced durably
    pub(crate) fn mark_durable(&mut self) {
        self.durable = true;
    }

    /// publish the fully initialized bucket under the structural lock
    pub(crate) fn commit(
        mut self,
        manager: &BucketManager,
        bucket_frontier: &DashMap<u64, GroupPositions>,
        ctx: Arc<BucketContext>,
        frontier: GroupPositions,
    ) -> BucketLease {
        // every panic-capable check runs before the first shared write
        debug_assert!(
            manager.runtime(self.bucket_id).is_none(),
            "create guard must publish a fresh runtime"
        );
        debug_assert!(
            self.durable,
            "creation must commit metadata before publishing"
        );
        let lease = must_exist!(self.lease.take(), "create guard must hold its lease");

        manager.insert_runtime(self.bucket_id, self.runtime.clone());
        ctx.publish();
        manager.insert_context(self.bucket_id, ctx);
        bucket_frontier.insert(self.bucket_id, frontier);
        self.catalog.options.insert(self.bucket_id, self.options);
        self.catalog.names.insert(self.name.clone(), self.bucket_id);
        self.catalog.nr_buckets.fetch_add(1, AcqRel);

        self.committed = true;
        lease
    }
}

impl Drop for CreateGuard<'_> {
    fn drop(&mut self) {
        if !self.durable {
            self.catalog.rollback_create(self.btree, self.bucket_id);
        } else if !self.committed {
            // commit returned but the in-memory publish did not finish (test unwind). The btree
            // tables are referenced by committed metadata and must stay; removing them here would
            // make the next open fail in recover_intervals.
            diag::error!(
                "bucket {} ({}) committed but was not published; leaving btree tables intact",
                self.name,
                self.bucket_id
            );
        }
    }
}

impl Manifest {
    pub(super) fn ensure_btree_bucket(btree: &BTree, name: &str) {
        match btree.new_bucket(name, false) {
            Ok(()) | Err(BTreeError::BucketExists) => {}
            Err(err) => panic!("can't ensure btree-store bucket {name}: {err:?}"),
        }
    }

    fn bucket_is_empty(&self, bucket: &str) -> Result<bool, OpCode> {
        self.btree
            .view(bucket, |txn| {
                let mut iter = txn.iter();
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
        must_exist!(self.buckets.context(bucket_id), "must exist")
    }

    /// reads only the persisted bucket options from metadata; unlike the open path this never
    /// touches the catalog, the bucket runtime, the page table or the context
    pub(crate) fn load_bucket_options(
        &self,
        name: &str,
    ) -> Result<Option<PersistedBucketOptions>, OpCode> {
        // creation publishes the btree record before the in-memory catalog; hold the same lock so
        // this read cannot observe a bucket that is still being published
        let _lock = self.catalog.lock();
        let mut options = None;
        let mut decode_error = None;
        self.btree
            .view(BUCKET_METAS, |txn| {
                match txn.get(name.as_bytes()) {
                    Ok(v) => match BucketMeta::try_decode(&v) {
                        Ok(meta) => options = Some(meta.options),
                        Err(err) => decode_error = Some(err),
                    },
                    Err(BTreeError::KeyNotFound) => {}
                    Err(err) => return Err(err),
                }
                Ok(())
            })
            .map_err(OpCode::from)?;
        if let Some(err) = decode_error {
            return Err(err);
        }
        Ok(options)
    }

    pub(crate) fn create_bucket(
        &self,
        name: &str,
        opt: BucketOptions,
    ) -> Result<(BucketLease, Arc<BucketContext>), OpCode> {
        let mut guard =
            self.catalog
                .begin_create(&self.btree, &self.sequences.next_bucket_id, name, &opt)?;
        let bucket_id = guard.bucket_id();

        // the guard holds the structural lock; options are not in the catalog yet, so build the
        // context from the requested values and the guard's local runtime
        let mut runtime_opt = PersistedBucketOptions::from(&opt).to_runtime();
        runtime_opt.merge_operator = opt.merge_operator.clone();
        let bucket_ctx = self.build_bucket_context(bucket_id, guard.runtime(), runtime_opt)?;

        // inactive floors are not durable positions
        let mut frontier = self.current_group_checkpoints();
        for slot in frontier.iter_mut() {
            if *slot == Position::MAX {
                *slot = Position::MIN;
            }
        }
        let frontier_rec = BucketDurableFrontier::new(bucket_id, frontier);

        let meta = BucketMeta {
            id: bucket_id,
            options: PersistedBucketOptions::from(&opt),
        };
        let mut txn = self.begin();
        txn.record(MetaKind::Sequences, self.sequences.as_ref());
        txn.record(MetaKind::BucketFrontier, &frontier_rec);
        txn.put_record(BUCKET_METAS, name.as_bytes().to_vec(), &meta);
        #[cfg(feature = "extra_check")]
        crate::testing::fire_bucket_create_sync_point(
            crate::testing::BucketCreateSyncPoint::BeforeManifestCommit,
            bucket_id,
            name,
        );
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_create_before_manifest_commit");
        txn.commit();
        guard.mark_durable();
        #[cfg(feature = "extra_check")]
        crate::testing::fire_bucket_create_sync_point(
            crate::testing::BucketCreateSyncPoint::AfterCommitBeforePublish,
            bucket_id,
            name,
        );
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_create_after_manifest_commit");

        let ctx_for_return = bucket_ctx.clone();
        let lease = guard.commit(&self.buckets, &self.bucket_frontier, bucket_ctx, frontier);
        Ok((lease, ctx_for_return))
    }

    fn bucket_option_conflicts(
        old: &PersistedBucketOptions,
        new: &PersistedBucketOptions,
    ) -> Vec<&'static str> {
        let mut conflicts = Vec::new();
        // inline_size may only move downward. Lowering the threshold is handled
        // by rebuild materialization; raising it would require converting
        // existing remote rows back to inline rows and retiring their blobs.
        if new.inline_size > old.inline_size {
            conflicts.push("inline_size");
        }
        // split_elems shapes persisted tree topology and stays immutable.
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
        let _lock = self.catalog.lock();
        let bucket_id = self.catalog.id(name).ok_or(OpCode::NotFound)?;
        let runtime = self.buckets.get_runtime(bucket_id);

        if self.buckets.contains_context(bucket_id) || runtime.has_rewrite() {
            diag::info!(
                "bucket {}({}) is busy, reject bucket option update",
                name,
                bucket_id
            );
            return Err(OpCode::Again);
        }

        let old_persisted =
            must_exist!(self.catalog.options(bucket_id), "bucket options must exist");
        let new_persisted = PersistedBucketOptions::from(&opt);
        if old_persisted == new_persisted {
            return Ok(());
        }

        let conflicts = Self::bucket_option_conflicts(&old_persisted, &new_persisted);
        if !conflicts.is_empty() {
            diag::error!(
                "bucket {}({}) option update conflicts on [{}], old: {:?}, new: {:?}",
                name,
                bucket_id,
                conflicts.join(", "),
                old_persisted,
                opt
            );
            return Err(OpCode::Invalid);
        }

        let new_meta = BucketMeta {
            id: bucket_id,
            options: new_persisted,
        };
        let mut txn = self.begin();
        txn.put_record(BUCKET_METAS, name.as_bytes().to_vec(), &new_meta);
        txn.commit();

        self.catalog.options.insert(bucket_id, new_persisted);
        Ok(())
    }

    pub(crate) fn load_bucket_context(&self, bucket_id: u64) -> Result<Arc<BucketContext>, OpCode> {
        self.load_bucket_context_with_options(bucket_id, None)
    }

    pub(crate) fn load_bucket_context_with_options(
        &self,
        bucket_id: u64,
        runtime_opt: Option<&BucketOptions>,
    ) -> Result<Arc<BucketContext>, OpCode> {
        // the hit path still has to prove the bucket is catalogued and active: a cached context
        // must never be published to a new reader after an unload/delete began
        if let Some(ctx) = self.buckets.context(bucket_id) {
            if !self.catalog.contains(bucket_id) || !ctx.state.is_active() {
                return Err(OpCode::NotFound);
            }
            ctx.validate_merge_operator(runtime_opt.and_then(|opt| opt.merge_operator.as_ref()))?;
            return Ok(ctx);
        }

        let _lock = self.catalog.lock();
        self.load_bucket_context_locked(bucket_id, runtime_opt)
    }

    pub(crate) fn try_acquire_rewrite(&self, bucket_id: u64) -> Option<BucketRewritePermit> {
        let _lock = self.catalog.lock();
        let meta = self.catalog.meta(bucket_id)?;
        let runtime = self.buckets.get_runtime(bucket_id);
        if !runtime.state.is_active() {
            return None;
        }
        let enable_compression = meta.options.enable_compression;
        Some(BucketRewritePermit {
            enable_compression,
            compressors: runtime.compressors.clone(),
            _guard: runtime.begin_rewrite(),
        })
    }

    /// build a context without publishing it or consulting the catalog
    ///
    /// used by creation (options are not in the catalog yet) and by the locked load path. It must
    /// keep the full `table.recover` + `recover_intervals` sequence and must reuse the passed
    /// runtime's state/compressors; lifecycle and "already loaded" checks belong to the caller.
    fn build_bucket_context(
        &self,
        bucket_id: u64,
        runtime: &Arc<BucketRuntime>,
        opt: BucketOptions,
    ) -> Result<Arc<BucketContext>, OpCode> {
        let state = runtime.state.clone();

        // PageMap lazy load
        let table = {
            let table = MutRef::new(PageMap::default());
            table.recover(bucket_id, Some(&self.btree));
            table
        };

        let flush = must_exist!(self.buckets.flush.as_ref(), "flusher started").clone();
        let ctx = Arc::new(BucketContext::new(
            self.buckets.ctx,
            &self.opt,
            Arc::new(opt),
            state,
            bucket_id,
            table,
            flush,
            self.buckets.lru,
            must_exist!(
                self.buckets.reader,
                "manifest reader must be set before loading a bucket"
            ),
            self.buckets.tx.clone(),
            runtime.compressors.clone(),
        ));

        self.recover_intervals(bucket_id, &ctx)?;
        Ok(ctx)
    }

    fn load_bucket_context_locked(
        &self,
        bucket_id: u64,
        runtime_opt: Option<&BucketOptions>,
    ) -> Result<Arc<BucketContext>, OpCode> {
        // double check: the unlocked hit path may have raced with another loader
        if let Some(ctx) = self.buckets.context(bucket_id) {
            ctx.validate_merge_operator(runtime_opt.and_then(|opt| opt.merge_operator.as_ref()))?;
            return Ok(ctx);
        }

        // lifecycle first: a logically deleted bucket already left the catalog, and its runtime
        // is what tells that apart from a caller asking for an id this store never had
        let runtime = must_exist!(
            self.buckets.runtime(bucket_id),
            "bucket {bucket_id} has no runtime"
        );
        if !runtime.state.is_active() || !self.catalog.contains(bucket_id) {
            // deleting/dropping contexts are never published to a new reader
            return Err(OpCode::NotFound);
        }

        let persisted = must_exist!(
            self.catalog.options(bucket_id),
            "bucket meta must exist for a catalogued bucket"
        );
        let mut opt = persisted.to_runtime();
        if let Some(runtime_opt) = runtime_opt {
            opt.merge_operator.clone_from(&runtime_opt.merge_operator);
        }
        let ctx = self.build_bucket_context(bucket_id, &runtime, opt)?;
        ctx.publish();
        self.buckets.insert_context(bucket_id, ctx.clone());
        Ok(ctx)
    }

    pub(crate) fn unload_bucket(&self, name: &str) -> Result<(), OpCode> {
        let bucket_id =
            self.catalog
                .begin_reclaim(&self.buckets, name, BucketRemoveMode::Unload)?;
        self.buckets.reclaim_bucket(
            bucket_id,
            ReclaimMode::FinalCheckpoint { force_fsync: false },
        );
        self.catalog.end_unload(&self.buckets, bucket_id);
        Ok(())
    }

    pub(crate) fn delete_bucket(&self, name: &str) -> Result<(), OpCode> {
        let bucket_id =
            self.catalog
                .begin_reclaim(&self.buckets, name, BucketRemoveMode::Delete)?;

        // a logical delete drops the sealed batch instead of publishing it
        self.buckets.reclaim_bucket(bucket_id, ReclaimMode::Discard);

        // collect and record obsolete files
        let mut files = [Vec::new(), Vec::new()];
        for kind in FileKind::ALL {
            files[kind.slot()] = self.file_state(kind).stat.bucket_file_ids(bucket_id);
        }

        // commit transaction (logical delete)
        let mut txn = self.begin();

        // remove from BUCKET_METAS
        txn.del(BUCKET_METAS, name.as_bytes().to_vec());

        // record for physical cleanup in background
        txn.put(BUCKET_PENDING_DEL, bucket_id.to_le_bytes().to_vec(), vec![]);
        if self.bucket_frontier.contains_key(&bucket_id) {
            txn.del(BUCKET_FRONTIER, bucket_id.to_le_bytes().to_vec());
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
            self.file_state(kind).stat.remove_stat_interval(ids);
            self.save_obsolete_files(kind, ids);
        }
        self.bucket_frontier.remove(&bucket_id);
        must_ok!(self.delete_files());

        Ok(())
    }

    pub(crate) fn recover_intervals(
        &self,
        bucket_id: u64,
        ctx: &BucketContext,
    ) -> Result<(), OpCode> {
        let mut max_addr = INIT_ADDR;
        let mut recovered = Vec::with_capacity(FileKind::ALL.len());

        for kind in FileKind::ALL {
            let interval_table = interval_bucket_name(kind, bucket_id);
            let mut map = IntervalMap::new();
            self.btree
                .view(&interval_table, |txn| {
                    let mut iter = txn.iter();
                    let mut k = Vec::new();
                    let mut v = Vec::new();
                    while iter.next_ref(&mut k, &mut v) {
                        let ivl = IntervalPair::decode(&v);
                        max_addr = max_addr.max(ivl.hi_addr);
                        map.upsert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
                    }
                    Ok(())
                })
                .map_err(OpCode::from)?;
            recovered.push((kind, map));
        }

        for (kind, map) in recovered {
            *stat_intervals(kind, ctx).write() = map;
        }
        ctx.state.next_addr.fetch_max(max_addr + 1, Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::data_interval_name;
    use crate::{BucketOptions, Mace, Options, RandomPath};

    fn open_mace() -> (RandomPath, Mace) {
        let root = RandomPath::tmp();
        let opt = Options::new(&*root);
        let mace = Mace::new(opt.validate().expect("options must validate")).expect("open");
        (root, mace)
    }

    #[test]
    fn bucket_open_acquires_lease_before_context_load() {
        let (_root, mace) = open_mace();
        let manifest = mace.inner.store.manifest;
        let bucket = mace
            .new_bucket("leased", BucketOptions::default())
            .expect("create");
        let bucket_id = bucket.id();
        drop(bucket);
        mace.drop_bucket("leased").expect("unload");
        assert!(!manifest.buckets.contains_context(bucket_id));

        let open = manifest
            .catalog
            .load_for_open(&manifest.buckets, "leased")
            .expect("catalog must resolve an active bucket");
        assert_eq!(open.meta.id, bucket_id);
        assert_eq!(manifest.buckets.get_runtime(bucket_id).lease_count(), 1);
        assert!(
            !manifest.buckets.contains_context(bucket_id),
            "resolving an open takes the lease but must not load the context"
        );
        assert_eq!(
            manifest.unload_bucket("leased"),
            Err(OpCode::Again),
            "the lease taken before the context load already blocks an unload"
        );

        drop(open);
        assert_eq!(manifest.buckets.get_runtime(bucket_id).lease_count(), 0);
    }

    #[test]
    fn bucket_context_load_rejects_deleting_bucket() {
        let (_root, mace) = open_mace();
        let manifest = mace.inner.store.manifest;
        let bucket = mace
            .new_bucket("victim", BucketOptions::default())
            .expect("create");
        let bucket_id = bucket.id();
        drop(bucket);
        mace.drop_bucket("victim").expect("unload");

        let id = manifest
            .catalog
            .begin_reclaim(&manifest.buckets, "victim", BucketRemoveMode::Delete)
            .expect("logical delete must begin");
        assert_eq!(id, bucket_id);
        assert!(
            matches!(
                manifest.load_bucket_context(bucket_id),
                Err(OpCode::NotFound)
            ),
            "a deleting bucket context must not be published to a new load"
        );

        // finish the logical phase locally so teardown does not carry a deleting runtime
        manifest
            .buckets
            .reclaim_bucket(bucket_id, ReclaimMode::Discard);
        manifest
            .catalog
            .finish_pending(&manifest.buckets, bucket_id);
    }

    #[test]
    fn bucket_create_interval_error_rolls_back() {
        let (_root, mace) = open_mace();
        let manifest = mace.inner.store.manifest;
        let opt = BucketOptions::default();
        let guard = manifest
            .catalog
            .begin_create(
                &manifest.btree,
                &manifest.sequences.next_bucket_id,
                "broken",
                &opt,
            )
            .expect("reservation must succeed");
        let reserved_id = guard.bucket_id();

        // the reservation and context build must not publish anything
        assert_eq!(manifest.catalog.nr_buckets.load(Acquire), 0);
        assert!(manifest.catalog.id("broken").is_none());
        assert!(manifest.catalog.options(reserved_id).is_none());
        assert!(manifest.buckets.runtime(reserved_id).is_none());
        assert!(!manifest.buckets.contains_context(reserved_id));
        assert!(!manifest.bucket_frontier.contains_key(&reserved_id));

        // break an interval table the reservation just ensured, so context initialization fails
        manifest
            .btree
            .del_bucket(data_interval_name(reserved_id))
            .expect("interval table must be deletable");
        let mut runtime_opt = PersistedBucketOptions::from(&opt).to_runtime();
        runtime_opt.merge_operator = opt.merge_operator.clone();
        assert!(
            manifest
                .build_bucket_context(reserved_id, guard.runtime(), runtime_opt)
                .is_err(),
            "a missing interval table must fail context initialization"
        );

        drop(guard);
        assert_eq!(
            manifest.catalog.nr_buckets.load(Acquire),
            0,
            "the count must never have been raised"
        );
        assert!(!manifest.catalog.contains(reserved_id));
        assert!(manifest.catalog.id("broken").is_none());
        for table in [
            crate::meta::page_table_name(reserved_id),
            crate::meta::blob_interval_name(reserved_id),
        ] {
            assert!(
                manifest.btree.view(&table, |_| Ok(())).is_err(),
                "rollback must delete {table}"
            );
        }
        assert_eq!(
            mace.update_bucket_opt("broken", BucketOptions::default()),
            Err(OpCode::NotFound)
        );
        assert_eq!(mace.drop_bucket("broken"), Err(OpCode::NotFound));
        assert_eq!(mace.del_bucket("broken"), Err(OpCode::NotFound));
        assert!(
            mace.get_bucket_options("broken")
                .expect("options read")
                .is_none()
        );
        assert!(!mace.active_buckets().iter().any(|name| name == "broken"));
        assert_eq!(mace.nr_buckets(), 0);

        let retried = mace
            .new_bucket("broken", BucketOptions::default())
            .expect("the rolled-back name must be reusable");
        assert_ne!(retried.id(), reserved_id);
    }

    #[cfg(feature = "extra_check")]
    #[test]
    fn bucket_create_publishes_only_after_commit() {
        use crate::testing::{self, BucketCreateSyncPoint};
        use std::sync::Mutex;
        use std::sync::mpsc::channel;
        use std::time::Duration;

        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                testing::clear_bucket_create_hook();
            }
        }

        let (_root, mace) = open_mace();
        let _reset = Reset;
        let manifest = mace.inner.store.manifest;
        let (paused_tx, paused_rx) = channel();
        let (release_tx, release_rx) = channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        testing::set_bucket_create_hook(Some(Arc::new(move |point, id, name: &str| {
            if name == "paused" && point == BucketCreateSyncPoint::BeforeManifestCommit {
                paused_tx.send(id).expect("signal paused create");
                release_rx
                    .lock()
                    .expect("lock release receiver")
                    .recv()
                    .expect("wait for release");
            }
        })));

        let creator = mace.clone();
        let create =
            std::thread::spawn(move || creator.new_bucket("paused", BucketOptions::default()));
        let bucket_id = paused_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("create must pause before commit");

        // lock-free internal probes: the guard holds the structural lock, so public reads would
        // block rather than prove the pre-commit state
        assert!(manifest.catalog.id("paused").is_none());
        assert_eq!(manifest.catalog.nr_buckets.load(Acquire), 0);
        assert!(manifest.catalog.options(bucket_id).is_none());
        assert!(manifest.buckets.runtime(bucket_id).is_none());
        assert!(!manifest.buckets.contains_context(bucket_id));
        assert!(!manifest.bucket_frontier.contains_key(&bucket_id));

        release_tx.send(()).expect("release create");
        let bucket = create
            .join()
            .expect("creator thread must finish")
            .expect("create");
        assert_eq!(bucket.id(), bucket_id);
        assert!(manifest.catalog.id("paused").is_some());
        assert_eq!(manifest.catalog.nr_buckets.load(Acquire), 1);
        drop(bucket);
    }

    #[test]
    fn bucket_create_at_limit_consumes_nothing() {
        let (_root, mace) = open_mace();
        let manifest = mace.inner.store.manifest;
        manifest.catalog.nr_buckets.store(MAX_BUCKETS, Release);
        let before = manifest.sequences.next_bucket_id.load(Relaxed);

        let err = manifest
            .catalog
            .begin_create(
                &manifest.btree,
                &manifest.sequences.next_bucket_id,
                "limit",
                &BucketOptions::default(),
            )
            .err()
            .expect("a full catalog must reject creation");
        assert_eq!(err, OpCode::NoSpace);
        assert_eq!(manifest.sequences.next_bucket_id.load(Relaxed), before);
        assert!(manifest.catalog.id("limit").is_none());
        assert!(manifest.catalog.options(before).is_none());
        assert!(manifest.buckets.runtime(before).is_none());
        for table in [
            crate::meta::page_table_name(before),
            crate::meta::data_interval_name(before),
            crate::meta::blob_interval_name(before),
        ] {
            assert!(
                manifest.btree.view(&table, |_| Ok(())).is_err(),
                "a rejected create must not reserve {table}"
            );
        }

        manifest.catalog.nr_buckets.store(0, Release);
    }
}
