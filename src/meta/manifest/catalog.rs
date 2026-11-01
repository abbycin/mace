use std::sync::atomic::Ordering::Release;

use super::*;

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
        self.nr_buckets.load(Acquire)
    }

    /// the active bucket names: an unloaded bucket stays listed, a logically deleted one does not
    pub(crate) fn active_names(&self) -> Vec<String> {
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

    /// reserve a name, id, count and the three physical btree buckets for a new bucket
    ///
    /// the reservation stays visible through the catalog before the metadata transaction commits,
    /// but the guard returned here owns the structural lock until it is consumed or dropped:
    /// creation is serialized against open, option updates, unload and delete, and the rollback
    /// runs under the same lock. Everything is undone by [`CreateGuard`] unless `commit` consumes
    /// the guard after the durable publish succeeded.
    ///
    /// the caller must initialize the context through the lock-free loader: taking the lock again
    /// in `load_bucket_context` would deadlock, and releasing it early would let an option update
    /// commit between the reservation and the publish.
    pub(crate) fn begin_create<'a>(
        &'a self,
        btree: &'a BTree,
        next_bucket_id: &AtomicU64,
        manager: &BucketManager,
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
        self.names.insert(name.to_string(), bucket_id);
        self.options
            .insert(bucket_id, PersistedBucketOptions::from(opt));
        self.nr_buckets.fetch_add(1, AcqRel);
        let lease = manager.ensure_runtime(bucket_id).acquire_lease();

        Ok(CreateGuard {
            catalog: self,
            btree,
            _lock: lock,
            name: name.to_string(),
            bucket_id,
            lease: Some(lease),
            committed: false,
        })
    }

    fn rollback_create(&self, btree: &BTree, name: &str, bucket_id: u64) {
        self.names.remove(name);
        self.options.remove(&bucket_id);
        self.nr_buckets.fetch_sub(1, AcqRel);
        for btree_bucket in [
            super::super::page_table_name(bucket_id),
            super::super::data_interval_name(bucket_id),
            super::super::blob_interval_name(bucket_id),
        ] {
            match btree.del_bucket(&btree_bucket) {
                Ok(()) | Err(BTreeError::BucketNotFound) => {}
                // the reservation is already gone from the catalog; a retry of this create
                // recreates the bucket from empty metadata
                Err(err) => log::error!(
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
/// it owns the catalog's structural lock for its whole life, so context initialization and the
/// metadata publish cannot interleave with an option update, an open, an unload or a delete.
/// Dropping it without `commit` rolls the name, id, count and physical btree buckets back under
/// that lock, so a failed context/interval initialization leaves no visible bucket and no
/// inflated count.
pub(crate) struct CreateGuard<'a> {
    catalog: &'a BucketCatalog,
    btree: &'a BTree,
    /// released after the rollback: struct fields drop after `Drop::drop`
    _lock: parking_lot::MutexGuard<'a, ()>,
    name: String,
    bucket_id: u64,
    lease: Option<BucketLease>,
    committed: bool,
}

impl CreateGuard<'_> {
    pub(crate) fn bucket_id(&self) -> u64 {
        self.bucket_id
    }

    /// the durable publish succeeded: keep the reservation and hand out the creation lease
    pub(crate) fn commit(mut self) -> BucketLease {
        self.committed = true;
        must_exist!(self.lease.take(), "create guard must hold its lease")
    }
}

impl Drop for CreateGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.catalog
                .rollback_create(self.btree, &self.name, self.bucket_id);
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
        let guard = self.catalog.begin_create(
            &self.btree,
            &self.sequences.next_bucket_id,
            &self.buckets,
            name,
            &opt,
        )?;
        let bucket_id = guard.bucket_id();
        // the guard holds the structural lock, so the loader must be the lock-free one: taking it
        // again would deadlock, and dropping it here would let an option update commit in between
        // this initialization and the metadata publish below
        // context/interval initialization failures roll the reservation back through the guard
        let bucket_ctx = self.load_bucket_context_locked(bucket_id, Some(&opt))?;

        // inactive floors are not durable positions
        let mut frontier = self.current_group_checkpoints();
        for slot in frontier.iter_mut() {
            if *slot == Position::MAX {
                *slot = Position::MIN;
            }
        }
        self.bucket_frontier.insert(bucket_id, frontier);
        let frontier_rec = BucketDurableFrontier::new(bucket_id, frontier);

        let meta = BucketMeta {
            id: bucket_id,
            options: PersistedBucketOptions::from(&opt),
        };
        let mut txn = self.begin();
        txn.record(MetaKind::Sequences, self.sequences.as_ref());
        txn.record(MetaKind::BucketFrontier, &frontier_rec);
        txn.put_record(BUCKET_METAS, name.as_bytes().to_vec(), &meta);
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_create_before_manifest_commit");
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_create_after_manifest_commit");

        Ok((guard.commit(), bucket_ctx))
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
            log::info!(
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
            log::error!(
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

    fn load_bucket_context_locked(
        &self,
        bucket_id: u64,
        runtime_opt: Option<&BucketOptions>,
    ) -> Result<Arc<BucketContext>, OpCode> {
        // double check
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

        let state = runtime.state.clone();

        // PageMap lazy load
        let table = {
            let table = MutRef::new(PageMap::default());
            table.recover(bucket_id, Some(&self.btree));
            table
        };

        let flush = must_exist!(self.buckets.flush.as_ref(), "flusher started").clone();
        let persisted = must_exist!(
            self.catalog.options(bucket_id),
            "bucket meta must exist for a catalogued bucket"
        );
        let mut opt = persisted.to_runtime();
        if let Some(runtime_opt) = runtime_opt {
            opt.merge_operator.clone_from(&runtime_opt.merge_operator);
        }
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
                &manifest.buckets,
                "broken",
                &opt,
            )
            .expect("reservation must succeed");
        let reserved_id = guard.bucket_id();

        // break an interval table the reservation just ensured, so context initialization fails
        manifest
            .btree
            .del_bucket(data_interval_name(reserved_id))
            .expect("interval table must be deletable");
        assert!(
            manifest
                .load_bucket_context_locked(reserved_id, Some(&opt))
                .is_err(),
            "a missing interval table must fail context initialization"
        );

        drop(guard);
        assert_eq!(manifest.catalog.nr_buckets(), 0, "the count must roll back");
        assert!(!manifest.catalog.contains(reserved_id));
        assert!(manifest.catalog.id("broken").is_none());

        let retried = mace
            .new_bucket("broken", BucketOptions::default())
            .expect("the rolled-back name must be reusable");
        assert_ne!(retried.id(), reserved_id);
    }
}
