use crate::cc::context::Context;
use crate::index::tree::Tree;
pub use crate::index::txn::{TxnKV, TxnView};
use crate::map::buffer::DataReader;
use crate::map::evictor::Evictor;
use crate::map::flush::{FlushDirective, FlushObserver, FlushResult};
use crate::meta::builder::ManifestBuilder;
use crate::meta::{BucketMeta, Manifest, MetaKind};
use crate::store::gc::{GCHandle, start_gc};
use crate::store::recovery::Recovery;
use crate::store::{META_VACUUM_TARGET_BYTES, MetaVacuumStats, VacuumStats};
use crate::types::refbox::BoxRef;
use crate::utils::Handle;
use crate::utils::MutRef;
pub use crate::utils::OpCode;
use crate::utils::ROOT_PID;
pub use crate::utils::options::Options;
use crate::utils::options::ParsedOptions;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::mpsc::channel;

struct StoreFlushObserver {
    manifest: Handle<Manifest>,
    ctx: Handle<Context>,
    pending_groups: Mutex<BTreeMap<u64, PendingFlushGroup>>,
}

struct PendingFlushGroup {
    expected: u32,
    results: Vec<FlushResult>,
    writer_dep_ids: Vec<Vec<u64>>,
}

struct StoreDataReader {
    meta: Handle<Manifest>,
}

impl StoreDataReader {
    fn new(meta: Handle<Manifest>) -> Self {
        Self { meta }
    }

    fn read_data(
        &self,
        bucket_id: u64,
        addr: u64,
        cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode> {
        self.meta.load_data(bucket_id, addr, cache)
    }

    fn read_blob(
        &self,
        bucket_id: u64,
        addr: u64,
        cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode> {
        self.meta.load_blob(bucket_id, addr, cache)
    }

    fn read_blob_uncached(&self, bucket_id: u64, addr: u64) -> Result<BoxRef, OpCode> {
        self.meta.load_blob_uncached(bucket_id, addr)
    }
}

impl DataReader for StoreDataReader {
    fn load_data(
        &self,
        bucket_id: u64,
        addr: u64,
        cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode> {
        self.read_data(bucket_id, addr, cache)
    }

    fn load_blob(
        &self,
        bucket_id: u64,
        addr: u64,
        cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode> {
        self.read_blob(bucket_id, addr, cache)
    }

    fn load_blob_uncached(&self, bucket_id: u64, addr: u64) -> Result<BoxRef, OpCode> {
        self.read_blob_uncached(bucket_id, addr)
    }
}

impl StoreFlushObserver {
    fn new(manifest: Handle<Manifest>, ctx: Handle<Context>) -> Self {
        Self {
            manifest,
            ctx,
            pending_groups: Mutex::new(BTreeMap::new()),
        }
    }

    fn push_flush_result(&self, mut result: FlushResult) -> Option<PendingFlushGroup> {
        let group_id = result.dep_group_id;
        let expected = result.dep_group_items;
        if expected == 0 {
            log::error!("invalid dep group size 0, group_id {}", group_id);
            panic!("invalid dep group size 0");
        }

        let groups = self.ctx.groups();
        debug_assert_eq!(result.flsn.len(), groups.len());

        let ctx = self
            .manifest
            .buckets
            .buckets
            .get(&result.bucket_id)
            .expect("bucket context must exist when flushing");
        ctx.data_intervals.write().insert(
            result.data_ivl.lo_addr,
            result.data_ivl.hi_addr,
            result.data_ivl.file_id,
        );
        if let Some(blob_ivl) = result.blob_ivl {
            ctx.blob_intervals
                .write()
                .insert(blob_ivl.lo_addr, blob_ivl.hi_addr, blob_ivl.file_id);
        }
        // release arena after pending visibility is in place
        // done callback stays deferred until manifest publish
        result.done.release();

        let mut lk = self.pending_groups.lock();
        let entry = lk.entry(group_id).or_insert_with(|| PendingFlushGroup {
            expected,
            results: Vec::with_capacity(expected as usize),
            writer_dep_ids: vec![Vec::new(); groups.len()],
        });
        if entry.expected != expected {
            log::error!(
                "dep group size mismatch, group {}, expected {}, got {}",
                group_id,
                entry.expected,
                expected
            );
            panic!("dep group size mismatch");
        }

        for (i, g) in groups.iter().enumerate() {
            let dep_id = g.dep_group.open_group_durable(
                result.flsn[i],
                result.flsn[i],
                vec![result.data_id],
            );
            entry.writer_dep_ids[i].push(dep_id);
        }

        entry.results.push(result);
        if entry.results.len() < expected as usize {
            return None;
        }

        lk.remove(&group_id)
    }

    fn publish_group(&self, pending: PendingFlushGroup) -> Result<(), OpCode> {
        let PendingFlushGroup {
            results,
            writer_dep_ids,
            ..
        } = pending;
        if results.is_empty() {
            return Ok(());
        }

        let groups = self.ctx.groups();
        let mut durable_targets = vec![crate::utils::data::Position::default(); groups.len()];

        for result in &results {
            debug_assert_eq!(result.flsn.len(), groups.len());
            for (i, pos) in result.flsn.iter().enumerate() {
                if *pos > durable_targets[i] {
                    durable_targets[i] = *pos;
                }
            }
        }

        // make wal durable before publishing flush metadata
        for (i, g) in groups.iter().enumerate() {
            let target = durable_targets[i];
            let mut lk = g.logging.lock();
            let ok = lk.should_durable(target);
            if ok {
                let mut f = lk.writer.clone();
                drop(lk);
                // it's safe because we make sure wal entry has been written before
                f.sync();
            }
        }

        let mut txn = self.manifest.begin();
        for result in &results {
            let data_stats =
                self.manifest
                    .apply_data_junks(result.bucket_id, result.tick, &result.data_junks);
            let blob_stats = self
                .manifest
                .apply_blob_junks(result.bucket_id, &result.blob_junks);

            txn.record(MetaKind::DataStat, &result.data_stat);
            txn.record(MetaKind::Map, &result.map_table);
            txn.record(MetaKind::DataInterval, &result.data_ivl);

            data_stats.iter().for_each(|x| {
                assert_ne!(result.data_id, x.file_id);
                txn.record(MetaKind::DataStat, x)
            });

            blob_stats.iter().for_each(|x| {
                txn.record(MetaKind::BlobStat, x);
            });

            if let (Some(blob_ivl), Some(blob_stat)) = (&result.blob_ivl, &result.blob_stat) {
                txn.record(MetaKind::BlobStat, blob_stat);
                txn.record(MetaKind::BlobInterval, blob_ivl);
            }
        }

        txn.record(MetaKind::Numerics, self.manifest.numerics.deref());
        for result in &results {
            self.manifest
                .clear_orphan_data_file(&mut txn, result.data_id);
            if let Some(blob_ivl) = result.blob_ivl {
                self.manifest
                    .clear_orphan_blob_file(&mut txn, blob_ivl.file_id);
            }
        }
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check("mace_flush_before_manifest_commit")?;
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check("mace_flush_after_manifest_commit")?;

        let mut ckpt_targets = Vec::with_capacity(results.len());
        let mut done_list = Vec::with_capacity(results.len());
        for result in results {
            self.manifest
                .add_data_stat(result.mem_data_stat, result.data_ivl);
            if let (Some(mem_blob_stat), Some(blob_ivl)) = (result.mem_blob_stat, result.blob_ivl) {
                self.manifest.add_blob_stat(mem_blob_stat, blob_ivl);
            }
            ckpt_targets.push(result.flsn);
            done_list.push(result.done);
        }

        for (i, dep_ids) in writer_dep_ids.into_iter().enumerate() {
            let manager = &groups[i].dep_group;
            for dep_id in dep_ids {
                manager
                    .mark_group_published(dep_id)
                    .expect("dep group publish transition failed");
            }
        }

        for done in done_list {
            done.mark_done();
        }

        // checkpoint update is a post-flush step and must run after mark_done
        // wal durability barrier is handled before manifest commit in the first loop
        for flsn in ckpt_targets {
            for (i, g) in groups.iter().enumerate() {
                let mut pos = flsn[i];
                if let Some(min) = g.active_txns.min_lsn()
                    && min < pos
                {
                    pos = min;
                }
                if let Some(min) = g.dep_group.pending_min_lsn()
                    && min < pos
                {
                    pos = min;
                }
                let mut lk = g.logging.lock();
                if lk.update_checkpoint(pos) {
                    let mut f = lk.writer.clone();
                    drop(lk);
                    // checkpoint must be synced
                    f.sync();
                }
            }
        }

        Ok(())
    }
}

impl FlushObserver for StoreFlushObserver {
    fn flush_directive(&self, bucket_id: u64) -> FlushDirective {
        match self.manifest.bucket_states.get(&bucket_id) {
            Some(state) => {
                if state.is_deleting() {
                    return FlushDirective::Skip;
                }
                FlushDirective::Normal
            }
            None => FlushDirective::Skip,
        }
    }

    fn stage_orphan_data_file(&self, file_id: u64) {
        self.manifest.stage_orphan_data_file(file_id);
    }

    fn stage_orphan_blob_file(&self, file_id: u64) {
        self.manifest.stage_orphan_blob_file(file_id);
    }

    fn on_flush(&self, result: FlushResult) -> Result<(), OpCode> {
        if let Some(group_results) = self.push_flush_result(result) {
            self.publish_group(group_results)
        } else {
            Ok(())
        }
    }
}

pub struct Store {
    pub(crate) manifest: Handle<Manifest>,
    pub(crate) context: Handle<Context>,
    pub(crate) opt: Arc<ParsedOptions>,
}

impl Store {
    pub fn new(opt: Arc<ParsedOptions>, manifest: Handle<Manifest>, ctx: Handle<Context>) -> Self {
        Self {
            manifest,
            context: ctx,
            opt,
        }
    }

    pub(crate) fn start(&self) {
        self.context.start();
    }

    pub(crate) fn quit(&self) {
        // 1) stop new writes and flush outstanding WAL
        let _ = self.context.sync();

        // 2) stop background workers in order: evictor -> flusher -> buckets
        // bucket.quit will send Quit to evictor thread and wait ack
        self.manifest.buckets.quit();

        // 3) after evictor/flush threads stopped, shut down WAL threads
        self.context.quit();

        // 4) reclaim contexts (arena/page caches) first, then manifest
        self.context.reclaim();
        self.manifest.reclaim();
    }
}

/// The internal storage engine instance.
pub struct Inner {
    pub(crate) store: MutRef<Store>,
    pub(crate) gc: GCHandle,
}

impl Inner {
    const MAX_BUCKET_NAME_LEN: usize = 32;

    fn new_bucket(this: &Arc<Inner>, name: &str) -> Result<Bucket, OpCode> {
        if name.len() >= Self::MAX_BUCKET_NAME_LEN {
            return Err(OpCode::TooLarge);
        }
        let (meta, bucket_ctx) = this.store.manifest.create_bucket(name)?;

        Ok(Bucket {
            tree: Tree::new(this.store.clone(), ROOT_PID, bucket_ctx),
            _holder: meta,
            inner: this.clone(),
        })
    }

    fn get_bucket(this: &Arc<Inner>, name: &str) -> Result<Bucket, OpCode> {
        if name.len() >= Self::MAX_BUCKET_NAME_LEN {
            return Err(OpCode::TooLarge);
        }
        let meta = this.store.manifest.load_bucket_meta(name)?;
        let bucket_ctx = this.store.manifest.load_bucket_context(meta.bucket_id)?;

        Ok(Bucket {
            tree: Tree::new(this.store.clone(), ROOT_PID, bucket_ctx),
            _holder: meta,
            inner: this.clone(),
        })
    }

    /// manually unload bucket to release memory
    fn drop_bucket(self: &Inner, name: &str) -> Result<(), OpCode> {
        self.store.context.sync()?;
        self.store.manifest.unload_bucket(name)
    }

    fn del_bucket(self: &Inner, name: &str) -> Result<(), OpCode> {
        self.store.manifest.delete_bucket(name)
    }

    fn vacuum_bucket(self: &Inner, name: &str) -> Result<VacuumStats, OpCode> {
        if name.len() >= Self::MAX_BUCKET_NAME_LEN {
            return Err(OpCode::TooLarge);
        }
        let meta = self.store.manifest.load_bucket_meta(name)?;
        let bucket_ctx = self.store.manifest.load_bucket_context(meta.bucket_id)?;
        crate::store::gc::vacuum_bucket(self.store.clone(), bucket_ctx)
    }

    fn is_bucket_vacuuming(self: &Inner, name: &str) -> Result<bool, OpCode> {
        if name.len() >= Self::MAX_BUCKET_NAME_LEN {
            return Err(OpCode::TooLarge);
        }
        let meta = self.store.manifest.load_bucket_meta(name)?;
        let bucket_ctx = self.store.manifest.load_bucket_context(meta.bucket_id)?;
        Ok(bucket_ctx.state.is_vacuuming())
    }

    fn vacuum_meta(self: &Inner) -> Result<MetaVacuumStats, OpCode> {
        self.store.manifest.vacuum_meta(META_VACUUM_TARGET_BYTES)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.gc.quit();
        self.store.quit();
    }
}

/// A bucket is a named collection of key-value pairs.
#[derive(Clone)]
pub struct Bucket {
    pub(crate) tree: Tree,
    pub(crate) _holder: Arc<BucketMeta>,
    pub(crate) inner: Arc<Inner>,
}

impl Bucket {
    /// Begins a new read-write transaction.
    pub fn begin(&'_ self) -> Result<TxnKV<'_>, OpCode> {
        TxnKV::new(&self.inner.store.context, &self.tree)
    }

    /// Begins a new read-only transaction (view).
    pub fn view(&'_ self) -> Result<TxnView<'_>, OpCode> {
        TxnView::new(&self.inner.store.context, &self.tree)
    }

    /// Returns the unique identifier of this bucket.
    pub fn id(&self) -> u64 {
        self.tree.bucket_id()
    }

    /// Returns the options used by this bucket.
    pub fn options(&self) -> &Options {
        &self.inner.store.opt
    }
}

impl Deref for Bucket {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// The main entry point for the Mace storage engine.
#[derive(Clone)]
pub struct Mace {
    pub(crate) inner: Arc<Inner>,
}

impl Mace {
    /// Creates a new Mace instance with the given options.
    pub fn new(opt: ParsedOptions) -> Result<Self, OpCode> {
        let opt = Arc::new(opt);
        let (tx, erx) = channel();
        let (etx, rx) = channel();

        let mut builder = ManifestBuilder::new_with_channels(opt.clone(), tx, rx);
        builder.load()?;
        let manifest = Handle::new(builder.finish());

        let mut recover = Recovery::new(opt.clone());
        let (wal_boot, ctx) = recover.phase1(manifest.numerics.clone())?;
        let observer = Arc::new(StoreFlushObserver::new(manifest, ctx));
        let reader = Arc::new(StoreDataReader::new(manifest));
        manifest.set_context(ctx, reader, observer);

        let store = MutRef::new(Store::new(opt.clone(), manifest, ctx));

        recover.phase2(&wal_boot, store.clone())?;
        store.start();
        let handle = start_gc(store.clone(), store.context);
        let evictor = Evictor::new(
            opt.clone(),
            manifest.buckets,
            manifest.numerics.clone(),
            manifest.buckets.used.clone(),
            erx,
            etx,
        );
        evictor.start();

        Ok(Self {
            inner: Arc::new(Inner { store, gc: handle }),
        })
    }

    /// Returns the options used by this Mace instance.
    pub fn options(&self) -> &Options {
        &self.inner.store.opt
    }

    /// Creates a bucket with the given name.
    /// NOTE: name must be less than 32 bytes.
    pub fn new_bucket<S: AsRef<str>>(&self, name: S) -> Result<Bucket, OpCode> {
        Inner::new_bucket(&self.inner, name.as_ref())
    }

    /// Gets an existing bucket with the given name.
    /// NOTE: name must be less than 32 bytes.
    pub fn get_bucket<S: AsRef<str>>(&self, name: S) -> Result<Bucket, OpCode> {
        Inner::get_bucket(&self.inner, name.as_ref())
    }

    /// Returns a list of all active bucket names.
    pub fn active_buckets(&self) -> Vec<String> {
        self.inner.store.manifest.loaded_bucket_names()
    }

    /// Manually unloads a bucket to release memory.
    pub fn drop_bucket<S: AsRef<str>>(&self, name: S) -> Result<(), OpCode> {
        Inner::drop_bucket(&self.inner, name.as_ref())
    }

    /// Deletes a bucket and all its data.
    pub fn del_bucket<S: AsRef<str>>(&self, name: S) -> Result<(), OpCode> {
        Inner::del_bucket(&self.inner, name.as_ref())
    }

    /// Vacuums a bucket by scavenging and compacting its pages
    pub fn vacuum_bucket<S: AsRef<str>>(&self, name: S) -> Result<VacuumStats, OpCode> {
        Inner::vacuum_bucket(&self.inner, name.as_ref())
    }

    /// Returns whether bucket vacuum is currently running
    pub fn is_bucket_vacuuming<S: AsRef<str>>(&self, name: S) -> Result<bool, OpCode> {
        Inner::is_bucket_vacuuming(&self.inner, name.as_ref())
    }

    /// Vacuums metadata by compacting the manifest btree
    pub fn vacuum_meta(&self) -> Result<MetaVacuumStats, OpCode> {
        Inner::vacuum_meta(&self.inner)
    }

    /// Disables garbage collection.
    pub fn disable_gc(&self) {
        self.inner.gc.pause();
    }

    /// Enables garbage collection.
    pub fn enable_gc(&self) {
        self.inner.gc.resume();
    }

    /// Starts a garbage collection cycle immediately.
    pub fn start_gc(&self) {
        self.inner.gc.start();
    }

    /// Returns the number of data garbage collection cycles performed.
    pub fn data_gc_count(&self) -> u64 {
        self.inner.gc.data_gc_count()
    }

    /// Returns the number of blob garbage collection cycles performed.
    pub fn blob_gc_count(&self) -> u64 {
        self.inner.gc.blob_gc_count()
    }

    /// Returns the total number of buckets, including active and pending deletion ones.
    pub fn nr_buckets(&self) -> u64 {
        self.inner
            .store
            .manifest
            .nr_buckets
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Synchronizes all WAL to disk.
    pub fn sync(&self) -> Result<(), OpCode> {
        self.inner.store.context.sync()
    }
}
