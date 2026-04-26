use parking_lot::Mutex;

use crate::cc::context::Context;
use crate::index::tree::Tree;
pub use crate::index::txn::{TxnKV, TxnView};
use crate::map::DataReader;
use crate::map::evictor::Evictor;
use crate::map::flush::{CheckpointObserver, FlushDirective, FlushResult};
use crate::meta::builder::ManifestBuilder;
use crate::meta::{
    BlobStat, BucketMeta, DataStat, IntervalPair, Manifest, MemBlobStat, MemDataStat, MetaKind, Txn,
};
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
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::channel;

struct StoreFlushObserver {
    manifest: Handle<Manifest>,
    ctx: Handle<Context>,
    handle: Mutex<Option<GCHandle>>,
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
            handle: Mutex::new(None),
        }
    }

    #[cfg(feature = "failpoints")]
    #[cold]
    fn abort_flush_publish(stage: &str, err: OpCode) -> ! {
        log::error!("flush publish {} failed: {:?}", stage, err);
        std::process::abort()
    }

    fn attach_handle(&self, handle: GCHandle) {
        self.handle.lock().replace(handle);
    }

    fn update_stat_interval(
        &self,
        txn: &mut Txn,
        result: &mut FlushResult,
    ) -> (Vec<DataStat>, Vec<BlobStat>) {
        let bucket_id = result.bucket_id;
        let data_tick = result
            .data_ivls
            .iter()
            .map(|x| x.file_id)
            .max()
            .unwrap_or_else(|| self.manifest.numerics.next_data_id.load(Relaxed));
        let mut data_by_file = BTreeMap::<u64, DataStat>::new();
        for stat in self
            .manifest
            .apply_data_junks(bucket_id, data_tick, &result.data_junk)
        {
            data_by_file.insert(stat.file_id, stat);
        }
        let mut blob_by_file = BTreeMap::<u64, BlobStat>::new();
        for stat in self.manifest.apply_blob_junks(bucket_id, &result.blob_junk) {
            blob_by_file.insert(stat.file_id, stat);
        }

        #[cfg(feature = "extra_check")]
        assert_eq!(result.data_stats.len(), result.data_ivls.len());
        #[cfg(feature = "extra_check")]
        assert_eq!(result.blob_stats.len(), result.blob_ivls.len());

        for (mem_stat, ivl) in result
            .data_stats
            .drain(..)
            .zip(result.data_ivls.iter().copied())
        {
            data_by_file
                .entry(mem_stat.file_id)
                .or_insert_with(|| mem_stat);
            self.manifest.clear_orphan_data_file(txn, ivl.file_id);
        }

        for (mem_stat, ivl) in result
            .blob_stats
            .drain(..)
            .zip(result.blob_ivls.iter().copied())
        {
            blob_by_file
                .entry(mem_stat.file_id)
                .or_insert_with(|| mem_stat);
            self.manifest.clear_orphan_blob_file(txn, ivl.file_id);
        }
        (
            data_by_file.into_values().collect(),
            blob_by_file.into_values().collect(),
        )
    }

    fn publish(&self, mut result: FlushResult) {
        let has_new_files = !result.data_ivls.is_empty() || !result.blob_ivls.is_empty();
        result.sync(); // must be called before updatin manifest
        if has_new_files {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_flush_after_data_dir_sync");
        }
        let bucket_id = result.bucket_id;
        let frontier_delta = *result.latest_chkpoint_lsn.deref();
        let bucket_frontier = self
            .manifest
            .merge_bucket_frontier(bucket_id, &frontier_delta);
        let mut txn = self.manifest.begin();
        let (data_stats, blob_stats) = self.update_stat_interval(&mut txn, &mut result);

        for ivl in &result.data_ivls {
            txn.record(MetaKind::DataInterval, ivl);
        }
        for ivl in &result.blob_ivls {
            txn.record(MetaKind::BlobInterval, ivl);
        }

        data_stats.iter().for_each(|x| {
            txn.record(MetaKind::DataStat, x);
        });
        blob_stats.iter().for_each(|x| {
            txn.record(MetaKind::BlobStat, x);
        });

        txn.record(MetaKind::BucketFrontier, &bucket_frontier);
        txn.record(MetaKind::Map, &result.map_table);
        txn.record(MetaKind::Numerics, self.manifest.numerics.deref());

        #[cfg(feature = "failpoints")]
        if let Err(e) = crate::utils::failpoint::check("mace_flush_before_manifest_commit") {
            Self::abort_flush_publish("before manifest commit", e);
        }
        txn.commit();

        self.manifest.clear_synced_data();
        self.manifest.clear_synced_blob();

        #[cfg(feature = "failpoints")]
        if let Err(e) = crate::utils::failpoint::check("mace_flush_after_manifest_commit") {
            Self::abort_flush_publish("after manifest commit", e);
        }

        let groups = self.ctx.groups();
        let sync = self.ctx.opt.sync_on_write;
        let global_frontier = self.manifest.global_frontier_lower_bound(groups.len());

        for (i, g) in groups.iter().enumerate() {
            let mut pos = global_frontier[i];
            if let Some(min) = g.active_txns.min_lsn()
                && min < pos
            {
                pos = min;
            }
            let mut lk = g.logging.lock();
            if lk.update_checkpoint(pos) && sync {
                let mut f = lk.writer.clone();
                drop(lk);
                // checkpoint must be synced in durable mode
                f.sync();
            }
        }
    }
}

impl CheckpointObserver for StoreFlushObserver {
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

    fn stage_unsynced_data_file(&self, file_id: u64) {
        self.manifest.stage_unsynced_data_file(file_id);
    }

    fn stage_unsynced_blob_file(&self, file_id: u64) {
        self.manifest.stage_unsynced_blob_file(file_id);
    }

    fn stage_orphan_data_file(&self, file_id: u64) {
        self.manifest.stage_orphan_data_file(file_id);
    }

    fn stage_orphan_blob_file(&self, file_id: u64) {
        self.manifest.stage_orphan_blob_file(file_id);
    }

    fn update_data_mem_interval_stat(&self, ivl: IntervalPair, stat: MemDataStat) {
        self.manifest.add_data_stat(stat, ivl);
    }

    fn update_blob_mem_interval_stat(&self, ivl: IntervalPair, stat: MemBlobStat) {
        self.manifest.add_blob_stat(stat, ivl);
    }

    fn on_checkpoint(&self, result: FlushResult) {
        self.publish(result)
    }

    fn finish_checkpoint(&self) {
        let h = self.handle.lock();
        if let Some(h) = h.as_ref() {
            h.wal_clean(self.ctx);
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

    fn checkpoint(&self, bucket_id: u64) {
        if let Ok(ctx) = self.store.manifest.load_bucket_context(bucket_id) {
            ctx.checkpoint();
        }
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

    /// Starts a manual checkpoint which will flush dirty pages to disk and may trigger WAL gc
    pub fn checkpoint(&self) {
        self.inner.checkpoint(self.id());
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
        manifest.set_context(ctx, reader, observer.clone());

        let store = MutRef::new(Store::new(opt.clone(), manifest, ctx));

        recover.phase2(&wal_boot, store.clone())?;
        store.start();
        let handle = start_gc(store.clone(), store.context);
        observer.attach_handle(handle.clone());
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
