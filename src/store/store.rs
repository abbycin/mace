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
use crate::types::refbox::BoxRef;
use crate::utils::Handle;
use crate::utils::MutRef;
pub use crate::utils::OpCode;
use crate::utils::ROOT_PID;
pub use crate::utils::options::Options;
use crate::utils::options::ParsedOptions;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::channel;

struct StoreFlushObserver {
    manifest: Handle<Manifest>,
    ctx: Handle<Context>,
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
        Self { manifest, ctx }
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

    fn on_flush(&self, result: FlushResult) -> Result<(), OpCode> {
        let done = result.done;
        let mut txn = self.manifest.begin();

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

        txn.record(MetaKind::Numerics, self.manifest.numerics.deref());
        txn.commit();

        self.manifest
            .add_data_stat(result.mem_data_stat, result.data_ivl);
        if let (Some(mem_blob_stat), Some(blob_ivl)) = (result.mem_blob_stat, result.blob_ivl) {
            self.manifest.add_blob_stat(mem_blob_stat, blob_ivl);
        }

        for (i, g) in self.ctx.groups().iter().enumerate() {
            let mut pos = result.flsn[i];
            if let Some(min) = g.active_txns.min_lsn()
                && min < pos
            {
                pos = min;
            }
            g.logging.lock().update_last_ckpt(pos);
        }
        self.manifest.numerics.signal.fetch_add(1, Relaxed);
        done.mark_done();
        Ok(())
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
}

impl Drop for Inner {
    fn drop(&mut self) {
        #[cfg(feature = "metric")]
        {
            use crate::index::{g_alloc_status, g_cas_status};
            use crate::map::g_flush_status;

            log::info!("\n{:#?}", g_alloc_status());
            log::info!("\n{:#?}", g_cas_status());
            log::info!("\n{:#?}", g_flush_status());
        }
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
        self.tree.bucket.bucket_id
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
        let (desc, ctx) = recover.phase1(manifest.numerics.clone())?;
        let observer = Arc::new(StoreFlushObserver::new(manifest, ctx));
        let reader = Arc::new(StoreDataReader::new(manifest));
        manifest.set_context(ctx, reader, observer);

        let store = MutRef::new(Store::new(opt.clone(), manifest, ctx));

        recover.phase2(ctx, &desc, store.clone())?;
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
