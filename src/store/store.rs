use crate::cc::context::Context;
use crate::index::tree::Tree;
pub use crate::index::txn::{TxnKV, TxnView};
use crate::map::adapter::{ManifestCheckpointObserver, ManifestDataReader};
use crate::map::evictor::Evictor;
use crate::meta::builder::ManifestBuilder;
use crate::meta::{BucketMeta, Manifest};
use crate::store::gc::{GCHandle, drain_abort_clean_at_exit, start_gc};
use crate::store::recovery::Recovery;
use crate::utils::Handle;
use crate::utils::MutRef;
pub use crate::utils::OpCode;
use crate::utils::ROOT_PID;
pub use crate::utils::options::Options;
use crate::utils::options::{
    BucketOptions, ParsedOptions, PersistedBucketOptions, PersistedOptions,
};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::mpsc::channel;

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

    pub(crate) fn abort(&mut self) {
        self.manifest.abort();
        self.context.quit();
        self.context.reclaim();
        self.manifest.reclaim();
    }

    pub(crate) fn quit(&mut self) {
        // wait for terminal publication while flushing remains available
        self.context.drain_inflight();

        // 2) stop background workers in order: evictor -> flusher -> buckets
        // bucket.quit will send Quit to evictor thread and wait ack
        self.manifest.buckets.quit();

        // force the WAL barrier and stop the collector
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

    fn new_bucket(this: &Arc<Inner>, name: &str, opt: BucketOptions) -> Result<Bucket, OpCode> {
        if name.len() >= Self::MAX_BUCKET_NAME_LEN {
            return Err(OpCode::TooLarge);
        }
        let (meta, bucket_ctx) = this.store.manifest.create_bucket(name, opt)?;

        Ok(Bucket {
            tree: Tree::new(this.store.clone(), ROOT_PID, bucket_ctx),
            _holder: meta,
            inner: this.clone(),
        })
    }

    fn open_bucket(this: &Arc<Inner>, name: &str) -> Result<Bucket, OpCode> {
        if name.len() >= Self::MAX_BUCKET_NAME_LEN {
            return Err(OpCode::TooLarge);
        }
        let meta = this.store.manifest.load_bucket_meta(name)?;
        let bucket_ctx = this.store.manifest.load_bucket_context(meta.id)?;

        Ok(Bucket {
            tree: Tree::new(this.store.clone(), ROOT_PID, bucket_ctx),
            _holder: meta,
            inner: this.clone(),
        })
    }

    /// reads the persisted bucket options from metadata only: no bucket
    /// runtime, page table, or context is loaded
    fn get_bucket_options(this: &Arc<Inner>, name: &str) -> Result<Option<BucketOptions>, OpCode> {
        if name.len() >= Self::MAX_BUCKET_NAME_LEN {
            return Err(OpCode::TooLarge);
        }
        Ok(this
            .store
            .manifest
            .load_bucket_options(name)?
            .map(PersistedBucketOptions::to_runtime))
    }

    fn open_bucket_with_options(
        this: &Arc<Inner>,
        name: &str,
        opt: BucketOptions,
    ) -> Result<Bucket, OpCode> {
        if name.len() >= Self::MAX_BUCKET_NAME_LEN {
            return Err(OpCode::TooLarge);
        }
        let meta = this.store.manifest.load_bucket_meta(name)?;
        // persisted fields always come from BucketMeta; runtime fields are fixed at context load
        let bucket_ctx = this
            .store
            .manifest
            .load_bucket_context_with_options(meta.id, Some(&opt))?;

        Ok(Bucket {
            tree: Tree::new(this.store.clone(), ROOT_PID, bucket_ctx),
            _holder: meta,
            inner: this.clone(),
        })
    }

    fn update_bucket_opt(this: &Arc<Inner>, name: &str, opt: BucketOptions) -> Result<(), OpCode> {
        if name.len() >= Self::MAX_BUCKET_NAME_LEN {
            return Err(OpCode::TooLarge);
        }
        this.store.manifest.update_bucket_options(name, opt)
    }

    /// manually unload bucket to release memory
    fn drop_bucket(self: &Inner, name: &str) -> Result<(), OpCode> {
        self.store.context.sync()?;
        self.store.manifest.unload_bucket(name)
    }

    fn del_bucket(self: &Inner, name: &str) -> Result<(), OpCode> {
        self.store.manifest.delete_bucket(name)
    }

    fn checkpoint(&self, bucket_id: u64) {
        if let Ok(ctx) = self.store.manifest.load_bucket_context(bucket_id) {
            ctx.checkpoint();
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // stop GC before draining abort-clean tasks
        self.gc.quit();
        self.store.raw_ref().context.drain_inflight();
        // drain abort-clean before the final checkpoint
        let ctx = self.store.raw_ref().context;
        drain_abort_clean_at_exit(self.store.clone(), ctx);
        self.store.raw_ref().quit();
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

    /// Flushes dirty pages in a fresh checkpoint and waits for it to finish
    pub fn checkpoint_and_wait(&self) {
        self.tree.bucket.checkpoint_and_wait(false);
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
        let manifest_path = opt.manifest();
        let _ = opt
            .fs
            .try_exists(&manifest_path)
            .map_err(|_| OpCode::IoError)?;

        let mut builder = ManifestBuilder::new_with_channels(opt.clone(), tx, rx);
        let persisted_options = builder.load()?;
        let manifest = Handle::new(builder.finish());
        // persisted route records the last completed recovery/switch
        let persisted_sync_on_write = manifest
            .load_persisted_options_if_present()?
            .map(|p| p.sync_on_write);
        let route_switch = persisted_sync_on_write.is_some_and(|p| p != opt.sync_on_write);
        // legacy per-group files require durable-layout migration
        let layout_migration = opt.sync_on_write && has_legacy_per_group_wal(opt.as_ref())?;
        let rebuild_epoch = route_switch || layout_migration;

        let mut recover = Recovery::new(opt.clone(), rebuild_epoch);
        let (wal_boot, ctx) = match recover.phase1(manifest, manifest.sequences.clone()) {
            Ok(parts) => parts,
            Err(err) => {
                manifest.reclaim();
                return Err(err);
            }
        };
        let observer = Arc::new(ManifestCheckpointObserver::new(manifest, ctx));
        let reader = Arc::new(ManifestDataReader::new(manifest));
        manifest.set_context(ctx, reader, observer.clone());

        let store = MutRef::new(Store::new(opt.clone(), manifest, ctx));

        if let Err(err) = recover.phase2(&wal_boot, store.clone()) {
            recover.abort(store.clone());
            store.raw_ref().abort();
            return Err(err);
        }
        // write options only after recovery and switching complete
        if let Some(_persisted_options) = persisted_options
            && let Err(err) =
                manifest.store_persisted_options(&PersistedOptions::from_options(opt.as_ref()))
        {
            recover.abort(store.clone());
            store.raw_ref().abort();
            return Err(err);
        }
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_switch_after_options_writeback");
        store.start();
        let handle = start_gc(store.clone(), store.context);
        let finish_handle = handle.clone();
        observer.set_finish_hook(Arc::new(move || {
            finish_handle.wal_clean(manifest, ctx);
        }));
        let evictor = Evictor::new(opt.clone(), manifest.buckets, erx, etx);
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
    pub fn new_bucket<S: AsRef<str>>(&self, name: S, opt: BucketOptions) -> Result<Bucket, OpCode> {
        Inner::new_bucket(&self.inner, name.as_ref(), opt.validate())
    }

    /// Gets an existing bucket with the given name.
    ///
    /// If the bucket is not loaded, this fixes its runtime merge operator to `None`
    /// until the context is unloaded.
    /// NOTE: name must be less than 32 bytes.
    pub fn open_bucket<S: AsRef<str>>(&self, name: S) -> Result<Bucket, OpCode> {
        Inner::open_bucket(&self.inner, name.as_ref())
    }

    /// Reads the persisted options of a bucket from its metadata without
    /// loading the bucket context (no page table or runtime state is touched).
    ///
    /// Returns `None` when the bucket does not exist. The returned
    /// [`BucketOptions`] carries the persisted subset only: the runtime merge
    /// operator is never persisted and is always `None` here — use
    /// [`Mace::open_bucket_with_options`] to adopt a runtime operator.
    ///
    /// NOTE: name must be less than 32 bytes.
    pub fn get_bucket_options<S: AsRef<str>>(
        &self,
        name: S,
    ) -> Result<Option<BucketOptions>, OpCode> {
        Inner::get_bucket_options(&self.inner, name.as_ref())
    }

    /// Gets an existing bucket and uses only the runtime fields of `options`
    /// (currently the [`crate::MergeOperator`]). Persisted bucket fields always come from
    /// the stored bucket metadata.
    ///
    /// The merge operator is fixed before a newly loaded bucket context is published.
    /// Re-opening a loaded context with a different operator, or providing one after the
    /// context was loaded without one, returns [`OpCode::Invalid`]. After
    /// [`Mace::drop_bucket`] unloads the context, the next load chooses the operator again.
    /// Providing an operator does not guarantee it matches the one that wrote existing
    /// merge operands; that semantic contract belongs to the caller.
    ///
    /// NOTE: name must be less than 32 bytes.
    pub fn open_bucket_with_options<S: AsRef<str>>(
        &self,
        name: S,
        opt: BucketOptions,
    ) -> Result<Bucket, OpCode> {
        Inner::open_bucket_with_options(&self.inner, name.as_ref(), opt.validate())
    }

    /// Updates the persisted bucket-scoped options of an existing bucket
    ///
    /// Runtime-only fields of the passed options (currently the merge operator)
    /// are ignored: they are never persisted. For an existing bucket the
    /// operator is adopted through [`Mace::open_bucket_with_options`]; a new
    /// bucket adopts it at creation via [`Mace::new_bucket`].
    ///
    /// Returns [`OpCode::Again`] if the bucket is currently loaded
    ///
    /// Returns [`OpCode::Invalid`] if the requested [`BucketOptions`] conflict with
    /// persisted compatibility-sensitive bucket options
    pub fn update_bucket_opt<S: AsRef<str>>(
        &self,
        name: S,
        opt: BucketOptions,
    ) -> Result<(), OpCode> {
        Inner::update_bucket_opt(&self.inner, name.as_ref(), opt.validate())
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

/// whether a durable open found legacy per-group WAL files
fn has_legacy_per_group_wal(opt: &ParsedOptions) -> Result<bool, OpCode> {
    let prefix = format!("{}{}", Options::WAL_PREFIX, Options::SEP);
    for entry in opt.fs.read_dir(&opt.log_root())? {
        let Some(name) = entry.file_name() else {
            continue;
        };
        let Some(raw) = name.to_str() else {
            continue;
        };
        if raw.starts_with(&prefix) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, sync::Arc};

    use crate::{
        RandomPath,
        io::testfs::{InjectOp, InjectedFileSystem},
    };

    use super::{Mace, Options};
    use crate::OpCode;

    #[test]
    fn new_surfaces_manifest_try_exists_error_through_file_system() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        let manifest_path = opt.manifest();
        let fs = Arc::new(InjectedFileSystem::new());
        fs.fail_once(
            InjectOp::TryExists,
            manifest_path.clone(),
            ErrorKind::PermissionDenied,
        );
        opt.fs = fs.clone();

        let err = Mace::new(opt.validate().expect("options must validate"))
            .err()
            .expect("manifest try_exists fault must fail open");
        assert_eq!(err, OpCode::IoError);
        assert!(
            fs.calls()
                .iter()
                .any(|(op, path)| *op == InjectOp::TryExists && *path == manifest_path),
            "manifest existence probe must go through FileSystem"
        );
    }
}
