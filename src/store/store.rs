use crate::cc::context::Context;
use crate::index::tree::Tree;
pub use crate::index::txn::{TxnKV, TxnView};
use crate::map::adapter::{ManifestCheckpointObserver, ManifestDataReader};
use crate::map::evictor::Evictor;
use crate::meta::builder::ManifestBuilder;
use crate::meta::{BucketMeta, Manifest};
use crate::store::gc::{GCHandle, start_gc};
use crate::store::recovery::Recovery;
use crate::utils::Handle;
use crate::utils::MutRef;
pub use crate::utils::OpCode;
use crate::utils::ROOT_PID;
pub use crate::utils::options::Options;
use crate::utils::options::{BucketOptions, ParsedOptions};
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

    fn get_bucket(this: &Arc<Inner>, name: &str) -> Result<Bucket, OpCode> {
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
        self.gc.quit();
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
        if let Some(persisted_options) = persisted_options
            && let Err(err) = manifest.store_persisted_options(&persisted_options)
        {
            manifest.reclaim();
            return Err(err);
        }

        let mut recover = Recovery::new(opt.clone());
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
    /// NOTE: name must be less than 32 bytes.
    pub fn get_bucket<S: AsRef<str>>(&self, name: S) -> Result<Bucket, OpCode> {
        Inner::get_bucket(&self.inner, name.as_ref())
    }

    /// Updates the persisted bucket-scoped options of an existing bucket
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

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, sync::Arc};

    use crate::{
        RandomPath,
        io::testfs::{InjectOp, InjectedFileSystem},
    };

    use super::{Mace, Options};

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
        assert_eq!(err, crate::OpCode::IoError);
        assert!(
            fs.calls()
                .iter()
                .any(|(op, path)| *op == InjectOp::TryExists && *path == manifest_path),
            "manifest existence probe must go through FileSystem"
        );
    }
}
