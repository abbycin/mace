use crate::cc::context::Context;
use crate::index::tree::Tree;
pub use crate::index::txn::{TxnKV, TxnView};
use crate::map::adapter::{ManifestCheckpointObserver, ManifestDataReader};
use crate::map::evictor::Evictor;
use crate::meta::builder::{ManifestBuilder, OpenPlan};
use crate::meta::{BucketMeta, Manifest};
use crate::store::checkpoint::CheckpointInfo;
use crate::store::gc::{GCHandle, drain_abort_clean_at_exit, start_gc};
use crate::store::recovery::{Recovery, TargetPlan};
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
use std::sync::atomic::Ordering::Acquire;
use std::sync::mpsc::channel;

pub struct Store {
    pub(crate) manifest: Handle<Manifest>,
    pub(crate) context: Handle<Context>,
    pub(crate) opt: Arc<ParsedOptions>,
    /// serializes `create_checkpoint` calls: the staging path must be absent before it is
    /// created, and two concurrent calls would otherwise race on the same one
    pub(crate) checkpoint_lock: parking_lot::Mutex<()>,
}

impl Store {
    pub fn new(opt: Arc<ParsedOptions>, manifest: Handle<Manifest>, ctx: Handle<Context>) -> Self {
        Self {
            manifest,
            context: ctx,
            opt,
            checkpoint_lock: parking_lot::Mutex::new(()),
        }
    }

    pub(crate) fn start(&self) {
        self.context.start();
    }

    pub(crate) fn abort(&mut self) {
        self.manifest.abort();
        // a failed open has no foreground write whose durability a barrier would
        // establish, and its loggers may be detached or only partially installed;
        // the runtime barrier is undefined in that state
        self.context.abort_recovery_open();
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

/// Shutdown is reachable only from `Inner::drop`, so no public API admits a
/// transaction or view while the store is quitting: begin/view require a live bucket
/// borrow, and the last `Arc<Inner>` drop is the only caller of `quit`.
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

        let mut builder = ManifestBuilder::new_with_channels(opt.clone(), tx, rx)?;
        let load = builder.load()?;
        let plan = load.plan;
        // the builder's own cleanups run before recovery: a failed directory sync there fails
        // the open instead of taking the process down
        let manifest = Handle::new(builder.finish()?);
        let mut recover = Recovery::new(opt.clone(), plan);
        let (wal_boot, recovery_ctx) = match recover.phase1(manifest, manifest.sequences.clone()) {
            Ok(parts) => parts,
            Err(err) => {
                manifest.reclaim();
                return Err(err);
            }
        };
        let mut ctx = recovery_ctx;
        let mut observer = Arc::new(ManifestCheckpointObserver::new(manifest, ctx));
        let reader = Arc::new(ManifestDataReader::new(manifest));
        if let Err(err) = manifest.set_context(ctx, reader.clone(), observer.clone()) {
            // the recovery flusher never started and no logger was detached, so the
            // context is dropped without a barrier; the observer must go first, because it
            // holds a handle to that context
            drop(observer);
            ctx.reclaim();
            manifest.reclaim();
            return Err(err);
        }

        let store = MutRef::new(Store::new(opt.clone(), manifest, ctx));

        if let Err(err) = recover.phase2(&wal_boot, store.clone()) {
            recover.abort(store.clone());
            store.raw_ref().abort();
            return Err(err);
        }
        // a same-width rebuild stopped the recovery flusher before detaching its
        // writers, and this Context becomes the runtime one: restart the flusher on it
        if plan.rebuild_epoch() && !plan.group_resize {
            let mut manifest = store.manifest;
            if let Err(err) =
                manifest.restart_checkpoint_flusher(ctx, reader.clone(), observer.clone())
            {
                recover.abort(store.clone());
                store.raw_ref().abort();
                return Err(err);
            }
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash(
                "mace_switch_after_epoch_writer_install_before_writeback",
            );
        }
        // a width change must hand recovery's Context over to the target-width runtime one: the
        // recovery Context never serves a foreground writer, and the migration cannot be recorded
        // until the runtime Context it hands over is installed
        if plan.group_resize {
            let installed = recover
                .target_plan(store.manifest, recover.epoch_start())
                .and_then(|target| install_target_context(&opt, store.clone(), plan, target));
            match installed {
                Ok((new_ctx, new_observer)) => {
                    ctx = new_ctx;
                    observer = new_observer;
                }
                Err(err) => {
                    recover.abort(store.clone());
                    store.raw_ref().abort();
                    return Err(err);
                }
            }
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash(
                "mace_switch_after_context_install_before_options_writeback",
            );
        }
        // write options only after recovery and switching complete
        if let Some(_persisted_options) = load.writeback
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
    /// persisted compatibility-sensitive bucket options, including an increase to `inline_size`
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

    /// Writes an independently openable copy of this instance to `target`.
    ///
    /// `target` must not exist. The returned [`CheckpointInfo`] names the metadata
    /// generation the copy's manifest equals and the per-bucket durable frontiers read
    /// from the copy; both are **lower bounds**, not the copy's read-as-of position. A
    /// commit landing between the metadata snapshot and the WAL cut is replayed when the
    /// copy opens, so the copy reads at or after those frontiers and never before them.
    /// What the copy guarantees is completeness: every commit the copied frontiers fold
    /// in is present. Runtime file deletions stay suspended while the copy is written, so
    /// every file the copy references survives until it is complete. The call is
    /// synchronous and returns only after the copy is durable.
    ///
    /// The copy is built in `<target>.tmp`, which must not exist either: the call is
    /// refused with [`OpCode::Exist`] when it does. Nothing at that path is ever deleted,
    /// so an unrelated directory whose name ends in `.tmp` is safe, and a caller retrying
    /// after a crashed run has to remove the staging directory that run left behind.
    ///
    /// A failure before the final rename removes the staging directory and leaves
    /// `target` absent. A failure after it (the parent-directory sync) returns `Err`
    /// while a complete, openable copy is already visible at `target`; a caller that
    /// must not keep that copy has to remove `target` itself.
    pub fn create_checkpoint<P: AsRef<std::path::Path>>(
        &self,
        target: P,
    ) -> Result<CheckpointInfo, OpCode> {
        self.inner.store.create_checkpoint(target.as_ref())
    }
}

/// hand the recovery Context over to the target-width runtime Context.
///
/// `Handle<Context>` is `Copy` and `reclaim` keeps no reference count, so the order here is
/// load-bearing: the recovery flusher must stop before the rebind detaches it (`Checkpoint` has
/// no `Drop` and `BucketMgr::start` replaces it), every holder must point at the target Context
/// before the retired one is reclaimed exactly once, and the new era's log directory entries must
/// be durable before the open may record the migration.
///
/// the caller relies on phase2 having evicted every recovery-loaded bucket: a live `BucketContext`
/// checkpoint clone or resident loader would still hold the retired handle
fn install_target_context(
    opt: &Arc<ParsedOptions>,
    store: MutRef<Store>,
    plan: OpenPlan,
    target: TargetPlan,
) -> Result<(Handle<Context>, Arc<ManifestCheckpointObserver>), OpCode> {
    let mut manifest = store.manifest;
    assert!(
        manifest.buckets.buckets.is_empty(),
        "context handoff requires every recovery-loaded bucket to be evicted first"
    );
    if let Some(flush) = manifest.take_checkpoint_flusher() {
        flush.quit();
    }

    let retired = store.context;
    // the target boot list carries the era start, the post-wipe floor seeds and the target width,
    // so the constructor is the single place this open initializes target logger state
    let ctx = Handle::new(Context::new(
        opt.clone(),
        plan.target_groups,
        manifest.sequences.clone(),
        &target.boots,
    ));
    ctx.init_safe_exclusive(manifest.sequences.oracle.load(Acquire));
    // data-first: the era files the target loggers just created must reach the directory before
    // the options writeback makes the width change durable
    if let Err(err) = opt.try_sync_log_dir() {
        // the target Context was never published and never started: its loggers own only fresh
        // era files, and the caller's abort path knows only the recovery Context
        ctx.reclaim();
        return Err(err);
    }

    let observer = Arc::new(ManifestCheckpointObserver::new(manifest, ctx));
    let reader = Arc::new(ManifestDataReader::new(manifest));
    if let Err(err) = manifest.set_context(ctx, reader, observer.clone()) {
        // the target Context was never published and never started, so no holder may
        // keep its handle: drop the observer, then the context, without a barrier
        drop(observer);
        ctx.reclaim();
        return Err(err);
    }
    store.raw_ref().context = ctx;
    retired.reclaim();
    Ok((ctx, observer))
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
    use crate::utils::options::BucketOptions;

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

    /// a route switch that fails inside the era wipe must leave through the recovery-open
    /// cleanup: the error returns, the WAL barrier never runs on the already detached
    /// writers, and the leftover durable intent is completed by the next open
    #[test]
    fn wipe_failure_after_the_writer_detach_returns_and_the_retry_migrates() {
        let root = RandomPath::tmp();
        let mut durable = Options::new(&*root);
        durable.concurrent_write = 1;
        durable.sync_on_write = true;
        durable.gc_timeout = 60_000;
        {
            let mace = Mace::new(durable.validate().expect("options must validate"))
                .expect("durable open must work");
            let bucket = mace
                .new_bucket("x", BucketOptions::default())
                .expect("bucket must open");
            let txn = bucket.begin().expect("begin must work");
            txn.put("k", b"v").expect("put must work");
            txn.commit().expect("commit must work");
        }

        // the switch to the relaxed route wipes the shared era; its first removal fails
        // after the recovery writer is already closed
        let mut switching = Options::new(&*root);
        switching.concurrent_write = 1;
        switching.sync_on_write = false;
        switching.gc_timeout = 60_000;
        let fs = Arc::new(InjectedFileSystem::new());
        fs.fail_once(
            InjectOp::RemoveFile,
            switching.wal_file(0, 0),
            ErrorKind::PermissionDenied,
        );
        switching.fs = fs;
        let err = Mace::new(switching.validate().expect("options must validate"))
            .err()
            .expect("the wipe fault must fail the open");
        assert_eq!(err, OpCode::IoError);

        // the failed attempt is recoverable: the retry completes the migration and reads
        // the committed value
        let mut retry = Options::new(&*root);
        retry.concurrent_write = 1;
        retry.sync_on_write = false;
        retry.gc_timeout = 60_000;
        let mace = Mace::new(retry.validate().expect("options must validate"))
            .expect("the retry must open");
        let bucket = mace.open_bucket("x").expect("bucket must reopen");
        let view = bucket.view().expect("view must open");
        assert_eq!(
            view.get("k").expect("get must work").slice(),
            b"v",
            "the retry must expose the committed value"
        );
    }

    /// a log-directory sync failure after the epoch writers are installed must return an error
    /// through the recovery-open cleanup: that sync is the migration's data-first gate, so a
    /// failure there means no options writeback, and it must not panic mid-handoff
    #[test]
    fn log_dir_sync_failure_after_the_epoch_install_returns_and_the_retry_migrates() {
        let root = RandomPath::tmp();
        // a database with no WAL history at all: the rebuild's wipe commits no recycle intent,
        // so the only log-directory sync this open performs is the one after the epoch install
        {
            let mut opt = Options::new(&*root);
            opt.concurrent_write = 1;
            opt.sync_on_write = false;
            opt.gc_timeout = 60_000;
            let _mace =
                Mace::new(opt.validate().expect("options must validate")).expect("seed open");
        }
        let log_root = Options::new(&*root).log_root();
        for entry in std::fs::read_dir(&log_root)
            .expect("read log dir")
            .flatten()
        {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("wal_") || name.starts_with("group_wal_") {
                std::fs::remove_file(entry.path()).expect("remove wal file");
            }
        }

        let mut switching = Options::new(&*root);
        switching.concurrent_write = 1;
        switching.sync_on_write = true;
        switching.gc_timeout = 60_000;
        let fs = Arc::new(InjectedFileSystem::new());
        fs.fail_once(
            InjectOp::SyncDir,
            log_root.clone(),
            ErrorKind::PermissionDenied,
        );
        switching.fs = fs.clone();
        let err = Mace::new(switching.validate().expect("options must validate"))
            .err()
            .expect("the log-directory sync fault must fail the open");
        assert_eq!(err, OpCode::IoError);
        assert!(
            fs.calls()
                .iter()
                .any(|(op, path)| *op == InjectOp::SyncDir && *path == log_root),
            "the open must reach the era install's log-directory sync"
        );

        // the failed attempt left a namespace the retry migrates: no panic, no recorded
        // migration
        let mut retry = Options::new(&*root);
        retry.concurrent_write = 1;
        retry.sync_on_write = true;
        retry.gc_timeout = 60_000;
        let mace = Mace::new(retry.validate().expect("options must validate"))
            .expect("the retry must open");
        drop(mace);
    }
}
