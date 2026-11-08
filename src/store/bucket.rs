//! public bucket API: naming, creation, opening, unloading and deletion

use std::sync::Arc;

use crate::index::tree::Tree;
use crate::map::buffer::BucketLease;
use crate::utils::OpCode;
use crate::utils::ROOT_PID;
use crate::utils::options::{BucketOptions, PersistedBucketOptions};

use super::store::{Inner, Mace};

/// the longest accepted bucket name, in bytes
pub(crate) const MAX_BUCKET_NAME_LEN: usize = 32;

/// the one name boundary of the bucket API
///
/// every entry point validates here, so the catalog and the manager never see an empty or
/// oversized name and no longer repeat the length check per call site
fn validate_bucket_name(name: &str) -> Result<(), OpCode> {
    if name.is_empty() {
        return Err(OpCode::Invalid);
    }
    if name.len() >= MAX_BUCKET_NAME_LEN {
        return Err(OpCode::TooLarge);
    }
    Ok(())
}

/// A bucket is a named collection of key-value pairs.
///
/// Every handle holds a lease on the bucket runtime; unloading or deleting the bucket is refused
/// while any clone is alive.
#[derive(Clone)]
pub struct Bucket {
    pub(crate) tree: Tree,
    /// the lease that keeps this bucket from being unloaded or deleted while the handle lives
    pub(crate) _lease: BucketLease,
    pub(crate) inner: Arc<Inner>,
}

impl Bucket {
    /// Begins a new read-write transaction.
    pub fn begin(&'_ self) -> Result<crate::TxnKV<'_>, OpCode> {
        crate::TxnKV::new(&self.inner.store.context, &self.tree)
    }

    /// Begins a new read-only transaction (view).
    pub fn view(&'_ self) -> Result<crate::TxnView<'_>, OpCode> {
        crate::TxnView::new(&self.inner.store.context, &self.tree)
    }

    /// Starts a manual checkpoint which will flush dirty pages to disk and may trigger WAL gc
    pub fn checkpoint(&self) {
        self.tree.bucket.checkpoint();
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
    ///
    /// This is the engine-wide [`crate::Options`] the store was opened with, not the bucket's
    /// persisted options: those are read through `Mace::get_bucket_options` and are what the
    /// bucket's pages were written with.
    pub fn options(&self) -> &crate::Options {
        &self.inner.store.opt
    }
}

impl Inner {
    fn new_bucket(this: &Arc<Inner>, name: &str, opt: BucketOptions) -> Result<Bucket, OpCode> {
        validate_bucket_name(name)?;
        let (lease, bucket_ctx) = this.store.manifest.create_bucket(name, opt)?;

        Ok(Bucket {
            tree: Tree::new(this.store.clone(), ROOT_PID, bucket_ctx),
            _lease: lease,
            inner: this.clone(),
        })
    }

    fn open_bucket(this: &Arc<Inner>, name: &str) -> Result<Bucket, OpCode> {
        validate_bucket_name(name)?;
        // the catalog resolves and leases the bucket under the lifecycle lock before the context
        // is loaded, so a concurrent unload/delete cannot hand out an expired runtime
        let open = this
            .store
            .manifest
            .catalog
            .load_for_open(&this.store.manifest.buckets, name)?;
        let bucket_ctx = this.store.manifest.load_bucket_context(open.meta.id)?;

        Ok(Bucket {
            tree: Tree::new(this.store.clone(), ROOT_PID, bucket_ctx),
            _lease: open.lease,
            inner: this.clone(),
        })
    }

    /// reads the persisted bucket options from metadata only: no bucket
    /// runtime, page table, or context is loaded
    fn get_bucket_options(this: &Arc<Inner>, name: &str) -> Result<Option<BucketOptions>, OpCode> {
        validate_bucket_name(name)?;
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
        validate_bucket_name(name)?;
        let open = this
            .store
            .manifest
            .catalog
            .load_for_open(&this.store.manifest.buckets, name)?;
        // persisted fields always come from the bucket catalog; runtime fields are fixed at
        // context load
        let bucket_ctx = this
            .store
            .manifest
            .load_bucket_context_with_options(open.meta.id, Some(&opt))?;

        Ok(Bucket {
            tree: Tree::new(this.store.clone(), ROOT_PID, bucket_ctx),
            _lease: open.lease,
            inner: this.clone(),
        })
    }

    fn update_bucket_opt(this: &Arc<Inner>, name: &str, opt: BucketOptions) -> Result<(), OpCode> {
        validate_bucket_name(name)?;
        this.store.manifest.update_bucket_options(name, opt)
    }

    /// manually unload bucket to release memory
    fn drop_bucket(self: &Inner, name: &str) -> Result<(), OpCode> {
        validate_bucket_name(name)?;
        // the active WAL route is synchronized before the final checkpoint of the unload
        self.store.context.sync()?;
        self.store.manifest.unload_bucket(name)
    }

    fn del_bucket(self: &Inner, name: &str) -> Result<(), OpCode> {
        validate_bucket_name(name)?;
        self.store.manifest.delete_bucket(name)
    }
}

/// The main entry point for the Mace storage engine.
impl Mace {
    /// Creates a bucket with the given name.
    ///
    /// Returns [`OpCode::Invalid`] for an empty name and [`OpCode::TooLarge`] for a name that is
    /// not shorter than 32 bytes.
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
    /// Blocks while a catalog operation holds the structural lock, including bucket creation,
    /// option updates and the start of an unload/delete.
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
    ///
    /// The list comes from the bucket catalog, not from the set of loaded contexts: a bucket
    /// that is merely unloaded stays listed, while a logically deleted one does not.
    ///
    /// Blocks while a catalog operation holds the structural lock, including bucket creation,
    /// option updates and the start of an unload/delete.
    pub fn active_buckets(&self) -> Vec<String> {
        self.inner.store.manifest.catalog.active_names()
    }

    /// Manually unloads a bucket to release memory.
    ///
    /// The unloaded bucket stays an active bucket and can be opened again; its dirty state is
    /// final-checkpointed before the runtime is released.
    pub fn drop_bucket<S: AsRef<str>>(&self, name: S) -> Result<(), OpCode> {
        Inner::drop_bucket(&self.inner, name.as_ref())
    }

    /// Deletes a bucket and all its data.
    pub fn del_bucket<S: AsRef<str>>(&self, name: S) -> Result<(), OpCode> {
        Inner::del_bucket(&self.inner, name.as_ref())
    }

    /// Returns the total number of buckets, including active and pending deletion ones.
    ///
    /// Blocks while a catalog operation holds the structural lock, including bucket creation,
    /// option updates and the start of an unload/delete.
    pub fn nr_buckets(&self) -> u64 {
        self.inner.store.manifest.catalog.nr_buckets()
    }
}
