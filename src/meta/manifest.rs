use btree_store::{BTree, Error as BTreeError};
use dashmap::{DashMap, DashSet};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering::{AcqRel, Acquire, Relaxed},
        },
        mpsc::{Receiver, Sender},
    },
};

#[cfg(feature = "metrics")]
use crate::utils::observe::{CounterMetric, EventKind, GaugeMetric, ObserveEvent};
use crate::{
    Options,
    cc::context::Context,
    map::{
        SharedState,
        adapter::ManifestCheckpointObserver,
        buffer::{
            BucketContext, BucketLease, BucketManager, BucketRewritePermit, BucketRuntime,
            ReclaimMode,
        },
        flush::Checkpoint,
        table::PageMap,
    },
    meta::{
        BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_OBSOLETE_BLOB, BUCKET_OBSOLETE_DATA, Sequences,
        entry::IMetaCodec,
    },
    must_exist, must_ok, must_true,
    types::refbox::{BoxRef, BoxView},
    utils::{
        Handle, INIT_ADDR, MutRef, OpCode,
        data::{GroupPositions, LenSeq, Position, init_group_pos},
        interval::IntervalMap,
        options::{BucketOptions, ParsedOptions, PersistedBucketOptions, PersistedOptions},
    },
};

use super::{
    BUCKET_FRONTIER, BUCKET_METAS, BUCKET_MISC, BUCKET_PENDING_DEL, BUCKET_VERSION,
    BucketDurableFrontier, BucketMeta, CURRENT_VERSION, Delete, DeletionGuard, FileKind,
    IntervalPair, MAX_BUCKETS, MemStat, MetaKind, OPTIONS_KEY, PersistStat, VERSION_KEY,
    WalRecycleIntent, WalRecycleState, delete_done_meta_kind, delete_meta_kind,
    interval_bucket_name, orphan_blob_marker_key, orphan_data_marker_key,
    stat::{JunkApplier, JunkRegistry, PublishCut, StatCtx},
    stat_file_path, stat_intervals,
    txn::Txn,
    wal_recycle_key,
};

mod catalog;
mod files;
mod frontier;
mod recycle;

pub(crate) use catalog::BucketCatalog;
pub(crate) use files::RetiredStatFlushSnapshot;
use files::{FileState, RetiredStatKeys};

pub(crate) struct Manifest {
    pub(crate) sequences: Arc<Sequences>,
    files: [FileState; 2],
    pub(crate) buckets: Handle<BucketManager>,
    /// the single owner of bucket names, ids, persisted options, pending deletes and the count
    pub(crate) catalog: BucketCatalog,
    pub(crate) bucket_frontier: DashMap<u64, GroupPositions>,
    retired_stat_keys: RetiredStatKeys,
    pub(crate) opt: Arc<ParsedOptions>,
    pub(crate) btree: BTree,
    /// suspends runtime file deletions while a checkpoint copies them
    pub(crate) deletions: Arc<DeletionGuard>,
    /// runtime-only junk coordinator, one per file kind; never persisted
    junk: JunkRegistry,
}

impl Drop for Manifest {
    fn drop(&mut self) {
        self.buckets.abort();
        self.buckets.reclaim();
    }
}

impl Manifest {
    pub(crate) fn new(
        opt: Arc<ParsedOptions>,
        tx: Sender<SharedState>,
        rx: Receiver<()>,
    ) -> Result<Self, OpCode> {
        let path = opt.manifest();
        // an open failure is recoverable for the caller: another instance holding the
        // lock maps to `OpCode::Again` and a file this build cannot read to
        // `OpCode::Corruption`, so opening must not abort the process
        let btree = BTree::open(path).map_err(OpCode::from)?;
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
            Self::ensure_btree_bucket(&btree, name);
        }

        Ok(Self {
            sequences: Arc::new(Sequences::default()),
            files: [
                FileState::new(opt.clone(), FileKind::Data),
                FileState::new(opt.clone(), FileKind::Blob),
            ],
            buckets: Handle::new(BucketManager::new(
                opt.clone(),
                Handle::from(std::ptr::null_mut()),
                tx,
                rx,
            )),
            catalog: BucketCatalog::new(),
            bucket_frontier: DashMap::new(),
            retired_stat_keys: RetiredStatKeys::default(),
            opt,
            btree,
            deletions: DeletionGuard::new(),
            junk: JunkRegistry::new(),
        })
    }

    pub(crate) fn load_persisted_options_if_present(
        &self,
    ) -> Result<Option<PersistedOptions>, OpCode> {
        match self.btree.view(BUCKET_MISC, |txn| txn.get(OPTIONS_KEY)) {
            Ok(raw) => PersistedOptions::from_json(&raw).map(Some),
            Err(BTreeError::KeyNotFound) => Ok(None),
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
        observer: Arc<ManifestCheckpointObserver>,
    ) -> Result<(), OpCode> {
        // non-owning handle: the manifest is boxed once by its owning handle and never moved,
        // so this pointer is that handle's box address; set_context must run on the boxed manifest
        let reader = Handle::from(self as *const Self as *mut Self);
        unsafe {
            let mgr = &mut *self.buckets.inner();
            mgr.start(ctx, reader, observer)
        }
    }

    /// restart the recovery checkpoint flusher on the recovered Context.
    ///
    /// a same-width rebuild stops the flusher before it detaches the recovery
    /// writers, so this entry must find no running flusher: replacing a live one
    /// would leave its thread and its tasks behind (`Checkpoint` has no `Drop`).
    /// `BucketManager::start` enforces that rule at the site that replaces the flusher
    /// and returns the violation as an error
    pub(crate) fn restart_checkpoint_flusher(
        &mut self,
        ctx: Handle<Context>,
        observer: Arc<ManifestCheckpointObserver>,
    ) -> Result<(), OpCode> {
        self.set_context(ctx, observer)
    }

    pub(crate) fn abort(&mut self) {
        self.buckets.abort();
    }

    /// detach the bucket checkpoint flusher.
    ///
    /// `BucketManager::start` refuses to publish a second flusher and `Checkpoint` has no `Drop`,
    /// so a Context rebind must stop the running flusher explicitly instead of dropping it
    pub(crate) fn take_checkpoint_flusher(&mut self) -> Option<Checkpoint> {
        self.buckets.flush.take()
    }

    pub(crate) fn junk(&self) -> &JunkRegistry {
        &self.junk
    }

    pub(crate) fn begin(&self) -> Txn<'_> {
        Txn::new(self)
    }

    pub(crate) fn file_state(&self, kind: FileKind) -> &FileState {
        &self.files[kind.slot()]
    }
}
