use btree_store::Error as BTreeError;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use crate::map::SharedState;
use crate::meta::entry::IMetaCodec;
use crate::meta::{
    BUCKET_FRONTIER, BUCKET_VERSION, BucketCatalog, BucketDurableFrontier, CURRENT_VERSION,
    FileKind, MemStat, ORPHAN_BLOB_MARKER_PREFIX, ORPHAN_DATA_MARKER_PREFIX, PersistStat,
    SEQUENCES_KEY, VERSION_KEY,
};
use crate::must_ok;
use crate::utils::diag;
use crate::{
    OpCode,
    meta::{
        BUCKET_MISC, BUCKET_OBSOLETE_BLOB, BUCKET_OBSOLETE_DATA, Manifest, entry::Sequences,
        stat_bucket, stat_file_path,
    },
    utils::options::{Options, ParsedOptions, PersistedOptions},
};
use std::sync::mpsc::{Receiver, Sender};

/// the single source/target decision point for an open.
///
/// `source_*` describes the configuration the on-disk state was written with; `target_*`
/// describes the configuration this open was asked for. Everything downstream (recovery
/// scan width, recycle/wipe stream bound, target boot construction, runtime width) reads
/// its width from here instead of re-deriving it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OpenPlan {
    pub source_groups: u8,
    pub target_groups: u8,
    pub group_resize: bool,
    pub route_switch: bool,
    pub layout_migration: bool,
}

impl OpenPlan {
    /// derive the open plan from the persisted configuration (absent on a pristine database).
    ///
    /// `durable_legacy_per_group_wal` is "a durable open already sees legacy per-group WAL
    /// files in the log directory"; a relaxed target never migrates that layout.
    pub(crate) fn derive(
        persisted: Option<(u8, bool)>,
        requested: &Options,
        durable_legacy_per_group_wal: bool,
    ) -> Self {
        let (source_groups, source_route) =
            persisted.unwrap_or((requested.concurrent_write, requested.sync_on_write));
        Self {
            source_groups,
            target_groups: requested.concurrent_write,
            group_resize: source_groups != requested.concurrent_write,
            route_switch: source_route != requested.sync_on_write,
            layout_migration: durable_legacy_per_group_wal,
        }
    }

    /// physical WAL stream bound for the era wipe. recycle completion and the epoch high-water
    /// cannot use it: a durable recycle slot outlives the width that staged its intent, so they
    /// cover the whole durable slot space instead.
    ///
    /// target-only streams (id >= `source_groups`) may hold era files left by an interrupted
    /// attempt, so the wipe must reach them as well.
    pub(crate) fn stream_bound(&self) -> u8 {
        self.source_groups.max(self.target_groups)
    }

    /// whether recovery must rebuild the WAL epoch
    pub(crate) fn rebuild_epoch(&self) -> bool {
        self.group_resize || self.route_switch || self.layout_migration
    }
}

/// what `ManifestBuilder::load` hands back to the open path
pub(crate) struct LoadOutcome {
    pub plan: OpenPlan,
    /// `Some(current)` when the persisted subset must be rewritten after the open completes
    pub writeback: Option<PersistedOptions>,
}

pub(crate) struct ManifestBuilder {
    inner: Manifest,
    max_file_id: u64,
    // a derived counter was raised while loading, so finish must persist the repair
    repair_sequences: bool,
    // accumulated sizes for DataStat to avoid re-iteration in finish(), by FileKind::slot
    active_sizes: [u64; 2],
    total_sizes: [u64; 2],
}

impl ManifestBuilder {
    /// opening the metadata store can fail recoverably (another instance holds the
    /// lock, or this build cannot read the file's format), so the builder is fallible
    /// too and the caller sees a typed `OpCode` instead of a failed invariant
    pub(crate) fn new_with_channels(
        opt: Arc<ParsedOptions>,
        tx: Sender<SharedState>,
        rx: Receiver<()>,
    ) -> Result<Self, OpCode> {
        Ok(Self {
            inner: Manifest::new(opt, tx, rx)?,
            max_file_id: 0,
            repair_sequences: false,
            active_sizes: [0; 2],
            total_sizes: [0; 2],
        })
    }

    pub(crate) fn load(&mut self) -> Result<LoadOutcome, OpCode> {
        self.repair_sequences = false;
        let current = PersistedOptions::from_options(self.inner.opt.as_ref());
        let (has_version, persisted, persisted_source) = self.load_config(&current)?;
        let needs_writeback = persisted != current;
        // the open plan is the single place that decides source/target widths and routes
        let durable_legacy_per_group_wal =
            self.inner.opt.sync_on_write && self.inner.opt.has_legacy_per_group_wal()?;
        let plan = OpenPlan::derive(
            persisted_source,
            self.inner.opt.as_ref(),
            durable_legacy_per_group_wal,
        );

        self.load_sequences(has_version)?;
        self.load_catalog()?;
        self.load_bucket_frontier()?;
        let (active_sizes, total_sizes) = self.load_file_stats()?;
        self.active_sizes = active_sizes;
        self.total_sizes = total_sizes;

        if !has_version {
            self.backfill_legacy_version()?;
        }

        Ok(LoadOutcome {
            plan,
            writeback: needs_writeback.then_some(current),
        })
    }

    /// loads the format version and the persisted option subset; a pristine database
    /// bootstraps both in one transaction, while a legacy database is upgraded later
    fn load_config(
        &self,
        current: &PersistedOptions,
    ) -> Result<(bool, PersistedOptions, Option<(u8, bool)>), OpCode> {
        let mut has_version = false;
        match self
            .inner
            .btree
            .view(BUCKET_VERSION, |txn| txn.get(VERSION_KEY))
        {
            Ok(val) => {
                let ver = u64::from_le_bytes(val[..8].try_into().map_err(|_| OpCode::Corruption)?);
                if ver != CURRENT_VERSION {
                    return Err(OpCode::BadVersion);
                }
                has_version = true;
            }
            Err(BTreeError::KeyNotFound) => {}
            Err(err) => return Err(OpCode::from(err)),
        }

        let mut persisted_source: Option<(u8, bool)> = None;
        let persisted = match self.inner.load_persisted_options_if_present()? {
            Some(persisted) => {
                persisted_source = Some((persisted.concurrent_write, persisted.sync_on_write));
                persisted
            }
            None if !has_version && self.inner.is_pristine_uninitialized()? => {
                self.inner.bootstrap_persisted_options(current)?;
                has_version = true;
                current.clone()
            }
            None => return Err(OpCode::Corruption),
        };
        Ok((has_version, persisted, persisted_source))
    }

    fn load_sequences(&mut self, has_version: bool) -> Result<(), OpCode> {
        match self
            .inner
            .btree
            .view(BUCKET_MISC, |txn| txn.get(SEQUENCES_KEY))
        {
            Ok(val) => {
                if !has_version {
                    return Err(OpCode::BadVersion);
                }
                let src = Sequences::decode(&val);
                macro_rules! set {
                    ($dst:expr, $src:expr; $($field:ident),*) => {
                        $(
                            $dst.$field.fetch_max($src.$field.load(Relaxed), Relaxed);
                        )*
                    };
                }
                set!(
                    self.inner.sequences,
                    src;
                    next_file_id,
                    next_bucket_id,
                    oracle
                );
                self.max_file_id = src.next_file_id.load(Relaxed).saturating_sub(1);
            }
            Err(BTreeError::KeyNotFound) => {}
            Err(err) => return Err(OpCode::from(err)),
        }
        Ok(())
    }

    fn load_catalog(&mut self) -> Result<(), OpCode> {
        let (catalog, max_bucket_id) = BucketCatalog::load(&self.inner.btree)?;
        if let Some(max_id) = max_bucket_id {
            let next = self.inner.sequences.next_bucket_id.load(Relaxed);
            if next <= max_id {
                // the counter is derived state: raise it instead of refusing the open, so a
                // stale value can never hand the same id to a second bucket
                let raised = max_id
                    .checked_add(1)
                    .filter(|raised| *raised < u64::MAX)
                    .ok_or(OpCode::Corruption)?;
                self.inner.sequences.next_bucket_id.store(raised, Relaxed);
                self.repair_sequences = true;
                diag::warn!("raised stale next_bucket_id {} to {}", next, raised);
            }
        }
        // the runtime entry exists for every catalogued bucket from open on: lifecycle state and
        // compression contexts outlive a lazy context load, and background GC reads them
        for bucket_id in catalog.ids() {
            self.inner.buckets.ensure_runtime(bucket_id);
        }
        self.inner.catalog = catalog;
        Ok(())
    }

    fn load_bucket_frontier(&self) -> Result<(), OpCode> {
        let mut frontier_error = None;
        self.inner
            .btree
            .view(BUCKET_FRONTIER, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let key_id = match k.as_slice().try_into() {
                        Ok(bytes) => u64::from_le_bytes(bytes),
                        Err(_) => {
                            frontier_error = Some(OpCode::Corruption);
                            return Ok(());
                        }
                    };
                    let item = match BucketDurableFrontier::try_decode(&v) {
                        Ok(item) => item,
                        Err(err) => {
                            frontier_error = Some(err);
                            return Ok(());
                        }
                    };
                    if item.bucket_id != key_id {
                        frontier_error = Some(OpCode::Corruption);
                        return Ok(());
                    }
                    self.inner.bucket_frontier.insert(item.bucket_id, item.lsn);
                }
                Ok(())
            })
            .map_err(OpCode::from)?;
        if let Some(err) = frontier_error {
            return Err(err);
        }
        Ok(())
    }

    fn load_file_stats(&mut self) -> Result<([u64; 2], [u64; 2]), OpCode> {
        let mut active_sizes = [0u64; 2];
        let mut total_sizes = [0u64; 2];
        // every id still referenced by a stat or an obsolete record must never be handed out
        // again, even if the durable counter predates a concurrent checkpoint
        let mut max_file_id = self.max_file_id;
        for kind in FileKind::ALL {
            let mut active_size = 0;
            let mut total_size = 0;
            self.inner
                .btree
                .view(stat_bucket(kind), |txn| {
                    let mut iter = txn.iter();
                    let mut k = Vec::new();
                    let mut v = Vec::new();
                    while iter.next_ref(&mut k, &mut v) {
                        let inner = PersistStat::decode_inner_only(&v);
                        active_size += inner.active_size as u64;
                        total_size += inner.total_size as u64;
                        max_file_id = max_file_id.max(inner.file_id);
                        let fstat = MemStat::from_parts(inner, None);
                        self.inner.file_state(kind).stat.insert_loaded_stat(fstat);
                        self.inner
                            .buckets
                            .get_runtime(inner.bucket_id)
                            .observe_stat_epoch(kind, inner.up1, inner.up2);
                    }
                    Ok(())
                })
                .map_err(|_| OpCode::IoError)?;
            active_sizes[kind.slot()] = active_size;
            total_sizes[kind.slot()] = total_size;

            self.inner
                .btree
                .view(
                    match kind {
                        FileKind::Data => BUCKET_OBSOLETE_DATA,
                        FileKind::Blob => BUCKET_OBSOLETE_BLOB,
                    },
                    |txn| {
                        let mut iter = txn.iter();
                        let mut k = Vec::new();
                        let mut v = Vec::new();
                        let mut obs = self.inner.file_state(kind).obsolete.lock();
                        while iter.next_ref(&mut k, &mut v) {
                            let id_bytes: [u8; 8] =
                                k[..8].try_into().expect("obsolete file key must be a u64");
                            let id = u64::from_le_bytes(id_bytes);
                            max_file_id = max_file_id.max(id);
                            obs.push(id);
                        }
                        Ok(())
                    },
                )
                .map_err(|_| OpCode::IoError)?;
        }
        if max_file_id > self.max_file_id {
            // a durable file id, or an obsolete one, is above the persisted counter
            self.repair_sequences = true;
        }
        self.max_file_id = max_file_id;
        Ok((active_sizes, total_sizes))
    }

    fn backfill_legacy_version(&self) -> Result<(), OpCode> {
        self.inner
            .btree
            .exec(BUCKET_VERSION, |txn| {
                txn.put(VERSION_KEY, CURRENT_VERSION.to_le_bytes())
            })
            .map_err(|_| OpCode::IoError)
    }

    pub(crate) fn finish(mut self) -> Result<Manifest, OpCode> {
        // both cleanups sync the data directory, and this runs before recovery: a failed sync
        // must fail the open with an error, which the caller reports as a failed open
        self.clean_orphans()?;
        self.inner.delete_files()?;

        #[cfg(feature = "extra_check")]
        for kind in FileKind::ALL {
            self.inner.file_state(kind).stat.update_size(
                self.active_sizes[kind.slot()],
                self.total_sizes[kind.slot()],
            );
        }

        let next_file_id = self
            .max_file_id
            .checked_add(1)
            .filter(|next| *next < u64::MAX)
            .ok_or(OpCode::Corruption)?;
        self.inner
            .sequences
            .next_file_id
            .store(next_file_id, Relaxed);
        if self.repair_sequences {
            // open is single-threaded here, so a direct put is the commit-time value; a failed
            // repair write must surface as a failed open instead of aborting the process
            let value = self.inner.sequences.snapshot_bytes();
            self.inner
                .btree
                .exec(BUCKET_MISC, |txn| txn.put(SEQUENCES_KEY, value))
                .map_err(OpCode::from)?;
        }

        Ok(self.inner)
    }

    fn clean_orphans(&mut self) -> Result<(), OpCode> {
        let mut markers: [Vec<(u64, Vec<u8>)>; 2] = [Vec::new(), Vec::new()];

        self.inner
            .btree
            .view(BUCKET_MISC, |txn| {
                // file ids can be sparse because some ids are reserved before any file is flushed
                // rely on explicit per-file markers instead of max_id tail probing or directory scan
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    for kind in FileKind::ALL {
                        let prefix = match kind {
                            FileKind::Data => ORPHAN_DATA_MARKER_PREFIX,
                            FileKind::Blob => ORPHAN_BLOB_MARKER_PREFIX,
                        };
                        if let Some(id) = Self::parse_orphan_marker_id(&k, prefix) {
                            markers[kind.slot()].push((id, k.clone()));
                            break;
                        }
                    }
                }
                Ok(())
            })
            .map_err(OpCode::from)?;

        let mut cleaned_marker_keys: [Vec<Vec<u8>>; 2] = [Vec::new(), Vec::new()];
        for kind in FileKind::ALL {
            for (id, key) in markers[kind.slot()].drain(..) {
                self.observe_orphan_id(id);
                let path = stat_file_path(self.inner.opt.as_ref(), kind, id);
                // this unlink runs only from `Store::new` while opening the instance, so
                // it can never overlap a checkpoint (which requires an already-open
                // instance) and does not claim a `DeletionGuard` slot. moving orphan
                // cleanup to a runtime path requires claiming a slot via
                // `DeletionGuard::begin` first
                must_ok!(
                    self.inner.opt.fs.remove_file_if_exists(&path),
                    "path {:?}",
                    path
                );
                cleaned_marker_keys[kind.slot()].push(key);
            }
        }

        if FileKind::ALL
            .iter()
            .all(|&kind| cleaned_marker_keys[kind.slot()].is_empty())
        {
            return Ok(());
        }

        self.inner.opt.try_sync_data_dir()?;
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(
            "mace_recovery_orphan_cleanup_after_data_dir_sync_before_marker_clear",
        );

        self.inner
            .btree
            .exec(BUCKET_MISC, |txn| {
                for kind in FileKind::ALL {
                    for key in &cleaned_marker_keys[kind.slot()] {
                        // a marker already cleared by an earlier attempt deletes
                        // nothing; every other failure must still surface
                        let _ = txn.del(key)?;
                    }
                }
                Ok(())
            })
            .map_err(OpCode::from)?;
        Ok(())
    }

    fn observe_orphan_id(&mut self, id: u64) {
        if id > self.max_file_id {
            // an unpublished file id above the persisted counter must be skipped on reuse
            self.repair_sequences = true;
        }
        self.max_file_id = self.max_file_id.max(id);
    }

    fn parse_orphan_marker_id(raw: &[u8], prefix: &str) -> Option<u64> {
        let prefix = prefix.as_bytes();
        if !raw.starts_with(prefix) {
            return None;
        }
        let raw_id = std::str::from_utf8(&raw[prefix.len()..]).ok()?;
        raw_id.parse::<u64>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::{ManifestBuilder, OpenPlan};
    use crate::meta::entry::{
        BucketDurableFrontier, BucketMeta, IMetaCodec, PersistStat, Sequences, StatInner,
    };
    use crate::meta::{
        BUCKET_DATA_STAT, BUCKET_METAS, BUCKET_MISC, BUCKET_PENDING_DEL, SEQUENCES_KEY,
    };
    use crate::utils::options::PersistedBucketOptions;
    use crate::{BucketOptions, Options, RandomPath};
    use std::sync::Arc;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::mpsc::channel;

    fn open_builder() -> (
        ManifestBuilder,
        Arc<crate::utils::options::ParsedOptions>,
        RandomPath,
    ) {
        let root = RandomPath::tmp();
        let opt = Arc::new(
            Options::new(&*root)
                .validate()
                .expect("options must validate"),
        );
        let (tx, _rx) = channel();
        let (_ack_tx, ack_rx) = channel();
        let builder =
            ManifestBuilder::new_with_channels(opt.clone(), tx, ack_rx).expect("builder must open");
        (builder, opt, root)
    }

    fn put_bucket_meta(builder: &ManifestBuilder, name: &str, id: u64) {
        let meta = BucketMeta {
            id,
            options: PersistedBucketOptions::from(&BucketOptions::default()),
        };
        let mut buf = vec![0u8; meta.packed_size()];
        meta.encode(&mut buf);
        builder
            .inner
            .btree
            .exec(BUCKET_METAS, |txn| txn.put(name.as_bytes(), &buf))
            .expect("bucket meta must be staged");
    }

    fn requested(groups: u8, durable: bool) -> Options {
        let mut opt = Options::new("/tmp/mace-open-plan-test");
        opt.concurrent_write = groups;
        opt.sync_on_write = durable;
        opt
    }

    /// the builder's orphan cleanup syncs the data directory before clearing its markers; an open
    /// that fails there must fail with an error instead of killing the process
    #[test]
    fn finish_propagates_a_failed_data_directory_sync() {
        use std::io::ErrorKind;
        use std::sync::Arc;
        use std::sync::mpsc::channel;

        use crate::RandomPath;
        use crate::io::testfs::{InjectOp, InjectedFileSystem};
        use crate::meta::{BUCKET_MISC, orphan_data_marker_key};

        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        let fs = Arc::new(InjectedFileSystem::new());
        opt.fs = fs.clone();
        let parsed = Arc::new(opt.validate().expect("options must validate"));
        let (tx, _rx) = channel();
        let (_ack_tx, ack_rx) = channel();
        let mut builder = super::ManifestBuilder::new_with_channels(parsed.clone(), tx, ack_rx)
            .expect("builder must open");
        builder.load().expect("manifest load must work");
        // an orphan marker is what makes the open path sync the data directory
        builder
            .inner
            .btree
            .exec(BUCKET_MISC, |txn| txn.put(orphan_data_marker_key(7), []))
            .expect("marker must be staged");
        // arm the rule after the marker, so only the cleanup's sync can consume it
        fs.fail_once(
            InjectOp::SyncDir,
            parsed.data_root(),
            ErrorKind::PermissionDenied,
        );

        assert_eq!(
            builder.finish().err(),
            Some(crate::OpCode::IoError),
            "a failed data-directory sync during orphan cleanup must fail the open"
        );
    }

    #[test]
    fn pristine_bootstrap_reopens_with_version_and_options() {
        let (mut builder, opt, _root) = open_builder();
        builder.load().expect("pristine bootstrap must load");
        let version = builder
            .inner
            .btree
            .view(crate::meta::BUCKET_VERSION, |txn| {
                txn.get(crate::meta::VERSION_KEY)
            })
            .expect("version must be written");
        assert_eq!(
            u64::from_le_bytes(version[..8].try_into().expect("version width")),
            crate::meta::CURRENT_VERSION
        );
        assert!(
            builder
                .inner
                .btree
                .view(crate::meta::BUCKET_MISC, |txn| {
                    txn.get(crate::meta::OPTIONS_KEY)
                })
                .is_ok(),
            "options must be written"
        );
        builder.finish().expect("manifest must finish");

        let (tx, _rx) = channel();
        let (_ack_tx, ack_rx) = channel();
        let mut reopened =
            ManifestBuilder::new_with_channels(opt, tx, ack_rx).expect("reopen must open");
        reopened.load().expect("reopen must load");
    }

    #[test]
    fn legacy_database_without_version_backfills_after_validation() {
        let (mut builder, opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        builder
            .inner
            .btree
            .exec(crate::meta::BUCKET_VERSION, |txn| {
                txn.del(crate::meta::VERSION_KEY)
            })
            .expect("version must be deletable");
        builder.finish().expect("manifest must finish");

        let (tx, _rx) = channel();
        let (_ack_tx, ack_rx) = channel();
        let mut reopened =
            ManifestBuilder::new_with_channels(opt, tx, ack_rx).expect("legacy open must open");
        reopened.load().expect("legacy load must backfill");
        assert!(
            reopened
                .inner
                .btree
                .view(crate::meta::BUCKET_VERSION, |txn| {
                    txn.get(crate::meta::VERSION_KEY)
                })
                .is_ok(),
            "legacy load must write the version back"
        );
    }

    #[test]
    fn legacy_backfill_stays_after_validation_failure() {
        let (mut builder, opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        builder
            .inner
            .btree
            .exec(crate::meta::BUCKET_VERSION, |txn| {
                txn.del(crate::meta::VERSION_KEY)
            })
            .expect("version must be deletable");
        let mut lsn = crate::utils::data::init_group_pos();
        lsn[0] = crate::utils::data::Position::new(1, 0);
        let frontier = BucketDurableFrontier::new(2, lsn);
        let mut buf = vec![0u8; frontier.packed_size()];
        frontier.encode(&mut buf);
        builder
            .inner
            .btree
            .exec(crate::meta::BUCKET_FRONTIER, |txn| {
                txn.put(1u64.to_le_bytes(), &buf)
            })
            .expect("frontier must be staged");
        builder.finish().expect("manifest must finish");

        let (tx, _rx) = channel();
        let (_ack_tx, ack_rx) = channel();
        let mut reopened =
            ManifestBuilder::new_with_channels(opt, tx, ack_rx).expect("legacy open must open");
        assert_eq!(
            reopened.load().err(),
            Some(crate::OpCode::Corruption),
            "validation must reject the mismatched frontier"
        );
        assert!(
            reopened
                .inner
                .btree
                .view(crate::meta::BUCKET_VERSION, |txn| {
                    txn.get(crate::meta::VERSION_KEY)
                })
                .is_err(),
            "a failed legacy load must not backfill the version"
        );
    }

    #[test]
    fn bucket_metadata_read_error_is_not_absent() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        let manifest = builder.finish().expect("manifest must finish");
        manifest
            .btree
            .del_bucket(BUCKET_METAS)
            .expect("bucket meta table must be deletable");
        assert!(
            manifest.load_bucket_options("missing").is_err(),
            "metadata read failure must not become None"
        );
    }

    #[test]
    fn bucket_builder_pending_scan_error_propagates() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        builder
            .inner
            .btree
            .del_bucket(BUCKET_PENDING_DEL)
            .expect("pending bucket table must be deletable");
        assert!(
            builder.load().is_err(),
            "pending scan read error must fail the open"
        );
    }

    #[test]
    fn bucket_orphan_marker_scan_error_propagates() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        builder
            .inner
            .btree
            .del_bucket(BUCKET_MISC)
            .expect("misc bucket must be deletable");
        assert!(
            builder.finish().is_err(),
            "orphan marker scan read error must fail the open"
        );
    }

    #[test]
    fn bucket_duplicate_id_rejected() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        put_bucket_meta(&builder, "a", 7);
        put_bucket_meta(&builder, "b", 7);
        assert_eq!(builder.load().err(), Some(crate::OpCode::Corruption));
    }

    #[test]
    fn stale_next_bucket_id_is_raised_instead_of_rejecting_the_open() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        put_bucket_meta(&builder, "a", 5);
        builder.inner.sequences.next_bucket_id.store(0, Relaxed);

        builder.load().expect("a stale counter must be repaired");
        assert_eq!(builder.inner.sequences.next_bucket_id.load(Relaxed), 6);

        let manifest = builder.finish().expect("repair must finish");
        let raw = manifest
            .btree
            .view(BUCKET_MISC, |txn| txn.get(SEQUENCES_KEY))
            .expect("the repair must be durable");
        let seq = Sequences::decode(&raw);
        assert_eq!(seq.next_bucket_id.load(Relaxed), 6);
    }

    #[test]
    fn pending_bucket_id_participates_in_the_next_id_bound() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        // active max is 5, but the highest id was logically deleted and awaits cleanup
        put_bucket_meta(&builder, "a", 5);
        builder
            .inner
            .btree
            .exec(BUCKET_PENDING_DEL, |txn| txn.put(9u64.to_le_bytes(), []))
            .expect("pending record must be staged");
        builder.inner.sequences.next_bucket_id.store(6, Relaxed);

        builder.load().expect("pending ids must raise the bound");
        assert_eq!(
            builder.inner.sequences.next_bucket_id.load(Relaxed),
            10,
            "a pending id at 9 must keep the next id above it"
        );
    }

    #[test]
    fn file_id_counter_covers_ids_still_referenced_by_stats() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        put_bucket_meta(&builder, "a", 5);
        let inner = StatInner {
            file_id: 100,
            up1: 0,
            up2: 0,
            active_elems: 0,
            total_elems: 0,
            active_size: 0,
            total_size: 0,
            bucket_id: 5,
        };
        let stat = PersistStat::from_parts(inner, Vec::new());
        let mut buf = vec![0u8; stat.packed_size()];
        stat.encode(&mut buf);
        builder
            .inner
            .btree
            .exec(BUCKET_DATA_STAT, |txn| txn.put(100u64.to_le_bytes(), &buf))
            .expect("stat must be staged");
        builder.inner.sequences.next_file_id.store(10, Relaxed);

        builder.load().expect("stats must raise the file id bound");
        let manifest = builder.finish().expect("repair must finish");
        let raw = manifest
            .btree
            .view(BUCKET_MISC, |txn| txn.get(SEQUENCES_KEY))
            .expect("the repair must be durable");
        let seq = Sequences::decode(&raw);
        assert_eq!(
            seq.next_file_id.load(Relaxed),
            101,
            "a stat for file 100 must keep the next id above it"
        );
    }

    #[test]
    fn sequences_commit_uses_the_value_at_commit_not_at_record() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        let manifest = builder.finish().expect("manifest must finish");

        let mut first = manifest.begin();
        first.put_sequences();
        // a concurrent writer advances the counters after `first` recorded but before it commits
        manifest.sequences.next_file_id.store(11, Relaxed);
        manifest.sequences.next_bucket_id.store(22, Relaxed);
        manifest.sequences.oracle.store(33, Relaxed);
        let mut second = manifest.begin();
        second.put_sequences();
        second.commit();

        first.commit();
        let raw = manifest
            .btree
            .view(BUCKET_MISC, |txn| txn.get(SEQUENCES_KEY))
            .expect("sequences must be durable");
        let seq = Sequences::decode(&raw);
        assert_eq!(seq.next_file_id.load(Relaxed), 11);
        assert_eq!(seq.next_bucket_id.load(Relaxed), 22);
        assert_eq!(seq.oracle.load(Relaxed), 33);
    }

    #[test]
    fn max_file_id_overflow_rejects_the_open() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        put_bucket_meta(&builder, "a", 5);
        let inner = StatInner {
            file_id: u64::MAX,
            up1: 0,
            up2: 0,
            active_elems: 0,
            total_elems: 0,
            active_size: 0,
            total_size: 0,
            bucket_id: 5,
        };
        let stat = PersistStat::from_parts(inner, Vec::new());
        let mut buf = vec![0u8; stat.packed_size()];
        stat.encode(&mut buf);
        builder
            .inner
            .btree
            .exec(BUCKET_DATA_STAT, |txn| {
                txn.put(u64::MAX.to_le_bytes(), &buf)
            })
            .expect("stat must be staged");
        builder.load().expect("the stat must load");
        assert_eq!(
            builder.finish().err(),
            Some(crate::OpCode::Corruption),
            "a file id at u64::MAX cannot be raised to a next id"
        );
    }

    #[test]
    fn max_pending_bucket_id_overflow_rejects_the_open() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        builder
            .inner
            .btree
            .exec(BUCKET_PENDING_DEL, |txn| {
                txn.put(u64::MAX.to_le_bytes(), [])
            })
            .expect("pending record must be staged");
        assert_eq!(
            builder.load().err(),
            Some(crate::OpCode::Corruption),
            "a pending bucket id at u64::MAX cannot be raised to a next id"
        );
    }

    #[test]
    fn bucket_id_at_the_counter_boundary_rejects_the_open() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        builder
            .inner
            .btree
            .exec(BUCKET_PENDING_DEL, |txn| {
                txn.put((u64::MAX - 1).to_le_bytes(), [])
            })
            .expect("pending record must be staged");
        assert_eq!(
            builder.load().err(),
            Some(crate::OpCode::Corruption),
            "a bound at u64::MAX - 1 leaves no room before the counter wraps"
        );
    }

    #[test]
    fn file_id_at_the_counter_boundary_rejects_the_open() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        put_bucket_meta(&builder, "a", 5);
        let inner = StatInner {
            file_id: u64::MAX - 1,
            up1: 0,
            up2: 0,
            active_elems: 0,
            total_elems: 0,
            active_size: 0,
            total_size: 0,
            bucket_id: 5,
        };
        let stat = PersistStat::from_parts(inner, Vec::new());
        let mut buf = vec![0u8; stat.packed_size()];
        stat.encode(&mut buf);
        builder
            .inner
            .btree
            .exec(BUCKET_DATA_STAT, |txn| {
                txn.put((u64::MAX - 1).to_le_bytes(), &buf)
            })
            .expect("stat must be staged");
        builder.load().expect("the stat must load");
        assert_eq!(
            builder.finish().err(),
            Some(crate::OpCode::Corruption),
            "a next id at u64::MAX would wrap on the following allocation"
        );
    }

    #[test]
    fn bucket_frontier_identity_mismatch_rejected() {
        let (mut builder, _opt, _root) = open_builder();
        builder.load().expect("initial load must work");
        let mut lsn = crate::utils::data::init_group_pos();
        lsn[0] = crate::utils::data::Position::new(1, 0);
        let frontier = BucketDurableFrontier::new(2, lsn);
        let mut buf = vec![0u8; frontier.packed_size()];
        frontier.encode(&mut buf);
        builder
            .inner
            .btree
            .exec(crate::meta::BUCKET_FRONTIER, |txn| {
                txn.put(1u64.to_le_bytes(), &buf)
            })
            .expect("frontier must be staged");
        assert_eq!(builder.load().err(), Some(crate::OpCode::Corruption));
    }

    #[test]
    fn open_plan_detects_group_resize_and_rebuilds_the_epoch() {
        let opt = requested(2, true);
        let plan = OpenPlan::derive(Some((4, true)), &opt, false);
        assert_eq!(plan.source_groups, 4);
        assert_eq!(plan.target_groups, 2);
        assert!(plan.group_resize);
        assert!(!plan.route_switch);
        assert!(!plan.layout_migration);
        assert!(plan.rebuild_epoch());
    }

    #[test]
    fn open_plan_keeps_the_scan_at_source_width_and_the_wipe_at_stream_bound() {
        // grow: the retained scan stays on the persisted width, the physical bound follows it
        let plan = OpenPlan::derive(Some((2, true)), &requested(4, true), false);
        assert_eq!(plan.source_groups, 2);
        assert_eq!(plan.target_groups, 4);
        assert_eq!(plan.stream_bound(), 4);
        assert!(plan.group_resize);
        assert!(plan.rebuild_epoch());
    }

    #[test]
    fn open_plan_pristine_database_has_no_migration() {
        let plan = OpenPlan::derive(None, &requested(4, true), false);
        assert_eq!(plan.source_groups, 4);
        assert_eq!(plan.target_groups, 4);
        assert_eq!(plan.stream_bound(), 4);
        assert!(!plan.rebuild_epoch());
    }

    #[test]
    fn open_plan_route_switch_rebuilds_without_resizing() {
        let plan = OpenPlan::derive(Some((4, false)), &requested(4, true), false);
        assert!(plan.route_switch);
        assert!(!plan.group_resize);
        assert!(plan.rebuild_epoch());
    }

    #[test]
    fn open_plan_migrates_legacy_layout_only_for_a_durable_open() {
        let durable = OpenPlan::derive(Some((2, true)), &requested(2, true), true);
        assert!(durable.layout_migration);
        assert!(durable.rebuild_epoch());

        let relaxed = OpenPlan::derive(Some((2, false)), &requested(2, false), false);
        assert!(!relaxed.layout_migration);
        assert!(!relaxed.rebuild_epoch());
    }
}
