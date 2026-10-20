use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use crate::map::SharedState;
use crate::meta::entry::IMetaCodec;
use crate::meta::{
    BUCKET_FRONTIER, BUCKET_VERSION, BucketDurableFrontier, CURRENT_VERSION, FileKind, MemStat,
    ORPHAN_BLOB_MARKER_PREFIX, ORPHAN_DATA_MARKER_PREFIX, PersistStat, SEQUENCES_KEY, VERSION_KEY,
    entry::BucketMeta,
};
use crate::must_ok;
use crate::{
    OpCode,
    meta::{
        BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_METAS, BUCKET_MISC, BUCKET_OBSOLETE_BLOB,
        BUCKET_OBSOLETE_DATA, BUCKET_PENDING_DEL, Manifest, entry::Sequences,
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
    // accumulated sizes for DataStat to avoid re-iteration in finish()
    data_active_size: u64,
    data_total_size: u64,
    blob_active_size: u64,
    blob_total_size: u64,
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
            data_active_size: 0,
            data_total_size: 0,
            blob_active_size: 0,
            blob_total_size: 0,
        })
    }

    pub(crate) fn load(&mut self) -> Result<LoadOutcome, OpCode> {
        let sequences_ref = &self.inner.sequences;
        let current = PersistedOptions::from_options(self.inner.opt.as_ref());

        // 0. check version
        let mut has_version = false;
        if let Ok(val) = self
            .inner
            .btree
            .view(BUCKET_VERSION, |txn| txn.get(VERSION_KEY))
        {
            let ver = u64::from_le_bytes(val[..8].try_into().map_err(|_| OpCode::Corruption)?);
            if ver != CURRENT_VERSION {
                return Err(OpCode::BadVersion);
            }
            has_version = true;
        }

        // 1. load persisted global options
        let mut persisted_source: Option<(u8, bool)> = None;
        let persisted = match self.inner.load_persisted_options_if_present()? {
            Some(persisted) => {
                persisted_source = Some((persisted.concurrent_write, persisted.sync_on_write));
                persisted
            }
            None if !has_version && self.inner.is_pristine_uninitialized()? => {
                self.inner.bootstrap_persisted_options(&current)?;
                has_version = true;
                current.clone()
            }
            None => return Err(OpCode::Corruption),
        };
        let needs_writeback = persisted != current;
        // the open plan is the single place that decides source/target widths and routes
        let durable_legacy_per_group_wal =
            self.inner.opt.sync_on_write && self.inner.opt.has_legacy_per_group_wal()?;
        let plan = OpenPlan::derive(
            persisted_source,
            self.inner.opt.as_ref(),
            durable_legacy_per_group_wal,
        );

        // 2. load sequences
        if let Ok(val) = self
            .inner
            .btree
            .view(BUCKET_MISC, |txn| txn.get(SEQUENCES_KEY))
        {
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
                sequences_ref,
                src;
                next_file_id,
                next_bucket_id,
                oracle
            );
            self.max_file_id = src.next_file_id.load(Relaxed).saturating_sub(1);
        }

        // 3. load BucketMeta
        self.inner
            .btree
            .view(BUCKET_METAS, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let meta = BucketMeta::decode(&v);
                    let name =
                        std::str::from_utf8(&k).expect("bucket metadata key must be valid utf-8");
                    let meta = Arc::new(meta);
                    let bucket_id = meta.id;
                    self.inner
                        .bucket_metas_by_id
                        .insert(bucket_id, meta.clone());
                    self.inner.bucket_metas.insert(name.into(), meta);
                    self.inner.ensure_bucket_runtime(bucket_id);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        // 2.1 count pending buckets
        let mut nr_buckets = self.inner.bucket_metas.len() as u64;
        let _ = self.inner.btree.view(BUCKET_PENDING_DEL, |txn| {
            let mut iter = txn.iter();
            let mut k = Vec::new();
            let mut v = Vec::new();
            while iter.next_ref(&mut k, &mut v) {
                nr_buckets += 1;
            }
            Ok(())
        });
        self.inner.nr_buckets.store(nr_buckets, Relaxed);

        // 3. load bucket durable frontier
        self.inner
            .btree
            .view(BUCKET_FRONTIER, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let item = BucketDurableFrontier::decode(&v);
                    self.inner.bucket_frontier.insert(item.bucket_id, item.lsn);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        for kind in FileKind::ALL {
            let mut active_size = 0;
            let mut total_size = 0;
            self.inner
                .btree
                .view(
                    match kind {
                        FileKind::Data => BUCKET_DATA_STAT,
                        FileKind::Blob => BUCKET_BLOB_STAT,
                    },
                    |txn| {
                        let mut iter = txn.iter();
                        let mut k = Vec::new();
                        let mut v = Vec::new();
                        while iter.next_ref(&mut k, &mut v) {
                            let inner = PersistStat::decode_inner_only(&v);
                            active_size += inner.active_size as u64;
                            total_size += inner.total_size as u64;
                            let fstat = MemStat::from_parts(inner, None);
                            self.inner.stat_ctx(kind).insert_loaded_stat(fstat);
                            self.inner
                                .get_bucket_runtime(inner.bucket_id)
                                .observe_stat_epoch(kind, inner.up1, inner.up2);
                        }
                        Ok(())
                    },
                )
                .map_err(|_| OpCode::IoError)?;
            self.set_loaded_sizes(kind, active_size, total_size);

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
                            obs.push(id);
                        }
                        Ok(())
                    },
                )
                .map_err(|_| OpCode::IoError)?;
        }

        if !has_version {
            self.inner
                .btree
                .exec(BUCKET_VERSION, |txn| {
                    txn.put(VERSION_KEY, CURRENT_VERSION.to_le_bytes())
                })
                .map_err(|_| OpCode::IoError)?;
        }

        Ok(LoadOutcome {
            plan,
            writeback: needs_writeback.then_some(current),
        })
    }

    pub(crate) fn finish(mut self) -> Result<Manifest, OpCode> {
        // both cleanups sync the data directory, and this runs before recovery: a failed sync
        // must fail the open with an error, which the caller reports as a failed open
        self.clean_orphans()?;
        self.inner.delete_files()?;

        #[cfg(feature = "extra_check")]
        self.inner
            .stat_ctx(FileKind::Data)
            .update_size(self.data_active_size, self.data_total_size);

        #[cfg(feature = "extra_check")]
        self.inner
            .stat_ctx(FileKind::Blob)
            .update_size(self.blob_active_size, self.blob_total_size);

        self.inner
            .sequences
            .next_file_id
            .store(self.max_file_id + 1, Relaxed);

        Ok(self.inner)
    }

    fn set_loaded_sizes(&mut self, kind: FileKind, active_size: u64, total_size: u64) {
        match kind {
            FileKind::Data => {
                self.data_active_size = active_size;
                self.data_total_size = total_size;
            }
            FileKind::Blob => {
                self.blob_active_size = active_size;
                self.blob_total_size = total_size;
            }
        }
    }

    fn clean_orphans(&mut self) -> Result<(), OpCode> {
        let mut markers: [Vec<(u64, Vec<u8>)>; 2] = [Vec::new(), Vec::new()];

        let _ = self.inner.btree.view(BUCKET_MISC, |txn| {
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
        });

        let mut cleaned_marker_keys: [Vec<Vec<u8>>; 2] = [Vec::new(), Vec::new()];
        for kind in FileKind::ALL {
            for (id, key) in markers[kind.slot()].drain(..) {
                self.observe_orphan_id(id);
                let path = match kind {
                    FileKind::Data => self.inner.opt.data_file(id),
                    FileKind::Blob => self.inner.opt.blob_file(id),
                };
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
    use super::OpenPlan;
    use crate::Options;

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
