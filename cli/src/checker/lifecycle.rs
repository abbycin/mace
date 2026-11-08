//! lifecycle records, the metadata manifest and the read-only open path

use std::collections::{HashMap, HashSet};

use btree_store::{BTree, Error as BTreeError, OpenError};
use mace::maintenance::{
    BUCKET_FRONTIER, BUCKET_METAS, BUCKET_MISC, BUCKET_OBSOLETE_BLOB, BUCKET_OBSOLETE_DATA,
    BUCKET_PENDING_DEL, BUCKET_VERSION, BucketDurableFrontier, BucketMeta, CURRENT_VERSION,
    FileKind, IMetaCodec, OPTIONS_KEY, ORPHAN_BLOB_MARKER_PREFIX, ORPHAN_DATA_MARKER_PREFIX,
    Options, PersistedOptions, VERSION_KEY, WAL_RECYCLE_PREFIX, WalRecycleState,
};

use super::payload::PayloadChecks;
use super::stats::read_stats;
use super::wal::wal_segment_name;
use super::{CheckCategory, CheckReport, Diagnostic};

#[derive(Default)]
pub(super) struct LifecycleFacts {
    pub(super) catalog: Vec<u64>,
    pub(super) pending: HashSet<u64>,
    pub(super) blocked_files: HashSet<(FileKind, u64)>,
    pub(super) frontiers: HashMap<u64, BucketDurableFrontier>,
    pub(super) inventory_complete: bool,
    pub(super) healthy: bool,
}

/// validates the lifecycle records against the metadata they belong to:
///
/// - an orphan marker must not name a file that a stat record still references;
/// - an obsolete entry must not name a file that a stat record still references;
/// - a pending-delete bucket must not still be in the bucket catalog;
/// - a bucket frontier must exist for every catalog bucket, and must not survive for a
///   bucket that is neither in the catalog nor pending deletion;
/// - a stream's durable recycle boundary must equal the oldest segment present.
pub(super) fn check_lifecycle(
    opt: &Options,
    btree: &BTree,
    checks: &mut PayloadChecks,
    report: &mut CheckReport,
) -> LifecycleFacts {
    let mut facts = LifecycleFacts {
        healthy: true,
        ..LifecycleFacts::default()
    };
    let mut catalog = Vec::new();
    let mut pending = Vec::new();
    let mut frontiers = Vec::new();
    let mut orphans: Vec<(FileKind, u64)> = Vec::new();
    let mut obsolete: Vec<(FileKind, u64)> = Vec::new();
    let mut recycle: Vec<WalRecycleState> = Vec::new();
    let mut undecodable_recycle: Vec<Vec<u8>> = Vec::new();

    let scan = (|| -> Result<(), BTreeError> {
        catalog = catalog_bucket_ids(btree)?;
        for (bucket, out) in [
            (BUCKET_PENDING_DEL, &mut pending),
            (BUCKET_FRONTIER, &mut frontiers),
        ] {
            btree.view(bucket, |txn| {
                let mut iter = txn.iter();
                let (mut key, mut value) = (Vec::new(), Vec::new());
                while iter.next_ref(&mut key, &mut value) {
                    if let Ok(bytes) = <[u8; 8]>::try_from(&key[..]) {
                        let id = u64::from_le_bytes(bytes);
                        out.push(id);
                        if bucket == BUCKET_FRONTIER {
                            if value.len() >= size_of::<BucketDurableFrontier>() {
                                let frontier = BucketDurableFrontier::decode(&value);
                                if frontier.bucket_id == id {
                                    facts.frontiers.insert(id, frontier);
                                } else {
                                    facts.healthy = false;
                                    report.push_diagnostic(
                                        Diagnostic::new(
                                            CheckCategory::BadLifecycleState,
                                            format!(
                                                "durable frontier row {id} carries bucket id {}",
                                                frontier.bucket_id
                                            ),
                                        )
                                        .with_bucket(id),
                                    );
                                }
                            } else {
                                facts.healthy = false;
                                report.push_diagnostic(
                                    Diagnostic::new(
                                        CheckCategory::BadLifecycleState,
                                        format!(
                                            "durable frontier row {id} carries {} bytes, expected {}",
                                            value.len(),
                                            size_of::<BucketDurableFrontier>()
                                        ),
                                    )
                                    .with_bucket(id),
                                );
                            }
                        }
                    } else {
                        facts.healthy = false;
                        report.push_diagnostic(Diagnostic::new(
                            CheckCategory::BadLifecycleState,
                            format!("lifecycle row in {bucket} has a {}-byte key", key.len()),
                        ));
                    }
                }
                Ok(())
            })?;
        }
        for (kind, marker_prefix) in [
            (FileKind::Data, ORPHAN_DATA_MARKER_PREFIX),
            (FileKind::Blob, ORPHAN_BLOB_MARKER_PREFIX),
        ] {
            btree.view(BUCKET_MISC, |txn| {
                let mut iter = txn.iter();
                let (mut key, mut value) = (Vec::new(), Vec::new());
                while iter.next_ref(&mut key, &mut value) {
                    let Some(raw) = key.strip_prefix(marker_prefix.as_bytes()) else {
                        continue;
                    };
                    if let Ok(id) = std::str::from_utf8(raw).unwrap_or("").parse::<u64>() {
                        orphans.push((kind, id));
                    }
                }
                Ok(())
            })?;
        }
        for (kind, bucket) in [
            (FileKind::Data, BUCKET_OBSOLETE_DATA),
            (FileKind::Blob, BUCKET_OBSOLETE_BLOB),
        ] {
            btree.view(bucket, |txn| {
                let mut iter = txn.iter();
                let (mut key, mut value) = (Vec::new(), Vec::new());
                while iter.next_ref(&mut key, &mut value) {
                    if let Ok(bytes) = <[u8; 8]>::try_from(&key[..]) {
                        obsolete.push((kind, u64::from_le_bytes(bytes)));
                    }
                }
                Ok(())
            })?;
        }
        btree.view(BUCKET_MISC, |txn| {
            let mut iter = txn.iter();
            let (mut key, mut value) = (Vec::new(), Vec::new());
            while iter.next_ref(&mut key, &mut value) {
                let Some(raw) = key.strip_prefix(WAL_RECYCLE_PREFIX.as_bytes()) else {
                    continue;
                };
                if std::str::from_utf8(raw).is_ok() && value.len() >= size_of::<WalRecycleState>() {
                    recycle.push(WalRecycleState::decode(&value));
                } else {
                    // the row carries a stream's durable recycle boundary; skipping it silently
                    // would drop that stream's boundary rule from this run
                    undecodable_recycle.push(key.clone());
                }
            }
            Ok(())
        })?;
        Ok(())
    })();
    if let Err(err) = scan {
        report.push(
            CheckCategory::UnreadableMetadata,
            format!("cannot scan the lifecycle records: {err:?}"),
        );
        facts.healthy = false;
        return facts;
    }
    facts.inventory_complete = true;
    for key in &undecodable_recycle {
        facts.healthy = false;
        report.push(
            CheckCategory::UnreadableMetadata,
            format!(
                "wal recycle row {} cannot be decoded: its key suffix is not a stream id or its value is short of a recycle state",
                String::from_utf8_lossy(key)
            ),
        );
    }

    // a duplicated row must not repeat every diagnostic for that bucket
    for ids in [&mut pending, &mut frontiers] {
        ids.sort_unstable();
        ids.dedup();
    }

    // the stat scan diagnostics were already reported by `check_bucket_metadata`, which
    // runs first; this phase only needs the identities
    let mut stats = Vec::new();
    for kind in FileKind::ALL {
        let view = read_stats(opt, btree, kind).0;
        stats.extend(view.records.into_iter().map(|stat| (kind, stat.file_id)));
    }

    for (kind, file_id) in &orphans {
        checks.mark_referenced(*kind, *file_id);
    }
    for (kind, file_id) in &obsolete {
        checks.mark_referenced(*kind, *file_id);
    }
    for (kind, file_id) in &orphans {
        if stats.contains(&(*kind, *file_id)) {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadLifecycleState,
                    "an orphan marker names a file that a stat record still references".to_string(),
                )
                .with_file(*file_id),
            );
        }
    }
    for (kind, file_id) in &obsolete {
        if stats.contains(&(*kind, *file_id)) {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadLifecycleState,
                    "an obsolete entry names a file that a stat record still references"
                        .to_string(),
                )
                .with_file(*file_id),
            );
        }
    }
    for bucket_id in &pending {
        if catalog.contains(bucket_id) {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadLifecycleState,
                    "a pending-delete bucket is still in the bucket catalog".to_string(),
                )
                .with_bucket(*bucket_id),
            );
        }
    }
    for bucket_id in &catalog {
        if !frontiers.contains(bucket_id) {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadLifecycleState,
                    "a catalog bucket has no durable frontier".to_string(),
                )
                .with_bucket(*bucket_id),
            );
        }
    }
    for bucket_id in &frontiers {
        if !catalog.contains(bucket_id) && !pending.contains(bucket_id) {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadLifecycleState,
                    "a durable frontier outlives its bucket".to_string(),
                )
                .with_bucket(*bucket_id),
            );
        }
    }

    // the durable recycle boundary of a stream with segments must equal its oldest one
    let mut oldest: std::collections::HashMap<u8, u64> = std::collections::HashMap::new();
    if let Ok(entries) = opt.fs().read_dir(&opt.log_root()) {
        for path in entries {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if let Some((physical_id, file_id)) = wal_segment_name(name) {
                oldest
                    .entry(physical_id)
                    .and_modify(|current| *current = (*current).min(file_id))
                    .or_insert(file_id);
            }
        }
    }
    for state in &recycle {
        if state.stage == WalRecycleState::STAGE_DONE
            && let Some(first) = oldest.get(&state.group_id)
            && *first != state.to_file_id
        {
            facts.healthy = false;
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadLifecycleState,
                    format!(
                        "stream {} recycle boundary {} does not match its oldest segment {first}",
                        state.group_id, state.to_file_id
                    ),
                )
                .with_file(*first),
            );
        }
    }
    facts.catalog = catalog;
    facts.pending.extend(pending);
    facts.blocked_files.extend(orphans);
    facts.blocked_files.extend(obsolete);
    facts
}

pub(super) fn catalog_bucket_ids(btree: &BTree) -> Result<Vec<u64>, BTreeError> {
    let mut ids = Vec::new();
    btree.view(BUCKET_METAS, |txn| {
        let mut iter = txn.iter();
        let mut key = Vec::new();
        let mut value = Vec::new();
        while iter.next_ref(&mut key, &mut value) {
            if value.len() >= size_of::<BucketMeta>() {
                ids.push(BucketMeta::decode(&value).id);
            }
        }
        Ok(())
    })?;
    // a duplicated catalog id must not repeat every diagnostic for that bucket
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

/// whether the metadata store exists but was never initialized: every metadata bucket is
/// absent, which is the state the runtime bootstraps on open
pub(super) fn metadata_is_uninitialized(btree: &BTree) -> bool {
    [BUCKET_VERSION, BUCKET_MISC, BUCKET_METAS]
        .iter()
        .all(|bucket| {
            matches!(
                btree.view(bucket, |txn| txn.get(&[] as &[u8]).map(|_| ())),
                Err(BTreeError::BucketNotFound)
            )
        })
}

/// Opens the metadata store through btree-store's read-only mode.
///
/// the diagnostic an open error gets when it is neither absence nor plain unreadability
pub(super) struct OpenErrorClass {
    pub(super) detail: fn(&OpenError, &std::path::Path) -> Diagnostic,
}

/// classifies the open failures that must not be reported as damage: a store another
/// process holds, a live instance of this path in this process, and a file whose on-disk
/// format this build cannot read (the runtime refuses it too, and the fix is an offline
/// rebuild rather than a repair)
pub(super) fn classify_open_error(err: &OpenError) -> Option<OpenErrorClass> {
    let class = match err {
        OpenError::DatabaseBusy { .. } => OpenErrorClass {
            detail: |_, path| {
                Diagnostic::new(
                    CheckCategory::MetadataBusy,
                    format!(
                        "metadata store {} is held open by another process",
                        path.display()
                    ),
                )
            },
        },
        // the backend raises `InvalidOptions` only for a live instance of this path
        // that was opened with different options, which is contention too
        OpenError::InvalidOptions(_) => OpenErrorClass {
            detail: |_, path| {
                Diagnostic::new(
                    CheckCategory::MetadataBusy,
                    format!(
                        "metadata store {} has a live read-write instance in this process",
                        path.display()
                    ),
                )
            },
        },
        OpenError::Corruption(report)
            if matches!(
                report.code,
                "UNSUPPORTED_FORMAT_VERSION" | "MIXED_FORMAT_VERSIONS"
            ) =>
        {
            OpenErrorClass {
                detail: |err, path| {
                    let OpenError::Corruption(report) = err else {
                        unreachable!("classified from a corruption error")
                    };
                    Diagnostic::new(
                        CheckCategory::UnsupportedMetadataFormat,
                        format!(
                            "metadata store {} is format {}, which this build cannot read; \
                             rebuild it offline with `btree-store migrate <source> \
                             --output <destination> --to {}`",
                            path.display(),
                            report.actual.as_deref().unwrap_or("?"),
                            btree_store::FORMAT_VERSION
                        ),
                    )
                },
            }
        }
        _ => return None,
    };
    Some(class)
}

/// The upstream mode guarantees the properties this checker needs: the file is not
/// created, no byte is written, and the lock is shared. It is taken through
/// `open_read_only`, so absence and an unusable file surface as typed `OpenError`s
/// that this function classifies; a page read on a damaged store still aborts inside
/// btree-store after logging `BTREE_FATAL_*`, which is pinned by
/// `cli/tests/checker.rs::metadata_page_damage_aborts_by_design`.
pub(super) fn open_metadata_read_only(opt: &Options, report: &mut CheckReport) -> Option<BTree> {
    let path = opt.manifest();
    match BTree::open_read_only(&path) {
        Ok(btree) => Some(btree),
        Err(OpenError::Io(error))
            if error.source_error().kind() == std::io::ErrorKind::NotFound =>
        {
            report.push(
                CheckCategory::MissingMetadata,
                format!("metadata store {} is absent", path.display()),
            );
            None
        }
        Err(OpenError::DatabaseBusy { .. }) => {
            report.push(
                CheckCategory::MetadataBusy,
                format!(
                    "metadata store {} is held open by another process",
                    path.display()
                ),
            );
            None
        }
        Err(err) => {
            // a file this build cannot read is not damage and not lock contention, so it
            // gets its own class and names the offline rebuild
            if let Some(class) = classify_open_error(&err) {
                report.push_diagnostic((class.detail)(&err, &path));
                return None;
            }
            report.push(
                CheckCategory::UnreadableMetadata,
                format!("cannot open {}: {err:?}", path.display()),
            );
            None
        }
    }
}

pub(super) fn check_version(btree: &BTree, report: &mut CheckReport) {
    let stored = match btree.view(BUCKET_VERSION, |txn| txn.get(VERSION_KEY)) {
        Ok(raw) => raw,
        Err(BTreeError::KeyNotFound) => {
            report.push(
                CheckCategory::BadVersion,
                "the metadata store carries no version record".to_string(),
            );
            return;
        }
        Err(err) => {
            report.push(
                CheckCategory::UnreadableMetadata,
                format!("cannot read the version record: {err:?}"),
            );
            return;
        }
    };
    let Ok(bytes) = <[u8; 8]>::try_from(&stored[..]) else {
        report.push(
            CheckCategory::BadVersion,
            format!("the version record has {} bytes, expected 8", stored.len()),
        );
        return;
    };
    let version = u64::from_le_bytes(bytes);
    if version != CURRENT_VERSION {
        report.push(
            CheckCategory::BadVersion,
            format!("stored version {version} is not supported (current {CURRENT_VERSION})"),
        );
    }
}

/// decodes the persisted options record and returns whether the database's own
/// `truncate_corrupted_wal` allows a malformed tail to be tolerated; an absent or
/// undecodable record reports `BadOptions` and leaves the strict answer, so an
/// unreadable record never widens what the checker tolerates
pub(super) fn check_options(btree: &BTree, report: &mut CheckReport) -> bool {
    let raw = match btree.view(BUCKET_MISC, |txn| txn.get(OPTIONS_KEY)) {
        Ok(raw) => raw,
        Err(BTreeError::KeyNotFound) => {
            report.push(
                CheckCategory::BadOptions,
                "the metadata store carries no persisted options".to_string(),
            );
            return false;
        }
        Err(err) => {
            report.push(
                CheckCategory::UnreadableMetadata,
                format!("cannot read the persisted options: {err:?}"),
            );
            return false;
        }
    };
    match PersistedOptions::from_json(&raw) {
        Ok(persisted) => persisted.truncate_corrupted_wal,
        Err(err) => {
            report.push(
                CheckCategory::BadOptions,
                format!("persisted options cannot be decoded: {err:?}"),
            );
            false
        }
    }
}

pub(super) fn check_bucket_catalog(btree: &BTree, report: &mut CheckReport) {
    let mut seen_ids: Vec<u64> = Vec::new();
    let outcome = btree.view(BUCKET_METAS, |txn| {
        let mut iter = txn.iter();
        let mut key = Vec::new();
        let mut value = Vec::new();
        while iter.next_ref(&mut key, &mut value) {
            let Ok(name) = std::str::from_utf8(&key) else {
                report.push(
                    CheckCategory::BadBucketCatalog,
                    "a bucket catalog key is not valid utf-8".to_string(),
                );
                continue;
            };
            if value.len() < size_of::<BucketMeta>() {
                // `decode` expects a complete record and panics on a short slice
                report.push_diagnostic(Diagnostic::new(
                    CheckCategory::BadBucketCatalog,
                    format!(
                        "bucket {name} carries {} bytes, expected {}",
                        value.len(),
                        size_of::<BucketMeta>()
                    ),
                ));
                continue;
            }
            let meta = BucketMeta::decode(&value);
            if seen_ids.contains(&meta.id) {
                report.push_diagnostic(
                    Diagnostic::new(
                        CheckCategory::BadBucketCatalog,
                        format!("bucket {name} reuses bucket id {}", meta.id),
                    )
                    .with_bucket(meta.id),
                );
            } else {
                seen_ids.push(meta.id);
            }
        }
        Ok(())
    });
    if let Err(err) = outcome {
        report.push(
            CheckCategory::UnreadableMetadata,
            format!("cannot scan the bucket catalog: {err:?}"),
        );
    }
}
