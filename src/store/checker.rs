//! Read-only consistency checking of a mace database directory.
//!
//! The checker never constructs a [`crate::Mace`] instance: opening one runs
//! writeback recovery (orphan cleanup, persisted-option writeback, epoch wipe) and
//! starts the background services, none of which a checker may do. It reads the
//! metadata store and the payload/WAL files directly and reports what it finds.
//!
//! Scope: a read-only metadata access path, manifest-level validation (version, global
//! options, bucket catalog), the cross-record consistency checks (intervals, stats, page
//! tables, lifecycle records), the payload file checks and the WAL framing checks.
//!
//! # Classification boundary
//!
//! Damage is classified only where classification is meaningful:
//!
//! - *open-time* metadata damage (absent store, empty file, unusable meta slots) is
//!   reported as a category;
//! - payload and WAL damage is reported by the file validators, which read those files
//!   through mace's own IO;
//! - *page-level* metadata damage is **fatal by design**: btree-store aborts with
//!   `BTREE_FATAL_IO`/`BTREE_FATAL_CORRUPTION` rather than pretending an unusable
//!   metadata store can still be described, after logging a `BTREE_FATAL_*` line (path,
//!   generation, offset). The CLI adopts that as its failure mode: page-level metadata
//!   damage terminates the tool with a non-zero signal after the log line.

/// the interval view of one bucket: the decoded pairs, plus how many rows could not be
/// decoded. a bucket with a dropped row has an incomplete interval set, so the page table
/// must not be judged against it (that damage was already reported as a bad interval row)
#[derive(Default)]
struct IntervalView {
    pairs: Vec<IntervalPair>,
    dropped: usize,
}

/// the stat view one kind's cross-checks need: the decoded records plus the file ids
/// whose record was already reported as undecodable (so the interval phase must not
/// report the same damage as a missing stat record)
#[derive(Default)]
struct StatView {
    records: Vec<StatRecord>,
    undecodable: std::collections::HashSet<u64>,
}

/// per-run state shared by the payload cross-checks: decoded relocation tables and the
/// payload files already reported as unreadable
#[derive(Default)]
struct PayloadChecks {
    relocs: std::collections::HashMap<(FileKind, u64), FileRelocs>,
    readers: std::collections::HashMap<(FileKind, u64), Result<Arc<FileReader>, OpCode>>,
    decoders: Option<Arc<DecompressorPool>>,
    reported: std::collections::HashSet<(FileKind, u64)>,
    referenced: std::collections::HashMap<FileKind, std::collections::HashSet<u64>>,
}

impl PayloadChecks {
    fn reader(
        &mut self,
        opt: &Options,
        kind: FileKind,
        file_id: u64,
    ) -> Result<Arc<FileReader>, OpCode> {
        if let Some(reader) = self.readers.get(&(kind, file_id)) {
            return reader.clone();
        }
        let decoders = self
            .decoders
            .get_or_insert_with(DecompressorPool::new)
            .clone();
        let path = match kind {
            FileKind::Data => opt.data_file(file_id),
            FileKind::Blob => opt.blob_file(file_id),
        };
        let reader = try_new_reader(path, decoders, opt.fs.clone());
        self.readers.insert((kind, file_id), reader.clone());
        reader
    }

    /// records that a payload file is named by metadata: a stat record, an orphan marker or
    /// an obsolete entry. every enumerated payload file must be named by one of them
    fn mark_referenced(&mut self, kind: FileKind, file_id: u64) {
        self.referenced.entry(kind).or_default().insert(file_id);
    }

    /// whether metadata names this payload file at all
    fn is_referenced(&self, kind: FileKind, file_id: u64) -> bool {
        self.referenced
            .get(&kind)
            .is_some_and(|ids| ids.contains(&file_id))
    }

    /// whether this file was already reported, whatever the phase that reported it
    fn is_reported(&self, kind: FileKind, file_id: u64) -> bool {
        self.reported.contains(&(kind, file_id))
    }

    /// reports an unreadable payload file once per file, not once per consumer
    fn report_unreadable(
        &mut self,
        kind: FileKind,
        file_id: u64,
        bucket_id: Option<u64>,
        reason: &str,
        report: &mut CheckReport,
    ) {
        if self.reported.insert((kind, file_id)) {
            let mut diagnostic = Diagnostic::new(
                CheckCategory::BadPayloadFile,
                format!("payload file cannot be read: {reason}"),
            )
            .with_file(file_id);
            if let Some(bucket_id) = bucket_id {
                diagnostic = diagnostic.with_bucket(bucket_id);
            }
            report.push_diagnostic(diagnostic);
        }
    }
}

use std::sync::Arc;

use crate::cc::wal::{
    EntryType, PayloadType, WalAbort, WalCheckpoint, WalUpdate, ptr_to, wal_record_sz,
};
use crate::io::{File, GatherIO};
use crate::map::data::MetaReader;
use crate::meta::{
    BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_FRONTIER, BUCKET_METAS, BUCKET_MISC,
    BUCKET_OBSOLETE_BLOB, BUCKET_OBSOLETE_DATA, BUCKET_PENDING_DEL, BUCKET_VERSION, BucketMeta,
    CURRENT_VERSION, FileKind, FileReader, IMetaCodec, IntervalPair, OPTIONS_KEY,
    ORPHAN_BLOB_MARKER_PREFIX, ORPHAN_DATA_MARKER_PREFIX, PersistStat, VERSION_KEY,
    WAL_RECYCLE_PREFIX, WalRecycleState, interval_bucket_name, page_table_name, try_new_reader,
};
use crate::types::header::DeltaHeader;
use crate::types::refbox::BoxRef;
use crate::types::{
    header::{BaseHeader, NodeType, TagFlag, TagKind},
    traits::IHeader,
};
use crate::utils::data::{AddrPair, Interval};
use crate::utils::{
    NULL_ADDR, NULL_PID, OpCode,
    compress::DecompressorPool,
    options::{Options, PersistedOptions},
    varint::Varint32,
};
use btree_store::{BTree, Error as BTreeError, OpenError};

/// stable diagnostic categories reported by the checker
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckCategory {
    /// the metadata store file is absent
    MissingMetadata,
    /// the metadata store cannot be opened or read
    UnreadableMetadata,
    /// the metadata store is locked by another process, so nothing could be checked
    MetadataBusy,
    /// the metadata store is a format this build cannot read, so an offline rebuild is
    /// required before it can be opened at all
    UnsupportedMetadataFormat,
    /// the stored metadata organization version is absent or unsupported
    BadVersion,
    /// the persisted global options record is absent or undecodable
    BadOptions,
    /// the bucket catalog entry cannot be decoded or is inconsistent
    BadBucketCatalog,
    /// a data/blob file is unreadable or self-inconsistent
    BadPayloadFile,
    /// a WAL stream has a gap, or a record that does not frame/checksum
    BadWal,
    /// a published interval disagrees with the file or stat record it names
    BadInterval,
    /// a file's stat accounting disagrees with its own relocation table
    BadStatAccounting,
    /// a lifecycle record (orphan marker, obsolete list, pending delete, wal recycle
    /// state, bucket frontier) contradicts the metadata it belongs to
    BadLifecycleState,
    /// a page table entry points at an address no interval of its bucket covers
    BadPageTable,
    /// a readable page record points at an address no interval or relocation resolves
    BadPageChain,
}

impl CheckCategory {
    /// stable machine-readable name, used by the CLI exit output
    pub const fn name(self) -> &'static str {
        match self {
            Self::MissingMetadata => "missing_metadata",
            Self::UnreadableMetadata => "unreadable_metadata",
            Self::MetadataBusy => "metadata_busy",
            Self::UnsupportedMetadataFormat => "unsupported_metadata_format",
            Self::BadVersion => "bad_version",
            Self::BadOptions => "bad_options",
            Self::BadBucketCatalog => "bad_bucket_catalog",
            Self::BadPayloadFile => "bad_payload_file",
            Self::BadWal => "bad_wal",
            Self::BadInterval => "bad_interval",
            Self::BadStatAccounting => "bad_stat_accounting",
            Self::BadLifecycleState => "bad_lifecycle_state",
            Self::BadPageTable => "bad_page_table",
            Self::BadPageChain => "bad_page_chain",
        }
    }
}

/// one located problem: category plus the identities needed to find it
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostic {
    pub category: CheckCategory,
    pub bucket_id: Option<u64>,
    pub file_id: Option<u64>,
    pub address: Option<u64>,
    pub detail: String,
}

impl Diagnostic {
    pub fn new(category: CheckCategory, detail: String) -> Self {
        Self {
            category,
            bucket_id: None,
            file_id: None,
            address: None,
            detail,
        }
    }

    pub fn with_bucket(mut self, bucket_id: u64) -> Self {
        self.bucket_id = Some(bucket_id);
        self
    }

    pub fn with_file(mut self, file_id: u64) -> Self {
        self.file_id = Some(file_id);
        self
    }

    pub fn with_address(mut self, address: u64) -> Self {
        self.address = Some(address);
        self
    }
}

/// outcome of a check run
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckReport {
    pub diagnostics: Vec<Diagnostic>,
}

impl CheckReport {
    /// true when no problem was found
    pub fn is_ok(&self) -> bool {
        self.diagnostics.is_empty()
    }

    /// process exit code for the CLI: `0` when clean, `1` when anything was found
    pub fn exit_code(&self) -> i32 {
        if self.is_ok() { 0 } else { 1 }
    }

    /// per-category counts in a stable order, for the CLI summary line
    pub fn summary(&self) -> Vec<(&'static str, usize)> {
        let mut counts: Vec<(&'static str, usize)> = Vec::new();
        for diagnostic in &self.diagnostics {
            let name = diagnostic.category.name();
            match counts.iter_mut().find(|(existing, _)| *existing == name) {
                Some((_, count)) => *count += 1,
                None => counts.push((name, 1)),
            }
        }
        counts.sort_unstable();
        counts
    }

    fn push(&mut self, category: CheckCategory, detail: String) {
        self.diagnostics.push(Diagnostic::new(category, detail));
    }

    fn push_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    /// sorts diagnostics so two runs over the same damage print the same order
    fn sort(&mut self) {
        self.diagnostics.sort_by(|left, right| {
            left.category
                .name()
                .cmp(right.category.name())
                .then(left.bucket_id.cmp(&right.bucket_id))
                .then(left.file_id.cmp(&right.file_id))
                .then(left.address.cmp(&right.address))
                .then(left.detail.cmp(&right.detail))
        });
    }
}

/// Checks a database directory without modifying it.
///
/// `opt` only supplies the paths (`db_root`, `log_root`) and the filesystem hook;
/// the caller's options are never validated here, because validation creates the
/// directory structure, and no other field is read: the WAL tail tolerance comes
/// from the database's own persisted options, so the verdict does not depend on
/// what the caller happened to pass.
pub fn check_database(opt: &Options) -> CheckReport {
    let mut report = CheckReport::default();
    // one read-only open serves every metadata-level phase, so a missing or unusable
    // store is reported exactly once
    let Some(btree) = open_metadata_read_only(opt, &mut report) else {
        report.sort();
        return report;
    };
    if metadata_is_uninitialized(&btree) {
        // btree-store creates the manifest file on open and the runtime treats the absence
        // of every metadata bucket as the bootstrap state
        // the metadata is uninitialized, so report this state once
        // reported once instead of once per absent bucket
        report.push(
            CheckCategory::MissingMetadata,
            "the metadata store carries no version, options or bucket catalog yet".to_string(),
        );
        report.sort();
        return report;
    }
    check_version(&btree, &mut report);
    let tolerate_tail = check_options(&btree, &mut report);
    check_bucket_catalog(&btree, &mut report);
    let mut checks = PayloadChecks::default();
    check_bucket_metadata(opt, &btree, &mut checks, &mut report);
    check_lifecycle(opt, &btree, &mut checks, &mut report);
    check_payload_files(opt, &mut checks, &mut report);
    check_wal(opt, tolerate_tail, &mut report);
    report.sort();
    report
}

/// validates every data/blob file on its own terms: footer and table framing, table
/// checksums, relocation ordering and bounds, and that every published interval
/// resolves to a relocation in the same file
fn check_payload_files(opt: &Options, checks: &mut PayloadChecks, report: &mut CheckReport) {
    let data_root = opt.data_root();
    let entries = match opt.fs.read_dir(&data_root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
        Err(err) => {
            report.push(
                CheckCategory::UnreadableMetadata,
                format!("cannot list {}: {err}", data_root.display()),
            );
            return;
        }
    };
    for path in entries {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some((kind, file_id)) = payload_file_id(name) else {
            continue;
        };
        check_payload_file(opt, &path, name, kind, file_id, checks, report);
        // a payload file that no stat record, orphan marker or obsolete entry names is a
        // leak: the runtime's own accounting treats it as a violation
        // (`protocol.persist_stat_matches_payload_relocations`)
        if !checks.is_referenced(kind, file_id) && !checks.is_reported(kind, file_id) {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadLifecycleState,
                    format!("{name} is named by no stat record, orphan marker or obsolete entry"),
                )
                .with_file(file_id),
            );
        }
    }
}

/// validates the lifecycle records against the metadata they belong to:
///
/// - an orphan marker must not name a file that a stat record still references;
/// - an obsolete entry must not name a file that a stat record still references;
/// - a pending-delete bucket must not still be in the bucket catalog;
/// - a bucket frontier must exist for every catalog bucket, and must not survive for a
///   bucket that is neither in the catalog nor pending deletion;
/// - a stream's durable recycle boundary must equal the oldest segment present.
fn check_lifecycle(
    opt: &Options,
    btree: &BTree,
    checks: &mut PayloadChecks,
    report: &mut CheckReport,
) {
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
                        out.push(u64::from_le_bytes(bytes));
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
        return;
    }
    for key in &undecodable_recycle {
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
    let stats: Vec<(FileKind, u64)> = FileKind::ALL
        .iter()
        .flat_map(|kind| {
            read_stats(opt, btree, *kind)
                .0
                .records
                .into_iter()
                .map(move |stat| (*kind, stat.file_id))
        })
        .collect();

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
    if let Ok(entries) = opt.fs.read_dir(&opt.log_root()) {
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
}

/// validates the page table of one bucket: every mapped logical address must be covered
/// by an interval of the same bucket (registry
/// `protocol.reloc_completeness_for_mapped_addrs`)
fn check_page_table(
    opt: &Options,
    btree: &BTree,
    bucket_id: u64,
    intervals: &IntervalView,
    checks: &mut PayloadChecks,
    report: &mut CheckReport,
) {
    if intervals.dropped > 0 {
        // the interval set is incomplete, and the dropped row was already reported: a page
        // whose address is not covered cannot be told from a page covered by that row
        return;
    }
    let bucket = page_table_name(bucket_id);
    // the page table is a bucket of `page id -> logical address` rows, and an address
    // equal to `NULL_ADDR` means the page is not mapped
    let mut entries: Vec<(u64, u64)> = Vec::new();
    let outcome = btree.view(&bucket, |txn| {
        let mut iter = txn.iter();
        let (mut key, mut value) = (Vec::new(), Vec::new());
        while iter.next_ref(&mut key, &mut value) {
            if key.len() < size_of::<u64>() || value.len() < size_of::<u64>() {
                report.push_diagnostic(
                    Diagnostic::new(
                        CheckCategory::BadPageTable,
                        format!(
                            "page table row carries {} key bytes and {} value bytes",
                            key.len(),
                            value.len()
                        ),
                    )
                    .with_bucket(bucket_id),
                );
                continue;
            }
            let pid = u64::from_le_bytes(key[..8].try_into().expect("checked length"));
            let address = u64::from_le_bytes(value[..8].try_into().expect("checked length"));
            entries.push((pid, address));
        }
        Ok(())
    });
    if let Err(err) = outcome {
        report.push(
            CheckCategory::UnreadableMetadata,
            format!("cannot scan {bucket}: {err:?}"),
        );
        return;
    }

    for (page_id, address) in entries {
        if address == NULL_ADDR {
            continue;
        }
        let Some(interval) = intervals
            .pairs
            .iter()
            .find(|interval| address >= interval.lo_addr && address <= interval.hi_addr)
        else {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadPageTable,
                    format!("page {page_id} maps an address no interval covers"),
                )
                .with_bucket(bucket_id)
                .with_address(address),
            );
            continue;
        };
        // the covering interval names the file; the address must be relocated there
        match reloc_keys(opt, FileKind::Data, interval.file_id, checks) {
            FileRelocs::Keys(keys) => {
                if keys.binary_search(&address).is_err() {
                    report.push_diagnostic(
                        Diagnostic::new(
                            CheckCategory::BadPageTable,
                            format!("page {page_id} maps an address without a relocation"),
                        )
                        .with_bucket(bucket_id)
                        .with_file(interval.file_id)
                        .with_address(address),
                    );
                }
            }
            FileRelocs::Unreadable(reason) => {
                checks.report_unreadable(
                    FileKind::Data,
                    interval.file_id,
                    Some(bucket_id),
                    &reason,
                    report,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn check_page_chains(
    opt: &Options,
    btree: &BTree,
    bucket_id: u64,
    data_intervals: &IntervalView,
    blob_intervals: &IntervalView,
    budget: usize,
    checks: &mut PayloadChecks,
    report: &mut CheckReport,
) {
    if data_intervals.dropped > 0 || blob_intervals.dropped > 0 {
        return;
    }
    let bucket = page_table_name(bucket_id);
    let mut queue = Vec::new();
    let mut page_table: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
    let outcome = btree.view(&bucket, |txn| {
        let mut iter = txn.iter();
        let (mut key, mut value) = (Vec::new(), Vec::new());
        while iter.next_ref(&mut key, &mut value) {
            if key.len() < 8 || value.len() < 8 {
                continue;
            }
            let pid = u64::from_le_bytes(key[..8].try_into().unwrap());
            let address = u64::from_le_bytes(value[..8].try_into().unwrap());
            if address != NULL_ADDR {
                page_table.insert(pid, address);
                queue.push((FileKind::Data, address));
            }
        }
        Ok(())
    });
    if outcome.is_err() {
        return;
    }
    let roots: std::collections::HashSet<(FileKind, u64)> = queue.iter().copied().collect();
    let mut visited = std::collections::HashSet::new();
    let mut remaining = budget;
    while let Some((kind, address)) = queue.pop() {
        if !visited.insert((kind, address)) {
            continue;
        }
        if remaining == 0 {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadPageChain,
                    format!(
                        "page-chain budget exhausted with {} queued references remaining",
                        queue.len() + 1
                    ),
                )
                .with_bucket(bucket_id)
                .with_address(address),
            );
            break;
        }
        remaining -= 1;
        let intervals = match kind {
            FileKind::Data => data_intervals,
            FileKind::Blob => blob_intervals,
        };
        let Some(interval) = intervals
            .pairs
            .iter()
            .find(|ivl| address >= ivl.lo_addr && address <= ivl.hi_addr)
        else {
            if roots.contains(&(kind, address)) {
                continue;
            }
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadPageChain,
                    format!("page record reference {address} has no covering interval"),
                )
                .with_bucket(bucket_id)
                .with_address(address),
            );
            continue;
        };
        let reader = match checks.reader(opt, kind, interval.file_id) {
            Ok(reader) => reader,
            Err(err) => {
                checks.report_unreadable(
                    kind,
                    interval.file_id,
                    Some(bucket_id),
                    &format!("page record reader failed: {err:?}"),
                    report,
                );
                continue;
            }
        };
        let page = match reader.try_read_at(address) {
            Ok(page) => page,
            Err(err) => {
                if err == OpCode::NotFound {
                    if roots.contains(&(kind, address)) {
                        continue;
                    }
                    report.push_diagnostic(
                        Diagnostic::new(
                            CheckCategory::BadPageChain,
                            format!("page record reference {address} has no relocation"),
                        )
                        .with_bucket(bucket_id)
                        .with_address(address),
                    );
                    continue;
                }
                checks.report_unreadable(
                    kind,
                    interval.file_id,
                    Some(bucket_id),
                    &format!("page record {address} failed to decode: {err:?}"),
                    report,
                );
                continue;
            }
        };
        let link = page.header().link;
        if link != NULL_ADDR {
            queue.push((kind, link));
        }
        if kind == FileKind::Data {
            match page_edges(&page) {
                Ok(edges) => {
                    for edge in edges {
                        match edge {
                            PageEdge::Address(edge_kind, address) => {
                                queue.push((edge_kind, address));
                            }
                            PageEdge::Child(pid) => {
                                if let Some(&child_address) = page_table.get(&pid) {
                                    queue.push((FileKind::Data, child_address));
                                } else {
                                    report.push_diagnostic(
                                        Diagnostic::new(
                                            CheckCategory::BadPageChain,
                                            format!(
                                                "page {address} references child page id {pid} with no page-table mapping"
                                            ),
                                        )
                                        .with_bucket(bucket_id)
                                        .with_address(address),
                                    );
                                }
                            }
                        }
                    }
                }
                Err(reason) => checks.report_unreadable(
                    kind,
                    interval.file_id,
                    Some(bucket_id),
                    reason,
                    report,
                ),
            }
        }
    }
}

fn value_edges(raw: &[u8]) -> Result<Vec<(FileKind, u64)>, &'static str> {
    if raw.len() < 6 {
        return Err("value header is truncated");
    }
    let flags = raw[0];
    let data_len = u32::from_le_bytes(raw[2..6].try_into().map_err(|_| "value length")?) as usize;
    let has_hist = flags & 0x80 != 0;
    let remote = flags & 0x10 != 0;
    let data_off = 6usize
        .checked_add(if has_hist { 14 } else { 0 })
        .ok_or("value offset overflow")?;
    if data_off > raw.len() {
        return Err("value reference header is truncated");
    }
    let mut edges = Vec::new();
    if has_hist {
        let addr = u64::from_le_bytes(raw[6..14].try_into().map_err(|_| "history address")?);
        if addr != NULL_ADDR {
            edges.push((FileKind::Data, addr));
        }
    }
    if remote {
        if data_off + 8 > raw.len() {
            return Err("remote address is truncated");
        }
        let addr = u64::from_le_bytes(
            raw[data_off..data_off + 8]
                .try_into()
                .map_err(|_| "remote address")?,
        );
        if addr != NULL_ADDR {
            edges.push((FileKind::Blob, addr));
        }
    } else if data_off
        .checked_add(data_len)
        .is_none_or(|end| end > raw.len())
    {
        return Err("inline value exceeds record");
    }
    Ok(edges)
}

/// one reference carried by a persisted page: a logical address that is already final, or a
/// page ID that must be resolved through the bucket page table first
enum PageEdge {
    Address(FileKind, u64),
    Child(u64),
}

fn internal_child_pid(raw: &[u8]) -> Result<Option<u64>, &'static str> {
    let (key_len, prefix) = Varint32::decode(raw).ok_or("internal key length is invalid")?;
    let value_start = prefix
        .checked_add(key_len as usize)
        .ok_or("internal key overflow")?;
    if value_start + 8 > raw.len() {
        return Err("internal value is truncated");
    }
    let child_pid = u64::from_le_bytes(
        raw[value_start..value_start + 8]
            .try_into()
            .map_err(|_| "internal child pid")?,
    );
    Ok((child_pid != NULL_PID).then_some(child_pid))
}

fn page_edges(page: &BoxRef) -> Result<Vec<PageEdge>, &'static str> {
    let header = page.header();
    let payload = page.data_slice::<u8>();
    match (header.kind, header.node_type) {
        (TagKind::Delta, NodeType::Leaf) => {
            let hdr_len = size_of::<DeltaHeader>();
            if payload.len() < hdr_len {
                return Err("delta header is truncated");
            }
            let key_len =
                u32::from_le_bytes(payload[..4].try_into().map_err(|_| "delta key")?) as usize;
            let val_len =
                u32::from_le_bytes(payload[4..8].try_into().map_err(|_| "delta value")?) as usize;
            let val_start = hdr_len
                .checked_add(key_len)
                .ok_or("delta length overflow")?;
            let val_end = val_start
                .checked_add(val_len)
                .ok_or("delta length overflow")?;
            if val_end > payload.len() {
                return Err("delta value exceeds record");
            }
            Ok(value_edges(&payload[val_start..val_end])?
                .into_iter()
                .map(|(kind, address)| PageEdge::Address(kind, address))
                .collect())
        }
        (TagKind::Delta, NodeType::Intl) => {
            let hdr_len = size_of::<DeltaHeader>();
            if payload.len() < hdr_len {
                return Err("delta header is truncated");
            }
            let delta_view = page.view().as_delta();
            let delta = delta_view.header();
            let key_len = delta.klen as usize;
            let val_len = delta.vlen as usize;
            let end = hdr_len
                .checked_add(key_len)
                .and_then(|end| end.checked_add(val_len))
                .ok_or("delta length overflow")?;
            if end > payload.len() || val_len < 8 {
                return Err("delta key/value exceeds record");
            }
            let mut edges = Vec::new();
            if let Some(pid) = internal_child_pid(&payload[hdr_len..end])? {
                edges.push(PageEdge::Child(pid));
            }
            Ok(edges)
        }
        (TagKind::Base, NodeType::Leaf) => {
            let hdr_len = size_of::<BaseHeader>();
            if payload.len() < hdr_len {
                return Err("base header is truncated");
            }
            let base_view = page.view().as_base();
            let base = base_view.header();
            let elems = base.elems as usize;
            let slots_end = hdr_len
                .checked_add(elems.checked_mul(size_of::<u32>()).ok_or("slot overflow")?)
                .ok_or("slot overflow")?;
            let logical_end = base.size as usize;
            if slots_end > logical_end || logical_end > payload.len() {
                return Err("base slots exceed payload");
            }
            let mut offsets = Vec::with_capacity(elems);
            for index in 0..elems {
                let off = hdr_len + index * 4;
                offsets
                    .push(u32::from_le_bytes(payload[off..off + 4].try_into().unwrap()) as usize);
            }
            let mut edges = Vec::new();
            if base.right_sibling != NULL_ADDR {
                edges.push(PageEdge::Child(base.right_sibling));
            }
            if base.merging_child != NULL_ADDR {
                edges.push(PageEdge::Child(base.merging_child));
            }
            for (index, start) in offsets.iter().copied().enumerate() {
                let end = offsets.get(index + 1).copied().unwrap_or(logical_end);
                if start < slots_end || start > end || end > logical_end {
                    return Err("leaf slot bounds are invalid");
                }
                let raw = &payload[start..end];
                let value = if header.flag == TagFlag::Sibling {
                    let ver_len = size_of::<u64>() + size_of::<u32>();
                    if raw.len() < ver_len {
                        return Err("sibling version key is truncated");
                    }
                    &raw[ver_len..]
                } else {
                    let (key_len, prefix) =
                        Varint32::decode(raw).ok_or("leaf key length is invalid")?;
                    let value_start = prefix
                        .checked_add(key_len as usize)
                        .ok_or("leaf key overflow")?;
                    if value_start > raw.len() {
                        return Err("leaf key exceeds slot");
                    }
                    &raw[value_start..]
                };
                edges.extend(
                    value_edges(value)?
                        .into_iter()
                        .map(|(kind, address)| PageEdge::Address(kind, address)),
                );
            }
            Ok(edges)
        }
        (TagKind::Base, NodeType::Intl) => {
            let hdr_len = size_of::<BaseHeader>();
            if payload.len() < hdr_len {
                return Err("base header is truncated");
            }
            let base_view = page.view().as_base();
            let base = base_view.header();
            let elems = base.elems as usize;
            let slots_end = hdr_len
                .checked_add(elems.checked_mul(size_of::<u32>()).ok_or("slot overflow")?)
                .ok_or("slot overflow")?;
            let logical_end = base.size as usize;
            if slots_end > logical_end || logical_end > payload.len() {
                return Err("base slots exceed payload");
            }
            let mut offsets = Vec::with_capacity(elems);
            for index in 0..elems {
                let off = hdr_len + index * 4;
                offsets
                    .push(u32::from_le_bytes(payload[off..off + 4].try_into().unwrap()) as usize);
            }
            let mut edges = Vec::new();
            if base.right_sibling != NULL_ADDR {
                edges.push(PageEdge::Child(base.right_sibling));
            }
            if base.merging_child != NULL_ADDR {
                edges.push(PageEdge::Child(base.merging_child));
            }
            for (index, start) in offsets.iter().copied().enumerate() {
                let end = offsets.get(index + 1).copied().unwrap_or(logical_end);
                if start < slots_end || start > end || end > logical_end {
                    return Err("internal slot bounds are invalid");
                }
                if let Some(pid) = internal_child_pid(&payload[start..end])? {
                    edges.push(PageEdge::Child(pid));
                }
            }
            Ok(edges)
        }
        _ => Ok(Vec::new()),
    }
}

/// one persisted per-file stat record, decoded for the cross-record checks
struct StatRecord {
    file_id: u64,
    bucket_id: u64,
    total_elems: u32,
    active_elems: u32,
    inactive: usize,
}

/// validates the per-bucket cross-record chain:
///
/// - every published interval must name a file that carries a stat record for the same
///   bucket and whose relocation table resolves the interval's start;
/// - every stat record's element counts must match its file's relocation table
///   (`total_elems` == relocations, `active_elems` + inactive == `total_elems`).
fn check_bucket_metadata(
    opt: &Options,
    btree: &BTree,
    checks: &mut PayloadChecks,
    report: &mut CheckReport,
) {
    let bucket_ids = match catalog_bucket_ids(btree) {
        Ok(ids) => ids,
        Err(err) => {
            report.push(
                CheckCategory::UnreadableMetadata,
                format!("cannot read the bucket catalog: {err:?}"),
            );
            return;
        }
    };
    let mut bucket_intervals = std::collections::HashMap::new();
    let mut bucket_budget = std::collections::HashMap::new();
    for kind in FileKind::ALL {
        let (stats, diagnostics) = read_stats(opt, btree, kind);
        for diagnostic in diagnostics {
            report.push_diagnostic(diagnostic);
        }
        for record in &stats.records {
            checks.mark_referenced(kind, record.file_id);
        }
        for file_id in &stats.undecodable {
            checks.mark_referenced(kind, *file_id);
        }
        for bucket_id in &bucket_ids {
            let intervals = read_intervals(btree, kind, *bucket_id, report);
            bucket_intervals.insert((kind, *bucket_id), intervals);
            let budget = stats
                .records
                .iter()
                .filter(|stat| stat.bucket_id == *bucket_id)
                .map(|stat| stat.total_elems as usize)
                .sum::<usize>();
            *bucket_budget.entry(*bucket_id).or_insert(0) += budget;
            let intervals = bucket_intervals.get(&(kind, *bucket_id)).unwrap();
            check_intervals_against_stats(opt, kind, *bucket_id, intervals, &stats, checks, report);
            if kind == FileKind::Data {
                check_page_table(opt, btree, *bucket_id, intervals, checks, report);
            }
        }
        check_stats_against_files(opt, kind, &stats.records, checks, report);
    }
    for bucket_id in bucket_ids {
        let empty_data = IntervalView::default();
        let empty_blob = IntervalView::default();
        let data = bucket_intervals
            .get(&(FileKind::Data, bucket_id))
            .unwrap_or(&empty_data);
        let blob = bucket_intervals
            .get(&(FileKind::Blob, bucket_id))
            .unwrap_or(&empty_blob);
        check_page_chains(
            opt,
            btree,
            bucket_id,
            data,
            blob,
            bucket_budget.get(&bucket_id).copied().unwrap_or(0),
            checks,
            report,
        );
    }
}

fn catalog_bucket_ids(btree: &BTree) -> Result<Vec<u64>, BTreeError> {
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

/// decodes the persisted per-file stat records of one kind, together with the diagnostics
/// the scan produced; callers that only need the file identities (the lifecycle phase,
/// which runs after the stat phase already reported them) drop the diagnostics
///
/// the returned [`StatView`] names the files whose stat record could not be decoded:
/// those records were reported here, so the interval phase must not add a second finding
/// for the same damage
fn read_stats(opt: &Options, btree: &BTree, kind: FileKind) -> (StatView, Vec<Diagnostic>) {
    let mut diagnostics = Vec::new();
    let mut undecodable = std::collections::HashSet::new();
    let bucket = match kind {
        FileKind::Data => BUCKET_DATA_STAT,
        FileKind::Blob => BUCKET_BLOB_STAT,
    };
    let mut records = Vec::new();
    let outcome = btree.view(bucket, |txn| {
        let mut iter = txn.iter();
        let mut key = Vec::new();
        let mut value = Vec::new();
        while iter.next_ref(&mut key, &mut value) {
            let inner = match PersistStat::try_decode_inner_only(&value) {
                Ok(inner) => inner,
                Err(err) => {
                    // the record is undecodable, so locate it by its key instead
                    let mut diagnostic = Diagnostic::new(
                        CheckCategory::BadStatAccounting,
                        format!("stat record is too short to decode: {err:?}"),
                    );
                    if let Ok(bytes) = <[u8; 8]>::try_from(&key[..]) {
                        diagnostic = diagnostic.with_file(u64::from_le_bytes(bytes));
                    }
                    if let Ok(bytes) = <[u8; 8]>::try_from(&key[..]) {
                        undecodable.insert(u64::from_le_bytes(bytes));
                    }
                    diagnostics.push(diagnostic);
                    continue;
                }
            };
            // a payload file that cannot be opened is reported by the payload phase, and
            // its record's mask cannot be bounded by the file, so the mask is not decoded
            // here: the stat record is kept for the identity checks and the mask-derived
            // inactive count stays zero
            let path = payload_path(opt, kind, inner.file_id);
            let Ok(file_len) = payload_file_len(opt, &path) else {
                records.push(StatRecord {
                    file_id: inner.file_id,
                    bucket_id: inner.bucket_id,
                    total_elems: inner.total_elems,
                    active_elems: inner.active_elems,
                    inactive: 0,
                });
                continue;
            };
            if MetaReader::try_new(opt.fs.as_ref(), &path).is_err() {
                records.push(StatRecord {
                    file_id: inner.file_id,
                    bucket_id: inner.bucket_id,
                    total_elems: inner.total_elems,
                    active_elems: inner.active_elems,
                    inactive: 0,
                });
                continue;
            }
            // the bitmap is sized from `total_elems`, so bound that field by what the
            // payload file can legally hold: every element occupies at least one byte
            // there. a record claiming more elements than a decodable file has bytes is
            // corrupt, and the bound keeps a corrupt count from driving a huge allocation
            if u64::from(inner.total_elems) > file_len {
                undecodable.insert(inner.file_id);
                diagnostics.push(
                    Diagnostic::new(
                        CheckCategory::BadStatAccounting,
                        format!(
                            "stat claims {} elements, the file holds {file_len} bytes",
                            inner.total_elems
                        ),
                    )
                    .with_bucket(inner.bucket_id)
                    .with_file(inner.file_id),
                );
                continue;
            }
            let mask = match PersistStat::try_decode_mask_only(&value, inner.total_elems) {
                Ok(mask) => mask,
                Err(err) => {
                    undecodable.insert(inner.file_id);
                    diagnostics.push(
                        Diagnostic::new(
                            CheckCategory::BadStatAccounting,
                            format!("stat record is unreadable: {err:?}"),
                        )
                        .with_bucket(inner.bucket_id)
                        .with_file(inner.file_id),
                    );
                    continue;
                }
            };
            records.push(StatRecord {
                file_id: inner.file_id,
                bucket_id: inner.bucket_id,
                total_elems: inner.total_elems,
                active_elems: inner.active_elems,
                inactive: mask.set_bits(inner.total_elems).len(),
            });
        }
        Ok(())
    });
    if let Err(err) = outcome {
        diagnostics.push(Diagnostic::new(
            CheckCategory::UnreadableMetadata,
            format!("cannot scan {bucket}: {err:?}"),
        ));
    }
    (
        StatView {
            records,
            undecodable,
        },
        diagnostics,
    )
}

/// the payload path of one file of one kind
fn payload_path(opt: &Options, kind: FileKind, file_id: u64) -> std::path::PathBuf {
    match kind {
        FileKind::Data => opt.data_file(file_id),
        FileKind::Blob => opt.blob_file(file_id),
    }
}

fn read_intervals(
    btree: &BTree,
    kind: FileKind,
    bucket_id: u64,
    report: &mut CheckReport,
) -> IntervalView {
    let bucket = interval_bucket_name(kind, bucket_id);
    let mut intervals = IntervalView::default();
    let outcome = btree.view(&bucket, |txn| {
        let mut iter = txn.iter();
        let mut key = Vec::new();
        let mut value = Vec::new();
        while iter.next_ref(&mut key, &mut value) {
            if value.len() < size_of::<IntervalPair>() {
                report.push_diagnostic(
                    Diagnostic::new(
                        CheckCategory::BadInterval,
                        format!(
                            "interval record carries {} bytes, expected {}",
                            value.len(),
                            size_of::<IntervalPair>()
                        ),
                    )
                    .with_bucket(bucket_id),
                );
                intervals.dropped += 1;
                continue;
            }
            intervals.pairs.push(IntervalPair::decode(&value));
        }
        Ok(())
    });
    if let Err(err) = outcome {
        report.push(
            CheckCategory::UnreadableMetadata,
            format!("cannot scan {bucket}: {err:?}"),
        );
    }
    intervals
}

/// what the checker learned about one payload file's relocation table
#[derive(Clone)]
enum FileRelocs {
    /// the table decoded, sorted by key so the lookups can binary search it. the cache hands out
    /// a shared handle: a page-table row or interval check resolves one of these per item, and
    /// copying the vector each time costs O(entries) per row
    Keys(Arc<[u64]>),
    /// the file could not be read at all, with the reason
    Unreadable(Arc<str>),
}

/// relocation keys of one payload file, cached per (kind, file id)
fn reloc_keys(
    opt: &Options,
    kind: FileKind,
    file_id: u64,
    checks: &mut PayloadChecks,
) -> FileRelocs {
    if let Some(cached) = checks.relocs.get(&(kind, file_id)) {
        // cloning the handle, not the table
        return cached.clone();
    }
    let path = match kind {
        FileKind::Data => opt.data_file(file_id),
        FileKind::Blob => opt.blob_file(file_id),
    };
    let outcome = MetaReader::try_new(opt.fs.as_ref(), &path).and_then(|mut reader| {
        reader.try_get_reloc().map(|relocs| {
            let mut keys = relocs.iter().map(|reloc| reloc.key).collect::<Vec<u64>>();
            // the lookups below binary search this vector, so it is sorted here rather than
            // trusted: a damaged table can be out of order, and that damage is reported on its
            // own by `check_relocations`, which reads the table in file order
            keys.sort_unstable();
            keys
        })
    });
    let entry = match outcome {
        Ok(keys) => FileRelocs::Keys(Arc::from(keys)),
        Err(err) => FileRelocs::Unreadable(Arc::from(
            match payload_file_len(opt, &path) {
                Ok(len) => format!("{err:?} (the file is {len} bytes)"),
                Err(sized) if sized.kind() == std::io::ErrorKind::NotFound => {
                    format!("{err:?} (the file is absent)")
                }
                Err(sized) => format!("{err:?} (the file cannot be sized: {sized})"),
            }
            .as_str(),
        )),
    };
    checks.relocs.insert((kind, file_id), entry.clone());
    entry
}

fn check_intervals_against_stats(
    opt: &Options,
    kind: FileKind,
    bucket_id: u64,
    intervals: &IntervalView,
    stats: &StatView,
    checks: &mut PayloadChecks,
    report: &mut CheckReport,
) {
    for interval in &intervals.pairs {
        let file_id = interval.file_id;
        if stats.undecodable.contains(&file_id) {
            // the stat record damage was already reported; a missing stat record must not
            // be reported again for the same file
            continue;
        }
        let Some(stat) = stats.records.iter().find(|stat| stat.file_id == file_id) else {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadInterval,
                    format!("interval names file {file_id} without a stat record"),
                )
                .with_bucket(bucket_id)
                .with_file(file_id)
                .with_address(interval.lo_addr),
            );
            continue;
        };
        if stat.bucket_id != interval.bucket_id {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadInterval,
                    format!(
                        "interval for bucket {bucket_id} names a file owned by bucket {}",
                        stat.bucket_id
                    ),
                )
                .with_bucket(bucket_id)
                .with_file(file_id)
                .with_address(interval.lo_addr),
            );
            continue;
        }
        match reloc_keys(opt, kind, file_id, checks) {
            FileRelocs::Keys(keys) => {
                if keys.binary_search(&interval.lo_addr).is_err() {
                    report.push_diagnostic(
                        Diagnostic::new(
                            CheckCategory::BadInterval,
                            format!("interval start does not resolve in file {file_id}"),
                        )
                        .with_bucket(bucket_id)
                        .with_file(file_id)
                        .with_address(interval.lo_addr),
                    );
                }
            }
            FileRelocs::Unreadable(reason) => {
                checks.report_unreadable(kind, file_id, Some(bucket_id), &reason, report);
            }
        }
    }
}

fn check_stats_against_files(
    opt: &Options,
    kind: FileKind,
    stats: &[StatRecord],
    checks: &mut PayloadChecks,
    report: &mut CheckReport,
) {
    for stat in stats {
        let keys = match reloc_keys(opt, kind, stat.file_id, checks) {
            FileRelocs::Keys(keys) => keys,
            FileRelocs::Unreadable(reason) => {
                checks.report_unreadable(kind, stat.file_id, Some(stat.bucket_id), &reason, report);
                continue;
            }
        };
        if keys.len() as u32 != stat.total_elems {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadStatAccounting,
                    format!(
                        "stat counts {} elements, the file holds {} relocations",
                        stat.total_elems,
                        keys.len()
                    ),
                )
                .with_bucket(stat.bucket_id)
                .with_file(stat.file_id),
            );
        }
        if stat.active_elems as usize + stat.inactive != stat.total_elems as usize {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadStatAccounting,
                    format!(
                        "stat active {} plus inactive {} does not reach total {}",
                        stat.active_elems, stat.inactive, stat.total_elems
                    ),
                )
                .with_bucket(stat.bucket_id)
                .with_file(stat.file_id),
            );
        }
    }
}

/// validates every retained WAL stream: segment contiguity, record framing, record
/// checksums, and the payload layout gate recovery applies before consuming a record.
///
/// A malformed record is tolerated only at the tail of the last segment of a stream and
/// only when the database's own persisted options allow WAL tail truncation - that case
/// is what recovery truncates. The persisted flag is authoritative rather than the
/// caller's `Options`, because an offline tool cannot know how the operator will open the
/// store; when the caller's flag disagrees with the persisted one the two verdicts differ,
/// and the report describes the database rather than the caller.
///
/// The checker is deliberately **stricter** than recovery for a malformed record in a
/// non-last segment: recovery truncates there and silently drops the rest of the stream,
/// while the checker reports `BadWal`, because a hole in the middle of a retained stream
/// is damage an operator must see.
fn check_wal(opt: &Options, tolerate_tail: bool, report: &mut CheckReport) {
    let log_root = opt.log_root();
    let entries = match opt.fs.read_dir(&log_root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
        Err(err) => {
            report.push(
                CheckCategory::UnreadableMetadata,
                format!("cannot list {}: {err}", log_root.display()),
            );
            return;
        }
    };
    let mut segments: Vec<(u8, u64, std::path::PathBuf)> = Vec::new();
    for path in entries {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if let Some((physical_id, file_id)) = wal_segment_name(name) {
            segments.push((physical_id, file_id, path));
        }
    }
    segments.sort_by_key(|(physical_id, file_id, _)| (*physical_id, *file_id));

    let mut previous: Option<(u8, u64)> = None;
    for (physical_id, file_id, path) in &segments {
        if let Some((previous_physical, previous_id)) = previous
            && previous_physical == *physical_id
            && *file_id != previous_id + 1
        {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadWal,
                    format!(
                        "stream {physical_id} is missing segment(s) {}..{}",
                        previous_id + 1,
                        file_id
                    ),
                )
                .with_file(*file_id),
            );
        }
        previous = Some((*physical_id, *file_id));
        let is_last = segments
            .iter()
            .filter(|(other, _, _)| other == physical_id)
            .map(|(_, id, _)| *id)
            .max()
            == Some(*file_id);
        check_wal_segment(
            opt,
            path,
            *physical_id,
            *file_id,
            is_last,
            tolerate_tail,
            report,
        );
    }
}

/// parses `wal_<group>_<seq>` / `group_wal_<seq>` into `(physical stream, file id)`
fn wal_segment_name(name: &str) -> Option<(u8, u64)> {
    if let Some(raw) = name.strip_prefix(Options::GROUP_WAL_PREFIX)
        && let Some(raw) = raw.strip_prefix(Options::SEP)
        && let Ok(file_id) = raw.parse::<u64>()
    {
        return Some((Options::SHARED_ID, file_id));
    }
    let raw = name.strip_prefix(Options::WAL_PREFIX)?;
    let raw = raw.strip_prefix(Options::SEP)?;
    let (group, file_id) = raw.split_once(Options::SEP)?;
    let group = group.parse::<u8>().ok()?;
    let file_id = file_id.parse::<u64>().ok()?;
    Some((group, file_id))
}

fn check_wal_segment(
    opt: &Options,
    path: &std::path::Path,
    physical_id: u8,
    file_id: u64,
    is_last: bool,
    tolerate_tail: bool,
    report: &mut CheckReport,
) {
    let data = match read_segment(opt, path) {
        Ok(data) => data,
        Err(err) => {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadWal,
                    format!("stream {physical_id} segment {file_id} cannot be read: {err}"),
                )
                .with_file(file_id),
            );
            return;
        }
    };
    let mut offset = 0usize;
    while offset < data.len() {
        let reason = match wal_record_len(&data, offset) {
            Ok(len) => {
                offset += len;
                continue;
            }
            Err(reason) => reason,
        };
        // recovery truncates a bad tail of the live segment, so the same damage is only
        // a finding when the stream continues after it, or when truncation is disabled
        let tolerated = is_last && tolerate_tail;
        if !tolerated {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadWal,
                    format!(
                        "stream {physical_id} segment {file_id} is invalid at offset {offset}: {reason}"
                    ),
                )
                .with_file(file_id)
                .with_address(offset as u64),
            );
        }
        return;
    }
}

/// length of the record starting at `offset`, or the reason it does not frame/checksum
fn wal_record_len(data: &[u8], offset: usize) -> Result<usize, &'static str> {
    let Some(kind) = EntryType::try_from(data[offset]).ok() else {
        return Err("unknown record type");
    };
    let Ok(header_len) = wal_record_sz(kind) else {
        return Err("unknown record size");
    };
    if offset + header_len > data.len() {
        return Err("truncated record header");
    }
    match kind {
        EntryType::Update => {
            let update = ptr_to::<WalUpdate>(unsafe { data.as_ptr().add(offset) });
            let Ok(payload_len) = WalUpdate::checked_payload_len(update.size) else {
                return Err("payload length is out of range");
            };
            let Ok(total_len) = WalUpdate::checked_encoded_len(payload_len) else {
                return Err("record length is out of range");
            };
            if offset + total_len > data.len() {
                return Err("truncated record payload");
            }
            if !update.is_intact() {
                return Err("record checksum mismatch");
            }
            // recovery refuses a record whose payload layout does not validate
            // (`PayloadType::try_from` + `WalUpdate::validate_record`), so the checker
            // must not walk past it and call the stream clean
            if PayloadType::try_from(data[offset + 1]).is_err() {
                return Err("unknown payload type");
            }
            if update
                .validate_record(&data[offset..offset + total_len])
                .is_err()
            {
                return Err("payload layout does not validate");
            }
            Ok(total_len)
        }
        EntryType::CheckPoint => {
            let checkpoint = ptr_to::<WalCheckpoint>(unsafe { data.as_ptr().add(offset) });
            if !checkpoint.is_intact() {
                return Err("checkpoint record checksum mismatch");
            }
            Ok(header_len)
        }
        EntryType::Begin | EntryType::Commit | EntryType::Abort => {
            let terminal = ptr_to::<WalAbort>(unsafe { data.as_ptr().add(offset) });
            if !terminal.is_intact() {
                return Err("terminal record checksum mismatch");
            }
            Ok(header_len)
        }
        EntryType::Unknown => Err("unknown record type"),
    }
}

fn read_segment(opt: &Options, path: &std::path::Path) -> Result<Vec<u8>, std::io::Error> {
    let file = File::options().read(true).open(opt.fs.as_ref(), path)?;
    let len = file.size()? as usize;
    let mut data = vec![0u8; len];
    let mut read = 0usize;
    while read < len {
        let got = file.read(&mut data[read..], read as u64)?;
        if got == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short segment read",
            ));
        }
        read += got;
    }
    Ok(data)
}

/// parses `data_<id>` / `blob_<id>` into the file id
fn payload_file_id(name: &str) -> Option<(FileKind, u64)> {
    for (prefix, kind) in [
        (Options::DATA_PREFIX, FileKind::Data),
        (Options::BLOB_PREFIX, FileKind::Blob),
    ] {
        if let Some(raw) = name.strip_prefix(prefix)
            && let Some(raw) = raw.strip_prefix(Options::SEP)
            && let Ok(id) = raw.parse::<u64>()
        {
            return Some((kind, id));
        }
    }
    None
}

fn check_payload_file(
    opt: &Options,
    path: &std::path::Path,
    name: &str,
    kind: FileKind,
    file_id: u64,
    checks: &mut PayloadChecks,
    report: &mut CheckReport,
) {
    let mut reader = match MetaReader::try_new(opt.fs.as_ref(), path) {
        Ok(reader) => reader,
        Err(err) => {
            // shared with the metadata phases, so one unreadable file is reported once
            checks.report_unreadable(
                kind,
                file_id,
                None,
                &format!("{name} cannot be read as a payload file: {err:?}"),
                report,
            );
            return;
        }
    };
    let file_len = match payload_file_len(opt, path) {
        Ok(len) => len,
        Err(err) => {
            // the metadata phases may already have reported this file
            checks.report_unreadable(
                kind,
                file_id,
                None,
                &format!("{name} cannot be sized: {err}"),
                report,
            );
            return;
        }
    };

    let relocs = match reader.try_get_reloc() {
        Ok(relocs) => relocs,
        Err(err) => {
            // the metadata phases read the same table through `reloc_keys`, so this damage
            // must not be reported a second time under a different wording
            checks.report_unreadable(
                kind,
                file_id,
                None,
                &format!("{name} relocation table is unreadable: {err:?}"),
                report,
            );
            return;
        }
    };
    check_relocations(name, file_id, file_len, &relocs, report);

    let intervals = match reader.try_get_interval() {
        Ok(intervals) => intervals,
        Err(err) => {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadPayloadFile,
                    format!("{name} interval table is unreadable: {err:?}"),
                )
                .with_file(file_id),
            );
            return;
        }
    };
    check_intervals(name, file_id, &intervals, &relocs, report);
}

fn payload_file_len(opt: &Options, path: &std::path::Path) -> Result<u64, std::io::Error> {
    let file = File::options().read(true).open(opt.fs.as_ref(), path)?;
    file.size()
}

/// relocations must be strictly ordered by logical address and stay inside the file
fn check_relocations(
    name: &str,
    file_id: u64,
    file_len: u64,
    relocs: &[AddrPair],
    report: &mut CheckReport,
) {
    let mut previous: Option<u64> = None;
    for reloc in relocs {
        // the tables are packed records: copy each entry out before reading fields
        let reloc = *reloc;
        let address = reloc.key;
        if let Some(previous) = previous
            && address <= previous
        {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadPayloadFile,
                    format!("{name} relocations are not strictly ordered at {address}"),
                )
                .with_file(file_id)
                .with_address(address),
            );
            return;
        }
        previous = Some(address);
        let raw_len = reloc.val.raw_len;
        if raw_len < BoxRef::DUMP_HDR_LEN as u32 {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadPayloadFile,
                    format!(
                        "{name} relocation for {address} has raw_len {raw_len} below minimum {}",
                        BoxRef::DUMP_HDR_LEN
                    ),
                )
                .with_file(file_id)
                .with_address(address),
            );
            return;
        }
        let stored = u64::from(reloc.val.active_len());
        let frame = reloc.val.off as u64;
        let end = frame.saturating_add(stored);
        if stored == 0 || end > file_len {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadPayloadFile,
                    format!(
                        "{name} relocation for {address} spans [{frame}, {end}) outside the file"
                    ),
                )
                .with_file(file_id)
                .with_address(address),
            );
            return;
        }
    }
}

/// intervals must be ordered, disjoint, and resolve to a relocation in this file
fn check_intervals(
    name: &str,
    file_id: u64,
    intervals: &[Interval],
    relocs: &[AddrPair],
    report: &mut CheckReport,
) {
    let mut previous_hi: Option<u64> = None;
    for interval in intervals {
        // packed record: copy it out before reading fields
        let interval = *interval;
        let (lo, hi) = (interval.lo, interval.hi);
        // intervals are inclusive: `[lo, hi]` with `lo == hi` a single address
        // (`IntervalMap::assert_disjoint` requires `lo <= hi`)
        if lo > hi {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadPayloadFile,
                    format!("{name} interval [{lo}, {hi}] is inverted"),
                )
                .with_file(file_id)
                .with_address(lo),
            );
            return;
        }
        if let Some(previous_hi) = previous_hi
            && lo <= previous_hi
        {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadPayloadFile,
                    format!(
                        "{name} interval [{lo}, {hi}] overlaps the previous interval ending at {previous_hi}"
                    ),
                )
                .with_file(file_id)
                .with_address(lo),
            );
            return;
        }
        previous_hi = Some(hi);
        let resolved = relocs.iter().any(|reloc| {
            let reloc = *reloc;
            reloc.key == lo
        });
        if !resolved {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::BadPayloadFile,
                    format!("{name} interval starts at an address with no relocation"),
                )
                .with_file(file_id)
                .with_address(lo),
            );
            return;
        }
    }
}

/// whether the metadata store exists but was never initialized: every metadata bucket is
/// absent, which is the state the runtime bootstraps on open
fn metadata_is_uninitialized(btree: &BTree) -> bool {
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
struct OpenErrorClass {
    detail: fn(&OpenError, &std::path::Path) -> Diagnostic,
}

/// classifies the open failures that must not be reported as damage: a store another
/// process holds, a live instance of this path in this process, and a file whose on-disk
/// format this build cannot read (the runtime refuses it too, and the fix is an offline
/// rebuild rather than a repair)
fn classify_open_error(err: &OpenError) -> Option<OpenErrorClass> {
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
/// `tests/checker.rs::metadata_page_damage_aborts_by_design`.
fn open_metadata_read_only(opt: &Options, report: &mut CheckReport) -> Option<BTree> {
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

fn check_version(btree: &BTree, report: &mut CheckReport) {
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
fn check_options(btree: &BTree, report: &mut CheckReport) -> bool {
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

fn check_bucket_catalog(btree: &BTree, report: &mut CheckReport) {
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

#[cfg(test)]
mod tests {
    use super::{CheckCategory, check_database, classify_open_error};
    use crate::RandomPath;
    use crate::io::testfs::{InjectOp, InjectedFileSystem};
    use crate::meta::IMetaCodec;
    use crate::utils::options::Options;
    use std::sync::Arc;

    /// every category keeps its published name: the CLI prints these strings
    #[test]
    fn category_names_are_stable() {
        let names: Vec<&'static str> = [
            CheckCategory::MissingMetadata,
            CheckCategory::UnreadableMetadata,
            CheckCategory::MetadataBusy,
            CheckCategory::UnsupportedMetadataFormat,
            CheckCategory::BadVersion,
            CheckCategory::BadOptions,
            CheckCategory::BadBucketCatalog,
            CheckCategory::BadPayloadFile,
            CheckCategory::BadWal,
            CheckCategory::BadInterval,
            CheckCategory::BadStatAccounting,
            CheckCategory::BadLifecycleState,
            CheckCategory::BadPageTable,
            CheckCategory::BadPageChain,
        ]
        .iter()
        .map(|category| category.name())
        .collect();
        assert_eq!(
            names,
            vec![
                "missing_metadata",
                "unreadable_metadata",
                "metadata_busy",
                "unsupported_metadata_format",
                "bad_version",
                "bad_options",
                "bad_bucket_catalog",
                "bad_payload_file",
                "bad_wal",
                "bad_interval",
                "bad_stat_accounting",
                "bad_lifecycle_state",
                "bad_page_table",
                "bad_page_chain"
            ]
        );
    }

    /// `reloc_keys` hands out a shared handle: a page-table row and an interval check resolve the
    /// same table once per item, so a copy per call would cost O(entries) per item
    #[test]
    fn relocation_cache_hands_out_shared_handles() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed(&root);
        let opt = Options::new(&*root);
        let mut checks = super::PayloadChecks::default();
        let first = super::reloc_keys(&opt, crate::meta::FileKind::Data, 4096, &mut checks);
        let second = super::reloc_keys(&opt, crate::meta::FileKind::Data, 4096, &mut checks);

        let (super::FileRelocs::Unreadable(first), super::FileRelocs::Unreadable(second)) =
            (first, second)
        else {
            panic!("a missing payload file must read as unreadable");
        };
        assert!(
            std::sync::Arc::ptr_eq(&first, &second),
            "the cache must hand out its decoded entry, not a copy"
        );
    }

    /// the cache reuses the decoded table itself: the `Keys` variant is what a page-table row and
    /// an interval check resolve once per item, so a fresh allocation per hit is the regression
    /// this guards
    #[test]
    fn relocation_cache_reuses_a_decoded_table() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let prefix = format!("{}{}", Options::DATA_PREFIX, Options::SEP);
        let mut ids: Vec<u64> = std::fs::read_dir(opt.data_root())
            .expect("read data dir")
            .flatten()
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix(prefix.as_str()))
                    .and_then(|id| id.parse::<u64>().ok())
            })
            .collect();
        ids.sort_unstable();
        let file_id = *ids.first().expect("the seed checkpoint writes a data file");

        let mut checks = super::PayloadChecks::default();
        let first = super::reloc_keys(&opt, crate::meta::FileKind::Data, file_id, &mut checks);
        let second = super::reloc_keys(&opt, crate::meta::FileKind::Data, file_id, &mut checks);

        let (super::FileRelocs::Keys(first), super::FileRelocs::Keys(second)) = (first, second)
        else {
            panic!("a written payload file must decode its relocation table");
        };
        assert!(
            !first.is_empty(),
            "the seed checkpoint must relocate the keys it wrote"
        );
        assert!(
            std::sync::Arc::ptr_eq(&first, &second),
            "the cache must reuse its decoded table instead of allocating one per hit"
        );
    }

    /// a lifecycle row the checker cannot decode must be reported: the durable recycle
    /// boundary rule it carries would otherwise disappear from the run without a diagnostic
    #[test]
    fn a_short_wal_recycle_row_is_reported() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed(&root);
        let opt = Options::new(&*root);
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        btree
            .exec(crate::meta::BUCKET_MISC, |txn| {
                txn.put(crate::meta::wal_recycle_key(3), [0u8; 4])
            })
            .expect("short recycle row");
        drop(btree);

        let report = check_database(&opt);

        assert!(
            report.diagnostics.iter().any(|diagnostic| {
                diagnostic.category == CheckCategory::UnreadableMetadata
                    && diagnostic.detail.contains("wrc_3")
            }),
            "the undecodable recycle row must be named in the report: {report:?}"
        );
    }

    /// a truncated catalog record is a diagnostic, not a panic
    #[test]
    fn a_truncated_catalog_record_is_reported() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed(&root);
        let opt = Options::new(&*root);
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        btree
            .exec(crate::meta::BUCKET_METAS, |txn| txn.put("short", [0u8; 4]))
            .expect("short record");
        drop(btree);

        let report = check_database(&opt);

        assert!(
            report
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.category == CheckCategory::BadBucketCatalog),
            "got {report:?}"
        );
    }

    /// builds an open error carrying `code`, the way the backend reports a rejected file
    fn format_error(code: &'static str, actual: &str) -> btree_store::OpenError {
        btree_store::OpenError::Corruption(btree_store::CorruptionReport {
            code,
            generation: None,
            page_kind: "meta",
            pid: None,
            check: "fixture",
            expected: None,
            actual: Some(actual.into()),
        })
    }

    /// the same live-instance error the backend raises for a second open of this path
    fn live_error() -> btree_store::OpenError {
        btree_store::OpenError::InvalidOptions(
            btree_store::OptionsError::LiveInstanceOptionsMismatch,
        )
    }

    /// the open failures that are neither absence nor plain unreadability get their own
    /// class: lock contention, and a file this build cannot read at all
    #[test]
    fn open_errors_are_classified_by_what_the_operator_must_do() {
        let path = std::path::Path::new("/db/log/manifest");
        let busy = btree_store::OpenError::DatabaseBusy {
            path: path.to_path_buf(),
        };
        let held = classify_open_error(&busy).expect("lock contention must have a class");
        let diagnostic = (held.detail)(&busy, path);
        assert_eq!(diagnostic.category, CheckCategory::MetadataBusy);
        assert!(diagnostic.detail.contains("another process"));

        let live = live_error();
        let live_class = classify_open_error(&live).expect("a live instance must have a class");
        assert_eq!(
            (live_class.detail)(&live, path).category,
            CheckCategory::MetadataBusy
        );

        for code in ["UNSUPPORTED_FORMAT_VERSION", "MIXED_FORMAT_VERSIONS"] {
            let err = format_error(code, "1");
            let class = classify_open_error(&err).expect("an unreadable format must have a class");
            let diagnostic = (class.detail)(&err, path);
            assert_eq!(
                diagnostic.category,
                CheckCategory::UnsupportedMetadataFormat
            );
            assert!(
                diagnostic.detail.contains("btree-store migrate"),
                "the finding must name the offline rebuild: {diagnostic:?}"
            );
        }

        // a plain corruption is still damage
        let corrupt = format_error("LIVE_ENGINE_CORRUPTION", "2");
        assert!(
            classify_open_error(&corrupt).is_none(),
            "a real corruption must stay unreadable metadata"
        );
    }

    /// whatever a single-byte payload corruption breaks, the file is reported at most once
    /// under the payload category: the metadata phases and the payload phase share one
    /// dedupe set, so a damaged relocation table cannot be reported twice
    #[test]
    fn a_corrupted_payload_file_is_reported_once_per_category() {
        use crate::Diagnostic;
        use crate::meta::{BUCKET_DATA_STAT, PersistStat};

        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let file_id = {
            let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
            let mut id = None;
            btree
                .view(BUCKET_DATA_STAT, |txn| {
                    let mut iter = txn.iter();
                    let (mut key, mut value) = (Vec::new(), Vec::new());
                    while iter.next_ref(&mut key, &mut value) {
                        id = Some(PersistStat::decode_inner_only(&value).file_id);
                    }
                    Ok(())
                })
                .expect("stats");
            id.expect("fixture must publish a stat record")
        };
        let path = opt.data_file(file_id);
        let original = std::fs::read(&path).expect("read payload file");
        assert!(original.len() > 8, "fixture payload file is too small");

        let mut details = std::collections::BTreeSet::new();
        for offset in 0..original.len() {
            let mut damaged = original.clone();
            damaged[offset] ^= 0xff;
            std::fs::write(&path, &damaged).expect("write damaged payload file");

            let report = check_database(&opt);
            let payload: Vec<&Diagnostic> = report
                .diagnostics
                .iter()
                .filter(|diagnostic| {
                    diagnostic.file_id == Some(file_id)
                        && diagnostic.category == CheckCategory::BadPayloadFile
                })
                .collect();
            assert!(
                payload.len() <= 1,
                "offset {offset} reported the payload file {} times: {payload:?}",
                payload.len()
            );
            if let Some(diagnostic) = payload.first() {
                details.insert(diagnostic.detail.clone());
            }
        }
        std::fs::write(&path, &original).expect("restore payload file");
        // non-vacuity: the sweep must reach both reporting paths, so the shared set is
        // exercised across phases (a metadata-phase report must silence the payload phase)
        assert!(
            details
                .iter()
                .any(|detail| detail.starts_with("payload file cannot be read:")),
            "the sweep must reach the metadata phase's payload report; saw {details:?}"
        );
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("interval table is unreadable")),
            "the sweep must reach the payload phase's own checks; saw {details:?}"
        );
        assert!(
            details.len() >= 3,
            "the sweep must cover several damage shapes; saw {details:?}"
        );
        // the payload version gate is one of them: a corrupted footer byte must be named
        assert!(
            details.iter().any(|detail| detail.contains("BadVersion")),
            "the sweep must reach the file version gate; saw {details:?}"
        );
    }

    /// a dropped interval row does not cascade into page-table coverage findings
    #[test]
    fn a_short_interval_row_does_not_cascade_into_page_table_findings() {
        use crate::Diagnostic;
        use crate::meta::{BUCKET_METAS, BucketMeta, data_interval_name};

        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        let bucket_id = {
            let mut id = None;
            btree
                .view(BUCKET_METAS, |txn| {
                    let mut iter = txn.iter();
                    let (mut key, mut value) = (Vec::new(), Vec::new());
                    while iter.next_ref(&mut key, &mut value) {
                        id = Some(BucketMeta::decode(&value).id);
                    }
                    Ok(())
                })
                .expect("catalog");
            id.expect("fixture must hold a bucket")
        };
        let bucket = data_interval_name(bucket_id);
        let key = {
            let mut key = None;
            btree
                .view(&bucket, |txn| {
                    let mut iter = txn.iter();
                    let (mut k, mut v) = (Vec::new(), Vec::new());
                    while iter.next_ref(&mut k, &mut v) {
                        key = Some(k.clone());
                    }
                    Ok(())
                })
                .expect("intervals");
            key.expect("fixture must publish an interval row")
        };
        // a row too short to decode: the interval phase reports it, the page table must not
        btree
            .exec(&bucket, |txn| txn.put(&key, [0u8; 4]))
            .expect("rewrite interval row");
        drop(btree);

        let report = check_database(&opt);

        let for_bucket: Vec<&Diagnostic> = report
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.bucket_id == Some(bucket_id))
            .collect();
        assert!(
            for_bucket.iter().any(
                |diagnostic| diagnostic.category == CheckCategory::BadInterval
                    && diagnostic.detail.contains("interval record carries")
            ),
            "the short interval row must be reported: {for_bucket:?}"
        );
        assert!(
            !for_bucket
                .iter()
                .any(|diagnostic| diagnostic.category == CheckCategory::BadPageTable),
            "a dropped interval row must not cascade into page-table findings: {for_bucket:?}"
        );
    }

    /// a payload file that metadata never names is reported: it is the file-to-stat half of
    /// the persisted accounting (`protocol.persist_stat_matches_payload_relocations`)
    #[test]
    fn an_unreferenced_payload_file_is_reported() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let source = {
            let mut found = None;
            for entry in std::fs::read_dir(opt.data_root()).expect("data dir") {
                let entry = entry.expect("entry");
                if entry.file_name().to_string_lossy().starts_with("data_") {
                    found = Some(entry.path());
                }
            }
            found.expect("fixture must write a data file")
        };
        let leaked = opt.data_file(1001);
        std::fs::copy(&source, &leaked).expect("leak a payload file");

        let report = check_database(&opt);

        let found = report
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.file_id == Some(1001))
            .unwrap_or_else(|| panic!("a leaked payload file must be reported: {report:?}"));
        assert_eq!(found.category, CheckCategory::BadLifecycleState);
        assert!(
            found.detail.contains("named by no stat record"),
            "unexpected detail: {found:?}"
        );
    }

    /// a manifest that exists but was never initialized is the bootstrap state, reported once
    #[test]
    fn an_uninitialized_manifest_is_reported_once() {
        let root = RandomPath::tmp();
        let opt = Options::new(&*root);
        std::fs::create_dir_all(opt.log_root()).expect("log root");
        // btree-store creates the manifest file on open; nothing is published into it
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        drop(btree);

        let report = check_database(&opt);

        assert_eq!(
            report.diagnostics.len(),
            1,
            "an uninitialized manifest must be one finding, not one per absent bucket: {report:?}"
        );
        assert_eq!(
            report.diagnostics[0].category,
            CheckCategory::MissingMetadata,
            "unexpected category: {report:?}"
        );
    }

    /// an undecodable stat record is reported once, not again as a missing stat record
    #[test]
    fn an_undecodable_stat_record_is_reported_once() {
        use crate::Diagnostic;
        use crate::meta::{BUCKET_DATA_STAT, PersistStat};

        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        let (key, file_id) = {
            let mut found = None;
            btree
                .view(BUCKET_DATA_STAT, |txn| {
                    let mut iter = txn.iter();
                    let (mut key, mut value) = (Vec::new(), Vec::new());
                    while iter.next_ref(&mut key, &mut value) {
                        found = Some((key.clone(), PersistStat::decode_inner_only(&value).file_id));
                    }
                    Ok(())
                })
                .expect("stats");
            found.expect("fixture must publish a stat record")
        };
        // a body that cannot hold the fixed header and body
        btree
            .exec(BUCKET_DATA_STAT, |txn| txn.put(&key, [0u8; 4]))
            .expect("rewrite stat");
        drop(btree);

        let report = check_database(&opt);

        let for_file: Vec<&Diagnostic> = report
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.file_id == Some(file_id))
            .collect();
        assert_eq!(
            for_file.len(),
            1,
            "an undecodable stat record must be reported once: {for_file:?}"
        );
        assert_eq!(
            for_file[0].category,
            CheckCategory::BadStatAccounting,
            "unexpected category: {for_file:?}"
        );
    }

    /// a present but unreadable payload file is reported once, under one category
    #[test]
    fn a_truncated_payload_file_is_reported_once() {
        use crate::Diagnostic;
        use crate::meta::{BUCKET_DATA_STAT, PersistStat};

        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let file_id = {
            let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
            let mut id = None;
            btree
                .view(BUCKET_DATA_STAT, |txn| {
                    let mut iter = txn.iter();
                    let (mut key, mut value) = (Vec::new(), Vec::new());
                    while iter.next_ref(&mut key, &mut value) {
                        id = Some(PersistStat::decode_inner_only(&value).file_id);
                    }
                    Ok(())
                })
                .expect("stats");
            id.expect("fixture must publish a stat record")
        };
        // a present file whose header cannot be read
        std::fs::write(opt.data_file(file_id), [0u8; 16]).expect("truncate the payload file");

        let report = check_database(&opt);

        let for_file: Vec<&Diagnostic> = report
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.file_id == Some(file_id))
            .collect();
        assert_eq!(
            for_file.len(),
            1,
            "an unreadable payload file must be reported once: {for_file:?}"
        );
        assert_eq!(for_file[0].category, CheckCategory::BadPayloadFile);
    }

    /// a page table entry that no interval covers is reported as a page-table defect
    #[test]
    fn a_page_table_entry_without_an_interval_is_reported() {
        use crate::meta::{BUCKET_METAS, BucketMeta, page_table_name};

        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        let bucket_id = {
            let mut id = None;
            btree
                .view(BUCKET_METAS, |txn| {
                    let mut iter = txn.iter();
                    let (mut key, mut value) = (Vec::new(), Vec::new());
                    while iter.next_ref(&mut key, &mut value) {
                        id = Some(BucketMeta::decode(&value).id);
                    }
                    Ok(())
                })
                .expect("catalog");
            id.expect("fixture must hold a bucket")
        };
        let bucket = page_table_name(bucket_id);
        let rows = {
            let mut rows = 0usize;
            btree
                .view(&bucket, |txn| {
                    let mut iter = txn.iter();
                    let (mut key, mut value) = (Vec::new(), Vec::new());
                    while iter.next_ref(&mut key, &mut value) {
                        rows += 1;
                    }
                    Ok(())
                })
                .expect("page table");
            rows
        };
        assert!(rows > 0, "fixture must publish page table rows");
        // a mapped address far above every interval of the bucket
        btree
            .exec(&bucket, |txn| {
                txn.put(999_999u64.to_le_bytes(), (u64::MAX - 1).to_le_bytes())
            })
            .expect("rewrite page table");
        drop(btree);

        let report = check_database(&opt);

        let found = report
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.category == CheckCategory::BadPageTable)
            .unwrap_or_else(|| panic!("a page table defect must be reported: {report:?}"));
        assert!(
            found.detail.contains("no interval covers"),
            "unexpected detail: {found:?}"
        );
    }

    /// an unreadable payload file is reported once, under the payload category
    #[test]
    fn an_unreadable_payload_file_is_reported_once() {
        use crate::Diagnostic;
        use crate::meta::{BUCKET_DATA_STAT, PersistStat};

        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let file_id = {
            let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
            let mut id = None;
            btree
                .view(BUCKET_DATA_STAT, |txn| {
                    let mut iter = txn.iter();
                    let (mut key, mut value) = (Vec::new(), Vec::new());
                    while iter.next_ref(&mut key, &mut value) {
                        id = Some(PersistStat::decode_inner_only(&value).file_id);
                    }
                    Ok(())
                })
                .expect("stats");
            id.expect("fixture must publish a stat record")
        };
        let path = opt.data_file(file_id);
        std::fs::remove_file(&path).expect("remove the payload file");

        let report = check_database(&opt);

        let for_file: Vec<&Diagnostic> = report
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.file_id == Some(file_id))
            .collect();
        let payload: Vec<&&Diagnostic> = for_file
            .iter()
            .filter(|diagnostic| diagnostic.category == CheckCategory::BadPayloadFile)
            .collect();
        assert_eq!(
            payload.len(),
            1,
            "an unreadable payload file must be reported exactly once: {for_file:?}"
        );
        // the damage must not be re-reported under the interval or accounting rules
        assert!(
            !for_file.iter().any(|diagnostic| matches!(
                diagnostic.category,
                CheckCategory::BadInterval | CheckCategory::BadStatAccounting
            )),
            "an unreadable file must not be reported as interval/accounting damage: {for_file:?}"
        );
    }

    /// lifecycle records that contradict the metadata they belong to are reported
    #[test]
    fn lifecycle_contradictions_are_reported() {
        use crate::meta::{
            BUCKET_FRONTIER, BUCKET_MISC, BUCKET_OBSOLETE_DATA, BUCKET_PENDING_DEL, BucketMeta,
            WalRecycleState, orphan_data_marker_key, wal_recycle_key,
        };
        use crate::types::traits::IAsSlice;

        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        let bucket_id = {
            let mut id = None;
            btree
                .view(crate::meta::BUCKET_METAS, |txn| {
                    let mut iter = txn.iter();
                    let (mut key, mut value) = (Vec::new(), Vec::new());
                    while iter.next_ref(&mut key, &mut value) {
                        id = Some(BucketMeta::decode(&value).id);
                    }
                    Ok(())
                })
                .expect("catalog");
            id.expect("fixture must hold a bucket")
        };
        let file_id = {
            let mut id = None;
            btree
                .view(crate::meta::BUCKET_DATA_STAT, |txn| {
                    let mut iter = txn.iter();
                    let (mut key, mut value) = (Vec::new(), Vec::new());
                    while iter.next_ref(&mut key, &mut value) {
                        id = Some(crate::meta::PersistStat::decode_inner_only(&value).file_id);
                    }
                    Ok(())
                })
                .expect("stats");
            id.expect("fixture must publish a stat record")
        };
        let frontier = btree
            .view(BUCKET_FRONTIER, |txn| txn.get(bucket_id.to_le_bytes()))
            .expect("frontier record");

        // 1. an orphan marker naming a live stat file
        btree
            .exec(BUCKET_MISC, |txn| {
                txn.put(orphan_data_marker_key(file_id), [])
            })
            .expect("orphan marker");
        // 2. an obsolete entry naming the same live file
        btree
            .exec(BUCKET_OBSOLETE_DATA, |txn| {
                txn.put(file_id.to_le_bytes(), [])
            })
            .expect("obsolete entry");
        // 3. a pending-delete bucket that is still in the catalog
        btree
            .exec(BUCKET_PENDING_DEL, |txn| {
                txn.put(bucket_id.to_le_bytes(), [])
            })
            .expect("pending delete");
        // 4. a frontier that outlives its bucket
        btree
            .exec(BUCKET_FRONTIER, |txn| {
                txn.put(9_999_999u64.to_le_bytes(), &frontier)
            })
            .expect("stray frontier");
        // 5. a recycle boundary that does not match the oldest segment
        let oldest = std::fs::read_dir(opt.log_root())
            .expect("log root")
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix("group_wal_"))
                    .and_then(|raw| raw.parse::<u64>().ok())
            })
            .min()
            .expect("fixture must write a shared segment");
        btree
            .exec(BUCKET_MISC, |txn| {
                txn.put(
                    wal_recycle_key(Options::SHARED_ID),
                    WalRecycleState::done(Options::SHARED_ID, oldest, oldest + 3).as_slice(),
                )
            })
            .expect("recycle state");
        drop(btree);

        let report = check_database(&opt);

        let lifecycle: Vec<&str> = report
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.category == CheckCategory::BadLifecycleState)
            .map(|diagnostic| diagnostic.detail.as_str())
            .collect();
        for expected in [
            "orphan marker",
            "obsolete entry",
            "pending-delete bucket",
            "outlives its bucket",
            "recycle boundary",
        ] {
            assert!(
                lifecycle.iter().any(|detail| detail.contains(expected)),
                "expected a {expected} finding, got {lifecycle:?}"
            );
        }
        // the catalog bucket keeps its own frontier: only the stray one is reported
        assert!(
            !lifecycle
                .iter()
                .any(|detail| detail.contains("no durable frontier")),
            "a catalog bucket with a frontier must not be reported: {lifecycle:?}"
        );
        // the fixture's own bucket keeps a frontier record for the stray comparison
        assert!(!frontier.is_empty());
    }

    /// a WAL record whose payload layout recovery refuses is reported even though its
    /// checksum is intact
    #[test]
    fn a_wal_record_with_an_invalid_payload_layout_is_reported() {
        use crate::cc::wal::{IWalCodec, PayloadType, WalUpdate, ptr_to};

        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data_strict_wal(&root);
        let opt = Options::new(&*root);
        // craft `klen = 2` over a one-byte key: the header/CRC stay self-consistent,
        // while recovery's `validate_record` rejects the value framing
        let key = b"k";
        let value = b"v";
        let payload_len = key.len() + size_of::<u32>() + value.len();
        let header_len = WalUpdate::size();
        let total_len = header_len + payload_len;
        let mut record = vec![0u8; total_len];
        {
            // write the header fields through raw pointers: the record is packed
            let base = record.as_mut_ptr();
            let write = |offset: usize, bytes: &[u8]| unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), base.add(offset), bytes.len());
            };
            write(
                std::mem::offset_of!(WalUpdate, wal_type),
                &[crate::cc::wal::EntryType::Update as u8],
            );
            write(
                std::mem::offset_of!(WalUpdate, sub_type),
                &[PayloadType::Insert as u8],
            );
            write(
                std::mem::offset_of!(WalUpdate, size),
                &(payload_len as u32).to_le_bytes(),
            );
            write(std::mem::offset_of!(WalUpdate, klen), &2u32.to_le_bytes());
            write(std::mem::offset_of!(WalUpdate, txid), &1u64.to_le_bytes());
        }
        record[header_len..header_len + key.len()].copy_from_slice(key);
        let prefix = header_len + key.len();
        record[prefix..prefix + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
        record[prefix + 4..].copy_from_slice(value);
        {
            let header = ptr_to::<WalUpdate>(record.as_ptr());
            let checksum = header.calc_checksum();
            let slot = record
                .as_mut_ptr()
                .wrapping_add(std::mem::offset_of!(WalUpdate, checksum))
                as *mut u32;
            unsafe { slot.write_unaligned(checksum) };
        }
        assert!(
            ptr_to::<WalUpdate>(record.as_ptr()).is_intact(),
            "the crafted record must carry a valid checksum"
        );
        let segment = opt.group_wal_file(1);
        std::fs::write(&segment, &record).expect("write crafted segment");

        let mut strict = Options::new(&*root);
        strict.truncate_corrupted_wal = false;
        let report = check_database(&strict);

        assert!(
            report.diagnostics.iter().any(|diagnostic| {
                diagnostic.category == CheckCategory::BadWal
                    && diagnostic.detail.contains("payload layout")
            }),
            "an invalid payload layout must be reported: {report:?}"
        );
    }

    /// a checksum mismatch inside a well-framed record is reported (the damage the
    /// tail rule must not hide)
    #[test]
    fn a_wal_checksum_mismatch_is_reported() {
        use crate::cc::wal::{EntryType, WalUpdate, ptr_to, wal_record_sz};

        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data_strict_wal(&root);
        let opt = Options::new(&*root);
        let segment = std::fs::read_dir(opt.log_root())
            .expect("log root")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("group_wal_"))
            })
            .expect("fixture must write a shared wal segment");
        let mut data = std::fs::read(&segment).expect("read segment");
        // walk to the first Update record and damage its payload: framing stays valid,
        // so only the checksum check can catch it
        let mut offset = 0usize;
        let mut victim = None;
        while offset < data.len() {
            let Ok(kind) = EntryType::try_from(data[offset]) else {
                break;
            };
            let Ok(header_len) = wal_record_sz(kind) else {
                break;
            };
            if offset + header_len > data.len() {
                break;
            }
            match kind {
                EntryType::Update => {
                    let update = ptr_to::<WalUpdate>(unsafe { data.as_ptr().add(offset) });
                    let Ok(payload_len) = WalUpdate::checked_payload_len(update.size) else {
                        break;
                    };
                    let Ok(total_len) = WalUpdate::checked_encoded_len(payload_len) else {
                        break;
                    };
                    if offset + total_len > data.len() {
                        break;
                    }
                    victim = Some(offset + header_len);
                    break;
                }
                _ => offset += header_len,
            }
        }
        let victim = victim.expect("fixture must hold an update record");
        data[victim] ^= 0xFF;
        std::fs::write(&segment, &data).expect("damage payload");

        let mut strict = Options::new(&*root);
        strict.truncate_corrupted_wal = false;
        let report = check_database(&strict);

        assert!(
            report.diagnostics.iter().any(|diagnostic| {
                diagnostic.category == CheckCategory::BadWal
                    && diagnostic.detail.contains("checksum mismatch")
            }),
            "a checksum mismatch must be reported as such: {report:?}"
        );
    }

    /// the tail tolerance is last-segment-only: a bad record in an earlier segment of
    /// the same stream is reported even with truncation enabled
    #[test]
    fn a_bad_record_in_a_non_last_segment_is_reported() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let segment = std::fs::read_dir(opt.log_root())
            .expect("log root")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("group_wal_"))
            })
            .expect("fixture must write a shared wal segment");
        let file_id: u64 = segment
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("group_wal_"))
            .and_then(|raw| raw.parse().ok())
            .expect("segment name carries its id");
        // a healthy later segment keeps the damaged one from being the stream tail
        std::fs::copy(&segment, opt.group_wal_file(file_id + 1)).expect("copy segment");
        let mut data = std::fs::read(&segment).expect("read segment");
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        std::fs::write(&segment, &data).expect("damage first segment tail");

        // truncation is enabled, so only the "not the last segment" rule can report it
        let report = check_database(&Options::new(&*root));

        assert!(
            report
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.category == CheckCategory::BadWal),
            "a bad record before the stream tail must be reported: {report:?}"
        );
    }

    /// an interval that names a file without a stat record is reported
    #[test]
    fn an_interval_without_a_stat_record_is_reported() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed(&root);
        let opt = Options::new(&*root);
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        let bucket_id = {
            let mut id = None;
            btree
                .view(crate::meta::BUCKET_METAS, |txn| {
                    let mut iter = txn.iter();
                    let mut key = Vec::new();
                    let mut value = Vec::new();
                    while iter.next_ref(&mut key, &mut value) {
                        id = Some(crate::meta::BucketMeta::decode(&value).id);
                    }
                    Ok(())
                })
                .expect("catalog");
            id.expect("fixture must hold a bucket")
        };
        let interval_bucket =
            crate::meta::interval_bucket_name(crate::meta::FileKind::Data, bucket_id);
        let (key, mut interval) = {
            let mut found = None;
            btree
                .view(&interval_bucket, |txn| {
                    let mut iter = txn.iter();
                    let mut key = Vec::new();
                    let mut value = Vec::new();
                    while iter.next_ref(&mut key, &mut value) {
                        found = Some((key.clone(), crate::meta::IntervalPair::decode(&value)));
                    }
                    Ok(())
                })
                .expect("interval bucket");
            found.expect("fixture must publish an interval")
        };
        interval.file_id = 999_999;
        let mut encoded = vec![0u8; size_of::<crate::meta::IntervalPair>()];
        crate::meta::IMetaCodec::encode(&interval, &mut encoded);
        btree
            .exec(&interval_bucket, |txn| txn.put(&key, &encoded))
            .expect("rewrite interval");
        drop(btree);

        let report = check_database(&opt);

        let diagnostic = report
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.category == CheckCategory::BadInterval)
            .expect("a dangling interval must be reported");
        assert_eq!(diagnostic.file_id, Some(999_999));
        assert_eq!(diagnostic.bucket_id, Some(bucket_id));
    }

    /// a stat record whose element count disagrees with its file is reported
    #[test]
    fn a_stat_count_mismatch_is_reported() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed(&root);
        let opt = Options::new(&*root);
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        let (key, mut value) = {
            let mut found = None;
            btree
                .view(crate::meta::BUCKET_DATA_STAT, |txn| {
                    let mut iter = txn.iter();
                    let mut key = Vec::new();
                    let mut value = Vec::new();
                    while iter.next_ref(&mut key, &mut value) {
                        found = Some((key.clone(), value.clone()));
                    }
                    Ok(())
                })
                .expect("stat bucket");
            found.expect("fixture must publish a stat record")
        };
        // the element counts live in `StatInner`, right after the fixed header
        let inner_offset = crate::meta::PersistStat::stat_inner_offset();
        let total_offset = inner_offset + std::mem::offset_of!(crate::meta::StatInner, total_elems);
        value[total_offset] = value[total_offset].wrapping_add(7);
        btree
            .exec(crate::meta::BUCKET_DATA_STAT, |txn| txn.put(&key, &value))
            .expect("rewrite stat");
        drop(btree);

        let report = check_database(&opt);

        // pin the reloc-count comparison itself: the detail only this check emits
        assert!(
            report.diagnostics.iter().any(|diagnostic| {
                diagnostic.category == CheckCategory::BadStatAccounting
                    && diagnostic.detail.contains("the file holds")
            }),
            "a stat count mismatch must be reported: {report:?}"
        );
    }

    /// a duplicated bucket id is located by bucket id and the report is ordered
    #[test]
    fn a_duplicated_bucket_id_is_located_and_ordered() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed(&root);
        let opt = Options::new(&*root);
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        let encoded = btree
            .view(crate::meta::BUCKET_METAS, |txn| txn.get("x"))
            .expect("seeded bucket record");
        let expected_id = crate::meta::BucketMeta::decode(&encoded).id;
        btree
            .exec(crate::meta::BUCKET_METAS, |txn| txn.put("dup", &encoded))
            .expect("duplicate record");
        drop(btree);

        let report = check_database(&opt);

        let duplicate = report
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.detail.contains("reuses bucket id"))
            .expect("duplicate id must be reported");
        assert_eq!(duplicate.bucket_id, Some(expected_id));
        assert_eq!(report.exit_code(), 1);
        assert_eq!(report.summary(), vec![("bad_bucket_catalog", 1)]);

        let mut again = check_database(&opt);
        assert_eq!(
            report.diagnostics, again.diagnostics,
            "two runs over the same damage must report the same order"
        );
        again.sort();
        assert_eq!(report.diagnostics, again.diagnostics);
    }

    /// seeds a store with committed records, so its WAL holds Update records
    fn seed_with_data(root: &std::path::Path) {
        let mut opt = Options::new(root);
        opt.sync_on_write = true;
        let mace = crate::Mace::new(opt.validate().expect("seed options")).expect("seed open");
        let db = mace
            .new_bucket("x", Default::default())
            .expect("seed bucket");
        for index in 0..64u32 {
            let tx = db.begin().expect("seed begin");
            tx.put(format!("k_{index:04}"), vec![b'v'; 128])
                .expect("seed put");
            tx.commit().expect("seed commit");
        }
        db.checkpoint_and_wait();
    }

    /// like `seed_with_data`, but the database itself refuses to truncate a corrupt
    /// wal tail; damage tests need the persisted option to be the strict one
    fn seed_with_data_strict_wal(root: &std::path::Path) {
        let mut opt = Options::new(root);
        opt.sync_on_write = true;
        opt.truncate_corrupted_wal = false;
        let mace = crate::Mace::new(opt.validate().expect("seed options")).expect("seed open");
        let db = mace
            .new_bucket("x", Default::default())
            .expect("seed bucket");
        for index in 0..64u32 {
            let tx = db.begin().expect("seed begin");
            tx.put(format!("k_{index:04}"), vec![b'v'; 128])
                .expect("seed put");
            tx.commit().expect("seed commit");
        }
        db.checkpoint_and_wait();
    }

    fn seed(root: &std::path::Path) {
        let mut opt = Options::new(root);
        opt.sync_on_write = true;
        let mace = crate::Mace::new(opt.validate().expect("seed options")).expect("seed open");
        let _ = mace
            .new_bucket("x", Default::default())
            .expect("seed bucket");
    }

    #[test]
    fn a_missing_metadata_store_is_reported_and_not_created() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        let opt = Options::new(&*root);

        let report = check_database(&opt);

        assert_eq!(report.diagnostics.len(), 1);
        assert_eq!(
            report.diagnostics[0].category,
            CheckCategory::MissingMetadata
        );
        assert!(
            !opt.manifest().exists(),
            "the checker must never create the metadata store"
        );
    }

    #[test]
    fn a_check_on_an_absent_root_creates_nothing() {
        let scratch = RandomPath::tmp();
        std::fs::create_dir_all(&*scratch).expect("scratch dir");
        // the root itself does not exist: `create_dir` (the only path that records
        // CreateDirAll) would have to run if the checker validated its options
        let root = scratch.join("absent");
        let fs = Arc::new(InjectedFileSystem::new());
        let mut opt = Options::new(&root);
        opt.fs = fs.clone();

        let report = check_database(&opt);

        assert_eq!(report.diagnostics.len(), 1, "got {report:?}");
        assert_eq!(
            report.diagnostics[0].category,
            CheckCategory::MissingMetadata
        );
        assert!(
            fs.calls()
                .iter()
                .all(|(op, _)| *op != InjectOp::CreateDirAll),
            "a check run must not create directories: {:?}",
            fs.calls()
        );
        assert!(
            !root.exists(),
            "a check run must not create the database root"
        );
    }

    #[test]
    fn the_checker_opens_the_metadata_store_read_only() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        // seed a metadata store by opening a real engine once
        {
            let mut seed = Options::new(&*root);
            seed.sync_on_write = true;
            let mace = crate::Mace::new(seed.validate().expect("seed options")).expect("seed open");
            let _ = mace
                .new_bucket("x", Default::default())
                .expect("seed bucket");
        }

        let fs = Arc::new(InjectedFileSystem::new());
        let mut opt = Options::new(&*root);
        opt.fs = fs.clone();
        let report = check_database(&opt);
        assert!(
            report.is_ok(),
            "a healthy store must check clean: {report:?}"
        );

        let writes = fs
            .calls()
            .into_iter()
            .filter(|(op, _)| {
                matches!(
                    op,
                    InjectOp::RemoveFile
                        | InjectOp::RemoveDirAll
                        | InjectOp::Rename
                        | InjectOp::HardLink
                        | InjectOp::CopyFile
                        | InjectOp::CreateDirAll
                )
            })
            .count();
        assert_eq!(writes, 0, "a check run must not perform namespace writes");
    }
}
