//! Offline consistency and fixed-root leak checking of a mace database directory.
//!
//! The checker never constructs a [`mace::Mace`] instance: opening one runs
//! writeback recovery (orphan cleanup, persisted-option writeback, epoch wipe) and
//! starts the background services, none of which a checker may do. It reads the
//! metadata store and the payload/WAL files directly and reports what it finds.
//!
//! `check` and detection-only `leak-check` use read-only metadata access. Leak repair
//! holds the exclusive metadata lock and only appends inactive marks to stat records,
//! rebuilding their counts in one atomic transaction before a complete recheck. It
//! requires a readable fixed-root closure and no updates beyond durable frontiers;
//! unknown ownership or root-external mappings prevent classification and repair.
//! Payloads, page tables, lifecycle metadata and WAL are never repaired.
//!
//! Scope: manifest-level validation (version, global
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

mod leak;
mod lifecycle;
mod payload;
mod stats;
mod wal;

use btree_store::BTree;
use mace::maintenance::Options;

use leak::{RepairPlan, classify_database_leaks};
use lifecycle::{
    check_bucket_catalog, check_lifecycle, check_options, check_version, metadata_is_uninitialized,
    open_metadata_read_only,
};
use payload::{PayloadChecks, check_payload_files};
use stats::check_bucket_metadata;
use wal::check_wal;

pub use leak::leak_check_database;
#[cfg(feature = "extra_check")]
pub use leak::leak_check_reachable_for_test;

#[cfg(test)]
use leak::{prepare_repairs, write_repairs};
#[cfg(test)]
use lifecycle::classify_open_error;
#[cfg(test)]
use payload::{FileRelocs, reloc_keys, value_edges};

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
    /// unreachable retained records are missing from the inactive ledger
    JunkLeak,
    /// the persisted reference closure cannot be established safely
    LeakUndecidable,
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
            Self::JunkLeak => "junk_leak",
            Self::LeakUndecidable => "leak_undecidable",
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

/// one stat record changed by a successful leak repair
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairRecord {
    pub bucket_id: u64,
    pub file_id: u64,
    pub kind: &'static str,
    pub added_inactive: usize,
    pub active_elems: (u32, u32),
    pub active_size: (usize, usize),
}

/// final check result and any successfully committed leak repairs
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LeakReport {
    pub report: CheckReport,
    pub repairs: Vec<RepairRecord>,
    pub fault: Option<String>,
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
    run_checks(opt, &btree, false).0
}

fn run_checks(opt: &Options, btree: &BTree, leak: bool) -> (CheckReport, Vec<RepairPlan>) {
    let mut report = CheckReport::default();
    if metadata_is_uninitialized(btree) {
        // btree-store creates the manifest file on open and the runtime treats the absence
        // of every metadata bucket as the bootstrap state
        // the metadata is uninitialized, so report this state once
        // reported once instead of once per absent bucket
        report.push(
            CheckCategory::MissingMetadata,
            "the metadata store carries no version, options or bucket catalog yet".to_string(),
        );
        report.sort();
        return (report, Vec::new());
    }
    check_version(btree, &mut report);
    let version_readable = report.is_ok();
    let tolerate_tail = check_options(btree, &mut report);
    check_bucket_catalog(btree, &mut report);
    let mut checks = PayloadChecks::default();
    check_bucket_metadata(opt, btree, &mut checks, &mut report);
    let mut lifecycle = check_lifecycle(opt, btree, &mut checks, &mut report);
    lifecycle.healthy &= version_readable;
    check_payload_files(opt, &mut checks, &mut report);
    let wal = check_wal(opt, tolerate_tail, &lifecycle.frontiers, &mut report);
    let repairs = if leak {
        classify_database_leaks(opt, btree, &lifecycle, &wal, &mut checks, &mut report)
    } else {
        Vec::new()
    };
    report.sort();
    (report, repairs)
}

#[cfg(test)]
mod tests {
    use mace::Options;
    use mace::RandomPath;
    use mace::maintenance::{AddrPair, FileKind, IMetaCodec, PersistStat, StatInner};
    #[cfg(feature = "extra_check")]
    use mace::testing::testfs::{InjectOp, InjectedFileSystem};
    use std::collections::{BTreeSet, HashSet};
    #[cfg(feature = "extra_check")]
    use std::sync::Arc;

    use super::leak::{classify_leaks, leak_bad_stat};
    use super::payload::payload_path;
    use super::stats::{StatRecord, StatView};
    use super::{CheckCategory, check_database, classify_open_error};

    #[test]
    fn one_overmarked_live_record_and_one_unmarked_dead_record_cannot_become_obsolete() {
        use super::*;
        use mace::maintenance::IAsSlice;
        for kind in FileKind::ALL {
            let root = RandomPath::tmp();
            let opt = Options::new(&*root);
            let path = payload_path(&opt, kind, 1);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let mut bytes = Vec::new();
            for (seq, addr) in [10, 20].into_iter().enumerate() {
                bytes.extend_from_slice(AddrPair::new(addr, 0, 1, 0, seq as u32, 0).as_slice());
            }
            let mut footer = [0u8; 24];
            footer[..2].copy_from_slice(&1u16.to_le_bytes());
            footer[8..12].copy_from_slice(&2u32.to_le_bytes());
            footer[16..20].copy_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
            bytes.extend_from_slice(&footer);
            std::fs::write(path, bytes).unwrap();
            let stat = PersistStat {
                inner: StatInner {
                    file_id: 1,
                    bucket_id: 1,
                    up1: 5,
                    up2: 7,
                    total_elems: 2,
                    active_elems: 1,
                    total_size: 2,
                    active_size: 1,
                },
                inactive_elems: vec![0],
            };
            let mut raw = vec![0; stat.packed_size()];
            stat.encode(&mut raw);
            let mask = PersistStat::try_decode_mask_only(&raw, 2).unwrap();
            let view = StatView {
                records: vec![StatRecord {
                    inner: stat.inner,
                    raw,
                    mask: Some(mask),
                    inactive: 1,
                }],
                ..Default::default()
            };
            let mut report = CheckReport::default();
            let plans = classify_leaks(
                &opt,
                1,
                kind,
                &view,
                &BTreeSet::from([10]),
                &HashSet::new(),
                &mut report,
            )
            .unwrap();
            assert!(plans.is_empty());
            assert!(
                report
                    .diagnostics
                    .iter()
                    .any(|d| d.category == CheckCategory::JunkLeak)
            );
            assert!(
                report
                    .diagnostics
                    .iter()
                    .any(|d| d.category == CheckCategory::BadStatAccounting)
            );
        }
    }

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
            CheckCategory::JunkLeak,
            CheckCategory::LeakUndecidable,
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
                "bad_page_chain",
                "junk_leak",
                "leak_undecidable"
            ]
        );
    }

    #[test]
    fn flagged_null_value_references_are_rejected() {
        let mut history = vec![0; 20];
        history[0] = 0x80;
        assert_eq!(
            super::value_edges(&history),
            Err("history reference has a NULL address")
        );

        let mut remote = vec![0; 14];
        remote[0] = 0x10;
        assert_eq!(
            super::value_edges(&remote),
            Err("remote reference has a NULL address")
        );
    }

    /// `reloc_keys` hands out a shared handle: a page-table row and an interval check resolve the
    /// same table once per item, so a copy per call would cost O(entries) per item
    #[test]
    fn relocation_cache_hands_out_shared_handles() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let mut checks = super::PayloadChecks::default();
        let first = super::reloc_keys(&opt, mace::maintenance::FileKind::Data, 4096, &mut checks);
        let second = super::reloc_keys(&opt, mace::maintenance::FileKind::Data, 4096, &mut checks);

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
        let first = super::reloc_keys(
            &opt,
            mace::maintenance::FileKind::Data,
            file_id,
            &mut checks,
        );
        let second = super::reloc_keys(
            &opt,
            mace::maintenance::FileKind::Data,
            file_id,
            &mut checks,
        );

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
            .exec(mace::maintenance::BUCKET_MISC, |txn| {
                txn.put(mace::maintenance::wal_recycle_key(3), [0u8; 4])
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
            .exec(mace::maintenance::BUCKET_METAS, |txn| {
                txn.put("short", [0u8; 4])
            })
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
        use mace::maintenance::{BUCKET_DATA_STAT, PersistStat};

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
        use mace::maintenance::{BUCKET_METAS, BucketMeta, data_interval_name};

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
        use mace::maintenance::{BUCKET_DATA_STAT, PersistStat};

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
        use mace::maintenance::{BUCKET_DATA_STAT, PersistStat};

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

    /// a payload footer that names a version this binary does not read is a payload problem:
    /// the file is reported under `BadPayloadFile`, not as a metadata version problem
    #[test]
    fn a_forged_payload_footer_version_is_a_bad_payload_file() {
        use crate::Diagnostic;
        use mace::maintenance::{BUCKET_DATA_STAT, PersistStat};

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
        let mut forged = std::fs::read(&path).expect("read payload file");
        let footer = forged.len() - 24;
        forged[footer..footer + 2].copy_from_slice(&2u16.to_le_bytes());
        std::fs::write(&path, &forged).expect("write forged payload file");

        let report = check_database(&opt);

        let for_file: Vec<&Diagnostic> = report
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.file_id == Some(file_id))
            .collect();
        assert_eq!(
            for_file.len(),
            1,
            "an unreadable payload version must be reported once: {for_file:?}"
        );
        assert_eq!(for_file[0].category, CheckCategory::BadPayloadFile);
        assert!(
            for_file[0].detail.contains("payload file cannot be read"),
            "unexpected detail: {for_file:?}"
        );
        assert!(
            !report
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.category == CheckCategory::BadVersion),
            "a payload footer version is not the metadata version gate: {:?}",
            report.diagnostics
        );
    }

    /// a page table entry that no interval covers is reported as a page-table defect
    #[test]
    fn a_page_table_entry_without_an_interval_is_reported() {
        use mace::maintenance::{BUCKET_METAS, BucketMeta, page_table_name};

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
        use mace::maintenance::{BUCKET_DATA_STAT, PersistStat};

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
        use mace::maintenance::IAsSlice;
        use mace::maintenance::{
            BUCKET_FRONTIER, BUCKET_MISC, BUCKET_OBSOLETE_DATA, BUCKET_PENDING_DEL, BucketMeta,
            WalRecycleState, orphan_data_marker_key, wal_recycle_key,
        };

        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        let bucket_id = {
            let mut id = None;
            btree
                .view(mace::maintenance::BUCKET_METAS, |txn| {
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
                .view(mace::maintenance::BUCKET_DATA_STAT, |txn| {
                    let mut iter = txn.iter();
                    let (mut key, mut value) = (Vec::new(), Vec::new());
                    while iter.next_ref(&mut key, &mut value) {
                        id =
                            Some(mace::maintenance::PersistStat::decode_inner_only(&value).file_id);
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
        use mace::maintenance::{IWalCodec, PayloadType, WalUpdate, ptr_to};

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
                &[mace::maintenance::EntryType::Update as u8],
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

    #[test]
    fn a_checksum_valid_out_of_range_group_id_is_reported() {
        use mace::maintenance::{EntryType, WalUpdate, ptr_to, wal_record_sz};

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
        let mut offset = 0usize;
        let mut changed = false;
        while offset < data.len() {
            let kind = EntryType::try_from(data[offset]).expect("valid WAL kind");
            let header_len = wal_record_sz(kind).expect("known WAL size");
            if offset + header_len > data.len() {
                break;
            }
            if kind == EntryType::Update {
                let update = ptr_to::<WalUpdate>(unsafe { data.as_ptr().add(offset) });
                let payload_len = WalUpdate::checked_payload_len(update.size).expect("payload");
                let total_len = WalUpdate::checked_encoded_len(payload_len).expect("record");
                if offset + total_len > data.len() {
                    break;
                }
                let record = &mut data[offset..offset + total_len];
                record[std::mem::offset_of!(WalUpdate, group_id)] = u8::MAX;
                let update = ptr_to::<WalUpdate>(record.as_ptr());
                let checksum = update.calc_checksum();
                let checksum_offset = std::mem::offset_of!(WalUpdate, checksum);
                unsafe {
                    (record.as_mut_ptr().add(checksum_offset) as *mut u32)
                        .write_unaligned(checksum);
                }
                changed = true;
                break;
            }
            offset += header_len;
        }
        assert!(changed, "fixture must contain an update record");
        std::fs::write(&segment, &data).expect("write crafted segment");

        let mut strict = Options::new(&*root);
        strict.truncate_corrupted_wal = false;
        let report = check_database(&strict);
        assert!(
            report.diagnostics.iter().any(|diagnostic| {
                diagnostic.category == CheckCategory::BadWal
                    && diagnostic.detail.contains("out-of-range group id 255")
                    && diagnostic.address.is_some()
            }),
            "an out-of-range group id must identify the WAL record: {report:?}"
        );
    }

    #[test]
    fn stat_diagnostics_are_deduplicated_per_file_kind() {
        use super::*;

        let mut report = CheckReport::default();
        leak_bad_stat(&mut report, 1, FileKind::Data, 7, "Data: first".to_string());
        leak_bad_stat(
            &mut report,
            1,
            FileKind::Data,
            7,
            "Data: duplicate".to_string(),
        );
        leak_bad_stat(
            &mut report,
            1,
            FileKind::Blob,
            7,
            "Blob: same file id".to_string(),
        );
        assert_eq!(report.diagnostics.len(), 2);
        assert!(report.diagnostics.iter().any(|d| d.detail == "Data: first"));
        assert!(
            report
                .diagnostics
                .iter()
                .any(|d| d.detail == "Blob: same file id")
        );
    }

    /// a checksum mismatch inside a well-framed record is reported (the damage the
    /// tail rule must not hide)
    #[test]
    fn a_wal_checksum_mismatch_is_reported() {
        use mace::maintenance::{EntryType, WalUpdate, ptr_to, wal_record_sz};

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
                .view(mace::maintenance::BUCKET_METAS, |txn| {
                    let mut iter = txn.iter();
                    let mut key = Vec::new();
                    let mut value = Vec::new();
                    while iter.next_ref(&mut key, &mut value) {
                        id = Some(mace::maintenance::BucketMeta::decode(&value).id);
                    }
                    Ok(())
                })
                .expect("catalog");
            id.expect("fixture must hold a bucket")
        };
        let interval_bucket =
            mace::maintenance::interval_bucket_name(mace::maintenance::FileKind::Data, bucket_id);
        let (key, mut interval) = {
            let mut found = None;
            btree
                .view(&interval_bucket, |txn| {
                    let mut iter = txn.iter();
                    let mut key = Vec::new();
                    let mut value = Vec::new();
                    while iter.next_ref(&mut key, &mut value) {
                        found =
                            Some((key.clone(), mace::maintenance::IntervalPair::decode(&value)));
                    }
                    Ok(())
                })
                .expect("interval bucket");
            found.expect("fixture must publish an interval")
        };
        interval.file_id = 999_999;
        let mut encoded = vec![0u8; size_of::<mace::maintenance::IntervalPair>()];
        mace::maintenance::IMetaCodec::encode(&interval, &mut encoded);
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
                .view(mace::maintenance::BUCKET_DATA_STAT, |txn| {
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
        let inner_offset = mace::maintenance::PersistStat::stat_inner_offset();
        let total_offset =
            inner_offset + std::mem::offset_of!(mace::maintenance::StatInner, total_elems);
        value[total_offset] = value[total_offset].wrapping_add(7);
        btree
            .exec(mace::maintenance::BUCKET_DATA_STAT, |txn| {
                txn.put(&key, &value)
            })
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

    /// a damaged stat mask must not reduce the readable page-chain budget to zero
    #[test]
    fn a_damaged_stat_mask_does_not_exhaust_page_chain_budget() {
        use mace::maintenance::{BUCKET_DATA_STAT, PersistStat};

        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        seed_with_data(&root);
        let opt = Options::new(&*root);
        let btree = btree_store::BTree::open(opt.manifest()).expect("open manifest");
        let (key, mut value) = btree
            .view(BUCKET_DATA_STAT, |txn| {
                let mut iter = txn.iter();
                let (mut key, mut value) = (Vec::new(), Vec::new());
                let found = iter.next_ref(&mut key, &mut value);
                Ok(found.then_some((key, value)))
            })
            .expect("stat scan")
            .expect("stat record");
        assert!(
            value.len() > PersistStat::stat_inner_offset(),
            "fixture must carry a stat"
        );
        // make the encoded inactive sequence count exceed the available record body
        value[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        btree
            .exec(BUCKET_DATA_STAT, |txn| txn.put(&key, &value))
            .expect("damage stat mask");
        drop(btree);

        let report = check_database(&opt);

        assert!(
            report
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.category == CheckCategory::BadStatAccounting),
            "the damaged mask must be reported: {report:?}"
        );
        assert!(
            !report.diagnostics.iter().any(|diagnostic| {
                diagnostic.category == CheckCategory::BadPageChain
                    && diagnostic.detail.contains("budget exhausted")
            }),
            "a readable relocation table must preserve the page-chain budget: {report:?}"
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
            .view(mace::maintenance::BUCKET_METAS, |txn| txn.get("x"))
            .expect("seeded bucket record");
        let expected_id = mace::maintenance::BucketMeta::decode(&encoded).id;
        btree
            .exec(mace::maintenance::BUCKET_METAS, |txn| {
                txn.put("dup", &encoded)
            })
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
        let mace = mace::Mace::new(opt.validate().expect("seed options")).expect("seed open");
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
        let mace = mace::Mace::new(opt.validate().expect("seed options")).expect("seed open");
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
        let mace = mace::Mace::new(opt.validate().expect("seed options")).expect("seed open");
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

    #[cfg(feature = "extra_check")]
    #[test]
    fn a_check_on_an_absent_root_creates_nothing() {
        let scratch = RandomPath::tmp();
        std::fs::create_dir_all(&*scratch).expect("scratch dir");
        // the root itself does not exist: `create_dir` (the only path that records
        // CreateDirAll) would have to run if the checker validated its options
        let root = scratch.join("absent");
        let fs = Arc::new(InjectedFileSystem::new());
        let mut opt = Options::new(&root);
        opt.set_fs(fs.clone());

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

    #[cfg(feature = "extra_check")]
    #[test]
    fn the_checker_opens_the_metadata_store_read_only() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).expect("scratch dir");
        // seed a metadata store by opening a real engine once
        {
            let mut seed = Options::new(&*root);
            seed.sync_on_write = true;
            let mace = mace::Mace::new(seed.validate().expect("seed options")).expect("seed open");
            let _ = mace
                .new_bucket("x", Default::default())
                .expect("seed bucket");
        }

        let fs = Arc::new(InjectedFileSystem::new());
        let mut opt = Options::new(&*root);
        opt.set_fs(fs.clone());
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
    fn repair_record() -> super::RepairRecord {
        super::RepairRecord {
            bucket_id: 1,
            file_id: 1,
            kind: "data",
            added_inactive: 1,
            active_elems: (2, 1),
            active_size: (2, 1),
        }
    }

    #[test]
    fn repair_transaction_rolls_back_earlier_records_on_returned_error() {
        let root = RandomPath::tmp();
        std::fs::create_dir_all(&*root).unwrap();
        let tree = btree_store::BTree::open(root.join("manifest")).unwrap();
        tree.new_bucket("present", false).unwrap();
        let key = 1u64.to_le_bytes();
        tree.exec("present", |tx| tx.put(key, b"old")).unwrap();
        let prepared = vec![
            ("present".to_string(), key, b"new".to_vec(), repair_record()),
            ("missing".to_string(), key, b"new".to_vec(), repair_record()),
        ];
        let fault = super::write_repairs(&tree, &prepared);
        assert!(fault.is_err());
        tree.view("present", |tx| {
            assert_eq!(tx.get(key)?, b"old");
            Ok(())
        })
        .unwrap();
        super::write_repairs(&tree, &[]).unwrap();
    }

    #[test]
    fn oversized_repair_is_rejected_before_a_transaction() {
        use mace::maintenance::{FileKind, PersistStat, StatInner};
        let stat = PersistStat {
            inner: StatInner {
                file_id: 1,
                bucket_id: 1,
                up1: 0,
                up2: 0,
                active_elems: 1,
                total_elems: 1,
                active_size: 1,
                total_size: 1,
            },
            inactive_elems: vec![0; btree_store::MAX_VAL_LEN / 4 + 1],
        };
        let result = super::prepare_repairs(vec![super::RepairPlan {
            kind: FileKind::Data,
            stat,
            record: repair_record(),
        }]);
        assert!(result.is_err());
    }
}
