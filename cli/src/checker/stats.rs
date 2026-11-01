//! stat and interval accounting checks across the metadata records

use btree_store::BTree;
use mace::maintenance::{
    BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BitMap, FileKind, IMetaCodec, IntervalPair, MetaReader,
    NULL_ADDR, Options, PersistStat, StatInner, interval_bucket_name, page_table_name,
};

use super::lifecycle::catalog_bucket_ids;
use super::payload::{
    FileRelocs, PayloadChecks, check_page_chains, payload_file_len, payload_path, reloc_keys,
};
use super::{CheckCategory, CheckReport, Diagnostic};

/// the interval view of one bucket: the decoded pairs, plus how many rows could not be
/// decoded. a bucket with a dropped row has an incomplete interval set, so the page table
/// must not be judged against it (that damage was already reported as a bad interval row)
#[derive(Default)]
pub(super) struct IntervalView {
    pub(super) pairs: Vec<IntervalPair>,
    pub(super) dropped: usize,
}

/// the stat view one kind's cross-checks need: the decoded records plus the file ids
/// whose record was already reported as undecodable (so the interval phase must not
/// report the same damage as a missing stat record)
#[derive(Default)]
pub(super) struct StatView {
    pub(super) records: Vec<StatRecord>,
    pub(super) undecodable: std::collections::HashSet<u64>,
    pub(super) unattributed: bool,
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
                if keys
                    .binary_search_by_key(&address, |entry| entry.0)
                    .is_err()
                {
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

/// one persisted per-file stat record, decoded for the cross-record checks
pub(super) struct StatRecord {
    pub(super) inner: StatInner,
    pub(super) raw: Vec<u8>,
    pub(super) mask: Option<BitMap>,
    pub(super) inactive: usize,
}

impl std::ops::Deref for StatRecord {
    type Target = StatInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// validates the per-bucket cross-record chain:
///
/// - every published interval must name a file that carries a stat record for the same
///   bucket and whose relocation table resolves the interval's start;
/// - every stat record's element counts must match its file's relocation table
///   (`total_elems` == relocations, `active_elems` + inactive == `total_elems`).
pub(super) fn check_bucket_metadata(
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
            // the traversal budget must not depend on the stat mask: a damaged mask still
            // leaves a readable relocation table whose length is a safe page-count bound
            let budget = stats
                .records
                .iter()
                .filter(|stat| stat.bucket_id == *bucket_id)
                .filter_map(|stat| match reloc_keys(opt, kind, stat.file_id, checks) {
                    FileRelocs::Keys(keys) => Some(keys.len()),
                    FileRelocs::Unreadable(_) if !stats.undecodable.contains(&stat.file_id) => {
                        Some(stat.total_elems as usize)
                    }
                    FileRelocs::Unreadable(_) => None,
                })
                .fold(0usize, usize::saturating_add);
            *bucket_budget.entry(*bucket_id).or_insert(0) += budget;
            let intervals = bucket_intervals.get(&(kind, *bucket_id)).unwrap();
            check_intervals_against_stats(opt, kind, *bucket_id, intervals, &stats, checks, report);
            if kind == FileKind::Data {
                check_page_table(opt, btree, *bucket_id, intervals, checks, report);
            }
        }
        check_stats_against_files(opt, kind, &stats, checks, report);
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

/// decodes the persisted per-file stat records of one kind, together with the diagnostics
/// the scan produced; callers that only need the file identities (the lifecycle phase,
/// which runs after the stat phase already reported them) drop the diagnostics
///
/// the returned [`StatView`] names the files whose stat record could not be decoded:
/// those records were reported here, so the interval phase must not add a second finding
/// for the same damage
pub(super) fn read_stats(
    opt: &Options,
    btree: &BTree,
    kind: FileKind,
) -> (StatView, Vec<Diagnostic>) {
    let mut diagnostics = Vec::new();
    let mut undecodable = std::collections::HashSet::new();
    let bucket = match kind {
        FileKind::Data => BUCKET_DATA_STAT,
        FileKind::Blob => BUCKET_BLOB_STAT,
    };
    let mut records = Vec::new();
    let mut unattributed = false;
    let outcome = btree.view(bucket, |txn| {
        let mut iter = txn.iter();
        let mut key = Vec::new();
        let mut value = Vec::new();
        while iter.next_ref(&mut key, &mut value) {
            let inner = match PersistStat::try_decode_inner_only(&value) {
                Ok(inner) => inner,
                Err(err) => {
                    unattributed = true;
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
            match <[u8; 8]>::try_from(&key[..]) {
                Ok(bytes) if u64::from_le_bytes(bytes) == inner.file_id => {}
                _ => unattributed = true,
            }
            // a payload file that cannot be opened is reported by the payload phase, and
            // its record's mask cannot be bounded by the file, so the mask is not decoded
            // here: the stat record is kept for the identity checks and the mask-derived
            // inactive count stays zero
            let path = payload_path(opt, kind, inner.file_id);
            let Ok(file_len) = payload_file_len(opt, &path) else {
                records.push(StatRecord {
                    inner,
                    raw: value.clone(),
                    mask: None,
                    inactive: 0,
                });
                continue;
            };
            if MetaReader::try_new(opt.fs(), &path).is_err() {
                records.push(StatRecord {
                    inner,
                    raw: value.clone(),
                    mask: None,
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
                records.push(StatRecord {
                    inner,
                    raw: value.clone(),
                    mask: None,
                    inactive: 0,
                });
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
                    records.push(StatRecord {
                        inner,
                        raw: value.clone(),
                        mask: None,
                        inactive: 0,
                    });
                    continue;
                }
            };
            records.push(StatRecord {
                inner,
                raw: value.clone(),
                inactive: mask.set_bits(inner.total_elems).len(),
                mask: Some(mask),
            });
        }
        Ok(())
    });
    if let Err(err) = outcome {
        unattributed = true;
        diagnostics.push(Diagnostic::new(
            CheckCategory::UnreadableMetadata,
            format!("cannot scan {bucket}: {err:?}"),
        ));
    }
    (
        StatView {
            records,
            undecodable,
            unattributed,
        },
        diagnostics,
    )
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
                if keys
                    .binary_search_by_key(&interval.lo_addr, |entry| entry.0)
                    .is_err()
                {
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
    stats: &StatView,
    checks: &mut PayloadChecks,
    report: &mut CheckReport,
) {
    for stat in stats
        .records
        .iter()
        .filter(|stat| !stats.undecodable.contains(&stat.file_id))
    {
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
