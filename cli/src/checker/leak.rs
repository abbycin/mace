//! fixed-root reachability and leak classification, plus the stat-ledger repair

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use btree_store::BTree;
use mace::maintenance::{
    BUCKET_BLOB_STAT, BUCKET_DATA_STAT, FileKind, IHeader, IMetaCodec, MetaReader, NULL_ADDR,
    Options, PersistStat, ROOT_PID, TagKind, page_table_name,
};

use super::lifecycle::{LifecycleFacts, classify_open_error, open_metadata_read_only};
use super::payload::{FileRelocs, PageEdge, PayloadChecks, page_edges, payload_path, reloc_keys};
use super::stats::{StatView, read_stats};
use super::wal::WalFacts;
use super::{CheckCategory, CheckReport, Diagnostic, LeakReport, RepairRecord, run_checks};

pub(super) struct RepairPlan {
    pub(super) kind: FileKind,
    pub(super) stat: PersistStat,
    pub(super) record: RepairRecord,
}

/// checks the fixed-root closure of a closed database, optionally repairing its stat ledger
pub fn leak_check_database(opt: &Options, fix: bool) -> LeakReport {
    let mut report = CheckReport::default();
    let btree = if fix {
        let path = opt.manifest();
        match opt.fs().try_exists(&path) {
            Ok(false) => {
                report.push(
                    CheckCategory::MissingMetadata,
                    format!("metadata store {} is absent", path.display()),
                );
                report.sort();
                return LeakReport {
                    report,
                    ..LeakReport::default()
                };
            }
            Err(err) => {
                report.push(
                    CheckCategory::UnreadableMetadata,
                    format!("cannot inspect {}: {err}", path.display()),
                );
                report.sort();
                return LeakReport {
                    report,
                    ..LeakReport::default()
                };
            }
            Ok(true) => {}
        }
        match BTree::open(path.clone()) {
            Ok(btree) => btree,
            Err(err) => {
                if let Some(class) = classify_open_error(&err) {
                    report.push_diagnostic((class.detail)(&err, &path));
                } else {
                    report.push(
                        CheckCategory::UnreadableMetadata,
                        format!("cannot open {}: {err:?}", path.display()),
                    );
                }
                report.sort();
                return LeakReport {
                    report,
                    ..LeakReport::default()
                };
            }
        }
    } else {
        let Some(btree) = open_metadata_read_only(opt, &mut report) else {
            report.sort();
            return LeakReport {
                report,
                ..LeakReport::default()
            };
        };
        btree
    };
    let (report, plans) = run_checks(opt, &btree, true);
    if !fix || plans.is_empty() {
        return LeakReport {
            report,
            ..LeakReport::default()
        };
    }
    let prepared = match prepare_repairs(plans) {
        Ok(prepared) => prepared,
        Err(fault) => {
            return LeakReport {
                report,
                fault: Some(fault),
                ..LeakReport::default()
            };
        }
    };
    match write_repairs(&btree, &prepared) {
        Ok(()) => {
            let report = run_checks(opt, &btree, true).0;
            let repairs = prepared
                .into_iter()
                .map(|(_, _, _, record)| record)
                .collect();
            LeakReport {
                report,
                repairs,
                fault: None,
            }
        }
        Err(fault) => LeakReport {
            report,
            fault: Some(fault),
            ..LeakReport::default()
        },
    }
}

pub(super) type PreparedRepair = (String, [u8; 8], Vec<u8>, RepairRecord);

pub(super) fn prepare_repairs(plans: Vec<RepairPlan>) -> Result<Vec<PreparedRepair>, String> {
    let mut prepared = Vec::with_capacity(plans.len());
    for plan in plans {
        let len = plan.stat.packed_size();
        if len > btree_store::MAX_VAL_LEN {
            return Err(format!(
                "stat repair value is {len} bytes, exceeding btree-store's {} byte limit",
                btree_store::MAX_VAL_LEN
            ));
        }
        let mut encoded = vec![0; len];
        plan.stat.encode(&mut encoded);
        prepared.push((
            stat_bucket(plan.kind).to_string(),
            plan.stat.file_id.to_le_bytes(),
            encoded,
            plan.record,
        ));
    }
    Ok(prepared)
}

pub(super) fn write_repairs(btree: &BTree, prepared: &[PreparedRepair]) -> Result<(), String> {
    if prepared.is_empty() {
        return Ok(());
    }
    btree
        .exec_multi(|multi| {
            for (bucket, key, value, _) in prepared {
                multi.exec(bucket, |txn| txn.put(key, value.clone()))?;
            }
            Ok(())
        })
        .map_err(|err| format!("repair transaction failed: {err:?}"))
}

fn stat_bucket(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Data => BUCKET_DATA_STAT,
        FileKind::Blob => BUCKET_BLOB_STAT,
    }
}

#[derive(Default)]
struct BucketReachable {
    seen: BTreeSet<u64>,
    blob_roots: BTreeSet<u64>,
    outside: Vec<(u64, u64)>,
}

fn undecidable(bucket: u64, kind: FileKind, reason: impl std::fmt::Display) -> Diagnostic {
    Diagnostic::new(
        CheckCategory::LeakUndecidable,
        format!("{kind:?}: {reason}"),
    )
    .with_bucket(bucket)
}

fn derive_bucket(
    opt: &Options,
    btree: &BTree,
    bucket: u64,
    kind: FileKind,
    stats: &StatView,
    checks: &mut PayloadChecks,
    seeds: Option<&BTreeSet<u64>>,
) -> Result<BucketReachable, Diagnostic> {
    let mut index = HashMap::new();
    for stat in stats.records.iter().filter(|stat| stat.bucket_id == bucket) {
        let keys = match reloc_keys(opt, kind, stat.file_id, checks) {
            FileRelocs::Keys(keys) => keys,
            FileRelocs::Unreadable(reason) => {
                return Err(undecidable(bucket, kind, reason).with_file(stat.file_id));
            }
        };
        for &(addr, seq) in keys.iter() {
            if index.insert(addr, (stat.file_id, seq)).is_some() {
                return Err(undecidable(bucket, kind, "duplicate relocation address")
                    .with_file(stat.file_id)
                    .with_address(addr));
            }
        }
    }
    let mut mapping = BTreeMap::new();
    let mut queue = Vec::new();
    let mut pids = Vec::new();
    if let Some(seeds) = seeds {
        queue.extend(seeds);
    } else {
        let mut malformed = None;
        btree
            .view(&page_table_name(bucket), |txn| {
                let mut iter = txn.iter();
                let (mut key, mut value) = (Vec::new(), Vec::new());
                while iter.next_ref(&mut key, &mut value) {
                    let (Ok(key), Ok(value)) =
                        (key.as_slice().try_into(), value.as_slice().try_into())
                    else {
                        malformed = Some("invalid page table row");
                        continue;
                    };
                    mapping.insert(u64::from_le_bytes(key), u64::from_le_bytes(value));
                }
                Ok(())
            })
            .map_err(|err| undecidable(bucket, kind, format!("page table: {err:?}")))?;
        if let Some(reason) = malformed {
            return Err(undecidable(bucket, kind, reason));
        }
        pids.push(ROOT_PID);
    }
    let mut reached_pids = HashSet::new();
    let mut result = BucketReachable::default();
    loop {
        for pid in pids.drain(..) {
            let addr = mapping
                .get(&pid)
                .copied()
                .filter(|addr| *addr != NULL_ADDR)
                .ok_or_else(|| {
                    undecidable(bucket, kind, format!("PID {pid} is missing or NULL"))
                })?;
            if reached_pids.insert(pid) {
                queue.push(addr);
            }
        }
        let Some(addr) = queue.pop() else { break };
        if result.seen.contains(&addr) {
            continue;
        }
        let &(file_id, _) = index.get(&addr).ok_or_else(|| {
            undecidable(bucket, kind, "reference has no relocation").with_address(addr)
        })?;
        let reader = checks.reader(opt, kind, file_id).map_err(|err| {
            undecidable(bucket, kind, format!("reader: {err:?}"))
                .with_file(file_id)
                .with_address(addr)
        })?;
        let page = reader.try_read_at(addr).map_err(|err| {
            undecidable(bucket, kind, format!("page: {err:?}"))
                .with_file(file_id)
                .with_address(addr)
        })?;
        if page.header().link != NULL_ADDR {
            queue.push(page.header().link);
        }
        if kind == FileKind::Data {
            if !matches!(page.header().kind, TagKind::Base | TagKind::Delta) {
                return Err(undecidable(bucket, kind, "invalid data page kind")
                    .with_file(file_id)
                    .with_address(addr));
            }
            for edge in page_edges(&page).map_err(|err| {
                undecidable(bucket, kind, err)
                    .with_file(file_id)
                    .with_address(addr)
            })? {
                match edge {
                    PageEdge::Child(pid) => pids.push(pid),
                    PageEdge::Address(FileKind::Data, target) => queue.push(target),
                    PageEdge::Address(FileKind::Blob, target) => {
                        result.blob_roots.insert(target);
                    }
                }
            }
        } else if page.header().kind != TagKind::Remote {
            return Err(undecidable(bucket, kind, "invalid blob page kind")
                .with_file(file_id)
                .with_address(addr));
        }
        result.seen.insert(addr);
    }
    result.outside = mapping
        .into_iter()
        .filter(|(pid, addr)| *addr != NULL_ADDR && !reached_pids.contains(pid))
        .collect();
    Ok(result)
}

pub(super) fn classify_database_leaks(
    opt: &Options,
    btree: &BTree,
    lifecycle: &LifecycleFacts,
    wal: &WalFacts,
    checks: &mut PayloadChecks,
    report: &mut CheckReport,
) -> Vec<RepairPlan> {
    let stats = FileKind::ALL.map(|kind| read_stats(opt, btree, kind).0);
    // the interval phase records the exact files whose owning bucket contradicts the stat
    // ledger; only those lose their reachability source
    let contradicted = checks.contradicted.clone();
    let mut repairs = Vec::new();
    if lifecycle.inventory_complete {
        for (kind, view) in FileKind::ALL.into_iter().zip(&stats) {
            for stat in &view.records {
                if !lifecycle.catalog.contains(&stat.bucket_id)
                    && !lifecycle.pending.contains(&stat.bucket_id)
                {
                    report.push_diagnostic(
                        Diagnostic::new(
                            CheckCategory::BadLifecycleState,
                            format!("{kind:?} stat has no catalog or pending-delete owner"),
                        )
                        .with_bucket(stat.bucket_id)
                        .with_file(stat.file_id),
                    );
                }
            }
        }
    }
    for &bucket in &lifecycle.catalog {
        if lifecycle.pending.contains(&bucket)
            || (!stats.iter().any(|view| view.unattributed)
                && !stats
                    .iter()
                    .any(|view| view.records.iter().any(|stat| stat.bucket_id == bucket)))
        {
            continue;
        }
        let mut data_complete = true;
        let mut blob_roots = BTreeSet::new();
        for (kind, view) in FileKind::ALL.into_iter().zip(&stats) {
            let reason = if !lifecycle.healthy || !wal.healthy {
                Some("lifecycle or WAL inventory is incomplete")
            } else if !lifecycle.frontiers.contains_key(&bucket) {
                Some("bucket frontier is missing")
            } else if wal.pending_redo.contains(&bucket) {
                Some("bucket has updates beyond its durable frontier")
            } else if view.unattributed {
                Some("stat ownership is incomplete")
            } else if kind == FileKind::Blob && !data_complete {
                Some("data closure is undecidable")
            } else {
                None
            };
            let derived = if let Some(reason) = reason {
                Err(undecidable(bucket, kind, reason))
            } else {
                derive_bucket(
                    opt,
                    btree,
                    bucket,
                    kind,
                    view,
                    checks,
                    (kind == FileKind::Blob).then_some(&blob_roots),
                )
            };
            let closure = match derived {
                Ok(closure) => closure,
                Err(diagnostic) => {
                    report.push_diagnostic(diagnostic);
                    if kind == FileKind::Data {
                        data_complete = false;
                    }
                    continue;
                }
            };
            if let Some(&(pid, addr)) = closure.outside.first() {
                report.push_diagnostic(
                    undecidable(
                        bucket,
                        kind,
                        format!("PID {pid} maps outside the fixed-root closure"),
                    )
                    .with_address(addr),
                );
                data_complete = false;
                continue;
            }
            if kind == FileKind::Data {
                blob_roots.clone_from(&closure.blob_roots);
            }
            match classify_leaks(
                opt,
                bucket,
                kind,
                view,
                &closure.seen,
                RepairBlocks {
                    files: &lifecycle.blocked_files,
                    contradicted: &contradicted,
                },
                report,
            ) {
                Ok(plans) => repairs.extend(plans),
                Err(diagnostic) => {
                    report.push_diagnostic(diagnostic);
                    if kind == FileKind::Data {
                        data_complete = false;
                    }
                }
            }
        }
    }
    repairs
}

/// files a repair must not touch
pub(super) struct RepairBlocks<'a> {
    /// lifecycle markers already blocked this file
    pub(super) files: &'a HashSet<(FileKind, u64)>,
    /// the interval owner contradicts the stat ledger for this file
    pub(super) contradicted: &'a HashSet<u64>,
}

pub(super) fn classify_leaks(
    opt: &Options,
    bucket: u64,
    kind: FileKind,
    stats: &StatView,
    reachable: &BTreeSet<u64>,
    blocks: RepairBlocks<'_>,
    report: &mut CheckReport,
) -> Result<Vec<RepairPlan>, Diagnostic> {
    let mut files = Vec::new();
    for stat in stats.records.iter().filter(|stat| stat.bucket_id == bucket) {
        // the interval rows of another bucket name this file: the stat ledger cannot decide
        // which bucket owns its reachability, so report that and derive no mark from either
        // bucket's closure
        if blocks.contradicted.contains(&stat.file_id) {
            report.push_diagnostic(
                undecidable(
                    bucket,
                    kind,
                    "interval ownership contradicts the stat ledger",
                )
                .with_file(stat.file_id),
            );
            continue;
        }
        let path = payload_path(opt, kind, stat.file_id);
        let relocs = MetaReader::try_new(opt.fs(), &path)
            .and_then(|mut reader| reader.try_get_reloc())
            .map_err(|err| {
                undecidable(bucket, kind, format!("relocation inventory: {err:?}"))
                    .with_file(stat.file_id)
            })?;
        files.push((stat, relocs));
    }
    files.sort_by_key(|(stat, _)| stat.file_id);
    let mut plans = Vec::new();
    for (stat, relocs) in files {
        let Some(mask) = &stat.mask else { continue };
        let mut sequences = HashSet::new();
        let mut accounting_valid = stat.total_elems as usize == relocs.len();
        for entry in &relocs {
            accounting_valid &=
                entry.val.seq < relocs.len() as u32 && sequences.insert(entry.val.seq);
        }
        let original = PersistStat::decode(&stat.raw);
        accounting_valid &= original.inactive_elems.len() == stat.inactive;
        if !accounting_valid {
            leak_bad_stat(
                report,
                bucket,
                kind,
                stat.file_id,
                format!("{kind:?} sequence/mask/count does not match the relocation domain"),
            );
            continue;
        }
        let total_size: usize = relocs.iter().map(|e| e.val.active_len() as usize).sum();
        let active_size: usize = relocs
            .iter()
            .filter(|e| !mask.test(e.val.seq))
            .map(|e| e.val.active_len() as usize)
            .sum();
        if total_size != stat.total_size || active_size != stat.active_size {
            leak_bad_stat(
                report,
                bucket,
                kind,
                stat.file_id,
                format!(
                    "{kind:?} byte totals disagree with relocations: total {} != {total_size}, active {} != {active_size}",
                    stat.total_size, stat.active_size
                ),
            );
        }
        let mut leaked = Vec::new();
        let mut overmarked = false;
        let mut leak_bytes = 0usize;
        let mut first_addr = None;
        for entry in &relocs {
            let addr = entry.key;
            let seq = entry.val.seq;
            match (reachable.contains(&addr), mask.test(seq)) {
                (true, true) => {
                    overmarked = true;
                }
                (false, false) => {
                    leaked.push(seq);
                    leak_bytes += entry.val.active_len() as usize;
                    first_addr = Some(first_addr.map_or(addr, |old: u64| old.min(addr)));
                }
                _ => {}
            }
        }
        if overmarked {
            leak_bad_stat(
                report,
                bucket,
                kind,
                stat.file_id,
                format!(
                    "{kind:?} reachable relocation is marked inactive; file is excluded from repair"
                ),
            );
        }
        if let Some(addr) = first_addr {
            report.push_diagnostic(
                Diagnostic::new(
                    CheckCategory::JunkLeak,
                    format!(
                        "{kind:?}: {} unreachable unmarked record(s), {leak_bytes} bytes",
                        leaked.len()
                    ),
                )
                .with_bucket(bucket)
                .with_file(stat.file_id)
                .with_address(addr),
            );
        }
        if leaked.is_empty() || overmarked || blocks.files.contains(&(kind, stat.file_id)) {
            continue;
        }
        let added_inactive = leaked.len();
        let before = (stat.active_elems, stat.active_size);
        let mut updated = original;
        updated.inactive_elems.extend(leaked);
        updated.inactive_elems.sort_unstable();
        updated.inactive_elems.dedup();
        let mut inactive = mask.clone();
        for &seq in &updated.inactive_elems {
            inactive.set(seq);
        }
        updated.active_elems = relocs.len() as u32 - updated.inactive_elems.len() as u32;
        updated.active_size = relocs
            .iter()
            .filter(|e| !inactive.test(e.val.seq))
            .map(|e| e.val.active_len() as usize)
            .sum();
        updated.total_elems = relocs.len() as u32;
        updated.total_size = total_size;
        plans.push(RepairPlan {
            kind,
            record: RepairRecord {
                bucket_id: bucket,
                file_id: stat.file_id,
                kind: if kind == FileKind::Data {
                    "data"
                } else {
                    "blob"
                },
                added_inactive,
                active_elems: (before.0, updated.active_elems),
                active_size: (before.1, updated.active_size),
            },
            stat: updated,
        });
    }
    Ok(plans)
}

pub(super) fn leak_bad_stat(
    report: &mut CheckReport,
    bucket: u64,
    kind: FileKind,
    file: u64,
    detail: String,
) {
    let kind_prefix = format!("{kind:?}:");
    if !report.diagnostics.iter().any(|d| {
        d.category == CheckCategory::BadStatAccounting
            && d.bucket_id == Some(bucket)
            && d.file_id == Some(file)
            && d.detail.starts_with(&kind_prefix)
    }) {
        report.push_diagnostic(
            Diagnostic::new(CheckCategory::BadStatAccounting, detail)
                .with_bucket(bucket)
                .with_file(file),
        );
    }
}

/// returns the actual raw fixed-root closure, independently of the engine's typed oracle
#[cfg(feature = "extra_check")]
pub fn leak_check_reachable_for_test(
    opt: &Options,
    bucket_id: u64,
) -> (BTreeSet<u64>, BTreeSet<u64>) {
    let btree = BTree::open_read_only(opt.manifest()).expect("read-only manifest");
    let mut checks = PayloadChecks::default();
    let data = derive_bucket(
        opt,
        &btree,
        bucket_id,
        FileKind::Data,
        &read_stats(opt, &btree, FileKind::Data).0,
        &mut checks,
        None,
    )
    .expect("data closure");
    let blob = derive_bucket(
        opt,
        &btree,
        bucket_id,
        FileKind::Blob,
        &read_stats(opt, &btree, FileKind::Blob).0,
        &mut checks,
        Some(&data.blob_roots),
    )
    .expect("blob closure");
    (data.seen, blob.seen)
}
