//! payload file checks: data/blob framing, relocation and interval tables, page chains

use std::sync::Arc;

use btree_store::BTree;
use mace::maintenance::{
    AddrPair, BaseHeader, BoxRef, DecompressorPool, DeltaHeader, File, FileKind, FileReader,
    GatherIO, IHeader, Interval, MetaReader, NULL_ADDR, NULL_PID, NodeType, OpCode, Options,
    TagFlag, TagKind, Varint32, page_table_name, try_new_reader,
};

use super::stats::IntervalView;
use super::{CheckCategory, CheckReport, Diagnostic};

/// per-run state shared by the payload cross-checks: decoded relocation tables and the
/// payload files already reported as unreadable
#[derive(Default)]
pub(super) struct PayloadChecks {
    relocs: std::collections::HashMap<(FileKind, u64), FileRelocs>,
    readers: std::collections::HashMap<(FileKind, u64), Result<Arc<FileReader>, OpCode>>,
    decoders: Option<Arc<DecompressorPool>>,
    reported: std::collections::HashSet<(FileKind, u64)>,
    referenced: std::collections::HashMap<FileKind, std::collections::HashSet<u64>>,
}

impl PayloadChecks {
    pub(super) fn reader(
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
        let reader = try_new_reader(path, decoders, opt.fs_arc());
        self.readers.insert((kind, file_id), reader.clone());
        reader
    }

    /// records that a payload file is named by metadata: a stat record, an orphan marker or
    /// an obsolete entry. every enumerated payload file must be named by one of them
    pub(super) fn mark_referenced(&mut self, kind: FileKind, file_id: u64) {
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
    pub(super) fn report_unreadable(
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

/// validates every data/blob file on its own terms: footer and table framing, table
/// checksums, relocation ordering and bounds, and that every published interval
/// resolves to a relocation in the same file
pub(super) fn check_payload_files(
    opt: &Options,
    checks: &mut PayloadChecks,
    report: &mut CheckReport,
) {
    let data_root = opt.data_root();
    let entries = match opt.fs().read_dir(&data_root) {
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

#[allow(clippy::too_many_arguments)]
pub(super) fn check_page_chains(
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

pub(super) fn value_edges(raw: &[u8]) -> Result<Vec<(FileKind, u64)>, &'static str> {
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
        if addr == NULL_ADDR {
            return Err("history reference has a NULL address");
        }
        edges.push((FileKind::Data, addr));
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
        if addr == NULL_ADDR {
            return Err("remote reference has a NULL address");
        }
        edges.push((FileKind::Blob, addr));
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
pub(super) enum PageEdge {
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

pub(super) fn page_edges(page: &BoxRef) -> Result<Vec<PageEdge>, &'static str> {
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

/// the payload path of one file of one kind
pub(super) fn payload_path(opt: &Options, kind: FileKind, file_id: u64) -> std::path::PathBuf {
    match kind {
        FileKind::Data => opt.data_file(file_id),
        FileKind::Blob => opt.blob_file(file_id),
    }
}

/// what the checker learned about one payload file's relocation table
#[derive(Clone)]
pub(super) enum FileRelocs {
    /// the table decoded, sorted by key so the lookups can binary search it. the cache hands out
    /// a shared handle: a page-table row or interval check resolves one of these per item, and
    /// copying the vector each time costs O(entries) per row
    Keys(Arc<[(u64, u32)]>),
    /// the file could not be read at all, with the reason
    Unreadable(Arc<str>),
}

/// relocation keys of one payload file, cached per (kind, file id)
pub(super) fn reloc_keys(
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
    let outcome = MetaReader::try_new(opt.fs(), &path).and_then(|mut reader| {
        reader.try_get_reloc().map(|relocs| {
            let mut keys = relocs
                .iter()
                .map(|reloc| (reloc.key, reloc.val.seq))
                .collect::<Vec<_>>();
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
    let mut reader = match MetaReader::try_new(opt.fs(), path) {
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

pub(super) fn payload_file_len(
    opt: &Options,
    path: &std::path::Path,
) -> Result<u64, std::io::Error> {
    let file = File::options().read(true).open(opt.fs(), path)?;
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
