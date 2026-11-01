//! wal stream, segment and record framing checks

use std::collections::{HashMap, HashSet};

use mace::maintenance::{
    BucketDurableFrontier, EntryType, File, GatherIO, Options, PayloadType, Position, WalAbort,
    WalCheckpoint, WalUpdate, ptr_to, wal_record_sz,
};

use super::{CheckCategory, CheckReport, Diagnostic};

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
#[derive(Default)]
pub(super) struct WalFacts {
    pub(super) pending_redo: HashSet<u64>,
    pub(super) healthy: bool,
}

pub(super) fn check_wal(
    opt: &Options,
    tolerate_tail: bool,
    frontiers: &HashMap<u64, BucketDurableFrontier>,
    report: &mut CheckReport,
) -> WalFacts {
    let mut facts = WalFacts {
        healthy: true,
        ..WalFacts::default()
    };
    let log_root = opt.log_root();
    let entries = match opt.fs().read_dir(&log_root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return facts,
        Err(err) => {
            report.push(
                CheckCategory::UnreadableMetadata,
                format!("cannot list {}: {err}", log_root.display()),
            );
            facts.healthy = false;
            return facts;
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
            facts.healthy = false;
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
        let segment = check_wal_segment(
            opt,
            path,
            (*physical_id, *file_id, is_last),
            tolerate_tail,
            frontiers,
            report,
        );
        facts.healthy &= segment.healthy;
        facts.pending_redo.extend(segment.pending_redo);
    }
    facts
}

/// parses `wal_<group>_<seq>` / `group_wal_<seq>` into `(physical stream, file id)`
pub(super) fn wal_segment_name(name: &str) -> Option<(u8, u64)> {
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
    segment: (u8, u64, bool),
    tolerate_tail: bool,
    frontiers: &HashMap<u64, BucketDurableFrontier>,
    report: &mut CheckReport,
) -> WalFacts {
    let (physical_id, file_id, is_last) = segment;
    let mut facts = WalFacts {
        healthy: true,
        ..WalFacts::default()
    };
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
            facts.healthy = false;
            return facts;
        }
    };
    let mut offset = 0usize;
    while offset < data.len() {
        let reason = match wal_record_len(&data, offset) {
            Ok(len) => {
                if EntryType::try_from(data[offset]) == Ok(EntryType::Update) {
                    let update = ptr_to::<WalUpdate>(unsafe { data.as_ptr().add(offset) });
                    let bucket_id = update.bucket_id;
                    let group = update.group_id as usize;
                    if group >= Options::MAX_CONCURRENT_WRITE as usize {
                        facts.healthy = false;
                        report.push_diagnostic(
                            Diagnostic::new(
                                CheckCategory::BadWal,
                                format!(
                                    "stream {physical_id} segment {file_id} has out-of-range group id {group}"
                                ),
                            )
                            .with_file(file_id)
                            .with_address(offset as u64),
                        );
                    } else {
                        let frontier = frontiers
                            .get(&bucket_id)
                            .map(|f| f.lsn[group])
                            .unwrap_or(Position::MIN);
                        if Position::new(file_id, offset as u64) > frontier {
                            facts.pending_redo.insert(bucket_id);
                        }
                    }
                }
                offset += len;
                continue;
            }
            Err(reason) => reason,
        };
        // recovery truncates a bad tail of the live segment, so the same damage is only
        // a finding when the stream continues after it, or when truncation is disabled
        let tolerated = is_last && tolerate_tail;
        if !tolerated {
            facts.healthy = false;
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
        return facts;
    }
    facts
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
    let file = File::options().read(true).open(opt.fs(), path)?;
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
