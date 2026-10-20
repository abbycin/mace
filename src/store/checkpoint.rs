use crate::cc::log::Logging;
use crate::io::{File, GatherIO};
use crate::map::data::FileVersion;
use crate::meta::guard::DeletionToken;
use crate::meta::{
    BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_FRONTIER, BucketDurableFrontier, CURRENT_VERSION,
    FileKind, IMetaCodec, PersistStat,
};
use crate::store::store::Store;
#[cfg(feature = "metrics")]
use crate::utils::observe::{CounterMetric, HistogramMetric};
use crate::utils::{
    OpCode,
    data::{GroupPositions, Position},
    options::{Options, ParsedOptions},
};
use btree_store::BTree;
use std::path::{Component, Path, PathBuf};

/// staging directory suffix; a crashed checkpoint leaves this behind and the next call
/// refuses it with `OpCode::Exist`, because a directory at that path is indistinguishable
/// from one the caller owns and is therefore never deleted
const STAGING_SUFFIX: &str = ".tmp";
const COPY_CHUNK: usize = 1 << 20;

/// one logical writer group's durable frontier position
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupPosition {
    pub file_id: u64,
    pub offset: u64,
}

impl From<Position> for GroupPosition {
    fn from(pos: Position) -> Self {
        Self {
            file_id: pos.file_id,
            offset: pos.offset,
        }
    }
}

/// durable frontier of one bucket at the checkpoint's cut, one entry per logical
/// writer group slot
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketFrontier {
    pub bucket_id: u64,
    pub groups: Vec<GroupPosition>,
}

/// result of a successful [`crate::Mace::create_checkpoint`]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointInfo {
    /// metadata store generation the copy's manifest equals
    ///
    /// This is not an upper bound on what the copy can hold: the WAL cut is taken after
    /// the metadata snapshot, so a commit landing in that window is replayed when the
    /// copy opens. The guarantee is completeness - every record the copied frontier
    /// folds in is present in the copied segments - so the copy never loses a
    /// pre-snapshot commit. [`Self::frontiers`] carries the same lower-bound framing.
    pub seq: u64,
    /// stored metadata organization version
    pub metadata_version: u64,
    /// data/blob file format version of the copied payload files
    pub data_blob_format_version: u32,
    /// per-bucket durable frontier read from the copy's own metadata
    ///
    /// A **lower bound**, not the copy's read-as-of position: the copy is complete
    /// through every position up to and including these frontiers, and because the WAL
    /// cut is taken after the metadata snapshot the copy may also hold - and replay -
    /// records past them. The copy therefore reads at or after the reported frontier,
    /// never before it.
    pub frontiers: Vec<BucketFrontier>,
}

/// one physical WAL stream's cut: every retained segment from `oldest` to
/// `file_id` belongs to the copy, with the last one truncated to `offset`
struct StreamCut {
    physical_id: u8,
    oldest: u64,
    file_id: u64,
    offset: u64,
}

impl Store {
    /// Writes a standalone, openable copy of this instance to `target`.
    ///
    /// The copy equals one published metadata generation plus the payload files and
    /// WAL records that generation needs. Runtime file deletions stay suspended from
    /// before the metadata cut until every copy is complete, so no file the copy
    /// references can be unlinked while it is being written. `target` must not exist;
    /// a stale `<target>.tmp` staging directory is refused. The result is
    /// published with a rename followed by a directory sync, so an interrupted call
    /// never leaves a copy that looks complete.
    pub(crate) fn create_checkpoint(&self, target: &Path) -> Result<CheckpointInfo, OpCode> {
        #[cfg(feature = "metrics")]
        {
            self.opt.observer.counter(CounterMetric::CheckpointStart, 1);
        }
        let result = self.run_checkpoint(target);
        match result {
            Ok(info) => {
                #[cfg(feature = "metrics")]
                {
                    self.opt
                        .observer
                        .counter(CounterMetric::CheckpointComplete, 1);
                }
                Ok(info)
            }
            Err(err) => {
                #[cfg(feature = "metrics")]
                {
                    self.opt
                        .observer
                        .counter(CounterMetric::CheckpointFailure, 1);
                }
                Err(err)
            }
        }
    }

    /// the checkpoint attempt itself: every error path here is reported by
    /// [`Self::create_checkpoint`] as a failure
    fn run_checkpoint(&self, target: &Path) -> Result<CheckpointInfo, OpCode> {
        // one copy at a time: the in-process lock serializes calls targeting the same directory
        let _serial = self.checkpoint_lock.lock();
        let (target, staging) = checkpoint_staging(self.opt.as_ref(), target)?;
        prepare_staging(self.opt.as_ref(), &staging)?;

        // suspension must precede the metadata cut and cover every copy below
        let guard = self.manifest.deletions.suspend();
        self.manifest.deletions.wait_quiescent();

        let outcome = match self.copy_closure(&staging) {
            Ok(build) => match publish(self.opt.as_ref(), &staging, &target) {
                Ok(()) => Ok(build),
                Err(err) => {
                    cleanup_staging(self.opt.as_ref(), &staging);
                    Err(err)
                }
            },
            Err(err) => {
                cleanup_staging(self.opt.as_ref(), &staging);
                Err(err)
            }
        };
        self.finish_guard(&guard);
        outcome
    }

    /// records how long deletions were suspended
    fn finish_guard(&self, guard: &DeletionToken) {
        #[cfg(feature = "metrics")]
        {
            self.opt.observer.histogram(
                HistogramMetric::CheckpointGuardHoldMicros,
                guard.held_micros(),
            );
        }
        let _ = guard;
    }

    fn copy_closure(&self, staging: &Path) -> Result<CheckpointInfo, OpCode> {
        let opt = self.opt.as_ref();
        let manifest_path = staging.join("log").join(Options::MANIFEST);
        let snapshot = self
            .manifest
            .btree
            .take_snapshot(&manifest_path)
            .map_err(|_| OpCode::IoError)?;
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_checkpoint_after_metadata_copy");

        // the payload set and the frontiers must come from the copy itself: the live
        // in-memory state may already have stopped referencing a file the copy needs
        let (data_files, blob_files, frontiers) = read_copy_metadata(&manifest_path)?;
        copy_payloads(opt, staging, FileKind::Data, &data_files)?;
        copy_payloads(opt, staging, FileKind::Blob, &blob_files)?;
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_checkpoint_after_payload_copy_before_wal_cut");

        // the cut is taken after the metadata copy, so every record the copy's
        // frontier folds in is present in the copied segments
        let cuts = self.wal_cuts();
        copy_wal(opt, staging, &cuts)?;
        // the byte total only feeds the copied-bytes counter; without the metrics
        // feature the total is simply not bound (the size query itself stays, since
        // sync_staged_files already opens every staged file)
        #[cfg(feature = "metrics")]
        let copied_bytes = sync_staged_files(opt, staging)?;
        #[cfg(not(feature = "metrics"))]
        sync_staged_files(opt, staging)?;
        #[cfg(feature = "metrics")]
        {
            self.opt
                .observer
                .counter(CounterMetric::CheckpointCopiedBytes, copied_bytes);
            self.opt
                .observer
                .counter(CounterMetric::CheckpointWalCut, cuts.len() as u64);
            self.opt
                .observer
                .counter(CounterMetric::CheckpointFrontier, frontiers.len() as u64);
        }

        Ok(CheckpointInfo {
            seq: snapshot.seq,
            metadata_version: CURRENT_VERSION,
            data_blob_format_version: u32::from(FileVersion::CURRENT.raw()),
            frontiers,
        })
    }

    /// snapshots each physical stream's retained range and flushed position under its
    /// logger lock
    fn wal_cuts(&self) -> Vec<StreamCut> {
        let mut cuts = Vec::new();
        if self.opt.sync_on_write {
            let shared = self
                .context
                .shared_logging()
                .expect("durable route must own a shared stream");
            let logging = shared.lock();
            cuts.push(stream_cut(Options::SHARED_ID, &logging));
        } else {
            for group in self.context.groups() {
                let logging = group.logging.lock();
                cuts.push(stream_cut(group.id as u8, &logging));
            }
        }
        cuts
    }
}

fn stream_cut(physical_id: u8, logging: &Logging) -> StreamCut {
    let flushed = logging.flushed_pos();
    StreamCut {
        physical_id,
        oldest: logging.oldest_wal_id(),
        file_id: flushed.file_id,
        offset: flushed.offset,
    }
}

/// validates the destination and returns the staging path it will be built under
fn checkpoint_staging(opt: &ParsedOptions, target: &Path) -> Result<(PathBuf, PathBuf), OpCode> {
    let target = absolute_normalized(target)?;
    if opt.fs.try_exists(&target)? {
        return Err(OpCode::Exist);
    }
    for root in [opt.db_root(), opt.data_root(), opt.log_root()] {
        if overlaps(&target, &absolute_normalized(&root)?) {
            return Err(OpCode::Invalid);
        }
    }
    let mut staging = target.as_os_str().to_os_string();
    staging.push(STAGING_SUFFIX);
    let staging = PathBuf::from(staging);
    for root in [opt.db_root(), opt.data_root(), opt.log_root()] {
        if overlaps(&staging, &absolute_normalized(&root)?) {
            return Err(OpCode::Invalid);
        }
    }
    Ok((target, staging))
}

fn absolute_normalized(path: &Path) -> Result<PathBuf, OpCode> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| OpCode::IoError)?
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    Ok(normalized)
}

fn overlaps(target: &Path, root: &Path) -> bool {
    target == root || target.starts_with(root) || root.starts_with(target)
}

fn prepare_staging(opt: &ParsedOptions, staging: &Path) -> Result<(), OpCode> {
    // create the root atomically so only this call owns cleanup of the staging tree
    if let Some(parent) = staging.parent() {
        opt.fs.create_dir_all(parent)?;
    }
    match opt.fs.create_dir(staging) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(OpCode::Exist);
        }
        Err(err) => return Err(err.into()),
    }
    if let Err(err) = opt.fs.create_dir_all(&staging.join("data")) {
        cleanup_staging(opt, staging);
        return Err(err.into());
    }
    if let Err(err) = opt.fs.create_dir_all(&staging.join("log")) {
        cleanup_staging(opt, staging);
        return Err(err.into());
    }
    Ok(())
}

fn cleanup_staging(opt: &ParsedOptions, staging: &Path) {
    let _ = opt.fs.remove_dir_all(staging);
}

/// reads the copied metadata: payload file ids and bucket frontiers
fn read_copy_metadata(
    manifest_path: &Path,
) -> Result<(Vec<u64>, Vec<u64>, Vec<BucketFrontier>), OpCode> {
    let copy = BTree::open(manifest_path).map_err(|_| OpCode::IoError)?;
    let mut data_files = Vec::new();
    let mut blob_files = Vec::new();
    for (bucket, files) in [
        (BUCKET_DATA_STAT, &mut data_files),
        (BUCKET_BLOB_STAT, &mut blob_files),
    ] {
        copy.view(bucket, |txn| {
            let mut iter = txn.iter();
            let mut key = Vec::new();
            let mut value = Vec::new();
            while iter.next_ref(&mut key, &mut value) {
                files.push(PersistStat::decode_inner_only(&value).file_id);
            }
            Ok(())
        })
        .map_err(|_| OpCode::IoError)?;
    }

    let mut frontiers = Vec::new();
    copy.view(BUCKET_FRONTIER, |txn| {
        let mut iter = txn.iter();
        let mut key = Vec::new();
        let mut value = Vec::new();
        while iter.next_ref(&mut key, &mut value) {
            let item = BucketDurableFrontier::decode(&value);
            frontiers.push(BucketFrontier {
                bucket_id: item.bucket_id,
                groups: group_positions(&item.lsn),
            });
        }
        Ok(())
    })
    .map_err(|_| OpCode::IoError)?;
    drop(copy);
    Ok((data_files, blob_files, frontiers))
}

fn group_positions(positions: &GroupPositions) -> Vec<GroupPosition> {
    positions.iter().copied().map(GroupPosition::from).collect()
}

fn payload_path(opt: &ParsedOptions, kind: FileKind, file_id: u64) -> PathBuf {
    match kind {
        FileKind::Data => opt.data_file(file_id),
        FileKind::Blob => opt.blob_file(file_id),
    }
}

fn copy_payloads(
    opt: &ParsedOptions,
    staging: &Path,
    kind: FileKind,
    file_ids: &[u64],
) -> Result<(), OpCode> {
    let dir = staging.join("data");
    for file_id in file_ids {
        let source = payload_path(opt, kind, *file_id);
        let file_name = source.file_name().ok_or(OpCode::Corruption)?;
        let destination = dir.join(file_name);
        // a published payload file is never written again, so a link is enough; the
        // call falls back to a copy when the destination is on another filesystem
        if opt.fs.hard_link(&source, &destination).is_err() {
            opt.fs.copy_file(&source, &destination)?;
        }
    }
    Ok(())
}

fn copy_wal(opt: &ParsedOptions, staging: &Path, cuts: &[StreamCut]) -> Result<(), OpCode> {
    let dir = staging.join("log");
    for cut in cuts {
        for seq in stream_segments(opt, cut)? {
            let source = opt.physical_wal_path(cut.physical_id, seq);
            let file_name = source.file_name().ok_or(OpCode::Corruption)?;
            let destination = dir.join(file_name);
            if seq == cut.file_id {
                // the cut segment is still the live append target and keeps growing, so
                // it is always copied as the cut prefix: that keeps the copy's WAL tail
                // record aligned and excludes every record above the cut
                copy_prefix(opt, &source, &destination, cut.offset)?;
            } else {
                // a rotated segment never changes again
                opt.fs.copy_file(&source, &destination)?;
            }
        }
    }
    Ok(())
}

/// segment ids this stream must contribute to the copy: the retained (first existing)
/// segment up to the cut, which must be contiguous. ids below the first existing one
/// were already recycled, so they hold no record the copy needs.
fn stream_segments(opt: &ParsedOptions, cut: &StreamCut) -> Result<Vec<u64>, OpCode> {
    let prefix = if cut.physical_id == Options::SHARED_ID {
        format!("{}{}", Options::GROUP_WAL_PREFIX, Options::SEP)
    } else {
        format!(
            "{}{}{}{}",
            Options::WAL_PREFIX,
            Options::SEP,
            cut.physical_id,
            Options::SEP
        )
    };
    let mut ids = Vec::new();
    for path in opt.fs.read_dir(&opt.log_root())? {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(raw) = name.strip_prefix(&prefix) else {
            continue;
        };
        let Ok(seq) = raw.parse::<u64>() else {
            continue;
        };
        if seq >= cut.oldest && seq <= cut.file_id {
            ids.push(seq);
        }
    }
    ids.sort_unstable();
    let Some(first) = ids.first().copied() else {
        return Ok(ids);
    };
    for seq in first..=cut.file_id {
        if !ids.contains(&seq) {
            return Err(OpCode::Corruption);
        }
    }
    Ok(ids)
}

fn copy_prefix(
    opt: &ParsedOptions,
    source: &Path,
    destination: &Path,
    len: u64,
) -> Result<(), OpCode> {
    let reader = open(opt, source, false)?;
    let mut writer = open(opt, destination, true)?;
    let mut buffer = vec![0u8; COPY_CHUNK];
    let mut offset = 0u64;
    while offset < len {
        let want = ((len - offset) as usize).min(buffer.len());
        let read = reader.read(&mut buffer[..want], offset)?;
        if read == 0 {
            return Err(OpCode::Corruption);
        }
        writer.write(&buffer[..read])?;
        offset += read as u64;
    }
    writer.truncate(len)?;
    Ok(())
}

fn open(opt: &ParsedOptions, path: &Path, write: bool) -> Result<File, OpCode> {
    Ok(File::options()
        .read(true)
        .write(write)
        .create(write)
        .trunc(false)
        .open(opt.fs.as_ref(), path)?)
}

/// makes the staged bytes and the staging directory entries durable before publish;
/// returns the number of copied bytes
fn sync_staged_files(opt: &ParsedOptions, staging: &Path) -> Result<u64, OpCode> {
    let mut bytes = 0u64;
    for dir in [staging.join("data"), staging.join("log")] {
        for path in opt.fs.read_dir(&dir)? {
            let mut file = open(opt, &path, true)?;
            bytes += file.size()?;
            file.sync()?;
        }
        opt.fs.sync_dir(&dir)?;
    }
    opt.fs.sync_dir(staging)?;
    Ok(bytes)
}

/// renames the staging directory into place and syncs the parent entry
fn publish(opt: &ParsedOptions, staging: &Path, target: &Path) -> Result<(), OpCode> {
    #[cfg(feature = "failpoints")]
    crate::utils::failpoint::crash("mace_checkpoint_after_write_before_rename");
    opt.fs.rename(staging, target)?;
    #[cfg(feature = "failpoints")]
    crate::utils::failpoint::crash("mace_checkpoint_after_rename_before_dir_sync");
    if let Some(parent) = crate::utils::options::dir_parent_for_sync(target) {
        opt.fs.sync_dir(parent)?;
    }
    #[cfg(feature = "failpoints")]
    crate::utils::failpoint::crash("mace_checkpoint_after_dir_sync");
    Ok(())
}
