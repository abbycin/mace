#![cfg(feature = "extra_check")]

use crate::cc::{
    context::AbortCleanState,
    wal::{EntryType, IWalCodec, WalAbort, WalCheckpoint, WalUpdate, ptr_to, wal_record_sz},
};
use crate::map::buffer::Pool;
use crate::map::data::MetaReader;
use crate::meta::builder::ManifestBuilder;
use crate::meta::{
    BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_MISC, BUCKET_OBSOLETE_BLOB, BUCKET_OBSOLETE_DATA,
    FileKind, IMetaCodec, IntervalPair, PersistStat, interval_bucket_name, page_table_name,
    stat_file_path, stat_intervals,
};
use crate::types::data::{Key, Val, Ver};
use crate::types::header::{NodeType, RemoteHeader, TagFlag, TagKind};
use crate::types::refbox::BoxRef;
use crate::types::traits::IHeader;
use crate::utils::compress::DecompressorPool;
use crate::utils::{Handle, NULL_ADDR};
use crate::{Bucket, OpCode, Options, TxnKV, TxnView};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::{offset_of, size_of};
use std::path::Path;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::sync::mpsc::channel;
use std::thread::JoinHandle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbortCleanStage {
    Pending,
    WaitingQuiesce,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AbortCleanTaskInfo {
    pub group_id: u8,
    pub tail_file_id: u64,
    pub tail_offset: u64,
    pub pin_file_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalUpdateProbe {
    pub physical_wal_id: u8,
    pub logical_group_id: u8,
    pub file_id: u64,
    pub offset: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalRecordKind {
    Begin,
    Update,
    Commit,
    Abort,
    Checkpoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalRecordProbe {
    pub physical_wal_id: u8,
    pub kind: WalRecordKind,
    pub txid: u64,
    pub logical_group_id: Option<u8>,
    pub file_id: u64,
    pub offset: u64,
    pub len: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnCommitSyncPoint {
    AfterFactWriteGuardBeforeCommitTimestamp,
    AfterCommitTimestampBeforeOutcomePublish,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnBeginSyncPoint {
    AfterBeginTimestampBeforeFactPublish,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewSyncPoint {
    AfterCcnodeRegisteringBeforeTimestampSample,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectorSyncPoint {
    AfterCollectorCutBeforeFactScan,
    AfterSafePublishBeforeCommittedFactPrune,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VisibilitySyncPoint {
    AfterPositiveFastPathBeforeRetainedAbortCheck,
    AfterProofMissBeforeExactRead,
    AfterExactMissBeforeSecondSafeRead,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnAbortSyncPoint {
    AfterAbortFloorBeforeAbortedFactPublish,
    AfterAbortFactBeforeAbortCleanEnqueue,
    AfterAbortCleanEnqueueBeforeLoggingRelease,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbortCleanSyncPoint {
    AfterQuiesceCallback,
    AfterCorruption,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TreeUpdateSyncPoint {
    AfterFindLeafBeforeLink,
    AfterLatestMetaCheckBeforeDeltaInsert,
    AfterTreeAgainBeforeLatestMetaRecheck,
    AfterIteratorPageCaptureBeforeCandidateWalk,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointSyncPoint {
    AfterRetiredPageDeferred(u64),
    BeforeSnapshotWaitZero,
    AfterRetiredPageFallbackRead(u64),
    AfterRetiredPageGuardDropped,
    AfterSnapshotBuilt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalSyncPoint {
    /// a durable commit/abort/barrier registered as the generation leader and
    /// is about to re-acquire the shared logging lock to seal
    AfterLeaderRegisterBeforeSeal,
    /// a durable commit/abort/barrier joined the in-flight generation as a
    /// follower and is about to wait for its completion
    AfterFollowerRegister,
    /// the leader sealed the cut and is about to sync the generation files
    AfterSealBeforeFileSync,
    /// the leader published the generation completion and woke every waiter
    AfterGenerationComplete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcRewriteSyncPoint {
    BeforeDataPublish,
    BeforeBlobPublish,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcStatSyncPoint {
    CheckpointBeforeManifestCommit,
    DataObsoleteAfterMetaCommit,
    DataObsoleteAfterRetiredMark,
    DataObsoleteAfterRuntimeRemove,
    BlobObsoleteAfterMetaCommit,
    BlobObsoleteAfterRetiredMark,
    BlobObsoleteAfterRuntimeRemove,
    DataRewritePublishing { output_count: usize },
    BlobRewritePublishing { output_count: usize },
    DataCheckpointPublishingWait,
    BlobCheckpointPublishingWait,
}

pub struct CheckpointRootRestore {
    root: Mutex<Option<(BoxRef, u64)>>,
}

impl Default for CheckpointRootRestore {
    fn default() -> Self {
        Self {
            root: Mutex::new(None),
        }
    }
}

impl CheckpointRootRestore {
    pub fn new() -> Self {
        Self::default()
    }

    fn install(&self, root: BoxRef, old_link: u64) {
        *self.root.lock() = Some((root, old_link));
    }

    pub fn restore(&self) {
        if let Some((mut root, old_link)) = self.root.lock().take() {
            root.header_mut().link = old_link;
        }
    }
}

pub fn spawn_checkpoint_retired_page_writer(
    bucket: &Bucket,
    ready: Arc<Barrier>,
    start: Arc<Barrier>,
    restore: Arc<CheckpointRootRestore>,
) -> JoinHandle<()> {
    let pool: Handle<Pool> = bucket.tree.bucket.pool;
    let writer_bucket = bucket.clone();
    std::thread::spawn(move || {
        let epoch = pool.capture_epoch();
        let tx = writer_bucket.begin().expect("test writer begin");
        tx.put("__checkpoint_retired_page", b"v")
            .expect("test writer put");
        tx.commit().expect("test writer commit");
        let (_, root_addr) = epoch
            .dirty_roots
            .first_live()
            .expect("test writer requires a dirty root");
        let mut root = epoch
            .pages
            .get(&root_addr)
            .expect("test writer root must remain dirty")
            .value()
            .clone();
        let old_link = root.header().link;
        restore.install(root.clone(), old_link);

        ready.wait();
        start.wait();
        let guard = crossbeam_epoch::pin();
        let mut target =
            pool.alloc_in(&epoch.pages, &epoch.bytes, size_of::<RemoteHeader>() as u32);
        let target_addr = target.header().addr;
        {
            let h = target.header_mut();
            h.kind = TagKind::Remote;
            h.node_type = NodeType::Leaf;
            h.link = old_link;
        }
        root.header_mut().link = target_addr;
        pool.test_retire_page(&epoch, &guard, target_addr);
        drop(epoch);
        drop(guard);
        // advance the collector after releasing the writer-side protection
        for _ in 0..16 {
            let guard = crossbeam_epoch::pin();
            guard.flush();
            drop(guard);
            std::thread::yield_now();
        }
        fire_checkpoint_sync_point(
            CheckpointSyncPoint::AfterRetiredPageGuardDropped,
            writer_bucket.id(),
        );
    })
}

pub fn checkpoint_and_wait(bucket: &Bucket) {
    bucket.tree.bucket.checkpoint_and_wait(false);
}

pub fn data_rewrite_collected_junk_count(bucket: &Bucket) -> usize {
    bucket
        .inner
        .store
        .manifest
        .stat_ctx(FileKind::Data)
        .collected_junk_count()
}

/// verifies that the stable persisted GC ledger exactly matches physical reachability
pub fn assert_persisted_gc_stats(mace: &crate::Mace) {
    for kind in FileKind::ALL {
        assert_persisted_gc_stats_for_kind(mace, kind);
    }
}

fn assert_persisted_gc_stats_for_kind(mace: &crate::Mace, kind: FileKind) {
    let manifest = &mace.inner.store.manifest;
    let stat_bucket = match kind {
        FileKind::Data => BUCKET_DATA_STAT,
        FileKind::Blob => BUCKET_BLOB_STAT,
    };
    let obsolete_bucket = match kind {
        FileKind::Data => BUCKET_OBSOLETE_DATA,
        FileKind::Blob => BUCKET_OBSOLETE_BLOB,
    };
    let mut stats = BTreeMap::<u64, PersistStat>::new();
    manifest
        .btree
        .view(stat_bucket, |txn| {
            let mut iter = txn.iter_uncached();
            let mut key = Vec::new();
            let mut value = Vec::new();
            while iter.next_ref(&mut key, &mut value) {
                let file_id =
                    u64::from_le_bytes(key.as_slice().try_into().expect("stat key must be a u64"));
                let stat = PersistStat::decode(&value);
                assert_eq!(
                    file_id, stat.file_id,
                    "{kind:?} stat key and payload file id disagree"
                );
                assert!(
                    stats.insert(file_id, stat).is_none(),
                    "duplicate {kind:?} stat for file {file_id}"
                );
            }
            Ok(())
        })
        .expect("read persisted stat metadata");

    let expected_active = stats
        .values()
        .map(|stat| stat.active_size as u64)
        .sum::<u64>();
    let expected_total = stats
        .values()
        .map(|stat| stat.total_size as u64)
        .sum::<u64>();
    let (runtime_active, runtime_total) = manifest.stat_ctx(kind).sizes();
    assert_eq!(
        (runtime_active, runtime_total),
        (expected_active, expected_total),
        "runtime {kind:?} aggregate sizes must match durable stats"
    );

    let mut obsolete = HashSet::new();
    manifest
        .btree
        .view(obsolete_bucket, |txn| {
            let mut iter = txn.iter_uncached();
            let mut key = Vec::new();
            let mut value = Vec::new();
            while iter.next_ref(&mut key, &mut value) {
                obsolete.insert(u64::from_le_bytes(
                    key.as_slice()
                        .try_into()
                        .expect("obsolete key must be a u64"),
                ));
            }
            Ok(())
        })
        .expect("read obsolete metadata");
    assert!(
        obsolete.is_empty(),
        "stable {kind:?} accounting check found obsolete files awaiting cleanup: {obsolete:?}"
    );

    let mut interval_meta = HashSet::new();
    let mut bucket_ids = stats
        .values()
        .map(|stat| stat.bucket_id)
        .collect::<BTreeSet<_>>();
    bucket_ids.extend(persisted_interval_bucket_ids(mace, kind));
    for bucket_id in bucket_ids {
        let bucket = interval_bucket_name(kind, bucket_id);
        manifest
            .btree
            .view(&bucket, |txn| {
                let mut iter = txn.iter_uncached();
                let mut key = Vec::new();
                let mut value = Vec::new();
                while iter.next_ref(&mut key, &mut value) {
                    let interval = IntervalPair::decode(&value);
                    assert_eq!(
                        interval.bucket_id, bucket_id,
                        "{kind:?} interval belongs to the wrong bucket"
                    );
                    interval_meta.insert((
                        interval.lo_addr,
                        interval.hi_addr,
                        interval.file_id,
                        interval.bucket_id,
                    ));
                }
                Ok(())
            })
            .expect("read persisted interval metadata");
    }

    for loaded in manifest.buckets.buckets.iter() {
        let bucket_id = *loaded.key();
        let expected = interval_meta
            .iter()
            .filter(|entry| entry.3 == bucket_id)
            .copied()
            .collect::<HashSet<_>>();
        let actual = stat_intervals(kind, loaded.value())
            .read()
            .entries()
            .map(|(lo, hi, file_id)| (lo, hi, file_id, bucket_id))
            .collect::<HashSet<_>>();
        assert_eq!(
            actual, expected,
            "runtime {kind:?} intervals must match durable intervals for bucket {bucket_id}"
        );
        for (_, _, file_id, _) in actual {
            assert!(
                manifest.stat_ctx(kind).contains_key(&file_id),
                "runtime {kind:?} interval references missing stat file {file_id}"
            );
        }
    }

    let mut payload_intervals = HashSet::new();
    let mut reloc_index = BTreeMap::new();
    for (&file_id, stat) in &stats {
        let path = stat_file_path(&mace.inner.store.opt, kind, file_id);
        assert!(
            mace.inner
                .store
                .opt
                .fs
                .try_exists(&path)
                .expect("check retained payload file existence"),
            "retained {kind:?} stat file {file_id} is missing: {path:?}"
        );
        let mut reader = MetaReader::new(mace.inner.store.opt.fs.as_ref(), &path);
        let relocs = reader.get_reloc();
        let intervals = reader.get_interval();
        let mut sequences = BTreeSet::new();
        let mut total_size = 0usize;
        for entry in relocs.iter() {
            let addr = entry.key;
            let reloc = entry.val;
            let seq = reloc.seq;
            assert!(
                sequences.insert(seq),
                "duplicate {kind:?} relocation sequence {} in file {file_id}",
                seq
            );
            assert!(
                reloc_index
                    .insert((stat.bucket_id, addr), (file_id, seq))
                    .is_none(),
                "duplicate {kind:?} relocation address {} in bucket {}",
                addr,
                stat.bucket_id
            );
            total_size += reloc.active_len() as usize;
        }
        assert_eq!(
            sequences.len(),
            relocs.len(),
            "{kind:?} relocation sequence count mismatch for file {file_id}"
        );
        assert_eq!(
            stat.total_elems as usize,
            relocs.len(),
            "{kind:?} stat total_elems mismatch for file {file_id}"
        );
        assert_eq!(
            stat.total_size, total_size,
            "{kind:?} stat total_size mismatch for file {file_id}"
        );

        let inactive = stat.inactive_elems.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(
            inactive.len(),
            stat.inactive_elems.len(),
            "duplicate {kind:?} inactive sequence in stat file {file_id}"
        );
        assert!(
            inactive.is_subset(&sequences),
            "{kind:?} stat contains an inactive sequence absent from file {file_id}"
        );
        let active_size = relocs.iter().fold(0usize, |sum, entry| {
            let reloc = entry.val;
            let seq = reloc.seq;
            if inactive.contains(&seq) {
                sum
            } else {
                sum + reloc.active_len() as usize
            }
        });
        assert_eq!(
            stat.active_elems as usize,
            relocs.len() - inactive.len(),
            "{kind:?} stat active_elems mismatch for file {file_id}"
        );
        assert_eq!(
            stat.active_size, active_size,
            "{kind:?} stat active_size mismatch for file {file_id}"
        );

        for interval in intervals.iter() {
            let lo = interval.lo;
            let hi = interval.hi;
            assert!(payload_intervals.insert((lo, hi, file_id, stat.bucket_id)));
        }
    }
    assert_eq!(
        interval_meta, payload_intervals,
        "{kind:?} manifest intervals must exactly match retained payload intervals"
    );

    if kind == FileKind::Data {
        assert_persisted_reachability(mace, &stats, &reloc_index);
    }

    let orphan_prefix = match kind {
        FileKind::Data => crate::meta::ORPHAN_DATA_MARKER_PREFIX,
        FileKind::Blob => crate::meta::ORPHAN_BLOB_MARKER_PREFIX,
    };
    manifest
        .btree
        .view(BUCKET_MISC, |txn| {
            let mut iter = txn.iter_uncached();
            let mut key = Vec::new();
            let mut value = Vec::new();
            while iter.next_ref(&mut key, &mut value) {
                assert!(
                    !key.starts_with(orphan_prefix.as_bytes()),
                    "stable {kind:?} accounting check found orphan marker {:?}",
                    String::from_utf8_lossy(&key)
                );
            }
            Ok(())
        })
        .expect("read orphan metadata");

    let root = mace.options().data_root();
    let prefix = match kind {
        FileKind::Data => Options::DATA_PREFIX,
        FileKind::Blob => Options::BLOB_PREFIX,
    };
    for path in mace
        .inner
        .store
        .opt
        .fs
        .read_dir(&root)
        .expect("read payload directory")
    {
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(file_id) = file_name
            .strip_prefix(prefix)
            .and_then(|name| name.strip_prefix('_'))
            .and_then(|id| id.parse::<u64>().ok())
        else {
            continue;
        };
        assert!(
            stats.contains_key(&file_id),
            "unaccounted retained {kind:?} file {file_id}: {:?}",
            path
        );
    }
}

fn persisted_interval_bucket_ids(mace: &crate::Mace, kind: FileKind) -> BTreeSet<u64> {
    let prefix = match kind {
        FileKind::Data => "data_interval_",
        FileKind::Blob => "blob_interval_",
    };
    mace.inner
        .store
        .manifest
        .btree
        .buckets()
        .expect("list persisted metadata buckets")
        .into_iter()
        .filter_map(|name| name.strip_prefix(prefix)?.parse::<u64>().ok())
        .collect()
}

fn assert_persisted_reachability(
    mace: &crate::Mace,
    data_stats: &BTreeMap<u64, PersistStat>,
    data_relocs: &BTreeMap<(u64, u64), (u64, u32)>,
) {
    let manifest = &mace.inner.store.manifest;
    let blob_stats = load_persisted_stats(mace, FileKind::Blob);
    let blob_relocs = load_reloc_index(mace, FileKind::Blob, &blob_stats);
    let decoders = DecompressorPool::new();
    let mut readers = BTreeMap::new();
    let mut pending_data = Vec::new();
    let mut reachable_data = BTreeSet::new();
    let mut reachable_blob = BTreeSet::new();

    let bucket_ids = data_stats
        .values()
        .map(|stat| stat.bucket_id)
        .collect::<BTreeSet<_>>();
    for bucket_id in bucket_ids {
        manifest
            .btree
            .view(&page_table_name(bucket_id), |txn| {
                let mut iter = txn.iter_uncached();
                let mut key = Vec::new();
                let mut value = Vec::new();
                while iter.next_ref(&mut key, &mut value) {
                    let addr = u64::from_le_bytes(
                        value
                            .as_slice()
                            .try_into()
                            .expect("page table address must be a u64"),
                    );
                    if addr != NULL_ADDR {
                        pending_data.push((bucket_id, addr));
                    }
                }
                Ok(())
            })
            .expect("read persisted page table");
    }

    let mut pending_blob = Vec::new();
    while let Some((bucket_id, addr)) = pending_data.pop() {
        if !reachable_data.insert((bucket_id, addr)) {
            continue;
        }
        let (file_id, _) = data_relocs
            .get(&(bucket_id, addr))
            .copied()
            .unwrap_or_else(|| {
                panic!("reachable data address {addr} in bucket {bucket_id} has no relocation")
            });
        let reader = readers.entry(file_id).or_insert_with(|| {
            crate::meta::new_reader(
                stat_file_path(&mace.inner.store.opt, FileKind::Data, file_id),
                decoders.clone(),
                mace.inner.store.opt.fs.clone(),
            )
        });
        let page = reader.read_at(addr);
        let header = page.header();
        if header.link != NULL_ADDR {
            pending_data.push((bucket_id, header.link));
        }
        match (header.kind, header.node_type) {
            (TagKind::Base, NodeType::Leaf) => {
                let base = page.view().as_base();
                for idx in 0..base.header().elems as usize {
                    let val = if header.flag == TagFlag::Sibling {
                        let (_, val) = base.sst::<Ver>().kv_at::<Val>(idx);
                        val
                    } else {
                        let (_, val) = base.sst::<Key>().kv_at::<Val>(idx);
                        val
                    };
                    if let Some(hist) = val.get_hist() {
                        pending_data.push((bucket_id, hist.page_addr));
                    }
                    let remote = val.get_remote();
                    if remote != NULL_ADDR {
                        pending_blob.push((bucket_id, remote));
                    }
                }
            }
            (TagKind::Delta, NodeType::Leaf) => {
                let val = page.view().as_delta().val();
                if let Some(hist) = val.get_hist() {
                    pending_data.push((bucket_id, hist.page_addr));
                }
                let remote = val.get_remote();
                if remote != NULL_ADDR {
                    pending_blob.push((bucket_id, remote));
                }
            }
            _ => {}
        }
    }

    readers.clear();
    while let Some((bucket_id, addr)) = pending_blob.pop() {
        if !reachable_blob.insert((bucket_id, addr)) {
            continue;
        }
        let (file_id, _) = blob_relocs
            .get(&(bucket_id, addr))
            .copied()
            .unwrap_or_else(|| {
                panic!("reachable blob address {addr} in bucket {bucket_id} has no relocation")
            });
        let reader = readers.entry(file_id).or_insert_with(|| {
            crate::meta::new_reader(
                stat_file_path(&mace.inner.store.opt, FileKind::Blob, file_id),
                decoders.clone(),
                mace.inner.store.opt.fs.clone(),
            )
        });
        let blob = reader.read_at(addr);
        if blob.header().link != NULL_ADDR {
            pending_blob.push((bucket_id, blob.header().link));
        }
    }

    assert_reachability_matches_inactive(FileKind::Data, data_stats, data_relocs, &reachable_data);
    assert_reachability_matches_inactive(
        FileKind::Blob,
        &blob_stats,
        &blob_relocs,
        &reachable_blob,
    );
}

fn load_persisted_stats(mace: &crate::Mace, kind: FileKind) -> BTreeMap<u64, PersistStat> {
    let bucket = match kind {
        FileKind::Data => BUCKET_DATA_STAT,
        FileKind::Blob => BUCKET_BLOB_STAT,
    };
    let mut stats = BTreeMap::new();
    mace.inner
        .store
        .manifest
        .btree
        .view(bucket, |txn| {
            let mut iter = txn.iter_uncached();
            let mut key = Vec::new();
            let mut value = Vec::new();
            while iter.next_ref(&mut key, &mut value) {
                let file_id =
                    u64::from_le_bytes(key.as_slice().try_into().expect("stat key must be a u64"));
                assert!(stats.insert(file_id, PersistStat::decode(&value)).is_none());
            }
            Ok(())
        })
        .expect("read persisted stats");
    stats
}

fn load_reloc_index(
    mace: &crate::Mace,
    kind: FileKind,
    stats: &BTreeMap<u64, PersistStat>,
) -> BTreeMap<(u64, u64), (u64, u32)> {
    let mut relocs = BTreeMap::new();
    for (&file_id, stat) in stats {
        let path = stat_file_path(&mace.inner.store.opt, kind, file_id);
        let mut reader = MetaReader::new(mace.inner.store.opt.fs.as_ref(), &path);
        for entry in reader.get_reloc().iter() {
            let addr = entry.key;
            let seq = entry.val.seq;
            assert!(
                relocs
                    .insert((stat.bucket_id, addr), (file_id, seq))
                    .is_none(),
                "duplicate {kind:?} relocation address {} in bucket {}",
                addr,
                stat.bucket_id
            );
        }
    }
    relocs
}

fn assert_reachability_matches_inactive(
    kind: FileKind,
    stats: &BTreeMap<u64, PersistStat>,
    relocs: &BTreeMap<(u64, u64), (u64, u32)>,
    reachable: &BTreeSet<(u64, u64)>,
) {
    for (&(bucket_id, addr), &(file_id, seq)) in relocs {
        let stat = stats
            .get(&file_id)
            .unwrap_or_else(|| panic!("{kind:?} relocation file {file_id} has no stat"));
        let inactive = stat.inactive_elems.contains(&seq);
        assert_eq!(
            inactive,
            !reachable.contains(&(bucket_id, addr)),
            "{kind:?} relocation {addr} in bucket {bucket_id} (file {file_id}, seq {seq}) reachability and inactive bitmap disagree"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn hooks_lock_blocks_second_holder_until_release() {
        let guard = crate::testing::hooks_lock();
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let _second = crate::testing::hooks_lock();
            tx.send(()).expect("main thread must stay alive");
        });

        // while the first guard is held the second holder must stay blocked
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "hooks_lock must serialize holders"
        );
        drop(guard);
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("second holder must acquire after release");
        handle.join().expect("lock thread must not panic");
    }

    #[test]
    fn persisted_gc_stats_rejects_interval_without_stat() -> Result<(), OpCode> {
        let path = crate::RandomPath::new();
        let mut opt = crate::Options::new(&*path);
        opt.tmp_store = true;
        let mace = crate::Mace::new(opt.validate()?)?;
        let bucket_id = 99;
        let interval = IntervalPair::new(1, 1, 99, bucket_id);
        let mut value = vec![0; interval.packed_size()];
        interval.encode(&mut value);
        mace.inner
            .store
            .manifest
            .btree
            .new_bucket(&interval_bucket_name(FileKind::Data, bucket_id), false)
            .expect("create orphan interval bucket");
        mace.inner
            .store
            .manifest
            .btree
            .exec(&interval_bucket_name(FileKind::Data, bucket_id), |txn| {
                txn.put(interval.lo_addr.to_le_bytes(), value)
            })
            .expect("insert interval-only metadata");

        let panic = catch_unwind(AssertUnwindSafe(|| assert_persisted_gc_stats(&mace)))
            .expect_err("oracle accepted interval metadata without a stat");
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or_default();
        assert!(
            message.contains("manifest intervals must exactly match retained payload intervals"),
            "interval-only metadata should fail the interval equality check, got: {message}"
        );
        Ok(())
    }
}

pub fn checkpoint_test_lock() -> parking_lot::MutexGuard<'static, ()> {
    // legacy alias: every hook-armed test must serialize on the single global
    // hooks lock, or a parallel HookReset::drop (clear_hooks) erases another
    // test's slot mid-flight
    hooks_lock()
}

/// process-wide serialization for every test that installs testing hooks;
/// cargo runs tests in parallel and the hook table is global, so hook-based
/// tests must hold this while registered to avoid cross-test interference
pub fn hooks_lock() -> parking_lot::MutexGuard<'static, ()> {
    static LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    LOCK.lock()
}

type TxnCommitHook = dyn Fn(TxnCommitSyncPoint, u64) + Send + Sync + 'static;
type TxnBeginHook = dyn Fn(TxnBeginSyncPoint, u64) + Send + Sync + 'static;
type ViewHook = dyn Fn(ViewSyncPoint) + Send + Sync + 'static;
type CollectorHook = dyn Fn(CollectorSyncPoint, u64) + Send + Sync + 'static;
type VisibilityHook = dyn Fn(VisibilitySyncPoint, u64) + Send + Sync + 'static;
type TxnAbortHook = dyn Fn(TxnAbortSyncPoint, u64) + Send + Sync + 'static;
type AbortCleanHook = dyn Fn(AbortCleanSyncPoint, u64) + Send + Sync + 'static;
type TreeUpdateHook = dyn Fn(TreeUpdateSyncPoint, u64) + Send + Sync + 'static;
type CheckpointHook = dyn Fn(CheckpointSyncPoint, u64) + Send + Sync + 'static;
type WalSyncHook = dyn Fn(WalSyncPoint) + Send + Sync + 'static;
type GcRewriteHook = dyn Fn(GcRewriteSyncPoint, u64, &Path) + Send + Sync + 'static;
type GcStatHook = dyn Fn(GcStatSyncPoint, u64, &Path) + Send + Sync + 'static;
type GcCompletedHook = dyn Fn() + Send + Sync + 'static;
type CollectorCompletedHook = dyn Fn(usize) + Send + Sync + 'static;
type EvictorCompletedHook = dyn Fn() + Send + Sync + 'static;

#[derive(Default)]
struct TestingHooks {
    txn_commit: Option<Arc<TxnCommitHook>>,
    txn_begin: Option<Arc<TxnBeginHook>>,
    view: Option<Arc<ViewHook>>,
    collector: Option<Arc<CollectorHook>>,
    visibility: Option<Arc<VisibilityHook>>,
    txn_abort: Option<Arc<TxnAbortHook>>,
    abort_clean: Option<Arc<AbortCleanHook>>,
    tree_update: Option<Arc<TreeUpdateHook>>,
    checkpoint: Option<Arc<CheckpointHook>>,
    wal_sync: Option<Arc<WalSyncHook>>,
    gc_rewrite: Option<Arc<GcRewriteHook>>,
    gc_stat: Option<Arc<GcStatHook>>,
    gc_completed: Option<Arc<GcCompletedHook>>,
    collector_completed: Option<Arc<CollectorCompletedHook>>,
    evictor_completed: Option<Arc<EvictorCompletedHook>>,
}

fn hooks() -> &'static Mutex<TestingHooks> {
    static HOOKS: OnceLock<Mutex<TestingHooks>> = OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(TestingHooks::default()))
}

pub fn set_txn_commit_hook(hook: Option<Arc<TxnCommitHook>>) {
    hooks().lock().txn_commit = hook;
}

pub fn set_txn_begin_hook(hook: Option<Arc<TxnBeginHook>>) {
    hooks().lock().txn_begin = hook;
}

pub fn set_view_hook(hook: Option<Arc<ViewHook>>) {
    hooks().lock().view = hook;
}

pub fn set_collector_hook(hook: Option<Arc<CollectorHook>>) {
    hooks().lock().collector = hook;
}

pub fn set_visibility_hook(hook: Option<Arc<VisibilityHook>>) {
    hooks().lock().visibility = hook;
}

pub fn set_txn_abort_hook(hook: Option<Arc<TxnAbortHook>>) {
    hooks().lock().txn_abort = hook;
}

pub fn set_abort_clean_hook(hook: Option<Arc<AbortCleanHook>>) {
    hooks().lock().abort_clean = hook;
}

pub fn set_tree_update_hook(hook: Option<Arc<TreeUpdateHook>>) {
    hooks().lock().tree_update = hook;
}

pub fn set_checkpoint_hook(hook: Option<Arc<CheckpointHook>>) {
    hooks().lock().checkpoint = hook;
}

pub fn set_wal_sync_hook(hook: Option<Arc<WalSyncHook>>) {
    hooks().lock().wal_sync = hook;
}

pub fn set_gc_rewrite_hook(hook: Option<Arc<GcRewriteHook>>) {
    hooks().lock().gc_rewrite = hook;
}

pub fn set_gc_stat_hook(hook: Option<Arc<GcStatHook>>) {
    hooks().lock().gc_stat = hook;
}

pub fn set_gc_completed_hook(hook: Option<Arc<GcCompletedHook>>) {
    hooks().lock().gc_completed = hook;
}

pub fn set_collector_completed_hook(hook: Option<Arc<CollectorCompletedHook>>) {
    hooks().lock().collector_completed = hook;
}

pub fn set_evictor_completed_hook(hook: Option<Arc<EvictorCompletedHook>>) {
    hooks().lock().evictor_completed = hook;
}

pub fn clear_txn_commit_hook() {
    set_txn_commit_hook(None);
}

pub fn clear_txn_begin_hook() {
    set_txn_begin_hook(None);
}

pub fn clear_view_hook() {
    set_view_hook(None);
}

pub fn clear_collector_hook() {
    set_collector_hook(None);
}

pub fn clear_visibility_hook() {
    set_visibility_hook(None);
}

pub fn clear_txn_abort_hook() {
    set_txn_abort_hook(None);
}

pub fn clear_abort_clean_hook() {
    set_abort_clean_hook(None);
}

pub fn clear_tree_update_hook() {
    set_tree_update_hook(None);
}

pub fn clear_checkpoint_hook() {
    set_checkpoint_hook(None);
}

pub fn clear_wal_sync_hook() {
    set_wal_sync_hook(None);
}

pub fn clear_gc_rewrite_hook() {
    set_gc_rewrite_hook(None);
}

pub fn clear_gc_stat_hook() {
    set_gc_stat_hook(None);
}

pub fn clear_gc_completed_hook() {
    set_gc_completed_hook(None);
}

pub fn clear_collector_completed_hook() {
    set_collector_completed_hook(None);
}

pub fn clear_evictor_completed_hook() {
    set_evictor_completed_hook(None);
}

pub fn clear_hooks() {
    clear_txn_commit_hook();
    clear_txn_begin_hook();
    clear_view_hook();
    clear_collector_hook();
    clear_visibility_hook();
    clear_txn_abort_hook();
    clear_abort_clean_hook();
    clear_tree_update_hook();
    clear_checkpoint_hook();
    clear_wal_sync_hook();
    clear_gc_rewrite_hook();
    clear_gc_stat_hook();
    clear_gc_completed_hook();
    clear_collector_completed_hook();
    clear_evictor_completed_hook();
}

pub(crate) fn fire_gc_completed() {
    let hook = hooks().lock().gc_completed.clone();
    if let Some(hook) = hook {
        hook();
    }
}

pub(crate) fn fire_collector_completed(collector_token: usize) {
    let hook = hooks().lock().collector_completed.clone();
    if let Some(hook) = hook {
        hook(collector_token);
    }
}

pub(crate) fn fire_evictor_completed() {
    let hook = hooks().lock().evictor_completed.clone();
    if let Some(hook) = hook {
        hook();
    }
}

pub(crate) fn fire_txn_commit_sync_point(point: TxnCommitSyncPoint, start_ts: u64) {
    let hook = hooks().lock().txn_commit.clone();
    if let Some(hook) = hook {
        hook(point, start_ts);
    }
}

pub(crate) fn fire_txn_begin_sync_point(point: TxnBeginSyncPoint, start_ts: u64) {
    let hook = hooks().lock().txn_begin.clone();
    if let Some(hook) = hook {
        hook(point, start_ts);
    }
}

pub(crate) fn fire_view_sync_point(point: ViewSyncPoint) {
    let hook = hooks().lock().view.clone();
    if let Some(hook) = hook {
        hook(point);
    }
}

pub(crate) fn fire_collector_sync_point(point: CollectorSyncPoint, value: u64) {
    let hook = hooks().lock().collector.clone();
    if let Some(hook) = hook {
        hook(point, value);
    }
}

pub(crate) fn fire_visibility_sync_point(point: VisibilitySyncPoint, txid: u64) {
    let hook = hooks().lock().visibility.clone();
    if let Some(hook) = hook {
        hook(point, txid);
    }
}

pub(crate) fn fire_txn_abort_sync_point(point: TxnAbortSyncPoint, start_ts: u64) {
    let hook = hooks().lock().txn_abort.clone();
    if let Some(hook) = hook {
        hook(point, start_ts);
    }
}

pub(crate) fn fire_abort_clean_sync_point(point: AbortCleanSyncPoint, txid: u64) {
    let hook = hooks().lock().abort_clean.clone();
    if let Some(hook) = hook {
        hook(point, txid);
    }
}

pub(crate) fn fire_tree_update_sync_point(point: TreeUpdateSyncPoint, page_pid: u64) {
    let hook = hooks().lock().tree_update.clone();
    if let Some(hook) = hook {
        hook(point, page_pid);
    }
}

pub(crate) fn fire_checkpoint_sync_point(point: CheckpointSyncPoint, bucket_id: u64) {
    let hook = hooks().lock().checkpoint.clone();
    if let Some(hook) = hook {
        hook(point, bucket_id);
    }
}

pub(crate) fn fire_wal_sync_point(point: WalSyncPoint) {
    let hook = hooks().lock().wal_sync.clone();
    if let Some(hook) = hook {
        hook(point);
    }
}

pub(crate) fn fire_gc_rewrite_sync_point(
    point: GcRewriteSyncPoint,
    bucket_id: u64,
    db_root: &Path,
) {
    let hook = hooks().lock().gc_rewrite.clone();
    if let Some(hook) = hook {
        hook(point, bucket_id, db_root);
    }
}

pub(crate) fn fire_gc_stat_sync_point(point: GcStatSyncPoint, bucket_id: u64, db_root: &Path) {
    let hook = hooks().lock().gc_stat.clone();
    if let Some(hook) = hook {
        hook(point, bucket_id, db_root);
    }
}

pub fn safe_exclusive(bucket: &Bucket) -> u64 {
    bucket.inner.store.context.safe_exclusive()
}

pub fn wake_cc_collector(bucket: &Bucket) {
    bucket.inner.store.context.request_collect();
}

/// stable identity of this engine's collector, used by the test harness to
/// accept only this engine's cycle-completed signal (the hook is process
/// global, so parallel tests' collectors must not be able to fire it)
pub fn collector_completion_token(bucket: &Bucket) -> usize {
    let ctx: &crate::cc::context::Context = &bucket.inner.store.context;
    &*ctx.sequences as *const _ as usize
}

pub fn view_start_ts(view: &TxnView<'_>) -> u64 {
    view.testing_start_ts()
}

pub fn txn_start_ts(txn: &TxnKV<'_>) -> u64 {
    txn.testing_start_ts()
}

pub fn fact_present(bucket: &Bucket, group_id: usize, txid: u64) -> bool {
    bucket
        .inner
        .store
        .context
        .group(group_id)
        .facts
        .contains_key(&txid)
}

pub fn wal_durable_pos(bucket: &Bucket, group_id: usize) -> (u64, u64) {
    let ctx = &bucket.inner.store.context;
    let log = ctx.group(group_id).logging.lock();
    let pos = log.durable_pos();
    (pos.file_id, pos.offset)
}

pub fn wal_recycle_boundary(bucket: &Bucket, physical_group_id: u8) -> u64 {
    bucket
        .inner
        .store
        .manifest
        .load_wal_recycle_state(physical_group_id)
        .oldest_id()
}

/// durable mode: every writer group shares one logging Arc; relaxed mode:
/// every group owns its own
pub fn loggings_shared_across_groups(bucket: &Bucket) -> bool {
    let ctx = &bucket.inner.store.context;
    Arc::ptr_eq(&ctx.group(0).logging, &ctx.group(1).logging)
}

/// durable mode: the shared logging's per-group checkpoint floor slot
pub fn shared_checkpoint_floor(bucket: &Bucket, logical_group: usize) -> (u64, u64) {
    let ctx = &bucket.inner.store.context;
    let shared = ctx
        .shared_logging()
        .expect("durable mode has a shared logging");
    let pos = shared.lock().logical_checkpoint_at(logical_group);
    (pos.file_id, pos.offset)
}

pub fn group_checkpoint_count(bucket: &Bucket, logical_group: usize) -> usize {
    bucket
        .inner
        .store
        .context
        .group(logical_group)
        .ckpt_cnt
        .load(std::sync::atomic::Ordering::Relaxed)
}

pub fn rewrite_persisted_sync_on_write(
    options: Options,
    sync_on_write: bool,
) -> Result<(), OpCode> {
    let parsed = Arc::new(options.validate()?);
    let (tx, _rx) = channel();
    let (_ack_tx, ack_rx) = channel();
    let mut builder = ManifestBuilder::new_with_channels(parsed, tx, ack_rx);
    builder.load()?;
    let manifest = Handle::new(builder.finish());
    let result = (|| {
        let mut persisted = manifest
            .load_persisted_options_if_present()?
            .ok_or(OpCode::Corruption)?;
        persisted.sync_on_write = sync_on_write;
        manifest.store_persisted_options(&persisted)
    })();
    manifest.reclaim();
    result
}

/// fail the next `n` generation sync attempts on the shared durable stream
/// (extra_check fault hook; 0 disables)
pub fn fail_next_wal_syncs(bucket: &Bucket, n: usize) {
    let ctx = &bucket.inner.store.context;
    let log = ctx.group(0).logging.lock();
    log.fail_next_syncs(n);
}

/// arm in-process failpoint rules without touching the process env; the rules
/// override any env-derived rule with the same name and survive the per-hit
/// env refresh. used by failpoint test children that must crash a site only
/// after a deterministic setup sequence (mid-process env mutation would race
/// the background threads' env reads)
#[cfg(feature = "failpoints")]
pub fn arm_failpoint_rule(raw: &str) {
    crate::utils::failpoint::arm_rules(raw);
}

/// total consultations of a named failpoint rule in this process, regardless
/// of whether its action fired; zero during a crash-window timeout means the
/// engine never reached the injection site, while hits without an abort mean
/// the failpoint state machine failed to act
#[cfg(feature = "failpoints")]
pub fn failpoint_hits(name: &str) -> u64 {
    crate::utils::failpoint::hit_count(name)
}

/// total consultations across every armed rule in this process; the
/// crash-window wait only needs "did any injection fire"
#[cfg(feature = "failpoints")]
pub fn failpoint_hits_total() -> u64 {
    crate::utils::failpoint::hits_total()
}

/// every armed rule with action, nth and hit count; append to crash-window
/// timeout panics so the failure message attributes itself
#[cfg(feature = "failpoints")]
pub fn failpoint_snapshot() -> String {
    crate::utils::failpoint::snapshot()
}

/// run `f` with named rules armed for its lifetime only; same-process tests
/// cannot leak overrides into each other (fs rules are not supported here)
#[cfg(feature = "failpoints")]
pub fn failpoint_scope<R>(raw: &str, f: impl FnOnce() -> R) -> R {
    let _scope = crate::utils::failpoint::arm_rules_scoped(raw);
    f()
}

/// engine-authoritative state dump appended to wait-timeout panics; counters
/// beyond these stay test-side through InMemoryObserver::snapshot because the
/// Observer trait is write-only by design
pub fn debug_snapshot(mace: &crate::Mace) -> String {
    let mut out = String::from("engine snapshot:");
    out.push_str(&format!("\n  data_gc_runs={}", mace.data_gc_count()));
    out.push_str(&format!("\n  blob_gc_runs={}", mace.blob_gc_count()));
    let tasks = mace.inner.store.context.abort_clean_tasks();
    out.push_str(&format!("\n  abort_clean_tasks={}", tasks.len()));
    out
}

/// try to acquire the stream-0 logging mutex without blocking; false means the
/// mutex is held (e.g. by an abort-clean handoff or a generation leader), which
/// is exactly what gc's boundary computation would observe
pub fn try_lock_wal_logging(bucket: &Bucket) -> bool {
    let ctx = &bucket.inner.store.context;
    ctx.group(0).logging.try_lock().is_some()
}

pub fn txn_group(bucket: &Bucket, start_ts: u64) -> Option<usize> {
    let ctx = &bucket.inner.store.context;
    (0..ctx.groups().len()).find(|&gid| fact_present(bucket, gid, start_ts))
}

pub fn retained_abort_present(bucket: &Bucket, group_id: usize, txid: u64) -> bool {
    bucket
        .inner
        .store
        .context
        .group(group_id)
        .retained_aborts
        .contains_key(&txid)
}

pub fn retained_abort_floor(bucket: &Bucket, group_id: usize) -> u64 {
    bucket
        .inner
        .store
        .context
        .group(group_id)
        .retained_abort_floor
        .load(std::sync::atomic::Ordering::Acquire)
}

pub fn abort_clean_task_stage(bucket: &Bucket, txid: u64) -> Option<AbortCleanStage> {
    bucket
        .inner
        .store
        .context
        .abort_clean_tasks()
        .into_iter()
        .find(|task| task.txid == txid)
        .map(|task| match task.state {
            AbortCleanState::Pending => AbortCleanStage::Pending,
            AbortCleanState::WaitingQuiesce => AbortCleanStage::WaitingQuiesce,
        })
}

pub fn abort_clean_task_info(bucket: &Bucket, txid: u64) -> Option<AbortCleanTaskInfo> {
    bucket
        .inner
        .store
        .context
        .abort_clean_tasks()
        .into_iter()
        .find(|task| task.txid == txid)
        .map(|task| AbortCleanTaskInfo {
            group_id: task.logical_group_id,
            tail_file_id: task.tail_lsn.file_id,
            tail_offset: task.tail_lsn.offset,
            pin_file_id: task.pin_file_id,
        })
}

pub fn corrupt_abort_clean_prev(
    bucket: &Bucket,
    txid: u64,
    prev_file_id: u64,
    prev_offset: u64,
) -> Result<(u64, u64), OpCode> {
    let task = bucket
        .inner
        .store
        .context
        .abort_clean_tasks()
        .into_iter()
        .find(|task| task.txid == txid)
        .ok_or(OpCode::NotFound)?;
    let path = bucket
        .inner
        .store
        .context
        .opt
        .physical_wal_path(task.physical_wal_id, task.tail_lsn.file_id);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|_| OpCode::IoError)?;
    let header_len = WalUpdate::size();
    file.seek(SeekFrom::Start(task.tail_lsn.offset))
        .map_err(|_| OpCode::IoError)?;
    let mut header = vec![0; header_len];
    file.read_exact(&mut header).map_err(|_| OpCode::IoError)?;
    let update = ptr_to::<WalUpdate>(header.as_ptr());
    let original_prev = (update.prev_id, update.prev_off);
    let payload_len = WalUpdate::checked_payload_len(update.size)?;
    let total_len = WalUpdate::checked_encoded_len(payload_len)?;
    let mut record = vec![0; total_len];
    file.seek(SeekFrom::Start(task.tail_lsn.offset))
        .map_err(|_| OpCode::IoError)?;
    file.read_exact(&mut record).map_err(|_| OpCode::IoError)?;
    unsafe {
        let base = record.as_mut_ptr();
        std::ptr::write_unaligned(
            base.add(offset_of!(WalUpdate, prev_id)).cast::<u64>(),
            prev_file_id,
        );
        std::ptr::write_unaligned(
            base.add(offset_of!(WalUpdate, prev_off)).cast::<u64>(),
            prev_offset,
        );
        let checksum = ptr_to::<WalUpdate>(record.as_ptr()).calc_checksum();
        std::ptr::write_unaligned(
            base.add(offset_of!(WalUpdate, checksum)).cast::<u32>(),
            checksum,
        );
    }
    file.seek(SeekFrom::Start(task.tail_lsn.offset))
        .map_err(|_| OpCode::IoError)?;
    file.write_all(&record).map_err(|_| OpCode::IoError)?;
    file.sync_all().map_err(|_| OpCode::IoError)?;
    Ok(original_prev)
}

pub fn group_logging_is_locked(bucket: &Bucket, group_id: usize) -> bool {
    bucket
        .inner
        .store
        .context
        .group(group_id)
        .logging
        .try_lock()
        .is_none()
}

pub fn wal_update_probes(log_root: &Path, groups: u8) -> Vec<WalUpdateProbe> {
    wal_record_probes(log_root, groups)
        .into_iter()
        .filter_map(|probe| {
            let WalRecordProbe {
                physical_wal_id,
                logical_group_id,
                file_id,
                offset,
                ..
            } = probe;
            logical_group_id.map(|logical_group_id| WalUpdateProbe {
                physical_wal_id,
                logical_group_id,
                file_id,
                offset,
            })
        })
        .collect()
}

pub fn wal_record_probes(log_root: &Path, groups: u8) -> Vec<WalRecordProbe> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(log_root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let parts: Vec<&str> = name.split('_').collect();
            if parts.len() != 3 {
                continue;
            }
            if parts[0] == "wal" {
                let Ok(physical_wal_id) = parts[1].parse::<u8>() else {
                    continue;
                };
                let Ok(file_id) = parts[2].parse::<u64>() else {
                    continue;
                };
                if physical_wal_id < groups {
                    files.push((physical_wal_id, file_id, entry.path()));
                }
            } else if parts[0] == "group" && parts[1] == "wal" {
                // the durable shared stream namespace `group_wal_<seq>` maps
                // to the reserved shared physical id
                let Ok(file_id) = parts[2].parse::<u64>() else {
                    continue;
                };
                files.push((Options::SHARED_ID, file_id, entry.path()));
            }
        }
    }
    files.sort_by_key(|(physical, file_id, _)| (*physical, *file_id));

    let mut probes = Vec::new();
    for (physical_wal_id, file_id, path) in files {
        let Ok(data) = std::fs::read(path) else {
            continue;
        };
        let mut off = 0usize;
        while off < data.len() {
            let Ok(e) = EntryType::try_from(data[off]) else {
                break;
            };
            let Ok(header_len) = wal_record_sz(e) else {
                break;
            };
            if off + header_len > data.len() {
                break;
            }
            let offset = off as u64;
            match e {
                EntryType::Update => {
                    let u = ptr_to::<WalUpdate>(unsafe { data.as_ptr().add(off) });
                    let Ok(payload_len) = WalUpdate::checked_payload_len(u.size) else {
                        break;
                    };
                    let Ok(total_len) = WalUpdate::checked_encoded_len(payload_len) else {
                        break;
                    };
                    if off + total_len > data.len() {
                        break;
                    }
                    probes.push(WalRecordProbe {
                        physical_wal_id,
                        kind: WalRecordKind::Update,
                        txid: u.txid,
                        logical_group_id: Some(u.group_id),
                        file_id,
                        offset,
                        len: total_len as u32,
                    });
                    off += total_len;
                }
                EntryType::CheckPoint => {
                    probes.push(WalRecordProbe {
                        physical_wal_id,
                        kind: WalRecordKind::Checkpoint,
                        txid: 0,
                        logical_group_id: None,
                        file_id,
                        offset,
                        len: WalCheckpoint::size() as u32,
                    });
                    off += WalCheckpoint::size();
                }
                EntryType::Begin | EntryType::Commit | EntryType::Abort => {
                    let w = ptr_to::<WalAbort>(unsafe { data.as_ptr().add(off) });
                    probes.push(WalRecordProbe {
                        physical_wal_id,
                        kind: match e {
                            EntryType::Begin => WalRecordKind::Begin,
                            EntryType::Commit => WalRecordKind::Commit,
                            EntryType::Abort => WalRecordKind::Abort,
                            _ => unreachable!("covered above"),
                        },
                        txid: w.txid,
                        logical_group_id: None,
                        file_id,
                        offset,
                        len: header_len as u32,
                    });
                    off += header_len;
                }
                EntryType::Unknown => break,
            }
        }
    }
    probes
}
