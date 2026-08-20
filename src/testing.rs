#![cfg(feature = "extra_check")]

use crate::cc::{
    context::AbortCleanState,
    wal::{EntryType, IWalCodec, WalAbort, WalCheckpoint, WalUpdate, ptr_to, wal_record_sz},
};
use crate::map::buffer::Pool;
use crate::meta::builder::ManifestBuilder;
use crate::types::header::{NodeType, RemoteHeader, TagKind};
use crate::types::refbox::BoxRef;
use crate::types::traits::IHeader;
use crate::utils::Handle;
use crate::{Bucket, OpCode, Options, TxnKV, TxnView};
use parking_lot::Mutex;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::{offset_of, size_of};
use std::path::Path;
use std::sync::Arc;
use std::sync::Barrier;
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

pub fn checkpoint_test_lock() -> parking_lot::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock()
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

pub fn safe_exclusive(bucket: &Bucket) -> u64 {
    bucket.inner.store.context.safe_exclusive()
}

pub fn wake_cc_collector(bucket: &Bucket) {
    bucket.inner.store.context.request_collect();
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
