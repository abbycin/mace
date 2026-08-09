#![cfg(feature = "extra_check")]

use crate::cc::{
    context::AbortCleanState,
    wal::{IWalCodec, WalUpdate, ptr_to},
};
use crate::map::buffer::Pool;
use crate::types::header::{NodeType, RemoteHeader, TagKind};
use crate::types::refbox::BoxRef;
use crate::types::traits::IHeader;
use crate::{Bucket, OpCode, TxnKV, TxnView};
use parking_lot::Mutex;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::{offset_of, size_of};
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::OnceLock;
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

pub struct CheckpointRootRestore {
    root: Mutex<Option<(BoxRef, u64)>>,
}

impl CheckpointRootRestore {
    pub fn new() -> Self {
        Self {
            root: Mutex::new(None),
        }
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
    let pool: crate::utils::Handle<Pool> = bucket.tree.bucket.pool.clone();
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
    bucket.tree.bucket.checkpoint_and_wait();
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
            group_id: task.group_id,
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
) -> Result<(), OpCode> {
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
        .wal_file(task.group_id, task.tail_lsn.file_id);
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
    Ok(())
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
