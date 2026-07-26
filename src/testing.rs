#![cfg(feature = "extra_check")]

use crate::{Bucket, TxnKV, TxnView};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::OnceLock;

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
pub enum TreeUpdateSyncPoint {
    AfterFindLeafBeforeLink,
    AfterLatestMetaCheckBeforeDeltaInsert,
    AfterTreeAgainBeforeLatestMetaRecheck,
    AfterIteratorPageCaptureBeforeCandidateWalk,
}

type TxnCommitHook = dyn Fn(TxnCommitSyncPoint, u64) + Send + Sync + 'static;
type TxnBeginHook = dyn Fn(TxnBeginSyncPoint, u64) + Send + Sync + 'static;
type ViewHook = dyn Fn(ViewSyncPoint) + Send + Sync + 'static;
type CollectorHook = dyn Fn(CollectorSyncPoint, u64) + Send + Sync + 'static;
type VisibilityHook = dyn Fn(VisibilitySyncPoint, u64) + Send + Sync + 'static;
type TxnAbortHook = dyn Fn(TxnAbortSyncPoint, u64) + Send + Sync + 'static;
type TreeUpdateHook = dyn Fn(TreeUpdateSyncPoint, u64) + Send + Sync + 'static;

#[derive(Default)]
struct TestingHooks {
    txn_commit: Option<Arc<TxnCommitHook>>,
    txn_begin: Option<Arc<TxnBeginHook>>,
    view: Option<Arc<ViewHook>>,
    collector: Option<Arc<CollectorHook>>,
    visibility: Option<Arc<VisibilityHook>>,
    txn_abort: Option<Arc<TxnAbortHook>>,
    tree_update: Option<Arc<TreeUpdateHook>>,
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

pub fn set_tree_update_hook(hook: Option<Arc<TreeUpdateHook>>) {
    hooks().lock().tree_update = hook;
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

pub fn clear_tree_update_hook() {
    set_tree_update_hook(None);
}

pub fn clear_hooks() {
    clear_txn_commit_hook();
    clear_txn_begin_hook();
    clear_view_hook();
    clear_collector_hook();
    clear_visibility_hook();
    clear_txn_abort_hook();
    clear_tree_update_hook();
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

pub(crate) fn fire_tree_update_sync_point(point: TreeUpdateSyncPoint, page_pid: u64) {
    let hook = hooks().lock().tree_update.clone();
    if let Some(hook) = hook {
        hook(point, page_pid);
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
