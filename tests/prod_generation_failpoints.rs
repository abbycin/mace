#![cfg(all(feature = "failpoints", feature = "extra_check"))]

//! crash windows for the shared durable wal: generation seal/sync,
//! abort fact publication, rotation, torn tail truncation, route-switch
//! recovery, route-switch epoch and switch-wipe crash windows

mod common;

use btree_store::BTree;
use common::child_test_command;
use mace::observe::{CounterMetric, InMemoryObserver};
use mace::testing;
use mace::{Bucket, BucketOptions, Mace, OpCode, Options, RandomPath};
use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const ENV_CHILD: &str = "MACE_GEN_FP_CHILD";
const ENV_CASE: &str = "MACE_GEN_FP_CASE";
const ENV_DB_ROOT: &str = "MACE_GEN_FP_DB_ROOT";
const ENV_LAYOUT_FAILPOINT: &str = "MACE_LAYOUT_FAILPOINT";

fn counter_value(observer: &InMemoryObserver, metric: CounterMetric) -> u64 {
    observer
        .snapshot()
        .counters
        .iter()
        .find(|(current, _)| *current == metric)
        .map(|(_, value)| *value)
        .unwrap_or(0)
}

fn wal_file_ids(log_root: &Path, physical: u8) -> Vec<u64> {
    let prefix = format!("wal_{physical}_");
    let mut ids = Vec::new();
    if let Ok(entries) = std::fs::read_dir(log_root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(rest) = name.strip_prefix(&prefix)
                && let Ok(file_id) = rest.parse::<u64>()
            {
                ids.push(file_id);
            }
        }
    }
    ids.sort_unstable();
    ids
}

fn group_wal_file_ids(log_root: &Path) -> Vec<u64> {
    let prefix = "group_wal_";
    let mut ids = Vec::new();
    if let Ok(entries) = std::fs::read_dir(log_root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(rest) = name.strip_prefix(prefix)
                && let Ok(file_id) = rest.parse::<u64>()
            {
                ids.push(file_id);
            }
        }
    }
    ids.sort_unstable();
    ids
}

fn open_with(
    db_root: &Path,
    concurrent_write: u8,
    sync_on_write: bool,
    observer: Option<std::sync::Arc<InMemoryObserver>>,
) -> Mace {
    let mut opt = Options::new(db_root);
    opt.sync_on_write = sync_on_write;
    opt.concurrent_write = concurrent_write;
    opt.data_file_size = 16 << 10;
    opt.wal_buffer_size = 32 << 10;
    opt.wal_file_size = 8 << 10;
    opt.gc_timeout = 60_000;
    opt.checkpoint_nudge_ms = 0;
    opt.gc_eager = false;
    if let Some(observer) = observer {
        opt.observer = observer;
    }
    Mace::new(opt.validate().expect("options must validate")).expect("open must succeed")
}

fn open_durable(db_root: &Path, observer: Option<std::sync::Arc<InMemoryObserver>>) -> Mace {
    open_with(db_root, 2, true, observer)
}

fn open_relaxed(db_root: &Path) -> Mace {
    open_with(db_root, 2, false, None)
}

/// open the relaxed route at an arbitrary writer width (the width-migration cases)
fn open_relaxed_at(db_root: &Path, concurrent_write: u8) -> Mace {
    open_with(db_root, concurrent_write, false, None)
}

fn bucket(mace: &Mace) -> Bucket {
    match mace.open_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket(
                "prod",
                BucketOptions {
                    inline_size: 4096,
                    cache_evict_pct: 10,
                    checkpoint_size: 32 << 10,
                    pool_capacity: 64 << 10,
                    enable_backpressure: false,
                    ..BucketOptions::default()
                },
            )
            .expect("create prod bucket failed"),
        Err(err) => panic!("open prod bucket failed: {err:?}"),
    }
}

fn seed_committed(bucket: &Bucket, count: usize) {
    let txn = bucket.begin().expect("begin committed txn failed");
    for idx in 0..count {
        txn.put(format!("k_{idx}"), format!("v_{idx}"))
            .expect("put committed key failed");
    }
    txn.commit().expect("commit committed txn failed");
}

fn seed_mixed_groups(bucket: &Bucket, count: usize) {
    let tx_a = bucket.begin().expect("begin group a txn failed");
    let txid_a = mace::testing::txn_start_ts(&tx_a);
    assert_eq!(
        mace::testing::txn_group(bucket, txid_a),
        Some(0),
        "mixed-stream redo requires a distinct logical group 0 txn"
    );
    for idx in 0..count {
        tx_a.put(format!("a_{idx}"), format!("va_{idx}"))
            .expect("put group a key failed");
    }
    let tx_b = bucket.begin().expect("begin group b txn failed");
    let txid_b = mace::testing::txn_start_ts(&tx_b);
    assert_eq!(
        mace::testing::txn_group(bucket, txid_b),
        Some(1),
        "mixed-stream redo requires a distinct logical group 1 txn"
    );
    for idx in 0..count {
        tx_b.put(format!("b_{idx}"), format!("vb_{idx}"))
            .expect("put group b key failed");
    }
    tx_a.commit().expect("commit group a txn failed");
    tx_b.commit().expect("commit group b txn failed");
}

fn spawn_child(case: &str, db_root: &Path, failpoint: &str) -> ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    child_test_command(&exe)
        .arg("--exact")
        .arg("gen_failpoint_child")
        .arg("--nocapture")
        .env(ENV_CHILD, "1")
        .env(ENV_CASE, case)
        .env(ENV_DB_ROOT, db_root.as_os_str())
        .env("MACE_FAILPOINT", failpoint)
        .status()
        .expect("spawn failpoint child failed")
}

fn spawn_layout_migration_child(db_root: &Path, failpoint: &str) -> ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    child_test_command(&exe)
        .arg("--exact")
        .arg("gen_failpoint_child")
        .arg("--nocapture")
        .env(ENV_CHILD, "1")
        .env(ENV_CASE, "layout_migration")
        .env(ENV_DB_ROOT, db_root.as_os_str())
        .env(ENV_LAYOUT_FAILPOINT, failpoint)
        .status()
        .expect("spawn layout migration child failed")
}

#[cfg(unix)]
fn assert_child_aborted(status: ExitStatus, msg: &str) {
    use std::os::unix::process::ExitStatusExt;

    assert_eq!(status.signal(), Some(6), "{msg}");
}

#[cfg(not(unix))]
fn assert_child_aborted(status: ExitStatus, msg: &str) {
    assert!(!status.success(), "{msg}");
}

fn wait_for_crash(timeout: Duration) -> ! {
    // the armed failpoint aborts on its acting consultation -- the nth-th hit
    // for nth rules, the first otherwise; benign pre-nth consultations are
    // expected during setup and must not fail the window early. past the
    // deadline either some rule acted without the abort landing, or none got
    // that far -- both shapes attribute via the rule snapshot
    let deadline = Instant::now() + timeout;
    loop {
        if mace::failpoint_testing::any_rule_actioned() {
            // the acting consultation and its abort run on an engine thread;
            // give an in-flight abort a beat to land before declaring failure
            std::thread::sleep(Duration::from_millis(200));
            panic!(
                "failpoint reached its acting consultation but the process survived; {}",
                mace::failpoint_testing::snapshot()
            )
        }
        if Instant::now() >= deadline {
            panic!(
                "no failpoint reached its acting consultation within {timeout:?}; {}",
                mace::failpoint_testing::snapshot()
            )
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn gen_failpoint_child() {
    if std::env::var(ENV_CHILD).ok().as_deref() != Some("1") {
        return;
    }

    let case = std::env::var(ENV_CASE).expect("missing failpoint case");
    let db_root = PathBuf::from(std::env::var(ENV_DB_ROOT).expect("missing failpoint db root"));

    match case.as_str() {
        "generation_before_file_sync" => child_generation_before_file_sync(&db_root),
        "generation_after_file_sync" => child_generation_after_file_sync(&db_root),
        "generation_before_file_sync_mixed" => child_generation_before_file_sync_mixed(&db_root),
        "txn_abort_after_wal_sync" => child_txn_abort_after_wal_sync(&db_root),
        "wal_rotation_after_file_create" => child_wal_rotation_after_file_create(&db_root),
        "wal_tail_corrupt" => child_wal_tail_corrupt(&db_root),
        "wal_tail_corrupt_big" => child_wal_tail_corrupt_big(&db_root),
        "wal_recycle_multi_group_shared" => child_wal_recycle_multi_group_shared(&db_root),
        "route_switch_relaxed_to_durable" => child_route_switch_relaxed_to_durable(&db_root),
        "route_switch_durable_to_relaxed" => child_route_switch_durable_to_relaxed(&db_root),
        "switch_wipe_midway" => child_switch_wipe_midway(&db_root),
        "switch_evict_many_buckets" => child_switch_evict_many_buckets(&db_root),
        "cw_seed" => child_cw_seed(&db_root),
        "cw_nonuniform_seed" => child_cw_nonuniform_seed(&db_root),
        "cw_migrate" => child_cw_migrate(&db_root),
        "switch_open_arm" => child_switch_open_arm(&db_root),
        "switch_open_arm_durable" => child_switch_open_arm_durable(&db_root),
        "switch_pending_abort" => child_switch_pending_abort(&db_root),
        "switch_abort_evict_many" => child_switch_abort_evict_many(&db_root),
        "layout_migration" => child_layout_migration(&db_root),
        "all_inactive_recycle" => child_all_inactive_recycle(&db_root),
        other => panic!("unknown failpoint case {other}"),
    }
}

fn child_layout_migration(db_root: &Path) -> ! {
    let mace = open_relaxed(db_root);
    let bucket = bucket(&mace);
    seed_mixed_groups(&bucket, 64);
    drop(bucket);
    drop(mace);

    let mut legacy = Options::new(db_root);
    legacy.sync_on_write = false;
    legacy.concurrent_write = 2;
    legacy.data_file_size = 16 << 10;
    legacy.wal_buffer_size = 32 << 10;
    legacy.wal_file_size = 8 << 10;
    legacy.gc_timeout = 60_000;
    legacy.checkpoint_nudge_ms = 0;
    legacy.gc_eager = false;
    testing::rewrite_persisted_sync_on_write(legacy, true)
        .expect("rewrite old durable route marker");

    let rule = std::env::var(ENV_LAYOUT_FAILPOINT).expect("missing layout failpoint");
    testing::arm_failpoint_rule(&rule);
    let _ = open_durable(db_root, None);
    wait_for_crash(Duration::from_secs(2))
}

fn child_all_inactive_recycle(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let bucket = bucket(&mace);
    for _ in 0..3000 {
        bucket
            .begin()
            .expect("unmodified begin")
            .commit()
            .expect("unmodified commit");
    }
    mace.sync().expect("sync unmodified wal");
    mace.start_gc();
    wait_for_crash(Duration::from_secs(2))
}

fn assert_mixed_groups_visible_after_layout_migration(db_root: &Path) {
    let mace = open_durable(db_root, None);
    let bucket = mace.open_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("migration verify view");
    for prefix in ["a", "b"] {
        for idx in 0..64 {
            let key = format!("{prefix}_{idx}");
            assert_eq!(
                view.get(&key)
                    .unwrap_or_else(|_| panic!("missing {key}"))
                    .slice(),
                format!("v{prefix}_{idx}").as_bytes()
            );
        }
    }
    let log_root = Options::new(db_root).log_root();
    assert!(wal_file_ids(&log_root, 0).is_empty());
    assert!(wal_file_ids(&log_root, 1).is_empty());
    assert!(!group_wal_file_ids(&log_root).is_empty());
}

fn child_generation_before_file_sync(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let bucket = bucket(&mace);
    seed_committed(&bucket, 64);
    wait_for_crash(Duration::from_secs(2))
}

fn child_generation_after_file_sync(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let bucket = bucket(&mace);
    seed_committed(&bucket, 64);
    wait_for_crash(Duration::from_secs(2))
}

fn child_generation_before_file_sync_mixed(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let bucket = bucket(&mace);
    seed_mixed_groups(&bucket, 64);
    wait_for_crash(Duration::from_secs(2))
}

fn child_txn_abort_after_wal_sync(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let bucket = bucket(&mace);
    seed_committed(&bucket, 64);
    let txn = bucket.begin().expect("begin abort txn failed");
    for idx in 0..24 {
        txn.put(format!("u_{idx}"), format!("u_{idx}"))
            .expect("put aborted key failed");
    }
    // drop triggers the modified abort: abort record durable, then crash
    // before the abort fact and abort-clean task are published
    drop(txn);
    wait_for_crash(Duration::from_secs(2))
}

fn child_wal_rotation_after_file_create(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let bucket = bucket(&mace);
    let payload = vec![b'w'; 2048];
    for round in 0..128 {
        let txn = bucket.begin().expect("begin rotation txn failed");
        txn.upsert(format!("rot_{round}"), &payload)
            .expect("upsert rotation key failed");
        txn.commit().expect("commit rotation txn failed");
    }
    wait_for_crash(Duration::from_secs(2))
}

fn child_wal_tail_corrupt(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let bucket = bucket(&mace);
    for round in 0..16 {
        let txn = bucket.begin().expect("begin tail txn failed");
        txn.upsert(format!("pre_{round}"), format!("pre_v_{round}"))
            .expect("upsert tail key failed");
        txn.commit().expect("commit tail txn failed");
    }

    // simulate a torn tail: garbage appended after the last valid record of
    // the latest group_wal file
    let opt = Options::new(db_root);
    let log_root = opt.log_root();
    let latest = group_wal_file_ids(&log_root)
        .last()
        .copied()
        .expect("group_wal file must exist");
    let path = opt.group_wal_file(latest);
    let mut tail = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open wal tail must succeed");
    tail.write_all(&[0xFFu8; 24])
        .expect("append torn tail must succeed");
    tail.sync_all().expect("sync torn tail must succeed");
    drop(tail);

    // arm the crash failpoint in-process: the failpoint state refreshes from
    // the env on every hit, and this override survives those refreshes without
    // mutating the process env (the child has background evictor/gc threads
    // that read the env on every hit). abort@2 lets one post-garbage commit
    // COMPLETE (and be synced) before the next seal crashes, so recovery must
    // choose between discarding the synced post-garbage record (first-bad-
    // record truncation) and keeping it (a forbidden fsync-watermark
    // truncation)
    testing::arm_failpoint_rule("mace_wal_tail_corrupt=abort@2");
    let txn = bucket.begin().expect("begin synced-after-tail txn failed");
    txn.upsert("synced_after_tail", b"kept_by_watermark")
        .expect("upsert synced-after-tail key failed");
    txn.commit().expect("commit synced-after-tail txn failed");
    let txn = bucket.begin().expect("begin after-tail txn failed");
    txn.upsert("after_tail", b"lost")
        .expect("upsert after-tail key failed");
    txn.commit().expect("commit after-tail txn failed");
    wait_for_crash(Duration::from_secs(2))
}

/// like child_wal_tail_corrupt, but the torn tail is far larger than any
/// single wal record: a stale logical position from the pre-truncation open
/// (missing the recovery rebase) then deterministically points past the
/// physical EOF of the truncated file, so a runtime abort's abort-clean
/// chain walk fails with Corruption instead of decoding by luck
fn child_wal_tail_corrupt_big(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let bucket = bucket(&mace);
    for round in 0..8 {
        let txn = bucket.begin().expect("begin tail txn failed");
        txn.upsert(format!("pre_{round}"), format!("pre_v_{round}"))
            .expect("upsert tail key failed");
        txn.commit().expect("commit tail txn failed");
    }

    // simulate a torn tail: garbage appended after the last valid record of
    // the latest group_wal file, synced so the crash preserves it
    let opt = Options::new(db_root);
    let log_root = opt.log_root();
    let mut latest = 0;
    for entry in std::fs::read_dir(log_root).expect("read wal dir failed") {
        let name = entry.expect("wal entry failed").file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix("group_wal_")
            && let Ok(file_id) = rest.parse::<u64>()
        {
            latest = latest.max(file_id);
        }
    }
    let path = opt.group_wal_file(latest);
    let mut tail = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open wal tail must succeed");
    tail.write_all(&[0xFFu8; 4096])
        .expect("append torn tail must succeed");
    tail.sync_all().expect("sync torn tail must succeed");
    drop(tail);

    std::process::abort()
}

fn child_wal_recycle_multi_group_shared(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let bucket = bucket(&mace);
    // many small wal files so the shared-stream recycle has a real range
    let payload = vec![b'w'; 1024];
    for round in 0..128 {
        let txn = bucket.begin().expect("begin recycle txn failed");
        txn.upsert(format!("rw_{round}"), &payload)
            .expect("upsert recycle key failed");
        txn.commit().expect("commit recycle txn failed");
    }
    // begin the pin now: its begin lands in the current wal file
    let pin = bucket.begin().expect("begin pin txn failed");
    pin.put("pin", b"v").expect("put pin key failed");
    // more commits rotate to newer files, so the checkpoint frontier ends up
    // ABOVE the pin's file: without the group-1 pin in the aggregate recycle
    // boundary the pin's file would be recycled, and recovery's abort-clean
    // would fail on the missing chain
    for round in 128..256 {
        let txn = bucket.begin().expect("begin recycle txn failed");
        txn.upsert(format!("rw_{round}"), &payload)
            .expect("upsert recycle key failed");
        txn.commit().expect("commit recycle txn failed");
    }
    bucket.checkpoint();
    mace.start_gc();
    wait_for_crash(Duration::from_secs(2))
}

fn child_route_switch_relaxed_to_durable(db_root: &Path) -> ! {
    let mace = open_relaxed(db_root);
    let bucket = bucket(&mace);
    // crash after a relaxed commit's flush, before the fact is published
    seed_committed(&bucket, 64);
    wait_for_crash(Duration::from_secs(2))
}

fn child_route_switch_durable_to_relaxed(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let bucket = bucket(&mace);
    // crash after the durable generation sync, before the fact is published
    seed_committed(&bucket, 64);
    wait_for_crash(Duration::from_secs(2))
}

fn child_switch_wipe_midway(db_root: &Path) -> ! {
    // history: a graceful durable run checkpoints everything at exit and its
    // exit-time wal_clean recycles all but the last file, so the switch open
    // below has exactly one file left to wipe
    let mace = open_durable(db_root, None);
    let bucket = bucket(&mace);
    let txn = bucket.begin().expect("begin wipe txn failed");
    txn.put("k", b"v").expect("put wipe key failed");
    txn.commit().expect("commit wipe txn failed");
    drop(bucket);
    drop(mace);

    // arm the mid-wipe crash window in-process right before the switch open
    // (the failpoint name is shared with runtime GC recycle, which must not
    // fire earlier: no runtime checkpoint runs in this child)
    testing::arm_failpoint_rule("mace_wal_recycle_after_dir_sync_before_done_commit=abort@1");
    let _mace = open_relaxed(db_root);
    wait_for_crash(Duration::from_secs(2))
}

/// tail construction for the switch durability windows: committed data in 24
/// buckets (> the recovery-lru cap of 16) with a crash on the last commit, so
/// recovery must redo a tail that spans more buckets than fit in the recovery
/// lru and evicts redo-dirtied buckets along the way
fn child_switch_evict_many_buckets(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    for i in 0..24 {
        let bucket = ensure_bucket(&mace, &format!("b{i}"));
        let txn = bucket.begin().expect("begin txn failed");
        txn.put("k", format!("v{i}")).expect("put failed");
        txn.commit().expect("commit failed");
    }
    wait_for_crash(Duration::from_secs(2))
}

/// run the relaxed switch open with a failpoint armed in-process so the crash
/// lands in the requested window (recovery eviction force-fsync, forced
/// checkpoint, wipe, abort-clean drain, or options writeback)
fn child_switch_open_arm(db_root: &Path) -> ! {
    let rule = std::env::var(ENV_SWITCH_FAILPOINT).expect("missing switch failpoint rule");
    testing::arm_failpoint_rule(&rule);
    let _mace = open_relaxed(db_root);
    wait_for_crash(Duration::from_secs(2))
}

/// same as child_switch_open_arm but reopens in the durable route (used to
/// crash a same-route recovery inside its eviction window)
fn child_switch_open_arm_durable(db_root: &Path) -> ! {
    let rule = std::env::var(ENV_SWITCH_FAILPOINT).expect("missing switch failpoint rule");
    testing::arm_failpoint_rule(&rule);
    let _mace = open_durable(db_root, None);
    wait_for_crash(Duration::from_secs(2))
}

/// leave one committed txn and one in-progress txn in the wal, crashing on the
/// committed txn's sync: recovery must reconstruct the abort-clean chain for
/// the in-progress txn and drain it before the switch wipe
fn child_switch_pending_abort(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let bucket = ensure_bucket(&mace, "prod");
    let pending = bucket.begin().expect("begin pending txn failed");
    pending
        .put("pending", b"never")
        .expect("put pending failed");
    let txn = bucket.begin().expect("begin committed txn failed");
    txn.put("k", b"v").expect("put committed failed");
    txn.commit().expect("commit committed failed");
    drop(pending);
    wait_for_crash(Duration::from_secs(2))
}

/// 24 buckets: b23 holds ONLY an in-progress txn (no committed records), the
/// rest hold committed records; crash at the last commit. recovery must evict
/// b23 during analyze (24 > the recovery-lru cap of 16) and reload it during
/// the abort-clean drain, so its scrub flush is not covered by the forced
/// checkpoint over still-loaded buckets
fn child_switch_abort_evict_many(db_root: &Path) -> ! {
    let mace = open_durable(db_root, None);
    let pending_bucket = ensure_bucket(&mace, "b23");
    let pending = pending_bucket.begin().expect("begin pending txn failed");
    pending
        .put("pending", b"never")
        .expect("put pending failed");
    for i in 0..23 {
        let bucket = ensure_bucket(&mace, &format!("b{i}"));
        let txn = bucket.begin().expect("begin txn failed");
        txn.put("k", format!("v{i}")).expect("put failed");
        txn.commit().expect("commit failed");
    }
    drop(pending);
    wait_for_crash(Duration::from_secs(2))
}

fn assert_committed_visible(db_root: &Path, count: usize) {
    let mace = open_durable(db_root, None);
    let bucket = mace.open_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    for idx in 0..count {
        let key = format!("k_{idx}");
        let val = view.get(&key).expect("committed key missing");
        assert_eq!(val.slice(), format!("v_{idx}").as_bytes());
    }
}

fn assert_mixed_visible(db_root: &Path, count: usize) {
    let mace = open_durable(db_root, None);
    let bucket = mace.open_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    for idx in 0..count {
        let val = view
            .get(format!("a_{idx}"))
            .expect("group a committed key missing");
        assert_eq!(val.slice(), format!("va_{idx}").as_bytes());
        let val = view
            .get(format!("b_{idx}"))
            .expect("group b committed key missing");
        assert_eq!(val.slice(), format!("vb_{idx}").as_bytes());
    }
}

#[test]
fn generation_crash_before_file_sync_recovers_committed_tail() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "generation_before_file_sync",
        &path,
        "mace_wal_generation_before_file_sync=abort@1",
    );
    assert_child_aborted(status, "before-file-sync child must abort");
    assert_committed_visible(&path, 64);
}

#[test]
fn generation_crash_after_file_sync_before_fact_recovers_committed_tail() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "generation_after_file_sync",
        &path,
        "mace_wal_generation_after_file_sync_before_complete=abort@1",
    );
    assert_child_aborted(status, "after-file-sync child must abort");
    assert_committed_visible(&path, 64);
}

#[test]
fn generation_crash_recovers_mixed_group_stream_by_logical_group() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "generation_before_file_sync_mixed",
        &path,
        "mace_wal_generation_before_file_sync=abort@2",
    );
    assert_child_aborted(status, "mixed-generation child must abort");
    assert_mixed_visible(&path, 64);
}

#[test]
fn txn_abort_crash_after_wal_sync_rolls_back_on_reopen() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "txn_abort_after_wal_sync",
        &path,
        "mace_txn_abort_after_wal_sync=abort@1",
    );
    assert_child_aborted(status, "abort child must abort");

    let mace = open_durable(&path, None);
    let bucket = mace.open_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    for idx in 0..64 {
        let key = format!("k_{idx}");
        let val = view.get(&key).expect("committed key missing");
        assert_eq!(val.slice(), format!("v_{idx}").as_bytes());
    }
    for idx in 0..24 {
        let key = format!("u_{idx}");
        assert!(
            view.get(&key).is_err(),
            "aborted key {key} must not survive the crash"
        );
    }
}

#[test]
fn wal_rotation_crash_after_file_create_survives_reopen_without_gap() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "wal_rotation_after_file_create",
        &path,
        "mace_wal_rotation_after_file_create=abort@1",
    );
    assert_child_aborted(status, "rotation child must abort");

    let mace = open_durable(&path, None);
    let bucket = mace.open_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    let got = view.get("rot_0").expect("pre-rotation commit must survive");
    assert_eq!(got.slice(), vec![b'w'; 2048].as_slice());

    // the wal file sequence must stay contiguous after the rotation crash
    let opt = Options::new(&*path);
    let log_root = opt.log_root();
    for physical in 0..2 {
        let ids = wal_file_ids(&log_root, physical);
        for pair in ids.windows(2) {
            assert_eq!(
                pair[1],
                pair[0] + 1,
                "physical stream {physical} must have no wal gap"
            );
        }
    }
}

#[test]
fn wal_tail_corruption_truncates_at_first_bad_record() {
    let observer = std::sync::Arc::new(InMemoryObserver::new(8));
    let path = RandomPath::tmp();
    let status = spawn_child("wal_tail_corrupt", &path, "");
    assert_child_aborted(status, "tail-corrupt child must abort");

    let mace = open_durable(&path, Some(observer.clone()));
    let bucket = mace.open_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    for idx in 0..16 {
        let key = format!("pre_{idx}");
        let val = view.get(&key).expect("pre-corruption key missing");
        assert_eq!(val.slice(), format!("pre_v_{idx}").as_bytes());
    }
    assert!(
        view.get("synced_after_tail").is_err(),
        "a record synced after the garbage must be discarded: only first-bad-record truncation drops it, a fsync-watermark truncation would keep it"
    );
    assert!(
        view.get("after_tail").is_err(),
        "records after the first bad record must be discarded"
    );
    assert_eq!(
        counter_value(&observer, CounterMetric::RecoveryWalTruncate),
        1,
        "recovery must truncate the stream at the first bad record"
    );
    drop(view);
    // the wal file must be truncated exactly at the first bad record start
    let opt = Options::new(&*path);
    let log_root = opt.log_root();
    let probes = testing::wal_record_probes(&log_root, 2);
    let truncation = probes
        .iter()
        .map(|p| p.offset + p.len as u64)
        .max()
        .expect("valid prefix must exist");
    let truncated_file = probes
        .iter()
        .map(|p| p.file_id)
        .max()
        .expect("valid prefix must exist");
    let file_len = std::fs::metadata(opt.group_wal_file(truncated_file))
        .expect("truncated group_wal must exist")
        .len();
    assert_eq!(
        file_len, truncation,
        "recovery must truncate exactly at the first bad record start"
    );

    // the active writer was opened before recovery truncated the torn tail
    // a post-recovery append must use the physical EOF, then survive a
    // checkpoint and a second reopen without a stale logical position
    let txn = bucket.begin().expect("post-truncation begin failed");
    txn.put("after_recovery", "survives")
        .expect("post-truncation put failed");
    txn.commit().expect("post-truncation commit failed");
    mace.sync().expect("post-truncation sync failed");
    let (durable_file, durable_offset) = testing::wal_durable_pos(&bucket, 0);
    let durable_len = std::fs::metadata(opt.group_wal_file(durable_file))
        .expect("active shared wal must exist")
        .len();
    assert_eq!(
        durable_offset, durable_len,
        "runtime durable position must track the physical WAL EOF after truncation"
    );
    drop(bucket);
    drop(mace);

    let reopened = open_durable(&path, None);
    let reopened_bucket = reopened
        .open_bucket("prod")
        .expect("bucket prod should reopen after post-truncation write");
    let reopened_view = reopened_bucket
        .view()
        .expect("post-truncation reopen view failed");
    assert_eq!(
        reopened_view
            .get("after_recovery")
            .expect("post-truncation key missing after reopen")
            .slice(),
        b"survives"
    );
}

/// F1 abort-clean variant: the torn-tail truncation runs after the active
/// writer was opened, so a runtime abort in the reopened session must not
/// anchor its abort-clean chain at stale pre-truncation positions. the big
/// garbage guarantees the stale tail_lsn (without the recovery rebase) points
/// beyond the physical EOF, so the chain walk fails with Corruption, the task
/// stays Pending, and its wal file stays pinned forever
#[test]
fn truncated_tail_runtime_abort_clean_completes_and_unpins() {
    let _hook_lock = testing::checkpoint_test_lock();
    let observer = std::sync::Arc::new(InMemoryObserver::new(8));
    let path = RandomPath::tmp();
    let status = spawn_child("wal_tail_corrupt_big", &path, "");
    assert_child_aborted(status, "big-tail child must abort");

    let mace = open_durable(&path, Some(observer.clone()));
    let bucket = mace.open_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    for idx in 0..8 {
        let key = format!("pre_{idx}");
        let val = view.get(&key).expect("pre-corruption key missing");
        assert_eq!(val.slice(), format!("pre_v_{idx}").as_bytes());
    }
    drop(view);
    assert_eq!(
        counter_value(&observer, CounterMetric::RecoveryWalTruncate),
        1,
        "recovery must truncate the stream at the first bad record"
    );

    // a runtime modified abort in the post-truncation session enqueues an
    // abort-clean task whose tail_lsn comes from the live logger position;
    // install the observer before the abort so completion/corruption of this
    // exact task cannot be missed
    let txid = {
        let txn = bucket.begin().expect("post-truncation begin failed");
        let txid = testing::txn_start_ts(&txn);
        txn.put("aborted_key", b"lost")
            .expect("post-truncation put failed");
        drop(txn); // durable modified abort -> abort-clean task enqueued
        txid
    };
    let info = testing::abort_clean_task_info(&bucket, txid)
        .expect("abort-clean task must be published after the runtime abort");

    let corruption_seen = std::sync::Arc::new(AtomicBool::new(false));
    let quiesce_seen = std::sync::Arc::new(AtomicBool::new(false));
    testing::set_abort_clean_hook(Some(std::sync::Arc::new({
        let corruption_seen = corruption_seen.clone();
        let quiesce_seen = quiesce_seen.clone();
        move |point, callback_txid| {
            if callback_txid != txid {
                return;
            }
            match point {
                testing::AbortCleanSyncPoint::AfterCorruption => {
                    corruption_seen.store(true, Ordering::Release);
                }
                testing::AbortCleanSyncPoint::AfterQuiesceCallback => {
                    quiesce_seen.store(true, Ordering::Release);
                }
            }
        }
    })));

    // drive gc until the task completes (rebase path) or sticks (no rebase);
    // the corruption hook fires inside the gc run, so a hit is an immediate
    // fail rather than a deadline timeout
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        assert!(
            !corruption_seen.load(Ordering::Acquire),
            "abort-clean must not hit Corruption on a truncated tail: the runtime abort's chain positions are stale"
        );
        mace.start_gc();
        if testing::abort_clean_task_info(&bucket, txid).is_none() {
            break;
        }
        if Instant::now() >= deadline {
            panic!("abort-clean task must complete after recovery rebased the writer positions");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        quiesce_seen.load(Ordering::Acquire),
        "abort-clean chain must complete (quiesce callback) for the truncated-tail abort"
    );
    assert!(
        !testing::retained_abort_present(&bucket, info.group_id as usize, txid),
        "the retained abort fact must be retired once the task completes"
    );
    testing::clear_abort_clean_hook();

    // the task removal releases the wal pin: after rotating past the aborted
    // txn's file and checkpointing, gc must recycle it instead of keeping it
    // pinned forever
    let opt = Options::new(&*path);
    let pinned = opt.group_wal_file(info.pin_file_id);
    for round in 0..128 {
        let txn = bucket.begin().expect("begin rotate txn failed");
        txn.upsert(format!("keep_{round}"), format!("keep_v_{round}"))
            .expect("upsert rotate key failed");
        txn.commit().expect("commit rotate txn failed");
    }
    bucket.checkpoint();
    let recycle_deadline = Instant::now() + Duration::from_secs(8);
    while pinned.exists() && Instant::now() < recycle_deadline {
        mace.start_gc();
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !pinned.exists(),
        "the aborted txn's wal file must be recycled once its abort-clean task completed"
    );

    // a post-abort commit survives; the aborted key does not
    let txn = bucket.begin().expect("post-abort begin failed");
    txn.put("post_abort", b"alive")
        .expect("post-abort put failed");
    txn.commit().expect("post-abort commit failed");
    drop(bucket);
    drop(mace);

    let reopened = open_durable(&path, None);
    let reopened_bucket = reopened
        .open_bucket("prod")
        .expect("bucket prod should reopen after truncation+abort");
    let reopened_view = reopened_bucket
        .view()
        .expect("post-abort reopen view failed");
    assert_eq!(
        reopened_view
            .get("pre_0")
            .expect("pre-truncation key must survive")
            .slice(),
        b"pre_v_0"
    );
    assert_eq!(
        reopened_view
            .get("keep_0")
            .expect("checkpointed key from the recycled wal must survive")
            .slice(),
        b"keep_v_0"
    );
    assert!(
        reopened_view.get("aborted_key").is_err(),
        "the aborted key must not survive the reopen"
    );
    assert_eq!(
        reopened_view
            .get("post_abort")
            .expect("post-abort key must survive the reopen")
            .slice(),
        b"alive"
    );
}

#[test]
fn relaxed_to_durable_crash_recovers_across_epochs() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "route_switch_relaxed_to_durable",
        &path,
        "mace_txn_commit_after_wal_sync=abort@1",
    );
    assert_child_aborted(status, "relaxed-to-durable child must abort");
    // reopen in the other route: the old per-group streams must be scanned and
    // the new epoch must start above their high water
    assert_committed_visible(&path, 64);
}

#[test]
fn durable_to_relaxed_crash_recovers_across_epochs() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "route_switch_durable_to_relaxed",
        &path,
        "mace_txn_commit_after_wal_sync=abort@1",
    );
    assert_child_aborted(status, "durable-to-relaxed child must abort");
    let mace = open_relaxed(&path);
    let bucket = mace.open_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    for idx in 0..64 {
        let key = format!("k_{idx}");
        let val = view.get(&key).expect("committed key missing");
        assert_eq!(val.slice(), format!("v_{idx}").as_bytes());
    }
}

#[test]
fn switch_wipe_mid_crash_recovers_after_intent_commit() {
    let path = RandomPath::tmp();
    let status = spawn_child("switch_wipe_midway", &path, "");
    assert_child_aborted(status, "switch wipe child must abort");

    // recovery must finish the pending wipe intent (finish_pending_wal_recycle
    // re-executes it) and reopen cleanly under the other route; the committed
    // key must be readable from the checkpointed data files
    let mace = open_relaxed(&path);
    let bucket = mace.open_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    assert_eq!(view.get("k").expect("pre-crash key missing").slice(), b"v");
    drop(view);
    drop(bucket);
    drop(mace);

    // each physical stream must have a contiguous (single-era) file range
    let opt = Options::new(&*path);
    let log_root = opt.log_root();
    for physical in 0..2 {
        let ids = wal_file_ids(&log_root, physical);
        for pair in ids.windows(2) {
            assert_eq!(
                pair[1],
                pair[0] + 1,
                "physical stream {physical} must have no wal gap after the wipe crash"
            );
        }
    }
}

#[test]
fn layout_migration_crash_after_remove_before_dir_sync_is_recoverable() {
    let path = RandomPath::tmp();
    let status = spawn_layout_migration_child(
        &path,
        "mace_wal_recycle_after_remove_before_dir_sync=abort@1",
    );
    assert_child_aborted(status, "layout migration must crash after remove");
    assert_mixed_groups_visible_after_layout_migration(&path);
}

#[test]
fn all_inactive_shared_recycle_crash_reopens_without_a_wal_gap() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "all_inactive_recycle",
        &path,
        "mace_wal_recycle_after_remove_before_dir_sync=abort@1",
    );
    assert_child_aborted(status, "all-inactive recycle must crash after remove");

    let mace = open_durable(&path, None);
    let bucket = mace.open_bucket("prod").expect("bucket prod should exist");
    let tx = bucket.begin().expect("post-recovery begin");
    tx.put("post", b"v").expect("post-recovery put");
    tx.commit().expect("post-recovery commit");
    assert_eq!(
        bucket
            .view()
            .expect("post-recovery view")
            .get("post")
            .expect("post-recovery value")
            .slice(),
        b"v"
    );
}

#[test]
fn layout_migration_crash_while_finishing_last_legacy_stream_is_recoverable() {
    let path = RandomPath::tmp();
    let status = spawn_layout_migration_child(
        &path,
        "mace_wal_recycle_after_dir_sync_before_done_commit=abort@3",
    );
    assert_child_aborted(status, "layout migration must crash before final done");
    assert_mixed_groups_visible_after_layout_migration(&path);
}

#[test]
fn layout_migration_crash_after_final_done_before_route_writeback_is_recoverable() {
    let path = RandomPath::tmp();
    let status = spawn_layout_migration_child(
        &path,
        "mace_wal_recycle_after_done_commit_before_publish=abort@3",
    );
    assert_child_aborted(status, "layout migration must crash after final done");
    assert_mixed_groups_visible_after_layout_migration(&path);
}

#[test]
fn shared_stream_recycle_crash_respects_multi_group_pin() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "wal_recycle_multi_group_shared",
        &path,
        "mace_wal_recycle_after_remove_before_dir_sync=abort@1",
    );
    assert_child_aborted(status, "recycle child must abort");

    // recovery must finish the pending recycle intent without touching the
    // file pinned by the group-1 txn: if the aggregate boundary forgot the
    // group-1 pin, the pin's wal chain would be recycled and recovery's
    // abort-clean would fail, so a successful reopen is the assertion
    let mace = open_durable(&path, None);
    let bucket = mace.open_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    for round in 0..8 {
        let key = format!("rw_{round}");
        assert_eq!(
            view.get(&key)
                .expect("committed recycle key missing")
                .slice(),
            vec![b'w'; 1024].as_slice()
        );
    }

    // retained wal files must stay contiguous (no half-recycled gap)
    let opt = Options::new(&*path);
    let log_root = opt.log_root();
    for physical in 0..2 {
        let ids = wal_file_ids(&log_root, physical);
        for pair in ids.windows(2) {
            assert_eq!(
                pair[1],
                pair[0] + 1,
                "physical stream {physical} must have no wal gap after recycle crash"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// earlier review fixes: switch-wipe durability and the crash-window matrix
// ---------------------------------------------------------------------------

const ENV_SWITCH_FAILPOINT: &str = "MACE_SWITCH_FAILPOINT";

fn ensure_bucket(mace: &Mace, name: &str) -> Bucket {
    match mace.open_bucket(name) {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket(
                name,
                BucketOptions {
                    inline_size: 4096,
                    cache_evict_pct: 10,
                    checkpoint_size: 32 << 10,
                    pool_capacity: 64 << 10,
                    enable_backpressure: false,
                    ..BucketOptions::default()
                },
            )
            .expect("create bucket failed"),
        Err(err) => panic!("open bucket failed: {err:?}"),
    }
}

fn spawn_switch_child(case: &str, db_root: &Path, switch_rule: &str) -> ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    child_test_command(&exe)
        .arg("--exact")
        .arg("gen_failpoint_child")
        .arg("--nocapture")
        .env(ENV_CHILD, "1")
        .env(ENV_CASE, case)
        .env(ENV_DB_ROOT, db_root.as_os_str())
        .env(ENV_SWITCH_FAILPOINT, switch_rule)
        .status()
        .expect("spawn switch failpoint child failed")
}

/// a failpoint crash aborts with SIGABRT; a wait_for_crash timeout is a panic
/// (exit 101). asserting the signal distinguishes "the failpoint fired" from
/// "the child timed out waiting for a failpoint that never fired"
#[cfg(unix)]
fn assert_aborted_at_failpoint(status: ExitStatus, msg: &str) {
    use std::os::unix::process::ExitStatusExt;

    assert!(!status.success(), "{msg}");
    assert_eq!(
        status.signal(),
        Some(6),
        "{msg}: expected SIGABRT from the failpoint crash, got {status:?}"
    );
}

#[cfg(not(unix))]
fn assert_aborted_at_failpoint(status: ExitStatus, msg: &str) {
    assert!(!status.success(), "{msg}");
}

fn assert_buckets_visible(mace: &Mace, count: usize) {
    let mut missing = Vec::new();
    for i in 0..count {
        let name = format!("b{i}");
        let bucket = mace.open_bucket(&name).expect("bucket must exist");
        let view = bucket.view().expect("open verify view failed");
        match view.get("k") {
            Ok(val) => {
                assert_eq!(
                    val.slice(),
                    format!("v{i}").as_bytes(),
                    "bucket {name} value mismatch"
                );
            }
            Err(_) => missing.push(name),
        }
    }
    assert!(
        missing.is_empty(),
        "committed keys missing in buckets: {missing:?}"
    );
}

fn assert_no_wal_gap(path: &Path) {
    assert_streams_gap_free(&Options::new(path).log_root(), 2);
}

/// the switch open must force-fsync the eviction of every redo-dirtied bucket:
/// the wipe deletes the wal that still guards the redone tail, so an
/// fdatasync-only eviction flush leaves file-size metadata un-durable and the
/// tail truncates on power loss (design P1). the failpoint lives only in the
/// force-fsync branch, so the crash itself proves the branch was taken
#[test]
fn switch_eviction_force_fsync_covers_redone_tail() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "switch_evict_many_buckets",
        &path,
        "mace_txn_commit_after_wal_sync=abort@24",
    );
    assert_aborted_at_failpoint(status, "tail-seed child must abort at commit 24");

    let status = spawn_switch_child(
        "switch_open_arm",
        &path,
        "mace_recovery_eviction_force_fsync=abort@1",
    );
    assert_aborted_at_failpoint(
        status,
        "recovery eviction must force-fsync under a pending switch",
    );

    // reopen: the switch re-runs (the writeback never happened), completes the
    // wipe, and every committed bucket must be readable
    let mace = open_relaxed(&path);
    assert_buckets_visible(&mace, 24);
    drop(mace);
    assert_no_wal_gap(&path);
}

/// crash window (before the forced checkpoint): the forced
/// full-fsync checkpoint of the still-loaded redone tail must be idempotent
/// across a crash
#[test]
fn switch_crash_before_forced_checkpoint_recovers() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "switch_evict_many_buckets",
        &path,
        "mace_txn_commit_after_wal_sync=abort@24",
    );
    assert_aborted_at_failpoint(status, "tail-seed child must abort at commit 24");

    let status = spawn_switch_child(
        "switch_open_arm",
        &path,
        "mace_switch_before_forced_checkpoint=abort@1",
    );
    assert_aborted_at_failpoint(
        status,
        "switch child must abort before the forced checkpoint",
    );

    let mace = open_relaxed(&path);
    assert_buckets_visible(&mace, 24);
    drop(mace);
    assert_no_wal_gap(&path);
}

/// a rebuild open over a database that kept no WAL history: phase 1 materializes the shared
/// stream's boot anchor (`max_frontier + 1`, and a bucket-less database has no frontier), so a
/// crash after that anchor and before the wipe must leave a namespace both retry widths can
/// open without a gap
#[test]
fn wal_free_rebuild_crash_before_the_wipe_retries_without_a_gap() {
    let rule = "mace_switch_before_forced_checkpoint=abort@1";
    let path = RandomPath::tmp();

    // a relaxed open that never creates a bucket, then a clean exit: the database is
    // WAL-independent, so removing every WAL file is legal and leaves no history at all
    {
        let mut opt = Options::new(&*path);
        opt.sync_on_write = false;
        opt.concurrent_write = NARROW;
        opt.gc_timeout = 60_000;
        let _mace = Mace::new(opt.validate().expect("options must validate")).expect("seed open");
    }
    let log_root = Options::new(&*path).log_root();
    for entry in std::fs::read_dir(&log_root)
        .expect("read log dir")
        .flatten()
    {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("wal_") || name.starts_with("group_wal_") {
            std::fs::remove_file(entry.path()).expect("remove wal file");
        }
    }

    // the durable rebuild materializes its shared anchor, then crashes before the wipe
    let status = spawn_switch_child("switch_open_arm_durable", &path, rule);
    assert_aborted_at_failpoint(status, "the wal-free rebuild must abort before the wipe");

    // the crash must land after phase 1 materialized the shared stream's boot anchor, and the
    // completed migration must then start its era above that anchor
    let crashed_shared = group_wal_file_ids(&log_root);
    assert!(
        !crashed_shared.is_empty(),
        "the crashed rebuild must leave the shared boot anchor behind"
    );
    let crash_high = crashed_shared.last().copied().unwrap_or(0);

    // the source-route retry is a plain reopen of the crashed namespace
    let mut source = Options::new(&*path);
    source.sync_on_write = false;
    source.concurrent_write = NARROW;
    source.gc_timeout = 60_000;
    let mace = Mace::new(source.validate().expect("options must validate"))
        .expect("the source retry must open the crashed rebuild");
    let bucket = ensure_bucket(&mace, "prod");
    drop(bucket);
    drop(mace);
    // a plain reopen materializes each relaxed stream's own anchor at its boot position, which in
    // a namespace with no history is zero: a wrongly-run rebuild would materialize it above that
    for physical in 0..NARROW {
        assert_eq!(
            wal_file_ids(&log_root, physical),
            vec![0],
            "the source retry must materialize stream {physical}'s own anchor at the inherited start"
        );
    }
    assert_eq!(
        group_wal_file_ids(&log_root),
        crashed_shared,
        "the source retry must not touch the crashed shared stream"
    );

    // the target-route retry completes the migration: the new era starts above the crashed
    // anchor's high water, and every swept stream ends with a durable done boundary at it
    let mut target = Options::new(&*path);
    target.sync_on_write = true;
    target.concurrent_write = NARROW;
    target.gc_timeout = 60_000;
    let mace = Mace::new(target.validate().expect("options must validate"))
        .expect("the target retry must complete the migration");
    let bucket = ensure_bucket(&mace, "prod");
    let era_start = testing::logger_oldest_wal_id(&bucket, 0);
    assert!(
        era_start > crash_high,
        "the retry must start its era above the crashed high water: {era_start} against {crash_high}"
    );
    for physical in (0..NARROW).chain(std::iter::once(Options::SHARED_ID)) {
        let (from, to, done) = testing::wal_recycle_state(&bucket, physical);
        assert!(
            done && to == era_start && from < to,
            "stream {physical} must hold a completed wipe at the era start: ({from}, {to}, done={done}) against {era_start}"
        );
    }
    drop(bucket);
    drop(mace);
    // the durable target retires the per-group namespace, and the shared stream must be exactly
    // the new era's file: a leftover per-group or higher-numbered shared file would be the gap
    // this commit exists to prevent
    for physical in 0..NARROW {
        assert!(
            wal_file_ids(&log_root, physical).is_empty(),
            "the completed migration must retire per-group stream {physical}: {:?}",
            wal_file_ids(&log_root, physical)
        );
    }
    assert_eq!(
        group_wal_file_ids(&log_root),
        vec![era_start],
        "the completed migration must leave the shared stream on its era file alone"
    );
    assert_eq!(
        persisted_width(&Options::new(&*path)),
        NARROW,
        "the completed retry must record the same width"
    );
    assert!(
        persisted_route(&Options::new(&*path)),
        "the completed retry must record the durable route"
    );

    // and the namespace must still open afterwards
    let mut reopen = Options::new(&*path);
    reopen.sync_on_write = true;
    reopen.concurrent_write = NARROW;
    reopen.gc_timeout = 60_000;
    let mace = Mace::new(reopen.validate().expect("options must validate"))
        .expect("the migrated namespace must reopen");
    drop(mace);
}

/// crash window "writeback-before": the wipe completed and the wal is gone,
/// but the options writeback never happened; the next open must re-detect the
/// switch and complete it with no files to redo or wipe
#[test]
fn switch_crash_after_wipe_before_writeback_recovers() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "switch_evict_many_buckets",
        &path,
        "mace_txn_commit_after_wal_sync=abort@24",
    );
    assert_aborted_at_failpoint(status, "tail-seed child must abort at commit 24");

    let status = spawn_switch_child(
        "switch_open_arm",
        &path,
        "mace_switch_after_wipe_before_writeback=abort@1",
    );
    assert_aborted_at_failpoint(status, "switch child must abort after the wipe");

    let mace = open_relaxed(&path);
    assert_buckets_visible(&mace, 24);
    drop(mace);
    assert_no_wal_gap(&path);
}

/// crash window "writeback-after": the switch completed and the writeback
/// recorded the relaxed route, so the next open is same-route: no epoch, no
/// wipe, streams continue in place (a second switch open would wipe and bump
/// the era, leaving the wal ids changed)
#[test]
fn switch_crash_after_writeback_reopens_same_route() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "switch_evict_many_buckets",
        &path,
        "mace_txn_commit_after_wal_sync=abort@24",
    );
    assert_aborted_at_failpoint(status, "tail-seed child must abort at commit 24");

    let status = spawn_switch_child(
        "switch_open_arm",
        &path,
        "mace_switch_after_options_writeback=abort@1",
    );
    assert_aborted_at_failpoint(
        status,
        "switch child must abort after the options writeback",
    );

    let mace = open_relaxed(&path);
    assert_buckets_visible(&mace, 24);
    let bucket = ensure_bucket(&mace, "b0");
    let txn = bucket.begin().expect("begin txn failed");
    txn.put("k2", b"v2").expect("put failed");
    txn.commit().expect("commit failed");
    drop(bucket);
    drop(mace);
    // graceful exit recycles wal to the last file per stream; snapshot it
    let opt = Options::new(&*path);
    let log_root = opt.log_root();
    let after_first = (0..2)
        .map(|physical| wal_file_ids(&log_root, physical))
        .collect::<Vec<_>>();

    // same-route reopen must not wipe or bump the era
    let mace = open_relaxed(&path);
    let after_second = (0..2)
        .map(|physical| wal_file_ids(&log_root, physical))
        .collect::<Vec<_>>();
    assert_eq!(
        after_first, after_second,
        "same-route reopen after a completed switch must keep the wal streams untouched"
    );
    let bucket = mace.open_bucket("b0").expect("bucket must exist");
    let view = bucket.view().expect("open verify view failed");
    assert_eq!(view.get("k2").expect("key missing").slice(), b"v2");
    drop(view);
    drop(bucket);
    drop(mace);
    assert_no_wal_gap(&path);
}

/// switch with a pending aborted txn: recovery reconstructs the abort-clean
/// chain and drains it before the wipe; crashing right after the drain proves
/// it ran, and the reopen proves the drain + wipe left no orphan pin
#[test]
fn switch_with_pending_abort_drains_before_wipe() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "switch_pending_abort",
        &path,
        "mace_txn_commit_after_wal_sync=abort@1",
    );
    assert_aborted_at_failpoint(status, "pending-abort child must abort at commit");

    let status = spawn_switch_child(
        "switch_open_arm",
        &path,
        "mace_recovery_abort_clean_after_drain_before_start=abort@1",
    );
    assert_aborted_at_failpoint(
        status,
        "switch child must abort after the reconstructed abort-clean drain",
    );

    let mace = open_relaxed(&path);
    let bucket = mace.open_bucket("prod").expect("bucket prod must exist");
    let view = bucket.view().expect("open verify view failed");
    assert_eq!(view.get("k").expect("committed key missing").slice(), b"v");
    assert!(
        view.get("pending").is_err(),
        "aborted key must not be visible after the switch"
    );
    drop(view);
    drop(bucket);
    drop(mace);
    assert_no_wal_gap(&path);
}

/// root-lsn regression (same-route domain): a recovery-created tree root that
/// stamps the synthetic append position would make an aborted recovery's
/// eviction flush advance the bucket frontier past records that were never
/// materialized; the next same-route open would then gate those records out
/// and lose the committed tail. the fix stamps recovery roots with
/// Position::MIN so the frontier only advances to really flushed records
#[test]
fn same_route_crash_during_recovery_eviction_keeps_redone_tail() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "switch_evict_many_buckets",
        &path,
        "mace_txn_commit_after_wal_sync=abort@24",
    );
    assert_aborted_at_failpoint(status, "tail-seed child must abort at commit 24");

    // same-route durable reopen that dies at the first recovery eviction flush
    let status = spawn_switch_child(
        "switch_open_arm_durable",
        &path,
        "mace_recovery_eviction_flush=abort@1",
    );
    assert_aborted_at_failpoint(
        status,
        "durable recovery eviction must abort at the failpoint",
    );

    let mace = open_durable(&path, None);
    assert_buckets_visible(&mace, 24);
    drop(mace);
    assert_no_wal_gap(&path);
}

/// abort-clean scrub durability on the switch path: a bucket loaded ONLY by
/// the reconstructed abort-clean drain (evicted from the recovery lru during
/// analyze, no committed records) is scrubbed by stabilize_cleaned_pages and
/// must be force-fsynced there -- the wipe deletes the wal that would
/// otherwise re-undo the scrub after power loss. the failpoint lives only in
/// the force-fsync branch, so the crash proves the branch was taken
#[test]
fn switch_abort_clean_scrub_force_fsyncs_evicted_bucket() {
    let path = RandomPath::tmp();
    let status = spawn_child(
        "switch_abort_evict_many",
        &path,
        "mace_txn_commit_after_wal_sync=abort@23",
    );
    assert_aborted_at_failpoint(status, "tail-seed child must abort at commit 23");

    let status = spawn_switch_child(
        "switch_open_arm",
        &path,
        "mace_recovery_abort_clean_stabilize_force_fsync=abort@1",
    );
    assert_aborted_at_failpoint(
        status,
        "abort-clean stabilize must force-fsync under a pending switch",
    );

    let mace = open_relaxed(&path);
    assert_buckets_visible(&mace, 23);
    let bucket = mace.open_bucket("b23").expect("bucket b23 must exist");
    let view = bucket.view().expect("open verify view failed");
    assert!(
        view.get("pending").is_err(),
        "aborted key in the evicted bucket must not be visible after the switch"
    );
    drop(view);
    drop(bucket);
    drop(mace);
    assert_no_wal_gap(&path);
}

// ---------------------------------------------------------------------------
// concurrent-write width migration: crash windows of the epoch rebuild
// ---------------------------------------------------------------------------

const WIDE: u8 = 8;
const NARROW: u8 = 2;
/// the design matrix's `4 -> 2` / `2 -> 4` width, wider than NARROW but small enough that the
/// wipe's per-stream windows stay enumerable
const MID: u8 = 4;
const ENV_CW_WIDTH: &str = "MACE_CW_WIDTH";
const ENV_CW_SOURCE_ROUTE: &str = "MACE_CW_SOURCE_ROUTE";
const ENV_CW_TARGET_ROUTE: &str = "MACE_CW_TARGET_ROUTE";
const CW_SEED_KEYS: u32 = 64;

fn env_cw_width() -> u8 {
    std::env::var(ENV_CW_WIDTH)
        .expect("missing concurrent-write width")
        .parse()
        .expect("concurrent-write width must fit a byte")
}

/// a child's route, defaulting to relaxed so the pre-existing width cases keep their shape
fn env_cw_durable(name: &str) -> bool {
    match std::env::var(name).ok().as_deref() {
        Some("durable") => true,
        Some("relaxed") | None => false,
        Some(other) => panic!("unknown concurrent-write route {other}"),
    }
}

/// one transaction per key: sequential begins rotate through `ticket % width`, so every
/// writer group of the seeded width owns a wal file
fn seed_one_per_key(bucket: &Bucket, count: u32) {
    for idx in 0..count {
        let txn = bucket.begin().expect("begin seed txn failed");
        txn.upsert(format!("k_{idx}"), format!("v_{idx}"))
            .expect("upsert seed key failed");
        txn.commit().expect("commit seed txn failed");
    }
}

fn assert_one_per_key(bucket: &Bucket, count: u32) {
    let view = bucket.view().expect("open verify view failed");
    for idx in 0..count {
        let key = format!("k_{idx}");
        let val = view
            .get(&key)
            .expect("committed key missing after the crash");
        assert_eq!(
            val.slice(),
            format!("v_{idx}").as_bytes(),
            "value mismatch for {key}"
        );
    }
}

/// seed at the source width and crash when the trailing txn enters commit. the first
/// `CW_SEED_KEYS` txns stay wal-durable; the abandoned tail is a separate txn whose update
/// records and abort record are flushed durably before the crash, so recovery must actually
/// roll its writes back instead of never having seen the tail at all. the crash is the armed
/// `MACE_FAILPOINT` rule, so no graceful exit checkpoints the tail away and the migration open
/// below has real redo and abort-clean work
fn child_cw_seed(db_root: &Path) -> ! {
    let width = env_cw_width();
    let durable = env_cw_durable(ENV_CW_SOURCE_ROUTE);
    let mace = open_with(db_root, width, durable, None);
    let bucket = ensure_bucket(&mace, "prod");
    seed_one_per_key(&bucket, CW_SEED_KEYS);

    // abandoning a modified txn makes the engine record an abort and flush the group ring, so the
    // tail key's update records and its abort record are both on disk when the process aborts;
    // the published abort-clean task is the in-process proof that the fixture really created a
    // durable abandoned tail (an unsynced ring would leave recovery nothing to reconstruct)
    let tail = bucket.begin().expect("begin tail txn failed");
    let tail_txid = testing::txn_start_ts(&tail);
    tail.put("tail", b"lost").expect("put tail key failed");
    drop(tail);
    assert!(
        testing::abort_clean_task_info(&bucket, tail_txid).is_some(),
        "the abandoned tail must be durable enough to publish an abort-clean task"
    );

    let crash = bucket.begin().expect("begin crash txn failed");
    crash.put("crash", b"lost").expect("put crash key failed");
    crash
        .commit()
        .expect("crash commit must abort at the failpoint");
    wait_for_crash(Duration::from_secs(2))
}

/// reopen at the target width and route with the requested crash window armed: a width change
/// must enter the epoch rebuild and crash inside the window
fn child_cw_migrate(db_root: &Path) -> ! {
    let width = env_cw_width();
    let durable = env_cw_durable(ENV_CW_TARGET_ROUTE);
    if let Ok(rule) = std::env::var(ENV_SWITCH_FAILPOINT) {
        testing::arm_failpoint_rule(&rule);
    }
    let _mace = open_with(db_root, width, durable, None);
    wait_for_crash(Duration::from_secs(2))
}

fn spawn_cw_seed_child(db_root: &Path, width: u8, durable: bool) -> ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    child_test_command(&exe)
        .arg("--exact")
        .arg("gen_failpoint_child")
        .arg("--nocapture")
        .env(ENV_CHILD, "1")
        .env(ENV_CASE, "cw_seed")
        .env(ENV_DB_ROOT, db_root.as_os_str())
        .env(ENV_CW_WIDTH, width.to_string())
        .env(
            ENV_CW_SOURCE_ROUTE,
            if durable { "durable" } else { "relaxed" },
        )
        .env(
            "MACE_FAILPOINT",
            format!("mace_txn_commit_begin=abort@{}", CW_SEED_KEYS + 1),
        )
        .status()
        .expect("spawn concurrent-write seed child failed")
}

/// one migration-open crash window: the target width and route plus the in-process named rule
/// (if any)
struct CwWindow<'a> {
    width: u8,
    durable: bool,
    name: Option<&'a str>,
}

fn spawn_cw_migrate_child(db_root: &Path, window: &CwWindow<'_>) -> ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    let mut cmd = child_test_command(&exe);
    cmd.arg("--exact")
        .arg("gen_failpoint_child")
        .arg("--nocapture")
        .env(ENV_CHILD, "1")
        .env(ENV_CASE, "cw_migrate")
        .env(ENV_DB_ROOT, db_root.as_os_str())
        .env(ENV_CW_WIDTH, window.width.to_string())
        .env(
            ENV_CW_TARGET_ROUTE,
            if window.durable { "durable" } else { "relaxed" },
        );
    if let Some(rule) = window.name {
        cmd.env(ENV_SWITCH_FAILPOINT, rule);
    }
    cmd.status()
        .expect("spawn concurrent-write migrate child failed")
}

/// the original width cases: relaxed source, relaxed target, named window only
fn cw_named_window(width: u8, rule: &str) -> CwWindow<'_> {
    CwWindow {
        width,
        durable: false,
        name: Some(rule),
    }
}

/// highest wal file id anywhere in the source-width namespace, captured after the seed crash
fn max_wal_file_id(log_root: &Path, width: u8) -> u64 {
    let mut high = 0;
    for physical in 0..width {
        if let Some(id) = wal_file_ids(log_root, physical).last() {
            high = high.max(*id);
        }
    }
    if let Some(id) = group_wal_file_ids(log_root).last() {
        high = high.max(*id);
    }
    high
}

fn assert_streams_gap_free(log_root: &Path, width: u8) {
    for physical in 0..width {
        let ids = wal_file_ids(log_root, physical);
        for pair in ids.windows(2) {
            assert_eq!(
                pair[1],
                pair[0] + 1,
                "physical stream {physical} must have no wal gap: {ids:?}"
            );
        }
    }
}

/// the persisted route marker: the last route whose transition completed. the writeback fires
/// whenever any persisted field differs from the request, so this reader is a discriminator only
/// in fixtures whose sole difference is the route, where it separates a recorded switch from a
/// crashed one
fn persisted_route(options: &Options) -> bool {
    let tree = BTree::open(options.manifest()).expect("open manifest");
    let mut raw = None;
    tree.view("misc", |txn| {
        raw = Some(txn.get("options")?);
        Ok(())
    })
    .expect("view persisted options");
    let value: serde_json::Value =
        serde_json::from_slice(&raw.expect("persisted options record")).expect("options json");
    value["sync_on_write"]
        .as_bool()
        .expect("persisted route marker")
}

/// the width recorded in the manifest's persisted options: the writeback runs only when a
/// migration completes, so reading it proves the target width was recorded
fn persisted_width(options: &Options) -> u8 {
    let tree = BTree::open(options.manifest()).expect("open manifest");
    let mut raw = None;
    tree.view("misc", |txn| {
        raw = Some(txn.get("options")?);
        Ok(())
    })
    .expect("view persisted options");
    let value: serde_json::Value =
        serde_json::from_slice(&raw.expect("persisted options record")).expect("options json");
    value["concurrent_write"]
        .as_u64()
        .expect("persisted width")
        .try_into()
        .expect("width fits a byte")
}

/// every wal file id in the namespace, per stream, in stream order (per-group streams first,
/// then the shared durable stream)
fn all_wal_ids(log_root: &Path, width: u8) -> Vec<Vec<u64>> {
    let mut out: Vec<Vec<u64>> = (0..width).map(|p| wal_file_ids(log_root, p)).collect();
    out.push(group_wal_file_ids(log_root));
    out
}

/// the surviving writer groups of `count` sequential transactions
fn sequential_groups(bucket: &Bucket, count: u32) -> BTreeSet<usize> {
    let mut groups = BTreeSet::new();
    for i in 0..count {
        let tx = bucket.begin().expect("begin probe txn");
        groups.insert(
            testing::txn_group(bucket, testing::txn_start_ts(&tx))
                .expect("a begun txn has a group"),
        );
        tx.put(format!("probe_{i}"), b"v").expect("put probe");
        tx.commit().expect("commit probe");
    }
    groups
}

/// seed the source width and crash on its tail, returning the pre-migration id high-water
fn cw_seed_and_crash(source_width: u8, durable: bool) -> (RandomPath, u64) {
    let path = RandomPath::tmp();
    let status = spawn_cw_seed_child(&path, source_width, durable);
    assert_aborted_at_failpoint(status, "seed child must abort on its trailing commit");

    // the seed must have spread over every writer group, or the migration has no source
    // streams to retire (a durable source keeps them all in the shared stream)
    let log_root = Options::new(&*path).log_root();
    if durable {
        assert!(
            !group_wal_file_ids(&log_root).is_empty(),
            "a durable source must own a shared wal file"
        );
    } else {
        for physical in 0..source_width {
            assert!(
                !wal_file_ids(&log_root, physical).is_empty(),
                "source stream {physical} owns no wal file: the seed did not cover every writer group"
            );
        }
    }
    let high_water = max_wal_file_id(&log_root, source_width);
    (path, high_water)
}

/// reopen at the target width after a migration crash and require convergence: every
/// committed key visible, the in-progress tail rolled back, a gap-free namespace, and every
/// target stream owning an era file above the pre-migration high-water. returns the persisted
/// width so the caller can assert the migration was recorded
fn assert_cw_target_retry(
    path: &Path,
    retry_width: u8,
    target_width: u8,
    pre_max: u64,
    durable: bool,
) -> u8 {
    let mace = open_with(path, retry_width, durable, None);
    let bucket = mace.open_bucket("prod").expect("bucket prod must exist");
    assert_one_per_key(&bucket, CW_SEED_KEYS);
    assert!(
        bucket
            .view()
            .expect("open verify view failed")
            .get("tail")
            .is_err(),
        "the in-progress tail txn must be rolled back by recovery"
    );

    // the retry's epoch rebuild must leave every target stream owning an era file above the
    // pre-migration namespace, with no gap inside a stream (no id reuse across the wipe)
    let log_root = Options::new(path).log_root();
    assert_streams_gap_free(&log_root, retry_width);
    if retry_width == target_width {
        if durable {
            let ids = group_wal_file_ids(&log_root);
            assert!(
                !ids.is_empty(),
                "a durable target must own a shared era file after the retry"
            );
            for id in &ids {
                assert!(
                    *id > pre_max,
                    "the shared target stream reuses pre-migration file id {id} (high-water {pre_max})"
                );
            }
        } else {
            for physical in 0..retry_width {
                let ids = wal_file_ids(&log_root, physical);
                assert!(
                    !ids.is_empty(),
                    "target stream {physical} owns no era file after the retry"
                );
                for id in &ids {
                    assert!(
                        *id > pre_max,
                        "target stream {physical} reuses pre-migration file id {id} (high-water {pre_max})"
                    );
                }
            }
        }
    }

    // the migrated runtime still accepts writes on every target group
    seed_one_per_key(&bucket, retry_width as u32 * 2);

    // a completed migration must leave a durable done boundary on every swept stream: one
    // stuck at STAGE_INTENT keeps its era files from ever being recycled, and the gap-free
    // scan above only looks at the streams the retry width owns
    for physical in (0..Options::MAX_CONCURRENT_WRITE).chain(std::iter::once(Options::SHARED_ID)) {
        let (from, to, done) = testing::wal_recycle_state(&bucket, physical);
        assert!(
            done || to == 0,
            "stream {physical} still holds an unfinished recycle intent after the retry: ({from}, {to}, done={done})"
        );
    }
    drop(bucket);
    drop(mace);
    persisted_width(&Options::new(path))
}

/// reopen at the SOURCE width after a migration crash: the migration was never recorded, so the
/// request matches the persisted width and this is a plain reopen that must keep every committed
/// key and report no corruption. on the relaxed route it must not rebuild the era it already
/// inherited: a stream that owned a file keeps it, and every stream that owns one now starts
/// exactly at the durable done boundary its wipe recorded (which is where a plain reopen
/// materializes a missing anchor). a durable open still owns the legacy-layout rebuild when the
/// failed attempt left per-group era files, so there the check is that the rebuild reuses no
/// pre-era file id
fn assert_cw_source_retry(path: &Path, source_width: u8, durable: bool) {
    let log_root = Options::new(path).log_root();
    // a rebuild would move every stream onto a fresh file id, so the streams that already
    // own a file before the open are the discriminators this retry was missing
    let crashed = all_wal_ids(&log_root, source_width);
    // the crashed attempt never recorded its width, so this open is a plain reopen by
    // construction: the request matches the persisted width and no rebuild may run
    assert_eq!(
        persisted_width(&Options::new(path)),
        source_width,
        "a source-width retry must open at the persisted width"
    );
    let mace = open_with(path, source_width, durable, None);
    let bucket = mace.open_bucket("prod").expect("bucket prod must exist");
    assert_one_per_key(&bucket, CW_SEED_KEYS);
    assert!(
        bucket
            .view()
            .expect("open verify view failed")
            .get("tail")
            .is_err(),
        "the in-progress tail txn must be rolled back by recovery"
    );
    // the durable wipe boundary of every per-group stream, read while the store is open
    let boundaries: Vec<(u64, bool)> = (0..source_width)
        .map(|physical| {
            let (_, to, done) = testing::wal_recycle_state(&bucket, physical);
            (to, done)
        })
        .collect();
    drop(bucket);
    drop(mace);
    assert_streams_gap_free(&log_root, source_width);
    let after = all_wal_ids(&log_root, source_width);
    if durable {
        // a durable open still owns the legacy-layout rebuild when the failed attempt left
        // per-group era files: that rebuild must open a strictly newer epoch rather than reuse
        // a file id at or below the pre-era high-water
        if after != crashed {
            let high_water = crashed.iter().flatten().copied().max().unwrap_or(0);
            assert!(
                after.iter().flatten().all(|id| *id > high_water),
                "a durable source-width retry reused a pre-era file id: {after:?} at or below the high-water {high_water}"
            );
        }
        // the durable steady state never writes per-group files, so a leftover one would keep
        // selecting the layout migration on every later open
        assert!(
            (0..source_width).all(|physical| wal_file_ids(&log_root, physical).is_empty()),
            "a durable source-width retry must retire the legacy per-group namespace"
        );
    } else {
        // the relaxed route's reopen must not rebuild the era it already inherited
        for (physical, before) in crashed.iter().enumerate() {
            if physical == source_width as usize {
                // the shared stream belongs to the durable route: a relaxed reopen neither
                // writes it nor recycles it, so its file set must be exactly the crash's
                assert_eq!(
                    group_wal_file_ids(&log_root),
                    *before,
                    "a relaxed source-width retry must not touch the shared stream"
                );
                continue;
            }
            let ids = wal_file_ids(&log_root, physical as u8);
            if !before.is_empty() {
                // a stream that already owned a file keeps its ids: a rebuild would have
                // recycled them and opened a fresh era above the inherited boundary
                assert_eq!(
                    &ids, before,
                    "a source-width retry must not rebuild the era it already inherited"
                );
            }
            let (to, done) = boundaries[physical];
            if done && to > 0 {
                // a done slot's oldest id is exactly the boundary it recorded, and a fileless
                // stream boots there, so the retry's own era anchor -- the file its runtime
                // writer opens -- sits exactly at it; a recycled id would open above it
                assert_eq!(
                    ids.first().copied(),
                    Some(to),
                    "stream {physical} must own its era anchor at the durable done boundary: {ids:?} against {to}"
                );
            }
        }
    }
}

/// crash window "before the forced checkpoint": the forced full-fsync checkpoint of the
/// redone tail must be idempotent across the width migration's crash
#[test]
fn concurrent_write_shrink_crash_before_forced_checkpoint_recovers() {
    let (path, pre_max) = cw_seed_and_crash(WIDE, false);
    let status = spawn_cw_migrate_child(
        &path,
        &cw_named_window(NARROW, "mace_switch_before_forced_checkpoint=abort@1"),
    );
    assert_aborted_at_failpoint(status, "shrink must abort before the forced checkpoint");
    assert_eq!(
        assert_cw_target_retry(&path, NARROW, NARROW, pre_max, false),
        NARROW,
        "the completed migration must record the target width"
    );
}

/// crash window "after the forced checkpoint before the wipe": the redone tail is durable and
/// the old era is still intact. the retry must reach the same era start and complete the wipe
/// on both directions
#[test]
fn concurrent_write_crash_after_forced_checkpoint_before_wipe_recovers() {
    let rule = "mace_switch_after_forced_checkpoint_before_wipe=abort@1";
    let (path, pre_max) = cw_seed_and_crash(NARROW, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(WIDE, rule));
    assert_aborted_at_failpoint(
        status,
        "grow must abort after the forced checkpoint before the wipe",
    );
    assert_eq!(
        assert_cw_target_retry(&path, WIDE, WIDE, pre_max, false),
        WIDE,
        "the completed migration must record the target width"
    );

    let (path, pre_max) = cw_seed_and_crash(WIDE, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(NARROW, rule));
    assert_aborted_at_failpoint(
        status,
        "shrink must abort after the forced checkpoint before the wipe",
    );
    assert_eq!(
        assert_cw_target_retry(&path, NARROW, NARROW, pre_max, false),
        NARROW,
        "the completed migration must record the target width"
    );
}

/// crash window "recovery eviction force-fsync": a redo-dirtied bucket evicted during the
/// width migration's recovery must force-fsync before the wipe deletes its wal. the growth
/// 2 -> 8 has target-only physical streams with no pre-migration wal, so the retry must own
/// an era file above the pre-migration namespace on every target stream
#[test]
fn concurrent_write_grow_crash_during_recovery_eviction_recovers() {
    let (path, pre_max) = cw_seed_and_crash(NARROW, false);
    let status = spawn_cw_migrate_child(
        &path,
        &cw_named_window(WIDE, "mace_recovery_eviction_force_fsync=abort@1"),
    );
    assert_aborted_at_failpoint(status, "grow must abort during recovery eviction");
    assert_eq!(
        assert_cw_target_retry(&path, WIDE, WIDE, pre_max, false),
        WIDE,
        "the completed migration must record the target width"
    );
}

/// crash window "era wipe half complete": the first swept slot (the shared stream, which owns no
/// files for a relaxed source) had its intent published and the log dir synced, but its done
/// boundary was never committed. the target retry must finish the intent; a source-width retry
/// must also survive on the checkpointed data
#[test]
fn concurrent_write_shrink_crash_mid_era_wipe_recovers() {
    let rule = "mace_wal_recycle_after_dir_sync_before_done_commit=abort@1";
    let (path, pre_max) = cw_seed_and_crash(WIDE, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(NARROW, rule));
    assert_aborted_at_failpoint(status, "shrink must abort mid era wipe");
    assert_eq!(
        assert_cw_target_retry(&path, NARROW, NARROW, pre_max, false),
        NARROW,
        "the completed migration must record the target width"
    );

    let (path, _) = cw_seed_and_crash(WIDE, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(NARROW, rule));
    assert_aborted_at_failpoint(status, "shrink must abort mid era wipe");
    assert_cw_source_retry(&path, WIDE, false);
}

/// crash window "after the wipe before the writeback": the wal is gone but the open never
/// recorded the target width. the target retry must re-detect and finish the migration; a
/// source-width retry must read the checkpointed data with no wal left
#[test]
fn concurrent_write_grow_crash_after_wipe_before_writeback_recovers() {
    let rule = "mace_switch_after_wipe_before_writeback=abort@1";
    let (path, pre_max) = cw_seed_and_crash(NARROW, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(WIDE, rule));
    assert_aborted_at_failpoint(status, "grow must abort after the wipe");
    assert_eq!(
        assert_cw_target_retry(&path, WIDE, WIDE, pre_max, false),
        WIDE,
        "the completed migration must record the target width"
    );

    let (path, _) = cw_seed_and_crash(NARROW, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(WIDE, rule));
    assert_aborted_at_failpoint(status, "grow must abort after the wipe");
    assert_cw_source_retry(&path, NARROW, false);
}

/// the shrink direction of the "after the wipe before the writeback" window: the era is already
/// at the narrow target while the width is unrecorded, so both retry widths must converge on
/// the wiped namespace instead of rebuilding it
#[test]
fn concurrent_write_shrink_crash_after_wipe_before_writeback_recovers() {
    let rule = "mace_switch_after_wipe_before_writeback=abort@1";
    let (path, pre_max) = cw_seed_and_crash(WIDE, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(NARROW, rule));
    assert_aborted_at_failpoint(status, "shrink must abort after the wipe");
    assert_eq!(
        assert_cw_target_retry(&path, NARROW, NARROW, pre_max, false),
        NARROW,
        "the completed migration must record the target width"
    );

    let (path, _) = cw_seed_and_crash(WIDE, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(NARROW, rule));
    assert_aborted_at_failpoint(status, "shrink must abort after the wipe");
    assert_cw_source_retry(&path, WIDE, false);
}

/// crash window "after the target context install before the options writeback": the runtime
/// target context is installed but the width is not yet recorded. the next open at the target
/// width must complete the migration and record it
#[test]
fn concurrent_write_shrink_crash_after_context_install_recovers() {
    let (path, pre_max) = cw_seed_and_crash(WIDE, false);
    let status = spawn_cw_migrate_child(
        &path,
        &cw_named_window(
            NARROW,
            "mace_switch_after_context_install_before_options_writeback=abort@1",
        ),
    );
    assert_aborted_at_failpoint(status, "shrink must abort after the target context install");
    assert_eq!(
        assert_cw_target_retry(&path, NARROW, NARROW, pre_max, false),
        NARROW,
        "the completed migration must record the target width"
    );
}

// ---------------------------------------------------------------------------
// the remaining design crash rows: the retired source stream's recycle-intent
// windows, the last swept stream (source-only and target-only), the recorded
// writeback, the handoff retried at the persisted source count, and the two
// route+width combinations
// ---------------------------------------------------------------------------

/// crash window "source recycle intent": the retired source stream's intent is durable but its
/// files, directory sync or done commit never completed. the target retry must finish the
/// intent first and show no wal gap. the first swept slot is the file-less shared stream, and
/// the two per-slot hooks also cover the last swept stream (physical 7); the removal hook fires
/// per removed file rather than per swept slot, so it has no slot-indexed form
#[test]
fn concurrent_write_shrink_source_recycle_intent_windows_recover() {
    for rule in [
        "mace_wal_recycle_before_intent_commit=abort@1",
        "mace_wal_recycle_after_remove_before_dir_sync=abort@1",
        "mace_wal_recycle_after_done_commit_before_publish=abort@1",
        "mace_wal_recycle_after_done_commit_before_publish=abort@9",
    ] {
        let (path, pre_max) = cw_seed_and_crash(WIDE, false);
        let status = spawn_cw_migrate_child(&path, &cw_named_window(NARROW, rule));
        assert_aborted_at_failpoint(status, "shrink must abort in the recycle intent window");
        assert_eq!(
            assert_cw_target_retry(&path, NARROW, NARROW, pre_max, false),
            NARROW,
            "the completed migration must record the target width ({rule})"
        );
    }
}

/// the wipe processes the shared stream first and the highest stream id last: crashing on the
/// last swept source-only stream must still leave a retryable namespace
#[test]
fn concurrent_write_shrink_crash_on_the_last_swept_stream_recovers() {
    let rule = "mace_wal_recycle_after_dir_sync_before_done_commit=abort@9";
    let (path, pre_max) = cw_seed_and_crash(WIDE, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(NARROW, rule));
    assert_aborted_at_failpoint(status, "shrink must abort on the last swept stream");
    assert_eq!(
        assert_cw_target_retry(&path, NARROW, NARROW, pre_max, false),
        NARROW,
        "the completed migration must record the target width"
    );
}

/// the same window on the last swept target-only stream of a grow: that stream has no
/// pre-migration wal, so its intent is the one the retry has to reconstruct
#[test]
fn concurrent_write_grow_crash_on_the_last_swept_target_only_stream_recovers() {
    let rule = "mace_wal_recycle_after_dir_sync_before_done_commit=abort@9";
    let (path, pre_max) = cw_seed_and_crash(NARROW, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(WIDE, rule));
    assert_aborted_at_failpoint(
        status,
        "grow must abort on the last swept target-only stream",
    );
    assert_eq!(
        assert_cw_target_retry(&path, WIDE, WIDE, pre_max, false),
        WIDE,
        "the completed migration must record the target width"
    );
}

/// crash window "after the options writeback": the migration is already recorded, so the next
/// open is a plain target-width open that must not wipe or open a second era
#[test]
fn concurrent_write_grow_crash_after_options_writeback_reopens_plain() {
    let rule = "mace_switch_after_options_writeback=abort@1";
    let (path, _) = cw_seed_and_crash(NARROW, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(WIDE, rule));
    assert_aborted_at_failpoint(status, "grow must abort after the recorded writeback");

    let log_root = Options::new(&*path).log_root();
    let recorded = all_wal_ids(&log_root, WIDE);
    let mace = open_relaxed_at(&path, WIDE);
    let bucket = mace.open_bucket("prod").expect("bucket prod must exist");
    assert_one_per_key(&bucket, CW_SEED_KEYS);
    drop(bucket);
    drop(mace);

    for (stream, before) in recorded.iter().enumerate() {
        let ceiling = before.iter().copied().max().unwrap_or(0);
        let after = if stream == WIDE as usize {
            group_wal_file_ids(&log_root)
        } else {
            wal_file_ids(&log_root, stream as u8)
        };
        for id in &after {
            assert!(
                *id <= ceiling,
                "stream {stream} opened a second era after a recorded migration: {after:?}"
            );
        }
    }
    assert_eq!(persisted_width(&Options::new(&*path)), WIDE);
}

/// crash window "context handoff": the grow+relaxed attempt crashed after the target context
/// replaced the store context and before the writeback. the next open must still recover at
/// the persisted source count, leaving the target-only era files of the failed attempt in
/// place, and a later rebuild must retire them above the new era start instead of reusing ids
#[test]
fn concurrent_write_grow_handoff_crash_retries_at_the_source_count() {
    let rule = "mace_switch_after_context_install_before_options_writeback=abort@1";
    let (path, pre_max) = cw_seed_and_crash(NARROW, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(WIDE, rule));
    assert_aborted_at_failpoint(status, "grow must abort after the context install");

    let log_root = Options::new(&*path).log_root();
    for physical in NARROW..WIDE {
        for id in wal_file_ids(&log_root, physical) {
            assert!(
                id > pre_max,
                "target-only stream {physical} holds pre-era file id {id}"
            );
        }
    }

    assert_cw_source_retry(&path, NARROW, false);
    for physical in NARROW..WIDE {
        assert!(
            !wal_file_ids(&log_root, physical).is_empty(),
            "a source-width retry must leave the uncovered target-only streams untouched"
        );
    }

    let leftovers = max_wal_file_id(&log_root, WIDE);
    assert_eq!(
        assert_cw_target_retry(&path, WIDE, WIDE, pre_max, false),
        WIDE,
        "the later open must complete the migration"
    );
    for physical in 0..WIDE {
        let ids = wal_file_ids(&log_root, physical);
        assert!(
            ids.iter().all(|id| *id > leftovers),
            "stream {physical} kept a leftover id at or below {leftovers}: {ids:?}"
        );
    }
}

/// the two route+width combinations, each crashed in the handoff window and retried on the
/// target route: a relaxed source with a durable target and a durable source with a relaxed one
#[test]
fn concurrent_write_route_and_width_change_crash_recovers() {
    for (source_durable, target_durable) in [(false, true), (true, false)] {
        let (path, pre_max) = cw_seed_and_crash(MID, source_durable);
        let window = CwWindow {
            width: NARROW,
            durable: target_durable,
            name: Some("mace_switch_after_context_install_before_options_writeback=abort@1"),
        };
        let status = spawn_cw_migrate_child(&path, &window);
        assert_aborted_at_failpoint(
            status,
            "a width+route change must abort in the handoff window",
        );
        assert_eq!(
            assert_cw_target_retry(&path, NARROW, NARROW, pre_max, target_durable),
            NARROW,
            "the completed migration must record the target width (source durable={source_durable})"
        );
    }
}

/// the durable shared stream's wipe windows: a durable source keeps its whole namespace in
/// `group_wal_*`, so crashing the shared stream's intent window exercises the shared removal
/// branch that a relaxed source never reaches
#[test]
fn concurrent_write_durable_source_mid_wipe_crash_recovers() {
    let rule = "mace_wal_recycle_after_dir_sync_before_done_commit=abort@1";
    let (path, pre_max) = cw_seed_and_crash(MID, true);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(NARROW, rule));
    assert_aborted_at_failpoint(
        status,
        "a durable source must abort in the shared wipe window",
    );
    assert_eq!(
        assert_cw_target_retry(&path, NARROW, NARROW, pre_max, false),
        NARROW,
        "the completed migration must record the target width"
    );
    assert!(
        group_wal_file_ids(&Options::new(&*path).log_root()).is_empty(),
        "a relaxed target must not keep the retired shared stream"
    );

    // the same window retried at the durable source width: the shared stream's pending intent
    // must not rebuild the era, and the committed tail must survive the source-width open
    let (path, _) = cw_seed_and_crash(MID, true);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(NARROW, rule));
    assert_aborted_at_failpoint(
        status,
        "a durable source must abort in the shared wipe window",
    );
    assert_cw_source_retry(&path, MID, true);
}

/// the shrink direction of the handoff window, retried at the persisted source count before the
/// target completion: the crash-produced half-migrated state must survive a source-width open
/// (same era files, same boundaries, retired streams appending in place, every source group
/// live again) and still complete afterwards
#[test]
fn concurrent_write_shrink_handoff_crash_retries_at_the_source_count() {
    let rule = "mace_switch_after_context_install_before_options_writeback=abort@1";
    let (path, pre_max) = cw_seed_and_crash(WIDE, false);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(NARROW, rule));
    assert_aborted_at_failpoint(status, "shrink must abort after the context install");

    // what the crash left on disk: the shrink's era is already built while the persisted width
    // still reads the source value
    let log_root = Options::new(&*path).log_root();
    let crashed = all_wal_ids(&log_root, WIDE);
    assert!(
        crashed.iter().flatten().next().is_some(),
        "the crashed attempt must leave era files behind"
    );

    assert_cw_source_retry(&path, WIDE, false);

    let mace = open_relaxed_at(&path, WIDE);
    let bucket = mace.open_bucket("prod").expect("bucket prod must exist");
    assert_one_per_key(&bucket, CW_SEED_KEYS);
    let era_start = testing::logger_oldest_wal_id(&bucket, 0);
    // the wipe steps stay in effect across the source-width retry: the crash left every swept
    // slot done at the era start, and a plain reopen must not reset or re-dirty one
    for physical in (0..WIDE).chain(std::iter::once(Options::SHARED_ID)) {
        let (from, to, done) = testing::wal_recycle_state(&bucket, physical);
        assert!(
            done && to == era_start,
            "stream {physical} lost its verified wipe boundary on the source-width retry: ({from}, {to}, done={done})"
        );
    }
    let tx = bucket.begin().expect("begin source retry txn failed");
    tx.put("after_retry", b"appended")
        .expect("put after source retry failed");
    tx.commit().expect("commit after source retry failed");
    // every source stream must reuse the era file the crash left it: a plain reopen may
    // materialize a missing anchor at the inherited era start, but it must never open a new
    // era, drop an id the crash left, or fall back to a pre-era file id
    let reopened = all_wal_ids(&log_root, WIDE);
    for (physical, before) in crashed.iter().enumerate() {
        let ids = &reopened[physical];
        if physical == WIDE as usize {
            // the shared stream belongs to the durable route, which this fixture never takes
            assert_eq!(
                ids, before,
                "a relaxed source retry must not touch the shared stream: {ids:?}"
            );
            continue;
        }
        assert_eq!(
            ids.first().copied(),
            Some(era_start),
            "source stream {physical} must open its era at the inherited start: {ids:?}"
        );
        assert!(
            before.iter().all(|id| ids.contains(id)),
            "source stream {physical} lost a file the crash left: {ids:?} against {before:?}"
        );
    }
    assert_eq!(
        sequential_groups(&bucket, WIDE as u32),
        (0..WIDE as usize).collect::<BTreeSet<_>>(),
        "a source-width retry must make every source group live again"
    );
    drop(bucket);
    drop(mace);

    // the appended record is durable through the reused era files
    let mace = open_relaxed_at(&path, WIDE);
    let bucket = mace.open_bucket("prod").expect("bucket prod must exist");
    assert_eq!(
        bucket
            .view()
            .expect("open verify view failed")
            .get("after_retry")
            .expect("the appended record must survive the reopen")
            .slice(),
        b"appended"
    );
    drop(bucket);
    drop(mace);

    assert_eq!(
        assert_cw_target_retry(&path, NARROW, NARROW, pre_max, false),
        NARROW,
        "the target open must still complete the migration after a source-width retry"
    );
}

/// a retry narrower than a crashed migration must still finish durable intents from the wider
/// attempt, even when the old intent's stream is outside the retry's target width
#[test]
fn concurrent_write_narrower_retry_finishes_wider_pending_intent() {
    let (path, pre_max) = cw_seed_and_crash(NARROW, false);
    let status = spawn_cw_migrate_child(
        &path,
        &CwWindow {
            width: WIDE,
            durable: false,
            name: Some("mace_wal_recycle_after_dir_sync_before_done_commit=abort@9"),
        },
    );
    assert_aborted_at_failpoint(status, "wider migration must abort on target-only stream 7");

    assert_eq!(
        assert_cw_target_retry(&path, MID, MID, pre_max, false),
        MID,
        "the narrower retry must complete every durable recycle intent"
    );
}

/// the same narrower-than-a-crashed-migration retry on the durable route, whose wider attempt also
/// stages intents for target-only streams
#[test]
fn concurrent_write_narrower_retry_finishes_wider_pending_intent_on_the_durable_route() {
    let (path, pre_max) = cw_seed_and_crash(NARROW, true);
    let status = spawn_cw_migrate_child(
        &path,
        &CwWindow {
            width: WIDE,
            durable: true,
            name: Some("mace_wal_recycle_after_dir_sync_before_done_commit=abort@9"),
        },
    );
    assert_aborted_at_failpoint(
        status,
        "wider durable migration must abort on target-only stream 7",
    );

    assert_eq!(
        assert_cw_target_retry(&path, MID, MID, pre_max, true),
        MID,
        "the narrower durable retry must complete every durable recycle intent"
    );
}

/// the nonuniform fixture's payload: one payload alone cannot cross the fixture's wal file
/// size, two group-0 rounds can, and the group-1 record is small enough that its stream
/// never leaves file 0
const NU_PAYLOAD: usize = 3 << 10;
const NU_SMALL: usize = 16;
/// rounds the fixture commits, one key each, so the key set is known without a side channel
const NU_ROUNDS: usize = 8;

/// the nonuniform fixture's options: a relaxed two-stream source whose small wal file size
/// lets one group rotate past file 0 while the other stays in file 0
fn open_nonuniform(db_root: &Path) -> Mace {
    let mut opt = Options::new(db_root);
    opt.sync_on_write = false;
    opt.concurrent_write = NARROW;
    opt.data_file_size = 16 << 10;
    opt.wal_buffer_size = 16 << 10;
    opt.wal_file_size = 4 << 10;
    opt.gc_timeout = 60_000;
    opt.checkpoint_nudge_ms = 0;
    opt.gc_eager = false;
    Mace::new(opt.validate().expect("options must validate")).expect("open must succeed")
}

/// a relaxed source whose physical streams stop at different file ids: one stream rotates
/// past file 0 while the other never leaves it. the same fixture leaves a durable abandoned
/// tail behind, so a migration over it must roll the tail back without touching the
/// retained prefix of either stream
fn child_cw_nonuniform_seed(db_root: &Path) -> ! {
    let mace = open_nonuniform(db_root);
    let bucket = ensure_bucket(&mace, "prod");
    let log_root = Options::new(db_root).log_root();
    let payload = vec![b'p'; NU_PAYLOAD];
    let small = vec![b's'; NU_SMALL];

    // the group a round selects is engine-chosen (the first one already depends on the
    // bucket's own setup), so the fixture drives the observed group: the large payloads are
    // what cross a file boundary, the small ones keep their stream inside file 0, and the
    // key set stays `r_0..r_{NU_ROUNDS}` either way
    let mut group0 = 0usize;
    let mut group1 = 0usize;
    for round in 0..NU_ROUNDS {
        let txn = bucket.begin().expect("begin seed txn failed");
        let txid = testing::txn_start_ts(&txn);
        match testing::txn_group(&bucket, txid) {
            Some(0) => {
                txn.upsert(format!("r_{round}"), payload.as_slice())
                    .expect("put large round key failed");
                group0 += 1;
            }
            Some(1) => {
                txn.upsert(format!("r_{round}"), small.as_slice())
                    .expect("put small round key failed");
                group1 += 1;
            }
            other => panic!("unexpected writer group {other:?}"),
        }
        txn.commit().expect("commit seed txn failed");
    }

    // the fixture is only meaningful with both groups present and a nonuniform distribution
    assert!(
        group0 >= 2 && group1 >= 1,
        "the rounds must cover both writer groups: group0={group0} group1={group1}"
    );
    let stream0 = wal_file_ids(&log_root, 0);
    let stream1 = wal_file_ids(&log_root, 1);
    assert!(
        stream0.last().copied().unwrap_or(0) > 0,
        "one stream must rotate past file 0: {stream0:?}"
    );
    assert_eq!(
        stream1,
        vec![0],
        "the other stream must stay inside file 0: {stream1:?}"
    );

    // abandoning a modified txn leaves a durable tail: the engine records the abort and
    // flushes the group ring, so recovery has an uncommitted record to roll back
    let tail = bucket.begin().expect("begin tail txn failed");
    let tail_txid = testing::txn_start_ts(&tail);
    tail.put("tail", b"lost").expect("put tail key failed");
    drop(tail);
    assert!(
        testing::abort_clean_task_info(&bucket, tail_txid).is_some(),
        "the abandoned tail must be durable enough to publish an abort-clean task"
    );

    // crash on the trailing commit: the source state is what a migration attempt starts from
    testing::arm_failpoint_rule("mace_txn_commit_begin=abort@1");
    let crash = bucket.begin().expect("begin crash txn failed");
    crash.put("crash", b"lost").expect("put crash key failed");
    let _ = crash.commit();
    wait_for_crash(Duration::from_secs(2))
}

/// the nonuniform fixture's seed child: it arms its own trailing crash in-process, so the
/// caller passes no global failpoint spec
fn spawn_cw_nonuniform_seed(db_root: &Path) -> ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    child_test_command(&exe)
        .arg("--exact")
        .arg("gen_failpoint_child")
        .arg("--nocapture")
        .env(ENV_CHILD, "1")
        .env(ENV_CASE, "cw_nonuniform_seed")
        .env(ENV_DB_ROOT, db_root.as_os_str())
        .status()
        .expect("spawn nonuniform seed child failed")
}

/// the source-width view of the nonuniform fixture: stream 0 past file 0, stream 1 in file 0
fn seed_nonuniform(path: &Path) -> u64 {
    let status = spawn_cw_nonuniform_seed(path);
    assert_aborted_at_failpoint(
        status,
        "the nonuniform seed must abort on its trailing commit",
    );

    let log_root = Options::new(path).log_root();
    let stream0 = wal_file_ids(&log_root, 0);
    let stream1 = wal_file_ids(&log_root, 1);
    assert!(
        stream0.last().copied().unwrap_or(0) > 0,
        "the fixture needs group 0 rotated past file 0: {stream0:?}"
    );
    assert_eq!(
        stream1,
        vec![0],
        "the fixture needs group 1 still inside file 0: {stream1:?}"
    );
    max_wal_file_id(&log_root, NARROW)
}

/// every key the nonuniform fixture committed is visible and neither tail key is
fn assert_nonuniform_keys(bucket: &Bucket) {
    let view = bucket.view().expect("open verify view failed");
    for round in 0..NU_ROUNDS {
        let key = format!("r_{round}");
        let value = view.get(&key).expect("the rotated round key must survive");
        assert!(
            matches!(value.slice().len(), NU_PAYLOAD | NU_SMALL),
            "round key {key} must keep its record length, got {}",
            value.slice().len()
        );
    }
    for key in ["tail", "crash"] {
        assert!(
            view.get(key).is_err(),
            "the uncommitted {key} txn must be rolled back by recovery"
        );
    }
}

/// a crash inside a pre-wipe migration window over a nonuniform source: the retry at the
/// source width reads every committed key from the retained streams without a gap, and the
/// migration that follows completes on the wider target
fn assert_nonuniform_window(rule: &str) {
    let path = RandomPath::tmp();
    let pre_max = seed_nonuniform(&path);
    let status = spawn_cw_migrate_child(&path, &cw_named_window(WIDE, rule));
    assert_aborted_at_failpoint(status, "the grow migration must abort inside its window");

    // the source retry is a plain reopen: both streams keep their own retained range
    let mace = open_relaxed_at(&path, NARROW);
    let bucket = mace.open_bucket("prod").expect("bucket prod must exist");
    assert_nonuniform_keys(&bucket);
    drop(bucket);
    drop(mace);
    let log_root = Options::new(&*path).log_root();
    assert_streams_gap_free(&log_root, NARROW);

    // the completed migration retires both source streams and starts the wide era above the
    // source high water
    assert_nonuniform_target(&path, pre_max);
}

/// the completed nonuniform migration: every committed key visible at the target width, no
/// gap anywhere, every target stream above the source high water, and every swept stream
/// holding a completed wipe at the new era start
fn assert_nonuniform_target(path: &Path, pre_max: u64) {
    let mace = open_relaxed_at(path, WIDE);
    let bucket = mace.open_bucket("prod").expect("bucket prod must exist");
    assert_nonuniform_keys(&bucket);
    let log_root = Options::new(path).log_root();
    assert_streams_gap_free(&log_root, WIDE);
    for physical in 0..WIDE {
        let ids = wal_file_ids(&log_root, physical);
        assert!(
            !ids.is_empty(),
            "target stream {physical} owns no era file after the migration"
        );
        for id in &ids {
            assert!(
                *id > pre_max,
                "target stream {physical} reuses a source file id {id} (high-water {pre_max})"
            );
        }
    }
    let era_start = testing::logger_oldest_wal_id(&bucket, 0);
    for physical in (0..WIDE).chain(std::iter::once(Options::SHARED_ID)) {
        let (from, to, done) = testing::wal_recycle_state(&bucket, physical);
        assert!(
            done && to == era_start,
            "stream {physical} must hold a completed wipe at the era start: ({from}, {to}, done={done})"
        );
    }
    drop(bucket);
    drop(mace);
    assert_eq!(
        persisted_width(&Options::new(path)),
        WIDE,
        "the completed grow must record the target width"
    );
}

/// the retained gap a rebuild could leave behind lives between a stream's own retained range
/// and the global epoch start, so the nonuniform source is the fixture that catches it: a
/// crash in either pre-wipe window must leave a database the source width can still reopen
#[test]
fn nonuniform_source_streams_survive_the_pre_wipe_windows() {
    assert_nonuniform_window("mace_switch_before_forced_checkpoint=abort@1");
    assert_nonuniform_window("mace_switch_after_forced_checkpoint_before_wipe=abort@1");
}

/// the route switch's last window: the epoch writers are installed and the log directory is
/// synced, but the persisted route still names the source. the source-route open must be a
/// plain reopen over the installed era, and the requested route must complete the migration
#[test]
fn route_switch_crash_after_the_epoch_writer_install_recovers() {
    let rule = "mace_switch_after_epoch_writer_install_before_writeback=abort@1";
    let (path, pre_max) = cw_seed_and_crash(NARROW, false);
    let status = spawn_cw_migrate_child(
        &path,
        &CwWindow {
            width: NARROW,
            durable: true,
            name: Some(rule),
        },
    );
    assert_aborted_at_failpoint(
        status,
        "the route switch must abort after the epoch writer install",
    );
    assert!(
        !persisted_route(&Options::new(&*path)),
        "the crashed switch must not have recorded the target route"
    );

    // the source-route reopen reads the installed era, and the requested route then completes
    // the switch
    assert_cw_source_retry(&path, NARROW, false);
    assert_eq!(
        assert_cw_target_retry(&path, NARROW, NARROW, pre_max, true),
        NARROW,
        "the completed route switch must record the same width"
    );
    assert!(
        persisted_route(&Options::new(&*path)),
        "the completed route switch must record the durable route"
    );
}
