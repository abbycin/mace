#![cfg(all(feature = "failpoints", feature = "extra_check"))]

//! phase c crash windows for the shared durable wal: generation seal/sync,
//! abort fact publication, rotation, torn tail truncation, route-switch
//! recovery, route-switch epoch and switch-wipe crash windows

mod common;

use common::child_test_command;
use mace::observe::{CounterMetric, InMemoryObserver};
use mace::testing;
use mace::{Bucket, BucketOptions, Mace, OpCode, Options, RandomPath};
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

fn open_durable(db_root: &Path, observer: Option<std::sync::Arc<InMemoryObserver>>) -> Mace {
    let mut opt = Options::new(db_root);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
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

fn open_relaxed(db_root: &Path) -> Mace {
    let mut opt = Options::new(db_root);
    opt.sync_on_write = false;
    opt.concurrent_write = 2;
    opt.data_file_size = 16 << 10;
    opt.wal_buffer_size = 32 << 10;
    opt.wal_file_size = 8 << 10;
    opt.gc_timeout = 60_000;
    opt.checkpoint_nudge_ms = 0;
    opt.gc_eager = false;
    Mace::new(opt.validate().expect("options must validate")).expect("open must succeed")
}

fn bucket(mace: &Mace) -> Bucket {
    match mace.get_bucket("prod") {
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
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
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
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    for idx in 0..count {
        let key = format!("k_{idx}");
        let val = view.get(&key).expect("committed key missing");
        assert_eq!(val.slice(), format!("v_{idx}").as_bytes());
    }
}

fn assert_mixed_visible(db_root: &Path, count: usize) {
    let mace = open_durable(db_root, None);
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
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
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
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
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
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
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
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
        .get_bucket("prod")
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
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
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
        .get_bucket("prod")
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
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
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
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
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
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
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
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
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
// phase-1 review fixes: switch-wipe durability and the crash-window matrix
// ---------------------------------------------------------------------------

const ENV_SWITCH_FAILPOINT: &str = "MACE_SWITCH_FAILPOINT";

fn ensure_bucket(mace: &Mace, name: &str) -> Bucket {
    match mace.get_bucket(name) {
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
        let bucket = mace.get_bucket(&name).expect("bucket must exist");
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
    let opt = Options::new(path);
    let log_root = opt.log_root();
    for physical in 0..2 {
        let ids = wal_file_ids(&log_root, physical);
        for pair in ids.windows(2) {
            assert_eq!(
                pair[1],
                pair[0] + 1,
                "physical stream {physical} must have no wal gap: {ids:?}"
            );
        }
    }
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
    let bucket = mace.get_bucket("b0").expect("bucket must exist");
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
    let bucket = mace.get_bucket("prod").expect("bucket prod must exist");
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
    let bucket = mace.get_bucket("b23").expect("bucket b23 must exist");
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
