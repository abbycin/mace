#![cfg(feature = "extra_check")]

mod common;

use common::child_test_command;
use mace::testing::{self, AbortCleanStage, AbortCleanSyncPoint, WalRecordKind};
use mace::{BucketOptions, Mace, OpCode, Options, RandomPath};
use std::path::{Path, PathBuf};
use std::time::Duration;

const ENV_CRASH_CHILD: &str = "MACE_GC_CRASH_CHILD";
const ENV_CRASH_DB: &str = "MACE_GC_CRASH_DB";
const ENV_EMPTY_CRASH_CHILD: &str = "MACE_EMPTY_CRASH_CHILD";
const ENV_EMPTY_CRASH_DB: &str = "MACE_EMPTY_CRASH_DB";

#[cfg(unix)]
fn assert_child_aborted(status: std::process::ExitStatus, msg: &str) {
    use std::os::unix::process::ExitStatusExt;

    assert_eq!(status.signal(), Some(6), "{msg}");
}

#[cfg(not(unix))]
fn assert_child_aborted(status: std::process::ExitStatus, msg: &str) {
    assert!(!status.success(), "{msg}");
}

fn spawn_empty_crash_child(db_root: &Path) -> std::process::ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    child_test_command(&exe)
        .arg("--exact")
        .arg("crash_child_empty_bucket")
        .arg("--nocapture")
        .env(ENV_EMPTY_CRASH_CHILD, "1")
        .env(ENV_EMPTY_CRASH_DB, db_root.as_os_str())
        .status()
        .expect("spawn empty-bucket crash child failed")
}

fn group_wal_file_ids(log_root: &Path) -> Vec<u64> {
    let mut ids = Vec::new();
    if let Ok(entries) = std::fs::read_dir(log_root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(rest) = name.strip_prefix("group_wal_")
                && let Ok(file_id) = rest.parse::<u64>()
            {
                ids.push(file_id);
            }
        }
    }
    ids.sort_unstable();
    ids
}

fn spawn_crash_child(db_root: &Path) -> std::process::ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    child_test_command(&exe)
        .arg("--exact")
        .arg("crash_child")
        .arg("--nocapture")
        .env(ENV_CRASH_CHILD, "1")
        .env(ENV_CRASH_DB, db_root.as_os_str())
        .status()
        .expect("spawn gc crash child failed")
}

#[test]
fn crash_child() {
    if std::env::var(ENV_CRASH_CHILD).ok().as_deref() != Some("1") {
        return;
    }
    let db_root = PathBuf::from(std::env::var(ENV_CRASH_DB).expect("missing crash db root"));
    let mut opt = Options::new(&db_root);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    opt.wal_file_size = 4096;
    opt.gc_timeout = 60_000;
    opt.checkpoint_nudge_ms = 0;
    opt.data_file_size = 1 << 30;
    let mace = Mace::new(opt.validate().expect("crash child options must validate"))
        .expect("crash child open must succeed");
    let db = mace
        .new_bucket(
            "x",
            BucketOptions {
                checkpoint_size: 1 << 30,
                pool_capacity: 1 << 30,
                ..BucketOptions::default()
            },
        )
        .expect("crash child bucket must succeed");

    for i in 0..201 {
        let tx = db.begin().expect("crash child begin");
        tx.put(format!("k_{i}"), format!("v_{i}"))
            .expect("crash child put");
        tx.commit().expect("crash child commit");
    }
    let tx = db.begin().expect("crash child tail begin");
    tx.put("tail", b"v").expect("crash child tail put");
    tx.commit().expect("crash child tail commit");
    std::process::abort();
}

#[test]
fn durable_shared_gc_respects_group1_active_pin() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    opt.wal_file_size = 4096;
    opt.gc_timeout = 60_000;
    opt.data_file_size = 100 << 10;
    let log_root = opt.log_root();
    let saved = opt.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    // 501 sequential commits consume tickets 0..=500, so ticket 501 lands in group 1
    for i in 0..501 {
        let tx = db.begin()?;
        tx.put(format!("k_{i}"), format!("v_{i}"))?;
        tx.commit()?;
    }

    let pin = db.begin()?;
    let pin_txid = testing::txn_start_ts(&pin);
    assert_eq!(testing::txn_group(&db, pin_txid), Some(1));
    pin.put("pin", b"v")?;

    db.checkpoint();

    let probes = testing::wal_record_probes(&log_root, 2);
    let begin = probes
        .iter()
        .find(|p| p.kind == WalRecordKind::Begin && p.txid == pin_txid)
        .expect("group 1 pin begin must be flushed before gc");
    let pin_file = saved.group_wal_file(begin.file_id);
    let first_file_id = probes
        .iter()
        .map(|p| p.file_id)
        .min()
        .expect("probes must exist");
    let first_file = saved.group_wal_file(first_file_id);
    // GC can only recycle before the group 1 pin after group 0's checkpoint
    // publication has crossed the earliest WAL file.  Wait for that async
    // publication explicitly instead of racing it with the GC assertion.
    db.checkpoint_and_wait();
    assert!(
        testing::shared_checkpoint_floor(&db, 0).0 > first_file_id,
        "scenario precondition: group 0 checkpoint must pass the earliest WAL file before gc"
    );
    let recycled_earlier = common::gc_rounds_until(&mace, 8, || !first_file.exists());
    assert!(
        recycled_earlier,
        "the shared group_wal stream gc should recycle files before the group 1 active pin"
    );
    assert!(
        pin_file.exists(),
        "the shared group_wal stream must not recycle the WAL file pinned by the active group 1 txn"
    );

    drop(pin);
    Ok(())
}

#[test]
fn route_switch_checkpoints_tail_then_wipes_old_wal() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let status = spawn_crash_child(&path);
    assert_child_aborted(
        status,
        "crash child must abort before checkpointing the group 1 tail",
    );

    let child_opt = Options::new(&*path);
    let log_root = child_opt.log_root();
    let saved = child_opt.clone();
    let probes = testing::wal_record_probes(&log_root, 2);
    let tail = probes
        .iter()
        .filter(|p| p.kind == WalRecordKind::Commit)
        .max_by_key(|p| p.txid)
        .expect("crash child must leave a committed group 1 tail");
    let tail_file = saved.group_wal_file(tail.file_id);

    let mut opt = Options::new(&*path);
    opt.sync_on_write = false;
    opt.concurrent_write = 2;
    opt.gc_timeout = 60_000;
    opt.checkpoint_nudge_ms = 0;
    opt.data_file_size = 1 << 30;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.open_bucket("x")?;

    // the switch open recovered the uncheckpointed tail, force-checkpointed it
    // (full fsync) and then wiped the old wal: the tail's data must be
    // readable from the now-durable data files and the old wal file must be
    // gone -- the previous invariant "the shared stream must retain the tail
    // file" is superseded by "make the tail durable, then delete the wal"
    let view = db.view()?;
    for i in 0..201 {
        let key = format!("k_{i}");
        let val = view.get(&key).expect("recovered key missing");
        assert_eq!(val.slice(), format!("v_{i}").as_bytes());
    }
    drop(view);
    assert!(
        !tail_file.exists(),
        "the switch wipe must delete the old wal after the tail is durable"
    );

    // relaxed runtime continues normally: group 0 gets a fresh checkpoint and
    // GC rounds stay healthy on the new era's contiguous files
    let tx = db.begin()?;
    assert_eq!(testing::txn_group(&db, testing::txn_start_ts(&tx)), Some(0));
    tx.put("g0", b"v")?;
    tx.commit()?;
    db.checkpoint();
    for _ in 0..8 {
        common::gc_round(&mace, Duration::from_secs(10));
    }
    Ok(())
}

#[test]
fn shared_stream_abort_clean_walks_interleaved_chain_without_range_escape() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    opt.gc_timeout = 60_000;
    opt.checkpoint_nudge_ms = 0;
    opt.data_file_size = 1 << 30;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    // two logical groups interleave their updates inside the shared group_wal stream
    let tx_a = db.begin()?;
    let txid_a = testing::txn_start_ts(&tx_a);
    assert_eq!(testing::txn_group(&db, txid_a), Some(0));
    tx_a.put("a1", b"1")?;
    let tx_b = db.begin()?;
    let txid_b = testing::txn_start_ts(&tx_b);
    assert_eq!(testing::txn_group(&db, txid_b), Some(1));
    tx_b.put("b1", b"1")?;
    tx_a.put("a2", b"2")?;
    tx_b.put("b2", b"2")?;
    tx_a.put("a3", b"3")?;
    tx_b.put("b3", b"3")?;

    // group 1 commits; group 0 aborts with its chain interleaved by b records
    tx_b.commit()?;
    let txid_a_abort = testing::txn_start_ts(&tx_a);
    drop(tx_a);

    // gc must walk a's prev chain strictly backward through the interleaved
    // shared stream and retire the task. phase 1 drives the synchronous walk:
    // each round fully processes pending tasks, so a small round budget is
    // structural, not a scheduling bet. the final retirement step is
    // different -- it rides an EBR defer whose callback is ordered after the
    // process-wide epoch grace period, and concurrent participants pin that
    // epoch across long operations (a checkpoint publish holds a guard over
    // its wait-zero window), so its completion is genuinely asynchronous
    // phase 2 therefore waits on the engine's AfterQuiesceCallback signal
    // instead of guessing rounds, then runs the one protocol-fixed drain
    // round before asserting
    let (quiesce_tx, quiesce_rx) = std::sync::mpsc::channel();
    // hold hooks_lock across registration and removal per the testing
    // contract; the span between them cannot hold it because gc_round takes
    // the same lock internally, and this binary has no other hook user to
    // race against
    {
        let _hooks = mace::testing::hooks_lock();
        testing::set_abort_clean_hook(Some(std::sync::Arc::new(move |point, txid| {
            if txid == txid_a_abort && point == AbortCleanSyncPoint::AfterQuiesceCallback {
                let _ = quiesce_tx.send(());
            }
        })));
    }
    let gone = || !testing::fact_present(&db, 0, txid_a_abort);
    let mut retired = gone();
    let mut rewritten = false;
    for _ in 0..4 {
        if retired {
            break;
        }
        common::gc_round(&mace, Duration::from_secs(30));
        retired = gone();
        rewritten = matches!(
            testing::abort_clean_task_stage(&db, txid_a_abort),
            Some(AbortCleanStage::WaitingQuiesce)
        );
    }
    let mut quiesce_signal = retired;
    if !retired && rewritten {
        // rewrite half is durable; only the epoch grace period remains
        // before the retire event lands
        quiesce_signal = quiesce_rx.recv_timeout(Duration::from_secs(30)).is_ok();
        retired = common::gc_rounds_until(&mace, 3, gone);
    }
    {
        let _hooks = mace::testing::hooks_lock();
        testing::clear_abort_clean_hook();
    }
    assert!(
        retired,
        "interleaved abort-clean chain must be fully walked and retired; \
         stage={:?} quiesce_signal={quiesce_signal}",
        testing::abort_clean_task_stage(&db, txid_a_abort)
    );
    let view = db.view()?;
    for key in ["a1", "a2", "a3"] {
        assert!(
            matches!(view.get(key), Err(OpCode::NotFound)),
            "aborted key {key} must stay invisible after chain cleanup"
        );
    }
    for (key, expected) in [("b1", b"1"), ("b2", b"2"), ("b3", b"3")] {
        let got = view.get(key).expect("committed interleaved key missing");
        assert_eq!(got.slice(), expected);
    }
    Ok(())
}

#[test]
fn unused_logical_groups_do_not_pin_the_shared_wal_stream() -> Result<(), OpCode> {
    // a logical group that never appended an update must not drag the shared
    // stream's recycle floor to file 0: only groups with records participate
    // in the min, so the stream can recycle down to the checkpoint boundary
    // of the groups that actually wrote
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 4;
    opt.wal_file_size = 4096;
    opt.gc_timeout = 60_000;
    opt.checkpoint_nudge_ms = 0;
    opt.data_file_size = 1 << 30;
    let log_root = opt.log_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    // a single large transaction lands on group 0 (round-robin starts at 0)
    // and spans many wal files; groups 1..3 never write anything
    let tx = db.begin()?;
    assert_eq!(testing::txn_group(&db, testing::txn_start_ts(&tx)), Some(0));
    for i in 0..3000 {
        tx.put(format!("k_{i}"), format!("v_{i}"))?;
    }
    tx.commit()?;

    // snapshot the large txn's wal footprint BEFORE the checkpoint: checkpoint
    // completion triggers the async wal_clean recycle, and under some schedules
    // (heavy stderr contention, parallel test load) the recycle deletes the
    // checkpointed files before this read, leaving only the current file
    let initial = group_wal_file_ids(&log_root);
    assert!(
        initial.len() > 1,
        "the large group 0 txn must span multiple wal files: {:?}",
        initial
    );

    db.checkpoint_and_wait();

    let recycled = common::gc_rounds_until(&mace, 8, || {
        group_wal_file_ids(&log_root).len() < initial.len()
    });
    assert!(
        recycled,
        "unused logical groups must not pin the shared stream: {} files stayed ({:?})",
        group_wal_file_ids(&log_root).len(),
        group_wal_file_ids(&log_root)
    );
    Ok(())
}

#[test]
fn all_inactive_groups_recycle_unmodified_transaction_wal() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 4;
    opt.wal_file_size = 4096;
    opt.gc_timeout = 60_000;
    opt.checkpoint_nudge_ms = 0;
    let log_root = opt.log_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    for _ in 0..3000 {
        db.begin()?.commit()?;
    }
    mace.sync()?;
    let initial = group_wal_file_ids(&log_root);
    assert!(
        initial.len() > 1,
        "unmodified transactions must rotate the shared wal: {initial:?}"
    );

    common::gc_rounds_until(&mace, 8, || {
        group_wal_file_ids(&log_root).len() < initial.len()
    });
    let retained = group_wal_file_ids(&log_root);
    assert!(
        retained.len() < initial.len(),
        "all-inactive floors must recycle complete files before the append file: {retained:?}"
    );
    assert!(
        !retained.is_empty(),
        "gc must retain the current append file as the recovery anchor"
    );
    Ok(())
}

#[test]
fn crash_child_empty_bucket() {
    if std::env::var(ENV_EMPTY_CRASH_CHILD).ok().as_deref() != Some("1") {
        return;
    }
    let db_root = PathBuf::from(std::env::var(ENV_EMPTY_CRASH_DB).expect("missing crash db root"));
    let mut opt = Options::new(&db_root);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    opt.wal_file_size = 4096;
    opt.gc_timeout = 60_000;
    opt.checkpoint_nudge_ms = 0;
    opt.data_file_size = 1 << 30;
    let mace = Mace::new(opt.validate().expect("crash child options must validate"))
        .expect("crash child open must succeed");
    let db = mace
        .new_bucket("main", BucketOptions::default())
        .expect("main bucket must succeed");

    let tx = db.begin().expect("crash child begin");
    for i in 0..3000 {
        tx.put(format!("k_{i}"), format!("v_{i}"))
            .expect("crash child put");
    }
    tx.commit().expect("crash child commit");
    db.checkpoint();
    // the checkpoint publish is asynchronous; wait until group 0's floor
    // advanced past the era start so the empty bucket is created with a
    // finite creation-time floor
    db.checkpoint_and_wait();
    assert!(
        testing::shared_checkpoint_floor(&db, 0).0 > 1,
        "scenario precondition: group 0 floor must advance past the era start"
    );
    // create a bucket that is never written and never flushed: its durable
    // frontier stays at the create-time value in the manifest
    let _empty = mace
        .new_bucket("empty", BucketOptions::default())
        .expect("empty bucket must succeed");
    std::process::abort();
}

#[test]
fn empty_bucket_does_not_pin_shared_stream_at_reopen() -> Result<(), OpCode> {
    // a bucket created but never written (crash before any flush) must not
    // drag the shared-stream floor back to file 0 on reopen: its durable
    // frontier is seeded from the current checkpoint floor (inactive slots
    // clamped to MIN), so the manifest floor of the active group stays at its
    // real checkpoint boundary instead of stalling GC until the first
    // post-reopen flush
    let path = RandomPath::tmp();
    let status = spawn_empty_crash_child(&path);
    assert_child_aborted(
        status,
        "empty-bucket crash child must abort before flushing the empty bucket",
    );

    let mut reopen = Options::new(&*path);
    reopen.sync_on_write = true;
    reopen.concurrent_write = 2;
    reopen.wal_file_size = 4096;
    reopen.gc_timeout = 60_000;
    reopen.checkpoint_nudge_ms = 0;
    reopen.data_file_size = 1 << 30;
    let mace = Mace::new(reopen.validate()?)?;
    let db = mace.open_bucket("main")?;
    let floor_at_open = testing::shared_checkpoint_floor(&db, 0);
    assert!(
        floor_at_open.0 > 0,
        "an empty bucket must not drag group 0's floor back to 0 on reopen: {:?}",
        floor_at_open
    );
    Ok(())
}
