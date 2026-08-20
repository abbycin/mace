use mace::observe::{CounterMetric, InMemoryObserver, ObserveSnapshot};
#[cfg(feature = "extra_check")]
use mace::testing::{self, CheckpointRootRestore, CheckpointSyncPoint};
use mace::{Bucket, BucketOptions, Mace, OpCode, Options, RandomPath};
use std::sync::Arc;
#[cfg(feature = "extra_check")]
use std::sync::Barrier;
#[cfg(feature = "extra_check")]
use std::sync::mpsc::channel;
#[cfg(feature = "extra_check")]
use std::time::Duration;

// Regression guard for the reachable-junk lifecycle changes bug class:
// reachable junk pages (via sibling/remote edges) were retired too early, so read path
// could hit "not in dirty pages" + "not in interval" hole before durability closure.
//
// Why this test exists:
// - future graph/lifecycle changes can add new reachable edges without updating junk-retire rules.
// - this test keeps a lagging view alive while forcing split/compact/checkpoint churn to ensure
//   reachable pages are never reclaimed prematurely.
//
// What a failure means:
// - if lag-view reads fail (NotFound / panic), dirty lifecycle closure is broken again
//   (reachable page got reclaimed before becoming durable).
// - if split/consolidate counters stay zero, the scenario is not exercising the risky path.
fn counter(snapshot: &ObserveSnapshot, metric: CounterMetric) -> u64 {
    snapshot
        .counters
        .iter()
        .find(|(m, _)| *m == metric)
        .map(|(_, v)| *v)
        .unwrap_or_default()
}

fn parity_payload(round: usize, value_size: usize) -> Vec<u8> {
    let mut p = vec![b'x'; value_size];
    p[0] = (round as u8).wrapping_add(1);
    p
}

fn upsert_parity_retry(
    bucket: &Bucket,
    keys: &[String],
    parity: usize,
    payload: &[u8],
) -> Result<(), OpCode> {
    const RETRY_LIMIT: usize = 2048;
    for _ in 0..RETRY_LIMIT {
        let tx = bucket.begin().unwrap();
        let mut retry = false;
        for (idx, key) in keys.iter().enumerate() {
            if idx % 2 != parity {
                continue;
            }
            match tx.upsert(key, payload) {
                Ok(_) => {}
                Err(OpCode::Again | OpCode::AbortTx) => {
                    retry = true;
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        if retry {
            std::thread::yield_now();
            continue;
        }
        match tx.commit() {
            Ok(()) => return Ok(()),
            Err(OpCode::Again | OpCode::AbortTx) => std::thread::yield_now(),
            Err(e) => return Err(e),
        }
    }
    panic!("upsert parity retry exhausted");
}

#[test]
fn reachable_junk_regression_guard() -> Result<(), OpCode> {
    #[cfg(feature = "extra_check")]
    let _checkpoint_test_lock = testing::checkpoint_test_lock();
    // Purpose:
    // 1) force sibling/remote-producing churn (split + consolidate),
    // 2) keep lagging readers alive across rounds and checkpoints,
    // 3) assert old view never loses reachability before persistence closure.
    const KEYS: usize = 128;
    const VALUE_SIZE: usize = 4096;
    const ROUNDS: usize = 48;

    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(256));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.concurrent_write = 1;
    opt.data_file_size = 64 << 10;
    opt.max_ckpt_per_txn = 64;
    opt.gc_eager = false;
    opt.gc_timeout = 60_000;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let bucket = mace
        .new_bucket(
            "x",
            BucketOptions {
                inline_size: 128,
                split_elems: 16,
                consolidate_threshold: 4,
                checkpoint_size: 256 << 10,
                pool_capacity: 2 << 20,
                ..BucketOptions::default()
            },
        )
        .unwrap();
    let keys: Vec<String> = (0..KEYS).map(|i| format!("k_{i:04}")).collect();

    let seed_payload = vec![b'a'; VALUE_SIZE];
    for parity in 0..2 {
        upsert_parity_retry(&bucket, &keys, parity, &seed_payload)?;
    }
    bucket.checkpoint();
    let mut expected = vec![b'a'; KEYS];

    for round in 0..ROUNDS {
        // Keep a lagging snapshot alive: this is exactly where premature junk reclaim used
        // to break (reachable old addresses disappeared before becoming durable).
        let lag = bucket.view().unwrap();
        let before = expected.clone();
        let payload = parity_payload(round, VALUE_SIZE);
        upsert_parity_retry(&bucket, &keys, round % 2, &payload)?;
        let byte = payload[0];
        for (idx, value) in expected.iter_mut().enumerate() {
            if idx % 2 == round % 2 {
                *value = byte;
            }
        }

        for idx in (0..KEYS).step_by(9) {
            let value = lag.get(&keys[idx]).expect("lag view key must exist");
            assert_eq!(value.slice().len(), VALUE_SIZE);
            assert_eq!(value.slice()[0], before[idx]);
        }
        for item in lag.seek("k_") {
            assert_eq!(item.val().len(), VALUE_SIZE);
        }

        if round % 3 == 0 {
            bucket.checkpoint();
        }
        let view = bucket.view().unwrap();
        let mut count = 0;
        for item in view.seek("k_") {
            count += 1;
            assert_eq!(item.val().len(), VALUE_SIZE);
        }
        assert_eq!(count, KEYS);
    }

    bucket.checkpoint();
    let final_view = bucket.view().unwrap();
    for (idx, key) in keys.iter().enumerate() {
        let value = final_view.get(key).expect("final key must exist");
        assert_eq!(value.slice().len(), VALUE_SIZE);
        assert_eq!(value.slice()[0], expected[idx]);
    }

    let snapshot = observer.snapshot();
    let split_count = counter(&snapshot, CounterMetric::TreeNodeSplit);
    let consolidate_count = counter(&snapshot, CounterMetric::TreeNodeConsolidate);
    // If these stay zero, this test is no longer covering the intended risky path.
    assert!(split_count > 0, "split was not exercised");
    assert!(consolidate_count > 0, "compaction path was not exercised");
    Ok(())
}

#[cfg(feature = "extra_check")]
struct CheckpointHookReset;

#[cfg(feature = "extra_check")]
impl Drop for CheckpointHookReset {
    fn drop(&mut self) {
        testing::clear_checkpoint_hook();
    }
}

#[cfg(feature = "extra_check")]
#[test]
fn checkpoint_snapshot_holds_ebr_guard_before_wait_zero() -> Result<(), OpCode> {
    let _checkpoint_test_lock = testing::checkpoint_test_lock();
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.tmp_store = false;
    opt.sync_on_write = false;
    opt.concurrent_write = 1;
    let reopen_opt = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let bucket = mace.new_bucket(
        "x",
        BucketOptions {
            checkpoint_size: 256 << 10,
            ..BucketOptions::default()
        },
    )?;

    for i in 0..32 {
        let tx = bucket.begin()?;
        tx.put(format!("k_{i:04}"), vec![b'x'; 1024])?;
        tx.commit()?;
    }
    testing::checkpoint_and_wait(&bucket);

    let (deferred_tx, deferred_rx) = channel();
    let (snapshot_tx, snapshot_rx) = channel();
    let (fallback_tx, fallback_rx) = channel();
    let (guard_dropped_tx, guard_dropped_rx) = channel();
    let (built_tx, built_rx) = channel();
    let writer_ready = Arc::new(Barrier::new(2));
    let start_writer = Arc::new(Barrier::new(2));
    let release_writer = Arc::new(Barrier::new(2));
    let restore = Arc::new(CheckpointRootRestore::new());
    let target_bucket = bucket.id();
    let _reset = CheckpointHookReset;
    testing::set_checkpoint_hook(Some(Arc::new({
        let deferred_tx = deferred_tx.clone();
        let snapshot_tx = snapshot_tx.clone();
        let fallback_tx = fallback_tx.clone();
        let guard_dropped_tx = guard_dropped_tx.clone();
        let built_tx = built_tx.clone();
        let release_writer = release_writer.clone();
        let restore = restore.clone();
        move |point, _bucket_id| {
            if _bucket_id != target_bucket {
                return;
            }
            match point {
                CheckpointSyncPoint::AfterRetiredPageDeferred(addr) => {
                    let _ = deferred_tx.send(addr);
                    release_writer.wait();
                }
                CheckpointSyncPoint::BeforeSnapshotWaitZero => {
                    let _ = snapshot_tx.send(());
                }
                CheckpointSyncPoint::AfterRetiredPageFallbackRead(addr) => {
                    restore.restore();
                    let _ = fallback_tx.send(addr);
                }
                CheckpointSyncPoint::AfterRetiredPageGuardDropped => {
                    let _ = guard_dropped_tx.send(());
                }
                CheckpointSyncPoint::AfterSnapshotBuilt => {
                    let _ = built_tx.send(());
                }
            }
        }
    })));

    let writer = testing::spawn_checkpoint_retired_page_writer(
        &bucket,
        writer_ready.clone(),
        start_writer.clone(),
        restore,
    );
    writer_ready.wait();

    let checkpoint_bucket = bucket.clone();
    let checkpoint = std::thread::spawn(move || testing::checkpoint_and_wait(&checkpoint_bucket));
    snapshot_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("snapshot must reach wait_zero while writer is blocked");
    start_writer.wait();
    let deferred_addr = deferred_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("writer must defer a real retired page");
    release_writer.wait();
    guard_dropped_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("writer must release its guard and advance the epoch");
    let fallback_addr = fallback_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("snapshot must read a real retired_pages fallback");
    assert_eq!(fallback_addr, deferred_addr);
    built_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("snapshot must finish after fallback read");
    checkpoint.join().expect("checkpoint thread must not panic");
    writer.join().expect("writer thread must not panic");

    drop(_reset);
    drop(bucket);
    drop(mace);
    let reopened = Mace::new(reopen_opt.validate().unwrap())?;
    let reopened_bucket = reopened.get_bucket("x")?;
    assert_eq!(
        reopened_bucket.view()?.get("k_0000")?.slice(),
        b"x".repeat(1024)
    );
    assert_eq!(
        reopened_bucket.view()?.get("k_0031")?.slice(),
        b"x".repeat(1024)
    );
    Ok(())
}
