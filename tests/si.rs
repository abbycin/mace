#![cfg(feature = "extra_check")]

use mace::observe::{CounterMetric, InMemoryObserver, ObserveSnapshot};
use mace::testing;
use mace::{Bucket, BucketOptions, OpCode, Options, RandomPath};
use std::collections::BTreeMap;

use mace::testing::{
    CollectorSyncPoint, TreeUpdateSyncPoint, TxnAbortSyncPoint, TxnBeginSyncPoint,
    TxnCommitSyncPoint, ViewSyncPoint, VisibilitySyncPoint,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

fn suite_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn open_visibility_bucket() -> Result<Bucket, OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.tmp_store = true;
    opts.sync_on_write = false;
    opts.concurrent_write = 1;
    let mace = mace::Mace::new(opts.validate().unwrap())?;
    mace.new_bucket(
        "x",
        BucketOptions {
            consolidate_threshold: 4,
            split_elems: 32,
            ..BucketOptions::default()
        },
    )
}

fn open_visibility_bucket_with_groups(groups: u8) -> Result<Bucket, OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.tmp_store = true;
    opts.sync_on_write = false;
    opts.concurrent_write = groups;
    let mace = mace::Mace::new(opts.validate().unwrap())?;
    mace.new_bucket(
        "x",
        BucketOptions {
            consolidate_threshold: 4,
            split_elems: 32,
            ..BucketOptions::default()
        },
    )
}

fn open_lagging_view_bucket() -> Result<Bucket, OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.tmp_store = true;
    opts.concurrent_write = 1;
    opts.data_file_size = 16 << 10;
    opts.blob_file_size = 16 << 10;
    opts.data_garbage_ratio = 0;
    opts.blob_garbage_ratio = 0;
    opts.gc_eager = true;
    let mace = mace::Mace::new(opts.validate().unwrap())?;
    mace.new_bucket(
        "x",
        BucketOptions {
            inline_size: 128,
            split_elems: 16,
            consolidate_threshold: 4,
            checkpoint_size: 128 << 10,
            pool_capacity: 1 << 20,
            cache_capacity: 1 << 20,
            cache_evict_pct: 100,
            ..BucketOptions::default()
        },
    )
}

fn vis_key(idx: usize) -> String {
    format!("k_{idx:02}")
}

fn vis_value(stage: &str, idx: usize) -> Vec<u8> {
    format!("{stage}_{idx:02}").into_bytes()
}

#[cfg(feature = "extra_check")]
fn counter_value(snapshot: &ObserveSnapshot, metric: CounterMetric) -> u64 {
    snapshot
        .counters
        .iter()
        .find(|(m, _)| *m == metric)
        .map(|(_, v)| *v)
        .unwrap_or(0)
}

#[derive(Clone, Copy)]
enum ModelReaderKind {
    View,
    Txn,
}

#[derive(Clone, Copy)]
enum ScriptOp {
    Upsert(&'static str, &'static str),
    Delete(&'static str),
}

#[derive(Clone, Copy)]
struct ScriptWriter<'a> {
    ops: &'a [ScriptOp],
}

#[derive(Clone, Copy)]
enum ScriptEvent {
    BeginWriter(usize),
    CommitWriter(usize),
    AbortWriter(usize),
    BeginReader,
}

enum LiveReader<'a> {
    View(mace::TxnView<'a>),
    Txn(mace::TxnKV<'a>),
}

macro_rules! collect_reader_observation {
    ($reader:expr, $keys:expr) => {{
        let mut points = Vec::new();
        for &key in $keys {
            let value = match $reader.get(key) {
                Ok(v) => Some(v.slice().to_vec()),
                Err(OpCode::NotFound) => None,
                Err(e) => return Err(e),
            };
            points.push((key.as_bytes().to_vec(), value));
        }
        let forward: Vec<(Vec<u8>, Vec<u8>)> = $reader
            .range::<_, _>("k".."l")
            .map(|item| (item.key().to_vec(), item.val().to_vec()))
            .collect();
        let reverse: Vec<(Vec<u8>, Vec<u8>)> = $reader
            .range::<_, _>("k".."l")
            .rev()
            .map(|item| (item.key().to_vec(), item.val().to_vec()))
            .collect();
        (points, forward, reverse)
    }};
}

fn apply_script_ops(tx: &mace::TxnKV<'_>, ops: &[ScriptOp]) -> Result<(), OpCode> {
    for op in ops {
        match *op {
            ScriptOp::Upsert(key, value) => tx.upsert(key, value)?,
            ScriptOp::Delete(key) => tx.del(key)?,
        }
    }
    Ok(())
}

fn apply_ref_ops(state: &mut BTreeMap<Vec<u8>, Vec<u8>>, ops: &[ScriptOp]) {
    for op in ops {
        match *op {
            ScriptOp::Upsert(key, value) => {
                state.insert(key.as_bytes().to_vec(), value.as_bytes().to_vec());
            }
            ScriptOp::Delete(key) => {
                state.remove(key.as_bytes());
            }
        }
    }
}

type PointObservation = Vec<(Vec<u8>, Option<Vec<u8>>)>;
type RangeObservation = Vec<(Vec<u8>, Vec<u8>)>;
type ReaderObservation = (PointObservation, RangeObservation, RangeObservation);

fn collect_live_reader_observation(
    reader: &LiveReader<'_>,
    keys: &[&str],
) -> Result<ReaderObservation, OpCode> {
    match reader {
        LiveReader::View(reader) => Ok(collect_reader_observation!(reader, keys)),
        LiveReader::Txn(reader) => Ok(collect_reader_observation!(reader, keys)),
    }
}

fn churn_reference_schedule(db: &Bucket) -> Result<(), OpCode> {
    for round in 0..4usize {
        let tx = db.begin()?;
        for idx in 0..24usize {
            let key = format!("pad_model_{round}_{idx:02}");
            if idx % 3 == 0 {
                tx.upsert(&key, vec![91u8; 256])?;
            } else {
                tx.upsert(&key, key.as_bytes())?;
            }
        }
        tx.commit()?;
        if round % 2 == 0 {
            db.checkpoint();
        }
    }
    Ok(())
}

struct ModelScheduleCase<'a> {
    name: &'a str,
    groups: u8,
    reader_kind: ModelReaderKind,
    base: &'a [(&'static str, &'static str)],
    writers: &'a [ScriptWriter<'a>],
    events: &'a [ScriptEvent],
    observed_keys: &'a [&'static str],
    churn_after_reader: bool,
}

fn run_model_schedule_case(case: ModelScheduleCase<'_>) -> Result<(), OpCode> {
    let ModelScheduleCase {
        name,
        groups,
        reader_kind,
        base,
        writers,
        events,
        observed_keys,
        churn_after_reader,
    } = case;
    let db = open_visibility_bucket_with_groups(groups)?;

    let mut committed = BTreeMap::new();
    for &(key, value) in base {
        let tx = db.begin()?;
        tx.put(key, value)?;
        tx.commit()?;
        committed.insert(key.as_bytes().to_vec(), value.as_bytes().to_vec());
    }

    let mut writers_live: Vec<Option<mace::TxnKV<'_>>> = Vec::with_capacity(writers.len());
    for _ in 0..writers.len() {
        writers_live.push(None);
    }
    let mut reader = None;
    let mut expected_snapshot = None;

    for event in events {
        match *event {
            ScriptEvent::BeginWriter(idx) => {
                let tx = db.begin()?;
                apply_script_ops(&tx, writers[idx].ops)?;
                writers_live[idx] = Some(tx);
            }
            ScriptEvent::CommitWriter(idx) => {
                let tx = writers_live[idx]
                    .take()
                    .unwrap_or_else(|| panic!("{name}: writer {idx} must be live before commit"));
                tx.commit()?;
                apply_ref_ops(&mut committed, writers[idx].ops);
            }
            ScriptEvent::AbortWriter(idx) => {
                let tx = writers_live[idx]
                    .take()
                    .unwrap_or_else(|| panic!("{name}: writer {idx} must be live before abort"));
                drop(tx);
            }
            ScriptEvent::BeginReader => {
                expected_snapshot = Some(committed.clone());
                reader = Some(match reader_kind {
                    ModelReaderKind::View => LiveReader::View(db.view()?),
                    ModelReaderKind::Txn => LiveReader::Txn(db.begin()?),
                });
            }
        }
    }

    if churn_after_reader {
        churn_reference_schedule(&db)?;
    }

    let expected_snapshot =
        expected_snapshot.unwrap_or_else(|| panic!("{name}: reader must start during schedule"));
    let reader = reader.unwrap_or_else(|| panic!("{name}: live reader must exist"));
    let (points, forward, reverse) = collect_live_reader_observation(&reader, observed_keys)?;

    let mut expected_points = Vec::new();
    for &key in observed_keys {
        expected_points.push((
            key.as_bytes().to_vec(),
            expected_snapshot.get(key.as_bytes()).cloned(),
        ));
    }
    assert_eq!(
        points, expected_points,
        "{name}: point observation mismatch"
    );

    let expected_forward: Vec<(Vec<u8>, Vec<u8>)> = expected_snapshot
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(forward, expected_forward, "{name}: forward range mismatch");

    let mut expected_reverse = expected_forward.clone();
    expected_reverse.reverse();
    assert_eq!(reverse, expected_reverse, "{name}: reverse range mismatch");
    Ok(())
}

#[test]
fn lagging_snapshot_survives_after_prior_quiescent_view() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_lagging_view_bucket()?;

    drop(db.view()?);

    let even_payload = vec![1u8; 8];
    let odd_payload = vec![92u8; 2048];

    let tx = db.begin()?;
    for idx in 0..32usize {
        if idx % 2 == 0 {
            tx.upsert(format!("k_{idx:02}"), &even_payload)?;
        }
    }
    tx.commit()?;

    let snapshot = db.view()?;

    let tx = db.begin()?;
    for idx in 0..32usize {
        if idx % 2 == 1 {
            tx.upsert(format!("k_{idx:02}"), &odd_payload)?;
        }
    }
    tx.commit()?;

    let tx = db.begin()?;
    tx.del("k_04")?;
    tx.commit()?;

    for idx in (0..32usize).step_by(5) {
        let key = format!("k_{idx:02}");
        if idx % 2 == 0 {
            let value = snapshot
                .get(&key)
                .unwrap_or_else(|e| panic!("snapshot lost {key}: {e:?}"));
            assert_eq!(
                value.slice(),
                even_payload.as_slice(),
                "snapshot mismatch {key}"
            );
        } else {
            assert!(matches!(snapshot.get(&key), Err(OpCode::NotFound)));
        }
    }
    Ok(())
}

#[test]
fn txn_reads_own_writes_in_command_order() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let seed = db.begin()?;
    seed.put("k_existing", "base")?;
    seed.put("k_removed", "gone")?;
    seed.commit()?;

    let tx = db.begin()?;
    tx.put("k_new", "n1")?;
    assert_eq!(tx.get("k_new")?.slice(), b"n1");

    tx.update("k_existing", "u1")?;
    assert_eq!(tx.get("k_existing")?.slice(), b"u1");

    tx.del("k_removed")?;
    assert!(matches!(tx.get("k_removed"), Err(OpCode::NotFound)));

    tx.put("k_seq", "v1")?;
    assert_eq!(tx.get("k_seq")?.slice(), b"v1");
    tx.update("k_seq", "v2")?;
    assert_eq!(tx.get("k_seq")?.slice(), b"v2");
    tx.del("k_seq")?;
    assert!(matches!(tx.get("k_seq"), Err(OpCode::NotFound)));
    tx.upsert("k_seq", "v3")?;
    assert_eq!(tx.get("k_seq")?.slice(), b"v3");

    let rows: Vec<(Vec<u8>, Vec<u8>)> = tx
        .range::<_, _>("k".."l")
        .map(|item| (item.key().to_vec(), item.val().to_vec()))
        .collect();
    assert_eq!(
        rows,
        vec![
            (b"k_existing".to_vec(), b"u1".to_vec()),
            (b"k_new".to_vec(), b"n1".to_vec()),
            (b"k_seq".to_vec(), b"v3".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn view_must_not_see_writer_whose_start_ts_equals_snapshot_ts() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let seed = db.begin()?;
    seed.put("k", "v0")?;
    seed.commit()?;

    let view = db.view()?;
    let view_start = testing::view_start_ts(&view);

    let writer = db.begin()?;
    let writer_start = testing::txn_start_ts(&writer);
    assert_eq!(
        writer_start, view_start,
        "writer started after a view should reuse the sampled oracle as its start ts"
    );
    writer.update("k", "v1")?;
    assert_eq!(writer.get("k")?.slice(), b"v1");

    assert_eq!(view.get("k")?.slice(), b"v0");
    let rows: Vec<(Vec<u8>, Vec<u8>)> = view
        .range::<_, _>("k".."l")
        .map(|item| (item.key().to_vec(), item.val().to_vec()))
        .collect();
    assert_eq!(rows, vec![(b"k".to_vec(), b"v0".to_vec())]);
    Ok(())
}

#[test]
fn view_keeps_pre_snapshot_version_when_older_active_tx_commits_later() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k", "v0")?;
    tx.commit()?;

    let tx = db.begin()?;
    tx.update("k", "v1")?;
    tx.commit()?;

    let pending = db.begin()?;
    pending.update("k", "v2")?;

    let snapshot = db.view()?;

    pending.commit()?;

    for i in 0..96 {
        let tx = db.begin()?;
        let key = format!("pad_view_{i:03}");
        tx.put(&key, &key)?;
        tx.commit()?;
    }

    let got = snapshot.get("k")?;
    assert_eq!(got.slice(), b"v1");

    let rows: Vec<(Vec<u8>, Vec<u8>)> = snapshot
        .range::<_, _>("k".."l")
        .map(|item| (item.key().to_vec(), item.val().to_vec()))
        .collect();
    assert_eq!(
        rows,
        vec![("k".as_bytes().to_vec(), "v1".as_bytes().to_vec())]
    );
    Ok(())
}

#[test]
fn newer_live_view_keeps_snapshot_when_older_view_is_still_alive_during_churn() -> Result<(), OpCode>
{
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k", "v0")?;
    tx.commit()?;

    let oldest = db.view()?;

    let tx = db.begin()?;
    tx.update("k", "v1")?;
    tx.commit()?;

    let newer = db.view()?;

    let tx = db.begin()?;
    tx.update("k", "v2")?;
    tx.commit()?;

    for i in 0..96 {
        let tx = db.begin()?;
        let key = format!("pad_multi_view_{i:03}");
        tx.put(&key, &key)?;
        tx.commit()?;
    }

    assert_eq!(oldest.get("k")?.slice(), b"v0");
    assert_eq!(newer.get("k")?.slice(), b"v1");

    let oldest_rows: Vec<(Vec<u8>, Vec<u8>)> = oldest
        .range::<_, _>("k".."l")
        .map(|item| (item.key().to_vec(), item.val().to_vec()))
        .collect();
    assert_eq!(
        oldest_rows,
        vec![("k".as_bytes().to_vec(), "v0".as_bytes().to_vec())]
    );

    let newer_rows: Vec<(Vec<u8>, Vec<u8>)> = newer
        .range::<_, _>("k".."l")
        .map(|item| (item.key().to_vec(), item.val().to_vec()))
        .collect();
    assert_eq!(
        newer_rows,
        vec![("k".as_bytes().to_vec(), "v1".as_bytes().to_vec())]
    );

    let fresh = db.view()?;
    assert_eq!(fresh.get("k")?.slice(), b"v2");
    Ok(())
}

#[test]
fn view_must_not_see_same_group_txn_that_was_active_at_snapshot() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k", "v0")?;
    tx.commit()?;

    let older = db.begin()?;
    older.update("k", "va")?;

    let newer = db.begin()?;
    newer.put("pad", "vb")?;
    newer.commit()?;

    let snapshot = db.view()?;

    older.commit()?;

    let got = snapshot.get("k")?;
    assert_eq!(got.slice(), b"v0");

    let rows: Vec<(Vec<u8>, Vec<u8>)> = snapshot
        .range::<_, _>("k".."l")
        .map(|item| (item.key().to_vec(), item.val().to_vec()))
        .collect();
    assert_eq!(
        rows,
        vec![("k".as_bytes().to_vec(), "v0".as_bytes().to_vec())]
    );
    Ok(())
}

#[test]
fn same_key_overlapping_put_and_upsert_allow_only_first_writer() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket_with_groups(2)?;

    let first = db.begin()?;
    let second = db.begin()?;

    first.put("fresh", "v1")?;
    assert_eq!(second.upsert("fresh", "v2"), Err(OpCode::AbortTx));

    first.commit()?;
    drop(second);

    let snapshot = db.view()?;
    assert_eq!(snapshot.get("fresh")?.slice(), b"v1");
    Ok(())
}

#[test]
fn same_key_overlapping_delete_and_upsert_allow_only_first_writer() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket_with_groups(2)?;

    let seed = db.begin()?;
    seed.put("k", "v0")?;
    seed.commit()?;

    let first = db.begin()?;
    let second = db.begin()?;

    first.del("k")?;
    assert_eq!(second.upsert("k", "v2"), Err(OpCode::AbortTx));

    first.commit()?;
    drop(second);

    let snapshot = db.view()?;
    assert!(matches!(snapshot.get("k"), Err(OpCode::NotFound)));
    Ok(())
}

#[test]
fn same_key_forced_race_commits_at_most_one_writer() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket_with_groups(2)?;

    let seed = db.begin()?;
    seed.put("k", "v0")?;
    seed.commit()?;

    let barrier = Arc::new(Barrier::new(3));
    let winner = std::thread::scope(|s| {
        let db1 = db.clone();
        let barrier1 = barrier.clone();
        let left = s.spawn(move || -> Result<Option<Vec<u8>>, OpCode> {
            let tx = db1.begin()?;
            barrier1.wait();
            if tx.update("k", "left").is_ok() {
                tx.commit()?;
                return Ok(Some(b"left".to_vec()));
            }
            drop(tx);
            Ok(None)
        });

        let db2 = db.clone();
        let barrier2 = barrier.clone();
        let right = s.spawn(move || -> Result<Option<Vec<u8>>, OpCode> {
            let tx = db2.begin()?;
            barrier2.wait();
            if tx.update("k", "right").is_ok() {
                tx.commit()?;
                return Ok(Some(b"right".to_vec()));
            }
            drop(tx);
            Ok(None)
        });

        barrier.wait();
        let left = left.join().unwrap()?;
        let right = right.join().unwrap()?;
        match (left, right) {
            (Some(winner), None) | (None, Some(winner)) => Ok::<Vec<u8>, OpCode>(winner),
            (Some(_), Some(_)) => panic!("same-key overlapping writers both committed"),
            (None, None) => panic!("same-key overlapping writers both aborted"),
        }
    })?;

    let snapshot = db.view()?;
    assert_eq!(snapshot.get("k")?.slice(), winner.as_slice());
    Ok(())
}

#[cfg(feature = "extra_check")]
#[test]
fn abort_exact_fact_stays_invisible_across_abort_publication() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k", "v0")?;
    tx.commit()?;

    let older = db.begin()?;
    older.update("k", "va")?;
    let older_txid = testing::txn_start_ts(&older);

    let snapshot_ready = Arc::new(Barrier::new(2));
    let reader_release = Arc::new(Barrier::new(2));
    let proof_entered = Arc::new(Barrier::new(2));
    let proof_release = Arc::new(Barrier::new(2));
    let abort_entered = Arc::new(Barrier::new(2));
    let abort_release = Arc::new(Barrier::new(2));
    let _reset = HookReset;

    testing::set_visibility_hook(Some(Arc::new({
        let proof_entered = proof_entered.clone();
        let proof_release = proof_release.clone();
        move |point, txid| {
            if point != VisibilitySyncPoint::AfterProofMissBeforeExactRead || txid != older_txid {
                return;
            }
            proof_entered.wait();
            proof_release.wait();
        }
    })));
    testing::set_txn_abort_hook(Some(Arc::new({
        let abort_entered = abort_entered.clone();
        let abort_release = abort_release.clone();
        move |point, txid| {
            if point != TxnAbortSyncPoint::AfterAbortFloorBeforeAbortedFactPublish
                || txid != older_txid
            {
                return;
            }
            abort_entered.wait();
            abort_release.wait();
        }
    })));

    std::thread::scope(|s| -> Result<(), OpCode> {
        let db_for_reader = db.clone();
        let snapshot_ready_worker = snapshot_ready.clone();
        let reader_release_worker = reader_release.clone();
        let reader = s.spawn(move || -> Result<Vec<u8>, OpCode> {
            let snapshot = db_for_reader.view()?;
            snapshot_ready_worker.wait();
            reader_release_worker.wait();
            Ok(snapshot.get("k")?.slice().to_vec())
        });

        snapshot_ready.wait();
        reader_release.wait();

        proof_entered.wait();
        let aborter = s.spawn(move || -> Result<(), OpCode> {
            drop(older);
            Ok(())
        });
        abort_entered.wait();
        abort_release.wait();
        aborter.join().unwrap()?;
        proof_release.wait();

        assert_eq!(reader.join().unwrap()?, b"v0".to_vec());
        Ok(())
    })
}

#[test]
fn abort_clean_pin_is_published_before_logging_handoff_releases() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;
    let observed = Arc::new(AtomicBool::new(false));
    let _reset = HookReset;
    testing::set_txn_abort_hook(Some(Arc::new({
        let db = db.clone();
        let observed = observed.clone();
        move |point, _start_ts| {
            if point == TxnAbortSyncPoint::AfterAbortCleanEnqueueBeforeLoggingRelease {
                observed.store(testing::group_logging_is_locked(&db, 0), Ordering::SeqCst);
            }
        }
    })));

    let tx = db.begin()?;
    tx.put("k", "uncommitted")?;
    drop(tx);

    assert!(
        observed.load(Ordering::SeqCst),
        "abort-clean task must be published before releasing the active WAL pin handoff"
    );
    Ok(())
}

#[test]
fn retained_abort_publish_updates_floor_and_membership() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;
    let observed = Arc::new(AtomicBool::new(false));
    let _reset = HookReset;
    testing::set_txn_abort_hook(Some(Arc::new({
        let db = db.clone();
        let observed = observed.clone();
        move |point, start_ts| {
            if point == TxnAbortSyncPoint::AfterAbortFloorBeforeAbortedFactPublish {
                observed.store(
                    testing::retained_abort_present(&db, 0, start_ts)
                        && testing::retained_abort_floor(&db, 0) <= start_ts,
                    Ordering::SeqCst,
                );
            }
        }
    })));

    let tx = db.begin()?;
    tx.put("k", "temp")?;
    drop(tx);

    assert!(
        observed.load(Ordering::SeqCst),
        "retained abort metadata must be published before the aborted fact is exposed"
    );
    Ok(())
}

#[test]
fn same_key_retry_rechecks_latest_head_after_abort_during_meta_window() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k", "v0")?;
    tx.commit()?;

    let older = db.begin()?;
    older.update("k", "va")?;

    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let armed = Arc::new(AtomicBool::new(true));
    let _reset = HookReset;
    testing::set_tree_update_hook(Some(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        let armed = armed.clone();
        move |point, _pid| {
            if point != TreeUpdateSyncPoint::AfterLatestMetaCheckBeforeDeltaInsert
                || !armed.swap(false, Ordering::SeqCst)
            {
                return;
            }
            entered.wait();
            release.wait();
        }
    })));

    std::thread::scope(|s| -> Result<(), OpCode> {
        let db_for_writer = db.clone();
        let writer = s.spawn(move || -> Result<(), OpCode> {
            let tx = db_for_writer.begin()?;
            tx.update("k", "vb")?;
            tx.commit()
        });

        entered.wait();
        drop(older);
        release.wait();
        writer.join().unwrap()?;
        Ok(())
    })?;

    let snapshot = db.view()?;
    assert_eq!(snapshot.get("k")?.slice(), b"vb");
    Ok(())
}

#[test]
fn same_key_page_replacement_retry_still_enforces_first_writer_wins() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(64));
    let mut opts = Options::new(&*RandomPath::new());
    opts.tmp_store = true;
    opts.sync_on_write = false;
    opts.concurrent_write = 2;
    opts.observer = observer.clone();
    let mace = mace::Mace::new(opts.validate().unwrap())?;
    let db = mace.new_bucket(
        "x",
        BucketOptions {
            split_elems: 64,
            consolidate_threshold: 16,
            ..BucketOptions::default()
        },
    )?;

    let seed = db.begin()?;
    seed.put("k", "v0")?;
    for i in 0..14 {
        let key = format!("s{i:02}");
        seed.put(&key, "seed")?;
    }
    seed.commit()?;

    let find_entered = Arc::new(Barrier::new(2));
    let find_release = Arc::new(Barrier::new(2));
    let again_entered = Arc::new(Barrier::new(2));
    let again_release = Arc::new(Barrier::new(2));
    let find_armed = Arc::new(AtomicBool::new(true));
    let again_armed = Arc::new(AtomicBool::new(true));
    let _reset = HookReset;
    testing::set_tree_update_hook(Some(Arc::new({
        let find_entered = find_entered.clone();
        let find_release = find_release.clone();
        let again_entered = again_entered.clone();
        let again_release = again_release.clone();
        let find_armed = find_armed.clone();
        let again_armed = again_armed.clone();
        move |point, _pid| match point {
            TreeUpdateSyncPoint::AfterFindLeafBeforeLink
                if find_armed.swap(false, Ordering::SeqCst) =>
            {
                find_entered.wait();
                find_release.wait();
            }
            TreeUpdateSyncPoint::AfterTreeAgainBeforeLatestMetaRecheck
                if again_armed.swap(false, Ordering::SeqCst) =>
            {
                again_entered.wait();
                again_release.wait();
            }
            _ => {}
        }
    })));

    std::thread::scope(|s| -> Result<(), OpCode> {
        let db1 = db.clone();
        let loser = s.spawn(move || -> Result<OpCode, OpCode> {
            let tx = db1.begin()?;
            let err = tx
                .update("k", "left")
                .expect_err("stale writer must abort after retry");
            drop(tx);
            Ok(err)
        });

        find_entered.wait();

        let winner = db.begin()?;
        winner.put("s14", "v14")?;
        winner.update("k", "right")?;
        winner.commit()?;

        find_release.wait();
        again_entered.wait();
        again_release.wait();
        assert_eq!(loser.join().unwrap()?, OpCode::AbortTx);
        Ok(())
    })?;

    let snapshot = db.view()?;
    assert_eq!(snapshot.get("k")?.slice(), b"right");

    let snapshot = observer.snapshot();
    assert!(
        counter_value(&snapshot, CounterMetric::TreeNodeConsolidate) > 0,
        "page replacement path must compact the leaf before the stale writer retries"
    );
    assert!(
        counter_value(&snapshot, CounterMetric::TxnRetryAgain) > 0,
        "same-key stale writer must observe at least one Again retry"
    );
    assert!(
        counter_value(&snapshot, CounterMetric::TreeRetryAgain) > 0,
        "tree update loop must record the structural retry"
    );
    Ok(())
}

#[test]
fn iterator_page_capture_keeps_snapshot_for_forward_and_reverse_after_late_commit()
-> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k0", "v0")?;
    tx.put("k1", "v0")?;
    tx.commit()?;

    let snapshot_ready = Arc::new(Barrier::new(2));
    let forward_entered = Arc::new(Barrier::new(2));
    let forward_release = Arc::new(Barrier::new(2));
    let reverse_entered = Arc::new(Barrier::new(2));
    let reverse_release = Arc::new(Barrier::new(2));
    let phase = Arc::new(AtomicUsize::new(0));
    let _reset = HookReset;
    testing::set_tree_update_hook(Some(Arc::new({
        let forward_entered = forward_entered.clone();
        let forward_release = forward_release.clone();
        let reverse_entered = reverse_entered.clone();
        let reverse_release = reverse_release.clone();
        let phase = phase.clone();
        move |point, _pid| {
            if point != TreeUpdateSyncPoint::AfterIteratorPageCaptureBeforeCandidateWalk {
                return;
            }
            match phase.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    forward_entered.wait();
                    forward_release.wait();
                }
                1 => {
                    reverse_entered.wait();
                    reverse_release.wait();
                }
                _ => {}
            }
        }
    })));

    std::thread::scope(|s| -> Result<(), OpCode> {
        let db_for_reader = db.clone();
        let snapshot_ready_worker = snapshot_ready.clone();
        let reader = s.spawn(move || -> Result<(), OpCode> {
            let snapshot = db_for_reader.view()?;
            snapshot_ready_worker.wait();

            let mut forward = snapshot.range::<_, _>("k0".."kz");
            let first = forward.next().expect("forward iterator must return k0");
            assert_eq!(first.key(), b"k0");
            assert_eq!(first.val(), b"v0");

            let mut reverse = snapshot.range::<_, _>("k0".."kz");
            let last = reverse
                .next_back()
                .expect("reverse iterator must return k1");
            assert_eq!(last.key(), b"k1");
            assert_eq!(last.val(), b"v0");
            Ok(())
        });

        snapshot_ready.wait();

        forward_entered.wait();
        let tx = db.begin()?;
        tx.update("k0", "v1")?;
        tx.commit()?;
        for round in 0..24usize {
            let tx = db.begin()?;
            let key = format!("pf_{round:02}");
            tx.put(&key, &key)?;
            tx.commit()?;
        }
        db.checkpoint();
        forward_release.wait();

        reverse_entered.wait();
        let tx = db.begin()?;
        tx.update("k1", "v1")?;
        tx.commit()?;
        for round in 0..24usize {
            let tx = db.begin()?;
            let key = format!("pr_{round:02}");
            tx.put(&key, &key)?;
            tx.commit()?;
        }
        db.checkpoint();
        reverse_release.wait();

        reader.join().unwrap()?;
        Ok(())
    })
}

#[test]
fn txn_must_not_see_same_group_txn_that_was_active_at_start() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k", "v0")?;
    tx.commit()?;

    let older = db.begin()?;
    older.update("k", "va")?;

    let newer = db.begin()?;
    newer.put("pad", "vb")?;
    newer.commit()?;

    let reader = db.begin()?;

    older.commit()?;

    let got = reader.get("k")?;
    assert_eq!(got.slice(), b"v0");

    let rows: Vec<(Vec<u8>, Vec<u8>)> = reader
        .range::<_, _>("k".."l")
        .map(|item| (item.key().to_vec(), item.val().to_vec()))
        .collect();
    assert_eq!(
        rows,
        vec![("k".as_bytes().to_vec(), "v0".as_bytes().to_vec())]
    );
    drop(reader);
    Ok(())
}

#[test]
fn snapshot_point_and_forward_reverse_traversal_match_reference_after_churn() -> Result<(), OpCode>
{
    let _guard = suite_lock();
    let db = open_lagging_view_bucket()?;
    let key_count = 32usize;
    let deleted = [4usize, 11, 18, 25];

    let tx = db.begin()?;
    for idx in 0..key_count {
        let key = vis_key(idx);
        tx.put(&key, vis_value("base", idx))?;
    }
    tx.commit()?;

    for round in 0..4usize {
        let tx = db.begin()?;
        for idx in (0..key_count).filter(|idx| idx % 2 == 0) {
            let key = vis_key(idx);
            tx.update(&key, vis_value(&format!("even_r{round}"), idx))?;
        }
        tx.commit()?;
    }

    let tx = db.begin()?;
    for &idx in &deleted {
        tx.del(vis_key(idx))?;
    }
    tx.commit()?;

    let snapshot = db.view()?;
    let mut expected = Vec::new();
    for idx in 0..key_count {
        if deleted.contains(&idx) {
            continue;
        }
        let value = if idx % 2 == 0 {
            vis_value("even_r3", idx)
        } else {
            vis_value("base", idx)
        };
        expected.push((vis_key(idx).into_bytes(), value));
    }

    let large_value = vec![92u8; 2048];
    for round in 0..6usize {
        let tx = db.begin()?;
        for idx in 0..key_count {
            let key = vis_key(idx);
            if deleted.contains(&idx) {
                tx.upsert(&key, &large_value)?;
                continue;
            }
            if idx % 2 == 1 {
                tx.update(&key, vis_value(&format!("odd_r{round}"), idx))?;
            } else if idx % 8 == 0 {
                if round == 0 {
                    tx.del(&key)?;
                } else {
                    tx.upsert(&key, &large_value)?;
                }
            } else {
                tx.update(&key, vis_value(&format!("even_post_r{round}"), idx))?;
            }
        }
        tx.commit()?;

        let tx = db.begin()?;
        for pad in 0..48usize {
            let key = format!("pad_{round}_{pad:02}");
            if pad % 3 == 0 {
                tx.upsert(&key, &large_value)?;
            } else {
                tx.upsert(&key, key.as_bytes())?;
            }
        }
        tx.commit()?;
        db.checkpoint();
    }

    for round in 0..6usize {
        let tx = db.begin()?;
        for pad in 0..48usize {
            if pad % 4 != 0 {
                tx.del(format!("pad_{round}_{pad:02}"))?;
            }
        }
        tx.commit()?;
    }

    for (key, value) in &expected {
        let got = snapshot
            .get(key)
            .unwrap_or_else(|e| panic!("point get failed for {:?}: {e:?}", key));
        assert_eq!(
            got.slice(),
            value.as_slice(),
            "point mismatch for key {:?}",
            key
        );
    }
    for &idx in &deleted {
        assert!(
            matches!(snapshot.get(vis_key(idx)), Err(OpCode::NotFound)),
            "snapshot must keep tombstone for {}",
            vis_key(idx)
        );
    }

    let rows: Vec<(Vec<u8>, Vec<u8>)> = snapshot
        .range::<_, _>("k".."l")
        .map(|item| (item.key().to_vec(), item.val().to_vec()))
        .collect();
    assert_eq!(rows, expected);

    let reverse_rows: Vec<(Vec<u8>, Vec<u8>)> = snapshot
        .range::<_, _>("k".."l")
        .rev()
        .map(|item| (item.key().to_vec(), item.val().to_vec()))
        .collect();
    let mut expected_reverse = expected.clone();
    expected_reverse.reverse();
    assert_eq!(reverse_rows, expected_reverse);
    Ok(())
}

#[test]
fn same_group_active_hole_keeps_later_commits_visible_without_range_proof() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let seed = db.begin()?;
    seed.put("seed", "v0")?;
    seed.commit()?;

    let hole = db.begin()?;
    let hole_txid = testing::txn_start_ts(&hole);
    hole.put("hole", "late")?;

    let mut short_txids = Vec::new();
    for idx in 0..24usize {
        let tx = db.begin()?;
        short_txids.push(testing::txn_start_ts(&tx));
        tx.put(format!("s_{idx:02}"), format!("v_{idx:02}"))?;
        tx.commit()?;
    }

    let snapshot = db.view()?;
    hole.commit()?;

    assert!(matches!(snapshot.get("hole"), Err(OpCode::NotFound)));
    assert_eq!(snapshot.get("s_00")?.slice(), b"v_00");
    assert_eq!(snapshot.get("s_23")?.slice(), b"v_23");

    testing::wake_cc_collector(&db);
    wait_until(Duration::from_millis(100), || {
        testing::safe_exclusive(&db) >= hole_txid
    });
    assert!(
        testing::fact_present(&db, 0, *short_txids.last().unwrap()),
        "the active hole keeps later facts exact instead of granting a range proof"
    );

    assert!(matches!(snapshot.get("hole"), Err(OpCode::NotFound)));
    assert_eq!(snapshot.get("s_00")?.slice(), b"v_00");
    assert_eq!(snapshot.get("s_23")?.slice(), b"v_23");
    Ok(())
}

#[test]
fn late_same_group_commit_remains_invisible_after_collector_cut() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let seed = db.begin()?;
    seed.put("seed", "v0")?;
    seed.commit()?;

    let hole = db.begin()?;
    let hole_txid = testing::txn_start_ts(&hole);
    hole.put("hole", "late")?;

    for idx in 0..24usize {
        let tx = db.begin()?;
        tx.put(format!("s_{idx:02}"), "v")?;
        tx.commit()?;
    }

    let snapshot = db.view()?;
    let snapshot_ts = testing::view_start_ts(&snapshot);
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let armed = Arc::new(AtomicBool::new(true));
    let _reset = HookReset;
    testing::set_collector_hook(Some(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        let armed = armed.clone();
        move |point, cut| {
            if point == CollectorSyncPoint::AfterCollectorCutBeforeFactScan
                && cut == snapshot_ts
                && armed.swap(false, Ordering::SeqCst)
            {
                entered.wait();
                release.wait();
            }
        }
    })));

    testing::wake_cc_collector(&db);
    entered.wait();
    hole.commit()?;
    release.wait();

    assert!(matches!(snapshot.get("hole"), Err(OpCode::NotFound)));
    assert!(
        testing::safe_exclusive(&db) <= hole_txid,
        "a commit after the snapshot must retain its exact fact until the snapshot retires"
    );
    Ok(())
}

#[test]
fn model_reference_schedules_match_point_forward_and_reverse_visibility() -> Result<(), OpCode> {
    let _guard = suite_lock();

    let case_a_w0 = [ScriptOp::Upsert("k0", "seen0")];
    let case_a_w1 = [ScriptOp::Delete("k1")];
    let case_a_w2 = [ScriptOp::Upsert("k2", "late2")];
    let case_a_writers = [
        ScriptWriter { ops: &case_a_w0 },
        ScriptWriter { ops: &case_a_w1 },
        ScriptWriter { ops: &case_a_w2 },
    ];
    let case_a_events = [
        ScriptEvent::BeginWriter(0),
        ScriptEvent::CommitWriter(0),
        ScriptEvent::BeginWriter(1),
        ScriptEvent::BeginReader,
        ScriptEvent::BeginWriter(2),
        ScriptEvent::AbortWriter(1),
        ScriptEvent::CommitWriter(2),
    ];

    let case_b_w0 = [ScriptOp::Delete("k0")];
    let case_b_w1 = [ScriptOp::Upsert("k2", "new2")];
    let case_b_writers = [
        ScriptWriter { ops: &case_b_w0 },
        ScriptWriter { ops: &case_b_w1 },
    ];
    let case_b_events = [
        ScriptEvent::BeginWriter(0),
        ScriptEvent::CommitWriter(0),
        ScriptEvent::BeginWriter(1),
        ScriptEvent::BeginReader,
        ScriptEvent::CommitWriter(1),
    ];

    let case_c_w0 = [ScriptOp::Upsert("k0", "late0")];
    let case_c_w1 = [ScriptOp::Upsert("k1", "seen1")];
    let case_c_w2 = [ScriptOp::Delete("k2")];
    let case_c_writers = [
        ScriptWriter { ops: &case_c_w0 },
        ScriptWriter { ops: &case_c_w1 },
        ScriptWriter { ops: &case_c_w2 },
    ];
    let case_c_events = [
        ScriptEvent::BeginWriter(0),
        ScriptEvent::BeginWriter(1),
        ScriptEvent::CommitWriter(1),
        ScriptEvent::BeginReader,
        ScriptEvent::BeginWriter(2),
        ScriptEvent::CommitWriter(0),
        ScriptEvent::AbortWriter(2),
    ];

    let cases = [
        (
            "commit_abort_and_late_commit",
            vec![("k0", "base0"), ("k1", "base1"), ("k2", "base2")],
            &case_a_writers[..],
            &case_a_events[..],
            vec!["k0", "k1", "k2"],
            true,
        ),
        (
            "visible_delete_and_invisible_insert",
            vec![("k0", "base0"), ("k1", "base1")],
            &case_b_writers[..],
            &case_b_events[..],
            vec!["k0", "k1", "k2"],
            false,
        ),
        (
            "commit_reorder_with_late_abort",
            vec![("k0", "base0"), ("k1", "base1"), ("k2", "base2")],
            &case_c_writers[..],
            &case_c_events[..],
            vec!["k0", "k1", "k2"],
            true,
        ),
    ];

    for groups in [1u8, 2u8] {
        for reader_kind in [ModelReaderKind::View, ModelReaderKind::Txn] {
            for (name, base, writers, events, keys, churn_after_reader) in &cases {
                run_model_schedule_case(ModelScheduleCase {
                    name,
                    groups,
                    reader_kind,
                    base,
                    writers,
                    events,
                    observed_keys: keys,
                    churn_after_reader: *churn_after_reader,
                })?;
            }
        }
    }
    Ok(())
}

#[test]
fn iterator_must_not_see_same_group_txn_that_was_active_at_snapshot() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k0", "v0")?;
    tx.put("k1", "v0")?;
    tx.commit()?;

    let older = db.begin()?;
    older.update("k1", "va")?;

    let newer = db.begin()?;
    newer.put("pad", "vb")?;
    newer.commit()?;

    let snapshot = db.view()?;
    let mut iter = snapshot.range::<_, _>("k0".."kz");

    let first = iter.next().expect("k0 must exist");
    assert_eq!(first.key(), b"k0");
    assert_eq!(first.val(), b"v0");

    older.commit()?;

    let second = iter.next().expect("k1 must remain at pre-snapshot version");
    assert_eq!(second.key(), b"k1");
    assert_eq!(second.val(), b"v0");
    assert!(iter.next().is_none());
    Ok(())
}

#[test]
fn txn_keeps_pre_snapshot_version_when_older_active_tx_commits_later() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k", "v0")?;
    tx.commit()?;

    let tx = db.begin()?;
    tx.update("k", "v1")?;
    tx.commit()?;

    let pending = db.begin()?;
    pending.update("k", "v2")?;

    let reader = db.begin()?;

    pending.commit()?;

    for i in 0..96 {
        let tx = db.begin()?;
        let key = format!("pad_txn_{i:03}");
        tx.put(&key, &key)?;
        tx.commit()?;
    }

    let got = reader.get("k")?;
    assert_eq!(got.slice(), b"v1");

    let rows: Vec<(Vec<u8>, Vec<u8>)> = reader
        .range::<_, _>("k".."l")
        .map(|item| (item.key().to_vec(), item.val().to_vec()))
        .collect();
    assert_eq!(
        rows,
        vec![("k".as_bytes().to_vec(), "v1".as_bytes().to_vec())]
    );
    drop(reader);
    Ok(())
}

#[test]
fn range_iterator_keeps_pre_snapshot_version_across_later_commit_and_churn() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k0", "v0")?;
    tx.put("k1", "v0")?;
    tx.commit()?;

    let tx = db.begin()?;
    tx.update("k0", "v1")?;
    tx.update("k1", "v1")?;
    tx.commit()?;

    let pending = db.begin()?;
    pending.update("k1", "v2")?;

    let snapshot = db.view()?;
    let mut iter = snapshot.range::<_, _>("k0".."kz");
    let first = iter.next().expect("k0 must exist");
    assert_eq!(first.key(), b"k0");
    assert_eq!(first.val(), b"v1");

    pending.commit()?;

    for i in 0..96 {
        let tx = db.begin()?;
        let key = format!("pad_iter_{i:03}");
        tx.put(&key, &key)?;
        tx.commit()?;
    }

    let second = iter.next().expect("k1 must remain visible");
    assert_eq!(second.key(), b"k1");
    assert_eq!(second.val(), b"v1");
    assert!(iter.next().is_none());
    Ok(())
}

#[test]
fn reopen_preserves_snapshot_visibility_for_recovered_state_and_new_churn() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let path = RandomPath::new();
    let mut opts = Options::new(&*path);
    opts.sync_on_write = false;
    let mut saved = opts.clone();

    let mace = mace::Mace::new(opts.validate().unwrap())?;
    let db = mace.new_bucket(
        "x",
        BucketOptions {
            split_elems: 16,
            consolidate_threshold: 4,
            ..BucketOptions::default()
        },
    )?;

    let tx = db.begin()?;
    tx.put("k0", "v0")?;
    tx.put("k1", "v0")?;
    tx.commit()?;

    let tx = db.begin()?;
    tx.update("k0", "v1")?;
    tx.del("k1")?;
    tx.commit()?;

    let aborted = db.begin()?;
    aborted.put("k2", "temp")?;
    drop(aborted);

    drop(db);
    drop(mace);

    saved.tmp_store = true;
    let mace = mace::Mace::new(saved.validate().unwrap())?;
    let db = mace.get_bucket("x").expect("bucket must reopen");

    let snapshot = db.view()?;
    assert_eq!(snapshot.get("k0")?.slice(), b"v1");
    assert!(matches!(snapshot.get("k1"), Err(OpCode::NotFound)));
    assert!(matches!(snapshot.get("k2"), Err(OpCode::NotFound)));

    let tx = db.begin()?;
    tx.update("k0", "v2")?;
    tx.upsert("k1", "resurrected")?;
    tx.commit()?;

    assert_eq!(snapshot.get("k0")?.slice(), b"v1");
    assert!(matches!(snapshot.get("k1"), Err(OpCode::NotFound)));

    let fresh = db.view()?;
    assert_eq!(fresh.get("k0")?.slice(), b"v2");
    assert_eq!(fresh.get("k1")?.slice(), b"resurrected");
    Ok(())
}

struct HookReset;

impl Drop for HookReset {
    fn drop(&mut self) {
        testing::clear_hooks();
    }
}

fn wait_until<F>(timeout: Duration, mut pred: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(pred(), "condition timed out after {timeout:?}");
}

#[test]
fn snapshot_cut_commit_publication_witness() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k", "v0")?;
    tx.commit()?;

    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let _reset = HookReset;
    testing::set_txn_commit_hook(Some(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        move |point, _start_ts| {
            if point != TxnCommitSyncPoint::AfterFactWriteGuardBeforeCommitTimestamp {
                return;
            }
            entered.wait();
            release.wait();
        }
    })));

    std::thread::scope(|s| -> Result<(), OpCode> {
        let db_for_commit = db.clone();
        let writer = s.spawn(move || -> Result<(), OpCode> {
            let tx = db_for_commit.begin()?;
            tx.update("k", "v1")?;
            tx.commit()
        });

        entered.wait();
        let snapshot = db.view()?;
        release.wait();

        writer.join().unwrap()?;

        let got = snapshot.get("k")?;
        assert_eq!(got.slice(), b"v0");
        Ok(())
    })
}

#[test]
fn txn_exact_read_waits_for_commit_publication_after_commit_timestamp_alloc() -> Result<(), OpCode>
{
    let _guard = suite_lock();
    let db = open_visibility_bucket_with_groups(2)?;

    let tx = db.begin()?;
    tx.put("k", "v0")?;
    tx.commit()?;

    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let _reset = HookReset;
    testing::set_txn_commit_hook(Some(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        move |point, _start_ts| {
            if point != TxnCommitSyncPoint::AfterCommitTimestampBeforeOutcomePublish {
                return;
            }
            entered.wait();
            release.wait();
        }
    })));

    std::thread::scope(|s| -> Result<(), OpCode> {
        let db_for_commit = db.clone();
        let writer = s.spawn(move || -> Result<(), OpCode> {
            let tx = db_for_commit.begin()?;
            tx.update("k", "v1")?;
            tx.commit()
        });

        entered.wait();
        let reader = db.begin()?;
        release.wait();

        writer.join().unwrap()?;

        let got = reader.get("k")?;
        assert_eq!(got.slice(), b"v1");
        drop(reader);
        Ok(())
    })
}

#[test]
fn collector_cut_includes_begin_registration_before_fact_publish() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("seed", "v0")?;
    tx.commit()?;

    let begin_entered = Arc::new(Barrier::new(2));
    let begin_release = Arc::new(Barrier::new(2));
    let cut_entered = Arc::new(Barrier::new(2));
    let cut_release = Arc::new(Barrier::new(2));
    let cut_armed = Arc::new(AtomicBool::new(true));
    let start_ts = Arc::new(Mutex::new(None));
    let _reset = HookReset;

    testing::set_txn_begin_hook(Some(Arc::new({
        let begin_entered = begin_entered.clone();
        let begin_release = begin_release.clone();
        let start_ts = start_ts.clone();
        move |point, txid| {
            if point != TxnBeginSyncPoint::AfterBeginTimestampBeforeFactPublish {
                return;
            }
            *start_ts.lock().unwrap() = Some(txid);
            begin_entered.wait();
            begin_release.wait();
        }
    })));

    let db_for_begin = db.clone();
    let worker = std::thread::spawn(move || -> Result<(), OpCode> {
        let tx = db_for_begin.begin()?;
        drop(tx);
        Ok(())
    });

    begin_entered.wait();
    testing::set_collector_hook(Some(Arc::new({
        let cut_entered = cut_entered.clone();
        let cut_release = cut_release.clone();
        let cut_armed = cut_armed.clone();
        move |point, _| {
            if point != CollectorSyncPoint::AfterCollectorCutBeforeFactScan
                || !cut_armed.swap(false, Ordering::SeqCst)
            {
                return;
            }
            cut_entered.wait();
            cut_release.wait();
        }
    })));

    testing::wake_cc_collector(&db);
    cut_entered.wait();
    cut_release.wait();
    testing::clear_collector_hook();

    let begin_ts = start_ts
        .lock()
        .unwrap()
        .expect("begin hook must publish start ts");
    wait_until(Duration::from_millis(100), || {
        testing::safe_exclusive(&db) == begin_ts
    });
    assert_eq!(testing::safe_exclusive(&db), begin_ts);

    begin_release.wait();
    worker.join().unwrap()?;
    Ok(())
}

#[test]
fn post_cut_view_registration_does_not_pin_safe_boundary() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("seed", "v0")?;
    tx.commit()?;

    testing::wake_cc_collector(&db);
    wait_until(Duration::from_millis(100), || {
        testing::safe_exclusive(&db) > 0
    });
    let old_safe = testing::safe_exclusive(&db);

    let view_entered = Arc::new(Barrier::new(2));
    let view_release = Arc::new(Barrier::new(2));
    let view_active = Arc::new(Barrier::new(2));
    let view_drop = Arc::new(Barrier::new(2));
    let cut_entered = Arc::new(Barrier::new(2));
    let cut_release = Arc::new(Barrier::new(2));
    let armed = Arc::new(AtomicBool::new(false));
    let cut_ts = Arc::new(Mutex::new(None));
    let view_ts = Arc::new(Mutex::new(None));
    let _reset = HookReset;

    testing::set_view_hook(Some(Arc::new({
        let view_entered = view_entered.clone();
        let view_release = view_release.clone();
        move |point| {
            if point != ViewSyncPoint::AfterCcnodeRegisteringBeforeTimestampSample {
                return;
            }
            view_entered.wait();
            view_release.wait();
        }
    })));
    testing::set_collector_hook(Some(Arc::new({
        let cut_entered = cut_entered.clone();
        let cut_release = cut_release.clone();
        let armed = armed.clone();
        let cut_ts = cut_ts.clone();
        move |point, cut| {
            if point != CollectorSyncPoint::AfterCollectorCutBeforeFactScan
                || !armed.swap(false, Ordering::SeqCst)
            {
                return;
            }
            *cut_ts.lock().unwrap() = Some(cut);
            cut_entered.wait();
            cut_release.wait();
        }
    })));

    let db_for_view = db.clone();
    let view_ts_worker = view_ts.clone();
    let view_active_worker = view_active.clone();
    let view_drop_worker = view_drop.clone();
    let view_worker = std::thread::spawn(move || -> Result<(), OpCode> {
        let view = db_for_view.view()?;
        let start_ts = testing::view_start_ts(&view);
        *view_ts_worker.lock().unwrap() = Some(start_ts);
        view_active_worker.wait();
        view_drop_worker.wait();
        drop(view);
        Ok(())
    });

    view_entered.wait();

    let tx = db.begin()?;
    tx.put("pad", "v1")?;
    tx.commit()?;

    armed.store(true, Ordering::SeqCst);
    testing::wake_cc_collector(&db);
    cut_entered.wait();
    view_release.wait();
    cut_release.wait();
    testing::clear_collector_hook();

    view_active.wait();
    wait_until(Duration::from_millis(100), || {
        testing::safe_exclusive(&db) > old_safe
    });
    let cut = cut_ts
        .lock()
        .unwrap()
        .expect("collector hook must capture cut");
    let start_ts = view_ts.lock().unwrap().expect("view must publish start ts");
    assert!(start_ts >= cut, "post-cut view must sample at or after cut");
    assert!(
        testing::safe_exclusive(&db) <= start_ts,
        "live post-cut view must still bound safe publication"
    );

    view_drop.wait();
    view_worker.join().unwrap()?;

    wait_until(Duration::from_millis(100), || {
        testing::safe_exclusive(&db) > old_safe
    });
    Ok(())
}

#[test]
fn exact_miss_rereads_safe_after_prune() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let db = open_visibility_bucket()?;

    let tx = db.begin()?;
    tx.put("k", "v0")?;
    tx.commit()?;

    let older = db.begin()?;

    let committed = db.begin()?;
    let committed_txid = testing::txn_start_ts(&committed);
    committed.update("k", "v1")?;
    committed.commit()?;

    let snapshot_ready = Arc::new(Barrier::new(2));
    let exact_entered = Arc::new(Barrier::new(2));
    let exact_release = Arc::new(Barrier::new(2));
    let prune_entered = Arc::new(Barrier::new(2));
    let prune_release = Arc::new(Barrier::new(2));
    let prune_armed = Arc::new(AtomicBool::new(false));
    let _reset = HookReset;

    testing::set_visibility_hook(Some(Arc::new({
        let exact_entered = exact_entered.clone();
        let exact_release = exact_release.clone();
        move |point, txid| {
            if point != VisibilitySyncPoint::AfterProofMissBeforeExactRead || txid != committed_txid
            {
                return;
            }
            exact_entered.wait();
            exact_release.wait();
        }
    })));
    testing::set_collector_hook(Some(Arc::new({
        let prune_entered = prune_entered.clone();
        let prune_release = prune_release.clone();
        let prune_armed = prune_armed.clone();
        move |point, safe| {
            if point != CollectorSyncPoint::AfterSafePublishBeforeCommittedFactPrune
                || !prune_armed.swap(false, Ordering::SeqCst)
                || safe <= committed_txid
            {
                return;
            }
            prune_entered.wait();
            prune_release.wait();
        }
    })));

    let db_for_reader = db.clone();
    let snapshot_ready_worker = snapshot_ready.clone();
    let reader = std::thread::spawn(move || -> Result<(), OpCode> {
        let snapshot = db_for_reader.view()?;
        snapshot_ready_worker.wait();
        let got = snapshot.get("k")?;
        assert_eq!(got.slice(), b"v1");
        Ok(())
    });

    snapshot_ready.wait();
    exact_entered.wait();
    older.commit()?;

    prune_armed.store(true, Ordering::SeqCst);
    testing::wake_cc_collector(&db);
    prune_entered.wait();
    assert!(
        testing::safe_exclusive(&db) > committed_txid,
        "safe must cover the committed record before prune"
    );
    prune_release.wait();

    wait_until(Duration::from_millis(100), || {
        !testing::fact_present(&db, 0, committed_txid)
    });

    exact_release.wait();
    reader.join().unwrap()?;
    Ok(())
}
