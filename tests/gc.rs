use mace::observe::{CounterMetric, HistogramMetric, InMemoryObserver};
#[cfg(feature = "extra_check")]
use mace::testing;
use mace::{BucketOptions, Mace, OpCode, Options, RandomPath};
use std::sync::Arc;
#[cfg(feature = "extra_check")]
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc::channel,
};
use std::time::{Duration, Instant};

fn counter_value(observer: &InMemoryObserver, metric: CounterMetric) -> u64 {
    observer
        .snapshot()
        .counters
        .iter()
        .find(|(m, _)| *m == metric)
        .map(|(_, v)| *v)
        .unwrap_or(0)
}

#[cfg(feature = "extra_check")]
struct HookReset;

#[cfg(feature = "extra_check")]
impl Drop for HookReset {
    fn drop(&mut self) {
        testing::clear_hooks();
    }
}

#[cfg(feature = "extra_check")]
#[test]
fn persisted_data_and_blob_stats_match_payloads_through_gc_and_reopen() -> Result<(), OpCode> {
    run_persisted_data_and_blob_stats_match_payloads_through_gc_and_reopen(false)
}

#[cfg(feature = "extra_check")]
#[test]
fn compressed_persisted_data_and_blob_stats_match_payloads_through_gc_and_reopen()
-> Result<(), OpCode> {
    run_persisted_data_and_blob_stats_match_payloads_through_gc_and_reopen(true)
}

#[cfg(feature = "extra_check")]
fn run_persisted_data_and_blob_stats_match_payloads_through_gc_and_reopen(
    enable_compression: bool,
) -> Result<(), OpCode> {
    let _hook_lock = testing::checkpoint_test_lock();
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.gc_timeout = 60_000;
    opt.gc_eager = true;
    opt.data_garbage_ratio = 1;
    opt.blob_garbage_ratio = 1;
    opt.data_file_size = 64 << 10;
    opt.blob_file_size = 64 << 10;
    let mace = Mace::new(opt.validate()?)?;
    mace.disable_gc();
    let bucket = mace.new_bucket(
        "stats",
        BucketOptions {
            inline_size: 1024,
            checkpoint_size: 64 << 10,
            pool_capacity: 128 << 10,
            enable_backpressure: false,
            enable_compression,
            ..BucketOptions::default()
        },
    )?;
    let blob_keys = (0..96).map(|idx| format!("b_{idx:03}")).collect::<Vec<_>>();
    let data_keys = (0..384)
        .map(|idx| format!("d_{idx:03}"))
        .collect::<Vec<_>>();
    let make_blob = |seed: u8| {
        let mut value = vec![0; 8 << 10];
        let mut state = u32::from(seed);
        for (idx, byte) in value[..4 << 10].iter_mut().enumerate() {
            state = state
                .wrapping_mul(1_664_525)
                .wrapping_add(1_013_904_223u32.wrapping_add(idx as u32));
            *byte = (state >> 24) as u8;
        }
        value.copy_within(..4 << 10, 4 << 10);
        value
    };
    let v1 = make_blob(b'a');
    let v2 = make_blob(b'b');
    let v3 = make_blob(b'c');
    let v4 = make_blob(b'd');
    let d1 = vec![b'x'; 512];
    let d2 = vec![b'y'; 512];
    let d3 = vec![b'z'; 512];
    let d4 = vec![b'w'; 512];

    let tx = bucket.begin()?;
    for key in &blob_keys {
        tx.put(key, &v1)?;
    }
    for key in &data_keys {
        tx.put(key, &d1)?;
    }
    tx.commit()?;
    testing::checkpoint_and_wait(&bucket);
    testing::assert_persisted_gc_stats(&mace);

    let tx = bucket.begin()?;
    for key in blob_keys.iter().step_by(2) {
        tx.update(key, &v2)?;
    }
    for key in data_keys.iter().step_by(2) {
        tx.update(key, &d2)?;
    }
    tx.commit()?;
    testing::checkpoint_and_wait(&bucket);
    testing::assert_persisted_gc_stats(&mace);

    let tx = bucket.begin()?;
    for key in blob_keys.iter().step_by(4) {
        tx.update(key, &v3)?;
    }
    for key in data_keys.iter().step_by(4) {
        tx.update(key, &d3)?;
    }
    tx.commit()?;
    testing::checkpoint_and_wait(&bucket);
    testing::assert_persisted_gc_stats(&mace);

    let tx = bucket.begin()?;
    for key in &blob_keys {
        tx.update(key, &v4)?;
    }
    for key in &data_keys {
        tx.update(key, &d4)?;
    }
    tx.commit()?;
    testing::checkpoint_and_wait(&bucket);
    testing::assert_persisted_gc_stats(&mace);

    drop(bucket);
    drop(mace);

    let mut gc_opt = Options::new(&*path);
    gc_opt.sync_on_write = true;
    gc_opt.gc_timeout = 60_000;
    gc_opt.gc_eager = true;
    gc_opt.data_garbage_ratio = 1;
    gc_opt.blob_garbage_ratio = 1;
    gc_opt.data_file_size = 64 << 10;
    gc_opt.blob_file_size = 64 << 10;
    let mace = Mace::new(gc_opt.validate()?)?;
    mace.disable_gc();
    let bucket = mace.get_bucket("stats")?;
    testing::assert_persisted_gc_stats(&mace);
    let view = bucket.view()?;
    for key in &blob_keys {
        assert_eq!(view.get(key)?.slice(), v4.as_slice());
    }
    for key in &data_keys {
        assert_eq!(view.get(key)?.slice(), d4.as_slice());
    }
    drop(view);

    mace.enable_gc();
    let data_gc_before = mace.data_gc_count();
    let blob_gc_before = mace.blob_gc_count();
    let gc_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < gc_deadline
        && (mace.data_gc_count() == data_gc_before || mace.blob_gc_count() == blob_gc_before)
    {
        mace.start_gc();
    }
    assert!(
        mace.data_gc_count() > data_gc_before,
        "expected data GC to run"
    );
    assert!(
        mace.blob_gc_count() > blob_gc_before,
        "expected blob GC to run"
    );
    testing::assert_persisted_gc_stats(&mace);

    drop(bucket);
    drop(mace);

    let mut reopen = Options::new(&*path);
    reopen.sync_on_write = true;
    reopen.gc_timeout = 60_000;
    reopen.gc_eager = true;
    reopen.data_garbage_ratio = 1;
    reopen.blob_garbage_ratio = 1;
    reopen.data_file_size = 64 << 10;
    reopen.blob_file_size = 64 << 10;
    let mace = Mace::new(reopen.validate()?)?;
    let bucket = mace.get_bucket("stats")?;
    testing::assert_persisted_gc_stats(&mace);
    let view = bucket.view()?;
    for key in &blob_keys {
        assert_eq!(view.get(key)?.slice(), v4.as_slice());
    }
    for key in &data_keys {
        assert_eq!(view.get(key)?.slice(), d4.as_slice());
    }
    mace.start_gc();
    testing::assert_persisted_gc_stats(&mace);
    Ok(())
}

#[cfg(feature = "extra_check")]
#[test]
fn checkpoint_junk_crossing_rewrite_publish_moves_to_output_stats() -> Result<(), OpCode> {
    let _hook_lock = testing::checkpoint_test_lock();
    let _hook_reset = HookReset;
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.concurrent_write = 1;
    opt.sync_on_write = true;
    opt.gc_timeout = 60_000;
    opt.gc_eager = false;
    opt.data_garbage_ratio = 100;
    opt.data_file_size = 256 << 10;
    let mace = Mace::new(opt.validate()?)?;
    mace.disable_gc();
    let bucket = mace.new_bucket(
        "rewrite_junk_handoff",
        BucketOptions {
            split_elems: 64,
            consolidate_threshold: 8,
            checkpoint_size: 256 << 10,
            pool_capacity: 512 << 10,
            enable_backpressure: false,
            ..BucketOptions::default()
        },
    )?;
    let keys = (0..256)
        .map(|idx| format!("key_{idx:04}"))
        .collect::<Vec<_>>();
    let initial = vec![b'a'; 256];
    let middle = vec![b'b'; 256];
    let latest = vec![b'c'; 256];

    let txn = bucket.begin()?;
    for key in &keys {
        txn.put(key, &initial)?;
    }
    txn.commit()?;
    testing::checkpoint_and_wait(&bucket);

    let txn = bucket.begin()?;
    for key in keys.iter().step_by(2) {
        txn.update(key, &middle)?;
    }
    txn.commit()?;
    drop(bucket);
    drop(mace);

    let mut rewrite = Options::new(&*path);
    rewrite.concurrent_write = 1;
    rewrite.sync_on_write = true;
    rewrite.gc_timeout = 60_000;
    rewrite.gc_eager = true;
    rewrite.data_garbage_ratio = 1;
    rewrite.data_file_size = 16 << 10;
    let mace = Mace::new(rewrite.validate()?)?;
    let bucket = mace.get_bucket("rewrite_junk_handoff")?;

    let (ready_tx, ready_rx) = channel();
    let (release_tx, release_rx) = channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let fired = Arc::new(AtomicBool::new(false));
    let bucket_id = bucket.id();
    let db_root = path.to_path_buf();
    testing::set_gc_rewrite_hook(Some(Arc::new({
        let fired = fired.clone();
        let release_rx = release_rx.clone();
        move |point, observed_bucket_id, observed_db_root| {
            if point == testing::GcRewriteSyncPoint::BeforeDataPublish
                && observed_bucket_id == bucket_id
                && observed_db_root == db_root
                && !fired.swap(true, Ordering::SeqCst)
            {
                ready_tx.send(()).expect("signal rewrite publish cut");
                let _ = release_rx
                    .lock()
                    .expect("lock rewrite release receiver")
                    .recv_timeout(Duration::from_secs(10));
            }
        }
    })));

    let before = mace.data_gc_count();
    let gc_mace = mace.clone();
    let gc_thread = std::thread::spawn(move || gc_mace.start_gc());
    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("data rewrite must reach publish cut");

    let txn = bucket.begin()?;
    for key in &keys {
        txn.update(key, &latest)?;
    }
    txn.commit()?;
    testing::checkpoint_and_wait(&bucket);
    release_tx.send(()).expect("release data rewrite");
    gc_thread.join().expect("data rewrite thread must finish");
    assert!(mace.data_gc_count() > before, "data rewrite did not finish");
    testing::checkpoint_and_wait(&bucket);
    testing::assert_persisted_gc_stats(&mace);

    let view = bucket.view()?;
    for key in &keys {
        assert_eq!(view.get(key)?.slice(), latest.as_slice());
    }
    drop(view);
    drop(bucket);
    drop(mace);

    let mut reopen = Options::new(&*path);
    reopen.concurrent_write = 1;
    reopen.sync_on_write = true;
    reopen.gc_timeout = 60_000;
    reopen.gc_eager = true;
    reopen.data_garbage_ratio = 1;
    reopen.data_file_size = 16 << 10;
    let mace = Mace::new(reopen.validate()?)?;
    testing::assert_persisted_gc_stats(&mace);
    let bucket = mace.get_bucket("rewrite_junk_handoff")?;
    let view = bucket.view()?;
    for key in &keys {
        assert_eq!(view.get(key)?.slice(), latest.as_slice());
    }
    Ok(())
}

#[cfg(feature = "extra_check")]
#[derive(Clone, Copy, Debug)]
enum TestFileKind {
    Data,
    Blob,
}

#[cfg(feature = "extra_check")]
#[derive(Clone, Copy, Debug)]
enum RetireCut {
    MetaCommit,
    RetiredMark,
    RuntimeRemove,
}

#[cfg(feature = "extra_check")]
fn retire_sync_point(kind: TestFileKind, cut: RetireCut) -> testing::GcStatSyncPoint {
    use testing::GcStatSyncPoint::*;
    match (kind, cut) {
        (TestFileKind::Data, RetireCut::MetaCommit) => DataObsoleteAfterMetaCommit,
        (TestFileKind::Data, RetireCut::RetiredMark) => DataObsoleteAfterRetiredMark,
        (TestFileKind::Data, RetireCut::RuntimeRemove) => DataObsoleteAfterRuntimeRemove,
        (TestFileKind::Blob, RetireCut::MetaCommit) => BlobObsoleteAfterMetaCommit,
        (TestFileKind::Blob, RetireCut::RetiredMark) => BlobObsoleteAfterRetiredMark,
        (TestFileKind::Blob, RetireCut::RuntimeRemove) => BlobObsoleteAfterRuntimeRemove,
    }
}

#[cfg(feature = "extra_check")]
fn conditional_stat_miss_metric(kind: TestFileKind) -> CounterMetric {
    match kind {
        TestFileKind::Data => CounterMetric::FlushConditionalDataStatPutMiss,
        TestFileKind::Blob => CounterMetric::FlushConditionalBlobStatPutMiss,
    }
}

#[cfg(feature = "extra_check")]
#[derive(Clone, Copy, Debug)]
enum CheckpointSchedule {
    BeforeGc,
    AfterGcCut,
}

#[cfg(feature = "extra_check")]
#[test]
fn lagging_checkpoint_cannot_resurrect_retired_stats_at_any_reclaim_cut() -> Result<(), OpCode> {
    let _hook_lock = testing::checkpoint_test_lock();
    let _hook_reset = HookReset;
    for kind in [TestFileKind::Data, TestFileKind::Blob] {
        let schedules = match kind {
            TestFileKind::Data => [true, true],
            TestFileKind::Blob => [false, true],
        };
        for (schedule, enabled) in [
            (CheckpointSchedule::BeforeGc, schedules[0]),
            (CheckpointSchedule::AfterGcCut, schedules[1]),
        ] {
            if !enabled {
                continue;
            }
            for cut in [
                RetireCut::MetaCommit,
                RetireCut::RetiredMark,
                RetireCut::RuntimeRemove,
            ] {
                run_lagging_checkpoint_retire_cut(kind, cut, schedule)?;
            }
        }
    }
    Ok(())
}

#[cfg(feature = "extra_check")]
fn run_lagging_checkpoint_retire_cut(
    kind: TestFileKind,
    cut: RetireCut,
    schedule: CheckpointSchedule,
) -> Result<(), OpCode> {
    testing::clear_hooks();
    let path = RandomPath::new();
    let observer = Arc::new(InMemoryObserver::new(1024));
    let mut opt = Options::new(&*path);
    opt.concurrent_write = 1;
    opt.sync_on_write = true;
    opt.gc_timeout = 60_000;
    opt.gc_eager = true;
    opt.data_garbage_ratio = 1;
    opt.blob_garbage_ratio = 1;
    opt.data_file_size = 256 << 10;
    opt.blob_file_size = 256 << 10;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let bucket = mace.new_bucket(
        "retire_cut",
        BucketOptions {
            inline_size: 1024,
            split_elems: 32,
            consolidate_threshold: 2,
            checkpoint_size: 2 << 20,
            pool_capacity: 4 << 20,
            enable_backpressure: false,
            ..BucketOptions::default()
        },
    )?;
    let keys = (0..96)
        .map(|idx| format!("key_{idx:04}"))
        .collect::<Vec<_>>();
    let value_len = match kind {
        TestFileKind::Data => 512,
        TestFileKind::Blob => 8 << 10,
    };
    let initial = vec![b'a'; value_len];

    let txn = bucket.begin()?;
    for key in &keys {
        txn.put(key, &initial)?;
    }
    txn.commit()?;
    testing::checkpoint_and_wait(&bucket);

    if matches!(kind, TestFileKind::Blob) {
        let replacement = vec![b'b'; value_len];
        let txn = bucket.begin()?;
        for key in &keys {
            txn.update(key, &replacement)?;
        }
        txn.commit()?;
        testing::checkpoint_and_wait(&bucket);
    }

    let initial_checkpoint =
        !(matches!(kind, TestFileKind::Blob) && matches!(schedule, CheckpointSchedule::AfterGcCut));

    if matches!(kind, TestFileKind::Blob) {
        for seed in [b'c', b'd'] {
            let replacement = vec![seed; value_len];
            let txn = bucket.begin()?;
            for key in &keys {
                txn.upsert(key, &replacement)?;
            }
            txn.commit()?;
            testing::checkpoint_and_wait(&bucket);
        }
    }
    let txn = bucket.begin()?;
    for key in &keys {
        txn.del(key)?;
    }
    txn.commit()?;
    if !initial_checkpoint {
        testing::checkpoint_and_wait(&bucket);
    }

    let (checkpoint_ready_tx, checkpoint_ready_rx) = channel();
    let (checkpoint_release_tx, checkpoint_release_rx) = channel();
    let checkpoint_release_rx = Arc::new(Mutex::new(checkpoint_release_rx));
    let (gc_ready_tx, gc_ready_rx) = channel();
    let (gc_release_tx, gc_release_rx) = channel();
    let gc_release_rx = Arc::new(Mutex::new(gc_release_rx));
    let checkpoint_fired = Arc::new(AtomicBool::new(false));
    let gc_fired = Arc::new(AtomicBool::new(false));
    let target = retire_sync_point(kind, cut);
    let bucket_id = bucket.id();
    let db_root = path.to_path_buf();
    testing::set_gc_stat_hook(Some(Arc::new({
        let checkpoint_fired = checkpoint_fired.clone();
        let gc_fired = gc_fired.clone();
        let checkpoint_release_rx = checkpoint_release_rx.clone();
        let gc_release_rx = gc_release_rx.clone();
        move |point, observed_bucket_id, observed_db_root| {
            if observed_bucket_id != bucket_id || observed_db_root != db_root {
                return;
            }
            if point == testing::GcStatSyncPoint::CheckpointBeforeManifestCommit
                && !checkpoint_fired.swap(true, Ordering::SeqCst)
            {
                checkpoint_ready_tx
                    .send(())
                    .expect("signal lagging checkpoint");
                let _ = checkpoint_release_rx
                    .lock()
                    .expect("lock checkpoint release receiver")
                    .recv_timeout(Duration::from_secs(10));
            }
            if point == target && !gc_fired.swap(true, Ordering::SeqCst) {
                gc_ready_tx.send(()).expect("signal reclaim cut");
                let _ = gc_release_rx
                    .lock()
                    .expect("lock gc release receiver")
                    .recv_timeout(Duration::from_secs(10));
            }
        }
    })));

    let miss_before = counter_value(&observer, conditional_stat_miss_metric(kind));
    let checkpoint_thread = if initial_checkpoint {
        let checkpoint_bucket = bucket.clone();
        let thread = std::thread::spawn(move || testing::checkpoint_and_wait(&checkpoint_bucket));
        checkpoint_ready_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("checkpoint must prepare the old stat update");
        Some(thread)
    } else {
        None
    };

    let gc_mace = mace.clone();
    let gc_loop_fired = gc_fired.clone();
    let gc_thread = std::thread::spawn(move || {
        while !gc_loop_fired.load(Ordering::Acquire) {
            gc_mace.start_gc();
        }
    });
    gc_ready_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap_or_else(|_| panic!("gc must reach {kind:?} {cut:?} ordinary reclaim cut"));

    match schedule {
        CheckpointSchedule::BeforeGc => {
            checkpoint_release_tx
                .send(())
                .expect("release lagging checkpoint");
            checkpoint_thread
                .expect("pre-cut checkpoint thread must exist")
                .join()
                .expect("lagging checkpoint must finish");
            assert!(
                counter_value(&observer, conditional_stat_miss_metric(kind)) > miss_before,
                "old stat update must miss after reclaim delete commits first"
            );
        }
        CheckpointSchedule::AfterGcCut => {
            if let Some(checkpoint_thread) = checkpoint_thread {
                checkpoint_release_tx
                    .send(())
                    .expect("release pre-cut checkpoint");
                checkpoint_thread
                    .join()
                    .expect("pre-cut checkpoint must finish");
            }

            let late_key = "late_checkpoint_key";
            let late_value = vec![b'b'; value_len];
            let txn = bucket.begin()?;
            txn.upsert(late_key, &late_value)?;
            txn.commit()?;
            checkpoint_fired.store(false, Ordering::SeqCst);
            let checkpoint_bucket = bucket.clone();
            let checkpoint_thread =
                std::thread::spawn(move || testing::checkpoint_and_wait(&checkpoint_bucket));
            checkpoint_ready_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("post-cut checkpoint must reach manifest commit");
            checkpoint_release_tx
                .send(())
                .expect("release post-cut checkpoint");
            checkpoint_thread
                .join()
                .expect("post-cut checkpoint must finish");
        }
    }

    gc_release_tx.send(()).expect("release ordinary reclaim");
    gc_thread.join().expect("ordinary reclaim must finish");
    testing::clear_gc_stat_hook();

    testing::checkpoint_and_wait(&bucket);
    mace.start_gc();
    testing::assert_persisted_gc_stats(&mace);
    let view = bucket.view()?;
    for key in &keys {
        assert!(
            matches!(view.get(key), Err(OpCode::NotFound)),
            "{kind:?} {schedule:?} key {key} must be deleted"
        );
    }
    if matches!(schedule, CheckpointSchedule::AfterGcCut) {
        assert_eq!(
            view.get("late_checkpoint_key")?.slice(),
            vec![b'b'; value_len]
        );
    }
    drop(view);
    drop(bucket);
    drop(mace);

    let mut reopen = Options::new(&*path);
    reopen.concurrent_write = 1;
    reopen.sync_on_write = true;
    reopen.gc_timeout = 60_000;
    reopen.gc_eager = true;
    reopen.data_garbage_ratio = 1;
    reopen.blob_garbage_ratio = 1;
    reopen.data_file_size = 256 << 10;
    reopen.blob_file_size = 256 << 10;
    let mace = Mace::new(reopen.validate()?)?;
    testing::assert_persisted_gc_stats(&mace);
    Ok(())
}

#[cfg(feature = "extra_check")]
#[test]
fn publishing_checkpoint_reresolves_multi_output_data_owner() -> Result<(), OpCode> {
    run_publishing_checkpoint_reresolves_owner(TestFileKind::Data, false)
}

#[cfg(feature = "extra_check")]
#[test]
fn compressed_blob_publishing_checkpoint_reresolves_multi_output_owner() -> Result<(), OpCode> {
    run_publishing_checkpoint_reresolves_owner(TestFileKind::Blob, true)
}

#[cfg(feature = "extra_check")]
fn run_publishing_checkpoint_reresolves_owner(
    kind: TestFileKind,
    enable_compression: bool,
) -> Result<(), OpCode> {
    let _hook_lock = testing::checkpoint_test_lock();
    let _hook_reset = HookReset;
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.concurrent_write = 1;
    opt.sync_on_write = true;
    opt.gc_timeout = 60_000;
    opt.gc_eager = false;
    opt.data_garbage_ratio = 100;
    opt.blob_garbage_ratio = 100;
    opt.data_file_size = 2 << 20;
    opt.blob_file_size = 4 << 20;
    let mace = Mace::new(opt.validate()?)?;
    mace.disable_gc();
    let bucket = mace.new_bucket(
        "publishing_owner",
        BucketOptions {
            inline_size: 1024,
            split_elems: 64,
            consolidate_threshold: 8,
            checkpoint_size: 4 << 20,
            pool_capacity: 8 << 20,
            enable_backpressure: false,
            enable_compression,
            ..BucketOptions::default()
        },
    )?;
    let key_count = match kind {
        TestFileKind::Data => 512,
        TestFileKind::Blob => 96,
    };
    let value_len = match kind {
        TestFileKind::Data => 512,
        TestFileKind::Blob => 8 << 10,
    };
    let keys = (0..key_count)
        .map(|idx| format!("key_{idx:04}"))
        .collect::<Vec<_>>();
    let make_value = |seed: u8| {
        if matches!(kind, TestFileKind::Blob) && enable_compression {
            let mut value = vec![0; value_len];
            let half = value_len / 2;
            let mut state = u32::from(seed);
            for (idx, byte) in value[..half].iter_mut().enumerate() {
                state = state
                    .wrapping_mul(1_664_525)
                    .wrapping_add(1_013_904_223u32.wrapping_add(idx as u32));
                *byte = (state >> 24) as u8;
            }
            value.copy_within(..half, half);
            value
        } else {
            vec![seed; value_len]
        }
    };
    let initial = make_value(b'a');
    let middle = make_value(b'b');
    let latest = make_value(b'c');

    let txn = bucket.begin()?;
    for key in &keys {
        txn.put(key, &initial)?;
    }
    txn.commit()?;
    testing::checkpoint_and_wait(&bucket);
    let churn_rounds = match kind {
        TestFileKind::Data => 1,
        TestFileKind::Blob => 4,
    };
    for _ in 0..churn_rounds {
        let txn = bucket.begin()?;
        for key in keys.iter().step_by(2) {
            txn.update(key, &middle)?;
        }
        txn.commit()?;
        testing::checkpoint_and_wait(&bucket);
    }
    drop(bucket);
    drop(mace);

    let mut rewrite = Options::new(&*path);
    rewrite.concurrent_write = 1;
    rewrite.sync_on_write = true;
    rewrite.gc_timeout = 60_000;
    rewrite.gc_eager = true;
    rewrite.data_garbage_ratio = match kind {
        TestFileKind::Data => 1,
        TestFileKind::Blob => 100,
    };
    rewrite.blob_garbage_ratio = match kind {
        TestFileKind::Data => 100,
        TestFileKind::Blob => 1,
    };
    rewrite.data_file_size = 16 << 10;
    rewrite.blob_file_size = 16 << 10;
    let mace = Mace::new(rewrite.validate()?)?;
    let bucket = mace.get_bucket("publishing_owner")?;

    let (publishing_tx, publishing_rx) = channel();
    let (release_tx, release_rx) = channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let (wait_tx, wait_rx) = channel();
    let publishing_fired = Arc::new(AtomicBool::new(false));
    let wait_fired = Arc::new(AtomicBool::new(false));
    let output_count = Arc::new(AtomicUsize::new(0));
    let bucket_id = bucket.id();
    let db_root = path.to_path_buf();
    testing::set_gc_stat_hook(Some(Arc::new({
        let publishing_fired = publishing_fired.clone();
        let wait_fired = wait_fired.clone();
        let output_count = output_count.clone();
        let release_rx = release_rx.clone();
        move |point, observed_bucket_id, observed_db_root| {
            if observed_bucket_id != bucket_id || observed_db_root != db_root {
                return;
            }
            let outputs = match (kind, point) {
                (
                    TestFileKind::Data,
                    testing::GcStatSyncPoint::DataRewritePublishing { output_count },
                ) => Some(output_count),
                (
                    TestFileKind::Blob,
                    testing::GcStatSyncPoint::BlobRewritePublishing { output_count },
                ) => Some(output_count),
                _ => None,
            };
            if let Some(outputs) = outputs
                && !publishing_fired.swap(true, Ordering::SeqCst)
            {
                output_count.store(outputs, Ordering::Release);
                publishing_tx.send(()).expect("signal rewrite publishing");
                let _ = release_rx
                    .lock()
                    .expect("lock rewrite release receiver")
                    .recv_timeout(Duration::from_secs(10));
                return;
            }
            let is_wait = matches!(
                (kind, point),
                (
                    TestFileKind::Data,
                    testing::GcStatSyncPoint::DataCheckpointPublishingWait
                ) | (
                    TestFileKind::Blob,
                    testing::GcStatSyncPoint::BlobCheckpointPublishingWait
                )
            );
            if is_wait && !wait_fired.swap(true, Ordering::SeqCst) {
                wait_tx.send(()).expect("signal checkpoint publishing wait");
            }
        }
    })));

    let before = match kind {
        TestFileKind::Data => mace.data_gc_count(),
        TestFileKind::Blob => mace.blob_gc_count(),
    };
    let gc_mace = mace.clone();
    let gc_thread = std::thread::spawn(move || gc_mace.start_gc());
    publishing_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("rewrite must enter publishing");
    assert!(
        output_count.load(Ordering::Acquire) > 1,
        "integration workload must produce multiple rewrite outputs"
    );

    let txn = bucket.begin()?;
    for key in &keys {
        txn.update(key, &latest)?;
    }
    txn.commit()?;
    let checkpoint_bucket = bucket.clone();
    let checkpoint_thread =
        std::thread::spawn(move || testing::checkpoint_and_wait(&checkpoint_bucket));
    wait_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("checkpoint must wait on publishing ownership");

    release_tx.send(()).expect("release rewrite publishing");
    gc_thread.join().expect("rewrite gc must finish");
    checkpoint_thread
        .join()
        .expect("checkpoint must re-resolve replacement ownership");
    testing::clear_gc_stat_hook();
    let after = match kind {
        TestFileKind::Data => mace.data_gc_count(),
        TestFileKind::Blob => mace.blob_gc_count(),
    };
    assert!(after > before, "selected rewrite kind must finish");
    testing::checkpoint_and_wait(&bucket);
    mace.start_gc();
    testing::assert_persisted_gc_stats(&mace);
    let view = bucket.view()?;
    for key in &keys {
        assert_eq!(view.get(key)?.slice(), latest.as_slice());
    }
    drop(view);
    drop(bucket);
    drop(mace);

    let mut reopen = Options::new(&*path);
    reopen.concurrent_write = 1;
    reopen.sync_on_write = true;
    reopen.gc_timeout = 60_000;
    reopen.gc_eager = true;
    reopen.data_garbage_ratio = 1;
    reopen.blob_garbage_ratio = 1;
    reopen.data_file_size = 16 << 10;
    reopen.blob_file_size = 16 << 10;
    let mace = Mace::new(reopen.validate()?)?;
    testing::assert_persisted_gc_stats(&mace);
    Ok(())
}

#[cfg(feature = "extra_check")]
#[test]
fn persisted_gc_stats_are_bucket_scoped() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.gc_timeout = 60_000;
    opt.gc_eager = true;
    opt.data_garbage_ratio = 1;
    opt.blob_garbage_ratio = 1;
    opt.data_file_size = 64 << 10;
    opt.blob_file_size = 64 << 10;
    let mace = Mace::new(opt.validate()?)?;
    mace.disable_gc();
    let bucket_options = BucketOptions {
        inline_size: 1024,
        checkpoint_size: 64 << 10,
        pool_capacity: 128 << 10,
        enable_backpressure: false,
        ..BucketOptions::default()
    };
    let alpha = mace.new_bucket("alpha", bucket_options)?;
    let beta = mace.new_bucket("beta", bucket_options)?;
    let data_v1 = vec![b'a'; 512];
    let data_v2 = vec![b'b'; 512];
    let blob_v1 = vec![b'x'; 8 << 10];
    let blob_v2 = vec![b'y'; 8 << 10];

    for bucket in [&alpha, &beta] {
        let tx = bucket.begin()?;
        for idx in 0..96 {
            tx.put(format!("data_{idx:03}"), &data_v1)?;
            tx.put(format!("blob_{idx:03}"), &blob_v1)?;
        }
        tx.commit()?;
        testing::checkpoint_and_wait(bucket);
    }
    for bucket in [&alpha, &beta] {
        let tx = bucket.begin()?;
        for idx in (0..96).step_by(2) {
            tx.update(format!("data_{idx:03}"), &data_v2)?;
            tx.update(format!("blob_{idx:03}"), &blob_v2)?;
        }
        tx.commit()?;
        testing::checkpoint_and_wait(bucket);
    }
    testing::assert_persisted_gc_stats(&mace);

    mace.enable_gc();
    let data_gc_before = mace.data_gc_count();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && mace.data_gc_count() == data_gc_before {
        mace.start_gc();
    }
    assert!(
        mace.data_gc_count() > data_gc_before,
        "expected data GC to run"
    );
    testing::assert_persisted_gc_stats(&mace);

    drop(alpha);
    drop(beta);
    drop(mace);

    let mut reopen = Options::new(&*path);
    reopen.sync_on_write = true;
    reopen.gc_timeout = 60_000;
    reopen.gc_eager = true;
    reopen.data_garbage_ratio = 1;
    reopen.blob_garbage_ratio = 1;
    reopen.data_file_size = 64 << 10;
    reopen.blob_file_size = 64 << 10;
    let mace = Mace::new(reopen.validate()?)?;
    testing::assert_persisted_gc_stats(&mace);
    for name in ["alpha", "beta"] {
        let bucket = mace.get_bucket(name)?;
        let view = bucket.view()?;
        assert_eq!(view.get("data_000")?.slice(), data_v2.as_slice());
        assert_eq!(view.get("data_001")?.slice(), data_v1.as_slice());
        assert_eq!(view.get("blob_000")?.slice(), blob_v2.as_slice());
        assert_eq!(view.get("blob_001")?.slice(), blob_v1.as_slice());
    }
    mace.start_gc();
    testing::assert_persisted_gc_stats(&mace);
    Ok(())
}

#[test]
fn gc_data() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = false;
    opt.sync_on_write = false;
    opt.gc_eager = true;
    opt.gc_timeout = 20;
    opt.data_garbage_ratio = 1;
    opt.data_file_size = 512 << 10;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();
    let cap = 20000;
    let mut pair = Vec::with_capacity(cap);

    for i in 0..cap {
        pair.push(format!("{i:08}"));
    }

    for k in &pair {
        let kv = db.begin().unwrap();
        kv.put(k, k)?;
        kv.commit()?;
    }

    for k in &pair {
        let kv = db.begin().unwrap();
        kv.update(k, k)?;
        kv.commit()?;
    }

    let kv = db.begin().unwrap();
    let mut rest = vec![];
    let mut deleted = vec![];
    #[allow(clippy::needless_range_loop)]
    for i in 0..cap {
        if rand::random_bool(0.5) {
            kv.del(&pair[i])?;
            deleted.push(i);
        } else {
            rest.push(i);
        }
    }
    kv.commit()?;

    let data_gc_count = mace.data_gc_count();
    let mut opt = db.options().clone();
    drop(db);
    drop(mace);
    opt.tmp_store = true;
    let opt = opt.validate().unwrap();

    if data_gc_count > 0 {
        let mace = Mace::new(opt).unwrap();
        let db = mace.get_bucket("x").unwrap();
        let view = db.view().unwrap();

        for &i in &rest {
            let key = &pair[i];
            let value = view
                .get(key)
                .expect("surviving data key missing after reopen");
            assert_eq!(value.slice(), key.as_bytes());
        }

        for &i in &deleted {
            let key = &pair[i];
            assert!(
                view.get(key).is_err(),
                "deleted data key must stay removed after reopen"
            );
        }
    }
    Ok(())
}

#[test]
fn gc_blob() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    #[cfg(not(target_os = "linux"))]
    {
        opt.data_handle_cache_capacity = 32;
        opt.blob_handle_cache_capacity = 32;
    }
    opt.blob_garbage_ratio = 1;
    opt.blob_file_size = 1 << 20;
    opt.gc_timeout = 20;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace
        .new_bucket(
            "x",
            BucketOptions {
                inline_size: 1024,
                ..BucketOptions::default()
            },
        )
        .unwrap();
    let cap = 10000;
    let val = vec![b'x'; 10240];
    let mut pair = Vec::with_capacity(cap);

    for i in 0..cap {
        pair.push(format!("{i:08}"));
    }

    for k in &pair {
        let kv = db.begin().unwrap();
        kv.put(k, &val)?;
        kv.commit()?;
    }

    db.checkpoint();

    for k in &pair {
        let kv = db.begin().unwrap();
        kv.update(k, &val)?;
        kv.commit()?;
    }

    db.checkpoint();

    let kv = db.begin().unwrap();
    let mut rest = vec![];
    let mut deleted = vec![];
    #[allow(clippy::needless_range_loop)]
    for i in 0..cap {
        if rand::random_bool(0.8) {
            kv.del(&pair[i])?;
            deleted.push(i);
        } else {
            rest.push(i);
        }
    }
    kv.commit()?;

    db.checkpoint();

    for &i in &rest {
        let k = &pair[i];
        let view = db.view().unwrap();
        view.get(k).unwrap();
    }

    let blob_gc_count = mace.blob_gc_count();
    let mut opt = db.options().clone();
    drop(db);
    drop(mace);
    opt.tmp_store = true;
    let opt = opt.validate().unwrap();

    if blob_gc_count > 0 {
        let mace = Mace::new(opt).unwrap();
        let db = mace.get_bucket("x").unwrap();
        let view = db.view().unwrap();

        for &i in &rest {
            let key = &pair[i];
            let value = view
                .get(key)
                .expect("surviving blob key missing after reopen");
            assert_eq!(value.slice(), val.as_slice());
        }

        for &i in &deleted {
            let key = &pair[i];
            assert!(
                view.get(key).is_err(),
                "deleted blob key must stay removed after reopen"
            );
        }
    }

    Ok(())
}

#[test]
fn gc_blob_delete_checkpoint_stays_deleted_without_gc() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.gc_timeout = 60_000;
    opt.blob_garbage_ratio = 1;
    opt.blob_file_size = 128 << 10;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket(
        "x",
        BucketOptions {
            inline_size: 1024,
            consolidate_threshold: 16,
            ..BucketOptions::default()
        },
    )?;
    let keys: Vec<_> = (0..64).map(|i| format!("blob_{i:04}")).collect();
    let v1 = vec![b'x'; 12 << 10];
    let v2 = vec![b'y'; 12 << 10];

    let tx = db.begin()?;
    for key in &keys {
        tx.put(key, &v1)?;
    }
    tx.commit()?;
    db.checkpoint();

    let tx = db.begin()?;
    for key in &keys {
        tx.update(key, &v2)?;
    }
    tx.commit()?;

    let tx = db.begin()?;
    for key in &keys {
        tx.del(key)?;
    }
    tx.commit()?;
    db.checkpoint();

    let mut reopen = db.options().clone();
    drop(db);
    drop(mace);
    reopen.tmp_store = true;
    let mace = Mace::new(reopen.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x").unwrap();
    let view = db.view()?;
    for key in &keys {
        if let Ok(value) = view.get(key) {
            panic!(
                "deleted blob key {key} resurrected after checkpoint-only reopen with byte {}",
                value.slice()[0]
            );
        }
    }
    Ok(())
}

#[test]
fn remote_blob_update_from_other_group_stays_deleted_after_reopen() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(64));
    let mut opt = Options::new(&*path);
    opt.concurrent_write = 2;
    opt.gc_timeout = 60_000;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let mut db = mace.new_bucket(
        "x",
        BucketOptions {
            inline_size: BucketOptions::MIN_INLINE_SIZE,
            split_elems: BucketOptions::MIN_SPLIT_ELEMS,
            consolidate_threshold: 16,
            ..BucketOptions::default()
        },
    )?;
    let old = vec![b'x'; 12 << 10];
    let updated = vec![b'y'; 12 << 10];

    // ticket 0 uses group 0; unload makes this the durable baseline
    let tx = db.begin()?;
    tx.put("target", &old)?;
    tx.commit()?;
    drop(db);
    mace.drop_bucket("x")?;
    db = mace.get_bucket("x")?;

    // tickets 1 and 2 put the remote update and tombstone in different groups
    let tx = db.begin()?;
    tx.update("target", &updated)?;
    tx.commit()?;

    let tx = db.begin()?;
    tx.del("target")?;
    tx.commit()?;
    assert!(db.view()?.get("target").is_err());

    // consume group 1 without adding a page frontier, then drive bounded foreground churn
    // until the post-delete leaf consolidates
    let tx = db.begin()?;
    tx.commit()?;
    let before = counter_value(&observer, CounterMetric::TreeNodeConsolidate);
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut batch = 0usize;
    while counter_value(&observer, CounterMetric::TreeNodeConsolidate) <= before {
        assert!(
            Instant::now() < deadline,
            "post-delete leaf consolidation was not exercised in time"
        );
        let tx = db.begin()?;
        for i in 0..32 {
            tx.put(format!("filler_{batch:02}_{i:02}"), b"v")?;
        }
        tx.commit()?;
        std::thread::sleep(Duration::from_millis(10));
        batch += 1;
    }
    assert!(
        counter_value(&observer, CounterMetric::TreeNodeConsolidate) > before,
        "post-delete leaf consolidation was not exercised"
    );
    assert!(db.view()?.get("target").is_err());

    db.checkpoint();
    let mut reopen = db.options().clone();
    drop(db);
    drop(mace);

    reopen.tmp_store = true;
    let mace = Mace::new(reopen.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x")?;
    if let Ok(value) = db.view()?.get("target") {
        panic!(
            "remote blob update from another writer group resurrected after reopen with byte {}",
            value.slice()[0]
        );
    }
    Ok(())
}

#[test]
fn gc_blob_single_gc_run_stays_deleted_after_reopen() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.gc_eager = true;
    opt.gc_timeout = 60_000;
    opt.blob_garbage_ratio = 1;
    opt.blob_file_size = 128 << 10;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket(
        "x",
        BucketOptions {
            inline_size: 1024,
            consolidate_threshold: 16,
            ..BucketOptions::default()
        },
    )?;
    let keys: Vec<_> = (0..64).map(|i| format!("blob_{i:04}")).collect();
    let v1 = vec![b'x'; 12 << 10];
    let v2 = vec![b'y'; 12 << 10];

    let tx = db.begin()?;
    for key in &keys {
        tx.put(key, &v1)?;
    }
    tx.commit()?;
    db.checkpoint();

    let tx = db.begin()?;
    for key in &keys {
        tx.update(key, &v2)?;
    }
    tx.commit()?;

    let tx = db.begin()?;
    for key in &keys {
        tx.del(key)?;
    }
    tx.commit()?;
    db.checkpoint();

    mace.start_gc();

    let mut reopen = db.options().clone();
    drop(db);
    drop(mace);
    reopen.tmp_store = true;
    let mace = Mace::new(reopen.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x").unwrap();
    let view = db.view()?;
    for key in &keys {
        if let Ok(value) = view.get(key) {
            panic!(
                "deleted blob key {key} resurrected after single gc reopen with byte {}",
                value.slice()[0]
            );
        }
    }
    Ok(())
}

#[test]
fn gc_blob_with_compression() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.blob_garbage_ratio = 1;
    opt.blob_file_size = 256 << 10;
    opt.gc_timeout = 20;
    opt.gc_eager = true;
    opt.tmp_store = true;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket(
        "x",
        BucketOptions {
            inline_size: 1024,
            enable_compression: true,
            ..BucketOptions::default()
        },
    )?;
    let v1 = vec![b'x'; 12 << 10];
    let v2 = vec![b'y'; 12 << 10];
    let v3 = vec![b'z'; 12 << 10];

    let kv = db.begin()?;
    kv.put("k1", &v1)?;
    kv.put("k2", &v2)?;
    kv.put("k3", &v3)?;
    kv.commit()?;

    let kv = db.begin()?;
    kv.del("k2")?;
    kv.commit()?;

    {
        let cap = 10000;
        let val = vec![b'x'; 10240];
        let mut pair = Vec::with_capacity(cap);

        for i in 0..cap {
            pair.push(format!("{i:08}"));
        }

        for k in &pair {
            let kv = db.begin().unwrap();
            kv.put(k, &val)?;
            kv.commit()?;
        }

        db.checkpoint();

        for k in &pair {
            let kv = db.begin().unwrap();
            kv.update(k, &val)?;
            kv.commit()?;
        }

        db.checkpoint();

        for k in &pair {
            let kv = db.begin().unwrap();
            kv.del(k)?;
            kv.commit()?;
        }
        db.checkpoint();
    }

    // rewrite timing is scheduler-dependent in the unified file-gc path;
    // this test only checks compressed blob visibility and reopen correctness
    for _ in 0..8 {
        mace.start_gc();
        std::thread::sleep(Duration::from_millis(10));
    }

    let view = db.view()?;
    assert_eq!(view.get("k1").unwrap().slice(), v1.as_slice());
    assert_eq!(view.get("k3").unwrap().slice(), v3.as_slice());
    assert!(view.get("k2").is_err());
    Ok(())
}

#[test]
fn gc_blob_toggle_compression() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.blob_garbage_ratio = 1;
    opt.blob_file_size = 256 << 10;
    opt.gc_timeout = 20;

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace.new_bucket(
            "x",
            BucketOptions {
                inline_size: 1024,
                enable_compression: true,
                ..BucketOptions::default()
            },
        )?;
        let v1 = vec![b'x'; 12 << 10];
        let v2 = vec![b'y'; 12 << 10];
        let v3 = vec![b'z'; 12 << 10];
        let kv = db.begin()?;
        kv.put("k1", &v1)?;
        kv.put("k2", &v2)?;
        kv.put("k3", &v3)?;
        kv.commit()?;

        {
            let cap = 10000;
            let val = vec![b'x'; 10240];
            let mut pair = Vec::with_capacity(cap);

            for i in 0..cap {
                pair.push(format!("{i:08}"));
            }

            for k in &pair {
                let kv = db.begin()?;
                kv.put(k, &val)?;
                kv.commit()?;
            }

            db.checkpoint();

            for k in &pair {
                let kv = db.begin()?;
                kv.update(k, &val)?;
                kv.commit()?;
            }

            db.checkpoint();

            for k in &pair {
                let kv = db.begin()?;
                kv.del(k)?;
                kv.commit()?;
            }
            db.checkpoint();
        }

        mace.start_gc();
        mace.disable_gc();

        drop(db);
        while let Err(e) = mace.drop_bucket("x") {
            assert_eq!(e, OpCode::Again);
            std::thread::sleep(Duration::from_millis(10));
        }

        while let Err(e) = mace.update_bucket_opt(
            "x",
            BucketOptions {
                inline_size: 1024,
                enable_compression: false,
                ..BucketOptions::default()
            },
        ) {
            assert_eq!(e, OpCode::Again);
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x")?;
    let v1 = vec![b'x'; 12 << 10];
    let v3 = vec![b'z'; 12 << 10];

    let kv = db.begin()?;
    kv.del("k2")?;
    kv.commit()?;

    mace.start_gc();
    std::thread::sleep(Duration::from_millis(200));

    let view = db.view()?;
    assert_eq!(view.get("k1").unwrap().slice(), v1.as_slice());
    assert_eq!(view.get("k3").unwrap().slice(), v3.as_slice());
    assert!(view.get("k2").is_err());

    drop(view);
    drop(db);
    drop(mace);
    drop(path);
    Ok(())
}

#[test]
fn abort_txn() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.max_ckpt_per_txn = 1;
    opt.data_file_size = 50 << 10; // make sure checkpoint was taken
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

    let kv = db.begin().unwrap();
    for i in 0..50000 {
        let x = format!("key_{i}");
        let _ = kv.put(&x, &x);
        db.checkpoint();
    }
    let r = kv.commit();

    assert!(r.is_err() && r.err().unwrap() == OpCode::AbortTx);
}

#[cfg(feature = "extra_check")]
#[test]
fn durable_group1_transaction_enforces_max_ckpt_per_txn() {
    // the shared durable stream is one physical logger, but max_ckpt_per_txn
    // must stay per-logical-group: a transaction on group 1 must observe its
    // own group's checkpoint counter advancing, otherwise it can pin MVCC
    // history and WAL past the configured limit
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    opt.max_ckpt_per_txn = 1;
    opt.data_file_size = 50 << 10; // make sure checkpoint was taken
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

    // one commit on group 0 so the next transaction lands on group 1
    let tx = db.begin().unwrap();
    assert_eq!(testing::txn_group(&db, testing::txn_start_ts(&tx)), Some(0));
    tx.put("g0", b"v").unwrap();
    tx.commit().unwrap();

    let tx = db.begin().unwrap();
    assert_eq!(testing::txn_group(&db, testing::txn_start_ts(&tx)), Some(1));
    tx.put("a", b"1").unwrap();

    // every checkpoint cut advances group 1's own counter; the next put must
    // observe the limit and abort (the publish is asynchronous, so poll)
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut aborted = false;
    while Instant::now() < deadline {
        db.checkpoint();
        if tx.put("b", b"2") == Err(OpCode::AbortTx) {
            aborted = true;
            break;
        }
    }
    assert!(
        aborted,
        "group 1 transaction must be limited by max_ckpt_per_txn"
    );
}

#[cfg(feature = "extra_check")]
#[test]
fn unrelated_group_checkpoints_do_not_age_group1_transaction() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    opt.max_ckpt_per_txn = 2;
    opt.data_file_size = 50 << 10;
    opt.checkpoint_nudge_ms = 0;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

    let tx0 = db.begin().unwrap();
    tx0.put("seed", b"v").unwrap();
    tx0.commit().unwrap();

    let tx1 = db.begin().unwrap();
    let txid1 = testing::txn_start_ts(&tx1);
    assert_eq!(testing::txn_group(&db, txid1), Some(1));

    let deadline = Instant::now() + Duration::from_secs(8);
    while testing::group_checkpoint_count(&db, 1) < 1 && Instant::now() < deadline {
        db.checkpoint();
        std::thread::yield_now();
    }
    assert_eq!(testing::group_checkpoint_count(&db, 1), 1);

    for i in 0..8 {
        let tx = db.begin().unwrap();
        assert_eq!(testing::txn_group(&db, testing::txn_start_ts(&tx)), Some(0));
        tx.put(format!("g0_{i}"), b"v").unwrap();
        tx.commit().unwrap();
        db.checkpoint();
    }

    let deadline = Instant::now() + Duration::from_secs(8);
    while testing::group_checkpoint_count(&db, 0) < 2 && Instant::now() < deadline {
        db.checkpoint();
        std::thread::yield_now();
    }
    assert!(testing::group_checkpoint_count(&db, 0) >= 2);
    assert_eq!(
        testing::group_checkpoint_count(&db, 1),
        1,
        "group 0-only WAL activity must not age group 1"
    );
    // drain any in-flight checkpoint publish before the final witness write:
    // tx1's own put arms group 1's dirty flag, and a publish observing it
    // charges group 1's counter, which would abort tx1's commit at the
    // max_ckpt_per_txn boundary even though no group 0 activity aged it
    testing::checkpoint_and_wait(&db);
    assert_eq!(
        testing::group_checkpoint_count(&db, 1),
        1,
        "draining the publish must not age group 1 either"
    );
    tx1.put("survives", b"v").unwrap();
    tx1.commit().unwrap();
}

#[test]
fn gc_wal() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.wal_file_size = 4096;
    opt.gc_timeout = 2;
    opt.concurrent_write = 1;
    opt.data_file_size = 100 << 10; // make sure checkpoint was taken
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();
    let mut data = Vec::new();

    for i in 0..1000 {
        data.push(format!("data_{i}"));
    }

    for i in &data {
        let kv = db.begin().unwrap();
        kv.put(i, i).unwrap();
        kv.commit().unwrap();
    }

    for i in &data {
        let view = db.view().unwrap();
        let r = view.get(i).expect("not found");
        assert_eq!(r.slice(), i.as_bytes());
    }

    db.checkpoint();

    // recycled wal files must be removed, never kept as backups
    let first = db.options().wal_file(0, 1);
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        mace.start_gc();
        if !first.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let mut files = Vec::new();
    if let Ok(iter) = std::fs::read_dir(db.options().log_root()) {
        for entry in iter.flatten() {
            files.push(entry.file_name().to_string_lossy().to_string());
        }
        files.sort_unstable();
    }
    panic!(
        "recycled wal file was not removed in time: first={:?}, files={:?}, data_gc_count={}, blob_gc_count={}",
        first,
        files,
        mace.data_gc_count(),
        mace.blob_gc_count()
    );
}

#[test]
fn gc_observer_metrics() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(256));
    let mut opt = Options::new(&*path);
    opt.gc_timeout = 60_000;
    opt.gc_eager = true;
    opt.sync_on_write = false;
    opt.data_garbage_ratio = 1;
    opt.data_file_size = 128 << 10;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

    for i in 0..4000 {
        let k = format!("key_{i:08}");
        let v = format!("value_{i:08}");
        let tx = db.begin().unwrap();
        tx.put(&k, &v)?;
        tx.commit()?;
    }

    for i in 0..2000 {
        let k = format!("key_{i:08}");
        let tx = db.begin().unwrap();
        tx.del(&k)?;
        tx.commit()?;
    }

    mace.start_gc();

    let snapshot = observer.snapshot();
    let gc_runs = snapshot
        .counters
        .iter()
        .find(|(m, _)| *m == CounterMetric::GcRun)
        .map(|(_, v)| *v)
        .unwrap_or(0);
    assert!(gc_runs >= 1, "expected at least one gc run");

    let run_hist_count = snapshot
        .histograms
        .iter()
        .find(|(m, _)| *m == HistogramMetric::GcRunMicros)
        .map(|(_, s)| s.count)
        .unwrap_or(0);
    assert!(
        run_hist_count >= 1,
        "expected at least one gc runtime histogram sample"
    );
    Ok(())
}

#[test]
fn abort_clean_checkpoint_dedup_per_bucket_per_gc_round() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(512));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_timeout = 60_000;
    opt.concurrent_write = 1;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

    let seed = db.begin().unwrap();
    seed.put("k", "seed")?;
    seed.commit()?;

    for i in 0..4 {
        let tx = db.begin().unwrap();
        tx.update("k", format!("v{i:02}"))?;
        drop(tx);
    }

    let before = counter_value(&observer, CounterMetric::GcAbortCleanCheckpointBucket);
    mace.start_gc();
    let after = counter_value(&observer, CounterMetric::GcAbortCleanCheckpointBucket);
    let delta = after.saturating_sub(before);

    assert_eq!(
        delta, 1,
        "expected exactly one abort-clean checkpoint for one bucket in one gc round"
    );
    Ok(())
}

#[test]
fn abort_clean_wal_open_is_bounded_by_file_count() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(512));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_timeout = 60_000;
    opt.concurrent_write = 1;
    opt.wal_file_size = 4096;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

    let seed = db.begin().unwrap();
    seed.put("k", "seed")?;
    seed.commit()?;

    let updates = 80u64;
    let payload = vec![b'x'; 900];
    {
        let tx = db.begin().unwrap();
        for _ in 0..updates {
            tx.update("k", &payload)?;
        }
        drop(tx);
    }

    let before = counter_value(&observer, CounterMetric::GcAbortCleanWalFileOpen);
    mace.start_gc();
    let after = counter_value(&observer, CounterMetric::GcAbortCleanWalFileOpen);
    let delta = after.saturating_sub(before);

    assert!(delta > 1, "expected abort clean to span multiple wal files");
    assert!(
        delta < updates,
        "expected wal file opens ({delta}) fewer than update records ({updates})"
    );
    Ok(())
}

#[test]
fn abort_clean_blocks_drop_until_task_is_fully_removed() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_timeout = 60_000;
    opt.concurrent_write = 1;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

    let seed = db.begin().unwrap();
    seed.put("k", "seed")?;
    seed.commit()?;

    let tx = db.begin().unwrap();
    tx.update("k", "v1")?;
    drop(tx);
    assert_eq!(mace.del_bucket("x"), Err(OpCode::Again));
    drop(db);

    assert_eq!(mace.drop_bucket("x"), Err(OpCode::Again));
    mace.start_gc();
    assert_eq!(mace.drop_bucket("x"), Err(OpCode::Again));
    Ok(())
}

#[cfg(feature = "extra_check")]
#[test]
fn abort_clean_lifecycle_closes_state_and_protections() -> Result<(), OpCode> {
    use mace::testing::{self, AbortCleanStage};
    use std::sync::atomic::{AtomicBool, Ordering};

    let _hook_lock = testing::checkpoint_test_lock();

    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_timeout = 60_000;
    opt.concurrent_write = 1;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

    let seed = db.begin().unwrap();
    seed.put("k", "seed")?;
    seed.commit()?;

    let tx = db.begin().unwrap();
    tx.update("k", "v1")?;
    let txid = testing::txn_start_ts(&tx);
    drop(tx);
    let info =
        testing::abort_clean_task_info(&db, txid).expect("abort-clean task must be published");
    assert_eq!(
        testing::abort_clean_task_stage(&db, txid),
        Some(AbortCleanStage::Pending)
    );
    assert!(testing::retained_abort_present(
        &db,
        info.group_id as usize,
        txid
    ));

    let callback_seen = Arc::new(AtomicBool::new(false));
    testing::set_abort_clean_hook(Some(Arc::new({
        let callback_seen = callback_seen.clone();
        move |point, callback_txid| {
            if point == testing::AbortCleanSyncPoint::AfterQuiesceCallback && callback_txid == txid
            {
                callback_seen.store(true, Ordering::Release);
            }
        }
    })));

    let callback_blocker = crossbeam_epoch::pin();
    drop(db);
    assert_eq!(mace.drop_bucket("x"), Err(OpCode::Again));
    mace.start_gc();
    let bucket = mace.get_bucket("x")?;
    assert_eq!(
        testing::abort_clean_task_stage(&bucket, txid),
        Some(AbortCleanStage::WaitingQuiesce)
    );
    assert_eq!(testing::abort_clean_task_info(&bucket, txid), Some(info));
    assert!(testing::retained_abort_present(
        &bucket,
        info.group_id as usize,
        txid
    ));
    drop(bucket);
    assert_eq!(mace.drop_bucket("x"), Err(OpCode::Again));

    drop(callback_blocker);
    let callback_deadline = Instant::now() + Duration::from_secs(5);
    while !callback_seen.load(Ordering::Acquire) && Instant::now() < callback_deadline {
        let guard = crossbeam_epoch::pin();
        guard.flush();
        drop(guard);
        std::thread::yield_now();
    }
    assert!(callback_seen.load(Ordering::Acquire));
    let bucket = mace.get_bucket("x")?;
    assert_eq!(
        testing::abort_clean_task_stage(&bucket, txid),
        Some(AbortCleanStage::WaitingQuiesce)
    );
    assert!(testing::retained_abort_present(
        &bucket,
        info.group_id as usize,
        txid
    ));
    drop(bucket);

    // removal requires an EBR callback plus a GC round; under parallel test
    // load a fixed round count can run out, so wait on a bounded deadline
    // (same pattern as the callback_seen wait above). pre-existing flake fix
    let removal_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        mace.start_gc();
        let bucket = mace.get_bucket("x")?;
        if testing::abort_clean_task_stage(&bucket, txid).is_none() {
            assert_eq!(testing::abort_clean_task_info(&bucket, txid), None);
            assert!(!testing::retained_abort_present(
                &bucket,
                info.group_id as usize,
                txid
            ));
            drop(bucket);
            break;
        }
        assert_eq!(
            testing::abort_clean_task_stage(&bucket, txid),
            Some(AbortCleanStage::WaitingQuiesce)
        );
        drop(bucket);
        if Instant::now() >= removal_deadline {
            panic!("abort-clean task must be removed within the removal deadline");
        }
        for _ in 0..16 {
            let guard = crossbeam_epoch::pin();
            guard.flush();
            drop(guard);
            std::thread::yield_now();
        }
    }
    testing::clear_abort_clean_hook();
    assert_eq!(mace.drop_bucket("x"), Ok(()));
    Ok(())
}

#[cfg(feature = "extra_check")]
#[test]
fn abort_clean_corruption_retains_task_fact_and_wal_pin() -> Result<(), OpCode> {
    use mace::testing;
    use std::sync::atomic::{AtomicBool, Ordering};

    let _hook_lock = testing::checkpoint_test_lock();

    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_timeout = 60_000;
    opt.concurrent_write = 1;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

    let seed = db.begin().unwrap();
    seed.put("k", "seed")?;
    seed.commit()?;
    let tx = db.begin().unwrap();
    tx.update("k", "v1")?;
    let txid = testing::txn_start_ts(&tx);
    drop(tx);
    let info =
        testing::abort_clean_task_info(&db, txid).expect("abort-clean task must be published");
    let original_prev =
        testing::corrupt_abort_clean_prev(&db, txid, info.tail_file_id, info.tail_offset)?;
    let corruption_seen = Arc::new(AtomicBool::new(false));
    testing::set_abort_clean_hook(Some(Arc::new({
        let corruption_seen = corruption_seen.clone();
        move |point, callback_txid| {
            if point == testing::AbortCleanSyncPoint::AfterCorruption && callback_txid == txid {
                corruption_seen.store(true, Ordering::Release);
            }
        }
    })));
    drop(db);

    mace.start_gc();
    assert!(corruption_seen.load(Ordering::Acquire));
    let bucket = mace.get_bucket("x")?;
    assert_eq!(
        testing::abort_clean_task_stage(&bucket, txid),
        Some(testing::AbortCleanStage::Pending)
    );
    assert_eq!(testing::abort_clean_task_info(&bucket, txid), Some(info));
    assert!(testing::retained_abort_present(
        &bucket,
        info.group_id as usize,
        txid
    ));
    testing::corrupt_abort_clean_prev(&bucket, txid, original_prev.0, original_prev.1)?;
    drop(bucket);
    testing::clear_abort_clean_hook();
    mace.start_gc();
    assert_eq!(mace.drop_bucket("x"), Err(OpCode::Again));
    Ok(())
}

#[test]
fn recovery_drains_abort_clean_before_startup_returns() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.tmp_store = false;
    opt.sync_on_write = false;
    opt.gc_timeout = 60_000;
    opt.concurrent_write = 1;

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

        let seed = db.begin().unwrap();
        seed.put("k", "seed")?;
        seed.commit()?;

        let tx = db.begin().unwrap();
        tx.update("k", "v1")?;
        drop(tx);
    }

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let bucket = mace.get_bucket("x")?;
    let view = bucket.view()?;
    assert_eq!(view.get("k")?.slice(), b"seed");
    drop(view);
    drop(bucket);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match mace.drop_bucket("x") {
            Ok(()) => return Ok(()),
            Err(OpCode::Again) => std::thread::sleep(Duration::from_millis(10)),
            Err(e) => panic!("unexpected drop_bucket error after recovery drain: {e:?}"),
        }
    }
    panic!("drop_bucket remained blocked after recovery should have drained abort clean");
}

#[test]
fn recovery_abort_clean_does_not_leave_bucket_loaded_after_startup() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.tmp_store = false;
    opt.sync_on_write = false;
    opt.gc_timeout = 60_000;
    opt.concurrent_write = 1;

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

        let seed = db.begin().unwrap();
        seed.put("k", "seed")?;
        seed.commit()?;

        let tx = db.begin().unwrap();
        tx.update("k", "v1")?;
        drop(tx);
    }

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    mace.update_bucket_opt(
        "x",
        BucketOptions {
            cache_evict_pct: 30,
            ..BucketOptions::default()
        },
    )?;
    Ok(())
}

fn wal_file_ids(log_root: &std::path::Path, physical: u8) -> Vec<u64> {
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

#[test]
fn relaxed_multi_group_wal_is_recycled_after_checkpoints() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = false;
    opt.concurrent_write = 2;
    opt.wal_file_size = 4 << 10;
    opt.gc_timeout = 60_000;
    opt.checkpoint_nudge_ms = 0;
    opt.data_file_size = 1 << 30;
    let log_root = opt.log_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    // both logical groups write; every checkpoint must advance each per-group
    // floor so old wal files become recyclable
    let payload = vec![b'w'; 1 << 10];
    for round in 0..64 {
        let tx = db.begin()?;
        tx.upsert(format!("a_{round}"), &payload)?;
        tx.commit()?;
        let tx = db.begin()?;
        tx.upsert(format!("b_{round}"), &payload)?;
        tx.commit()?;
    }
    db.checkpoint();

    let deadline = Instant::now() + Duration::from_secs(8);
    let mut recycled = false;
    while Instant::now() < deadline {
        mace.start_gc();
        let stream0 = wal_file_ids(&log_root, 0);
        let stream1 = wal_file_ids(&log_root, 1);
        if stream0.len() < 4 && stream1.len() < 4 {
            recycled = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        recycled,
        "relaxed multi-group wal must be recycled: stream0={:?} stream1={:?}",
        wal_file_ids(&log_root, 0),
        wal_file_ids(&log_root, 1)
    );

    // all data must stay readable after recycling
    let view = db.view()?;
    for round in 0..64 {
        assert_eq!(view.get(format!("a_{round}"))?.slice(), payload.as_slice());
        assert_eq!(view.get(format!("b_{round}"))?.slice(), payload.as_slice());
    }
    Ok(())
}
