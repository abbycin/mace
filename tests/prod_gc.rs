mod common;

use common::{TestEnv, wait_until};
use mace::observe::{CounterMetric, InMemoryObserver};
#[cfg(feature = "extra_check")]
use mace::testing;
use mace::{Bucket, BucketOptions, OpCode, Options};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn counter_value(observer: &InMemoryObserver, metric: CounterMetric) -> u64 {
    observer
        .snapshot()
        .counters
        .iter()
        .find(|(current, _)| *current == metric)
        .map(|(_, value)| *value)
        .unwrap_or(0)
}

fn drive_foreground_compaction(
    bucket: &Bucket,
    payload: &[u8],
    observer: &InMemoryObserver,
) -> Result<(), OpCode> {
    let before = counter_value(observer, CounterMetric::TreeNodeConsolidate);
    for round in 0..3 {
        let txn = bucket.begin()?;
        if round == 0 {
            txn.put("compaction_probe", payload)?;
        }
        for _ in 0..32 {
            txn.update("compaction_probe", payload)?;
        }
        txn.commit()?;
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(
        counter_value(observer, CounterMetric::TreeNodeConsolidate) >= before + 3,
        "expected repeated foreground consolidation"
    );
    Ok(())
}

fn prefixed_files(root: &Path, prefix: &str) -> Vec<String> {
    std::fs::read_dir(root)
        .expect("list data root failed")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with(prefix))
        .collect()
}

fn prefixed_file_sizes(root: &Path, prefix: &str) -> Vec<(u64, u64)> {
    let mut files = std::fs::read_dir(root)
        .expect("list data root failed")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            let id = name.strip_prefix(prefix)?.strip_prefix('_')?.parse().ok()?;
            let size = entry.metadata().ok()?.len();
            Some((id, size))
        })
        .collect::<Vec<_>>();
    files.sort_unstable_by_key(|(id, _)| *id);
    files
}

fn assert_rewrite_outputs_near_target(files: &[(u64, u64)], target: u64) {
    assert!(
        files.len() > 1,
        "oversized victim must produce multiple outputs"
    );
    assert!(
        files.iter().any(|(_, size)| *size >= target / 2),
        "at least one rewrite output should be packed near the target: {files:?}"
    );
    assert!(
        files.iter().all(|(_, size)| *size <= target * 2),
        "rewrite output exceeded the target by more than one normal record: {files:?}"
    );
}

#[test]
fn oversized_data_rewrite_splits_outputs_and_reopens() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let source_target = 256 << 10;
    let rewrite_target = 16 << 10;
    let keys = (0..512)
        .map(|idx| format!("data_{idx:04}"))
        .collect::<Vec<_>>();
    let initial = vec![b'a'; 128];
    let updated = vec![b'b'; 128];

    let engine = env
        .open_with(|options| {
            options.concurrent_write = 1;
            options.sync_on_write = true;
            options.gc_timeout = 60_000;
            options.gc_eager = false;
            options.data_garbage_ratio = 100;
            options.data_file_size = source_target;
        })
        .expect("open oversized data source");
    engine.disable_gc();
    let bucket = engine
        .new_bucket(
            "oversized_data",
            BucketOptions {
                split_elems: 64,
                consolidate_threshold: 16,
                checkpoint_size: source_target,
                pool_capacity: source_target * 2,
                enable_backpressure: false,
                ..BucketOptions::default()
            },
        )
        .expect("create oversized data bucket");
    for key in &keys {
        let txn = bucket.begin()?;
        txn.put(key, &initial)?;
        txn.commit()?;
    }
    bucket.checkpoint();
    let data_root = bucket.options().data_root();
    assert!(
        wait_until(Duration::from_secs(6), Duration::from_millis(20), || {
            !prefixed_file_sizes(&data_root, Options::DATA_PREFIX).is_empty()
        }),
        "expected initial data checkpoint"
    );
    for key in keys.iter().step_by(2) {
        let txn = bucket.begin()?;
        txn.upsert(key, &updated)?;
        txn.commit()?;
    }
    drop(bucket);
    drop(engine);

    let engine = env
        .open_with(|options| {
            options.concurrent_write = 1;
            options.sync_on_write = true;
            options.gc_timeout = 60_000;
            options.gc_eager = true;
            options.data_garbage_ratio = 1;
            options.data_file_size = rewrite_target;
        })
        .expect("reopen oversized data for rewrite");
    let bucket = engine
        .get_bucket("oversized_data")
        .expect("load oversized data bucket for rewrite");
    let before = prefixed_file_sizes(&data_root, Options::DATA_PREFIX);
    let before_max = before.iter().map(|(id, _)| *id).max().unwrap_or(0);
    assert!(
        before
            .iter()
            .any(|(_, size)| *size > rewrite_target as u64 * 2),
        "fixture must contain an oversized data victim: {before:?}"
    );
    assert!(
        wait_until(Duration::from_secs(10), Duration::from_millis(20), || {
            engine.start_gc();
            engine.data_gc_count() > 0
        }),
        "expected oversized data rewrite"
    );
    let outputs = prefixed_file_sizes(&data_root, Options::DATA_PREFIX)
        .into_iter()
        .filter(|(id, _)| *id > before_max)
        .collect::<Vec<_>>();
    assert_rewrite_outputs_near_target(&outputs, rewrite_target as u64);
    #[cfg(feature = "extra_check")]
    testing::assert_persisted_gc_stats(&engine);
    drop(bucket);
    drop(engine);

    let engine = env
        .open_with(|options| {
            options.concurrent_write = 1;
            options.sync_on_write = true;
            options.data_file_size = rewrite_target;
        })
        .expect("reopen rewritten data");
    let bucket = engine
        .get_bucket("oversized_data")
        .expect("load rewritten data bucket");
    #[cfg(feature = "extra_check")]
    testing::assert_persisted_gc_stats(&engine);
    let view = bucket.view()?;
    for (idx, key) in keys.iter().enumerate() {
        let expected = if idx % 2 == 0 { &updated } else { &initial };
        assert_eq!(view.get(key)?.slice(), expected.as_slice());
    }
    Ok(())
}

#[test]
fn oversized_blob_rewrite_splits_outputs_and_reopens() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let observer = Arc::new(InMemoryObserver::new(64));
    let source_target = 2 << 20;
    let rewrite_target = 128 << 10;
    let keys = (0..128)
        .map(|idx| format!("blob_{idx:04}"))
        .collect::<Vec<_>>();
    let initial = vec![b'x'; 16 << 10];
    let updated = vec![b'y'; 16 << 10];

    let engine = env.open_with(|options| {
        options.concurrent_write = 1;
        options.sync_on_write = true;
        options.gc_timeout = 60_000;
        options.gc_eager = false;
        options.blob_garbage_ratio = 100;
        options.blob_file_size = source_target;
        options.observer = observer.clone();
    })?;
    engine.disable_gc();
    let bucket = engine.new_bucket(
        "oversized_blob",
        BucketOptions {
            inline_size: 512,
            consolidate_threshold: 8,
            checkpoint_size: 256 << 10,
            pool_capacity: 512 << 10,
            enable_backpressure: false,
            ..BucketOptions::default()
        },
    )?;
    let txn = bucket.begin()?;
    for key in &keys {
        txn.put(key, &initial)?;
    }
    txn.commit()?;
    bucket.checkpoint();
    let data_root = bucket.options().data_root();
    assert!(
        wait_until(Duration::from_secs(6), Duration::from_millis(20), || {
            !prefixed_file_sizes(&data_root, Options::BLOB_PREFIX).is_empty()
        }),
        "expected initial blob checkpoint"
    );
    for key in keys.iter().step_by(2) {
        let txn = bucket.begin()?;
        txn.upsert(key, &updated)?;
        txn.commit()?;
    }
    drive_foreground_compaction(&bucket, &updated, &observer)?;
    bucket.checkpoint();
    std::thread::sleep(Duration::from_millis(100));
    drop(bucket);
    drop(engine);

    let engine = env.open_with(|options| {
        options.concurrent_write = 1;
        options.sync_on_write = true;
        options.gc_timeout = 60_000;
        options.gc_eager = true;
        options.blob_garbage_ratio = 1;
        options.blob_file_size = rewrite_target;
    })?;
    let bucket = engine.get_bucket("oversized_blob")?;
    let before = prefixed_file_sizes(&data_root, Options::BLOB_PREFIX);
    let before_max = before.iter().map(|(id, _)| *id).max().unwrap_or(0);
    assert!(
        before
            .iter()
            .any(|(_, size)| *size > rewrite_target as u64 * 2),
        "fixture must contain an oversized blob victim: {before:?}"
    );
    assert!(
        wait_until(Duration::from_secs(10), Duration::from_millis(20), || {
            engine.start_gc();
            engine.blob_gc_count() > 0
        }),
        "expected oversized blob rewrite"
    );
    let outputs = prefixed_file_sizes(&data_root, Options::BLOB_PREFIX)
        .into_iter()
        .filter(|(id, _)| *id > before_max)
        .collect::<Vec<_>>();
    assert_rewrite_outputs_near_target(&outputs, rewrite_target as u64);
    #[cfg(feature = "extra_check")]
    testing::assert_persisted_gc_stats(&engine);
    drop(bucket);
    drop(engine);

    let engine = env.open_with(|options| {
        options.concurrent_write = 1;
        options.sync_on_write = true;
        options.blob_file_size = rewrite_target;
    })?;
    let bucket = engine.get_bucket("oversized_blob")?;
    #[cfg(feature = "extra_check")]
    testing::assert_persisted_gc_stats(&engine);
    let view = bucket.view()?;
    for (idx, key) in keys.iter().enumerate() {
        let expected = if idx % 2 == 0 { &updated } else { &initial };
        assert_eq!(view.get(key)?.slice(), expected.as_slice());
    }
    Ok(())
}

#[test]
fn fast_manual_data_cycle() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
        options.gc_eager = true;
        options.gc_timeout = 60_000;
        options.data_garbage_ratio = 1;
        options.data_file_size = 16 << 10;
    })?;

    let bucket = engine.new_bucket(
        "prod_gc_data",
        BucketOptions {
            checkpoint_size: 32 << 10,
            pool_capacity: 64 << 10,
            ..BucketOptions::default()
        },
    )?;
    let seed_payload = vec![b's'; 1024];
    let updated_payload = vec![b'u'; 1024];

    let mut keys = Vec::new();
    for index in 0..512 {
        keys.push(format!("k_{index:04}"));
    }

    for key in &keys {
        let txn = bucket.begin()?;
        txn.put(key, &seed_payload)?;
        txn.commit()?;
    }

    for key in &keys {
        let txn = bucket.begin()?;
        txn.update(key, &updated_payload)?;
        txn.commit()?;
    }

    bucket.checkpoint();

    let gc_done = wait_until(Duration::from_secs(6), Duration::from_millis(50), || {
        engine.start_gc();
        engine.data_gc_count() > 0
    });

    assert!(gc_done, "expected at least one data gc cycle");

    let view = bucket.view()?;
    for key in &keys {
        let value = view.get(key)?;
        assert_eq!(value.slice(), updated_payload.as_slice());
    }

    let mut count = 0usize;
    for entry in std::fs::read_dir(bucket.options().data_root()).expect("list data root failed") {
        let entry = entry.expect("read dir entry failed");
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };

        if !name.starts_with(Options::DATA_PREFIX) {
            continue;
        }

        count += 1;
    }

    assert!(count > 0, "expected at least one data file");

    Ok(())
}

#[test]
#[ignore]
fn stress_blob_cycle() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let observer = Arc::new(InMemoryObserver::new(64));
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
        options.gc_eager = true;
        options.gc_timeout = 60_000;
        options.blob_garbage_ratio = 1;
        options.blob_file_size = 1 << 20;
        options.observer = observer.clone();
    })?;

    let bucket = engine.new_bucket(
        "prod_gc_blob",
        BucketOptions {
            inline_size: 512,
            consolidate_threshold: 16,
            ..BucketOptions::default()
        },
    )?;
    let payload = vec![b'x'; 16 << 10];

    for index in 0..300 {
        let key = format!("blob_{index:04}");
        let txn = bucket.begin()?;
        txn.put(&key, &payload)?;
        txn.commit()?;
    }
    bucket.checkpoint();

    let blob_root = bucket.options().data_root();
    let files_ready = wait_until(Duration::from_secs(6), Duration::from_millis(50), || {
        !prefixed_files(&blob_root, Options::BLOB_PREFIX).is_empty()
    });
    assert!(files_ready, "expected blob files after checkpoint");

    for index in 0..300 {
        let key = format!("blob_{index:04}");
        let txn = bucket.begin()?;
        txn.del(&key)?;
        txn.commit()?;
    }

    drive_foreground_compaction(&bucket, &payload, &observer)?;
    bucket.checkpoint();

    let before_gc = prefixed_files(&blob_root, Options::BLOB_PREFIX);
    assert!(!before_gc.is_empty(), "expected blob files before gc");
    let gc_done = wait_until(Duration::from_secs(20), Duration::from_millis(100), || {
        engine.start_gc();
        let after_gc = prefixed_files(&blob_root, Options::BLOB_PREFIX);
        engine.blob_gc_count() > 0 || after_gc.len() < before_gc.len() || after_gc.is_empty()
    });

    assert!(
        gc_done,
        "expected blob gc rewrite and file reclaim to happen"
    );
    Ok(())
}
