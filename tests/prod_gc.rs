mod common;

use common::{TestEnv, wait_until};
use mace::observe::{CounterMetric, InMemoryObserver};
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
            txn.put("blob_0000", payload)?;
        }
        for _ in 0..32 {
            txn.update("blob_0000", payload)?;
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
