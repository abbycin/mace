mod common;

use common::{TestEnv, wait_until};
use mace::{OpCode, Options};
use std::path::Path;
use std::time::Duration;

fn count_prefixed_files(root: &Path, prefix: &str) -> usize {
    std::fs::read_dir(root)
        .expect("list data root failed")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with(prefix))
        .count()
}

fn max_prefixed_file_id(root: &Path, prefix: &str, sep: &str) -> u64 {
    std::fs::read_dir(root)
        .expect("list data root failed")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with(prefix))
        .filter_map(|name| {
            let mut parts = name.split(sep);
            let _ = parts.next();
            parts.next()?.parse::<u64>().ok()
        })
        .max()
        .unwrap_or(0)
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
        options.gc_compacted_size = options.data_file_size;
    })?;

    let bucket = engine.new_bucket("prod_gc_data")?;

    let mut keys = Vec::new();
    for index in 0..512 {
        keys.push(format!("k_{index:04}"));
    }

    for key in &keys {
        let txn = bucket.begin()?;
        txn.put(key, "seed_data_payload")?;
        txn.commit()?;
    }

    for key in &keys {
        let txn = bucket.begin()?;
        let updated = format!("updated_{key}");
        txn.update(key, updated.as_bytes())?;
        txn.commit()?;
    }

    let gc_done = wait_until(Duration::from_secs(6), Duration::from_millis(50), || {
        engine.start_gc();
        engine.data_gc_count() > 0
    });

    assert!(gc_done, "expected at least one data gc cycle");

    let view = bucket.view()?;
    for key in &keys {
        let expected = format!("updated_{key}");
        assert_eq!(view.get(key)?.slice(), expected.as_bytes());
    }

    let mut count = 0u32;
    let mut max_id = 0u32;
    for entry in std::fs::read_dir(bucket.options().data_root()).expect("list data root failed") {
        let entry = entry.expect("read dir entry failed");
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };

        if !name.starts_with(Options::DATA_PREFIX) {
            continue;
        }

        let mut parts = name.split(Options::SEP);
        let _ = parts.next();
        let Some(raw_id) = parts.next() else {
            continue;
        };

        let Ok(id) = raw_id.parse::<u32>() else {
            continue;
        };

        max_id = max_id.max(id);
        count += 1;
    }

    assert!(count > 0, "expected at least one data file");
    assert!(max_id >= count, "expected sparse file id layout after gc");

    Ok(())
}

#[test]
#[ignore]
fn stress_blob_cycle() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
        options.gc_eager = true;
        options.gc_timeout = 60_000;
        options.inline_size = 512;
        options.max_log_size = 1 << 20;
        options.blob_garbage_ratio = 1;
        options.blob_gc_ratio = 100;
        options.blob_max_size = 1 << 20;
    })?;

    let bucket = engine.new_bucket("prod_gc_blob")?;
    let payload = vec![b'x'; 16 << 10];

    for index in 0..300 {
        let key = format!("blob_{index:04}");
        let txn = bucket.begin()?;
        txn.put(&key, &payload)?;
        txn.commit()?;
    }

    for index in 0..300 {
        let key = format!("blob_{index:04}");
        let txn = bucket.begin()?;
        txn.del(&key)?;
        txn.commit()?;
    }

    let blob_root = bucket.options().data_root();
    let gc_done = wait_until(Duration::from_secs(20), Duration::from_millis(100), || {
        engine.start_gc();
        let blob_count = count_prefixed_files(&blob_root, Options::BLOB_PREFIX);
        let blob_max_id = max_prefixed_file_id(&blob_root, Options::BLOB_PREFIX, Options::SEP);
        engine.blob_gc_count() > 0 || blob_max_id == 0 || blob_count < (blob_max_id + 1) as usize
    });

    assert!(
        gc_done,
        "expected blob gc rewrite and file reclaim to happen"
    );
    Ok(())
}
