mod common;

use btree_store::BTree;
use mace::{BucketOptions, Mace, OpCode, Options, PersistedBucketOptions, RandomPath};
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[repr(C)]
#[derive(Clone, Copy)]
struct PersistedBucketMeta {
    id: u64,
    options: PersistedBucketOptions,
}

fn load_persisted_bucket_options(opt: &Options, name: &str) -> PersistedBucketOptions {
    let tree = BTree::open(opt.manifest()).unwrap();
    let mut raw = None;
    tree.view("bucket_metas", |txn| {
        raw = Some(txn.get(name.as_bytes())?);
        Ok(())
    })
    .unwrap();
    let raw = raw.unwrap();
    let meta = unsafe { std::ptr::read_unaligned(raw.as_ptr().cast::<PersistedBucketMeta>()) };
    meta.options
}

fn load_persisted_global_options(opt: &Options) -> Value {
    let tree = BTree::open(opt.manifest()).unwrap();
    let mut raw = None;
    tree.view("misc", |txn| {
        raw = Some(txn.get("options")?);
        Ok(())
    })
    .unwrap();
    serde_json::from_slice(&raw.unwrap()).unwrap()
}

fn assert_runtime_only_fields_absent(value: &Value) {
    let obj = value.as_object().expect("options json must be an object");
    for key in ["db_root", "log_root", "tmp_store", "observer", "fs"] {
        assert!(
            !obj.contains_key(key),
            "runtime-only field {key} should not be persisted"
        );
    }
}

#[test]
fn bucket_concurrency_non_blocking() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    mace.new_bucket("stable", BucketOptions::default()).unwrap();

    let mace_clone = mace.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();

    let t1 = std::thread::spawn(move || {
        let mut count = 0;
        while !stop_clone.load(Ordering::Relaxed) {
            let name = format!("temp_{}", count);
            let b = mace_clone
                .new_bucket(&name, BucketOptions::default())
                .unwrap();
            drop(b); // ensure count drops
            let _ = mace_clone.del_bucket(&name);
            count += 1;
        }
    });

    let start = std::time::Instant::now();
    let mut iterations = 0;
    while iterations < 100_000 {
        mace.open_bucket("stable").unwrap();
        iterations += 1;

        // if it's blocked, it might take a long time to finish 100k iterations
        // on a modern machine, lock-free DashMap::get should easily do this in < 1s
        if start.elapsed() > std::time::Duration::from_secs(5) {
            break;
        }
    }

    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    t1.join().unwrap();

    // assert that we completed the iterations within a reasonable time
    assert!(
        iterations == 100_000,
        "Lookups were likely blocked, only completed {} in {:?}",
        iterations,
        elapsed
    );
}

#[test]
fn global_options_bootstrap_writes_manifest_json() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
    drop(mace);

    let value = load_persisted_global_options(&opt);
    assert_eq!(value["concurrent_write"], 16);
    assert_eq!(value["sync_on_write"], true);
    assert_eq!(value["gc_timeout"], 60_000);
    assert_runtime_only_fields_absent(&value);
}

#[test]
fn global_options_bootstrap_recovers_visible_uninitialized_manifest() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    opt.clone().validate().unwrap();

    let tree = BTree::open(opt.manifest()).unwrap();
    drop(tree);

    let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
    drop(mace);

    let value = load_persisted_global_options(&opt);
    assert_eq!(value["concurrent_write"], 16);
    assert_eq!(value["sync_on_write"], true);
    assert_runtime_only_fields_absent(&value);
}

#[test]
fn global_options_reopen_updates_mutable_fields() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.concurrent_write = 8;
    opt.sync_on_write = false;
    opt.gc_timeout = 1234;
    let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
    drop(mace);

    let mut reopen = Options::new(&*path);
    reopen.concurrent_write = 8;
    reopen.sync_on_write = true;
    reopen.gc_timeout = 7777;
    let mace = Mace::new(reopen.clone().validate().unwrap()).unwrap();
    drop(mace);

    let value = load_persisted_global_options(&reopen);
    assert_eq!(value["concurrent_write"], 8);
    assert_eq!(value["sync_on_write"], true);
    assert_eq!(value["gc_timeout"], 7777);
    assert_runtime_only_fields_absent(&value);
}

#[test]
fn global_options_missing_record_is_corruption() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
    drop(mace);

    let tree = BTree::open(opt.manifest()).unwrap();
    tree.exec("misc", |txn| txn.del("options")).unwrap();

    let err = Mace::new(opt.validate().unwrap()).err().unwrap();
    assert_eq!(err, OpCode::Corruption);
}

#[test]
fn global_options_bad_json_is_corruption() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
    drop(mace);

    let tree = BTree::open(opt.manifest()).unwrap();
    tree.exec("misc", |txn| txn.put("options", b"{")).unwrap();

    let err = Mace::new(opt.validate().unwrap()).err().unwrap();
    assert_eq!(err, OpCode::Corruption);
}

/// file ids of one per-group WAL stream, ascending
fn wal_file_ids(log_root: &std::path::Path, physical: u8) -> Vec<u64> {
    let prefix = format!("wal_{physical}_");
    let mut ids: Vec<u64> = std::fs::read_dir(log_root)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy().into_owned();
            name.strip_prefix(&prefix)
                .and_then(|id| id.parse::<u64>().ok())
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// file ids of the shared durable WAL stream, ascending
fn group_wal_file_ids(log_root: &std::path::Path) -> Vec<u64> {
    let prefix = "group_wal_";
    let mut ids: Vec<u64> = std::fs::read_dir(log_root)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy().into_owned();
            name.strip_prefix(prefix)
                .and_then(|id| id.parse::<u64>().ok())
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn global_options_resize_migrates_the_epoch_and_writes_options_back() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    common::deterministic_gc(&mut opt);
    opt.concurrent_write = 8;
    opt.sync_on_write = false;
    let log_root = opt.log_root();
    let mace = Mace::new(opt.validate()?)?;
    let bucket = mace.new_bucket("resize", BucketOptions::default())?;
    // one transaction per key spreads versions across all eight writer groups, so the later
    // shrink leaves record groups beyond the runtime width for the settled-history path
    for i in 0..64u32 {
        let tx = bucket.begin()?;
        tx.put(format!("k{i}"), format!("v{i}"))?;
        tx.commit()?;
    }
    drop(bucket);
    drop(mace);

    let all_stream_ids = |log_root: &std::path::Path| -> Vec<u64> {
        (0..8u8)
            .flat_map(|physical| wal_file_ids(log_root, physical))
            .chain(group_wal_file_ids(log_root))
            .collect()
    };
    let first_era_max = *all_stream_ids(&log_root)
        .iter()
        .max()
        .expect("the first open must own WAL files");

    // shrink across a route switch: the reopen scans the persisted width, retires every
    // pre-migration stream and hands a two-group durable runtime Context, which must answer for
    // record groups the old writers owned
    let mut shrink = Options::new(&*path);
    common::deterministic_gc(&mut shrink);
    shrink.concurrent_write = 2;
    shrink.sync_on_write = true;
    let mace = Mace::new(shrink.clone().validate()?)?;
    assert_eq!(mace.options().concurrent_write, 2);
    let bucket = mace.open_bucket("resize")?;
    let view = bucket.view()?;
    for i in 0..64u32 {
        assert_eq!(
            view.get(format!("k{i}"))?.slice(),
            format!("v{i}").as_bytes()
        );
    }
    drop(view);
    let tx = bucket.begin()?;
    tx.put("after_shrink", b"x")?;
    tx.commit()?;
    drop(bucket);
    drop(mace);

    for id in all_stream_ids(&log_root) {
        assert!(
            id > first_era_max,
            "file id {id} survives the wipe: the migration must retire every pre-migration stream"
        );
    }
    assert!(
        !group_wal_file_ids(&log_root).is_empty(),
        "a durable target must own a shared era file"
    );
    let value = load_persisted_global_options(&shrink);
    assert_eq!(
        value["concurrent_write"], 2,
        "a completed migration records the target width"
    );

    // grow across the route switch back: every target-only stream starts a fresh era above the
    // retired namespace instead of reusing an old file id
    let second_era_max = *all_stream_ids(&log_root)
        .iter()
        .max()
        .expect("the shrink must leave era files");
    let mut grow = Options::new(&*path);
    common::deterministic_gc(&mut grow);
    grow.concurrent_write = 8;
    grow.sync_on_write = false;
    let mace = Mace::new(grow.clone().validate()?)?;
    assert_eq!(mace.options().concurrent_write, 8);
    let bucket = mace.open_bucket("resize")?;
    let view = bucket.view()?;
    for i in 0..64u32 {
        assert_eq!(
            view.get(format!("k{i}"))?.slice(),
            format!("v{i}").as_bytes()
        );
    }
    assert_eq!(view.get("after_shrink")?.slice(), b"x");
    drop(view);
    let tx = bucket.begin()?;
    tx.put("after_grow", b"x")?;
    tx.commit()?;
    drop(bucket);
    drop(mace);

    for id in all_stream_ids(&log_root) {
        assert!(
            id > second_era_max,
            "file id {id} must start above the retired namespace"
        );
    }
    for physical in 0..8u8 {
        assert!(
            !wal_file_ids(&log_root, physical).is_empty(),
            "a relaxed target must give stream {physical} its own era file"
        );
    }
    let value = load_persisted_global_options(&grow);
    assert_eq!(value["concurrent_write"], 8);
    Ok(())
}

#[test]
fn bucket_limit_safety() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    common::deterministic_gc(&mut opt);
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    let mut buckets = Vec::new();
    for i in 0..10 {
        let name = format!("limit_test_{}", i);
        buckets.push(mace.new_bucket(&name, BucketOptions::default()).unwrap());
    }

    let initial_count = mace.nr_buckets();
    assert!(initial_count >= 10);

    for i in 0..10 {
        let name = format!("limit_test_{}", i);
        // explicitly drop the handle to ensure strong_count decreases
        buckets.clear();

        loop {
            match mace.del_bucket(&name) {
                Ok(_) => break,
                Err(OpCode::Again) => mace.start_gc(),
                Err(e) => panic!("Delete failed for {}: {:?}", name, e),
            }
        }
    }

    // counter should still be same because gc hasn't run
    let after_del_count = mace.nr_buckets();
    assert_eq!(
        initial_count, after_del_count,
        "Counter should include pending-delete buckets"
    );

    // drive explicit rounds; each is a synchronous engine point
    let released = common::gc_rounds_until(&mace, 8, || mace.nr_buckets() < after_del_count);
    assert!(
        released,
        "engine did not release pending buckets within 8 explicit gc rounds"
    );
}

#[test]
fn bucket_simple() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    let b1 = mace
        .new_bucket("bucket1", BucketOptions::default())
        .unwrap();

    {
        let tx = b1.begin().unwrap();
        tx.put("key1", "val1").unwrap();
        tx.commit().unwrap();

        let view = b1.view().unwrap();
        let val = view.get("key1").unwrap();
        assert_eq!(val.slice(), b"val1");
    }

    {
        let d = mace
            .new_bucket("default", BucketOptions::default())
            .unwrap();
        let tx = d.begin().unwrap();
        tx.put("key1", "default_val").unwrap();
        tx.commit().unwrap();
    }

    let tx = b1.begin().unwrap();
    let val_b1 = tx.get("key1").unwrap();
    assert_eq!(val_b1.slice(), b"val1");

    let d = mace.open_bucket("default").unwrap();
    let view = d.view().unwrap();
    let val_def = view.get("key1").unwrap();
    assert_eq!(val_def.slice(), b"default_val");

    tx.del("key1").unwrap();
    assert!(tx.get("key1").is_err());

    let val_def = view.get("key1").unwrap();
    assert_eq!(val_def.slice(), b"default_val");
}

#[test]
fn bucket_new_get_semantics() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    assert_eq!(mace.open_bucket("missing").err(), Some(OpCode::NotFound));

    let b1 = mace.new_bucket("x", BucketOptions::default()).unwrap();
    assert_eq!(
        mace.new_bucket("x", BucketOptions::default()).err(),
        Some(OpCode::Exist)
    );

    drop(b1);
    mace.drop_bucket("x")?;
    assert_eq!(
        mace.new_bucket("x", BucketOptions::default()).err(),
        Some(OpCode::Exist)
    );

    let b1 = mace.open_bucket("x")?;
    drop(b1);

    mace.del_bucket("x")?;
    let _b2 = mace.new_bucket("x", BucketOptions::default())?;

    Ok(())
}

#[test]
fn bucket_persistence() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace.new_bucket("x", BucketOptions::default()).unwrap();
        let b1 = db.begin().unwrap();
        b1.put("k", "v").unwrap();
        b1.commit().unwrap();
    }

    {
        let mace = Mace::new(opt.validate().unwrap()).unwrap();
        let db = mace.open_bucket("x").unwrap();
        let b1 = db.view().unwrap();
        let val = b1.get("k").unwrap();
        assert_eq!(val.slice(), b"v");
    }
}

/// get_bucket_options reads the persisted subset from metadata without
/// loading the bucket context; the runtime operator is never persisted
#[test]
fn get_bucket_options_reads_persisted_subset_without_loading() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let bucket_opt = BucketOptions {
        split_elems: 128,
        consolidate_threshold: 16,
        inline_size: 2048,
        ..BucketOptions::default()
    };
    let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
    assert!(mace.get_bucket_options("missing")?.is_none());

    let bucket = mace.new_bucket("x", bucket_opt.clone())?;
    drop(bucket);
    let read = mace.get_bucket_options("x")?.expect("bucket must exist");
    assert_eq!(read.split_elems, 128);
    assert_eq!(read.consolidate_threshold, 16);
    assert_eq!(read.inline_size, 2048);
    assert!(
        read.merge_operator.is_none(),
        "the runtime operator is never persisted"
    );
    Ok(())
}

#[test]
fn bucket_update_opt_persists_compatible_changes() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let bucket_opt = BucketOptions {
        split_elems: 128,
        consolidate_threshold: 16,
        inline_size: 2048,
        checkpoint_size: 64 << 10,
        pool_capacity: 128 << 10,
        cache_capacity: 256 << 10,
        cache_evict_pct: 20,
        enable_backpressure: true,
        ..BucketOptions::default()
    };
    let updated = BucketOptions {
        checkpoint_size: 32 << 10,
        pool_capacity: 64 << 10,
        cache_capacity: 128 << 10,
        cache_evict_pct: 40,
        enable_backpressure: false,
        ..bucket_opt.clone()
    };

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let bucket = mace.new_bucket("x", bucket_opt.clone()).unwrap();
        assert_eq!(
            mace.update_bucket_opt("x", updated.clone()),
            Err(OpCode::Again)
        );
        drop(bucket);
        mace.drop_bucket("x")?;
        mace.update_bucket_opt("x", updated.clone())?;
    }

    assert_eq!(
        load_persisted_bucket_options(&opt, "x"),
        PersistedBucketOptions::from(&updated)
    );

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let bucket = mace.open_bucket("x")?;
        drop(bucket);
        mace.drop_bucket("x")?;
        let conflict = BucketOptions {
            split_elems: 64,
            ..updated.clone()
        };
        assert_eq!(mace.update_bucket_opt("x", conflict), Err(OpCode::Invalid));
        let increase = BucketOptions {
            inline_size: updated.inline_size + 1,
            ..updated.clone()
        };
        assert_eq!(mace.update_bucket_opt("x", increase), Err(OpCode::Invalid));
        mace.update_bucket_opt("x", bucket_opt.clone())?;
    }

    assert_eq!(
        load_persisted_bucket_options(&opt, "x"),
        PersistedBucketOptions::from(&bucket_opt)
    );

    {
        let mace = Mace::new(opt.validate().unwrap()).unwrap();
        let conflict = BucketOptions {
            split_elems: 64,
            ..bucket_opt.clone()
        };
        assert_eq!(mace.update_bucket_opt("x", conflict), Err(OpCode::Invalid));
    }

    Ok(())
}

#[test]
fn bucket_update_opt_persists_inline_size_change() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let bucket_opt = BucketOptions {
        inline_size: 4096,
        checkpoint_size: 64 << 10,
        pool_capacity: 128 << 10,
        ..BucketOptions::default()
    };
    // an explicit in-window change persists; a default-constructed request
    // (inline_size = 1024 = the new default) is an equally explicit change
    // when the stored threshold differs, and must not be treated as a no-op
    // or rejected as a conflict
    let changed = BucketOptions {
        inline_size: 2048,
        ..bucket_opt.clone()
    };
    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let bucket = mace.new_bucket("x", bucket_opt.clone())?;
        drop(bucket);
        mace.drop_bucket("x")?;
        mace.update_bucket_opt("x", changed.clone())?;
        let read = mace.get_bucket_options("x")?.expect("bucket must exist");
        assert_eq!(read.inline_size, 2048);
        assert_eq!(read.pool_capacity, 128 << 10);
        // and again down to the current default
        mace.update_bucket_opt(
            "x",
            BucketOptions {
                inline_size: BucketOptions::MIN_INLINE_SIZE,
                ..changed.clone()
            },
        )?;
        let read = mace.get_bucket_options("x")?.expect("bucket must exist");
        assert_eq!(read.inline_size, BucketOptions::MIN_INLINE_SIZE);
    }
    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let read = mace.get_bucket_options("x")?.expect("bucket must exist");
        assert_eq!(read.inline_size, BucketOptions::MIN_INLINE_SIZE);
        mace.open_bucket("x")?; // loads with the persisted threshold
    }
    Ok(())
}

#[test]
fn bucket_update_inline_size_keeps_existing_data_readable() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let bucket_opt = BucketOptions {
        inline_size: 4096,
        split_elems: 32,
        checkpoint_size: 64 << 10,
        pool_capacity: 128 << 10,
        ..BucketOptions::default()
    };
    let small = vec![b's'; 500]; // inline before and after
    let mid = vec![b'm'; 3000]; // inline at 4096, blob-sized at 1024
    let big = vec![b'z'; 6000]; // blob before and after
    let n = 48usize;
    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace.new_bucket("x", bucket_opt.clone())?;
        let kv = db.begin()?;
        for i in 0..n {
            let k = format!("k{i:04}");
            kv.put(k.as_bytes(), &mid)?;
            kv.put(format!("{k}.s").as_bytes(), &small)?;
        }
        kv.put("big".as_bytes(), &big)?;
        kv.commit()?;
        db.checkpoint();
        // lower the threshold on the unloaded bucket
        drop(db);
        while let Err(e) = mace.drop_bucket("x") {
            assert_eq!(e, OpCode::Again);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        mace.update_bucket_opt(
            "x",
            BucketOptions {
                inline_size: BucketOptions::MIN_INLINE_SIZE,
                ..bucket_opt.clone()
            },
        )?;
    }
    // reopen and rewrite: gc must re-encode every live record under the new
    // threshold (the 3000B inline values become blob-backed) without losing
    // or corrupting data
    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace.open_bucket("x")?;
        let kv = db.begin()?;
        for i in 0..n {
            let k = format!("k{i:04}");
            kv.update(k.as_bytes(), &mid)?;
            kv.update(format!("{k}.s").as_bytes(), &small)?;
        }
        kv.commit()?;
        db.checkpoint();
        let before = mace.data_gc_count();
        let ran = common::gc_rounds_until_with_settle(
            &mace,
            40,
            std::time::Duration::from_millis(120),
            || mace.data_gc_count() > before,
        );
        assert!(ran, "expected a data gc rewrite round");
        let view = db.view()?;
        for i in 0..n {
            let k = format!("k{i:04}");
            assert_eq!(view.get(&k).unwrap().slice(), mid.as_slice());
            assert_eq!(
                view.get(format!("{k}.s").as_bytes()).unwrap().slice(),
                small.as_slice()
            );
        }
        assert_eq!(view.get("big".as_bytes()).unwrap().slice(), big.as_slice());
    }
    // reopen once more: the split-converted leaves and their materialized
    // blob boxes were flushed by checkpoints; everything must read back from
    // files through the interval maps
    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace.open_bucket("x")?;
        let view = db.view()?;
        for i in 0..n {
            let k = format!("k{i:04}");
            assert_eq!(view.get(&k).unwrap().slice(), mid.as_slice());
            assert_eq!(
                view.get(format!("{k}.s").as_bytes()).unwrap().slice(),
                small.as_slice()
            );
        }
        assert_eq!(view.get("big".as_bytes()).unwrap().slice(), big.as_slice());
    }
    Ok(())
}

#[test]
fn multiple_buckets() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    let b1 = mace.new_bucket("b1", BucketOptions::default()).unwrap();
    let b2 = mace.new_bucket("b2", BucketOptions::default()).unwrap();

    let tx1 = b1.begin().unwrap();
    let tx2 = b2.begin().unwrap();
    tx1.put("key", "val1").unwrap();
    tx2.put("key", "val2").unwrap();
    tx1.commit().unwrap();
    tx2.commit().unwrap();

    let v1 = b1.view().unwrap();
    let v2 = b2.view().unwrap();

    assert_eq!(v1.get("key").unwrap().slice(), b"val1");
    assert_eq!(v2.get("key").unwrap().slice(), b"val2");
}

#[test]
fn bucket_deletion_safety() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

    let tx1 = db.begin().unwrap();
    tx1.put("k", "v").unwrap();
    tx1.commit().unwrap();

    assert_eq!(mace.del_bucket("x"), Err(OpCode::Again));

    drop(db);

    mace.del_bucket("x").unwrap();
    assert_eq!(mace.del_bucket("x"), Err(OpCode::NotFound));
}

#[test]
fn bucket_deletion_cleanup() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.data_file_size = 1024;
    common::deterministic_gc(&mut opt);
    let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();
    let b1 = db.begin().unwrap();
    let val = vec![0u8; 512];
    for i in 0..10 {
        b1.put(format!("k{}", i), &val).unwrap();
    }
    b1.commit().unwrap();
    drop(db);

    while mace.del_bucket("x") == Err(OpCode::Again) {
        // drive the pending-delete cleanup synchronously instead of sleeping
        mace.start_gc();
    }
    drop(mace);

    let db = Mace::new(opt.validate().unwrap()).unwrap();
    assert!(db.del_bucket("x").is_err());
}

#[test]
fn bucket_physical_cleanup_basic() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    let b1 = mace.new_bucket("b1", BucketOptions::default()).unwrap();
    let uid = b1.id();
    let tx = b1.begin().unwrap();
    tx.put("key", "val").unwrap();
    tx.commit().unwrap();
    drop(b1);

    mace.del_bucket("b1").unwrap();

    mace.start_gc();

    let b2 = mace.new_bucket("b1", BucketOptions::default()).unwrap();
    assert_ne!(b2.id(), uid);
}

#[test]
fn bucket_physical_cleanup_recovery() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);

    let bucket_id;

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let bkt = mace.new_bucket("b1", BucketOptions::default()).unwrap();
        bucket_id = bkt.id();
        let b1 = bkt.begin().unwrap();
        b1.put("k", "v").unwrap();
        b1.commit().unwrap();
        drop(bkt);
        mace.del_bucket("b1").unwrap();
        drop(mace);
    }

    {
        let mace = Mace::new(opt.validate().unwrap()).unwrap();
        mace.start_gc();

        assert!(mace.del_bucket("b1").is_err());
        let bkt = mace.new_bucket("b1", BucketOptions::default()).unwrap();
        assert_ne!(bkt.id(), bucket_id);
    }
}

#[test]
fn bucket_physical_file_cleanup() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.data_file_size = 1024;
    common::deterministic_gc(&mut opt);

    let mut b1_files = Vec::new();
    let initial_files: std::collections::HashSet<std::ffi::OsString>;

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();

        initial_files = std::fs::read_dir(opt.data_root())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();

        let db = mace.new_bucket("b1", BucketOptions::default()).unwrap();
        let big_val = vec![0u8; 2048];
        for i in 0..10 {
            let tx = db.begin().unwrap();
            tx.put(format!("k{}", i), &big_val).unwrap();
            tx.commit().unwrap();
        }
        let tx = db.begin().unwrap();
        tx.put("blob", vec![1u8; 100 * 1024]).unwrap();
        tx.commit().unwrap();
        mace.start_gc();

        drop(db);
        drop(mace);
    }

    for entry in std::fs::read_dir(opt.data_root()).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if !initial_files.contains(&name) {
            b1_files.push(name.into_string().unwrap());
        }
    }

    assert!(!b1_files.is_empty(), "bucket b1 should have produced files");

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        mace.del_bucket("b1").unwrap();

        let released = common::gc_rounds_until(&mace, 8, || {
            b1_files
                .iter()
                .all(|name| !opt.data_root().join(name).exists())
        });
        assert!(
            released,
            "engine did not delete files of b1 within 8 explicit gc rounds: {:?}",
            b1_files
        );
    }
}

#[test]
fn test_drop_bucket_safety() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path).validate()?;
    let db = Mace::new(opt)?;

    let bucket_name = "safety_test";

    // 1. test: cannot drop while bucket handle is held
    {
        let bucket = db.new_bucket(bucket_name, BucketOptions::default())?;
        // should fail because 'bucket' holds an Arc<BucketMeta>
        let res = db.drop_bucket(bucket_name);
        assert_eq!(res.err(), Some(OpCode::Again));
        drop(bucket);
    }
    // now it should succeed
    db.drop_bucket(bucket_name).unwrap();

    // 2. test: cannot drop while transaction is active
    {
        let bucket = db.open_bucket(bucket_name)?;
        let tx = bucket.begin()?;

        let res = db.drop_bucket(bucket_name);
        assert_eq!(res.err(), Some(OpCode::Again));

        tx.commit()?;
    }

    // after commit and dropping everything, it should succeed
    db.drop_bucket(bucket_name).unwrap();

    Ok(())
}

#[test]
fn test_drop_bucket_persistence() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path).validate()?;
    let db = Mace::new(opt)?;

    let bucket_name = "persist_test";
    {
        let bucket = db.new_bucket(bucket_name, BucketOptions::default())?;
        let kv = bucket.begin()?;
        kv.put("k", "v")?;
        kv.commit()?;
    }

    db.drop_bucket(bucket_name)?;

    // re-open and verify
    let bucket = db.open_bucket(bucket_name)?;
    let view = bucket.view()?;
    assert_eq!(view.get("k")?.slice(), b"v");

    Ok(())
}
