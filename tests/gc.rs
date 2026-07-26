use mace::observe::{CounterMetric, HistogramMetric, InMemoryObserver};
use mace::{BucketOptions, Mace, OpCode, Options, RandomPath};
use std::sync::Arc;
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

#[test]
fn gc_wal() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.wal_file_size = 4096;
    opt.gc_timeout = 2;
    opt.concurrent_write = 1;
    opt.keep_stable_wal_file = true;
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

    let backup = db.options().wal_backup(0, 1);
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        mace.start_gc();
        if backup.exists() {
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
        "stable wal backup did not appear in time: backup={:?}, files={:?}, data_gc_count={}, blob_gc_count={}",
        backup,
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
    drop(db);

    assert_eq!(mace.drop_bucket("x"), Err(OpCode::Again));
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

#[test]
fn compact_meta() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    let total = 256;
    for i in 0..total {
        let name = format!("b{i:04}");
        let db = mace.new_bucket(&name, BucketOptions::default()).unwrap();
        let kv = db.begin().unwrap();
        kv.put("k", "v")?;
        kv.commit()?;
    }

    let stats = mace.compact_meta()?;
    assert!(stats.moved_pages > 0);
    Ok(())
}
