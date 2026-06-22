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
    #[allow(clippy::needless_range_loop)]
    for i in 0..cap {
        if rand::random_bool(0.5) {
            kv.del(&pair[i])?;
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
        let mut count = 0;
        let mut max_id = 0;
        let dir = std::fs::read_dir(opt.data_root()).unwrap();
        for d in dir {
            let x = d.unwrap();
            let f = x.file_name();
            let name = f.to_str().unwrap();
            if name.starts_with(Options::DATA_PREFIX) {
                let v: Vec<&str> = name.split(Options::SEP).collect();
                let id = v[1].parse::<u32>().expect("invalid number");
                count += 1;
                max_id = max_id.max(id);
            }
        }
        assert!(count < max_id);
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
    opt.blob_gc_ratio = 20;
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
    #[allow(clippy::needless_range_loop)]
    for i in 0..cap {
        if rand::random_bool(0.8) {
            kv.del(&pair[i])?;
        } else {
            rest.push(i);
        }
    }
    kv.commit()?;

    db.checkpoint();

    for i in rest {
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
        let mut count = 0;
        let mut max_id = 0;
        let dir = std::fs::read_dir(opt.data_root()).unwrap();
        for d in dir {
            let x = d.unwrap();
            let f = x.file_name();
            let name = f.to_str().unwrap();
            if name.starts_with(Options::BLOB_PREFIX) {
                let v: Vec<&str> = name.split(Options::SEP).collect();
                let id = v[1].parse::<u32>().expect("invalid number");
                count += 1;
                max_id = max_id.max(id);
            }
        }
        assert!(count < max_id);
    }

    Ok(())
}

#[test]
fn gc_blob_with_compression() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.blob_garbage_ratio = 1;
    opt.blob_gc_ratio = 100;
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

    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        mace.start_gc();
        if mace.blob_gc_count() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        mace.blob_gc_count() > 0,
        "blob gc rewrite did not complete in time"
    );

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
    opt.blob_gc_ratio = 100;
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

    let scanned_pages = snapshot
        .counters
        .iter()
        .find(|(m, _)| *m == CounterMetric::GcScavengePageScan)
        .map(|(_, v)| *v)
        .unwrap_or(0);
    assert!(
        scanned_pages > 0,
        "expected gc scavenge scan counter to be positive"
    );

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
fn vacuum_bucket_blocks_delete() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_timeout = 1000;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    mace.disable_gc();
    let db = mace
        .new_bucket(
            "x",
            BucketOptions {
                split_elems: 64,
                consolidate_threshold: 2,
                ..BucketOptions::default()
            },
        )
        .unwrap();
    let cap = 30000;
    let mut keys = Vec::with_capacity(cap);

    for i in 0..cap {
        keys.push(format!("{i:08}"));
    }

    let kv = db.begin().unwrap();
    for k in &keys {
        kv.put(k, k)?;
    }
    kv.commit()?;

    for _ in 0..3 {
        let kv = db.begin().unwrap();
        for k in &keys {
            kv.update(k, k)?;
        }
        kv.commit()?;
    }

    drop(db);

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let mace_vac = mace.clone();
    let handle = std::thread::spawn(move || {
        let res = mace_vac.vacuum_bucket("x");
        let _ = done_tx.send(());
        res
    });

    let start_wait = Instant::now();
    let mut started = false;
    while start_wait.elapsed() < Duration::from_millis(2000) {
        if mace.is_bucket_vacuuming("x")? {
            started = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(started, "vacuum did not enter inflight state");

    let mut blocked = false;
    let mut deleted = false;
    let mut last_err = None;
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(2000) {
        if done_rx.try_recv().is_ok() {
            break;
        }
        match mace.del_bucket("x") {
            Err(OpCode::Again) => {
                blocked = true;
                break;
            }
            Ok(()) => {
                deleted = true;
                break;
            }
            Err(e) => last_err = Some(e),
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    if deleted {
        panic!("bucket deletion succeeded while vacuum was running");
    }
    if let Some(e) = last_err {
        panic!("unexpected delete error {e}");
    }
    assert!(blocked, "bucket deletion was not blocked by vacuum");

    let stats = handle.join().unwrap()?;
    assert!(stats.scanned > 0);

    assert!(mace.del_bucket("x").is_ok());
    Ok(())
}

#[test]
fn vacuum_bucket_effect() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_timeout = 1000;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    mace.disable_gc();
    let db = mace
        .new_bucket(
            "x",
            BucketOptions {
                split_elems: 64,
                consolidate_threshold: 2,
                ..BucketOptions::default()
            },
        )
        .unwrap();
    let cap = 20000;
    let mut keys = Vec::with_capacity(cap);

    for i in 0..cap {
        keys.push(format!("{i:08}"));
    }

    let kv = db.begin().unwrap();
    for k in &keys {
        kv.put(k, k)?;
    }
    kv.commit()?;

    let view = db.view().unwrap();
    for _ in 0..3 {
        let kv = db.begin().unwrap();
        for k in &keys {
            kv.update(k, k)?;
        }
        kv.commit()?;
    }
    let kv = db.begin().unwrap();
    for (i, k) in keys.iter().enumerate() {
        if i % 3 == 0 {
            kv.del(k)?;
        }
    }
    kv.commit()?;
    drop(view);

    let stats = mace.vacuum_bucket("x")?;
    assert!(stats.scanned > 0);
    let view = db.view().unwrap();
    for k in keys.iter().step_by(3) {
        assert!(view.get(k).is_err());
    }
    for (i, k) in keys.iter().enumerate() {
        if i % 3 != 0 {
            assert_eq!(view.get(k).unwrap().slice(), k.as_bytes());
        }
    }
    Ok(())
}

#[test]
fn vacuum_meta_effect() -> Result<(), OpCode> {
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

    let stats = mace.vacuum_meta()?;
    assert!(stats.moved_pages > 0);
    Ok(())
}
