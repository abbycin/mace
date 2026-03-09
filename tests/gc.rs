use mace::observe::{CounterMetric, HistogramMetric, InMemoryObserver};
use mace::{Mace, OpCode, Options, RandomPath};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    opt.gc_compacted_size = opt.data_file_size;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
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

    drop(db);
    drop(mace);

    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x").unwrap();

    for i in rest {
        let k = &pair[i];
        let view = db.view().unwrap();
        view.get(k).unwrap();
    }

    let mut count = 0;
    let mut max_id = 0;
    let dir = std::fs::read_dir(db.options().data_root()).unwrap();
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
    opt.blob_max_size = 1 << 20;
    opt.gc_timeout = 20;
    opt.inline_size = 1024;
    opt.max_log_size = 20480;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
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

    for k in &pair {
        let kv = db.begin().unwrap();
        kv.update(k, &val)?;
        kv.commit()?;
    }

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

    for i in rest {
        let k = &pair[i];
        let view = db.view().unwrap();
        view.get(k).unwrap();
    }

    let mut opt = db.options().clone();
    if mace.blob_gc_count() > 0 {
        drop(db);
        drop(mace);
        opt.tmp_store = true;
        let opt = opt.validate().unwrap();
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
fn abort_txn() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.max_ckpt_per_txn = 1;
    opt.data_file_size = 50 << 10; // make sure checkpoint was taken
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();

    let kv = db.begin().unwrap();
    for i in 0..50000 {
        let x = format!("key_{i}");
        let _ = kv.put(&x, &x);
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
    opt.max_log_size = 1024;
    opt.keep_stable_wal_file = true;
    opt.data_file_size = 100 << 10; // make sure checkpoint was taken
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
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
    opt.gc_compacted_size = opt.data_file_size;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();

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

fn counter(snapshot: &mace::observe::ObserveSnapshot, metric: CounterMetric) -> u64 {
    snapshot
        .counters
        .iter()
        .find(|(m, _)| *m == metric)
        .map(|(_, v)| *v)
        .unwrap_or(0)
}

#[test]
fn retire_pending_metrics_progress() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let observer = Arc::new(InMemoryObserver::new(256));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_eager = true;
    opt.gc_timeout = 20;
    opt.data_garbage_ratio = 1;
    opt.data_file_size = 128 << 10;
    opt.gc_compacted_size = opt.data_file_size;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();

    for i in 0..3000 {
        let k = format!("k_{i:08}");
        let v = format!("v_{i:08}");
        let tx = db.begin().unwrap();
        tx.put(&k, &v)?;
        tx.commit()?;
    }

    for i in 0..3000 {
        let k = format!("k_{i:08}");
        let tx = db.begin().unwrap();
        tx.update(&k, &k)?;
        tx.commit()?;
    }

    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        mace.start_gc();
        let snapshot = observer.snapshot();
        let recorded = counter(&snapshot, CounterMetric::RetireRecorded);
        let applied = counter(&snapshot, CounterMetric::RetireDataApplied);
        let cleared = counter(&snapshot, CounterMetric::RetireCleared);
        if recorded > 0 && applied > 0 && cleared > 0 {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    let snapshot = observer.snapshot();
    panic!(
        "retire metrics did not progress in time, recorded={}, applied={}, cleared={}",
        counter(&snapshot, CounterMetric::RetireRecorded),
        counter(&snapshot, CounterMetric::RetireDataApplied),
        counter(&snapshot, CounterMetric::RetireCleared)
    );
}

#[test]
fn pending_retire_preserved_across_unload() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let observer = Arc::new(InMemoryObserver::new(256));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_eager = false;
    opt.gc_timeout = 60_000;
    opt.data_garbage_ratio = 1;
    opt.data_file_size = 64 << 10;
    opt.gc_compacted_size = opt.data_file_size;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();

    for i in 0..2000 {
        let k = format!("u_{i:08}");
        let tx = db.begin().unwrap();
        tx.put(&k, &k)?;
        tx.commit()?;
    }

    for _ in 0..3 {
        for i in 0..2000 {
            let k = format!("u_{i:08}");
            let tx = db.begin().unwrap();
            tx.update(&k, &k)?;
            tx.commit()?;
        }
    }

    drop(db);
    mace.drop_bucket("x")?;

    let snap0 = observer.snapshot();
    let recorded0 = counter(&snap0, CounterMetric::RetireRecorded);
    assert!(
        recorded0 > 0,
        "expected retire to be recorded before unload"
    );
    let applied0 = counter(&snap0, CounterMetric::RetireDataApplied)
        + counter(&snap0, CounterMetric::RetireBlobApplied);
    let cleared0 = counter(&snap0, CounterMetric::RetireCleared);

    mace.start_gc();

    let snap1 = observer.snapshot();
    let applied1 = counter(&snap1, CounterMetric::RetireDataApplied)
        + counter(&snap1, CounterMetric::RetireBlobApplied);
    let cleared1 = counter(&snap1, CounterMetric::RetireCleared);
    assert_eq!(
        applied1, applied0,
        "unloaded bucket should not apply pending retire"
    );
    assert_eq!(
        cleared1, cleared0,
        "unloaded bucket should not clear pending retire"
    );

    let reopened = mace.get_bucket("x")?;
    let view = reopened.view()?;
    let _ = view.get("u_00000000");
    drop(view);
    drop(reopened);

    mace.start_gc();
    let snap2 = observer.snapshot();
    let applied2 = counter(&snap2, CounterMetric::RetireDataApplied)
        + counter(&snap2, CounterMetric::RetireBlobApplied);
    let cleared2 = counter(&snap2, CounterMetric::RetireCleared);
    assert!(
        applied2 > applied1,
        "reloaded bucket should replay pending retire"
    );
    assert!(
        cleared2 > cleared1,
        "reloaded bucket should clear replayed pending retire"
    );
    Ok(())
}

#[test]
fn vacuum_bucket_blocks_delete() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_timeout = 1000;
    opt.split_elems = 64;
    opt.consolidate_threshold = 2;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    mace.disable_gc();
    let db = mace.new_bucket("x").unwrap();
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
    opt.split_elems = 64;
    opt.consolidate_threshold = 2;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    mace.disable_gc();
    let db = mace.new_bucket("x").unwrap();
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
    assert!(stats.compacted > 0);
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
        let db = mace.new_bucket(&name).unwrap();
        let kv = db.begin().unwrap();
        kv.put("k", "v")?;
        kv.commit()?;
    }

    let stats = mace.vacuum_meta()?;
    assert!(stats.moved_pages > 0);
    Ok(())
}
