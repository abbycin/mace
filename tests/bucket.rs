use mace::{Mace, OpCode, Options, RandomPath};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[test]
fn bucket_concurrency_non_blocking() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    mace.new_bucket("stable").unwrap();

    let mace_clone = mace.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();

    let t1 = std::thread::spawn(move || {
        let mut count = 0;
        while !stop_clone.load(Ordering::Relaxed) {
            let name = format!("temp_{}", count);
            let b = mace_clone.new_bucket(&name).unwrap();
            drop(b); // ensure count drops
            let _ = mace_clone.del_bucket(&name);
            count += 1;
        }
    });

    let start = std::time::Instant::now();
    let mut iterations = 0;
    while iterations < 100_000 {
        mace.get_bucket("stable").unwrap();
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
fn bucket_limit_safety() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    let mut buckets = Vec::new();
    for i in 0..10 {
        let name = format!("limit_test_{}", i);
        buckets.push(mace.new_bucket(&name).unwrap());
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
                Err(OpCode::Again) => std::thread::yield_now(),
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

    // run GC and wait for cleanup
    mace.start_gc();

    let mut success = false;
    for _ in 0..50 {
        if mace.nr_buckets() < after_del_count {
            success = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(success, "nr_buckets was not decreased by GC");
}

#[test]
fn bucket_simple() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    let b1 = mace.new_bucket("bucket1").unwrap();

    {
        let tx = b1.begin().unwrap();
        tx.put("key1", "val1").unwrap();
        tx.commit().unwrap();

        let view = b1.view().unwrap();
        let val = view.get("key1").unwrap();
        assert_eq!(val.slice(), b"val1");
    }

    {
        let d = mace.new_bucket("default").unwrap();
        let tx = d.begin().unwrap();
        tx.put("key1", "default_val").unwrap();
        tx.commit().unwrap();
    }

    let tx = b1.begin().unwrap();
    let val_b1 = tx.get("key1").unwrap();
    assert_eq!(val_b1.slice(), b"val1");

    let d = mace.get_bucket("default").unwrap();
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

    assert_eq!(mace.get_bucket("missing").err(), Some(OpCode::NotFound));

    let b1 = mace.new_bucket("x").unwrap();
    assert_eq!(mace.new_bucket("x").err(), Some(OpCode::Exist));

    drop(b1);
    mace.drop_bucket("x")?;
    assert_eq!(mace.new_bucket("x").err(), Some(OpCode::Exist));

    let b1 = mace.get_bucket("x")?;
    drop(b1);

    mace.del_bucket("x")?;
    let _b2 = mace.new_bucket("x")?;

    Ok(())
}

#[test]
fn bucket_persistence() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace.new_bucket("x").unwrap();
        let b1 = db.begin().unwrap();
        b1.put("k", "v").unwrap();
        b1.commit().unwrap();
    }

    {
        let mace = Mace::new(opt.validate().unwrap()).unwrap();
        let db = mace.get_bucket("x").unwrap();
        let b1 = db.view().unwrap();
        let val = b1.get("k").unwrap();
        assert_eq!(val.slice(), b"v");
    }
}

#[test]
fn multiple_buckets() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    let b1 = mace.new_bucket("b1").unwrap();
    let b2 = mace.new_bucket("b2").unwrap();

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
    let db = mace.new_bucket("x").unwrap();

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
    let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    let b1 = db.begin().unwrap();
    let val = vec![0u8; 512];
    for i in 0..10 {
        b1.put(format!("k{}", i), &val).unwrap();
    }
    b1.commit().unwrap();
    drop(db);

    while mace.del_bucket("x") == Err(OpCode::Again) {
        std::thread::sleep(std::time::Duration::from_millis(10));
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

    let b1 = mace.new_bucket("b1").unwrap();
    let uid = b1.id();
    let tx = b1.begin().unwrap();
    tx.put("key", "val").unwrap();
    tx.commit().unwrap();
    drop(b1);

    mace.del_bucket("b1").unwrap();

    mace.start_gc();

    let b2 = mace.new_bucket("b1").unwrap();
    assert_ne!(b2.id(), uid);
}

#[test]
fn bucket_physical_cleanup_recovery() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);

    let bucket_id;

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let bkt = mace.new_bucket("b1").unwrap();
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
        let bkt = mace.new_bucket("b1").unwrap();
        assert_ne!(bkt.id(), bucket_id);
    }
}

#[test]
fn bucket_physical_file_cleanup() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.data_file_size = 1024;

    let mut b1_files = Vec::new();
    let initial_files: std::collections::HashSet<std::ffi::OsString>;

    {
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();

        initial_files = std::fs::read_dir(opt.data_root())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();

        let db = mace.new_bucket("b1").unwrap();
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

        let mut success = false;
        for _ in 0..10 {
            mace.start_gc();
            let mut has_remaining = false;
            for name in &b1_files {
                if opt.data_root().join(name).exists() {
                    has_remaining = true;
                    break;
                }
            }
            if !has_remaining {
                success = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        assert!(success, "files of b1 were not deleted: {:?}", b1_files);
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
        let bucket = db.new_bucket(bucket_name)?;
        // should fail because 'bucket' holds an Arc<BucketMeta>
        let res = db.drop_bucket(bucket_name);
        assert_eq!(res.err(), Some(OpCode::Again));
        drop(bucket);
    }
    // now it should succeed
    db.drop_bucket(bucket_name).unwrap();

    // 2. test: cannot drop while transaction is active
    {
        let bucket = db.get_bucket(bucket_name)?;
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
        let bucket = db.new_bucket(bucket_name)?;
        let kv = bucket.begin()?;
        kv.put("k", "v")?;
        kv.commit()?;
    }

    db.drop_bucket(bucket_name)?;

    // re-open and verify
    let bucket = db.get_bucket(bucket_name)?;
    let view = bucket.view()?;
    assert_eq!(view.get("k")?.slice(), b"v");

    Ok(())
}
