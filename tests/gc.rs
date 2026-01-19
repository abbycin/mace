use mace::{Mace, OpCode, Options, RandomPath};

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
    let db = Mace::new(opt.validate().unwrap()).unwrap();

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

    let kv = db.begin()?;
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

    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    let db = Mace::new(opt.validate().unwrap()).unwrap();

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
    opt.over_provision = true;
    let db = Mace::new(opt.validate()?)?;
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

    for k in &pair {
        let kv = db.begin()?;
        kv.update(k, &val)?;
        kv.commit()?;
    }

    let kv = db.begin()?;
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
    if db.blob_gc_count() > 0 {
        drop(db);
        opt.tmp_store = true;
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
    let db = Mace::new(opt.validate().unwrap()).unwrap();

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
    let db = Mace::new(opt.validate().unwrap()).unwrap();
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
    assert!(backup.exists());
}
