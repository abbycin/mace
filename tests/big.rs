use mace::{BucketOptions, Mace, Options, RandomPath};
use rand::RngExt;

#[test]
fn upsert_delete() {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    let mut saved = opt.clone();
    opt.tmp_store = false;
    opt.gc_eager = true;
    opt.gc_timeout = 1000;
    opt.data_garbage_ratio = 10;
    opt.wal_file_size = 32 << 20;

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();
    let mut rng = rand::rng();

    const N: usize = 1200;
    let mut kvs = Vec::with_capacity(N);
    for i in 0..N {
        kvs.push((
            format!(
                "{}{}{}_{:06}",
                rng.random_range(48..112),
                rng.random_range(48..112),
                rng.random_range(48..120),
                i,
            ),
            vec![233u8; 16384],
        ));
    }
    kvs.push(("sb".into(), vec![114u8; 131122]));

    for _ in 0..5 {
        for (k, v) in kvs.iter() {
            let kv = db.begin().unwrap();
            kv.upsert(k, v).unwrap();
            kv.commit().unwrap();
        }

        for (k, _) in kvs.iter() {
            let kv = db.begin().unwrap();
            kv.del(k).unwrap();
            kv.commit().unwrap();
        }
    }

    drop(db);
    drop(mace);

    saved.tmp_store = true;
    let mace = Mace::new(saved.validate().unwrap()).unwrap();
    let db = mace.open_bucket("x").unwrap();

    for (k, _) in &kvs {
        let view = db.view().unwrap();
        assert!(view.get(k).is_err());
    }
}

#[test]
fn big_kv() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut saved = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    const N: usize = 200;
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();
    let kv = db.begin().unwrap();
    let val = vec![233; 56 << 10];
    let keys: Vec<String> = (0..N).map(|x| format!("key_{x}")).collect();

    for k in &keys {
        kv.put(k, &val).unwrap();
    }
    kv.commit().unwrap();

    let kv = db.begin().unwrap();
    for k in &keys {
        let x = kv.get(k);
        assert!(x.is_ok());
        assert_eq!(x.unwrap().slice(), val.as_slice());
    }
    kv.commit().unwrap();

    drop(db);
    drop(mace);

    saved.tmp_store = true;
    let mace = Mace::new(saved.validate().unwrap()).unwrap();
    let db = mace.open_bucket("x").unwrap();
    let view = db.view().unwrap();

    for k in &keys {
        let x = view.get(k);
        assert!(x.is_ok());
        assert_eq!(x.unwrap().slice(), val.as_slice());
    }
}

#[test]
fn big_kv2() {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.wal_buffer_size = 1024;
    opt.data_file_size = 4096;
    let mut saved = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();

    let kv = db.begin().unwrap();

    kv.put("key1", vec![233u8; 512]).unwrap();
    kv.put("key2", vec![114u8; db.options().wal_buffer_size])
        .unwrap();
    let r = kv.put("key3", vec![114u8; db.options().data_file_size]);
    assert!(r.is_ok());
    kv.commit().unwrap();

    let view = db.view().unwrap();

    let r = view.get("key1").unwrap();
    assert_eq!(r.slice(), vec![233u8; 512]);

    drop(r);
    drop(view);
    drop(db);
    drop(mace);

    saved.tmp_store = true;
    let mace = Mace::new(saved.validate().unwrap()).unwrap();
    let db = mace.open_bucket("x").unwrap();
    let view = db.view().unwrap();

    let r = view.get("key1").unwrap();
    assert_eq!(r.slice(), vec![233u8; 512]);

    let r = view.get("key2").unwrap();
    assert_eq!(r.slice(), vec![114u8; db.options().wal_buffer_size]);
}

#[test]
fn big_kv3() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    // non-tmp store: this test reopens after graceful close (tmp_store would
    // wipe the whole db_root on exit, leaving nothing to reopen)
    let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
    let db = mace.new_bucket("x", BucketOptions::default()).unwrap();
    let val = vec![b'0'; 10240];
    let ksz = 1024;
    let count = 10000;

    let mut keys = Vec::with_capacity(count);
    for i in 0..count {
        let mut tmp = format!("key_{i}").into_bytes();
        tmp.resize(ksz, b'x');
        let tx = db.begin().unwrap();
        tx.put(&tmp, &val).unwrap();
        keys.push(tmp);
        tx.commit().unwrap();
    }

    // sampled reads before reopen
    for i in [0, count / 2, count - 1] {
        let view = db.view().unwrap();
        assert_eq!(view.get(&keys[i]).unwrap().slice(), val.as_slice());
    }

    // full-scale write must survive graceful close and reopen
    drop(db);
    drop(mace);
    // tmp_store only on the teardown open: the reopen assertions above need a
    // persistent root, and this flag wipes db_root when this instance drops
    let mut reopen_opt = opt.clone();
    reopen_opt.tmp_store = true;
    let mace = Mace::new(reopen_opt.validate().unwrap()).unwrap();
    let db = mace.open_bucket("x").unwrap();
    for i in [0, count / 4, count / 2, count - 1] {
        let view = db.view().unwrap();
        assert_eq!(view.get(&keys[i]).unwrap().slice(), val.as_slice());
    }
}
