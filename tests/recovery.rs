use mace::{Mace, OpCode, Options, RandomPath};
use std::sync::{Arc, Barrier};

#[test]
fn intact_meta() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut saved = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    let nr_kv = 10;
    let mut pair = Vec::with_capacity(nr_kv);

    for i in 0..nr_kv {
        pair.push((format!("key_{i}"), format!("val_{i}")));
    }

    let kv = db.begin().unwrap();
    for (k, v) in &pair {
        kv.put(k, v).expect("can't insert kv");
    }
    kv.commit().unwrap();

    let kv = db.begin().unwrap();
    for (i, (k, _)) in pair.iter().enumerate() {
        if i % 2 == 0 {
            kv.del(k).expect("can't del");
        }
    }
    kv.commit().unwrap();

    drop(db);
    drop(mace);

    saved.tmp_store = true;
    let mace = Mace::new(saved.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x").unwrap();
    let view = db.view().unwrap();
    for (i, (k, v)) in pair.iter().enumerate() {
        if i % 2 == 0 {
            let r = view.get(k);
            assert!(r.is_err());
        } else {
            let r = view.get(k).expect("can't get key");
            assert_eq!(r.slice(), v.as_bytes());
        }
    }
}

#[test]
fn bad_meta() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();

    let kv = db.begin().unwrap();
    kv.put("114514", "1919810").unwrap();
    kv.commit().unwrap();

    let kv = db.begin().unwrap();
    kv.put("mo", "ha").unwrap();
    drop(kv);

    drop(db);
    drop(mace);

    save.tmp_store = true;
    let mace = Mace::new(save.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x").unwrap();
    let view = db.view().unwrap();
    let x = view.get("114514").expect("not found");
    assert_eq!(x.slice(), "1919810".as_bytes());
    let x = view.get("mo");
    assert!(x.is_err());
}

#[test]
fn crash_again() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();

    {
        let mace = Mace::new(opt.validate().unwrap()).unwrap();
        let db = mace.new_bucket("x").unwrap();
        let kv = db.begin().unwrap();
        kv.put("foo", "bar").unwrap();
        kv.commit().unwrap();

        let kv = db.begin().unwrap();
        kv.put("mo", "+1s").unwrap();
        // implicitly rollback
    }

    {
        let mace = Mace::new(save.clone().validate().unwrap()).unwrap();
        let db = mace.get_bucket("x").unwrap();

        let kv = db.begin().unwrap();
        let x = kv.get("foo").expect("not found");
        assert_eq!(x.slice(), "bar".as_bytes());
        let x = kv.get("mo");
        assert!(x.is_err());

        kv.put("114", "514").unwrap();
        // implicitly rollback
    }

    {
        save.tmp_store = true;
        let mace = Mace::new(save.validate().unwrap()).unwrap();
        let db = mace.get_bucket("x").unwrap();

        let view = db.view().unwrap();
        let r = view.get("foo").expect("not found");
        assert_eq!(r.slice(), "bar".as_bytes());
        let r = view.get("mo");
        assert!(r.is_err());
        let r = view.get("114");
        assert!(r.is_err());
    }
}

#[test]
fn recover_after_insert() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let mut pairs = Vec::new();
    let db = mace.new_bucket("x").unwrap();

    for i in 0..1000 {
        pairs.push((format!("key_{i}"), format!("val_{i}")));
    }

    let kv = db.begin().unwrap();
    for (k, v) in &pairs {
        kv.put(k, v).unwrap();
    }

    kv.commit().unwrap();

    drop(db);
    drop(mace);

    save.tmp_store = true;
    let mace = Mace::new(save.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x").unwrap();
    let view = db.view().unwrap();
    for (k, v) in &pairs {
        let r = view.get(k).unwrap();
        assert_eq!(r.slice(), v.as_bytes());
    }
}

#[test]
fn recover_after_update() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    let mut pairs = Vec::new();
    let mut new_pairs = Vec::new();

    for i in 0..1 {
        pairs.push((format!("key_{i}"), format!("val_{i}")));
        new_pairs.push((format!("key_{i}"), format!("new_val_{i}")));
    }

    for (k, v) in &pairs {
        let kv = db.begin().unwrap();
        kv.put(k, v).unwrap();
        kv.commit().unwrap();
    }

    for _ in 0..3 {
        for (k, v) in &new_pairs {
            let kv = db.begin().unwrap();
            kv.update(k, v).unwrap();
            kv.commit().unwrap();
        }
    }

    let view = db.view().unwrap();
    for (k, v) in &new_pairs {
        let r = view.get(k).expect("not found");
        assert_eq!(r.slice(), v.as_bytes());
    }

    drop(view);
    drop(db);
    drop(mace);

    save.tmp_store = true;
    let mace = Mace::new(save.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x").unwrap();

    let view = db.view().unwrap();
    for (k, v) in &new_pairs {
        let r = view.get(k).expect("not found");
        assert_eq!(r.slice(), v.as_bytes());
    }
}

#[test]
fn recover_after_remove() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    let mut pairs = Vec::new();

    for i in 0..1000 {
        pairs.push((format!("key_{i}"), format!("val_{i}")));
    }

    for (k, v) in &pairs {
        let kv = db.begin().unwrap();
        kv.put(k, v).unwrap();
        kv.commit().unwrap();
    }

    for (k, _) in &pairs {
        let kv = db.begin().unwrap();
        kv.del(k).unwrap();
        kv.commit().unwrap();
    }

    drop(db);
    drop(mace);

    save.tmp_store = true;
    let mace = Mace::new(save.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x").unwrap();
    let view = db.view().unwrap();
    for (k, _) in &pairs {
        let r = view.get(k);
        assert!(r.is_err());
        assert_eq!(r.err().unwrap(), OpCode::NotFound);
    }
}

fn ckpt_wal(keys: usize, wal_len: u32) {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.data_file_size = 512 << 10;
    opt.wal_file_size = wal_len;
    let mut save = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    let mut data = Vec::new();

    for i in 0..keys {
        data.push((format!("key_{i}"), format!("val_{i}")));
    }

    let kv = db.begin().unwrap();
    for (k, v) in &data {
        kv.put(k, v).unwrap();
    }
    kv.commit().unwrap();

    for (k, v) in &data {
        let kv = db.begin().unwrap();
        kv.update(k, v).unwrap();
        kv.commit().unwrap();
    }

    drop(db);
    drop(mace);

    save.tmp_store = true;
    let mace = Mace::new(save.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x").unwrap();
    let view = db.view().unwrap();
    for (k, v) in &data {
        let r = view.get(k).expect("not found");
        assert_eq!(r.slice(), v.as_bytes());
    }
}

#[test]
fn checkpoint() {
    ckpt_wal(1000, 1 << 20);
}

#[test]
fn roll_log() {
    ckpt_wal(1000, 50 << 10);
}

fn long_txn_impl(before: bool) {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.wal_file_size = 1024;
    let mut save = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    let b = Arc::new(Barrier::new(2));
    let mut pair = Vec::new();

    for i in 0..20 {
        pair.push((format!("key_{i}"), format!("key_{i}")));
    }

    let cb = b.clone();
    let db2 = db.clone();
    let t = std::thread::spawn(move || {
        let kv = db2.begin().unwrap();
        kv.put("foo", "bar").unwrap();
        kv.commit().unwrap();

        cb.wait();
        let kv = db2.begin().unwrap();
        kv.put("mo", "+1s").unwrap();
        // implicitly rollback
    });

    if before {
        b.wait();
    }
    for (k, v) in &pair {
        let kv = db.begin().unwrap();
        kv.put(k, v).unwrap();
        kv.commit().unwrap();
    }

    if !before {
        b.wait();
    }
    t.join().unwrap();

    drop(db);
    drop(mace);

    save.tmp_store = true;
    let mace = Mace::new(save.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x").unwrap();
    let view = db.view().unwrap();
    for (k, v) in &pair {
        let r = view.get(k).expect("not found");
        assert_eq!(r.slice(), v.as_bytes());
    }

    let r = view.get("foo").expect("not found");
    assert_eq!(r.slice(), "bar".as_bytes());
    let r = view.get("mo");
    assert!(r.is_err() && r.err().unwrap() == OpCode::NotFound);
}

#[test]
fn long_txn() {
    long_txn_impl(true);
    long_txn_impl(false);
}
