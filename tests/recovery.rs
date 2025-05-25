use mace::{Mace, OpCode, Options, RandomPath};
use std::{
    fs::File,
    io::Write,
    path::PathBuf,
    sync::{Arc, Barrier},
};

#[test]
fn intact_meta() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut saved = opt.clone();
    let db = Mace::new(opt.validate().unwrap()).unwrap();
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
    drop(kv);

    let kv = db.begin().unwrap();
    for (i, (k, _)) in pair.iter().enumerate() {
        if i % 2 == 0 {
            kv.del(k).expect("can't del");
        }
    }
    kv.commit().unwrap();
    drop(kv);

    drop(db);

    saved.tmp_store = true;
    let db = Mace::new(saved.validate().unwrap()).unwrap();
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

fn break_meta(path: PathBuf) {
    let mut f = File::options()
        .truncate(false)
        .append(true)
        .open(path)
        .unwrap();
    f.write_all(&[233]).unwrap();
}

#[test]
fn bad_meta() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let db = Mace::new(opt.validate().unwrap()).unwrap();

    let kv = db.begin().unwrap();
    kv.put("114514", "1919810").unwrap();
    kv.commit().unwrap();
    drop(kv);

    let kv = db.begin().unwrap();
    kv.put("mo", "ha").unwrap();
    kv.rollback().unwrap();
    drop(kv);

    drop(db);

    break_meta(save.meta_file());

    save.tmp_store = true;
    let db = Mace::new(save.validate().unwrap()).unwrap();

    let view = db.view().unwrap();
    view.show();
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
        let db = Mace::new(opt.validate().unwrap()).unwrap();
        let kv = db.begin().unwrap();
        kv.put("foo", "bar").unwrap();
        kv.commit().unwrap();

        let kv = db.begin().unwrap();
        kv.put("mo", "+1s").unwrap();
        // implicitly rollback
    }

    break_meta(save.meta_file());

    {
        let db = Mace::new(save.clone().validate().unwrap()).unwrap();

        let kv = db.begin().unwrap();
        let x = kv.get("foo").expect("not found");
        assert_eq!(x.slice(), "bar".as_bytes());
        let x = kv.get("mo");
        assert!(x.is_err());

        kv.put("114", "514").unwrap();
        // implicitly rollback
    }

    break_meta(save.meta_file());

    {
        save.tmp_store = true;
        let db = Mace::new(save.validate().unwrap()).unwrap();

        let view = db.view().unwrap();
        let r = view.get("foo").expect("not found");
        assert_eq!(r.slice(), "bar".as_bytes());
        let r = view.get("mo");
        assert!(r.is_err());
        let r = view.get("114");
        assert!(r.is_err());
    }
}

fn lost_impl<F>(f: F)
where
    F: Fn(&Options, u32),
{
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let db = Mace::new(opt.validate().unwrap()).unwrap();

    let kv = db.begin().unwrap();
    kv.put("114514", "1919810").unwrap();
    kv.commit().unwrap();
    drop(kv);

    let kv = db.begin().unwrap();
    drop(kv);

    drop(db);

    f(&save, 1);

    save.tmp_store = true;
    let db = Mace::new(save.validate().unwrap()).unwrap();
    let view = db.view().unwrap();
    let x = view.get("114514").expect("not found");
    assert_eq!(x.slice(), "1919810".as_bytes());
}

#[test]
fn lost_meta() {
    lost_impl(|opt, _id| {
        break_meta(opt.meta_file());
    });
}

#[test]
fn lost_all() {
    lost_impl(|opt, id| {
        break_meta(opt.meta_file());
        std::fs::remove_file(opt.data_file(id)).unwrap();
    });
    put_update(true);
}

#[test]
fn recover_after_insert() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let db = Mace::new(opt.validate().unwrap()).unwrap();
    let mut pairs = Vec::new();

    for i in 0..1000 {
        pairs.push((format!("key_{}", i), format!("val_{}", i)));
    }

    let kv = db.begin().unwrap();
    for (k, v) in &pairs {
        kv.put(k, v).unwrap();
    }

    kv.commit().unwrap();
    drop(kv);

    drop(db);

    break_meta(save.meta_file());

    save.tmp_store = true;
    let db = Mace::new(save.validate().unwrap()).unwrap();
    let view = db.view().unwrap();
    for (k, v) in &pairs {
        let r = view.get(k).unwrap();
        assert_eq!(r.slice(), v.as_bytes());
    }
}

#[test]
fn recover_after_update() {
    put_update(false);
}

fn put_update(remove_data: bool) {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let db = Mace::new(opt.validate().unwrap()).unwrap();
    let mut pairs = Vec::new();
    let mut new_pairs = Vec::new();

    for i in 0..10000 {
        pairs.push((format!("key_{}", i), format!("val_{}", i)));
        new_pairs.push((format!("key_{}", i), format!("new_val_{}", i)));
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
    break_meta(save.meta_file());
    if remove_data {
        let entries = std::fs::read_dir(&save.db_root).unwrap();
        for e in entries {
            let e = e.unwrap();
            let file = e.path();
            let name = e.file_name();
            let s = name.to_str().unwrap();

            if s.starts_with(Options::DATA_PREFIX) {
                std::fs::remove_file(file).unwrap();
            }
        }
    }

    save.tmp_store = true;
    let db = Mace::new(save.validate().unwrap()).unwrap();

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
    let db = Mace::new(opt.validate().unwrap()).unwrap();
    let mut pairs = Vec::new();

    for i in 0..1000 {
        pairs.push((format!("key_{}", i), format!("val_{}", i)));
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
    break_meta(save.meta_file());

    save.tmp_store = true;
    let db = Mace::new(save.validate().unwrap()).unwrap();
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
    opt.buffer_count = 10;
    opt.buffer_size = 512 << 10;
    opt.wal_file_size = wal_len;
    let mut save = opt.clone();
    let db = Mace::new(opt.validate().unwrap()).unwrap();
    let mut data = Vec::new();

    for i in 0..keys {
        data.push((format!("key_{}", i), format!("val_{}", i)));
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

    drop(kv);
    drop(db);

    break_meta(save.meta_file());

    save.tmp_store = true;
    let db = Mace::new(save.validate().unwrap()).unwrap();
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
    let db = Mace::new(opt.validate().unwrap()).unwrap();
    let b = Arc::new(Barrier::new(2));
    let mut pair = Vec::new();

    for i in 0..20 {
        pair.push((format!("key_{}", i), format!("key_{}", i)));
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
    break_meta(save.meta_file());

    save.tmp_store = true;
    let db = Mace::new(save.validate().unwrap()).unwrap();
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
