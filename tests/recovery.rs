use mace::{IsolationLevel, Mace, OpCode, Options, RandomPath};
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
    let db = Mace::open(opt).unwrap();
    let nr_kv = 10;
    let tx = db.default();
    let mut pair = Vec::with_capacity(nr_kv);

    for i in 0..nr_kv {
        pair.push((format!("key_{i}"), format!("val_{i}")));
    }

    let _ = tx.begin(IsolationLevel::SI, |kv| {
        for (k, v) in &pair {
            kv.put(k, v).expect("can't insert kv");
        }
        kv.commit()
    });

    let _ = tx.begin(IsolationLevel::SI, |kv| {
        for (i, (k, _)) in pair.iter().enumerate() {
            if i % 2 == 0 {
                kv.del(k).expect("can't del");
            }
        }
        kv.commit()
    });

    drop(db);

    saved.tmp_store = true;
    let db = Mace::open(saved).unwrap();
    let tx = db.default();
    let _ = tx.view(IsolationLevel::SI, |view| {
        for (i, (k, v)) in pair.iter().enumerate() {
            if i % 2 == 0 {
                let r = view.get(k);
                assert!(r.is_err());
            } else {
                let r = view.get(k).expect("can't get key");
                assert_eq!(r.data(), v.as_bytes());
            }
        }
        Ok(())
    });
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
    let db = Mace::open(opt).unwrap();

    let tx = db.default();

    let _ = tx.begin(IsolationLevel::SI, |kv| {
        kv.put("114514", "1919810").unwrap();
        kv.commit()
    });

    let _ = tx.begin(IsolationLevel::SI, |kv| {
        kv.put("mo", "ha").unwrap();
        Ok(())
    });

    drop(db);

    break_meta(save.meta_file());

    save.tmp_store = true;
    let db = Mace::open(save).unwrap();
    let tx = db.default();

    let _ = tx.view(IsolationLevel::SI, |kv| {
        let x = kv.get("114514").expect("not found");
        assert_eq!(x.data(), "1919810".as_bytes());
        let x = kv.get("mo");
        assert!(x.is_err());
        Ok(())
    });
}

#[test]
fn crash_again() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();

    {
        let db = Mace::open(opt).unwrap();
        let tx = db.default();
        let _ = tx.begin(IsolationLevel::SI, |kv| {
            kv.put("foo", "bar").unwrap();
            kv.commit()
        });

        let _ = tx.begin(IsolationLevel::SI, |kv| {
            kv.put("mo", "+1s").unwrap();
            Ok(())
        });
    }

    break_meta(save.meta_file());

    {
        let db = Mace::open(save.clone()).unwrap();
        let tx = db.default();

        let _ = tx.begin(IsolationLevel::SI, |kv| {
            let x = kv.get("foo").expect("not found");
            assert_eq!(x.data(), "bar".as_bytes());
            let x = kv.get("mo");
            assert!(x.is_err());

            kv.put("114", "514").unwrap();
            Ok(())
        });
    }

    break_meta(save.meta_file());

    {
        save.tmp_store = true;
        let db = Mace::open(save.clone()).unwrap();
        let tx = db.default();

        let _ = tx.view(IsolationLevel::SI, |view| {
            let r = view.get("foo").expect("not found");
            assert_eq!(r.data(), "bar".as_bytes());
            let r = view.get("mo");
            assert!(r.is_err());
            let r = view.get("114");
            assert!(r.is_err());
            Ok(())
        });
    }
}

fn lost_impl<F>(f: F)
where
    F: Fn(&Options, u16),
{
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let db = Mace::open(opt).unwrap();

    let tx = db.default();
    let _ = tx.begin(IsolationLevel::SI, |kv| {
        kv.put("114514", "1919810").unwrap();
        kv.commit()
    });

    let _ = tx.begin(IsolationLevel::SI, |_| Ok(()));

    drop(db);

    f(&save, 1);

    save.tmp_store = true;
    let db = Mace::open(save).unwrap();
    let tx = db.default();
    let _ = tx.view(IsolationLevel::SI, |view| {
        let x = view.get("114514").expect("not found");
        assert_eq!(x.data(), "1919810".as_bytes());
        Ok(())
    });
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
    let db = Mace::open(opt).unwrap();
    let mut pairs = Vec::new();

    let tx = db.default();
    for i in 0..1000 {
        pairs.push((format!("key_{}", i), format!("val_{}", i)));
    }

    let _ = tx.begin(IsolationLevel::SI, |kv| {
        for (k, v) in &pairs {
            kv.put(k, v).unwrap();
        }

        kv.commit()
    });

    drop(db);

    break_meta(save.meta_file());

    save.tmp_store = true;
    let db = Mace::open(save).unwrap();
    let tx = db.default();
    let _ = tx.view(IsolationLevel::SI, |view| {
        for (k, v) in &pairs {
            let r = view.get(k).unwrap();
            assert_eq!(r.data(), v.as_bytes());
        }
        Ok(())
    });
}

#[test]
fn recover_after_update() {
    put_update(false);
}

fn put_update(remov_data: bool) {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let db = Mace::open(opt).unwrap();
    let mut pairs = Vec::new();
    let mut new_pairs = Vec::new();

    for i in 0..10000 {
        pairs.push((format!("key_{}", i), format!("val_{}", i)));
        new_pairs.push((format!("key_{}", i), format!("new_val_{}", i)));
    }

    let tx = db.get("moha");
    for (k, v) in &pairs {
        let _ = tx.begin(IsolationLevel::SI, |kv| {
            kv.put(k, v).unwrap();
            kv.commit()
        });
    }

    for _ in 0..3 {
        for (k, v) in &new_pairs {
            let _ = tx.begin(IsolationLevel::SI, |kv| {
                kv.update(k, v).unwrap();
                kv.commit()
            });
        }
    }

    let _ = tx.view(IsolationLevel::SI, |view| {
        for (k, v) in &new_pairs {
            let r = view.get(k).expect("not found");
            assert_eq!(r.data(), v.as_bytes());
        }
        Ok(())
    });

    drop(db);
    break_meta(save.meta_file());
    if remov_data {
        for i in 1.. {
            let p = save.data_file(i);
            if !p.exists() {
                break;
            }
            std::fs::remove_file(p).unwrap();
        }
    }

    save.tmp_store = true;
    let db = Mace::open(save).unwrap();
    let tx = db.get("moha");

    let _ = tx.view(IsolationLevel::SI, |view| {
        for (k, v) in &new_pairs {
            let r = view.get(k).expect("not found");
            assert_eq!(r.data(), v.as_bytes());
        }
        Ok(())
    });
}

#[test]
fn recover_after_remove() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let db = Mace::open(opt).unwrap();
    let mut pairs = Vec::new();

    for i in 0..1000 {
        pairs.push((format!("key_{}", i), format!("val_{}", i)));
    }

    let tx = db.default();
    for (k, v) in &pairs {
        let _ = tx.begin(IsolationLevel::SI, |kv| {
            kv.put(k, v).unwrap();
            kv.commit()
        });
    }

    for (k, _) in &pairs {
        let _ = tx.begin(IsolationLevel::SI, |kv| {
            kv.del(k).unwrap();
            kv.commit()
        });
    }

    drop(db);
    break_meta(save.meta_file());

    save.tmp_store = true;
    let db = Mace::open(save).unwrap();
    let tx = db.default();
    let _ = tx.view(IsolationLevel::SI, |view| {
        for (k, _) in &pairs {
            let r = view.get(k);
            assert!(r.is_err());
            assert_eq!(r.err().unwrap(), OpCode::NotFound);
        }
        Ok(())
    });
}

fn ckpt_wal(keys: usize, ckpt_per: usize, wal_len: usize) {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.chkpt_per_bytes = ckpt_per;
    opt.wal_file_size = wal_len;
    let mut save = opt.clone();
    let db = Mace::open(opt).unwrap();
    let mut data = Vec::new();

    for i in 0..keys {
        data.push((format!("key_{}", i), format!("val_{}", i)));
    }

    let tx = db.default();
    let _ = tx.begin(IsolationLevel::SI, |kv| {
        for (k, v) in &data {
            kv.put(k, v).unwrap();
        }
        kv.commit()
    });

    for _ in 0..keys {
        for (k, v) in &data {
            let _ = tx.begin(IsolationLevel::SI, |kv| {
                kv.update(k, v).unwrap();
                kv.commit()
            });
        }
    }

    drop(db);

    break_meta(save.meta_file());

    save.tmp_store = true;
    let db = Mace::open(save).unwrap();
    let tx = db.default();
    let _ = tx.view(IsolationLevel::SI, |view| {
        for (k, v) in &data {
            let r = view.get(k).expect("not found");
            assert_eq!(r.data(), v.as_bytes());
        }
        Ok(())
    });
}

#[test]
fn checkpoint() {
    ckpt_wal(3, 512, 10 << 20);
}

#[test]
fn roll_log() {
    ckpt_wal(5, 256, 1024);
}

#[test]
fn recover_from_middle() {
    ckpt_wal(4, 256, 1024);
}

fn long_txn_impl(before: bool) {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.chkpt_per_bytes = 256;
    opt.wal_file_size = 1024;
    let mut save = opt.clone();
    let db = Mace::open(opt).unwrap();
    let b = Arc::new(Barrier::new(2));
    let mut pair = Vec::new();

    for i in 0..20 {
        pair.push((format!("key_{}", i), format!("key_{}", i)));
    }

    let tx = db.default();
    let ctx = tx.clone();
    let cb = b.clone();
    let t = std::thread::spawn(move || {
        let _ = ctx.begin(IsolationLevel::SI, |kv| {
            kv.put("foo", "bar").unwrap();
            kv.commit()
        });

        cb.wait();
        let _ = ctx.begin(IsolationLevel::SI, |kv| {
            kv.put("mo", "+1s").unwrap();
            Ok(())
        });
    });

    if before {
        b.wait();
    }
    for (k, v) in &pair {
        let _ = tx.begin(IsolationLevel::SI, |kv| {
            kv.put(k, v).unwrap();
            kv.commit()
        });
    }

    if !before {
        b.wait();
    }
    t.join().unwrap();

    drop(db);
    break_meta(save.meta_file());

    save.tmp_store = true;
    let db = Mace::open(save).unwrap();
    let tx = db.default();
    let _ = tx.view(IsolationLevel::SI, |view| {
        for (k, v) in &pair {
            let r = view.get(k).expect("not found");
            assert_eq!(r.data(), v.as_bytes());
        }

        let r = view.get("foo").expect("not found");
        assert_eq!(r.data(), "bar".as_bytes());
        let r = view.get("mo");
        assert!(r.is_err() && r.err().unwrap() == OpCode::NotFound);
        Ok(())
    });
}

#[test]
fn long_txn() {
    long_txn_impl(true);
    long_txn_impl(false);
}
