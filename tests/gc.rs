use std::{thread::sleep, time::Duration};

use mace::{IsolationLevel, Mace, OpCode, Options, RandomPath};

#[test]
fn gc_data() {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_timeout = 20;
    opt.gc_ratio = 10;
    opt.buffer_size = 512 << 10;
    let db = Mace::new(opt).unwrap();

    let tx = db.default();
    let cap = 20000;
    let mut pair = Vec::with_capacity(cap);

    for i in 0..cap {
        pair.push(format!("{:08}", i));
    }

    for k in &pair {
        tx.begin(IsolationLevel::SI, |kv| {
            kv.put(k, k)?;
            kv.commit()
        })
        .unwrap();
    }

    for k in &pair {
        tx.view(IsolationLevel::SI, |view| {
            view.get(k)?;
            Ok(())
        })
        .unwrap();
    }

    let mut count = 0;
    let mut max_id = 0;
    let dir = std::fs::read_dir(&db.options().db_root).unwrap();
    for d in dir {
        let x = d.unwrap();
        let f = x.file_name();
        let name = f.to_str().unwrap();
        if name.starts_with(Options::DATA_PREFIX) {
            let v: Vec<&str> = name.split(Options::DATA_PREFIX).collect();
            let id = v[1].parse::<u32>().expect("invalid number");
            count += 1;
            max_id = max_id.max(id);
        }
    }
    assert!(count < max_id);
}

#[test]
fn abort_txn() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.max_ckpt_per_txn = 1;
    opt.buffer_size = 50 << 10; // make sure checkpoint was taken
    let db = Mace::new(opt).unwrap();

    let tx = db.default();
    let ctx = tx.clone();
    let h = std::thread::spawn(move || {
        let _ = ctx.begin(IsolationLevel::SI, |kv| {
            kv.put("foo", "bar")?;
            sleep(Duration::from_millis(100));
            let r = kv.commit();
            assert!(r.is_err() && r.err().unwrap() == OpCode::AbortTx);
            Ok(())
        });
    });

    let _ = tx.begin(IsolationLevel::SI, |kv| {
        for i in 0..200 {
            let x = format!("key_{}", i);
            kv.put(&x, &x)?;
        }
        kv.commit()
    });

    h.join().unwrap();
}

#[test]
fn gc_wal() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.wal_file_size = 4096;
    opt.gc_timeout = 2;
    opt.buffer_size = 100 << 10; // make sure checkpoint was taken
    let db = Mace::new(opt).unwrap();
    let tx = db.default();
    let mut data = Vec::new();

    for i in 0..1000 {
        data.push(format!("data_{}", i));
    }

    for i in &data {
        tx.begin(IsolationLevel::SI, |kv| {
            kv.put(i, i)?;
            kv.commit()
        })
        .unwrap();
    }

    for i in &data {
        tx.view(IsolationLevel::SI, |view| {
            let r = view.get(i).expect("not found");
            assert_eq!(r.data(), i.as_bytes());
            Ok(())
        })
        .unwrap();
    }

    let backup = db.options().wal_backup(1);
    assert!(backup.exists());
}

#[test]
fn remove_tree() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.page_size = 2048;
    let db = Mace::new(opt).unwrap();
    let tx = db.alloc().unwrap();

    tx.begin(IsolationLevel::SI, |kv| {
        for i in 0..100 {
            let s = format!("xx-{}", i);
            kv.put(&s, &s)?;
        }
        kv.commit()
    })
    .unwrap();

    db.remove(tx.id()).unwrap();

    let tx = db.get(tx.id());
    assert!(tx.is_err());
}
