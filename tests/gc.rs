use std::{thread::sleep, time::Duration};

use mace::{IsolationLevel, Mace, OpCode, Options, RandomPath};

#[test]
fn gc_data() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.gc_timeout = 10;
    opt.buffer_size = 1 << 20;
    opt.data_file_size = 1 << 20;
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
    assert!(!db.options().data_file(1).exists());
}

#[test]
fn abort_txn() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.max_ckpt_per_txn = 1;
    opt.ckpt_per_bytes = 512;
    let db = Mace::new(opt).unwrap();

    let tx = db.default();
    let ctx = tx.clone();
    let h = std::thread::spawn(move || {
        let _ = ctx.begin(IsolationLevel::SI, |kv| {
            kv.put("foo", "bar")?;
            sleep(Duration::from_millis(200));
            let r = kv.commit();
            assert!(r.is_err() && r.err().unwrap() == OpCode::AbortTx);
            Ok(())
        });
    });

    sleep(Duration::from_millis(100));

    let _ = tx.begin(IsolationLevel::SI, |kv| {
        for i in 0..10 {
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
    opt.ckpt_per_bytes = 512;
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
