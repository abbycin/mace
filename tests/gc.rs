#[cfg(feature = "test_disable_recycle")]
use std::io::Write;

use mace::{Mace, OpCode, Options, RandomPath};

#[cfg(feature = "test_disable_recycle")]
#[test]
fn gc_data() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = false;
    opt.sync_on_write = false;
    opt.gc_eager = true;
    opt.gc_timeout = 20;
    opt.gc_ratio = 10;
    opt.data_file_size = 512 << 10;
    opt.gc_compacted_size = opt.data_file_size as usize;
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

    let meta = db.options().meta_file();

    drop(db);

    // break the meta file
    let mut f = std::fs::File::options()
        .truncate(false)
        .append(true)
        .open(meta)
        .unwrap();
    f.write_all(&[233]).unwrap();

    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    let db = Mace::new(opt.validate().unwrap()).unwrap();

    for k in &pair {
        let view = db.view().unwrap();
        view.get(k).unwrap();
    }

    let mut count = 0;
    let mut max_id = 0;
    let dir = std::fs::read_dir(&*path).unwrap();
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
    for i in 0..5000 {
        let x = format!("key_{i}");
        let _ = kv.put(&x, &x);
    }
    let r = kv.commit();

    assert!(r.is_err() && r.err().unwrap() == OpCode::AbortTx);
}

#[cfg(feature = "test_disable_recycle")]
#[test]
fn gc_wal() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.wal_file_size = 4096;
    opt.gc_timeout = 2;
    opt.workers = 1;
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
