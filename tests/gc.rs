use mace::{Mace, OpCode, Options, RandomPath};

#[test]
fn gc_data() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.gc_timeout = 20;
    opt.gc_ratio = 10;
    opt.buffer_size = 512 << 10;
    let db = Mace::new(opt).unwrap();

    let cap = 20000;
    let mut pair = Vec::with_capacity(cap);

    for i in 0..cap {
        pair.push(format!("{:08}", i));
    }

    for k in &pair {
        let kv = db.begin().unwrap();
        kv.put(k, k)?;
        kv.commit()?;
    }

    for k in &pair {
        let view = db.view().unwrap();
        view.get(k)?;
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
    Ok(())
}

#[test]
fn abort_txn() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.max_ckpt_per_txn = 1;
    opt.buffer_size = 50 << 10; // make sure checkpoint was taken
    let db = Mace::new(opt).unwrap();

    let kv = db.begin().unwrap();
    for i in 0..500 {
        let x = format!("key_{}", i);
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
    opt.buffer_size = 100 << 10; // make sure checkpoint was taken
    let db = Mace::new(opt).unwrap();
    let mut data = Vec::new();

    for i in 0..1000 {
        data.push(format!("data_{}", i));
    }

    for i in &data {
        let kv = db.begin().unwrap();
        kv.put(i, i).unwrap();
        kv.commit().unwrap();
    }

    for i in &data {
        let view = db.view().unwrap();
        let r = view.get(i).expect("not found");
        assert_eq!(r.data(), i.as_bytes());
    }

    let backup = db.options().wal_backup(1);
    assert!(backup.exists());
}
