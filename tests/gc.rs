use mace::{Mace, OpCode, Options, RandomPath};

#[test]
#[ignore = "it's hard to produce enough garbage in release mode, run it manually instead"]
fn gc_data() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = false;
    opt.sync_on_write = false;
    opt.gc_eager = true;
    opt.gc_timeout = 20;
    opt.gc_ratio = 1;
    opt.data_file_size = 512 << 10;
    opt.gc_compacted_size = opt.data_file_size;
    let saved = opt.clone();
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

    break_meta(&saved);

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

#[test]
fn gc_wal() {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.wal_file_size = 4096;
    opt.gc_timeout = 2;
    opt.workers = 1;
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

#[allow(unused)]
fn break_meta(opt: &Options) {
    use std::fs::File;
    use std::io::Write;

    let d = std::fs::read_dir(opt.db_root()).unwrap();
    let mut last = 0;
    for f in d.flatten() {
        let tmp = f.file_name();
        let name = tmp.to_str().unwrap();
        if name.starts_with(Options::MANIFEST_PREFIX) {
            let v: Vec<&str> = name.split(Options::SEP).collect();
            let seq = v[1].parse::<u64>().unwrap();
            last = last.max(seq);
        }
    }
    let mut f = File::options()
        .truncate(false)
        .append(true)
        .open(opt.manifest(last))
        .unwrap();
    // append a Begin
    f.write_all(&[1]).unwrap();
}
