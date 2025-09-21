use crc32c::Crc32cHasher;
use mace::{Mace, OpCode, Options, RandomPath};
use std::{
    fs::File,
    hash::Hasher,
    io::Write,
    path::Path,
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

fn imcomplete_manifest(opt: &Options) {
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
    // append a Begin EntryKind
    f.write_all(&[1]).unwrap();
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

    imcomplete_manifest(&save);

    save.tmp_store = true;
    let db = Mace::new(save.validate().unwrap()).unwrap();

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
        let db = Mace::new(opt.validate().unwrap()).unwrap();
        let kv = db.begin().unwrap();
        kv.put("foo", "bar").unwrap();
        kv.commit().unwrap();

        let kv = db.begin().unwrap();
        kv.put("mo", "+1s").unwrap();
        // implicitly rollback
    }

    imcomplete_manifest(&save);

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

    imcomplete_manifest(&save);

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

#[test]
fn recover_after_insert() {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let db = Mace::new(opt.validate().unwrap()).unwrap();
    let mut pairs = Vec::new();

    for i in 0..1000 {
        pairs.push((format!("key_{i}"), format!("val_{i}")));
    }

    let kv = db.begin().unwrap();
    for (k, v) in &pairs {
        kv.put(k, v).unwrap();
    }

    kv.commit().unwrap();
    drop(kv);

    drop(db);

    imcomplete_manifest(&save);

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

#[test]
fn recover_after_update2() {
    put_update(true);
}

fn put_update(remove_data: bool) {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut save = opt.clone();
    let db = Mace::new(opt.validate().unwrap()).unwrap();
    let mut pairs = Vec::new();
    let mut new_pairs = Vec::new();

    for i in 0..10000 {
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
    imcomplete_manifest(&save);
    if remove_data {
        let entries = std::fs::read_dir(save.data_root()).unwrap();
        let mut data_seq = 0;
        let mut seq = 0;
        for e in entries {
            let e = e.unwrap();
            let name = e.file_name();
            let s = name.to_str().unwrap();

            if s.starts_with(Options::DATA_PREFIX) {
                let v: Vec<&str> = s.split(Options::SEP).collect();
                let x = v[1].parse::<u32>().unwrap();
                data_seq = data_seq.max(x);
            }
            if s.starts_with(Options::MANIFEST_PREFIX) {
                let v: Vec<&str> = s.split(Options::SEP).collect();
                let x = v[1].parse::<u64>().unwrap();
                seq = seq.max(x);
            }
        }
        // remove the last data file
        log::debug!("unlink {:?}", save.data_file(data_seq));
        let _ = std::fs::remove_file(save.data_file(data_seq));
        // break the last manifest
        {
            let f = File::options()
                .write(true)
                .open(save.manifest(seq))
                .unwrap();
            let size = f.metadata().unwrap().len();
            f.set_len(size - 10).unwrap();
        }
        // by the way, we assume log file is not cleaned, and we modify desc file to force it recover
        // from log file
        let mut w = WalDesc {
            checkpoint: Position {
                file_id: 0,
                offset: 0,
            },
            wal_id: 0,
            worker: 0,
            padding: 0,
            checksum: 0,
        };

        for i in 0..save.workers {
            w.worker = i as u16;
            w.write(save.desc_file(i as u16));
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
    imcomplete_manifest(&save);

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
    opt.data_file_size = 512 << 10;
    opt.wal_file_size = wal_len;
    let mut save = opt.clone();
    let db = Mace::new(opt.validate().unwrap()).unwrap();
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

    drop(kv);
    drop(db);

    imcomplete_manifest(&save);

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
    imcomplete_manifest(&save);

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

#[derive(Clone, Copy)]
#[repr(C)]
struct Position {
    file_id: u64,
    offset: u64,
}
#[derive(Clone, Copy)]
#[repr(C)]
struct WalDesc {
    checkpoint: Position,
    wal_id: u64,
    worker: u16,
    padding: u16,
    checksum: u32,
}

impl WalDesc {
    fn as_slice(&self) -> &[u8] {
        unsafe {
            let p = self as *const Self as *const u8;
            std::slice::from_raw_parts(p, size_of::<WalDesc>())
        }
    }

    fn crc32(&self) -> u32 {
        let s = self.as_slice();
        let src = &s[0..s.len() - size_of::<u32>()];
        let mut h = Crc32cHasher::default();
        h.write(src);
        h.finish() as u32
    }

    fn write<P>(&mut self, path: P)
    where
        P: AsRef<Path>,
    {
        let mut f = File::options()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .expect("can't open desc file");

        self.checksum = self.crc32();
        f.write_all(self.as_slice()).expect("can't write desc file");
        f.sync_all().expect("can't sync desc file");
    }
}
