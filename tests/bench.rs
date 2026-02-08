use std::{ops::Deref, time::Instant};

use mace::{Mace, Options, RandomPath};

#[test]
fn bench() {
    let path = RandomPath::new();
    println!("db_root {:?}", path.deref());
    let mut opt = Options::new(&*path);
    opt.sync_on_write = false;
    let mut copy = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();

    let cap = 100000;
    let mut pair = Vec::with_capacity(cap);

    for i in 0..cap {
        // let mut tmp = format!("key_{i}").into_bytes();
        // tmp.resize(1024, b'x');
        let tmp = format!("{i:08}");
        pair.push(tmp);
    }

    let b = Instant::now();
    for k in &pair {
        let kv = db.begin().unwrap();
        kv.put(k, k).unwrap();
        kv.commit().unwrap();
    }

    let e1 = b.elapsed();

    let b = Instant::now();
    for k in &pair {
        let view = db.view().unwrap();
        view.get(k).unwrap();
    }

    let e2 = b.elapsed();

    drop(db);
    drop(mace);

    copy.tmp_store = true;
    let mace = Mace::new(copy.validate().unwrap()).unwrap();
    let db = mace.get_bucket("x").unwrap();

    let b = Instant::now();
    for k in &pair {
        let view = db.view().unwrap();
        view.get(k).unwrap();
    }

    let e3 = b.elapsed();
    println!(
        "{:<10}{}ms\n{:<10}{}ms\n{:<10}{}ms",
        "put",
        e1.as_millis(),
        "hot get",
        e2.as_millis(),
        "cold get",
        e3.as_millis()
    );
}
