use std::{ops::Deref, time::Instant};

use mace::{Mace, Options, RandomPath};

#[test]
fn bench() {
    let path = RandomPath::new();
    println!("db_root {:?}", path.deref());
    let mut opt = Options::new(&*path);
    opt.sync_on_write = false;
    let mut copy = opt.clone();
    let db = Mace::new(opt).unwrap();

    let cap = 100000;
    let mut pair = Vec::with_capacity(cap);

    for i in 0..cap {
        pair.push(format!("{:08}", i));
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
    copy.tmp_store = true;
    let db = Mace::new(copy).unwrap();

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
