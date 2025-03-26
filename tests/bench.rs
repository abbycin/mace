use std::{ops::Deref, time::Instant};

use mace::{Mace, Options, RandomPath};

#[test]
fn bench() {
    let path = RandomPath::new();
    println!("db_root {:?}", path.deref());
    let mut opt = Options::new(&*path);
    opt.sync_on_write = false;
    opt.tmp_store = true;
    let db = Mace::new(opt).unwrap();

    let cap = 100000;
    let mut pair = Vec::with_capacity(cap);

    for i in 0..cap {
        pair.push(format!("{:06}", i));
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
    println!("put {}ms get {}ms", e1.as_millis(), e2.as_millis());
}
