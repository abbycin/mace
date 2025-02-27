use std::{ops::Deref, time::Instant};

use mace::{IsolationLevel, Mace, Options, RandomPath};

#[test]
fn bench() {
    let path = RandomPath::new();
    println!("db_root {:?}", path.deref());
    let mut opt = Options::new(&*path);
    opt.sync_on_write = false;
    opt.tmp_store = true;
    let db = Mace::new(opt).unwrap();

    let tx = db.default();
    let cap = 100000;
    let mut pair = Vec::with_capacity(cap);

    for i in 0..cap {
        pair.push(format!("{:06}", i));
    }

    let b = Instant::now();
    for k in &pair {
        tx.begin(IsolationLevel::SI, |kv| {
            kv.put(k, k)?;
            kv.commit()
        })
        .unwrap();
    }

    let e1 = b.elapsed();

    let b = Instant::now();
    for k in &pair {
        tx.view(IsolationLevel::SI, |view| {
            view.get(k)?;
            Ok(())
        })
        .unwrap();
    }

    let e2 = b.elapsed();
    println!("put {} get {}", e1.as_millis(), e2.as_millis());
}
