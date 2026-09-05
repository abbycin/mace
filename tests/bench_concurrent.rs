use std::{
    hint::black_box,
    ops::Deref,
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

use mace::{BucketOptions, Mace, Options, RandomPath};

/// concurrent workload bench: stresses the shared atomics and pool locks that
/// the instance-local default-on path removes contention from.
fn setup() -> (RandomPath, Options) {
    let path = RandomPath::new();
    let mut opt = Options::new(path.deref());
    opt.sync_on_write = false;
    (path, opt)
}

fn seed(mace: &Mace, bucket: &str, n: usize) {
    let db = mace.new_bucket(bucket, BucketOptions::new()).unwrap();
    for i in 0..n {
        let key = format!("{i:08}");
        let kv = db.begin().unwrap();
        kv.put(key.as_bytes(), key.as_bytes()).unwrap();
        kv.commit().unwrap();
    }
    drop(db);
}

fn concurrent_puts(mace: &Mace, bucket: &str, threads: usize, per_thread: usize) -> Duration {
    let db = mace.open_bucket(bucket).unwrap();
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut workers = Vec::with_capacity(threads);
    for t in 0..threads {
        let db = db.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            for i in 0..per_thread {
                // distinct key space per thread: no cross-thread conflicts
                let key = format!("{t}_{i:08}");
                let kv = db.begin().unwrap();
                kv.put(key.as_bytes(), key.as_bytes()).unwrap();
                kv.commit().unwrap();
            }
        }));
    }
    barrier.wait();
    let started = Instant::now();
    for w in workers {
        w.join().unwrap();
    }
    started.elapsed()
}

fn concurrent_views(mace: &Mace, bucket: &str, threads: usize, per_thread: usize) -> Duration {
    let db = mace.open_bucket(bucket).unwrap();
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut workers = Vec::with_capacity(threads);
    for _ in 0..threads {
        let db = db.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            for i in 0..per_thread {
                // round-robin over the seeded single-writer keys
                let key = format!("{:08}", i % 100000);
                let view = db.view().unwrap();
                let v = view.get(key.as_bytes()).unwrap();
                black_box(v.slice());
                drop(view);
            }
        }));
    }
    barrier.wait();
    let started = Instant::now();
    for w in workers {
        w.join().unwrap();
    }
    started.elapsed()
}

#[test]
#[ignore]
fn bench_concurrent() {
    const THREADS: usize = 8;
    const PUT_PER_THREAD: usize = 200_000; // 1.6M txns total
    const VIEW_PER_THREAD: usize = 400_000; // 3.2M short views total

    let (_path, opt) = setup();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();

    let t_seed = Instant::now();
    seed(&mace, "seed", 100_000);
    println!("seed 100k: {}ms", t_seed.elapsed().as_millis());

    // concurrent puts go to a fresh empty bucket
    let db = mace.new_bucket("writes", BucketOptions::new()).unwrap();
    drop(db);

    let e_put = concurrent_puts(&mace, "writes", THREADS, PUT_PER_THREAD);
    println!(
        "concurrent put {THREADS}x{PUT_PER_THREAD}: {}ms ({:.0} txn/s)",
        e_put.as_millis(),
        (THREADS * PUT_PER_THREAD) as f64 / e_put.as_secs_f64()
    );

    let e_view = concurrent_views(&mace, "seed", THREADS, VIEW_PER_THREAD);
    println!(
        "concurrent short views {THREADS}x{VIEW_PER_THREAD}: {}ms ({:.0} view/s)",
        e_view.as_millis(),
        (THREADS * VIEW_PER_THREAD) as f64 / e_view.as_secs_f64()
    );

    drop(mace);
}
