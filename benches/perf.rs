use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mace::{Mace, Options, RandomPath};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const TOTAL_ITEMS: usize = 100_000;

fn setup_mace() -> (Mace, RandomPath) {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false; // Disable fsync for benchmarking engine throughput
    opt.cache_capacity = 1024 * 1024 * 1024; // 1GB
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    (mace, path)
}

fn generate_keys(count: usize, len: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let mut tmp = format!("key_{}", i).into_bytes();
            tmp.resize(len, b'x');
            tmp
        })
        .collect()
}

fn bench_put(c: &mut Criterion) {
    let thread_counts = [1, 2, 4, 8];
    let kv_sizes = [(16, 16), (100, 1024), (1024, 1024), (16, 10240)];

    let mut group = c.benchmark_group("Put");
    group.throughput(Throughput::Elements(TOTAL_ITEMS as u64));
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10);

    for (k_len, v_len) in kv_sizes {
        let size_label = format!("K{}-V{}", k_len, v_len);
        let keys = Arc::new(generate_keys(TOTAL_ITEMS, k_len));
        let value = Arc::new(vec![0u8; v_len]);

        for threads in thread_counts {
            group.bench_with_input(BenchmarkId::new(&size_label, threads), &threads, |b, &t| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;
                    for _ in 0..iters {
                        let (mace, _path) = setup_mace();
                        let bucket = mace.new_bucket("bench").unwrap();
                        let barrier = Arc::new(Barrier::new(t));
                        let mut handles = vec![];
                        let chunk_size = TOTAL_ITEMS / t;

                        let start = Instant::now();
                        for i in 0..t {
                            let b = bucket.clone();
                            let keys = keys.clone();
                            let val = value.clone();
                            let br = barrier.clone();
                            handles.push(std::thread::spawn(move || {
                                br.wait();
                                let start_idx = i * chunk_size;
                                let end_idx = if i == t - 1 {
                                    TOTAL_ITEMS
                                } else {
                                    (i + 1) * chunk_size
                                };
                                for j in start_idx..end_idx {
                                    let txn = b.begin().unwrap();
                                    txn.put(&keys[j], &*val).unwrap();
                                    txn.commit().unwrap();
                                }
                            }));
                        }
                        for h in handles {
                            h.join().unwrap();
                        }
                        total_duration += start.elapsed();
                    }
                    total_duration
                });
            });
        }
    }
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let thread_counts = [1, 2, 4, 8];
    let kv_sizes = [(16, 16), (100, 1024), (1024, 1024), (16, 10240)];

    let mut group = c.benchmark_group("Get");
    group.throughput(Throughput::Elements(TOTAL_ITEMS as u64));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    for (k_len, v_len) in kv_sizes {
        let size_label = format!("K{}-V{}", k_len, v_len);
        let keys = Arc::new(generate_keys(TOTAL_ITEMS, k_len));
        let value = Arc::new(vec![0u8; v_len]);

        // Pre-fill
        let (mace, _path) = setup_mace();
        let bucket = mace.new_bucket("bench").unwrap();
        {
            let txn = bucket.begin().unwrap();
            for i in 0..TOTAL_ITEMS {
                txn.put(&keys[i], &*value).unwrap();
            }
            txn.commit().unwrap();
        }
        let bucket_arc = Arc::new(bucket);

        for threads in thread_counts {
            group.bench_with_input(BenchmarkId::new(&size_label, threads), &threads, |b, &t| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;
                    for _ in 0..iters {
                        let barrier = Arc::new(Barrier::new(t));
                        let mut handles = vec![];
                        let chunk_size = TOTAL_ITEMS / t;

                        let start = Instant::now();
                        for i in 0..t {
                            let b = bucket_arc.clone();
                            let keys = keys.clone();
                            let br = barrier.clone();
                            handles.push(std::thread::spawn(move || {
                                br.wait();
                                let start_idx = i * chunk_size;
                                let end_idx = if i == t - 1 {
                                    TOTAL_ITEMS
                                } else {
                                    (i + 1) * chunk_size
                                };
                                let view = b.view().unwrap();
                                for j in start_idx..end_idx {
                                    let _v = view.get(&keys[j]).unwrap();
                                }
                            }));
                        }
                        for h in handles {
                            h.join().unwrap();
                        }
                        total_duration += start.elapsed();
                    }
                    total_duration
                });
            });
        }
    }
    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let thread_counts = [1, 2, 4, 8];
    let kv_sizes = [(16, 16), (100, 1024), (1024, 1024), (16, 10240)];

    let mut group = c.benchmark_group("Scan");
    group.throughput(Throughput::Elements(TOTAL_ITEMS as u64));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    for (k_len, v_len) in kv_sizes {
        let size_label = format!("K{}-V{}", k_len, v_len);
        let keys = Arc::new(generate_keys(TOTAL_ITEMS, k_len));
        let value = Arc::new(vec![0u8; v_len]);

        let (mace, _path) = setup_mace();
        let bucket = mace.new_bucket("bench").unwrap();
        {
            let txn = bucket.begin().unwrap();
            for i in 0..TOTAL_ITEMS {
                txn.put(&keys[i], &*value).unwrap();
            }
            txn.commit().unwrap();
        }
        let bucket_arc = Arc::new(bucket);

        for threads in thread_counts {
            group.bench_with_input(BenchmarkId::new(&size_label, threads), &threads, |b, &t| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;
                    for _ in 0..iters {
                        let barrier = Arc::new(Barrier::new(t));
                        let mut handles = vec![];
                        // each scan reads 100 items, so we need TOTAL_ITEMS / 100 total scans
                        let total_scans = TOTAL_ITEMS / 100;
                        let scans_per_thread = total_scans / t;

                        let start = Instant::now();
                        for i in 0..t {
                            let b = bucket_arc.clone();
                            let keys = keys.clone();
                            let br = barrier.clone();
                            handles.push(std::thread::spawn(move || {
                                br.wait();
                                let view = b.view().unwrap();
                                for j in 0..scans_per_thread {
                                    let key_idx = (i * scans_per_thread + j) * 100;
                                    let mut iter = view.seek(&keys[key_idx]);
                                    for _ in 0..100 {
                                        if iter.next().is_none() {
                                            break;
                                        }
                                    }
                                }
                            }));
                        }
                        for h in handles {
                            h.join().unwrap();
                        }
                        total_duration += start.elapsed();
                    }
                    total_duration
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_put, bench_get, bench_scan);
criterion_main!(benches);
