use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mace::{Mace, Options, RandomPath};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[derive(Clone)]
struct BenchConfig {
    total_items: usize,
    thread_counts: Vec<usize>,
    kv_sizes: Vec<(usize, usize)>,
    put_measure_secs: u64,
    get_measure_secs: u64,
    scan_measure_secs: u64,
    sample_size: usize,
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| {
            let value = value.trim().to_ascii_lowercase();
            match value.as_str() {
                "1" | "true" | "yes" | "on" => Some(true),
                "0" | "false" | "no" | "off" => Some(false),
                _ => None,
            }
        })
        .unwrap_or(default)
}

fn parse_thread_counts(default: &[usize]) -> Vec<usize> {
    std::env::var("MACE_BENCH_THREADS")
        .ok()
        .and_then(|value| {
            let parsed: Vec<usize> = value
                .split(',')
                .filter_map(|part| part.trim().parse::<usize>().ok())
                .filter(|&x| x > 0)
                .collect();
            if parsed.is_empty() {
                None
            } else {
                Some(parsed)
            }
        })
        .unwrap_or_else(|| default.to_vec())
}

fn parse_kv_sizes(default: &[(usize, usize)]) -> Vec<(usize, usize)> {
    std::env::var("MACE_BENCH_KV_CASES")
        .ok()
        .and_then(|value| {
            let parsed: Vec<(usize, usize)> = value
                .split(',')
                .filter_map(|item| {
                    let mut parts = item.split(':').map(|x| x.trim());
                    let key = parts.next()?.parse::<usize>().ok()?;
                    let val = parts.next()?.parse::<usize>().ok()?;
                    Some((key, val))
                })
                .collect();
            if parsed.is_empty() {
                None
            } else {
                Some(parsed)
            }
        })
        .unwrap_or_else(|| default.to_vec())
}

fn bench_config() -> BenchConfig {
    let quick = env_bool("MACE_BENCH_QUICK", false);

    let default_total_items = if quick { 20_000 } else { 100_000 };
    let default_threads: Vec<usize> = if quick { vec![1, 4] } else { vec![1, 2, 4, 8] };
    let default_kv_sizes: Vec<(usize, usize)> = if quick {
        vec![(16, 16), (100, 1024)]
    } else {
        vec![(16, 16), (100, 1024), (1024, 1024), (16, 10240)]
    };

    let default_put_measure_secs = if quick { 5 } else { 15 };
    let default_get_measure_secs = if quick { 5 } else { 10 };
    let default_scan_measure_secs = if quick { 5 } else { 10 };
    let default_sample_size = 10;

    BenchConfig {
        total_items: env_usize("MACE_BENCH_TOTAL_ITEMS", default_total_items),
        thread_counts: parse_thread_counts(&default_threads),
        kv_sizes: parse_kv_sizes(&default_kv_sizes),
        put_measure_secs: env_u64("MACE_BENCH_PUT_MEAS_SECS", default_put_measure_secs),
        get_measure_secs: env_u64("MACE_BENCH_GET_MEAS_SECS", default_get_measure_secs),
        scan_measure_secs: env_u64("MACE_BENCH_SCAN_MEAS_SECS", default_scan_measure_secs),
        sample_size: env_usize("MACE_BENCH_SAMPLE_SIZE", default_sample_size).max(10),
    }
}

fn setup_mace() -> (Mace, RandomPath) {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.cache_capacity = 1024 * 1024 * 1024;
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
    let cfg = bench_config();

    let mut group = c.benchmark_group("Put");
    group.throughput(Throughput::Elements(cfg.total_items as u64));
    group.measurement_time(Duration::from_secs(cfg.put_measure_secs));
    group.sample_size(cfg.sample_size);

    for (k_len, v_len) in cfg.kv_sizes {
        let size_label = format!("K{}-V{}", k_len, v_len);
        let keys = Arc::new(generate_keys(cfg.total_items, k_len));
        let value = Arc::new(vec![0u8; v_len]);

        for threads in &cfg.thread_counts {
            let t = *threads;
            group.bench_with_input(BenchmarkId::new(&size_label, t), &t, |b, &thread_cnt| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;
                    for _ in 0..iters {
                        let (mace, _path) = setup_mace();
                        let bucket = mace.new_bucket("bench").unwrap();
                        let barrier = Arc::new(Barrier::new(thread_cnt));
                        let mut handles = vec![];
                        let chunk_size = (cfg.total_items / thread_cnt).max(1);

                        let start = Instant::now();
                        for i in 0..thread_cnt {
                            let b = bucket.clone();
                            let keys = keys.clone();
                            let val = value.clone();
                            let br = barrier.clone();
                            handles.push(std::thread::spawn(move || {
                                br.wait();
                                let start_idx = i * chunk_size;
                                if start_idx >= keys.len() {
                                    return;
                                }
                                let end_idx = if i == thread_cnt - 1 {
                                    keys.len()
                                } else {
                                    ((i + 1) * chunk_size).min(keys.len())
                                };
                                for key in &keys[start_idx..end_idx] {
                                    let txn = b.begin().unwrap();
                                    txn.put(key, &*val).unwrap();
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
    let cfg = bench_config();

    let mut group = c.benchmark_group("Get");
    group.throughput(Throughput::Elements(cfg.total_items as u64));
    group.measurement_time(Duration::from_secs(cfg.get_measure_secs));
    group.sample_size(cfg.sample_size);

    for (k_len, v_len) in cfg.kv_sizes {
        let size_label = format!("K{}-V{}", k_len, v_len);
        let keys = Arc::new(generate_keys(cfg.total_items, k_len));
        let value = Arc::new(vec![0u8; v_len]);

        let (mace, _path) = setup_mace();
        let bucket = mace.new_bucket("bench").unwrap();
        {
            let txn = bucket.begin().unwrap();
            for key in keys.iter() {
                txn.put(key, &*value).unwrap();
            }
            txn.commit().unwrap();
        }
        let bucket_arc = Arc::new(bucket);

        for threads in &cfg.thread_counts {
            let t = *threads;
            group.bench_with_input(BenchmarkId::new(&size_label, t), &t, |b, &thread_cnt| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;
                    for _ in 0..iters {
                        let barrier = Arc::new(Barrier::new(thread_cnt));
                        let mut handles = vec![];
                        let chunk_size = (cfg.total_items / thread_cnt).max(1);

                        let start = Instant::now();
                        for i in 0..thread_cnt {
                            let b = bucket_arc.clone();
                            let keys = keys.clone();
                            let br = barrier.clone();
                            handles.push(std::thread::spawn(move || {
                                br.wait();
                                let start_idx = i * chunk_size;
                                if start_idx >= keys.len() {
                                    return;
                                }
                                let end_idx = if i == thread_cnt - 1 {
                                    keys.len()
                                } else {
                                    ((i + 1) * chunk_size).min(keys.len())
                                };
                                let view = b.view().unwrap();
                                for key in &keys[start_idx..end_idx] {
                                    let _v = view.get(key).unwrap();
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
    let cfg = bench_config();

    let mut group = c.benchmark_group("Scan");
    group.throughput(Throughput::Elements(cfg.total_items as u64));
    group.measurement_time(Duration::from_secs(cfg.scan_measure_secs));
    group.sample_size(cfg.sample_size);

    for (k_len, v_len) in cfg.kv_sizes {
        let size_label = format!("K{}-V{}", k_len, v_len);
        let keys = Arc::new(generate_keys(cfg.total_items, k_len));
        let value = Arc::new(vec![0u8; v_len]);

        let (mace, _path) = setup_mace();
        let bucket = mace.new_bucket("bench").unwrap();
        {
            let txn = bucket.begin().unwrap();
            for key in keys.iter() {
                txn.put(key, &*value).unwrap();
            }
            txn.commit().unwrap();
        }
        let bucket_arc = Arc::new(bucket);

        for threads in &cfg.thread_counts {
            let t = *threads;
            group.bench_with_input(BenchmarkId::new(&size_label, t), &t, |b, &thread_cnt| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;
                    for _ in 0..iters {
                        let barrier = Arc::new(Barrier::new(thread_cnt));
                        let mut handles = vec![];
                        let total_scans = (cfg.total_items / 100).max(thread_cnt);
                        let scans_per_thread = (total_scans / thread_cnt).max(1);

                        let start = Instant::now();
                        for i in 0..thread_cnt {
                            let b = bucket_arc.clone();
                            let keys = keys.clone();
                            let br = barrier.clone();
                            handles.push(std::thread::spawn(move || {
                                br.wait();
                                let view = b.view().unwrap();
                                for j in 0..scans_per_thread {
                                    let key_idx = ((i * scans_per_thread + j) * 100) % keys.len();
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
