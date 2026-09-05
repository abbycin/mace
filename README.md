# Mace

[![CI](https://github.com/abbycin/mace/actions/workflows/ci.yml/badge.svg)](https://github.com/abbycin/mace/actions)
[![Crates.io](https://img.shields.io/crates/v/mace-kv.svg)](https://crates.io/crates/mace-kv)
[![License](https://img.shields.io/crates/l/mace-kv.svg)](./LICENSE)

Mace is a high-performance, embedded key-value database written in Rust, combining the predictable read performance of B+ Trees with the write throughput of LSM Trees.

## Key Features

- **Hybrid Performance**: B+ Tree-like read speeds with LSM-Tree-like write throughput.
- **Concurrent MVCC**: Non-blocking concurrent reads and writes with snapshot isolation.
- **ACID Transactions**: Crash-safe commit, abort, and recovery.
- **Flash-Optimized**: Log-structured design tailored for SSD/NVMe endurance.
- **Large Value Separation**: Large values live outside the index, cutting maintenance I/O.
- **Merge Operators**: Optional per-bucket operators support durable merge operands and reads.
- **Optional Zstd Compression**: Per-bucket `enable_compression` (default: `false`).
- **Data Integrity**: CRC checksums on persisted records, verified across restarts and crashes.
- **Flow Control**: Optional foreground write backpressure to bound memory growth.
- **Cross-Platform**: Linux, Windows, FreeBSD and macOS.

## Quick Start

The following example demonstrates basic transaction management and data retrieval:

```rust
use mace::{BucketOptions, Mace, OpCode, Options};

fn main() -> Result<(), OpCode> {
    // 1. Initialize the storage
    let opts = Options::new("./data_dir");
    let db = Mace::new(opts.validate().unwrap())?;
    let bkt = db.new_bucket("tmp", BucketOptions::default())?;

    // 2. Perform a write transaction
    let txn = bkt.begin()?;
    txn.put("moha", "+1s")?;
    txn.commit()?;

    // 3. Read data using a consistent view
    let view = bkt.view()?;
    let value = view.get("moha")?;
    println!("moha => {:?}", std::str::from_utf8(value.slice()));

    // 4. Remove data
    let txn = bkt.begin()?;
    txn.del("moha")?;
    txn.commit()?;

    Ok(())
}
```

Additional runnable examples are available in the [examples/](./examples/) directory.

## Benchmarks

Mace vs RocksDB on small values, relaxed durability (snapshot reads):

| Workload | Mace ops/s | RocksDB ops/s | mace/rocksdb ops | Mace p99 | RocksDB p99 |
| --- | :--- | :--- | :--- | :--- | :--- |
| W1 (95% Read, 5% Update, 1 thread) | **541,950** | 281,074 | 1.93x | **4** | 8 |
| W1 (95% Read, 5% Update, 8 threads) | **2,941,301** | 1,100,399 | 2.67x | **8** | 16 |
| W2 (95% Read, 5% Update, zipf, 1 thread) | **1,314,220** | 824,557 | 1.59x | **2** | 4 |
| W2 (95% Read, 5% Update, zipf, 8 threads) | **4,156,555** | 1,442,814 | 2.88x | **4** | 16 |
| W3 (50% Read, 50% Update, 1 thread) | **265,720** | 184,274 | 1.44x | **4** | 8 |
| W3 (50% Read, 50% Update, 8 threads) | **1,306,037** | 662,721 | 1.97x | **8** | 16 |
| W4 (5% Read, 95% Update, 1 thread) | **177,297** | 157,432 | 1.13x | **8** | **8** |
| W4 (5% Read, 95% Update, 8 threads) | **707,002** | 473,584 | 1.49x | **16** | **16** |
| W5 (70% Read, 25% Update, 5% Scan, 1 thread) | **261,862** | 163,576 | 1.60x | **16** | **16** |
| W5 (70% Read, 25% Update, 5% Scan, 8 threads) | **1,475,692** | 729,926 | 2.02x | **16** | 32 |
| W6 (100% Scan, 1 thread) | **94,106** | 50,880 | 1.85x | **8** | 16 |
| W6 (100% Scan, 8 threads) | **460,125** | 284,069 | 1.62x | **16** | 32 |

_Dataset: 16B key / 128B value, 1M keys, relaxed durability, snapshot reads._

Merge-operator workloads — u64 counter: merge (+1) vs get, fixed 8-byte values, 32-byte keys:

| Workload | Mace ops/s | RocksDB ops/s | mace/rocksdb ops | Mace p99 | RocksDB p99 |
| --- | :--- | :--- | :--- | :--- | :--- |
| MERGE_70 (30% get, 70% merge), 1 thread | 210,386 | **253,281** | 0.83x | **4** | 8 |
| MERGE_70 (30% get, 70% merge), 8 threads | **1,085,001** | 524,390 | 2.07x | **8** | 16 |
| MERGE_100 (100% merge), 1 thread | 192,250 | **282,164** | 0.68x | 8 | **4** |
| MERGE_100 (100% merge), 8 threads | **842,411** | 418,602 | 2.01x | **8** | 16 |
| MERGE_GET_0 (get after 0 merges/key), 1 thread | **3,867,072** | 2,009,637 | 1.92x | **1** | **1** |
| MERGE_GET_0 (get after 0 merges/key), 8 threads | **7,981,551** | 1,472,972 | 5.42x | **1** | 16 |
| MERGE_GET_1000 (get after 1k merges/key), 1 thread | **1,806,531** | 29,866 | 60.49x | **2** | 32 |
| MERGE_GET_1000 (get after 1k merges/key), 8 threads | **4,038,680** | 180,538 | 22.37x | **32** | 64 |
| MERGE_GET_10000 (get after 10k merges/key), 1 thread | **826,647** | 123,659 | 6.68x | **64** | 256 |
| MERGE_GET_10000 (get after 10k merges/key), 8 threads | **2,595,502** | 764,122 | 3.40x | **128** | **128** |

_ops/s = successful operations per second (higher is better); p99 = 99th-percentile latency in µs (lower is better). Bold marks the best value per row._

These tables cover small-value workloads; results for larger values and write-heavy
workloads can differ, and the full sweep — all value sizes, thread counts 1/2/4/8,
and merge operators — is in the latest full results below.

Latest full results: https://abbycin.github.io/kv_bench/index.html

Methodology and comparison with other engines: [kv_bench](https://github.com/abbycin/kv_bench).

## Design Notes

Architecture and crash-safety notes are in [docs/design.md](./docs/design.md).

## Status

Storage format and public APIs are essentially stable and ready for production evaluation.
Breaking format changes will ship with a migration path rather than silently.

## Discussion

Use [GitHub Discussions](https://github.com/abbycin/mace/discussions) for questions and design
talk, or open an [issue](https://github.com/abbycin/mace/issues) for bugs and feature requests.

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
