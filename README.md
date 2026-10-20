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

## Status

Storage format and public APIs are essentially stable and ready for production evaluation.
Breaking format changes will ship with a migration path rather than silently.

> **Upgrading an existing database?** A breaking on-disk format change needs one offline rebuild
> before the newer release can open the database - see [Migrating a database](./docs/migration.md).

## Benchmarks

Mace vs RocksDB, benchmarked with [kv_bench](https://github.com/abbycin/kv_bench) on small values only (W1–W6: 16B keys / 128B values over 1M keys; merge workloads: 32B keys / 8B values with a u64 counter), relaxed durability, snapshot reads. Values are ops/s, higher is better; **bold** marks the faster engine for that thread count.

| Workload | Mace (1 thread) | RocksDB (1 thread) | Mace (8 threads) | RocksDB (8 threads) |
| :--- | ---: | ---: | ---: | ---: |
| W1: 95% read / 5% update | **541,950** | 281,074 | **2,941,301** | 1,100,399 |
| W2: W1 with Zipf access | **1,314,220** | 824,557 | **4,156,555** | 1,442,814 |
| W3: 50% read / 50% update | **265,720** | 184,274 | **1,306,037** | 662,721 |
| W4: 5% read / 95% update | **177,297** | 157,432 | **707,002** | 473,584 |
| W5: 70% read / 25% update / 5% scan | **261,862** | 163,576 | **1,475,692** | 729,926 |
| W6: 100% scan | **94,106** | 50,880 | **460,125** | 284,069 |
| Merge: 70% merge / 30% get | 210,386 | **253,281** | **1,085,001** | 524,390 |
| Merge: 100% merge | 192,250 | **282,164** | **842,411** | 418,602 |
| Get after 0 merges/key | **3,867,072** | 2,009,637 | **7,981,551** | 1,472,972 |
| Get after 1k merges/key | **1,806,531** | 29,866 | **4,038,680** | 180,538 |
| Get after 10k merges/key | **826,647** | 123,659 | **2,595,502** | 764,122 |

Mace wins every workload except single-threaded merges; gets over long merge chains are up to 60× faster.
Latest full results: https://abbycin.github.io/kv_bench/index.html

## Design Notes

Architecture and crash-safety notes are in [docs/design.md](./docs/design.md).

## Discussion

Use [GitHub Discussions](https://github.com/abbycin/mace/discussions) for questions and design
talk, or open an [issue](https://github.com/abbycin/mace/issues) for bugs and feature requests.

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
