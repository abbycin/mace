# Mace

[![CI](https://github.com/abbycin/mace/actions/workflows/ci.yml/badge.svg)](https://github.com/abbycin/mace/actions)
[![Crates.io](https://img.shields.io/crates/v/mace-kv.svg)](https://crates.io/crates/mace-kv)
[![License](https://img.shields.io/crates/l/mace-kv.svg)](./LICENSE)

Mace is a high-performance, embedded key-value storage engine written in Rust. It is designed to combine the predictable read performance of B+ Trees with the high write throughput of LSM Trees.

Built for modern storage hardware, Mace employs a flash-optimized log-structured architecture and implements Key-Value separation (Blob storage) to minimize write amplification and compaction overhead.

## Key Features

- **Hybrid Performance**: Achieves B+ Tree-like read speeds alongside LSM-Tree-like write performance.
- **Concurrent MVCC**: Supports non-blocking concurrent reads and writes through Multi-Version Concurrency Control.
- **Flash-Optimized**: Log-structured design specifically tailored for SSD/NVMe endurance and performance.
- **Large Value Separation**: Efficiently handles large values by separating them from the indexing structure, significantly reducing I/O overhead during maintenance.
- **ACID Transactions**: Full support for Atomicity, Consistency, Isolation, and Durability.
- **Data Integrity**: Integrated CRC checksums ensure data remains uncorrupted across restarts and crashes.
- **Flow Control**: Optional foreground write backpressure to prevent memory pressure spikes.
- **Cross-Platform**: Native support for Linux, Windows, and macOS.

## Installation

Add mace-kv to your Cargo.toml:

```bash
cargo add mace-kv
```

## Quick Start

The following example demonstrates basic transaction management and data retrieval:

```rust
use mace::{Mace, OpCode, Options};

fn main() -> Result<(), OpCode> {
    // 1. Initialize the storage
    let opts = Options::new("./data_dir");
    let db = Mace::new(opts.validate().unwrap())?;
    let bkt = db.new_bucket("tmp")?;

    // 2. Perform a write transaction
    let txn = bkt.begin()?;
    txn.put("user:1", "alice")?;
    txn.put("user:2", "bob")?;
    txn.commit()?;

    // 3. Read data using a consistent view
    let view = bkt.view()?;
    let value = view.get("user:1")?;
    println!("Value for user:1: {:?}", std::str::from_utf8(value.slice()));

    // 4. Remove data
    let txn = bkt.begin()?;
    txn.del("user:2")?;
    txn.commit()?;

    Ok(())
}
```

Detailed usage can be found in [examples/demo.rs](./examples/demo.rs).

## Benchmarks

Mace is engineered for heavy workloads. For detailed performance analysis and comparison with other engines, refer to the [kv_bench](https://github.com/abbycin/kv_bench) repository. The summary below is from `kv_bench`, comparing `Mace 0.0.29` with `RocksDB 10.4.2`.

### Benchmark Summary

| Workload | Mace wins (ops) | ops median ratio (Mace/RocksDB) | Mace wins (p99) | p99 median ratio (Mace/RocksDB) |
|---|---:|---:|---:|--:|
| `W1` (95R/5U, uniform) | 16 / 16 | **2.3x** | 5 / 16 | **1.0x** |
| `W2` (95R/5U, zipf) | 16 / 16 | **1.5x** | 11 / 16 | **0.5x** |
| `W3` (50R/50U) | 15 / 16 | **1.4x** | 9 / 16 | **0.5x** |
| `W4` (5R/95U) | 12 / 16 | **1.3x** | 7 / 16 | **1.0x** |
| `W5` (70R/25U/5S) | 15 / 16 | **2.1x** | 16 / 16 | **0.2x** |
| `W6` (100% scan) | 16 / 16 | **4.6x** | 15 / 16 | **0.2x** |

> Note: for `ops` median ratio (`Mace/RocksDB`), larger means higher Mace throughput. For `p99` median ratio (`Mace/RocksDB`), smaller means lower Mace tail latency.

### Test Environment

- OS: openSUSE Tumbleweed
- CPU: AMD Ryzen 5 3600 (6 cores / 12 threads)
- Memory: 32 GiB RAM
- Kernel: Linux `6.19.12-1-default`
- Filesystem: `xfs` (`/dev/nvme1n1p4`, mounted at `/nvme`)
- SSD: ZHITAI TiPlus5000 1TB

## Validation

- Correctness/crash matrix: `./scripts/prod_test.sh all 8`
- Perf regression gate: `./scripts/perf_gate.sh compare`
- Script details: [scripts/README.md](./scripts/README.md)

## Design Notes

Architecture and crash-safety notes are in [docs/design.md](./docs/design.md).

## Status

`mace-kv` is still pre-1.0. Storage format and APIs can change between minor versions.

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
