# Mace

[![CI](https://github.com/abbycin/mace/actions/workflows/ci.yml/badge.svg)](https://github.com/abbycin/mace/actions)
[![Crates.io](https://img.shields.io/crates/v/mace-kv.svg)](https://crates.io/crates/mace-kv)
[![License](https://img.shields.io/crates/l/mace-kv.svg)](./LICENSE)

Mace is a high-performance, embedded key-value storage engine written in Rust. It is designed to combine the predictable read performance of B+ Trees with the high write throughput of LSM Trees.

## Key Features

- **Hybrid Performance**: Achieves B+ Tree-like read speeds alongside LSM-Tree-like write performance.
- **Concurrent MVCC**: Supports non-blocking concurrent reads and writes through Multi-Version Concurrency Control.
- **Flash-Optimized**: Log-structured design specifically tailored for SSD/NVMe endurance and performance.
- **Large Value Separation**: Efficiently handles large values by separating them from the indexing structure, significantly reducing I/O overhead during maintenance.
- **Optional Zstd Compression**: Supports zstd compression for persisted data and blob records through bucket option `enable_compression` (default: `false`).
- **ACID Transactions**: Full support for Atomicity, Consistency, Isolation, and Durability.
- **Data Integrity**: Integrated CRC checksums ensure data remains uncorrupted across restarts and crashes.
- **Flow Control**: Optional foreground write backpressure to prevent memory pressure spikes.
- **Cross-Platform**: Native support for Linux, Windows, and macOS.

## Installation

```bash
cargo add mace-kv
```

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

Detailed usage can be found in [examples/demo.rs](./examples/demo.rs).

## Benchmarks

Recent complete benchmark results: https://o2c.fun/benchmark.html

For detailed performance analysis and comparison with other engines, refer to the [kv_bench](https://github.com/abbycin/kv_bench) repository.

### Test Environment

- OS: openSUSE Tumbleweed
- CPU: AMD Ryzen 5 3600 (6 cores / 12 threads)
- Memory: 32 GiB RAM
- Filesystem: `xfs` (`/dev/nvme1n1p4`, mounted at `/nvme`)
- SSD: ZHITAI TiPlus5000 1TB

## Validation

- Correctness/crash matrix: `./scripts/prod_test.sh all 8`
- Script details: [scripts/README.md](./scripts/README.md)

### Stateful Fuzzing

Mace uses `cargo-fuzz` for stateful lifecycle fuzzing.

The goal here is not "feed invalid bytes into a decoder and see whether it crashes". For this
project, the more important question is whether Mace's own write, checkpoint, GC, reopen, and
bucket lifecycle can produce state that later becomes unreadable, invisible, or inconsistent for
lagging views and recovery.

The focus is on lifecycle closure:

- lagging snapshot views must not lose versions that should still be visible
- checkpoint and reopen must not make committed state disappear or change visibility
- publish, GC, and bucket churn must not produce broken metadata, stale runtime state, or
  self-inconsistent durable state

Detailed usage, targets, and replay commands are in [fuzz/README.md](./fuzz/README.md).

## Design Notes

Architecture and crash-safety notes are in [docs/design.md](./docs/design.md).

## Status

`mace` is not stable yet. Storage format and APIs can change between minor versions.

## Discussion

If you want to join the discussion, you are welcome to join the QQ group: `1023032506`.

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
