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

Mace is engineered for heavy workloads. For detailed performance analysis and comparison with other engines, refer to the [kv_bench](https://github.com/abbycin/kv_bench) repository.

## Status and Caveats

Mace is currently in early development. While the core engine is stable for testing, the storage format and API are subject to change until the 1.0 release. It is recommended to perform thorough testing before using it in critical production systems.

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.