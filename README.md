# Mace

[![CI](https://github.com/abbycin/mace/actions/workflows/ci.yml/badge.svg)](https://github.com/abbycin/mace/actions)
[![Crates.io](https://img.shields.io/crates/v/mace-kv.svg)](https://crates.io/crates/mace-kv)
[![License](https://img.shields.io/crates/l/mace-kv.svg)](./LICENSE)

Mace is a high-performance, embedded key-value storage engine written in Rust, combining the predictable read performance of B+ Trees with the write throughput of LSM Trees.

## Key Features

- **Hybrid Performance**: B+ Tree-like read speeds with LSM-Tree-like write throughput.
- **Concurrent MVCC**: Non-blocking concurrent reads and writes with snapshot isolation.
- **ACID Transactions**: Crash-safe commit, abort, and recovery.
- **Flash-Optimized**: Log-structured design tailored for SSD/NVMe endurance.
- **Large Value Separation**: Large values live outside the index, cutting maintenance I/O.
- **Optional Zstd Compression**: Per-bucket `enable_compression` (default: `false`).
- **Data Integrity**: CRC checksums on persisted records, verified across restarts and crashes.
- **Flow Control**: Optional foreground write backpressure to bound memory growth.
- **Cross-Platform**: Linux, Windows, and macOS.
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

Latest results: https://o2c.fun/benchmark.html

Methodology and comparison with other engines: [kv_bench](https://github.com/abbycin/kv_bench).

## Validation

- Correctness/crash matrix: `./scripts/prod_test.sh all 8`
- Script details: [scripts/README.md](./scripts/README.md)

### Stateful Fuzzing

Mace uses `cargo-fuzz` for stateful lifecycle fuzzing. The target is not decoder robustness against
random bytes, but whether Mace's own write, checkpoint, GC, reopen, and bucket-lifecycle paths can
produce state that later becomes unreadable, invisible, or inconsistent:

- lagging snapshot views must not lose versions that should still be visible
- checkpoint and reopen must not make committed state disappear or change visibility
- publish, GC, and bucket churn must not produce broken metadata or self-inconsistent durable state

Targets and replay commands are in [fuzz/README.md](./fuzz/README.md).

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
