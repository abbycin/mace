# mace

An embedded key-value storage engine based on Bw-Tree and supporting ACID

example see [demo.rs](examples/demo.rs)

```
cargo run --example demo
```

single-threaded read/write benchmarks see [bench.rs](tests/bench.rs)

```
cargo test --test bench --release -- --nocapture
```

### highlights

- B+ tree like read performance with LSM tree write performance
- non-blocking concurrent reads and writes (MVCC)
- scalable lock-free implementation
- flash-optimized log-structured storage

### caveats

still in the very early stages
