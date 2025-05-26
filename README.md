# mace

an embedded key-value storage engine based on Bw-Tree and supporting ACID

example see [demo.rs](examples/demo.rs)

```
λ cargo run --example demo
   Compiling mace v0.0.3 (/home/workspace/gits/github/mace)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.82s
     Running `target/debug/examples/demo`
```

single-threaded read/write benchmarks see [bench.rs](tests/bench.rs)

```
λ cargo test --test bench --release -- --nocapture
   Compiling mace v0.0.7 (/home/workspace/gits/github/mace)
    Finished `release` profile [optimized] target(s) in 9.09s
     Running tests/bench.rs (target/release/deps/bench-e464b0deeba5a28d)

running 1 test
db_root "/tmp/mace_tmp_100400117343"
put       228ms
hot get   29ms
cold get  46ms
test bench ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.47s
```

### highlights

- B+ tree like read performance with LSM tree write performance
- non-blocking concurrent reads and writes (MVCC)
- scalable lock-free implementation
- flash-optimized log-structured storage
- cross-platform

### caveats

since it is still in the early stages, the storage format and API may change
