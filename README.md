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
   Compiling mace v0.0.3 (/home/workspace/gits/github/mace)
    Finished `release` profile [optimized] target(s) in 7.89s
     Running tests/bench.rs (target/release/deps/bench-accc8d2811434c5e)

running 1 test
db_root "/tmp/mace_tmp_169093"
put 443ms get 35ms
test bench ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.58s
```

### highlights

- B+ tree like read performance with LSM tree write performance
- non-blocking concurrent reads and writes (MVCC)
- scalable lock-free implementation
- flash-optimized log-structured storage
- cross-platform

### limitations

sine each transaction is bound to a worker, and the default number of workers is the number of CPU cores, it is recommended not to start multiple transactions in the same thread at the same time     

in addition, configuring workers that exceed the number of CPU cores may cause performance degradation

### caveats

since it is still in the early stages, the storage format and API may change
