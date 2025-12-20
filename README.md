# mace

Embedded key-value storage engine with ACID support, example see [demo.rs](./examples/demo.rs)

benchmark see [kv_bench](https://github.com/abbycin/kv_bench)

### features

- B+ tree like read performance with LSM tree write performance
- non-blocking concurrent reads and writes (MVCC)
- flash-optimized log-structured storage
- separate large value storage
- data integrity check
- cross-platform

### caveats

since it is still in the early stages, the storage format and API may change
