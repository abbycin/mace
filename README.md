# mace

Embedded key-value storage engine with ACID support, example see [demo.rs](./examples/demo.rs)

### highlights

- B+ tree like read performance with LSM tree write performance
- non-blocking concurrent reads and writes (MVCC)
- scalable lock-free implementation
- flash-optimized log-structured storage
- cross-platform

### caveats

since it is still in the early stages, the storage format and API may change
