## [0.0.10] 2025-08-10
### New Features
- Support prefix encoding

### Changes
- Removed all xxRef except BoxRef
- Unified BoxRef management, which greatly reduces the occurrence of cloning, especially in ImTree
- Disabled cc compact for read-only txn
- Replaced the `Queue` implementation to improve concurrency 

## [0.0.9] 2025-08-03
### Changes
- Replace delta-chain implementation to ImTree
- Abstract `Node` implementation
- Save remote addresses directly into sst for quickly recycle
- Reduce garbage creation

### Bug fixes
- Fix `LruInner::get` data race
- Fix `log.rs` rollback lsn point to junk data and subtraction overflow

## [0.0.8] 2025-05-29
### Changes
- Separate `map` from `data` file
- Support `map` file compaction

### Bug fixes
- Fix `BitMap` bits calculation

## [0.0.7] 2025-05-26
### Changes
- `consolidation` will collect remote frames
- Simplify wal file cleaning
- Optimize branch misses by rearranging code layout

### Bug fixes
- Fix typo in `need_split`
- Fix `last_ckpt` init in `log.rs`
- Fix `use-after-free` issue in consolidate

## [0.0.6] 2025-05-25
### New Features
- Support key-value size up to half `buffer_size` (it may cause up to 7% degradation in read/write performance, we will try to optimize it in subsequent versions)

### Bug fixes
- Fix a `use-after-free` issue that may occur when manually drop db instance

## [0.0.5] 2025-05-16
### New Features
- Support `scalable-logging`
