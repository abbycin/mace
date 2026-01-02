## [0.0.23] 2026-01-02
### Changes
- Separate Read-Only transaction from worker, use a pool which can dynamic allocate new ConcurrencyControl on demand
- Share ConcurrencyControl between `Iter` and it's creator

### Bug fixes
- Crash when `extra_check` feature enabled caused by `FuseBaseIter` may return duplicated keys
- Crash in `rollback` test. When the ring buffer was filled exactly to the boundary (modulo capacity), the subsequent allocation would start at index 0 without flushing the previous data. This caused flush to attempt to slice a contiguous range that actually wrapped around the physical buffer, leading to an out-of-bounds access.
- Incorrect condition check in `Cache::evict` trigger assertion in evictor
- `Tree::traverse_sibling` not walk through candidate versions

## [0.0.22] 2025-12-26
### Changes
- Stop share cache_key with lo, because it's obscure and hard to understand
### Bug fixes
- Fxied Iter can return wrong key-value, because when newer version of key is in delta it's not separated to prefix + base, so store base in Filter is wrong, change to store full key
- Fxied `Node::find_latest`, the expect behavior is return latest version of data based on given key and version, the original implementation use binary search which is wrong when key + version is not exist in ImTree, use `range_from` instead

## [0.0.21] 2025-12-21
### Changes
- Share cache_key with lo in Iter to avoid extra allocation

### Bug fixes
- Fixed scan crash when key in base was updated or deleted (or both), it's possible when a node was scanned and swith to another node and the key in that node was changed by other transaction

## [0.0.20] 2025-12-20
### Changes
- Optimize prefix scanning performance

## [0.0.19] 2025-11-17
### Bug fixes
- Fixed compile error introduced in 0.0.18 on Windows

## [0.0.18] 2025-11-17
### Changes
- Reduce `Reloc` size

## [0.0.17] 2025-11-16
### New Features
- Support value separate storage by default, user can set `Options::inline_size` to control whether value should be stored in blob files

### Changes
- Support verify checksum everytime when load data from disk (including GC)
- The data file footer has been changed (swapped relocations and intervals)
- Add priority LRU cache to support cache blob (which is Low priority)

### Bug fixes
- Fxied `index out of bound error` in builder.rs:redo_impl by never `clear` the `data/blob_deleted`
- Fixed GC assertion because of remove `del_intervals` more then once

## [0.0.16] 2025-11-05
### Bug fixes
- Fixed manifest's txid was not initialized correctly
- Fixed some obsolete WAL files can't be removed

## [0.0.15] 2025-11-04
### Changes
- Optimize data file's gc_ratio calculation and victims selection
- Separate key-value storage, keys and small values are always stored in sst, while big value will be stored out of sst

### Bug fixes
- Fixed garbage can't be cleaned (active frames after filter mey be marked as deactive by flush thread)

## [0.0.14] 2025-10-12
### Changes
- Add a layer of indirection allows the data file ID to be represented using 64 bits
- Force checkpoint when WAL file too large
- Reduce memory consumption
    - always `clone` loader when a node was compacted
    - remove unnecessary `pin` in `SysTxn`
    - clear arena when it was flushed
- Remove `rollback`, if the txn is not committed, it will be implicitly rolled back

### Bug fixes
- Fixed `NodeCache` not being able to limit memory usage
- Fix arena being flushed while still being mutably referenced
- Fix `Layout` in `Block`

## [0.0.13] 2025-09-21
### Changes
- Simplify data file layout, move meta fields into a manifest file
  actually, it's not the best solution, we should use something like BoltDB, so that we don't need to load all the mappings into memory

### Bug fixes
- Fix leak in cache evict

## [0.0.12] 2025-09-03
### Changes
- Remove some unused wal record type, and unused fields in WalCheckpoint
- Bring back log flush before data
- Extend wal id and checkpoint position to 64 bits
- Partially support data file id wrapping (use an extra `tick` to generate `up2`, an extra `epoch` to keep `MapEntry` ordered)
- Allow to store wal file in separate directory
- Limit maximum node size

### Bug fixes
- Fix `warm_up` not change cache size
- Fix possible assert in evictor (when pid unmap happens)
- Fix possible infinite loop in `NodeCache::evict` and `GarbageCollector::process_data`
- Fix `fetch_sub` memory order


## [0.0.11] 2025-08-27
### Changes
- Add new evictor thread compact node on cache evict and randomly pick active node for compaction
- Change `consolidate_threshold`'s maximum value to `split_elems`
- Create `SysTxn` when it's necessary
- Add `Mutex` to each node which 
   - mitigate multi-threaded contention in both compaction and SMO
   - reduce wasted memory allocation 
   - remove check in `Node::insert_index` which is unnecessary now
- Move node compact in `Tree::link` to `Tree::try_find_leaf` which mitigate multi-threaded contention

### Bug fixes
- Fix `upsert`
- Enable missing node cache warm up when cache hits
- Fix GC remove WALs that still in use
- Fix duplicated garbage collection in `merge_node`
- Fix `MapBuilder:add`'s assert which is possible in some case
- Fix `Node::search_sst`'s assert which is possible in some case


## [0.0.10] 2025-08-10
### New Features
- Support prefix encoding

### Changes
- Dynamic arena allocation
- Removed all xxRef except BoxRef
- Unified BoxRef management, which greatly reduces the occurrence of cloning, especially in ImTree
- Disabled cc compact for read-only txn
- Replaced the `Queue` implementation to improve concurrency 

### Bug fixes
- Atomic `upsert`

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
