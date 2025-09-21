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
