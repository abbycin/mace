## [0.0.27] 2026-02-20 (AI-Assisted)
### New Features
- Added `Mace::vacuum_bucket` and `VacuumStats` (`scanned`, `compacted`) to support explicit per-bucket vacuuming
- Added `Mace::vacuum_meta` and `MetaVacuumStats` to compact manifest metadata btree
- Added `Mace::is_bucket_vacuuming` to expose bucket vacuum inflight state
- Added production validation suites and scripts: `scripts/prod_test.sh` (`fast|stress|chaos|all`), `scripts/prod_soak.sh`, `scripts/perf_gate.sh`

### Changes
- Removed WAL descriptor side files (`meta_{group}`) and rebuilt WAL bootstrap from WAL file scanning during recovery
- Extended checkpoint records to carry checkpoint position in WAL and used latest valid checkpoint discovery during recovery
- Added explicit WAL durability barrier before manifest flush commit, then moved checkpoint advancement to post-`mark_done` phase
- Reworked WAL recycle boundary tracking from descriptor persistence to logging in-memory oldest-id propagation
- Added production integration tests across bucket/concurrency/evictor/gc/recovery/workload with failpoint chaos coverage
- Added failpoint runtime (`MACE_FAILPOINT`) and injected crash/io/abort points across flush/wal/manifest/gc/evictor/txn commit boundaries

### Performance
- Switched SMO flow to runtime markers and aligned split/merge execution paths to reduce structural race amplification
- Removed arena chunk allocator and reverted to direct allocation path
- Optimized rollback log sync path and `Tree::link` locking behavior
- Simplified hot-path alloc API by removing redundant `bucket_id` argument
- Consolidated WAL/group optimization series for 16B/10K scale goals, including inflight-aware group selection, logging/checkpoint flow cleanup, in-memory oldest WAL boundary propagation, and reduced lock-held read contention in stat cache paths

### Bug Fixes
- Fixed packed-allocation crash-closure correctness: dependent frames now stay in one packed allocation scope (same arena + contiguous address span), and plan/usage mismatch fails fast instead of silently degrading
- Fixed iterator lifetime UB in `Tree::iter` and stabilized leaf iterator state to avoid intermittent crashes
- Fixed orphan-file crash-recovery boundary by switching to marker-driven cleanup (`odf_*` / `obf_*`) instead of tail probing
- Fixed rollback WAL sync ordering on transaction drop
- Hardened bucket context reclaim lifecycle under gc/evictor concurrency
- Fixed GC/txn path correctness issues and improved error mapping (`btree_store::Error::Internal -> OpCode::Invalid`)


## [0.0.26] 2026-02-06 (AI-Assisted)
### Changes
  - Bucket-centric lifecycle and runtime/metadata separation: Store owns bucket state, flush results are routed through Store to update Manifest, and runtime components no longer write metadata directly
  - Metadata and interval structure refactor: ImTree moved to `src/utils`, interval/stat paths updated, and per-bucket file indexes added for faster deletion
  - Manifest bucket tables migrated to `DashMap` with a structural lock to reduce contention on create/delete
  - Store API reorganization plus test/bench expansion, including new `src/store/store.rs`, `tests/bucket.rs`, and `benches/perf.rs`

## [0.0.25] 2026-01-23 (AI-Assisted)
### Changes
  - Enhanced error handling across the system
  - Optimized Arena to avoid allocating small objects via alloc; integrated FxHash for DashMap
  - Added version validation for metadata
  - Simplified transaction recording for Flush and GC operations
  - Removed TxnKV concurrency limits and decoupled Logging from TxnKV, allowing multiple TxnKV instances to share the same Logging component.
  - Implemented full checksum support for every WAL entry to ensure data integrity

### Bug Fixes
  - Fixed an incorrect assertion in flush.rs:flush_data

## [0.0.24] 2026-01-18
### Changes
- Refactor metadata storage with `btree-store`
- Add checksum to `WalUpdate`
- Add a scavenger routine to clean up stale versions
- Optimize random insert performance

### Bug fixes
- Fixed data race in `DataStatCtx` and `BlobStatCtx`

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
