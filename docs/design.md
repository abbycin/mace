# MACE design notes

This document summarizes the current MACE design as reflected by the codebase. It focuses on the architecture, lifecycles, and invariants that matter for correctness and performance.

## High-level goals

- Embedded key-value storage engine with a log-structured data path
- Bw-Tree style indexing for predictable reads and high write throughput
- Snapshot isolation (SI) via MVCC with WAL-based durability
- Key-value separation: large values stored in blob files, indexed by pointers

## Core architecture

- **Store** (`src/store`): owns `Manifest`, `Context`, GC, evictor, flusher, and orchestrates shutdown order
- **Manifest** (`src/meta`): metadata root persisted in `btree_store`, and the source of truth for buckets, stats, and mappings
- **Bucket-centric runtime** (`src/map`): each bucket has its own `BucketContext` (Pool, PageMap, IntervalMaps, caches)
- **Index** (`src/index`): Bw-Tree variant with delta chains, compaction, split/merge, and sibling pages
- **Concurrency control** (`src/cc`): SI visibility with `CommitTree`, WAL, and checkpoint coordination

## Metadata and persistence

### Manifest as root

`Manifest` stores all metadata in a persistent B-Tree namespace. Key buckets include:

- `numerics`: global counters (oracle, next ids, wmk_oldest, log_size)
- `bucket_metas`: bucket name -> `BucketMeta` (id)
- `pending_del`: buckets pending physical cleanup
- `page_table_{bucket_id}`: page id -> addr mappings
- `data_interval_{bucket_id}` / `blob_interval_{bucket_id}`: addr ranges -> file ids
- `data_stat` / `blob_stat`: per-file stats and masks for GC
- `obsolete_data` / `obsolete_blob`: files to be deleted

### Data-first, meta-last

Flush order is enforced by `StoreFlushObserver`:

1. Write data/blob files to disk
2. Ensure WAL durability barrier for this flush boundary (`flsn`) before publishing metadata
3. Commit metadata transaction (stats, map table, intervals, numerics)
4. Mark flush done, then advance per-group checkpoint positions and persist checkpoint records

This ensures data is durable before metadata points to it.

## Bucket model

### Bucket lifecycle

- **Create**: `Manifest::create_bucket` uses `structural_lock`, checks `MAX_BUCKETS`, allocates id, initializes state and page map, and persists `BucketMeta`
- **Load**: contexts are lazy-loaded; repeated loads reuse cached `BucketContext`
- **Unload**: `unload_bucket` unpublishes bucket context from runtime maps and flushes pending data; actual memory release is deferred to `Arc` drop without touching on-disk data
- **Delete**: two-phase
  - Logical delete: remove meta, record in `pending_del`, mark obsolete files
  - Physical delete: GC cleans page table/interval tables and decrements `nr_buckets`

`nr_buckets` counts both active and pending-delete buckets to prevent bypassing the limit.

### BucketContext

Each bucket has its own runtime context:

- `Pool` (Arenas)
- `PageMap` (page id -> SWIP)
- `IntervalMap` for data and blob
- `NodeCache` and `ShardPriorityLru` caches
- eviction candidate ring and per-bucket loaders

`BucketContext::reclaim` is idempotent (CAS-guarded). Runtime remove paths only unpublish and flush; final reclaim is deferred to `Drop` of the last `Arc<BucketContext>` so GC/evictor snapshots cannot observe nulled shared refs.

`BucketState` is in-memory only and includes:

- `txn_ref`: active txn count to block deletion
- `is_deleting` / `is_drop`: lifecycle markers
- `next_addr`: per-bucket address allocator (isolated address space)

### Lazy loading

`PageMap` and interval maps are reconstructed on first bucket access from manifest tables:

- `page_table_{bucket_id}` is replayed into `PageMap`
- `data_interval_{bucket_id}` / `blob_interval_{bucket_id}` are loaded into `IntervalMap`

This keeps startup time and memory usage low for unused buckets.

## Addressing and mapping

- Each bucket has its own **logical address space** (`BucketState::next_addr`)
- `PageMap` maps **page id -> SWIP**
  - **untagged** SWIP is an in-memory pointer
  - **tagged** SWIP stores a logical address for on-disk pages
- `IntervalMap` resolves **addr -> file id**
- Per-file relocation tables resolve **addr -> file offset**

This replaces the old logical/physical id mapping: the logical address is stable; the file and offset are resolved through intervals and relocations.

## Buffer and memory management

### Arena and Pool

- `Arena` is an append-only allocator with reference counting
- Allocation uses direct `BoxRef` allocation (`alloc_exact`) with per-arena accounting (no chunk allocator)
- `Arena` states: `HOT` (allocating), `WARM` (sealed), `COLD` (ready to flush), `FLUSH` (flushed)
- Tombstones mark reclaimed entries; junk addresses are collected for GC

`Pool` manages arenas for a bucket:

- `Iim` tracks the addr -> arena id mapping for in-memory loads
- `flsn` per writer group records WAL position of dirty data
- `safe_to_flush` ensures WAL is flushed past arena `flsn` before flush
- `over_provision` allows extra arenas if all are busy

### Packed allocation crash-closure

To keep crash consistency for dependent frames (for example base+sibling and delta+remote), MACE uses a packed allocation lifecycle:

- `IAlloc` uses `u32` sizes for `allocate`, `allocate_pair`, and `begin_packed_alloc`
- `begin_packed_alloc` reserves `real_size + frames` in one arena and reserves one contiguous logical address span
- allocations inside the packed scope always use the pre-selected arena and sequential addresses from that span
- `end_packed_alloc` verifies plan/usage equality (`frames`, address cursor, and bytes) and fails fast on mismatch
- rollback removes packed entries from arena visibility first, then rolls back batch counters (`real_size`/`refs`)

The default trait methods are fallback-only for compatibility and tests; production allocators (`SysTxn`, `Evictor`) override them to enforce the closure guarantees.

### Loader and caches

- `Loader` resolves data from:
  1. LRU (priority cache)
  2. Arena (hot data)
  3. Disk via `DataReader`
- `NodeCache` tracks node memory usage and hot/warm/cold state
- `CandidateRing` samples evictable pids to reduce contention

### Eviction and compaction

- Evictor runs when cache usage reaches ~80% of capacity
- It samples candidate pids, cools to `Cold`, then evicts by tagging SWIPs
- If delta chains are long or contain garbage, eviction triggers compaction
- Leaf rebuild during compaction uses the same packed allocation hooks as foreground transactions
- Compaction writes junk frames into arenas for later GC

## Index (Bw-Tree variant)

- Nodes are stored as base + delta chains
- Delta inserts use CAS with retry; compaction merges delta chains
- `merge` and `split` are explicit and serialized with locks
- Leaf nodes store versions; sibling pages keep versions for the same key together

### Key-value separation

- Values larger than `inline_size` are stored as remote blobs
- Leaf entries store remote pointers; blob files are managed separately
- LRU has high/low priorities; remote values are cached with lower priority

## MVCC and visibility

### Visibility checks

Visibility is based on `CommitTree` and LCB (Last Commit Before):

- if `record_txid == start_ts`, only visible in the same writer group
- if `record_txid > start_ts`, invisible
- if `safe_txid > record_txid`, visible
- otherwise compute LCB and compare

`ConcurrencyControl` maintains per-group caches to avoid cross-thread LCB on the fast path.

### Watermark and safe_txid

`collect_wmk` computes a global watermark:

1. find the smallest active txid across groups
2. compute each group’s LCB for that txid
3. take the global minimum and publish as `wmk_oldest`

`safe_txid` is derived from `wmk_oldest` and drives compaction/GC safety.

### Transaction types

- **TxnKV** (read-write):
  - selects writer group via inflight-aware scheduling
  - records `Begin` when txn starts, and WAL updates on first mutation
  - commits by `record_commit` + `log.sync(false)` (checkpoint is not advanced here)
  - appends `(start_ts, commit_ts)` to `CommitTree`
- **TxnView** (read-only):
  - allocates a CC node from `CCPool`
  - uses snapshot `start_ts`, no WAL

Long transactions are bounded by `max_ckpt_per_txn` to prevent WAL starvation.

## WAL, checkpoints, and recovery

- WAL is per writer group, backed by a ring buffer
- `log.sync(force)` flushes ring-buffered WAL; fsync happens only when `force` is true or `sync_on_write` is enabled
- Checkpoint advancement is driven by flush completion (`StoreFlushObserver`), not by a `numerics.signal`
- `Logging::checkpoint` writes `WalCheckpoint { checkpoint: Position }` after flushing pending ring data
- After checkpoint write, `StoreFlushObserver` explicitly fsyncs the writer to persist checkpoint boundary
- `last_ckpt` is advanced from flush results, clamped by active txns' minimum LSN

### WAL recycling guard

GC reclaims WAL files in `[oldest_id, checkpoint_id)` where:

```
checkpoint_id = min(active_txns.min_file_id, logging.last_ckpt.file_id)
```

This prevents deleting WAL still needed for recovery or active transactions.

### Recovery (crash handling)

- Phase 1: scan WAL files per writer group to build `GroupBoot` (`oldest_id`, `latest_id`, `checkpoint`) and build `Context`
- Phase 2: analyze from last checkpoint, then redo/undo
- Bucket-aware replay skips buckets already deleted
- After redo/undo, `oracle` and `wmk_oldest` are advanced, and loaded bucket contexts are evicted

#### Orphan data/blob cleanup

Orphans are data/blob files that were written to disk but never published by manifest metadata
(for example, crash between rewrite output sync and metadata commit).

`clean_orphans` is intentionally scoped to **file-level orphan closure only**. It does not repair
runtime references.

Current protocol:

1. GC rewrite stages per-file orphan intent markers in `numerics` before writing new output files.
   - data marker key: `odf_{file_id}`
   - blob marker key: `obf_{file_id}`
2. Rewrite output is written and synced.
3. In the same metadata txn that publishes the new file, GC clears the corresponding marker.
4. During startup, `clean_orphans` scans marker keys in `numerics`, removes corresponding files,
   then removes cleaned markers.

Important boundaries:

- Recovery uses marker scan only.
  - no data directory traversal
  - no max-id tail probing
- file ids can be sparse; cleanup is marker-driven and idempotent.
- orphan marker update failure is fatal (`expect("orphan marker update failed")`), because a
  partial marker cleanup makes recovery boundary ambiguous.

Crash safety:

- Crash after marker stage but before metadata publish: marker survives, startup cleanup removes
  uncommitted output file.
- Crash after metadata publish and marker clear commit: file is already part of durable metadata
  state and is not treated as orphan.

#### Obsolete file deletion (crash-safe)

Obsolete files are normal GC outputs (not orphans). Deletion is handled by `Manifest::delete_files`:

- `obsolete_data` / `obsolete_blob` lists are kept in manifest.
- If a file is missing or deleted successfully, its id is recorded as `DataDeleteDone` / `BlobDeleteDone` in a manifest txn.
- If a crash happens mid-delete, the remaining entries stay in the obsolete lists and will be retried at the next run.

This makes file deletion idempotent across crashes.


## Disk GC and rewrite

### Data and blob stats

- `DataStatInner` tracks `active_size`, `total_size`, `up1`, `up2`, and counts
- `BlobStatInner` tracks active size/count
- Bitmaps mark inactive entries for GC filtering

### MDC-based victim selection

- `Score` computes a decline rate based on `(active_size, total_size, up2, tick)`
- Lower decline rate implies higher priority for compaction
- Victims are chosen until they can form a compacted file

### Rewrite flow

- Load relocations and intervals, filter by bitmap
- Rewrite live frames into new data/blob files
- Update intervals and stats in manifest
- Mark old files obsolete and delete them in background

### Pending bucket cleanup

- `delete_bucket` records bucket id in `pending_del`
- GC cleans page tables and interval tables in batches
- After cleanup, `nr_buckets` is decremented and metadata is dropped

### Scavenge

- GC periodically scans a small batch of pids per **loaded** bucket
- Compacts pages with long delta chains or garbage
- Uses I/O quota per tick to limit impact

## Memory safety model

- `BoxRef` uses a plain atomic refcount with direct alloc/dealloc (no chunk/MSB allocation flag)
- `Handle<T>` is a manual owner and requires explicit `reclaim()`
- `MutRef<T>` is intrusive refcounted and reclaimed by clone/drop lifecycle (not manual `reclaim()`)
- Bucket shutdown order ensures background threads stop before reclaiming arenas and page maps

## Key invariants to preserve

- Data must be flushed before manifest commits
- PageMap entries must be consistent with map tables written in the same flush txn
- Dependent allocations must be closed in one packed scope (same arena + contiguous address span)
- Packed plan mismatch must fail fast (`error` + panic), never silently downgrade to split allocation
- Bucket deletion must be two-phase; do not decrement `nr_buckets` early
- WAL files can only be deleted up to the safe checkpoint boundary

## Operational validation pipeline

To keep production behavior aligned with crash-safety and performance invariants:

- `scripts/prod_test.sh`
  - `fast` for quick correctness checks
  - `stress` for long-run pressure
  - `chaos` for failpoint crash/fault windows
  - `all` for full matrix (`fast + stress + chaos`)
- `scripts/perf_gate.sh`
  - `snapshot` captures current benchmark summary
  - `compare` gates regressions against a baseline ref (`MACE_PERF_BASE_REF`)
- `scripts/prod_soak.sh`
  - loops stress/chaos with periodic perf snapshots
  - writes trend data into `target/prod_soak/perf_cycles.csv`
- `scripts/perf_thresholds.env`
  - central threshold/profile knobs for local runs and CI

CI integration:

- `.github/workflows/ci.yml`
  - `test-prod-all` runs `prod_test.sh all` on `ubuntu-latest` `x86_64` only
  - `perf-regression` runs `perf_gate.sh compare`
- `.github/workflows/prod-soak.yml`
  - scheduled + manual soak workflow with artifact upload
- test child-process spawning in prod recovery suites honors `CARGO_TARGET_*_RUNNER`
