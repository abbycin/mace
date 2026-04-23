# MACE design notes

This document describes the current implementation in this repository.
It is focused on behavior that affects correctness, crash safety, and operational tuning.

## High-level goals

- Embedded key-value engine with predictable point-read latency.
- High write throughput via append-only WAL + asynchronous checkpoint publish.
- Snapshot isolation (SI) with MVCC visibility.
- Key/value separation: large values stored in blob files.
- Crash-safe metadata updates and idempotent startup cleanup.

## Core architecture

- `src/store`
  - Owns startup/shutdown orchestration, recovery, GC thread, and public `Mace` APIs.
- `src/meta`
  - Owns persistent metadata (`btree_store`) and in-memory stat/index caches.
- `src/map`
  - Bucket runtime (dirty-page generations, checkpoint build, loader/cache, flow control).
- `src/index`
  - Bw-Tree style index, delta chains, split/merge/compact paths, iterator paths.
- `src/cc`
  - Concurrency control, writer groups, commit tree, WAL encode/decode, rollback.
- `src/utils/observe.rs`
  - Fixed-cardinality observability model.

## Metadata model

`Manifest` persists metadata in BTree buckets.

Main buckets:

- `numerics`
  - global counters and orphan markers (`odf_*`, `obf_*`).
- `bucket_metas`
  - bucket name -> `BucketMeta { bucket_id }`.
- `bucket_frontier`
  - `BucketDurableFrontier` per bucket (per-writer-group durable LSN frontier).
- `pending_del`
  - buckets pending physical table cleanup.
- `page_table_{bucket_id}`
  - `pid -> logical_addr` map.
- `data_interval_{bucket_id}` / `blob_interval_{bucket_id}`
  - `addr-range -> file_id` mapping.
- `data_stat` / `blob_stat`
  - per-file stats + bitmap references used by GC.
- `obsolete_data` / `obsolete_blob`
  - files pending physical delete.

Addressing model:

- each bucket has an independent logical address space (`BucketState::next_addr`).
- page table maps `pid -> swip` (`tagged` means on-disk logical address).
- interval tables map logical address ranges to data/blob file IDs.
- relocation tables in file footers map logical address to file offset.

## Bucket lifecycle

Creation (`Manifest::create_bucket`):

1. Take `structural_lock`.
2. Reject duplicates and enforce `MAX_BUCKETS` via `nr_buckets`.
3. Allocate `bucket_id`, create runtime context/state lazily, initialize frontier.
4. Commit one metadata txn (`numerics`, `bucket_frontier`, `bucket_metas`).

Loading (`load_bucket_context`):

- lazy-load runtime context when first accessed.
- recover page table + data/blob intervals from metadata.
- cache context in `BucketMgr`.

Unload (`unload_bucket`):

- runtime-only operation.
- unpublish from in-memory maps, checkpoint-and-reclaim loaded context.
- persisted bucket metadata/data are kept.

Delete (`delete_bucket`):

- logical delete txn removes `bucket_metas` entry, inserts `pending_del`, records obsolete files.
- runtime context/state are removed first.
- GC later performs physical cleanup (`pending_del`) and decrements `nr_buckets`.

`BucketState` (in-memory only):

- `txn_ref`, `is_deleting`, `is_drop`.
- vacuum coordination: `vacuum_inflight`, `vacuum_epoch`, wait/notify primitives.

## Dirty-page generations and checkpoint cut

`Pool` uses generation slots instead of arena/chunk allocators.

- hot generation: writable pages/retired-chain/root marks.
- sealed generation: previous hot generation retained while checkpoint is in flight.
- epoch gate (`epochs.gate`) ensures writers do not straddle a checkpoint cut.

Write-side flow:

1. writer captures `WriteEpoch` (`capture_epoch`) and increments inflight counter.
2. allocation uses `BoxRef::alloc` and inserts into hot page map.
3. publish path marks dirty roots (`pid -> latest addr`) and optional unmap marks.
4. page replacement/evict paths move retired logical addresses into lineage chains (`RetiredChain`).

Checkpoint trigger conditions:

- hot bytes >= `checkpoint_size`, or
- total dirty bytes (hot + sealed) >= `pool_capacity`, or
- proactive nudge from evictor (`checkpoint_nudge_ms`) when dirty data remains stale.

Checkpoint cut (`Pool::checkpoint`):

1. atomically swap hot generation to new empty maps.
2. publish old generation as sealed.
3. rotate inflight/root slots in one critical section.
4. create `CheckpointTask` and enqueue to checkpointer thread.

`CheckpointTask::snapshot`:

- waits old-generation writers to leave (`EpochInflight::wait_zero`).
- walks dirty roots and reachable chains (link/sibling/remote hints).
- carries `addr > snap_addr` mutations back to new hot generation.
- computes per-group checkpoint frontier delta.
- emits data/blob junk candidates for stat apply.

`CheckpointTask::finish`:

- clears sealed slots.
- requires sealed generations to be uniquely owned (`Arc::try_unwrap`).
- recycles maps for reuse.
- signals flow-controller completion.

## Frontier design (durable boundary)

Background issue solved by the frontier design:

- a compacted/materialized durable page can absorb updates from multiple writer groups.
- each page header still carries only one `(group, lsn)` pair.
- if recovery correctness relies on per-group WAL checkpoint only, multi-group absorbed history can be replayed incorrectly.

Correctness model:

- the durable boundary is `per bucket, per writer group`, not a single global/group scalar.
- this boundary is represented by `BucketDurableFrontier` in manifest (`bucket_frontier` bucket).
- frontier metadata is committed in the same manifest txn as map/stat publish; page/data/blob formats do not need extra frontier fields.

Runtime carrier and propagation:

- hot paths keep building lineage frontier using `SparseFrontier` carried in `RetiredChain`.
- replace/evict transfer paths fold source lineage by pointwise-max per group.
- checkpoint snapshot applies chained frontier to group checkpoint positions (`frontier.apply_to`).
- when no chain frontier is present for an item, snapshot falls back to page-header `(group, lsn)` contribution.

Durable publish boundary:

- checkpoint builds a bucket-level frontier delta for this publish.
- `StoreFlushObserver::publish` merges and persists it via `Manifest::merge_bucket_frontier`.
- map + frontier are durable together, so crash after manifest commit still has a consistent durable boundary.

## Flush publish protocol

Checkpointer builds flushed files from snapshot pages.

Data/blob file layout:

- payload frames
- interval table
- relocation table
- footer (`DataFooter` / `BlobFooter`)

Protocol for each new file:

1. stage orphan marker in `numerics` (`odf_*` / `obf_*`).
2. stage in-memory unsynced-file marker (`data_unsync` / `blob_unsync`).
3. write file bytes.
4. publish metadata and clear orphan marker in the same manifest txn.

`StoreFlushObserver::publish` flow:

1. sync built data/blob writers (`sync_data` or `sync` based on `sync_on_write`).
2. merge bucket frontier with snapshot frontier delta.
3. begin manifest txn:
   - apply junk to data/blob stats,
   - clear orphan markers for published files,
   - record intervals/stats/map/frontier/numerics in one atomic commit boundary.
4. commit txn.
5. clear in-memory unsynced sets.
6. update WAL checkpoint records per writer group from global frontier lower bound (scan/recycle hint).

Global frontier lower bound (`Manifest::global_frontier_lower_bound`):

- considers all active buckets.
- buckets with pending flush can pin frontier via durable bucket frontier.
- clean/read-only buckets fallback to current group checkpoint to avoid stale pinning.

## Foreground admission / backpressure

`FlowController` exists for every bucket; enforcement is gated by `Options::enable_backpressure`.

Admission model:

- foreground write acquires `ForegroundWritePermit` before `tree.update`.
- permit reserves bytes against dirty-memory budget.
- if projected dirty bytes exceed admission limit, writer waits on condvar.
- checkpoint progress wakes waiting writers in byte-based quanta.
- permit drop releases reservation.

Throughput/debt model:

- checkpoint enqueue records estimated bytes.
- actual built bytes reconcile debt and feed IO EWMA.
- completion updates end-to-end EWMA.
- idle windows reset stale EWMA samples.

Current flow metrics:

- `CounterMetric::FlowFgAdmissionWait`
- `HistogramMetric::FlowFgAdmissionWaitMicros`

## Transactions, MVCC, and WAL

`TxnKV`:

- allocated to writer group via inflight-aware scheduling.
- records `Begin` at start.
- first mutation logs `WalUpdate` and links via `prev_lsn` chain.
- commit path:
  - `record_commit(start_ts)`
  - `log.sync(false)`
  - append `(start_ts, commit_ts)` to commit tree
  - update watermark (`collect_wmk`)

Drop of uncommitted `TxnKV`:

- no writes: log `Abort`.
- with writes: rollback through WAL chain (`WalReader::rollback`), logging CLR records and inverse versions.

`TxnView`:

- allocates from `CCPool`, holds snapshot `start_ts`, no WAL writes.

Visibility (`ConcurrencyControl::is_visible_to`):

- own uncommitted version visible only in same writer group.
- future txid invisible.
- `safe_txid` fast path from global watermark.
- fallback to per-group `CommitTree::lcb(start_ts)`.

WAL payload semantics:

- `Insert`: new value image.
- `Update`: old value + new value.
- `Delete`: old value image.
- `Clr`: tombstone/value + undo pointer (`undo_id`, `undo_off`).

## Recovery

Startup sequence:

1. `ManifestBuilder::load` loads metadata caches and counters.
2. `clean_orphans` scans orphan markers (`odf_*`, `obf_*`), removes stray files, deletes markers.
3. WAL phase1 scans wal files per group and finds latest valid checkpoint as conservative scan start.
4. context is created with per-group bootstrap info.
5. phase2 runs `analyze -> redo -> undo`.

Analyze stage:

- reads WAL from checkpoint position.
- truncates tail on corruption if `truncate_corrupted_wal=true`.
- checks bucket durable frontier first (`durable_frontier_lsn`):
  - if `record_lsn <= frontier[bucket][group]`, treat as already durable and skip dirty-table enrollment.
  - only records beyond frontier continue to redo/undo candidate logic.

Boundary split (important semantic change):

- `WalCheckpoint[group]` is a scan-start optimization and WAL recycle aid.
- `BucketDurableFrontier[bucket][group]` is the correctness gate for "already durable or not".
- therefore "one WAL record before checkpoint and another after checkpoint" can still exist, but only affects scan cost, not replay correctness.

Redo stage:

- reapplies remaining update/insert/delete/clr records in version order.

Undo stage:

- rolls back incomplete txns using same WAL rollback path used at runtime.

Post recovery:

- advances `oracle`/`wmk_oldest`.
- evicts loaded recovery bucket contexts.

## GC, scavenge, and vacuum

GC thread (`gc_timeout` periodic + manual trigger):

1. `process_data` (victim selection + rewrite)
2. `process_blob` (victim selection + rewrite)
3. `process_pending_buckets` (physical cleanup for `pending_del`)
4. `scavenge` (bounded in-memory page compaction)
5. `delete_files` (idempotent obsolete file unlink + metadata ack)

Rewrite crash safety:

- rewrite stages orphan marker before file build.
- metadata txn publishes new intervals/stats + delete intents + clears marker.
- old files are then physically deleted through obsolete-file pipeline.

Scavenge:

- scans loaded bucket page tables in bounded batches.
- skips deleting/dropped/vacuuming buckets.
- enforces per-tick compaction quota to cap IO impact.

Manual vacuum APIs:

- `vacuum_bucket(name)`
  - serialized per bucket using `vacuum_inflight`/`vacuum_epoch`.
  - returns `VacuumStats { scanned, compacted }`.
- `vacuum_meta()`
  - compacts manifest btree (`btree_store::compact`).

## Observability model

Injection point:

- `Options::observer: Arc<dyn Observer>` (`NoopObserver` by default).

API shape:

- `counter(CounterMetric, delta)`
- `gauge(GaugeMetric, value)`
- `histogram(HistogramMetric, value)`
- `event(ObserveEvent)`

Built-in helper:

- `InMemoryObserver` with fixed arrays and bounded FIFO events.

Latency sampling:

- `LATENCY_SAMPLE_SHIFT = 6` (`1/64`) for hot metrics such as:
  - txn commit/rollback latency,
  - tree link lock-hold latency,
  - WAL sync latency.

Low-frequency recovery/GC metrics are reported without sampling.

## Key invariants

- Data/blob files referenced by metadata must be written before metadata commit.
- Orphan marker stage/clear is the only startup orphan cleanup source of truth.
- Checkpoint epoch cut must atomically rotate hot/sealed generation handles.
- Writers must not straddle checkpoint cut (`epochs.gate` + inflight wait-zero).
- Sealed generations must be uniquely owned when checkpoint finishes.
- Bucket durable frontier must monotonically advance per group.
- Map publish and bucket frontier publish must be in the same manifest txn.
- Recovery correctness must be gated by bucket frontier, not by group checkpoint alone.
- Recovery analyze must ignore updates already <= durable frontier.
- Bucket delete is two-phase (`pending_del`), and `nr_buckets` decrements only after physical cleanup.
- WAL recycling must stay below min(active-txn boundary, last checkpoint boundary).
- Foreground admission wait must happen before entering `tree.update` critical mutation path.

## Operational validation

- `./scripts/prod_test.sh fast|stress|chaos|all`
- `./scripts/perf_gate.sh snapshot|compare`
- tuning defaults in `scripts/perf_thresholds.env`
- CI wiring in `.github/workflows/ci.yml`
