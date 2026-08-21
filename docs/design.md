# Mace Design

This document describes how the current Mace storage engine is designed and how its major runtime
and persistence flows work.

The live correctness constraints, their evidence, and their verifier coverage are maintained in
`docs/constraints/registry.yaml`. This document does not duplicate that ledger and does not serve
as a source-code tour.

## 1. System Overview

Mace is an embedded key-value engine built around:

- a Bw-Tree style ordered index
- snapshot isolation with multi-version records
- an append-only redo WAL
- asynchronous checkpoint publication
- key/value separation for large values
- bucket-local runtime state
- background file reclamation and rewrite

The database is divided into named buckets. A bucket owns its logical page address space, index
state, checkpoint generations, persisted frontier, cache policy, and data/blob accounting. Global
services coordinate transaction timestamps, WAL streams, recovery, and background maintenance
across buckets.

### 1.1 Persistent domains

Mace stores state in four persistent domains:

- metadata store
  - bucket catalog and options
  - global sequences and persisted engine options
  - page tables and logical-address intervals
  - per-file accounting
  - per-bucket durable frontiers
  - pending bucket deletion, orphan cleanup, obsolete files, and WAL recycle state
- WAL files
  - transactional redo records and checkpoint hints
- data files
  - persisted index pages and structural records
- blob files
  - persisted large-value payloads

The metadata store is authoritative for reconstructing the durable database. Data and blob files
become part of the database only through metadata publication.

### 1.2 Runtime domains

The runtime has three main domains:

- foreground transactions and read-only views
- bucket runtimes containing index, cache, dirty generations, and checkpoint state
- background services for checkpoint completion, recovery, abort-clean, WAL recycling, payload GC,
  and bucket cleanup

Bucket runtimes are loaded lazily. Opening a database reconstructs global durable state and WAL
state without eagerly loading every bucket index.

## 2. Metadata And Addressing

Mace separates logical identity from physical placement.

The main identities are:

- bucket ID: durable identity of a named bucket
- page ID: logical identity of an index page inside a bucket
- logical address: durable identity of a persisted record inside a bucket address space
- file ID: identity of a data, blob, or WAL file

A persisted page is resolved in three steps:

1. the bucket page table maps page ID to logical address
2. the bucket interval map maps logical address to a data or blob file
3. the file relocation table maps logical address to a byte range in that file

GC rewrite changes physical placement while preserving logical addresses. Page references and
history references therefore remain stable across file rewrite.

### 2.1 Metadata groups

The metadata store contains separate logical groups for:

- global sequences and persisted options
- bucket names, IDs, options, and lifecycle state
- per-bucket page tables
- per-bucket data and blob interval maps
- per-file data and blob statistics
- per-bucket durable WAL frontiers
- orphan, obsolete-file, pending-delete, and WAL-recycle records

Changes that form one durable checkpoint closure are committed in one metadata transaction. This
includes the page map, address intervals, file statistics, bucket frontier, and related global
sequences.

### 2.2 File accounting

Each retained data or blob file has persisted accounting for:

- total record count and bytes
- active record count and bytes
- inactive record sequences
- bucket ownership and recency information used by GC

The relocation table is the physical inventory of records in a file. Checkpoint and GC update the
persisted accounting as logical addresses become unreachable or move to replacement files.

## 3. Bucket Lifecycle

### 3.1 Creation

Creating a bucket allocates a durable bucket ID, stores the bucket options, initializes its durable
frontier, and increments global bucket accounting in one metadata publication.

The bucket becomes visible after that publication. Its runtime index is then created on demand.

### 3.2 Loading

Loading a bucket reconstructs its runtime from:

- bucket options
- page table entries
- data and blob interval maps
- durable frontier and file-accounting state

The loaded runtime owns the bucket cache, dirty generations, address allocator, checkpoint state,
and flow-control state.

### 3.3 Unloading

Unloading removes only the cached bucket runtime. Durable metadata and payload files remain
unchanged.

Before unload, the active WAL route is synchronized. A bucket with pending abort-clean work remains
loaded until that work has completed its page rewrite, durable checkpoint, and reader-quiescence
phase.

### 3.4 Deletion

Deletion has two phases:

1. logical deletion removes the bucket from the visible catalog and records a pending-delete entry
2. background cleanup removes page-table state, interval metadata, file accounting, obsolete files,
   and the remaining bucket metadata in bounded batches

Global bucket accounting includes pending-delete buckets. It is decremented when physical cleanup
finishes.

### 3.5 Bucket options

Bucket options are persisted with the bucket.

The inline-value boundary and node split cardinality define persisted page interpretation and cannot
be changed after bucket creation. Other bucket options are runtime or output policies and may be
updated while the bucket is unloaded and no rewrite is active. The updated values take effect on
the next load.

Compression is an output policy. Enabling or disabling it affects newly written data/blob records;
existing raw and compressed records remain readable in the same bucket.

### 3.6 Global options

An initialized metadata store contains a persisted subset of global engine options. Opening the
database validates the persisted record before recovery.

Writer-group cardinality defines persistent transaction ownership and is fixed for an initialized
database. Runtime and maintenance policies are refreshed from the current open options. The
persisted WAL route records the last route whose startup transition completed.

Process-local objects, such as the observer and filesystem instance, are not persisted.

## 4. Index And Value Layout

Each bucket uses a Bw-Tree style ordered index with immutable page images and delta-based updates.
Tree mutation publishes replacement pages rather than modifying durable pages in place.

Leaf records contain versioned key/value state. Values below the bucket inline boundary remain in
the leaf representation. Larger values are stored as blob records and referenced by logical
address.

Page consolidation, split, merge, and eviction can produce replacement page images. These images
enter the normal dirty-generation and checkpoint flow; there is no separate whole-bucket vacuum
path.

### 4.1 Version history

Older versions of a key are stored in a key-local history region. A history descriptor identifies
the first history page, the first slot, and the number of versions belonging to that key. A region
may continue across linked history pages.

Different keys may share a history page, but each key is traversed only within its own declared
region. History-page retirement and blob-payload retirement are accounted independently.

### 4.2 Lookup and iteration

Point lookup and range iteration evaluate all candidate versions for a raw key against one fixed
snapshot. Forward and reverse iteration use the same visibility model as point lookup.

The snapshot remains pinned for the lifetime of a transaction or read-only view and for the
lifetime of iterators and borrowed values derived from it.

## 5. Transactions And Snapshot Isolation

Writer transactions are distributed across logical writer groups. Group selection compares two
candidate groups and chooses the less loaded one. A writer keeps the selected group for its entire
lifetime.

A transaction records:

- a start timestamp
- logical writer-group ownership
- a WAL begin position
- an exact outcome: active, committed with commit timestamp, or aborted

A read-only view has a snapshot timestamp but no writer-group ownership.

### 5.1 Writer start and outcome publication

Writer start first marks its registration as in progress, then allocates and exposes the start
timestamp, appends the WAL begin record, publishes the active outcome, and finally marks the
registration stable. The visibility collector treats an in-progress registration as an unfinished
scan source and does not publish a new safe boundary from that scan.

Commit records the terminal WAL entry, completes the selected WAL durability policy, allocates the
commit timestamp, and publishes the committed outcome. Abort publishes the aborted outcome and,
for a modified transaction, transfers WAL retention to an abort-clean task.

### 5.2 Visibility

Visibility is evaluated in this order:

1. a transaction sees its own versions
2. versions whose writer started at or after the snapshot are excluded
3. versions below the published safe boundary use the positive visibility path
4. retained abort state is checked before a positive result is accepted
5. versions outside the safe boundary use their exact transaction outcome
6. if an exact committed outcome has already been pruned, a newer safe-boundary publication may
   provide the result

An exact commit is visible only when its commit timestamp precedes the snapshot. Active and aborted
versions are not visible.

### 5.3 Safe-boundary collection

The background collector takes one timestamp cut and scans:

- writer registrations
- exact transaction outcomes
- live read-only views

The resulting safe boundary applies to every snapshot covered by that scan. Committed outcomes below
the published boundary may then be pruned. Retained abort state remains available until abort-clean
finishes.

The collector starts after recovery has installed the recovered transaction oracle and initial safe
boundary.

### 5.4 Write conflicts

Same-key writers use first-writer-wins conflict handling. Every retry or page-replacement path checks
the latest key head again before publishing a mutation, so an earlier successful writer cannot be
silently overwritten by a stale retry.

## 6. Dirty State And Checkpoint

Each loaded bucket keeps two dirty generations:

- hot generation: receives current mutations
- sealed generation: is owned by the checkpoint currently being published

The generation cut rotates all checkpoint inputs together:

- dirty page images and their byte accounting
- retired page lineage
- newly discovered junk addresses
- dirty roots
- page-unmap markers
- in-flight writer-root state

After the cut, new writers use the new hot generation. The checkpoint owns the sealed generation
until publication finishes.

### 6.1 Checkpoint snapshot

The checkpoint walks the sealed dirty-root graph and materializes the live pages that are not yet
durable. Pages created after the snapshot address boundary remain in the hot generation or are
carried forward to a later checkpoint.

Structural links, retired lineage, and compaction-produced junk remain associated with the page
generation that still owns their reachability. The checkpoint separates addresses that can retire
with the current closure from addresses that still belong to a live or newer page image.

When a bucket is leaving the loaded set, its sealed batch is discarded instead of being published;
the unload/delete path has already excluded new mutations and retains the previous durable state.

### 6.2 Bucket durable frontier

Every checkpoint derives a per-bucket frontier with one position for each logical writer group. It
includes the WAL effects folded into the durable page closure, including effects from groups other
than the group recorded in an individual page header.

The frontier is published together with the page map and file metadata. Recovery uses it to decide
whether a WAL update is already represented in durable pages.

### 6.3 Checkpoint completion

After metadata publication:

- the sealed generation is released
- still-live state is carried into the current hot generation
- retired page images move to deferred reclamation
- WAL checkpoint floors are advanced for logical groups that participated in the closure
- checkpoint progress updates bucket flow control

## 7. Data And Blob Publication

Checkpoint and rewrite both publish payload files with data first and metadata last.

For each output file, the flow is:

1. persist an orphan marker
2. build the file
3. synchronize the file and required directory state
4. commit intervals, relocations, statistics, map/frontier updates, and orphan-marker removal in
   metadata
5. publish the new runtime interval and accounting state

Before metadata commit, the output is an orphan and is not reachable through durable metadata.
After metadata commit, the output is the authoritative owner of its published logical-address
intervals.

### 7.1 Data/blob file structure

A data or blob file contains four regions:

1. payload frames
2. logical-address intervals
3. relocation entries
4. a fixed footer at end of file

The footer records the file format version, reserved bytes, interval and relocation cardinalities,
and checksums for both tables. The footer is the discovery point used when reopening a file.

Each relocation entry records:

- file offset
- raw logical length
- stored compressed length
- payload checksum
- sequence used by inactive-record accounting

A zero compressed length means the payload is stored raw. A nonzero compressed length means the
payload is decoded to the recorded raw length before page or value decoding.

### 7.2 Compression

Compression is selected per record when the bucket output policy is enabled. Records that do not
benefit sufficiently remain raw.

Raw and compressed records can coexist in one file and across files of the same bucket. WAL records
are not compressed by the bucket data/blob compression policy.

## 8. WAL Design

The WAL is redo-only. It contains begin, update, commit, abort, and checkpoint-hint records.
Insert and update records carry the new value image; delete records carry the tombstone operation.

Every WAL record has two ownership dimensions:

- logical group: transaction facts, visibility, frontier, and checkpoint-age ownership
- physical stream: WAL file namespace used for append, recovery, and abort-chain traversal

The logical group is encoded in the record and is not inferred from its physical stream.

### 8.1 Relaxed route

With `sync_on_write` disabled, each logical group writes its own physical WAL stream. Commit flushes
the WAL bytes to the file/page cache and publishes the outcome without a WAL fsync generation or a
durability wait.

Each group maintains its own checkpoint counter and retained WAL floor.

### 8.2 Durable route

With `sync_on_write` enabled, all logical groups append to one shared physical stream. The shared
stream has a namespace distinct from the per-group streams used by the relaxed route.

Concurrent terminal records and explicit barriers join a caller-led sync generation. The generation
seals one stream cut, synchronizes every file writer contributing bytes through that cut plus the log
directory, and then completes every participant whose target is covered by the cut. Transaction
outcomes are published after their durability target completes.

WAL buffer rotation and ordinary flushing do not advance the durable stream position. Only a
successful sync generation advances it.

### 8.3 Logical checkpoint age

Checkpoint age remains per logical group in both routes. WAL activity is tracked independently for
each group. A checkpoint publication advances the counter only for groups with activity since their
previous publication.

The durable route may append one physical checkpoint-hint record for the shared stream, but the
counter and retained floor for each logical group remain independent.

### 8.4 WAL recycling

WAL recycling uses three durable phases:

1. intent records the physical stream and file range selected for deletion
2. deletion removes the files and synchronizes the namespace
3. done clears the intent and advances the retained lower boundary

Startup completes an unfinished intent before scanning the affected stream.

The recycle cut combines:

- active transaction WAL pins
- pending abort-clean pins
- durable checkpoint floors
- the already-persisted recycle boundary

In the shared stream, logical groups without retained update history have inactive checkpoint-floor
slots. A group's first update activates its slot from the transaction's begin position. Checkpoint
publication advances only active slots. Route migration clears the old-era slots before new-era WAL
activity begins.

If no logical group or task pins an older file, complete files before the current append file can be
recycled while the current file remains as the stream anchor.

## 9. Recovery

Recovery finishes before foreground transaction or view admission begins.

The startup flow is:

1. load and validate metadata version and persisted options
2. complete orphan-file cleanup from durable markers
3. complete pending bucket and WAL recycle work needed for bootstrap
4. discover retained WAL streams and their durable lower boundaries
5. validate and analyze WAL records to reconstruct transaction outcomes and abort-clean tasks
6. redo committed updates not covered by each bucket frontier
7. drain reconstructed abort-clean work
8. checkpoint recovery-produced pages when the startup transition requires durable closure
9. finish any WAL route or layout transition
10. publish runtime checkpoint floors and safe visibility state
11. unload bucket runtimes loaded only for recovery
12. start the normal collector, checkpoint, and GC services

### 9.1 WAL discovery

Recovery scans every retained physical stream, independent of the route requested for the new open.
This allows it to discover history from both per-group and shared namespaces.

The durable recycle boundary defines the first file that can still exist. A validated checkpoint
hint may move record analysis forward, while the bucket frontier remains the redo decision boundary.

### 9.2 WAL validation and tail handling

Records are validated in physical-stream order. Header, length, payload layout, checksum, logical
group, and chain fields are checked before a record is consumed.

The first incomplete or malformed record ends that stream scan at the record start. With WAL tail
truncation enabled, recovery truncates there and ignores later files in the same stream. Otherwise
startup returns corruption. Runtime append positions are rebased to the truncated end before any
new append or checkpoint work begins.

### 9.3 Route and layout transition

The persisted `sync_on_write` value identifies the last WAL route whose transition completed. A
different requested value starts a route transition. Opening the durable route while legacy
per-group durable-layout files still exist starts the same rebuild process.

The rebuild computes a file-ID high-water mark from WAL files, bucket frontiers, and recycle state.
It closes recovery-produced state durably, removes the old WAL era through the normal recycle
protocol, and starts the new era above that high-water mark.

The route option is written back after the transition completes. If startup stops earlier, the next
open sees the old persisted route or remaining legacy files and repeats the transition.

### 9.4 Recovery boundaries

Recovery uses three separate WAL-related boundaries:

- bucket frontier: whether an update is already durable in bucket data
- checkpoint hint: where WAL analysis may start
- recycle boundary: which older WAL files have already been removed

These boundaries are loaded and advanced independently.

## 10. Abort-Clean

Abort-clean removes versions belonging to aborted or incomplete transactions by rewriting pages. It
does not apply inverse-value undo.

A modified abort retains:

- the aborted transaction outcome
- the physical WAL stream and chain range
- the set of buckets touched by cleanup

Abort-clean follows the transaction update chain backward. Chain positions are interpreted in the
physical stream's file-and-offset order, while logical group ownership selects the retained abort
state. Traversal stays inside the task's retained WAL range and consumes a finite step budget derived
from the retained bytes. An invalid or non-decreasing link, missing retained file, or exhausted budget
returns corruption while leaving the task and its retained state in place.

### 10.1 Runtime phases

An abort-clean task moves through two conceptual phases:

- pending rewrite
  - affected pages are rewritten
  - every touched bucket completes a fresh checkpoint
  - the abort outcome and WAL pin remain retained
- waiting for quiescence
  - the rewritten pages are durable
  - readers that could still hold old page images are allowed to drain
  - the task, WAL pin, and retained abort outcome are then removed

An unloaded bucket is not reloaded solely for steady-state abort-clean. Its task remains pending and
blocks unload/delete completion. Recovery may load a bucket to finish reconstructed cleanup, then
unloads that recovery-only runtime before open returns.

Aborted versions remain invisible throughout both phases.

## 11. Garbage Collection And Rewrite

Background GC performs four kinds of maintenance:

- reclaim fully obsolete data/blob files
- rewrite partially live data/blob files
- recycle WAL files
- finish physical cleanup of deleted buckets

Payload-file selection and rewrite execution are bucket-local. Candidate discovery may scan global
file accounting, after which files are grouped by bucket.

### 11.1 Fully obsolete files

A file with no active relocation is removed from durable statistics and interval metadata, published
as retired in runtime state, removed from runtime interval/accounting maps, and then moved through
the obsolete-file deletion pipeline.

Checkpoint old-file accounting uses conditional metadata updates, so a checkpoint prepared before
retirement does not recreate accounting for a file whose deletion committed first.

### 11.2 Partial rewrite

Partial rewrite selects files using bucket garbage ratio, target output size, recency, and live-byte
cost. The selected set is rechecked against current accounting immediately before rewrite.

The rewriter:

1. reads active relocations from the selected files
2. groups records into one or more replacement outputs
3. stages every output as an orphan
4. writes and synchronizes the replacement files
5. publishes replacement statistics and intervals together with victim removal
6. switches runtime ownership to the replacement files
7. deletes the old files through the obsolete-file pipeline

Only one rewrite runs for a bucket at a time.

### 11.3 Concurrent junk accounting

Rewrite registers its selected victim files before reading their inactive-record state. While output
is being built, newly retired logical addresses are collected with the rewrite ownership state.

When replacement relocations are complete, collected addresses that were copied are transferred to
the corresponding replacement-file accounting. A checkpoint that reaches the ownership switch
waits for rewrite publication, resolves the logical address again, and applies the retirement to its
new file owner.

The ownership state returns to idle after durable metadata and runtime interval/accounting state both
refer to the replacement files. A rewrite with no replacement output cancels the ownership transfer.

## 12. Cache, Eviction, And Backpressure

Each bucket has independent cache, page-pool, checkpoint-size, and foreground-admission policies.

Eviction selects resident pages, consolidates them when needed, and publishes the resulting page
images through the same dirty-generation flow as foreground tree changes. Eviction does not create a
separate durability path.

When bucket backpressure is enabled, a writer reserves dirty-memory budget before entering tree
mutation. Admission is based on current dirty bytes, checkpoint progress, and a short burst allowance.
Checkpoint completion releases pressure and wakes waiting writers in batches.

Buckets with backpressure disabled enter mutation directly.

## 13. Startup And Shutdown Lifecycle

Database startup completes metadata loading, recovery, route transition, and recovery-only cleanup
before background services and foreground admission begin.

Shutdown begins when the final database or bucket handle is dropped. The runtime then:

1. stops background GC admission
2. waits for admitted transactions to publish terminal outcomes
3. drains pending abort-clean work
4. checkpoints every loaded bucket
5. fully synchronizes files written by the final checkpoint
6. completes a WAL barrier on the active route
7. stops the visibility collector and releases metadata state

The final checkpoint runs with full file synchronization even when relaxed WAL commit mode was used
during normal operation. After a successful graceful shutdown, persisted pages represent the
committed model without relying on retained WAL files.

## 14. Filesystem Boundary

Mace uses an internal filesystem boundary for:

- runtime file opens
- path existence checks
- directory enumeration and creation
- rename and removal
- directory synchronization

Opened-file read/write behavior remains part of the data, blob, WAL, and metadata components rather
than a general virtual filesystem interface. The metadata-store dependency also manages its own file
access.

Path probes distinguish absence from other I/O errors. Namespace synchronization remains explicit in
payload publication, WAL rotation/recycle, route transition, and recovery cleanup.

The filesystem object is process-local and is not part of persisted engine options.

## 15. Persistent Format And Upgrade Model

Mace currently supports 64-bit little-endian platforms. Persisted scalar and packed-layout handling
is defined under that platform boundary.

There are three format families:

- data/blob files
- metadata organization
- WAL records

### 15.1 Data/blob format

The current data/blob file version is 1. The version is stored once in the fixed footer; individual
payload frames do not carry a separate format version.

The writer emits the current version. Readers dispatch from the file version and retain support for
every data/blob version still inside the supported online-upgrade window. A directory may therefore
contain files from more than one supported version.

Checkpoint and GC rewrite emit the current version. When source and target versions differ, rewrite
decodes the source record and encodes it in the target version instead of copying encoded bytes.

Retiring historical reader support requires an explicit migration boundary.

### 15.2 Metadata format

The metadata store has one current Mace metadata version. Startup validates it before loading
database state.

An incompatible metadata organization change uses a new metadata version and an offline metadata
migration. Runtime startup does not rewrite an unsupported metadata organization in place.

### 15.3 WAL format

The current route split changes physical stream placement but not WAL record encoding. Both routes
use the same WAL record format and recovery decoder.

WAL format evolution is handled independently from data/blob file evolution. A WAL format change
requires a migration design that first reaches a WAL-independent durable state.

## 16. Observability

Mace exposes fixed-cardinality counters, gauges, histograms, and events.

High-frequency latency observations may be sampled. Lifecycle, recovery, checkpoint, GC, and
durability events are emitted directly. Metric identities remain bounded and do not include
unbounded bucket, key, or transaction labels.

## References

- The Bw-Tree: A B-tree for New Hardware Platforms
- Bf-Tree: A Modern Read-Write-Optimized Concurrent
- LLAMA: A Cache/Storage Subsystem for Modern Hardware
- Efficiently Reclaiming Space in a Log Structured Store
- LeanStore: In-Memory Data Management Beyond Main Memory
- Scalable and Robust Snapshot Isolation for High-Performance Storage Engines
- Rethinking Logging, Checkpoints, and Recovery for High-Performance Storage Engines
- Larger-Than-Memory Range Index
- Optimistic Lock Coupling: A Scalable and Efficient General-Purpose Synchronization Method
...