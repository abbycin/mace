# Mace Design

This document describes how the current Mace storage engine is designed and how its major runtime
and persistence flows work.

The live correctness constraints, their evidence, and their verifier coverage are maintained in
`docs/constraints/registry.yaml`. This document does not duplicate that ledger and does not serve
as a source-code tour.

## 0. Upstream contracts Mace relies on

`btree-store` owns the metadata store. Four of its contracts are load-bearing, and a
checker or a reviewer should be able to find them here rather than in a test index:

- **conditional update.** `exec`/`update` on an existing key changes it and reports success;
  on an absent key it changes nothing and reports a miss. Mace treats that miss as
  "someone deleted this stat first" and never recreates the record.
- **conditional delete.** `del` on an existing key removes it and reports success; on an absent
  key it removes nothing and reports a miss. Mace tolerates that miss only for idempotent reclaim
  and cleanup deletes: a stat record, an obsolete-file marker, and an orphan marker cleared during
  open cleanup. Every other metadata delete treats a miss as an invariant failure.
- **one-generation publication.** `exec_multi` publishes every bucket-root change of one
  manifest transaction in a single generation. On failure it restores the previous bucket roots;
  consumed metadata high-water state may still be published as a rollback generation. Mace relies
  on the bucket-root property to publish map, frontier, stats and sequences as one step.
- **fatal page damage.** Corruption that only a page read reaches terminates the process:
  `btree-store` prints a `BTREE_FATAL_*` line and aborts. The I/O variant names the file and
  offset; the corruption variant names the fault, generation and page identity instead. A
  read-only consumer of a database directory (the offline checker) inherits that termination
  instead of reporting a partial picture.

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
- background services for checkpoint completion, abort-clean, WAL recycling, payload GC, bucket
  cleanup, visibility collection, and eviction

Bucket runtimes are loaded lazily. Opening a database reconstructs global durable state and WAL
state without eagerly loading every bucket index; recovery itself runs synchronously before
foreground admission begins.

## 2. Metadata And Addressing

Mace separates logical identity from physical placement.

The main identities are:

- bucket ID: durable identity of a named bucket
- page ID: logical identity of an index page inside a bucket
- logical address: durable identity of a persisted record inside a bucket address space
- file ID: identity of a data or blob file; a WAL file is identified by its physical stream id
  together with its per-stream sequence number

A persisted page is resolved in three steps:

1. the bucket page table maps page ID to logical address
2. the bucket interval map maps logical address to a data or blob file
3. the file relocation table maps logical address to a byte range in that file

GC rewrite changes physical placement while preserving logical addresses. Page references and
history references therefore remain stable across file rewrite.

### 2.1 Metadata groups

The metadata store organizes state into:

- global sequences and persisted options
- bucket names, IDs, options, and lifecycle state
- per-bucket page tables
- per-bucket data and blob interval maps
- per-file data and blob statistics
- per-bucket durable WAL frontiers
- orphan and WAL-recycle records as prefixed keys in the miscellaneous group, plus separate
  obsolete-file and pending-delete records

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

Creating a bucket allocates a durable bucket ID, stores the bucket options, and initializes its
durable frontier in one metadata publication. Global bucket accounting is runtime state: it is
incremented with the creation and recomputed from the catalog plus pending deletes on open.

The name enters the in-memory catalog before that transaction commits, so a concurrent reader can
observe the bucket during creation. The new bucket's runtime context and root leaf are initialized
as part of creation; other buckets stay lazy.

### 3.2 Loading

Loading a bucket reconstructs its runtime from:

- bucket options
- page table entries
- data and blob interval maps

The durable frontier and per-file accounting are manifest-global state. Checkpoint publication
reads them; recovery compares bucket frontiers with WAL positions. They are not per-bucket load
inputs.

The loaded runtime owns the bucket cache, dirty generations, address allocator, checkpoint state,
and flow-control state.

### 3.3 Unloading

Unloading removes the cached bucket runtime. A live bucket is final-checkpointed before its
runtime is dropped, so dirty pages, its page table, frontier and statistics are published first;
delete instead discards the sealed batch and removes the bucket. The active WAL route is
synchronized before unload.

An unload or delete attempt while abort-clean work is pending is refused with `Again`, so the
bucket remains loaded until that task has completed its page rewrite, durable checkpoint, and
reader-quiescence phase.

### 3.4 Deletion

Deletion has two phases:

1. logical deletion removes the bucket from the visible catalog, records a pending-delete entry,
   records the bucket's file deletions (which also delete their stat records), and removes its
   obsolete files once the commit succeeds, or on a later retry if deletion is suspended
2. background cleanup drains the page-table and interval btrees and the pending-delete record in
   bounded batches

Global bucket accounting includes pending-delete buckets. It is decremented when physical cleanup
finishes.

### 3.5 Bucket options

Bucket options are persisted with the bucket. The persisted fields define page interpretation and
maintenance policy: the inline-value boundary, node split cardinality, cache and pool budgets,
checkpoint sizing, eviction and consolidation thresholds, compression, and backpressure. The merge
operator is runtime-only and never reaches manifest metadata, WAL records, or pages.

The inline-value boundary and node split cardinality define persisted page interpretation. Node split
cardinality cannot change after bucket creation, and the inline-value boundary may only be lowered:
rebuild materialization moves the affected values into large-value records, while raising it would
have to convert stored large values back into inline rows and retire their blob payloads. Other
persisted bucket options are runtime or output policies and may be updated while the bucket is
unloaded and no rewrite is active. The updated values take effect on the next load.

A bucket runtime chooses either one merge operator or no operator before it becomes visible to
foreground operations. That choice is immutable for the lifetime of the runtime. Re-opening an
already loaded bucket without an operator leaves its choice unchanged; presenting the same operator
is compatible, while presenting a different operator or adding one to an operator-less runtime is
rejected. Unloading removes the runtime choice, so the next load may choose again. Operator identity
is not persisted and semantic compatibility across process restarts belongs to the caller.

The operator is a total user function with no failure channel. It combines a chain of operands and
then applies that result to an optional base; `None` is the only logical deletion. Empty results
are contract violations. The same bucket may use different algebra for different stable user keys,
but the meaning of a key must not change while its runtime is loaded. Merge admission never invokes
the operator, and operator panics are not caught by the engine.

Merge-aware compaction uses the selected operator to materialize merge runs at the snapshot-safe
boundary: a contiguous run older than that boundary folds into one plain base row carrying the
boundary version identity, while newer versions remain verbatim. The replacement retains the
lineage of every absorbed WAL position, so recovery neither redoes nor skips an operand. Retained
aborted versions are invisible to snapshots and never participate in a fold or serve as its barrier:
below-safe aborted versions may be dropped as junk by ordinary consolidation, while abort-clean owns
the retained-abort fact and WAL pin that make the abort state retire; above-safe aborted versions
remain verbatim until then. This prevents both leaking aborted bytes into a synthesized row and
dropping a row whose boundary version is still needed by abort-clean. Folded values use the normal
inline or large-value persistence rules.

When no operator is selected, compaction remains raw-preserving. Reads that encounter a visible
merge operand return an invalid-operation result rather than interpreting its bytes. If folding
produces an empty or oversized result, compaction also remains raw-preserving and reads report a
contract violation. In both cases the operand chain remains available for a later runtime loaded
with a compatible operator. A safe tombstone is an absence barrier; versions older than it,
including raw merge operands, may be discarded as unreachable history.

A folded value past the global value-size limit, or an empty fold output, is a merge contract
violation. Compaction of such a key never fails publication or a committed transaction. Reliable
reads report the contract-violation result instead of a truncated or degraded value, and subsequent
merges on that key are rejected until a committed delete resets it; an aborted delete does not reset
it. This prohibition is runtime-only and need not survive unloading. The raw operand chain remains
crash-safe, and a later compaction may retry materialization. Recovery replays operands without
folding or structural maintenance, then defers those operations until normal runtime has started.

Compression is an output policy. Enabling or disabling it affects newly written data/blob records;
existing raw and compressed records remain readable in the same bucket.

### 3.6 Global options

An initialized metadata store contains a persisted subset of global engine options. Opening the
database validates the persisted record before recovery.

Writer-group cardinality defines persistent transaction ownership. It is fixed for an ordinary
reopen: a reopen that requests a different `concurrent_write` starts an epoch migration instead of
being rejected. The transition recovers at the persisted (source) width, rebuilds the WAL epoch,
brings its runtime state up at the requested width, and only then records the new width in the
persisted options. Runtime and maintenance policies are refreshed from the current open options.
The persisted WAL route records the last route whose startup transition completed.

Process-local objects, such as the observer and filesystem instance, are not persisted.

## 4. Index And Value Layout

Each bucket uses a Bw-Tree style ordered index with immutable page images and delta-based updates.
Tree mutation publishes replacement pages rather than modifying durable pages in place.

Leaf records contain versioned key/value state. The split is on the packed value record, including
its group byte: a record whose packed size is strictly below the bucket inline boundary remains in
the leaf representation; a record at or above the boundary is stored as a blob record and referenced
by logical address.

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

The snapshot remains pinned for the lifetime of a transaction or read-only view. An iterator
borrows that transaction or view, so the pin covers iteration; a returned `ValRef` holds its own
page-image reference and keeps the bytes alive without extending the snapshot pin.

## 5. Transactions And Snapshot Isolation

Writer transactions are distributed across logical writer groups. Group selection compares two
candidate groups and chooses the less loaded one. A writer keeps the selected group for its entire
lifetime.

A transaction records:

- a start timestamp
- logical writer-group ownership
- a WAL begin position

A modified transaction also records an exact outcome: active, committed with commit timestamp, or
aborted.

Every writer publishes an active fact at start. A transaction that performs no write keeps that
active fact until commit or abort removes it; it allocates no commit timestamp and publishes no
committed or aborted fact.

A read-only view has a snapshot timestamp but no writer-group ownership.

### 5.1 Writer start and outcome publication

Writer start first marks its registration as in progress, then allocates and exposes the start
timestamp, appends the WAL begin record, publishes the active outcome, and finally marks the
registration stable. The visibility collector treats an in-progress registration as an unfinished
scan source and does not publish a new safe boundary from that scan.

For a modified transaction, commit records the terminal WAL entry, completes the selected WAL
durability policy, allocates the commit timestamp, and publishes the committed outcome. Abort
publishes the aborted outcome and transfers WAL retention to an abort-clean task. For an unmodified
transaction, commit records the terminal entry and removes its active fact without a commit
timestamp; abort removes it without publishing an aborted fact.

### 5.2 Visibility

Visibility is evaluated in this order:

1. a transaction sees its own versions
2. versions whose writer started at or after the snapshot are excluded
3. records whose logical group id is at or above the current runtime width use the settled-history
   class described below; this test runs before any group-table access, so a retired group index is
   never dereferenced
4. versions below the published safe boundary use the positive visibility path
5. retained abort state is checked before a positive result is accepted
6. versions outside the safe boundary use their exact transaction outcome
7. if an exact committed outcome has already been pruned, a newer safe-boundary publication may
   provide the result

An exact commit is visible only when its commit timestamp precedes the snapshot. Active and aborted
versions are not visible.

Once a width-change epoch migration has completed, retained records whose logical group id is at or
above the current runtime width are answered from the settled-history class: they predate the
current epoch, so their transactions were resolved and their aborted versions physically removed
while the migration still ran at the persisted (source) width. They are accepted without consulting
a live transaction fact or the retained-abort state, and they never index the runtime group table.
The class covers every record-derived decision, not only point reads: compaction and merge folding
treat such a record as committed history, and abort-clean never claims it as a retained abort.

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

Same-key writers use first-writer-wins conflict handling. Concurrent merge operands on one key are
the exception: when the bucket has a runtime operator, merge/merge writers may stack operands.
Every retry or page-replacement path checks the latest key head again before publishing a mutation,
so an earlier successful writer cannot be silently overwritten by a stale retry.

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

When a bucket is being deleted, its sealed batch is discarded instead of being published; the
delete path has already excluded new mutations and removes the bucket. A live bucket that is only
unloaded is final-checkpointed before its runtime is dropped, so its dirty state is published
rather than discarded.

### 6.2 Bucket durable frontier

Every checkpoint derives a per-bucket frontier with one position for each logical writer group. It
includes the WAL effects folded into the durable page closure, including effects from groups other
than the group recorded in an individual page header.

The frontier is published together with the page map and file metadata. Recovery uses it to decide
whether a WAL update is already represented in durable pages.

### 6.3 Checkpoint completion

After metadata publication:

- the sealed generation is released and its containers are recycled
- still-live state above the snapshot boundary was carried into the hot generation when the cut was
  taken, not after publication
- retired page images were already handed to deferred reclamation when their page was replaced or
  evicted
- WAL checkpoint floors are advanced for every configured group with an active floor; inactive
  shared-stream slots stay inactive. The per-group checkpoint counter advances only for groups with
  WAL activity since the previous publication
- checkpoint progress updates bucket flow control

## 7. Data And Blob Publication

Checkpoint and rewrite both publish payload files with data first and metadata last.

For each output file, the flow is:

1. persist an orphan marker
2. build the file
3. synchronize the file and required directory state
4. commit intervals, statistics, map/frontier updates, sequences, and orphan-marker removal in
   metadata; relocations are already in the payload file's relocation region, covered by the
   footer and by step 3's synchronization
5. make the runtime interval and accounting view reflect the output (a checkpoint updates it during
   file construction; a rewrite publishes it after the metadata commit)

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
Update records carry an insert/update value image, a tombstone, or a merge operand; merge operands
are replayed as merge records, not folded during recovery.

WAL records have two ownership dimensions:

- logical group: transaction facts, visibility, frontier, and checkpoint-age ownership
- physical stream: WAL file namespace used for append, recovery, and abort-chain traversal

An update record encodes its logical group. Begin, commit, abort, and checkpoint-hint records carry
transaction or position identity instead; recovery resolves terminal facts by transaction id and
takes an abort's logical group from its update chain.

### 8.1 Relaxed route

With `sync_on_write` disabled, each logical group writes its own physical WAL stream. A modified
commit flushes the WAL bytes to the file/page cache and publishes the outcome without a WAL fsync
generation or a durability wait; an unmodified commit records its terminal entry and removes its
active fact without a flush or durability wait.

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
3. complete WAL recycle work needed for bootstrap; pending bucket reaping is counted here but runs
   in the GC service after recovery
4. discover retained WAL streams and their durable lower boundaries
5. validate and analyze WAL records to reconstruct transaction outcomes and abort-clean tasks
6. redo committed updates not covered by each bucket frontier
7. drain reconstructed abort-clean work
8. checkpoint recovery-produced pages when the startup transition requires durable closure
9. unload bucket runtimes loaded only for recovery
10. finish any WAL route or layout transition
11. publish runtime checkpoint floors and safe visibility state
12. start the normal collector, checkpoint, and GC services

### 9.1 WAL discovery

Recovery scans every retained physical stream, independent of the route requested for the new open.
This allows it to discover history from both per-group and shared namespaces.

A reopen that requests a different `concurrent_write` keeps the retained record scan at the
persisted (source) width. Recycle completion and the epoch high-water cover the fixed slot space of
`MAX_CONCURRENT_WRITE` writer-group slots unioned with every WAL namespace present on disk and with
the shared stream, so a recycle intent left behind by a wider interrupted attempt is completed even
when the current open requests a narrower width. The era wipe sweeps the `max(source, target)` bound
unioned with every WAL namespace present on disk, so streams that only the target width owns are
cleaned and given a durable recycle boundary as well. The redo scan and group-id validation stay
at the source width until the wipe has completed; the target width takes over only after the wipe,
once the runtime state has been rebuilt at that width.

The durable recycle boundary defines the first file that can still exist. A validated checkpoint
hint may move record analysis forward, while the bucket frontier remains the redo decision boundary.

### 9.2 WAL validation and tail handling

Records are validated in physical-stream order. Header, length, payload layout, checksum, and the
update's logical group are checked before a record is consumed; chain positions are validated by
abort-clean when it traverses a retained chain.

The first incomplete or malformed record ends that stream scan at the record start. With WAL tail
truncation enabled, recovery truncates there and ignores later files in the same stream. Otherwise
startup returns corruption. Runtime append positions are rebased to the truncated end before any
new WAL append or WAL checkpoint record; reconstructed abort-clean may already have checkpointed
pages before that point.

### 9.3 Route and layout transition

The persisted `sync_on_write` value identifies the last WAL route whose transition completed. A
different requested value starts a route transition. A width change starts the same rebuild; the
rebuild completes before the requested options are recorded, and a crash before that writeback is
retried with the persisted width. Opening the durable route while legacy per-group durable-layout
files still exist starts the same rebuild process.

The rebuild computes a file-ID high-water mark from WAL files, bucket frontiers, and recycle state.
It closes recovery-produced state durably, removes the old WAL era through the normal recycle
protocol, and starts the new era above that high-water mark. The wipe sweeps every physical stream
in `[0, max(source, target))`, every WAL namespace present on disk, and the shared stream —
including streams that only the target width owns, which can only hold era files left by an earlier
interrupted attempt — and advances each swept stream's durable recycle boundary to the new era
start.

The recovery scan itself is per stream: every retained physical stream is read at that stream's own
boot position, and only the global epoch start crosses over to the runtime era. A rebuild therefore
ends the recovery scan positions before it removes the old era:

- it stops the recovery checkpoint flusher, then detaches and closes every logger's writer, so the
  wipe never rides on a filesystem that tolerates removing an open file;
- it wipes the swept streams and advances each one's durable recycle boundary to the new era start,
  leaving a stream already at or above that start untouched;
- it installs the new era's writer at `epoch_start` with the new era's checkpoint floors. A
  same-width rebuild requires an empty epoch file and rejects non-empty content; a width-change
  context opens the target file with append at its current end;
- it syncs the log directory, then restarts the checkpoint flusher on that context for a same-width
  rebuild, because the flusher was stopped above and every later bucket load requires one;
- only then does the options writeback record the transition.

The new era starts at `epoch_start`, the file-ID high-water mark plus one. If no WAL file, bucket
frontier or recycle state anywhere has history, `epoch_start` is zero; otherwise every swept stream
below the global start receives a durable recycle boundary, including an empty stream.

Until the install completes, a foreground writer does not exist and the runtime WAL barrier is
undefined: an error in that window leaves through a recovery-open cleanup that closes the context
without a barrier. Because every stream keeps its own scan position, no step of this sequence can
leave a hole inside a retained range, so a crash at any point is either continued by the next open
or migrated again.

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
- the bucket that owns the aborted updates

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

Steady-state abort-clean does not reload a bucket that is not loaded. A bucket with pending
abort-clean work is not unloaded in the first place: unload and delete are refused with `Again`
until the task's page rewrite, durable checkpoint, and reader-quiescence phase complete. Recovery
may load a bucket to finish reconstructed cleanup, then unloads that recovery-only runtime before
open returns.

Aborted versions remain invisible throughout both phases.

## 11. Garbage Collection And Rewrite

Background GC runs abort-clean first, then four payload/maintenance kinds:

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

Rewrite closes the file-kind producer gate before registering its selected victim files. The gate,
active producer permits, collector phase, and collected addresses share one collector mutex per file
kind; waits use a condition variable. Closing it for one bucket's rewrite therefore holds off junk
permits for other buckets of the same kind while it is closed. A checkpoint acquires a permit for
every kind with junk before merging the bucket frontier or opening its metadata transaction, and
releases it only after the transaction and immediate runtime bookkeeping complete. A producer waits
with no manifest or stat lock held.

After the initial drain, rewrite reopens the gate before reading durable inactive masks and scanning
victim records. When replacement files are synchronized, it closes the gate again, drains permits,
and enters the publishing cut only while the gate is closed and the permit count is zero. It then
transfers collected addresses and publishes replacement statistics, intervals, remaps, victim
deletion, and orphan cleanup in one metadata transaction. Physical deletion remains outside the
gate. The publishing phase has no producer retry or wait path.

The collector separates final rewrite victims from fully-obsolete candidates. For a final victim,
checkpoint rechecks its owner, loads the inactive bitmap, resolves relocation, updates the old stat,
and queues the address under the collector mutex. A fully-obsolete candidate becomes runtime-only
`Ordinary` under that mutex, is reclaimed without it, and is cleared before publish. Stale
`Ordinary` snapshots follow ordinary reclassification; a missing final-victim stat is fatal.

When replacement relocations are complete, collected addresses that were copied are transferred to
the corresponding replacement-file accounting. A checkpoint that enters after the gate reopens
resolves the logical address again and applies the retirement to its current owner. Fully obsolete
reclaim closes the same gate and rechecks durable statistics before deleting; a missing durable
stat means that GC already retired the candidate, while an active durable stat is an invariant
failure.

The ownership state returns to idle after durable metadata and runtime interval/accounting state both
refer to the replacement files. A rewrite with no replacement output cancels the ownership transfer.

## 12. Cache, Eviction, And Backpressure

Each bucket has independent cache, page-pool, checkpoint-size, and foreground-admission policies.

Eviction selects resident pages, consolidates them when needed, and publishes the resulting page
images through the same dirty-generation flow as foreground tree changes. Eviction does not create a
separate durability path.

When bucket backpressure is enabled, a writer reserves dirty-memory budget before entering tree
mutation. Admission compares dirty bytes plus outstanding reservations and the new write against the
pool capacity; when dirty is already at capacity and a checkpoint round is draining, a bounded
credit of `min(checkpoint_size, pool_capacity / 8)` raises that ceiling. Checkpoint progress paces
batched waiter wake-ups, never admission headroom; completion wakes all remaining waiters.

Buckets with backpressure disabled enter mutation directly.

## 13. Startup And Shutdown Lifecycle

Database startup completes metadata loading, recovery, route transition, and recovery-only cleanup
before foreground admission begins. The bucket checkpoint flusher starts earlier so recovery
checkpoints can run; the visibility collector, GC, and evictor start after recovery.

Shutdown begins when the final database or bucket handle is dropped. The runtime then:

1. stops background GC admission
2. waits for admitted transactions to publish terminal outcomes
3. drains pending abort-clean work
4. stops the evictor
5. final-checkpoints every loaded bucket with full file synchronization and joins the checkpoint
   flusher
6. completes the active route's WAL durability step (a shared barrier on the durable route,
   per-logger flush on the relaxed route)
7. stops the visibility collector and releases metadata state

The final checkpoint runs with full file synchronization even when relaxed WAL commit mode was used
during normal operation. Persisted pages carry the committed model, but recovery may still analyze
retained WAL to rebuild transaction outcomes for folded versions.

## 14. Filesystem Boundary

Mace uses an internal filesystem boundary for:

- runtime file opens
- path existence checks
- directory enumeration and creation
- rename and removal
- hard links and whole-file copies used by checkpoint export
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
payload frames do not carry a separate format version. Writers emit version 1, and readers reject
unknown versions or unsupported footer fields.

Checkpoint and GC rewrite emit version 1. A future format version must define its reader support and
an explicit migration boundary before it can coexist with the current format.

### 15.2 Metadata format

Two version axes gate the metadata store. `btree-store` owns the manifest file format (currently
version 2), validated when the store is opened. Mace owns a `current_version` metadata record
(currently 1), validated before database state loads.

A change to either axis uses an offline migration; runtime startup does not rewrite an unsupported
format in place. The 0.1.2 -> 0.2.0 hop moves the `btree-store` manifest format from 1 to 2; Mace's
`current_version` stays 1. The migrations, the format versions they move between, and the procedure
are in [docs/migration.md](./migration.md).

### 15.3 WAL format

The current route split changes physical stream placement but not WAL record encoding. Both routes
use the same WAL record format and recovery decoder.

WAL format evolution under the current release model is one-way: only newer binaries open a
directory after merge-capable payloads are written, so additive payload types (such as the merge
operand payload) require no migration barrier and no old-binary compatibility window. Deploying a
binary that writes new WAL payload kinds therefore commits that directory to binaries of at least
that capability level; downgrades are unsupported. This boundary applies to additive payload types
under an unchanged record envelope. Any change to the shared Update envelope itself, or any other
non-additive encoding change, still requires a migration design that first reaches a WAL-independent
durable state.

## 16. Observability

With the non-default `metrics` feature enabled, Mace exposes fixed-cardinality counters, gauges,
histograms, and events; without it the observer and every observation call are compiled out.

High-frequency latency observations may be sampled. Recovery, GC, orphan, transaction-conflict and
merge-contract activity is reported through counters, gauges, histograms where appropriate, and a
small fixed set of events. Metric identities remain bounded and do not include unbounded bucket,
key, or transaction labels.

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
