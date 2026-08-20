# Mace Design

This document records Mace's stable architecture, persistence boundaries, lifecycle rules, and
format-upgrade model.

It intentionally describes protocol and design, not function-level implementation.

## 1. Goals

Mace is an embedded key-value engine with the following design goals:

- predictable point-read latency
- high write throughput through append-only WAL plus asynchronous durable publish
- snapshot isolation with MVCC visibility
- bucket-local runtime state and lazy bucket loading
- key/value separation, with large values stored in blob files
- optional per-bucket compression for persisted data/blob files
- crash-safe startup, publish, cleanup, and file-rewrite behavior

## 2. System Model

Mace is organized around four persistent domains and three long-lived runtime domains.

Persistent domains:

- metadata
  - stored in a stable B-tree metadata store
  - records bucket catalog, numeric state, address maps, file stats, durable frontiers, and
    cleanup queues
- WAL
  - append-only redo log for transactional updates
- data files
  - persisted page and structural records
- blob files
  - persisted large-value payload records

Runtime domains:

- foreground transaction path
  - reads, writes, conflict checks, view creation, and commit/abort
- bucket runtime
  - dirty generations, page cache, address allocation, backpressure, and checkpoint preparation
- background services
  - checkpoint publish, recovery, abort-clean, GC rewrite, and durable file deletion

The bucket is the main unit of runtime isolation.
Logical addresses, dirty state, backpressure, durable frontier, and most GC decisions are scoped
per bucket.

### Filesystem Boundary

Mace has an internal filesystem boundary for runtime namespace operations and runtime file opens.

This boundary is intentionally narrow.
It exists to make correctness-sensitive path operations explicit and injectable, not to provide a
general virtual filesystem layer.

Its design scope is:

- opening runtime-owned files
- existence checks
- directory enumeration
- directory creation
- rename and file removal
- directory sync

Its design non-goals are:

- extending path-level injection into the opened-file read/write layer
- hiding durability barriers inside a black-box abstraction
- becoming a public extension surface for user-defined filesystems
- forcing the metadata store dependency to share the same filesystem boundary

The boundary is internal to Mace.
Opened-file IO remains outside this boundary, while path-level operations go through it.

Existence checks must preserve the difference between:

- a path that is genuinely absent
- an IO failure while checking that path

Correctness-sensitive code must not collapse those two outcomes into the same "does not exist"
result.

The filesystem boundary also serves fault injection.
Its role is different from crash failpoints:

- crash failpoints model where the process stops inside a protocol window
- filesystem injection models what syscall-level error a path operation returns

Both are required.
Crash-window coverage alone is not enough to validate namespace and IO error handling.

## 3. Persistent Metadata Model

Metadata is stored separately from data/blob payload files.
Its job is to describe which payload files are durable, which logical addresses map to which files,
and which cleanup work is still pending.

The metadata model contains these conceptual groups:

- numeric state
  - global ID allocation
  - orphan-file markers
  - durable WAL recycle state
- bucket catalog
  - bucket identity
  - bucket options
  - pending-delete state
- durable frontier
  - per bucket, per writer-group durable boundary
- durable address mapping
  - page table from logical page identity to durable logical address
  - interval maps from logical address ranges to data/blob file IDs
- file accounting
  - per-file live/total bytes and element accounting
  - obsolete-file queues

This metadata is authoritative for durable reopen and recovery.
Runtime-only caches may be dropped and rebuilt from it.

Mace relies on the metadata store for atomic commit and conditional-update semantics.
Those semantics are part of Mace's correctness boundary, not an interchangeable implementation
detail.

## 4. Addressing And File Mapping

Mace uses several different identities that must not be conflated:

- bucket ID
  - identifies a bucket in metadata
- file ID
  - identifies one persisted data/blob file
- page ID
  - identifies a logical page within a bucket
- logical address
  - identifies one persisted record position in the bucket's logical address space

Design rules:

- each bucket owns an independent logical address space
- page table entries resolve page IDs to the currently durable logical address
- interval maps resolve logical address ranges to the owning data/blob file
- each data/blob file carries a relocation table that resolves a logical address to a byte offset
  inside that file

The address-resolution path is therefore:

1. page ID to logical address
2. logical address to file ID
3. logical address to file-local byte offset

This layering allows file rewrite without changing logical identities.

## 5. Bucket Lifecycle

### 5.1 creation

Bucket creation is an atomic metadata publication that:

- allocates a bucket ID
- persists bucket options
- initializes the durable frontier
- updates global bucket accounting

Creation must fail cleanly if the bucket already exists or if global bucket limits are exceeded.

### 5.2 loading

Bucket runtime state is lazy-loaded.
Opening the database does not require eagerly materializing every bucket runtime.

Loading a bucket reconstructs runtime state from metadata, especially:

- page table state
- data/blob interval maps
- bucket options that affect runtime policy

### 5.3 unload

Unload is a runtime-only operation.
It removes cached runtime state but does not change durable metadata or payload files.

Unload must not weaken recovery or background-clean correctness.
If page-touching abort-clean work is still pending for a bucket, unload is blocked until that work
crosses its durability barrier.

### 5.4 delete

Bucket deletion is two-phase:

1. logical delete
   - remove the bucket from the visible bucket catalog
   - mark the bucket as pending physical cleanup
   - record all durable auxiliary state that must later be removed
2. physical cleanup
   - delete durable page table, interval state, obsolete files, and related metadata in bounded
     background work

The global bucket count is decremented only after physical cleanup is complete.

### 5.5 option updates

Bucket options fall into two design classes:

- compatibility-sensitive options
  - changing them would alter how persisted bytes are interpreted
  - they are not updated across already-created durable state
- runtime-policy options
  - they affect future runtime behavior but do not invalidate old persisted bytes
  - they may be updated and take effect on the next bucket load

Compression enablement belongs to the second class.
Turning compression on or off is mixed-format safe because each persisted record remains
self-describing.

### 5.6 global engine options

Initialized databases must also carry durable global engine configuration.

Global options follow the same split as bucket options:

- compatibility-sensitive options
  - they define persisted runtime shape or recovery expectations
  - reopening an existing database must not silently change them
- runtime-policy options
  - they tune future runtime behavior without invalidating previously persisted bytes
  - reopening may update their durable baseline for subsequent opens

Transient process-local inputs are not part of the durable global configuration.

If durable global configuration is missing or malformed for an otherwise initialized database,
startup must treat that as metadata corruption instead of rebuilding defaults implicitly.

## 6. Dirty Generations And Checkpoint Cut

Mace uses a two-generation dirty-state model:

- hot generation
  - receives new writes and publications
- sealed generation
  - retains the previous hot state while a checkpoint publish is in flight

The checkpoint cut is an atomic transition from one hot generation to the next.

The two-generation model applies across multiple independent dirty-state channels: page
identities, retired address chains, junk address sets, and dirty root / inflight / unmap
markers. All channels rotate together under the same cut gate. Each channel type accumulates
independently; the cut is a single gate operation that atomically seals all of them.

Design requirements:

- foreground writers must not straddle the cut
- live pages must never disappear from both hot and sealed generations at the same time
- pages newer than the checkpoint snapshot boundary must remain discoverable until they are either
  carried forward or durably published

The checkpoint process therefore has two responsibilities:

1. establish a closed snapshot boundary
2. carry forward any still-live state that cannot yet be considered durably published

When a bucket is being unloaded or deleted at cut time, its sealed batch is skipped rather than
published. This is safe because the bucket's durable state is already consistent and no new
mutations can enter a bucket that is leaving the active set.

## 7. Reachability And Dirty-State Safety

Dirty state is not only about roots.
It must also preserve reachability through structural links that keep old or auxiliary pages alive
during ongoing publication.

There are two important classes of retired addresses:

- structural junk
  - old versions or pages retired directly by replacement or eviction
- compaction junk
  - addresses discovered while reorganizing a structure, but still potentially reachable until the
    checkpoint publish boundary closes

Design rule:

- structural junk may be retired from hot state under the normal deferred-reclamation rules
- compaction junk must remain discoverable until durable closure proves it is no longer reachable

If this rule is broken, a reader may still hold a path to an old address that no longer exists in
dirty memory and is not yet reflected in durable interval metadata.

## 8. MVCC And History Model

Mace provides snapshot isolation through fact-based MVCC.

Stable rules:

- every writer transaction has a unique start timestamp and belongs to one writer group
- writer group assignment uses two-choices load balancing: each new writer samples two candidate
  groups and joins the one with the lower inflight count, keeping groups balanced without a
  centralized scheduler
- a transactional reader carries its snapshot timestamp and ownership identity, while a read-only
  view carries an independent lifetime pin without writer ownership
- every writer group tracks the exact active, committed, or aborted outcome of its transactions
- group-local and global resolved boundaries are positive proofs only; they never bypass abort
  validation
- the published abort boundary is a conservative negative proof and may lag safely behind exact
  transaction outcomes

### 8.1 writer begin and reader registration

Foreground writer begin is serialized within its writer group and follows this publication order:

1. mark the registration as unstable
2. allocate the transaction start timestamp
3. expose that timestamp to background collection
4. record the WAL begin
5. publish the active transaction outcome
6. mark the registration as stable before returning

A read-only view transitions through registering, active, and idle states. Its snapshot pin remains
active until the view and every iterator or borrowed read derived from it are dropped.

The collector timestamp cut, writer registration publication, writer start timestamp allocation,
and reader registration publication must have one global order. A collector round takes its cut
before scanning writer registration state, exact transaction outcomes, and the reader registry
captured for that round. If a writer is absent from that scan, the order proves its start timestamp
was allocated at or after the cut.

### 8.2 visibility evaluation

For a fixed snapshot, visibility uses this proof order:

1. a version written by that same transaction is visible to provide read-your-writes
2. a version whose writer started at or after the snapshot is invisible
3. an older version covered by the global safe boundary has a positive commit proof
4. every positive proof still passes abort validation
5. when no boundary proves the outcome, the exact transaction outcome decides visibility:
   committed before the snapshot is visible, while active or aborted is invisible
6. if the exact outcome has already been pruned, the global safe boundary is read again; only a
   boundary published after the first lookup may justify treating the missing outcome as committed

The global safe boundary is a proof accelerator, not an alternate source of truth. It may lag
conservatively, but it must not admit a version that exact outcome or abort validation would
reject.

### 8.3 collector-safe boundary and reclamation

Each collector round takes one globally ordered timestamp cut and performs one proof scan over:

- all writer begin-registration states
- all exact transaction outcomes
- all live reader registrations

The collector starts only after recovery has published its final oracle and safe boundary. It must
not scan recovery-created state or publish a candidate derived from the pre-recovery oracle.

From that single scan it derives:

- the next global safe-boundary candidate
- maintenance hints for active presence, minimum WAL position, and minimum WAL file id
- abort-boundary refresh candidates

Group-local resolution backlog is drained separately under the writer group's serialization
boundary. The collector samples a fresh prefix-publication timestamp only after it owns that
boundary, so a terminal outcome published after the proof-scan cut cannot be exposed to a snapshot
at that older cut.

The global safe boundary may advance only after the collector has covered every reader that was
live at the cut. Committed outcomes may be pruned only after the new boundary is published.

Compaction and GC must stay behind both visibility safety and abort-clean durability:

- the reclamation boundary is the older of the visibility-safe boundary and the oldest pending
  abort-clean boundary
- WAL checkpoint and recycle retention use collector-published maintenance hints instead of
  foreground scans

### 8.4 history, traversal, and write conflicts

Old versions are stored in history regions with the following contract:

- a key owns an explicit history region descriptor
- the region is logically contiguous for that key
- the region may span multiple linked history pages
- traversal must remain inside the key's declared region window
- there is no global version ordering across different keys sharing the same history page

Point lookup and forward or reverse iterators must keep enumerating candidates for one raw key
until either:

- one version satisfies the fixed snapshot predicate, or
- the key is proven absent for that snapshot

Overlapping same-key writers enforce first-writer-wins by re-checking the latest version for that
key on every retry path before publishing a new write.

Retiring a history page and reclaiming blob payloads are separate decisions.
History-page reclamation never implies that all referenced blob payloads are collectible.

## 9. Durable Boundary: Bucket Frontier

The durable correctness boundary is not a single global WAL position.
It is a per-bucket, per-writer-group frontier.

This is necessary because one durable page or materialized record may absorb updates from multiple
writer groups, while any individual page-local header can describe only a narrower write history.

Design rules:

- the durable boundary is tracked per bucket and per writer group
- that frontier is persisted atomically with durable map/stat publication
- recovery uses the bucket frontier as the correctness gate for deciding whether a WAL record is
  already durable

WAL checkpoint positions remain useful, but only as scan-start and retention hints.
They are not the source of truth for durable visibility.

## 10. Flush Publish Protocol

Checkpoint publish follows the fundamental rule:

- data first
- metadata last

Metadata must never point to payload files that are not durably written.

### 10.1 data/blob file layout

Each data/blob file is a self-describing persisted artifact with four logical regions:

1. payload frames
2. interval table
3. relocation table
4. footer

The footer is the stable discovery anchor for the file.

Namespace operations around these files, such as creation, rename, deletion, and directory sync,
are part of the same correctness surface and therefore remain explicit in the design.

### 10.2 record-level payload contract

Each relocation entry describes how to interpret one persisted record:

- file offset
- logical raw length
- stored compressed length
- checksum

The interpretation rule is:

- stored compressed length is zero
  - the payload bytes are stored raw
- stored compressed length is nonzero
  - the payload bytes are stored in compressed form and decode back to the raw length

The WAL format is independent from this rule.
Compression applies only to persisted data/blob files.

### 10.3 publish sequence

For each newly built data/blob file:

1. record an orphan marker in metadata before the file becomes durable
2. build and write the file contents
3. durably sync the file
4. publish metadata that makes the file reachable and clears the orphan marker in the same atomic
   metadata transaction
5. only after metadata commit may the runtime treat the file as part of the durable address space

This ordering guarantees that crash windows are closed in the safe direction:

- crash before metadata commit
  - payload file may exist, but metadata does not reference it
- crash after metadata commit
  - payload file is already durable

Directory durability barriers remain explicit.
When namespace persistence matters, directory sync is treated as a first-class part of the publish
protocol rather than an implementation detail hidden behind the filesystem boundary.

### 10.4 old-file stat updates

Checkpoint publish may make previously live file entries newly obsolete.
Those stat updates must observe concurrent background retirement correctly:

- publish works against a stable snapshot of retire state
- it must not recreate file-stat metadata that GC has already retired
- retire state is cleared only after the enclosing metadata commit closes

## 11. Compression Model

Compression is a bucket policy for persisted data/blob files.

Stable rules:

- compression is optional and bucket-local
- compression decisions are made record by record
- records are compressed only when the stored bytes become meaningfully smaller than the raw image
- the WAL remains byte-identical regardless of bucket compression policy
- one bucket directory may contain a mix of raw and compressed data/blob records at the same time

This mixed state is always valid because each relocation entry is self-describing.

## 12. Foreground Admission And Backpressure

Backpressure is enforced before entering tree mutation paths.

Design rules:

- admission is bucket-local and opt-in per bucket; buckets with backpressure disabled bypass
  the wait entirely
- a foreground write reserves dirty-memory budget before it mutates durable structures
- the admission limit is not a fixed threshold; it is derived from an exponentially weighted
  moving average of observed checkpoint progress, with an asymmetric alpha that reacts faster to
  deteriorating throughput than to recovering throughput
- on top of the average, a burst quota allows short spikes above the smoothed limit without
  immediately stalling writers; when abort-clean or other checkpoint progress is detected,
  an additional progress extra burst is granted at a multiple of the baseline burst quota to
  avoid thundering-herd wake behavior after a backpressure release
- writers are not woken individually on every progress event; instead a collective wake threshold
  ensures that the condition is broadcast only when enough pressure has been released to make
  progress meaningful for a batch of waiters
- checkpoint progress is the primary pressure-release signal

This makes backpressure a correctness-preserving flow-control mechanism rather than a late
best-effort throttle. The EWMA model adapts the limit to observed system throughput rather than
relying on a static capacity estimate.

## 13. Transactions And WAL

The WAL is redo-only.
Mace does not rely on physical undo or CLR records.

Stable WAL semantics:

- insertion
  - carries the new value image
- update
  - carries the new value image
- deletion
  - records the tombstone operation only

Transaction design:

- a writer transaction is assigned to a writer group
- begin is logged first
- the first mutation creates the transactional WAL update chain
- commit publishes commit order after WAL durability
- abort records abort outcome and, if necessary, schedules abort-clean using the WAL chain
- a modified abort publishes its pending abort-clean WAL retention before releasing the group
  serialization boundary, so the active-transaction retention and pending-task retention always
  overlap
- transaction length is bounded; a single transaction may not span more than a configured
  maximum number of checkpoint units, preventing runaway write amplification from unusually
  long-lived writers

Conflict checking on the foreground path is metadata-based.
It does not require loading old value images merely to decide whether a write may proceed.

Aborted versions may remain physically present for some time.
Correctness depends on visibility rules hiding them until abort-clean eventually rewrites them away.

### 13.1 WAL route: physical stream vs logical group

The runtime selects one active WAL route from the `sync_on_write` engine option, immutable for
the lifetime of an open instance:

- relaxed route: every logical group writes its own physical stream. A relaxed commit flushes
  its records to the file/page cache and publishes the fact without any fsync, generation
  coordination, or wait.
- durable route: all logical groups funnel through one shared physical stream. The shared
  stream uses a dedicated namespace separate from every per-group namespace, identified by a
  reserved stream identifier above the per-group range so the two namespaces can never collide.
  Each record still carries the logical group id inside the record itself; the physical stream
  id only decides the file namespace.

Two concepts must never be conflated:

- physical stream id — which WAL file namespace a record lives in; recovery and abort-clean
  reopen files by this id
- logical group id — which writer group owns the record's frontier, fact, and visibility; it is
  read from the record, never inferred from the file name

WAL record format, manifest schema, and encoded record sizes are unchanged by the route split.

### 13.2 Durable generation sync

In the durable route, the shared stream coordinates a caller-led sync generation:

- a commit, a modified abort, or a barrier appends its terminal record, flushes the ring,
  and registers its target position under the shared logging mutex; an unmodified abort
  (a transaction with no update chain) stays record-only: no generation, no sync, no wait
- the first register whose target is not yet covered by the durable stream position becomes
  the generation leader; registers arriving before the leader seals join the same generation
  as followers
- a register whose target is already covered by the durable stream position short-circuits to
  a completed ticket: no generation, no leader, no wait, no sync counter
- the leader holds a bounded merge window between registering and sealing without holding the
  shared logging mutex, so writers already advancing toward the stream can register as followers;
  the window exits after a short quiet period when no follower joins
  (solo leaders pay only that quiet period) and otherwise runs up to its hard cap so the whole
  burst coalesces into one generation. the cap is a configurable duration (a default in the low
  hundreds of microseconds; zero disables the window and relies on natural lock contention only)
- the leader re-acquires the shared logging mutex, seals the flushed position as the cut, syncs
  every writer detached by rotation plus the current writer plus the log directory, and advances
  the durable stream position to the cut only on success; only then does it publish the
  per-generation completion and wake the waiters
- every successful ticket therefore observes that the durable stream position covers its
  target; a failed generation broadcasts the same error to all participants and no fact is
  published
- completion results are isolated per generation (reference-counted completion objects); a
  later generation never overwrites an earlier result

Only generation leaders perform physical syncs in the durable route. Ring advancement, ring
overflow, auto-stable flush, large records, and the checkpoint hint append are flush-only and
never advance the durable stream position. A checkpoint hint record in the shared stream
carries the minimum over all per-logical-group checkpoint floors and is a conservative scan
hint, not a redo gate.

**Checkpoint counters stay per logical group.** The physical logger is shared, but
`max_ckpt_per_txn` is a per-logical-group age limit: each writer group reads only its own
counter. Begin, Update, Commit, and Abort append activity is tracked independently for every
logical group. A publish round appends at most one shared checkpoint record, but advances only
the counters of groups with activity since their previous publish; a group is then marked clean.
This preserves the old per-group logger rhythm: if that group's old logger would have emitted a
checkpoint, its counter advances once, while records and checkpoint activity belonging only to
another group cannot age it. A disabled or no-activity publish ages no group. Relaxed mode keeps
one logger and one counter per group unchanged.

### 13.3 Sync barrier and shutdown

A force barrier (the explicit sync entry point, the checkpoint publish barrier, or shutdown)
registers its own target, releases the shared logging mutex, and waits for its own generation;
a barrier never holds the shared logging mutex while waiting for another leader. Shutdown order
is: stop the gc thread -> wait admitted transactions to reach terminal publication
-> synchronously drain all pending abort-clean tasks (recovery-style; the drain
must complete before shutdown continues) -> final checkpoint of every loaded
bucket (sealing and
flushing all dirty pages, with a full fsync of the files written by that final
checkpoint even when `sync_on_write = false`, so a graceful close leaves the
final dirty set on disk; files synced by earlier run-time relaxed checkpoints
keep their fdatasync state) -> force barrier on the active route -> stop the
collector.

There is no runtime admission gate: shutdown runs only when the last database or bucket handle
is dropped, and a transaction or view requires a live bucket borrow, so no thread can be on an
admission path at that point. The in-flight drain (an acquire/release handshake) still precedes
the force barrier. If a public close/quit API that can run while buckets are alive, or a
drop-order change that releases the store before the buckets, is ever introduced, an admission
gate must be restored first.

Because the exit path flushes everything, a graceful close is WAL-independent: deleting every
WAL file of both namespaces — the legacy per-group `wal_<group>_<seq>` and the durable shared
`group_wal_<seq>` stream — and reopening must expose exactly the committed model. The exit
verifier deletes both namespaces and asserts zero files remain before the reopen, so recovery
cannot lean on any WAL record and the committed-only result is proof the final checkpoint
published everything.

### 13.4 WAL recycling protocol

WAL recycling is a two-phase commit protocol executed within a durable metadata transaction.

The phases are:

1. intent — record a durable recycle intent naming the WAL files to be removed; this intent
   survives a crash and will be re-executed on recovery before normal operation resumes
2. deletion — physically remove the named WAL files from the filesystem
3. done — mark the intent complete and advance the durable WAL recycle frontier in the same
   metadata commit that clears the intent

The durable recycle frontier is monotonic. A runtime logger may retain a pre-switch in-memory
oldest id, but its later recycle intent is clamped to the current durable boundary; an empty
clamped range creates no intent and cannot regress the next boot's scan start.
The intent transition reads that boundary and writes the normalized intent in one metadata
transaction. An active intent owns its physical stream until only the matching done transition
can advance the boundary, so a shutdown fallback collector cannot overwrite a concurrent GC
round's newer state.

A crash between phases 1 and 3 is safe: recovery finds the intent, re-executes the deletion,
then clears it. A crash after phase 3 leaves no intent to re-execute. The durable recycle
frontier is therefore always a reliable lower bound on which WAL files have been permanently
removed.

The recycle intent names a physical stream and a file range. In the durable route only the
shared stream is writable, so GC issues exactly one intent for it per round; per-group streams
do not exist in durable steady state (a route switch wipes the previous era), and the shared
stream's recycle state lives in its own slot, separate from every per-group slot so the two
never collide. The shared stream's safe boundary is the minimum over every logical group's
active-transaction pin, every pending abort-clean pin (matched by physical stream id), and the
durable checkpoint floor; the checkpoint floor is tracked per logical group because a single
scalar hint may come from a historical relaxed group logger.

**Per-logical-group floor slots are inactive until first use.** Each slot is a
two-state cell: `inactive` (Position::MAX, the group has no record in the retained stream)
or `active` (a finite position, the group has records that the cut must respect). There is no
persisted bitmap; the state is maintained in memory:

- recovery analyze collects the logical groups that hold at least one valid update record in
  the retained shared stream; after analyze, redo, and abort-clean finish — and before runtime
  checkpoint/GC starts — the slots are published: groups with retained updates keep their
  manifest checkpoint lower bound (Position::MIN when the manifest has no frontier evidence
  for them), groups without retained updates stay inactive at MAX;
- the first update a logical group appends to the stream activates its slot in the same
  logging-mutex critical section as the append, setting it to the transaction's begin LSN
  (the prev_lsn of the first update), so the cut can never pass a record whose checkpoint
  evidence does not exist yet; the active-txn pin covers the begin→first-update window;
- a checkpoint publish advances an active slot by max() and can never activate an inactive
  slot (a MAX position is rejected, and MAX.max(pos) stays MAX);
- an era wipe (route switch or layout migration) deactivates every slot afterwards, so stale
  old-era floors never pin the fresh stream; the new era reactivates each group on its first
  update.

Because inactive slots never participate in the min, a logical group that never wrote an Update
cannot pin the shared stream at file 0, while a group's begin-to-first-checkpoint window stays
fully protected. If every slot is inactive and there is no active transaction or pending
abort-clean pin, the stream may still contain Begin/Commit/Abort records from unmodified
transactions. GC uses the current append file id as the finite cut: complete older files are
recycled through intent/delete/done, and the current file remains as the recovery anchor.

## 14. Recovery

Recovery is responsible for reconstructing a correct runtime state before the database becomes
usable.

The startup flow is:

1. load durable metadata
2. clean orphan-file markers and remove stray payload files
3. finish any durable pending WAL recycle intent
4. bootstrap WAL scanning from conservative retained boundaries
5. handle a route switch when the requested route differs from the persisted last-completed
   route (see below); no transaction is admitted before recovery completes
6. analyze WAL to rebuild transaction outcomes and pending abort-clean work
7. redo committed records that are not yet durable under the bucket frontier
8. finish reconstructed abort-clean work before open returns

Recovery scans every physical stream — all per-group streams plus the shared stream —
regardless of the current `sync_on_write` value, so both shared and per-group history are
always discovered. Every scanned record carries a physical stream id (from the file it was
read from) and a logical group id (from the record itself); redo and abort-clean open files
by the physical id and resolve frontier/fact ownership by the logical id.

**Route switches, layout migration, and the file-id epoch**: an epoch runs when the requested
route differs from the persisted last-completed route (a route switch), or when a durable open
finds legacy per-group `wal_<group>_<seq>` files (a same-route physical layout migration —
the durable steady state never writes the per-group namespace, so such files are always
leftover history from an older durable format or an interrupted migration). A same-route
reopen with a matched layout skips the epoch entirely and continues each active stream in its
existing latest file. On a rebuild, the high water `H` is the maximum file id over all WAL
files (per-group and shared), persisted bucket frontiers, and recycle states; the new era
starts at `H + 1` (or 0 for a database with no history). Before the delete, the old WAL must
be fully recyclable: a graceful exit already checkpointed everything (frontier covers every
record, no pins), and a crash exit forces a full-fsync checkpoint of the recovery-redone tail
first, with every eviction and abort-clean flush during recovery also full-fsync. The rebuild
then deletes every old WAL file via the two-phase recycle protocol — shared stream first,
legacy per-group streams last, so the last surviving legacy file keeps re-triggering the
migration until the wipe is complete — and starts the new era at `H + 1` — no empty prefix
files are ever created. Deletion enumerates actual matching directory entries in the intent
range instead of probing every numeric id up to `H`; every stream's durable done state still
advances to the new era start. A missing file inside a retained stream's range, or an `H + 1`
overflow, is reported as corruption. The options write-back happens only after a route switch
completes, so the persisted route always means "last completed route"; a crash mid-switch
leaves the old route persisted and the next open re-detects the switch idempotently. A
same-route migration needs no marker: while any legacy file exists the next open re-runs the
migration; a crash mid-wipe leaves pending recycle intents that the next open completes
first (finish_pending_wal_recycle), then re-runs the migration while legacy files remain.
Once the legacy files are gone the frontier and recycle states alone are enough to start the
shared stream safely at a higher file id.

A shared stream with no files (for example a switch wipe that removed them, or a database that
never ran the durable route) starts its era strictly above the highest persisted bucket
frontier file id, so fresh records can never land at positions the redo gate would treat as
already covered.

WAL tail handling is per physical stream: records are validated in order (header, length,
payload, checksum); at the first malformed or incomplete record the scan stops at that
record's start, truncates there when the truncation policy is enabled, and does not scan
later files of that stream. If recovery truncates an active file after its runtime writer was
opened, it must rebase every runtime position for that writer to the truncated physical EOF
before any checkpoint, cleanup, or foreground append can run. There is no persisted fsync
watermark; the runtime durable stream position is not a truncation boundary. A newly created
active stream's first file may be
empty — it is not a malformed tail.

Recovery-created tree roots carry `Position::MIN`. The recovery marker remains set until
analyze, redo, abort-clean drain, forced checkpoint/eviction, route wipe, and checkpoint-floor
publication have all completed; only then may runtime tree roots use the append position.

In relaxed mode, WAL recycle may read the live active pin under the group logging mutex and read
the pending abort-clean pin after releasing it. A completed modified abort has already published
the task pin in that same mutex critical section; an abort still in progress retains its active
begin pin, whose file id is no later than the task pin it will transfer to. Either observation is
therefore a conservative recycle boundary.

Every WAL Update reader must first derive checked record bounds, then read the complete record and
validate its checksum and payload layout before using its key, value, transaction, or chain-link
fields. A malformed Update terminates the affected scan or returns corruption; it must not drive an
allocation or read outside the engine's checked record maximum, replay, or abort-clean traversal.

Startup namespace repair, orphan cleanup, WAL recycle, and other path-level recovery steps are part
of the same filesystem boundary described above.
Recovery must distinguish "not found" from other IO failures instead of treating every failed
existence check as absence.

Three different boundaries must remain distinct:

- bucket durable frontier
  - correctness gate for "already durable or not"
- WAL checkpoint position
  - scan-start optimization
- WAL recycle frontier
  - durable lower bound for already-removed old WAL files

Recovery correctness depends on keeping those three concepts separate.

## 15. Abort-Clean

Abort-clean removes the durable effects of aborted or incomplete transactions by page rewrite and
compaction, not by inverse-value undo.

Design rules:

- abort-clean follows the transactional WAL chain backward
- abort-clean validates every chain position against the retained WAL range and a checked finite
  step budget; malformed links surface as corruption and retain the task
- in a shared stream, chain traversal opens files by the task's physical wal id and compares
  every link strictly backward in the stream's `Position` total order (`file_id` + offset);
  interleaved records from other logical groups do not change the range or the direction
  judgment, and the logical group id only decides retained-abort ownership
- page-touching cleanup is not considered retired until its durability barrier is crossed
- recovery must drain reconstructed abort-clean before normal runtime GC begins
- recovery must finish reconstructed abort-clean before post-start checkpoint recording and WAL
  recycle are trusted again
- GC must not reload a bucket that the user explicitly unloaded
- if a bucket has an abort-clean task that has not been removed, including `WaitingQuiesce`,
  unload is blocked instead

An abort-clean task uses two states around a copy-on-write rewrite:

- Pending — the abort fact and WAL pin remain retained while cleanup is incomplete. Runtime GC may
  rewrite the affected pages in this state, then must complete a fresh checkpoint for every touched
  bucket. If the rewrite or checkpoint fails, the task remains Pending and retains both protections.
- WaitingQuiesce — every touched bucket's fresh checkpoint has completed, so the rewrite is durable.
  The task now waits for the EBR readers that existed before the rewrite to drain. Once the EBR
  callback marks it quiesced, the task is removed and the retained abort fact is retired. Recovery
  has no runtime readers, so it may remove a successfully checkpointed task during its closed
  startup drain.

Aborted versions are never visible to snapshots; quiescence protects page replacement and
retirement, not visibility of an aborted value.

Abort-clean therefore interacts with bucket lifecycle, recovery, and GC as one shared correctness
surface.

## 16. GC, Rewrite, And Metadata Compaction

Background maintenance has three distinct responsibilities:

- reclaim fully obsolete payload files
- rewrite high-garbage data/blob files into denser files
- physically delete obsolete files and fully deleted buckets

Loaded-page compaction still exists, but it is produced by foreground tree replace, split, merge,
and consolidate publishes and closed by the normal checkpoint durability boundary.
There is no separate background scavenge pass and no bucket-wide manual vacuum interface.

### 16.1 victim selection

Victim ranking is bucket-local in execution, even if candidate discovery is global.

Stable rules:

- fully obsolete files are reclaimed immediately
- partial-rewrite work is gated by per-bucket garbage ratios
- a rewrite batch normally requires at least two files and enough live bytes to justify a rewrite
- eager GC may bypass the usual size threshold but not correctness checks

Candidate files are scored to rank rewrite priority. The score rewards files that are both
space-dense with garbage and relatively stale. Density is weighted more heavily than age: a file
that is almost entirely garbage is an urgent candidate regardless of how recently it was written,
while a file that is mostly live data is a poor target even if it is old. Age serves as a
tiebreaker between files at similar density levels, and live element count damps the score for
files that hold many small live entries — rewriting them produces high write amplification for
little reclaim benefit.

### 16.2 rewrite safety

Rewrite is a publish protocol, not an in-place edit.

Its crash-safety model is:

1. build a new file under orphan protection
2. publish new interval/stat metadata and delete intent atomically
3. retire old files through the normal obsolete-file pipeline

Old files remain valid until metadata commit makes the new file authoritative.

Before committing to a full rewrite pass, the rewriter performs a pre-flight live-ratio check
against the current state of the candidate files. The garbage ratio can change substantially
between when a victim was selected and when the rewrite actually starts — other concurrent
operations may have already rendered the file mostly clean or fully obsolete. If the pre-flight
check finds that the file no longer meets the rewrite threshold, the pass is abandoned rather
than producing unnecessary write amplification.

Only one rewrite may be active per bucket at a time. This constraint prevents multiple concurrent
passes from competing over the same candidate set, inflating write amplification, and producing
redundant output files that all need to be reconciled at publish time.

### 16.3 manual metadata compaction

Manual maintenance currently exposes one best-effort interface:

- metadata-store compaction

It does not load bucket runtime state and is not part of the foreground consistency protocol.

## 17. Storage Format Versioning And Upgrade

This section defines the stable upgrade model for persisted formats.

### 17.1 compatibility boundary

Mace has three persistent format families:

- data/blob payload files
- metadata organization in the metadata store
- WAL

Their compatibility policies are intentionally different.

### 17.2 platform boundary

Mace supports only 64-bit machines.

Under that boundary, durable use of machine-word-sized unsigned integers is acceptable.
The format contract is "stable on supported 64-bit Mace platforms", not "portable across arbitrary
machine architectures".

### 17.3 data/blob format policy

The current stable data/blob format version is 1.

Data/blob files are self-describing at file scope.
Their stable discovery anchor is the fixed footer at EOF, which records:

- file format version
- reserved padding for future use
- relocation-table cardinality and checksum
- interval-table cardinality and checksum

The format does not require a separate magic value.
Structural validation comes from the fixed footer position, version field, table lengths, and
checksums.

There is no per-frame or per-record version byte inside one file.
All payloads in a file are interpreted under that file's single version.

### 17.4 reader and writer contract

The long-term contract is:

- the writer emits only the current data/blob version
- the reader supports every still-supported historical data/blob version
- old runtimes are not required to read newer files
- new runtimes must be able to open mixed-version directories produced by supported upgrade chains

This is a read-old, write-current model.

### 17.5 why read support must accumulate

Mace does not assume that all old payload files are rewritten during one release upgrade.

Therefore a later runtime may encounter:

- files written several releases ago
- files written by an intermediate release
- files written by the current release

All of them may coexist until ordinary checkpoint or GC rewrite converges them.

For that reason, data/blob compatibility is not "adjacent version only".
Reader support accumulates across all historical versions that remain within the supported online
upgrade window.

### 17.6 rewrite as online upgrade

Checkpoint publish and GC rewrite are the normal online format-convergence paths.

Design rules:

- old files may be read in place
- any rewritten output is emitted only in the current version
- if source and target version differ, payload bytes must be decoded and re-encoded
- raw byte carry-over across a version boundary is forbidden

As a result, ordinary maintenance gradually upgrades the directory to the newest payload-file
format without a dedicated online migration step.

### 17.7 future version evolution

Future data/blob versions should preserve one simple dispatch model:

- file-level version switch
- footer-based discovery
- versioned payload decode
- write-current only

If later versions need more footer meaning, they should prefer reusing or reinterpreting reserved
footer space before inventing a new discovery protocol.

### 17.8 version retirement

Historical data/blob reader support may be retired only at an explicit migration boundary.

That boundary must be deliberate and documented.
It is not the default behavior of ordinary releases.

If a historical data/blob version is retired, the release that retires it must require an offline
migration or an equivalent explicit operational step.

### 17.9 metadata format policy

Metadata follows a stricter policy than data/blob payload files.

If metadata organization changes incompatibly:

- bump the metadata version
- reject the old metadata at runtime
- require an offline metadata migration step

This tradeoff is acceptable because metadata volume is small and the underlying metadata-store file
format is considered stable.

### 17.10 WAL format policy

The WAL remains byte-identical under the current design.

There is no WAL format branching in this versioning model.
If WAL format evolution is ever needed, it should be designed independently rather than inheriting
the data/blob compatibility policy by accident.

## 18. Observability

Mace exposes a fixed-cardinality observability surface:

- counters
- gauges
- histograms
- events

The design goal is to make always-on instrumentation cheap, bounded, and predictable.

High-frequency latency metrics may be sampled.
Low-frequency maintenance and recovery events are expected to be reported directly.

## References

- The Bw-Tree: A B-tree for New Hardware Platforms
- Bf-Tree: A Modern Read-Write-Optimized Concurrent
- LLAMA: A Cache/Storage Subsystem for Modern Hardware
- Efficiently Reclaiming Space in a Log Structured Store
- LeanStore: In-Memory Data Management Beyond Main Memory
- Scalable and Robust Snapshot Isolation for High-Performance Storage Engines
- Rethinking Logging, Checkpoints, and Recovery for High-Performance Storage engines
- Larger-Than-Memory Range Index
- Optimistic Lock Coupling: A Scalable and Efficient General-Purpose Synchronization Method
...
