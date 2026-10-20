# Constraint Ledger

This file is the top layer of the Mace constraint ledger: what the ledger is for, what may become an entry, what an entry looks like. Nothing below repeats it, nothing above replaces it.

## The five concerns

Every entry names the concern it protects, and they are the ledger's purpose and its only admission criterion. One break can hurt a second concern - a stat that is decoded wrong both miscounts space and can unlink a live file - and the entry's `break_consequence` says so when it does. The entry still names one concern, because that is the one whose violation would otherwise be silent. An entry goes under `concurrency` when its subject is a data race, a deadlock or a livelock; a race whose subject is the ordering rule it breaks belongs under the consequence it produces.

### data-loss

Data the database has acknowledged as committed is never lost: once acknowledged it is durable, and it survives process death, machine death, and every later maintenance pass. From the outside, a violation is a value that read back successfully and is later gone with nothing that removed it.

### crash-safety

A crash at any point leaves a database that reopens to a consistent, recoverable state: recovery resumes the interrupted work or discards it, never half-applies it, never resurrects a deleted value. From the outside, a violation is a database that will not reopen, reopens to an inconsistent tree, or reopens with a stale value reappearing.

### space-leak

Bytes no longer reachable are eventually reclaimed, and no file is deleted while something still references it. Reachability is what makes an address live, its absence is what makes it garbage, and garbage is collected rather than accumulated. From the outside, a violation is a data directory that only grows under steady load, or a reclaimed file that a live structure still points at.

### visibility

A reader never observes a value it must not see, and never loses one it must see: isolation against values it is not entitled to, completeness against values the current state says exist. From the outside, a violation is a read returning an uncommitted value, or a key that is present on one path and missing on another.

### concurrency

Concurrent access is free of the three classical failures:

- **no data race** - every access to mutable state from two threads is ordered by a lock, an atomic with the right ordering, or ownership that does not overlap. In practice this covers unsafe aliasing that outlives its guard, a mutable field read outside the lock that protects it, a node reclaimed and reused while someone still holds it, and the same value freed twice.
- **no deadlock** - no thread waits for a resource another thread can only release after that wait, and no blocking wait is unbounded where the other side needs this side to move.
- **no livelock or starvation** - the system as a whole advances: a retry loop converges, a requeue eventually wins, and no participant can be postponed forever.

From the outside, a violation is memory corruption or a silently wrong value nobody can explain, a hang that no load recovers from, or a component that spins at full CPU while making no progress. This concern exists because these three break the *means* of the other four as well: undefined behaviour cannot be argued about, and a deadlocked collector stops reclaiming space at all. File an entry here when its subject is thread safety itself; when a race merely *produces* a durability or visibility failure and its subject is the ordering rule that failure breaks, file it under the consequence - `data-loss`, `crash-safety`, `space-leak` or `visibility`.

## The three layers

1. `README.md` - the five concerns, in prose, as the ledger's purpose.
2. `registry.yaml` - the concrete invariants, grouped by `protects` so the five concerns are visible in the file. Each entry is one enforcement boundary plus the rules that share it: when several rules are enforced by the same cut, the same transaction or the same publication window, they are one entry, because splitting them would put the same counterexample in two places. The registry stands on its own; it is not a pointer back to another document.
3. `retired.md` - invariants that were removed, with the reason.

Anything that is not a concrete invariant does not live in the registry. Stable architecture, protocol explanation, and API shape belong in `docs/design.md` or in a comment at the code that owns them.

The ledger's floor is the owner's original set of hard constraints: snapshot flush scope, interval non-overlap, relocation completeness, junk-address atomicity with owner-page durability, the checkpoint frontier, and the reachable-junk lifecycle. Each is carried by an entry below, and none of them may be lost in a later restructure.

## Admission rule

An invariant earns an entry only if breaking it produces one of the five consequences above: lost committed data, an unrecoverable reopen, bytes that are never reclaimed, a wrong or missing read, or a racing shared state. If you cannot name which of those a violation causes, it is not an entry. API shape, tool behavior, CLI exit codes, test-harness signals, memory budgets, and style rules go to `docs/design.md` or a code comment instead.

## Entry schema

Each entry in `registry.yaml` has exactly these fields, in this order:

```yaml
  - id: emergent.checkpoint_live_set_only
    title: a checkpoint flushes exactly the live dirty set under one atomic epoch cut
    protects: visibility
    status: active
    statement: >
      A checkpoint may flush only the pages reachable from the dirty-root live graph that are not already
      durable, and must not write stale or unreachable pages as if they were live. Starting a checkpoint
      is a single cut that swaps the hot generations, publishes the sealed generations and rotates the
      writer root and inflight slots together; no writer straddles the cut, and a live page carried into
      the next round moves its bytes and its retired lineage together. Never: a page above the cut that is
      still live disappearing from both the sealed and the hot generation before it is durable, or a
      reader observing a page that is momentarily in neither generation.
    break_consequence: >
      A live reader is refused the page it must see, so a get or an iteration loses a key that exists; the
      same cut is what lets a flush close an old generation and release its bytes, so a broken cut also
      strands dirty pages and their space.
    anchors:
      - src/map/buffer.rs
      - src/map/data.rs
    reproducer: checkpoint_cut_keeps_post_cut_writer_visible
```

- `id` - stable and unique; referenced from commits and reviews.
- `title` - one line, for scanning the file.
- `protects` - one of `data-loss`, `crash-safety`, `space-leak`, `visibility`, `concurrency`.
- `status` - `active` (relied on today), `suspect` (probably still true, but the evidence is weak or an anchor moved), `draft` (observed, not yet strong enough to rely on).
- `statement` - the invariant, tight enough to check against the code.
- `break_consequence` - what the operator sees, in the concern's own terms.
- `anchors` - one to three tracked repo-relative paths. Paths only, no line numbers, so they do not rot on every edit.
- `reproducer` - the name of one test that would fail if the invariant broke, or the literal `none`.

`watch_points`, `witnesses`, `verifiers`, `kind`, `confidence`, and `failure_symptoms` are gone. They made the ledger a test index and a list of code coordinates: they were rewritten on every refactor, so they generated churn and buried the invariants under their own bookkeeping.

## Maintenance rules

The ledger is edited in exactly three cases: an invariant changes, an anchor actually moves, or an entry is retired. A code change that does not change an invariant does not touch the ledger, whatever files it rewrites. Reviewers do not add entries for new features unless the feature introduces a new invariant with one of the five consequences. An entry earns `active` by being relied on, not by being well documented. When an invariant is no longer intended, move it to `retired.md` with the reason instead of deleting it. Folding one entry into another is a retirement: name the survivor and the reason in `retired.md`, so the ledger shrinks without losing the reasoning.

## Checking an entry without a test

Read the statement, then apply the counterexample in `break_consequence` to the current code. If the code still makes that state unreachable, the entry is standing. If it no longer does, the entry is stale: either the code regressed and must be fixed, or the invariant is not the one we want, and the entry must be rewritten or retired. Do not add a test whose only job is to make the ledger look complete: an untested invariant is honest, a test that asserts nothing is not.
