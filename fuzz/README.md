# Fuzzing

This directory contains Mace's `cargo-fuzz` targets.

## What These Targets Try To Break

The main goal is not "what if an external caller feeds malformed bytes into a decoder". Mace
mostly reads data that Mace itself previously wrote, so the more important question is:

- can Mace's own complex lifecycle produce state that later becomes unreadable
- can lagging views lose versions they should still see
- can checkpoint, GC, publish, reopen, or bucket churn leave metadata and runtime state out of sync

In short, the focus is whether internal state transitions can manufacture bad state by themselves.

## Targets

- `txn_checkpoint_reopen`
  - mixes transactional writes, checkpoints, and reopen
  - checks that committed state stays visible across lifecycle transitions

- `publish_gc_reopen`
  - keeps lagging snapshot views alive while publish, GC, and reopen continue
  - checks that old views never lose versions they should still see

- `bucket_lifecycle`
  - exercises bucket create/get/drop/delete/reopen churn
  - checks that bucket lifecycle transitions do not leave broken metadata or stale runtime state

## Setup

```bash
cargo install cargo-fuzz
rustup toolchain install nightly
```

## Check Targets

```bash
cargo +nightly fuzz check txn_checkpoint_reopen
cargo +nightly fuzz check publish_gc_reopen
cargo +nightly fuzz check bucket_lifecycle
```

## Run Fuzzing

```bash
cargo +nightly fuzz run txn_checkpoint_reopen
cargo +nightly fuzz run publish_gc_reopen
cargo +nightly fuzz run bucket_lifecycle
```

For bounded runs:

```bash
cargo +nightly fuzz run publish_gc_reopen -- -max_total_time=120
```

In restricted environments, LeakSanitizer may need to be disabled:

```bash
LSAN_OPTIONS=detect_leaks=0 cargo +nightly fuzz run publish_gc_reopen -- -max_total_time=120
```

## Reproduce An Artifact

Crash artifacts are written under `fuzz/artifacts/<target>/`.

Example:

```bash
LSAN_OPTIONS=detect_leaks=0 cargo +nightly fuzz run publish_gc_reopen \
  fuzz/artifacts/publish_gc_reopen/<artifact>
```
