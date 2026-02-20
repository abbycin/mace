# Scripts Guide

## Scope and Responsibilities

Production-validation-related scripts currently under `scripts/`:

- `prod_test.sh`: correctness and crash-recovery test orchestration (`fast` / `stress` / `chaos` / `all`)
- `perf_gate.sh`: performance snapshot and regression gate (`snapshot` / `compare`)
- `prod_soak.sh`: long-running soak orchestration (stress/chaos loop + periodic performance sampling)
- `perf_thresholds.env`: default template for performance gate thresholds and quick benchmark knobs

---

## 1) `prod_test.sh`

### Purpose

Unified layered execution for production-style integration tests:

- `fast`: quick regression (default)
- `stress`: long-run/high-pressure cases (`#[ignore]`)
- `chaos`: failpoint crash/fault-injection cases (`#[ignore]`)
- `all`: `fast + stress + chaos`

### Basic Usage

```bash
./scripts/prod_test.sh [mode] [threads]
```

- `mode`: `fast|stress|chaos|all`, default `fast`
- `threads`: passed to `cargo test -- --test-threads=...`, default `8`

Examples:

```bash
./scripts/prod_test.sh fast 8
./scripts/prod_test.sh all 8
```

### Key Environment Variables

- `FEATURES`: default `failpoints`
- `FAST_TIMEOUT` / `STRESS_TIMEOUT` / `CHAOS_TIMEOUT`
- `CHAOS_RETRY`: retry count for chaos cases
- `MACE_PROD_BUCKET_STRESS_ROUNDS`
- `MACE_PROD_BUCKET_CHURN_ROUNDS`
- `MACE_PROD_BUCKET_CHURN_WORKERS`
- `MACE_PROD_EVICTOR_STRESS_ROUNDS`

### Result Semantics

- Exit code `0`: full pass
- Non-zero: failed items are listed in `failures` at the end

> Note: in chaos mode, child-process failure logs can be expected; rely on final script exit code and `failures` summary as ground truth.

---

## 2) `perf_gate.sh`

### Purpose

Provides baseline sampling and regression gating:

- `snapshot`: capture performance snapshot for current branch only
- `compare`: compare current branch against a baseline (default `origin/master`) and enforce threshold rules

### Basic Usage

```bash
./scripts/perf_gate.sh snapshot
./scripts/perf_gate.sh compare
```

### Key Environment Variables

- `MACE_PERF_THRESHOLD_FILE`: threshold file, default `scripts/perf_thresholds.env`
- `MACE_PERF_BASE_REF`: baseline ref, default `origin/master`
- `MACE_PERF_BENCH_ARGS`: arguments forwarded to Criterion
- `MACE_PERF_COMPARE_FILTERS`: case filters for compare mode (comma-separated)
- `MACE_PERF_REGRESS_PCT`: per-case regression threshold (%)
- `MACE_PERF_MAX_REGRESS_CASES`: max allowed regressed case count
- `MACE_PERF_MIN_COMPARE_CASES`: minimum valid comparable case count
- `MACE_PERF_MIN_BASE_NS`: ignores ultra-small baseline noise

### Outputs and Metric Definitions

Default output directory: `target/perf_gate`

Common files:

- `base_summary.json` / `head_summary.json`
- `head_snapshot.json`
- `base.bench.log` / `head.bench.log` / `snapshot.bench.log`

Core metric fields:

- `median_ns`: Criterion median point estimate in nanoseconds (lower is better)
- `mean_ns`: Criterion mean point estimate in nanoseconds (lower is better)
- `delta_pct` (from compare output):
  - formula: `(head - base) / base * 100%`
  - positive means slower, negative means faster

Gate rule:

- compare fails when count of cases with `delta_pct > MACE_PERF_REGRESS_PCT` is greater than `MACE_PERF_MAX_REGRESS_CASES`.

---

## 3) `prod_soak.sh`

### Purpose

Long-running soak validation for:

- mixed-workload stability over time
- crash/recovery behavior under periodic failpoint/chaos disturbance
- long-horizon performance trend via periodic snapshots

### Basic Usage

```bash
./scripts/prod_soak.sh
```

### Default Flow

For each cycle:

1. run `prod_test.sh stress`
2. run `prod_test.sh chaos` at configured cadence
3. run `perf_gate.sh snapshot` at configured cadence
4. sleep between cycles

After total duration is reached, run `prod_test.sh fast` as final post-check.

### Key Environment Variables

- `SOAK_DURATION_SECS`: total duration, default `21600` (6h)
- `SOAK_THREADS`: test thread count
- `SOAK_STRESS_TIMEOUT` / `SOAK_CHAOS_TIMEOUT` / `SOAK_FAST_TIMEOUT`
- `SOAK_CHAOS_EVERY`: run chaos every N cycles (`0` disables)
- `SOAK_PERF_EVERY`: run perf snapshot every N cycles (`0` disables)
- `SOAK_CYCLE_PAUSE_SECS`: sleep interval between cycles
- `SOAK_METRICS_DIR`: metrics directory, default `target/prod_soak`

### Outputs and Metric Definitions

- `target/prod_soak/perf_cycles.csv`
- `target/prod_soak/perf_cycle_<n>.json` (generated from snapshots)
- `target/perf_gate/*` (perf_gate artifacts)

`perf_cycles.csv` columns:

- `cycle`: cycle index
- `elapsed_secs`: elapsed seconds since soak start
- `cases`: valid benchmark case count in that snapshot
- `median_ns_p50`: p50 of per-case `median_ns` in that snapshot
- `median_ns_p95`: p95 of per-case `median_ns` in that snapshot

> `median_ns_p50/p95` are useful for jitter/regression trend tracking; compare against historical runs on equivalent hardware.

---

## 4) `perf_thresholds.env`

### Purpose

Central place for default perf gate and quick benchmark parameters, reused locally and in CI.

### Maintenance Recommendations

- First run 3–5 rounds on target production-equivalent hardware to establish a stable baseline
- Then tighten:
  - `MACE_PERF_REGRESS_PCT`
  - `MACE_PERF_MAX_REGRESS_CASES`
  - `MACE_PERF_COMPARE_FILTERS`
- If CI noise is high, start with looser thresholds and narrower case filters, then tighten gradually

---

## CI Mapping

- `.github/workflows/ci.yml`
  - `test-prod-all`: `./scripts/prod_test.sh all 8`
  - `perf-regression`: `./scripts/perf_gate.sh compare`
- `.github/workflows/prod-soak.yml`
  - `Prod Soak Nightly`: `./scripts/prod_soak.sh`
