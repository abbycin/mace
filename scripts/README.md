# Scripts Guide

## Scope and Responsibilities

Production-validation-related scripts currently under `scripts/`:

- `prod_test.sh`: correctness and crash-recovery test orchestration (`fast` / `stress` / `chaos` / `all`)

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

## CI Mapping

- `.github/workflows/ci.yml`
  - `test-prod-all`: `./scripts/prod_test.sh all 8`
