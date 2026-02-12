#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

features="${FEATURES:-failpoints}"
mode="${1:-fast}"
test_threads="${2:-8}"

if [[ $# -gt 2 ]]; then
  echo "usage: $0 [mode] [threads]"
  echo "  mode: fast|stress|chaos|all (default: fast)"
  echo "  threads: cargo --test-threads value (default: 8)"
  exit 2
fi

fast_timeout="${FAST_TIMEOUT:-300}"
stress_timeout="${STRESS_TIMEOUT:-600}"
chaos_timeout="${CHAOS_TIMEOUT:-300}"
chaos_retry="${CHAOS_RETRY:-3}"

: "${MACE_PROD_BUCKET_STRESS_ROUNDS:=512}"
: "${MACE_PROD_BUCKET_CHURN_ROUNDS:=128}"
: "${MACE_PROD_BUCKET_CHURN_WORKERS:=2}"
: "${MACE_PROD_EVICTOR_STRESS_ROUNDS:=512}"

failures=()

fast_targets=(
  prod_bucket
  prod_concurrency
  prod_evictor
  prod_gc
  prod_workload
  prod_recovery
  prod_recovery_failpoints
)

stress_cases=(
  "prod_bucket:stress_create_delete"
  "prod_concurrency:stress_bucket_churn"
  "prod_evictor:stress_drop_reload_loop"
  "prod_gc:stress_blob_cycle"
  "prod_recovery:stress_crash_reopen_loop"
  "prod_workload:stress_hotspot"
)

chaos_cases=(
  "prod_recovery:chaos_failpoint_txn_commit_io"
  "prod_recovery:chaos_failpoint_txn_commit_abort"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_data_sync"
  "prod_recovery_failpoints:chaos_failpoint_flush_before_manifest_commit"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_manifest_commit"
  "prod_recovery_failpoints:chaos_failpoint_wal_after_checkpoint_write"
  "prod_recovery_failpoints:chaos_failpoint_manifest_before_multi_commit"
  "prod_recovery_failpoints:chaos_failpoint_txn_commit_after_record_commit"
  "prod_recovery_failpoints:chaos_failpoint_txn_commit_after_wal_sync"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_before_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_after_stage_marker"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_after_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_rewrite_before_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_rewrite_after_stage_marker"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_rewrite_after_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_evictor_before_evict_once"
)

run_cmd() {
  local phase="$1"
  local timeout_secs="$2"
  shift 2
  local code=0

  echo ""
  echo "==> [${phase}] timeout ${timeout_secs}s"
  echo "    $*"

  set +e
  timeout "${timeout_secs}" "$@"
  code=$?
  set -e

  if [[ "$code" -eq 0 ]]; then
    echo "==> [${phase}] ok"
    return 0
  fi

  if [[ "$code" -eq 124 ]]; then
    echo "==> [${phase}] timeout"
  else
    echo "==> [${phase}] failed with exit code ${code}"
  fi
  return "$code"
}

run_fast() {
  local args=(test --features "$features")
  for target in "${fast_targets[@]}"; do
    args+=(--test "$target")
  done
  args+=(-- --nocapture --test-threads="$test_threads")

  if ! run_cmd "fast" "$fast_timeout" cargo "${args[@]}"; then
    failures+=("fast")
  fi
}

run_stress() {
  local entry target test_name
  for entry in "${stress_cases[@]}"; do
    target="${entry%%:*}"
    test_name="${entry##*:}"

    if ! run_cmd "stress:${test_name}" "$stress_timeout" \
      cargo test --features "$features" \
        --test "$target" "$test_name" \
        -- --ignored --exact --nocapture --test-threads="$test_threads"; then
      failures+=("stress:${test_name}")
    fi
  done
}

run_chaos_case() {
  local target="$1"
  local test_name="$2"
  local attempt=1
  local code=0

  while (( attempt <= chaos_retry )); do
    echo ""
    echo "==> [chaos:${test_name}] attempt ${attempt}/${chaos_retry}"

    if run_cmd "chaos:${test_name}" "$chaos_timeout" \
      env RUST_BACKTRACE=1 \
        cargo test --features "$features" \
          --test "$target" "$test_name" \
          -- --ignored --exact --nocapture --test-threads="$test_threads"; then
      return 0
    fi

    code=$?
    attempt=$((attempt + 1))
    if (( attempt <= chaos_retry )); then
      echo "==> [chaos:${test_name}] retry after failure"
      sleep 1
    fi
  done

  failures+=("chaos:${test_name}")
  return "$code"
}

run_chaos() {
  local entry target test_name
  for entry in "${chaos_cases[@]}"; do
    target="${entry%%:*}"
    test_name="${entry##*:}"
    run_chaos_case "$target" "$test_name" || true
  done
}

echo "repo_root=${repo_root}"
echo "mode=${mode} features=${features} test_threads=${test_threads}"
echo "fast_timeout=${fast_timeout} stress_timeout=${stress_timeout} chaos_timeout=${chaos_timeout} chaos_retry=${chaos_retry}"
echo "stress_env: MACE_PROD_BUCKET_STRESS_ROUNDS=${MACE_PROD_BUCKET_STRESS_ROUNDS} MACE_PROD_BUCKET_CHURN_ROUNDS=${MACE_PROD_BUCKET_CHURN_ROUNDS} MACE_PROD_BUCKET_CHURN_WORKERS=${MACE_PROD_BUCKET_CHURN_WORKERS} MACE_PROD_EVICTOR_STRESS_ROUNDS=${MACE_PROD_EVICTOR_STRESS_ROUNDS}"

case "$mode" in
  fast)
    run_fast
    ;;
  stress)
    run_stress
    ;;
  chaos)
    run_chaos
    ;;
  all)
    run_fast
    run_stress
    run_chaos
    ;;
  *)
    echo "invalid mode=${mode}, expected: fast|stress|chaos|all"
    exit 2
    ;;
esac

echo ""
if [[ "${#failures[@]}" -eq 0 ]]; then
  echo "prod test flow finished"
  exit 0
fi

echo "prod test flow finished with failures"
printf '  - %s\n' "${failures[@]}"
exit 1
