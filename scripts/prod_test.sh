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

gc_space_accounting_cases=(
  "gc:persisted_data_and_blob_stats_match_payloads_through_gc_and_reopen:normal"
  "gc:compressed_persisted_data_and_blob_stats_match_payloads_through_gc_and_reopen:normal"
  "gc:persisted_gc_stats_are_bucket_scoped:normal"
  "prod_recovery_failpoints:chaos_failpoint_stat_mask_before_load:crash"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_data_sync:crash"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_data_dir_sync:crash"
  "prod_recovery_failpoints:chaos_failpoint_flush_before_manifest_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_manifest_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_manifest_commit_with_retire:crash"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_data_sync_with_retire:crash"
  "prod_recovery_failpoints:chaos_failpoint_flush_before_manifest_commit_with_retire:crash"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_manifest_commit_with_retire_multi_bucket:crash"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_old_stat_delta:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_before_meta_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_after_stage_marker:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_after_data_dir_sync:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_after_meta_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_collecting_junk_before_meta_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_collecting_junk_after_meta_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_rewrite_before_meta_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_rewrite_after_stage_marker:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_rewrite_after_data_dir_sync:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_rewrite_after_meta_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_obsolete_after_meta_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_obsolete_after_retired_mark:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_obsolete_after_remove_stat:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_obsolete_after_meta_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_obsolete_after_retired_mark:crash"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_obsolete_after_remove_stat:crash"
  "prod_recovery_failpoints:chaos_failpoint_bucket_delete_before_manifest_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_bucket_delete_after_manifest_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_pending_bucket_reap_after_batch_before_finalize:crash"
  "prod_recovery_failpoints:chaos_failpoint_pending_bucket_reap_after_finalize_before_meta_commit:crash"
  "prod_recovery_failpoints:chaos_failpoint_pending_bucket_reap_after_meta_commit:crash"
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
  "prod_recovery:chaos_failpoint_fs_create_dir_all_io"
  "prod_recovery:chaos_failpoint_fs_sync_dir_io"
  "prod_recovery:chaos_failpoint_fs_read_dir_io"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_data_sync"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_data_dir_sync"
  "prod_recovery_failpoints:chaos_failpoint_flush_before_manifest_commit"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_manifest_commit"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_manifest_commit_with_retire"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_data_sync_with_retire"
  "prod_recovery_failpoints:chaos_failpoint_flush_before_manifest_commit_with_retire"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_manifest_commit_with_retire_multi_bucket"
  "prod_recovery_failpoints:chaos_failpoint_flush_after_old_stat_delta"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_obsolete_after_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_obsolete_after_retired_mark"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_obsolete_after_remove_stat"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_obsolete_after_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_obsolete_after_retired_mark"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_obsolete_after_remove_stat"
  "prod_recovery_failpoints:chaos_failpoint_wal_after_checkpoint_write"
  "prod_recovery_failpoints:chaos_failpoint_manifest_before_multi_commit"
  "prod_recovery_failpoints:chaos_failpoint_wal_recycle_before_intent_commit"
  "prod_recovery_failpoints:chaos_failpoint_wal_recycle_after_remove_before_dir_sync"
  "prod_recovery_failpoints:chaos_failpoint_wal_recycle_after_dir_sync_before_done_commit"
  "prod_recovery_failpoints:chaos_failpoint_wal_recycle_after_done_commit_before_publish"
  "prod_recovery_failpoints:chaos_failpoint_recovery_wal_recycle_after_dir_sync_before_done_commit"
  "prod_recovery_failpoints:chaos_failpoint_recovery_wal_recycle_after_done_commit_before_publish"
  "prod_recovery_failpoints:chaos_failpoint_recovery_fs_remove_file_io"
  "prod_recovery_failpoints:wal_recycle_done_reopen_is_idempotent"
  "prod_recovery_failpoints:wal_recycle_done_does_not_weaken_gap_detection_after_frontier"
  "prod_recovery_failpoints:chaos_failpoint_txn_commit_after_wal_file_sync_before_dir_sync"
  "prod_recovery_failpoints:chaos_failpoint_txn_commit_after_record_commit"
  "prod_recovery_failpoints:chaos_failpoint_txn_commit_after_wal_sync"
  "prod_recovery_failpoints:chaos_failpoint_bucket_create_before_manifest_commit"
  "prod_recovery_failpoints:chaos_failpoint_bucket_create_after_manifest_commit"
  "prod_recovery_failpoints:chaos_failpoint_bucket_delete_before_manifest_commit"
  "prod_recovery_failpoints:chaos_failpoint_bucket_delete_after_manifest_commit"
  "prod_recovery_failpoints:chaos_failpoint_pending_bucket_reap_after_batch_before_finalize"
  "prod_recovery_failpoints:chaos_failpoint_pending_bucket_reap_after_finalize_before_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_pending_bucket_reap_after_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_before_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_after_stage_marker"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_after_data_dir_sync"
  "prod_recovery_failpoints:chaos_failpoint_gc_data_rewrite_after_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_rewrite_before_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_rewrite_after_stage_marker"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_rewrite_after_data_dir_sync"
  "prod_recovery_failpoints:chaos_failpoint_gc_blob_rewrite_after_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_delete_files_after_dir_sync_before_meta_commit"
  "prod_recovery_failpoints:chaos_failpoint_recovery_orphan_cleanup_after_data_dir_sync_before_marker_clear"
  "prod_recovery_failpoints:chaos_failpoint_recovery_abort_clean_after_drain_before_start"
  "prod_recovery_failpoints:chaos_failpoint_recovery_abort_clean_does_not_recycle_wal_before_runtime_checkpoint"
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

run_gc_space_accounting_case() {
  local target="$1"
  local test_name="$2"
  local mode="$3"
  local case_features="failpoints,extra_check"
  local listed
  local list_code=0

  set +e
  listed="$(timeout "$fast_timeout" cargo test --features "$case_features" --test "$target" "$test_name" -- --list 2>/dev/null)"
  list_code=$?
  set -e
  if [[ "$list_code" -ne 0 ]]; then
    echo "==> [fast:gc_space_accounting:${test_name}:list] failed with exit code ${list_code}"
    return "$list_code"
  fi

  local count
  count="$(printf '%s\n' "$listed" | grep -Fxc "${test_name}: test" || true)"
  if [[ "$count" != "1" ]]; then
    echo "==> [fast:gc_space_accounting:${test_name}:list] expected one test with --features ${case_features}, found ${count}"
    run_cmd "fast:gc_space_accounting:${test_name}:list-diagnostic" "$fast_timeout" \
      cargo test --features "$case_features" --test "$target" "$test_name" -- --list || true
    return 1
  fi

  local args=(test --features "$case_features" --test "$target" "$test_name" -- --exact --nocapture --test-threads="$test_threads")
  if [[ "$mode" == "crash" ]]; then
    args=(test --features "$case_features" --test "$target" "$test_name" -- --ignored --exact --nocapture --test-threads="$test_threads")
  fi
  run_cmd "fast:gc_space_accounting:${test_name}" "$fast_timeout" cargo "${args[@]}"
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

  # the shared-durable-wal generation/route-switch crash windows require BOTH
  # failpoints and extra_check; neither the default matrix (extra_check only)
  # nor this script's failpoints-only feature set runs them, so they are gated
  # here as a dedicated invocation. a --list count guard prevents a feature
  # gate drift from silently turning this into a passing "0 tests"
  local gen_features="failpoints,extra_check"
  local gen_target="prod_generation_failpoints"
  local gen_list
  local gen_list_code=0
  set +e
  gen_list="$(timeout "$fast_timeout" cargo test --features "$gen_features" --test "$gen_target" -- --list 2>/dev/null)"
  gen_list_code=$?
  set -e
  if [[ "$gen_list_code" -ne 0 ]]; then
    if [[ "$gen_list_code" -eq 124 ]]; then
      echo "==> [fast:prod_generation_failpoints:list] timeout"
    else
      echo "==> [fast:prod_generation_failpoints:list] failed with exit code ${gen_list_code}"
    fi
    failures+=("fast:prod_generation_failpoints:list")
    return 0
  fi
  local gen_count
  gen_count="$(printf '%s\n' "$gen_list" | grep -c ': test$' || true)"
  echo "==> [fast:prod_generation_failpoints] feature-gated tests found: ${gen_count}"
  if [[ -z "${gen_count}" || "${gen_count}" -le 0 ]]; then
    echo "==> [fast:prod_generation_failpoints] FAILED: zero tests with --features ${gen_features} (feature gate drift?)"
    echo "==> [fast:prod_generation_failpoints] re-running the --list probe with stderr visible:"
    run_cmd "fast:prod_generation_failpoints:list-diagnostic" "$fast_timeout" \
      cargo test --features "$gen_features" --test "$gen_target" -- --list || true
    failures+=("fast:prod_generation_failpoints")
    return 0
  fi
  if ! run_cmd "fast:prod_generation_failpoints" "$fast_timeout" \
    cargo test --features "$gen_features" --test "$gen_target" -- --nocapture --test-threads="$test_threads"; then
    failures+=("fast:prod_generation_failpoints")
  fi

  # gc space accounting needs the physical payload oracle, which is intentionally
  # compiled only with extra_check. keep this explicit so the failpoints-only
  # default cannot silently turn the recovery checks into visibility-only tests.
  local entry target test_name mode
  for entry in "${gc_space_accounting_cases[@]}"; do
    IFS=':' read -r target test_name mode <<<"$entry"
    if ! run_gc_space_accounting_case "$target" "$test_name" "$mode"; then
      failures+=("fast:gc_space_accounting:${test_name}")
    fi
  done
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
