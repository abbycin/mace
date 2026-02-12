#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

mode="${1:-compare}"
if [[ $# -gt 1 ]]; then
  echo "usage: $0 [compare|snapshot]"
  exit 2
fi

threshold_file="${MACE_PERF_THRESHOLD_FILE:-scripts/perf_thresholds.env}"
if [[ -f "$threshold_file" ]]; then
  set -a
  source "$threshold_file"
  set +a
fi

bench_args="${MACE_PERF_BENCH_ARGS:---noplot}"
out_dir="${MACE_PERF_OUT_DIR:-target/perf_gate}"
mkdir -p "$out_dir"
out_dir="$(cd "$out_dir" && pwd)"

base_ref="${MACE_PERF_BASE_REF:-origin/master}"
regress_pct="${MACE_PERF_REGRESS_PCT:-20}"
max_regress_cases="${MACE_PERF_MAX_REGRESS_CASES:-0}"
min_compare_cases="${MACE_PERF_MIN_COMPARE_CASES:-6}"
min_base_ns="${MACE_PERF_MIN_BASE_NS:-10000}"

compare_filters="${MACE_PERF_COMPARE_FILTERS:-}"

base_worktree=""
cleanup_base_worktree() {
  if [[ -n "${base_worktree:-}" ]]; then
    git worktree remove --force "$base_worktree" >/dev/null 2>&1 || true
  fi
}
trap cleanup_base_worktree EXIT

run_bench() {
  local workdir="$1"
  local label="$2"
  local summary_json="$3"
  local filters="$4"
  local bench_log="$out_dir/${label}.bench.log"

  echo "==> bench ${label}"
  (
    cd "$workdir"
    rm -rf target/criterion
    if [[ -n "$filters" ]]; then
      IFS=',' read -ra filter_items <<< "$filters"
      for filter in "${filter_items[@]}"; do
        filter="$(echo "$filter" | xargs)"
        if [[ -z "$filter" ]]; then
          continue
        fi
        cargo bench --bench perf -- ${bench_args} "$filter"
      done
    else
      cargo bench --bench perf -- ${bench_args}
    fi
  ) | tee "$bench_log"

  python3 - "$workdir/target/criterion" "$summary_json" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
out = pathlib.Path(sys.argv[2])

cases = {}
for estimates in root.rglob("new/estimates.json"):
    rel = estimates.relative_to(root).as_posix()
    case = rel.removesuffix("/new/estimates.json")
    with estimates.open("r", encoding="utf-8") as f:
        data = json.load(f)
    median = data.get("median", {}).get("point_estimate")
    mean = data.get("mean", {}).get("point_estimate")
    if median is None:
        continue
    cases[case] = {
        "median_ns": float(median),
        "mean_ns": float(mean if mean is not None else median),
    }

summary = {
    "count": len(cases),
    "cases": dict(sorted(cases.items())),
}
out.parent.mkdir(parents=True, exist_ok=True)
with out.open("w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2, sort_keys=True)

print(f"cases={summary['count']}")
PY
}

run_snapshot() {
  local output="${MACE_PERF_OUTPUT:-$out_dir/head_snapshot.json}"
  run_bench "$repo_root" "snapshot" "$output" ""

  python3 - "$output" <<'PY'
import json
import pathlib
import statistics
import sys

path = pathlib.Path(sys.argv[1])
with path.open("r", encoding="utf-8") as f:
    data = json.load(f)

vals = [x["median_ns"] for x in data["cases"].values()]
if not vals:
    print("perf snapshot: no cases")
    sys.exit(1)

vals.sort()
p50 = statistics.median(vals)
p95 = vals[max(0, int(len(vals) * 0.95) - 1)]
print(f"perf snapshot: cases={len(vals)} median_ns_p50={p50:.2f} median_ns_p95={p95:.2f}")
PY
}

run_compare() {
  if ! git rev-parse --verify "${base_ref}^{commit}" >/dev/null 2>&1; then
    echo "base ref ${base_ref} not found locally, fetching"
    git fetch --no-tags --depth=1 origin master
  fi

  base_worktree="$(mktemp -d /tmp/mace-perf-base.XXXXXX)"

  git worktree add --detach "$base_worktree" "$base_ref" >/dev/null

  local base_json="$out_dir/base_summary.json"
  local head_json="$out_dir/head_summary.json"

  run_bench "$base_worktree" "base" "$base_json" "$compare_filters"
  run_bench "$repo_root" "head" "$head_json" "$compare_filters"

  python3 - "$base_json" "$head_json" "$regress_pct" "$max_regress_cases" "$min_compare_cases" "$min_base_ns" <<'PY'
import json
import sys

base_path, head_path, regress_pct, max_regress_cases, min_compare_cases, min_base_ns = sys.argv[1:7]
regress_pct = float(regress_pct)
max_regress_cases = int(max_regress_cases)
min_compare_cases = int(min_compare_cases)
min_base_ns = float(min_base_ns)

with open(base_path, "r", encoding="utf-8") as f:
    base = json.load(f)["cases"]
with open(head_path, "r", encoding="utf-8") as f:
    head = json.load(f)["cases"]

common = sorted(set(base.keys()) & set(head.keys()))
compared = []
for case in common:
    base_m = float(base[case]["median_ns"])
    head_m = float(head[case]["median_ns"])
    if base_m < min_base_ns:
        continue
    delta_pct = (head_m - base_m) / base_m * 100.0
    compared.append((case, base_m, head_m, delta_pct))

if len(compared) < min_compare_cases:
    print(f"perf gate failed: compared cases {len(compared)} < {min_compare_cases}")
    sys.exit(1)

compared.sort(key=lambda x: x[3], reverse=True)
regress = [x for x in compared if x[3] > regress_pct]

print(f"perf compare: base_cases={len(base)} head_cases={len(head)} compared={len(compared)}")
print(f"perf threshold: regress_pct>{regress_pct:.2f} max_regress_cases={max_regress_cases}")
print("top deltas (positive is slower):")
for case, base_m, head_m, delta_pct in compared[:12]:
    print(f"  {case}: base={base_m:.2f}ns head={head_m:.2f}ns delta={delta_pct:+.2f}%")

if len(regress) > max_regress_cases:
    print(f"perf gate failed: regress_cases={len(regress)} > {max_regress_cases}")
    for case, base_m, head_m, delta_pct in regress:
        print(f"  regress {case}: base={base_m:.2f}ns head={head_m:.2f}ns delta={delta_pct:+.2f}%")
    sys.exit(1)

print("perf gate passed")
PY
}

case "$mode" in
  snapshot)
    run_snapshot
    ;;
  compare)
    run_compare
    ;;
  *)
    echo "invalid mode=${mode}, expected compare|snapshot"
    exit 2
    ;;
esac
