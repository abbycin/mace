#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

threads="${SOAK_THREADS:-8}"
duration_secs="${SOAK_DURATION_SECS:-21600}"
stress_timeout="${SOAK_STRESS_TIMEOUT:-900}"
chaos_timeout="${SOAK_CHAOS_TIMEOUT:-600}"
fast_timeout="${SOAK_FAST_TIMEOUT:-300}"
chaos_every="${SOAK_CHAOS_EVERY:-3}"
perf_every="${SOAK_PERF_EVERY:-1}"
cycle_pause_secs="${SOAK_CYCLE_PAUSE_SECS:-5}"
metrics_dir="${SOAK_METRICS_DIR:-target/prod_soak}"

mkdir -p "$metrics_dir"
perf_csv="$metrics_dir/perf_cycles.csv"
if [[ ! -f "$perf_csv" ]]; then
  echo "cycle,elapsed_secs,cases,median_ns_p50,median_ns_p95" > "$perf_csv"
fi

start_ts="$(date +%s)"
deadline_ts="$((start_ts + duration_secs))"
cycle=0

collect_perf_snapshot() {
  local cycle_id="$1"
  local elapsed="$2"
  local snapshot="$metrics_dir/perf_cycle_${cycle_id}.json"

  MACE_PERF_OUTPUT="$snapshot" ./scripts/perf_gate.sh snapshot

  python3 - "$snapshot" "$perf_csv" "$cycle_id" "$elapsed" <<'PY'
import json
import statistics
import sys

snapshot_path, csv_path, cycle, elapsed = sys.argv[1:5]
with open(snapshot_path, "r", encoding="utf-8") as f:
    data = json.load(f)

vals = [x["median_ns"] for x in data.get("cases", {}).values()]
if not vals:
    raise SystemExit("perf snapshot has no cases")

vals.sort()
p50 = statistics.median(vals)
p95 = vals[max(0, int(len(vals) * 0.95) - 1)]
with open(csv_path, "a", encoding="utf-8") as f:
    f.write(f"{cycle},{elapsed},{len(vals)},{p50:.2f},{p95:.2f}\n")

print(f"perf cycle={cycle} cases={len(vals)} median_ns_p50={p50:.2f} median_ns_p95={p95:.2f}")
PY
}

echo "repo_root=${repo_root}"
echo "soak duration_secs=${duration_secs} threads=${threads}"
echo "timeouts stress=${stress_timeout}s chaos=${chaos_timeout}s fast=${fast_timeout}s"
echo "cadence chaos_every=${chaos_every} perf_every=${perf_every} pause=${cycle_pause_secs}s"
echo "metrics_dir=${metrics_dir}"

while true; do
  now_ts="$(date +%s)"
  if (( now_ts >= deadline_ts )); then
    break
  fi

  cycle=$((cycle + 1))
  elapsed="$((now_ts - start_ts))"

  echo ""
  echo "==> soak cycle ${cycle} elapsed=${elapsed}s"

  FAST_TIMEOUT="$fast_timeout" STRESS_TIMEOUT="$stress_timeout" CHAOS_TIMEOUT="$chaos_timeout" \
    ./scripts/prod_test.sh stress "$threads"

  if (( chaos_every > 0 )) && (( cycle % chaos_every == 0 )); then
    FAST_TIMEOUT="$fast_timeout" STRESS_TIMEOUT="$stress_timeout" CHAOS_TIMEOUT="$chaos_timeout" \
      ./scripts/prod_test.sh chaos "$threads"
  fi

  if (( perf_every > 0 )) && (( cycle % perf_every == 0 )); then
    collect_perf_snapshot "$cycle" "$elapsed"
  fi

  sleep "$cycle_pause_secs"
done

FAST_TIMEOUT="$fast_timeout" STRESS_TIMEOUT="$stress_timeout" CHAOS_TIMEOUT="$chaos_timeout" \
  ./scripts/prod_test.sh fast "$threads"

echo ""
echo "soak finished cycles=${cycle} elapsed=$(( $(date +%s) - start_ts ))s"
echo "perf csv: ${perf_csv}"
