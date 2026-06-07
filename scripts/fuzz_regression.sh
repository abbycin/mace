#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

: "${CARGO_BIN:=cargo}"
: "${RUSTUP_TOOLCHAIN:=nightly}"
: "${LSAN_OPTIONS:=detect_leaks=0}"

run_target() {
    local target="$1"
    local seconds="$2"

    echo "[fuzz] running ${target} for ${seconds}s"
    LSAN_OPTIONS="$LSAN_OPTIONS" \
        "$CARGO_BIN" +"$RUSTUP_TOOLCHAIN" fuzz run "$target" -- -max_total_time="$seconds"
}

run_target publish_gc_reopen 600
run_target txn_checkpoint_reopen 300
run_target bucket_lifecycle 300

echo "[fuzz] all targets passed"
