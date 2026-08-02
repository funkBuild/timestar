#!/bin/bash
# Focused release gate for the file-backed exact-v1 snapshot pipeline. It forces
# 128 MiB + 1 byte through leader hydration, v1 Raft framing, bounded 4-MiB
# chunks, receiver disk staging, and final size/hash validation. The source is
# sparse; the received sidecar is real. All temporary data lives below build/tmp.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${1:-$ROOT/build/test/timestar_test}"
WORK="$ROOT/build/tmp/large_snapshot_streaming_gate"

[ -x "$BIN" ] || { echo "no test binary at $BIN" >&2; exit 2; }
case "$WORK" in
    "$ROOT"/build/tmp/large_snapshot_streaming_gate) ;;
    *) echo "unsafe gate workspace: $WORK" >&2; exit 2 ;;
esac
rm -rf "$WORK"
mkdir -p "$WORK"
trap 'rm -rf "$WORK"' EXIT

echo "LARGE_SNAPSHOT_GATE workspace=$WORK memory=1G chunk=4MiB payload=128MiB+1"
TMPDIR="$WORK" timeout 240 "$BIN" \
    --gtest_filter=RaftJournalPersistenceTest.DISABLED_FileSnapshotStreamsBeyond128MiBInlineLimit \
    --gtest_also_run_disabled_tests \
    --smp 1 --memory 1G --overprovisioned

echo "LARGE_SNAPSHOT_GATE PASSED"
