#!/usr/bin/env bash
#
# ALP vs Gorilla TSM file size benchmark.
#
# Inserts a large deterministic dataset, then restarts the server to force
# all WAL data into TSM files before measuring on-disk sizes.
#
# Usage: bash filesize_bench.sh
#
set -euo pipefail

TIMESTAR_ROOT="/home/matt/Desktop/source/timestar"
BUILD_DIR="$TIMESTAR_ROOT/build"
SERVER_BIN="$BUILD_DIR/bin/timestar_http_server"
BENCH_SCRIPT="$TIMESTAR_ROOT/test_api/http_api_benchmark/filesize_bench_insert.js"
FLOAT_HEADER="$TIMESTAR_ROOT/lib/encoding/float_encoder.hpp"
PORT=8086
BASE_URL="http://localhost:$PORT"

ALP_LOG="/tmp/filesize_alp.log"
GORILLA_LOG="/tmp/filesize_gorilla.log"

# ── Helpers ─────────────────────────────────────────────────────────

kill_server() {
  local pids
  pids=$(pgrep -f timestar_http_server 2>/dev/null || true)
  if [[ -n "$pids" ]]; then
    echo "  Stopping server (PIDs: $pids)..."
    kill -SIGINT $pids 2>/dev/null || true
    # Wait for graceful shutdown (drains background TSM conversions)
    for i in $(seq 1 30); do
      if ! pgrep -f timestar_http_server >/dev/null 2>&1; then
        echo "  Server stopped."
        return 0
      fi
      sleep 1
    done
    echo "  Force killing..."
    kill -9 $pids 2>/dev/null || true
    sleep 2
  fi
}

clean_shards() {
  echo "  Cleaning shard directories..."
  rm -rf "$BUILD_DIR"/shard_*
}

start_server() {
  echo "  Starting server on port $PORT..."
  cd "$BUILD_DIR"
  "$SERVER_BIN" --port "$PORT" >/dev/null 2>&1 &
  cd "$TIMESTAR_ROOT"

  # Wait for health
  for i in $(seq 1 60); do
    if curl -sf "$BASE_URL/health" >/dev/null 2>&1; then
      echo "  Server healthy."
      return 0
    fi
    sleep 1
  done
  echo "  ERROR: Server did not become healthy in 60s"
  exit 1
}

wait_for_port_free() {
  for i in $(seq 1 15); do
    if ! ss -tlnp 2>/dev/null | grep -q ":$PORT "; then
      return 0
    fi
    sleep 1
  done
  echo "  WARNING: Port $PORT still in use after 15s"
}

set_compression() {
  local algo="$1"  # ALP or GORILLA
  echo "  Setting FLOAT_COMPRESSION = $algo"
  sed -i "s/FLOAT_COMPRESSION = FloatCompression::[A-Z]*/FLOAT_COMPRESSION = FloatCompression::$algo/" "$FLOAT_HEADER"
}

build() {
  echo "  Building..."
  cd "$BUILD_DIR"
  cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo >/dev/null 2>&1
  make -j"$(nproc)" timestar_http_server 2>&1 | tail -5
  cd "$TIMESTAR_ROOT"
  echo "  Build complete."
}

measure_disk() {
  local label="$1"
  echo ""
  echo "=== ON-DISK SIZE [$label] (after WAL→TSM compaction) ==="

  local wal_bytes=0 tsm_bytes=0 index_bytes=0 total_bytes=0 shard_count=0

  if ls "$BUILD_DIR"/shard_* 1>/dev/null 2>&1; then
    while IFS=' ' read -r size path; do
      total_bytes=$((total_bytes + size))
      if [[ "$path" == *.wal ]]; then
        wal_bytes=$((wal_bytes + size))
      elif [[ "$path" == */tsm/* ]]; then
        tsm_bytes=$((tsm_bytes + size))
      elif [[ "$path" == */index/* ]]; then
        index_bytes=$((index_bytes + size))
      fi
    done < <(find "$BUILD_DIR"/shard_* -type f -printf '%s %p\n' 2>/dev/null)

    shard_count=$(ls -d "$BUILD_DIR"/shard_* 2>/dev/null | wc -l)
  fi

  local fmt_wal fmt_tsm fmt_idx fmt_total
  fmt_wal=$(numfmt --to=iec-i --suffix=B $wal_bytes 2>/dev/null || echo "${wal_bytes} B")
  fmt_tsm=$(numfmt --to=iec-i --suffix=B $tsm_bytes 2>/dev/null || echo "${tsm_bytes} B")
  fmt_idx=$(numfmt --to=iec-i --suffix=B $index_bytes 2>/dev/null || echo "${index_bytes} B")
  fmt_total=$(numfmt --to=iec-i --suffix=B $total_bytes 2>/dev/null || echo "${total_bytes} B")

  echo "  WAL files:     $fmt_wal"
  echo "  TSM files:     $fmt_tsm"
  echo "  Index files:   $fmt_idx"
  echo "  Total:         $fmt_total"
  echo "  Shards:        $shard_count"
  echo ""

  # Compute raw size: totalPoints * 8 bytes per double
  # totalPoints = NUM_HOSTS * MINUTES_TOTAL * NUM_FIELDS = 500 * 259200 * 5 = 648,000,000
  local total_points=648000000
  local raw_bytes=$((total_points * 8))

  if [[ $tsm_bytes -gt 0 ]]; then
    echo "  Raw value data:          $(numfmt --to=iec-i --suffix=B $raw_bytes)"
    echo "  Bytes/point (TSM only):  $(echo "scale=3; $tsm_bytes / $total_points" | bc)"
    echo "  Bytes/point (total):     $(echo "scale=3; $total_bytes / $total_points" | bc)"
    echo "  TSM compression ratio:   $(echo "scale=2; $raw_bytes / $tsm_bytes" | bc)x"
    echo "  Total compression ratio: $(echo "scale=2; $raw_bytes / $total_bytes" | bc)x"
  else
    echo "  WARNING: No TSM files found!"
  fi

  echo "=== END [$label] ==="
  echo ""

  # Save results to a variable file for the final comparison
  eval "${label}_wal=$wal_bytes"
  eval "${label}_tsm=$tsm_bytes"
  eval "${label}_index=$index_bytes"
  eval "${label}_total=$total_bytes"
}

# ── Main ────────────────────────────────────────────────────────────

run_benchmark() {
  local label="$1"  # ALP or GORILLA

  echo ""
  echo "################################################################"
  echo "  BENCHMARK: $label"
  echo "################################################################"

  # 1. Set compression and build
  set_compression "$label"
  build

  # 2. Clean slate
  kill_server
  wait_for_port_free
  clean_shards

  # 3. Start server and insert data
  start_server
  echo ""
  echo "--- Inserting data [$label] ---"
  cd "$TIMESTAR_ROOT/test_api/http_api_benchmark"
  node filesize_bench_insert.js "$label" 2>&1
  cd "$TIMESTAR_ROOT"

  # 4. Stop server gracefully (drains in-flight TSM conversions)
  echo ""
  echo "--- Stopping server (flush in-flight conversions) ---"
  kill_server
  wait_for_port_free
  sleep 2

  # 5. Restart server → WAL replay forces all remaining data into TSM
  echo ""
  echo "--- Restarting server (WAL replay → TSM compaction) ---"
  start_server

  # Give it time to finish WAL replay (each WAL file → TSM + delete)
  echo "  Waiting for WAL replay to complete..."
  sleep 10

  # 6. Stop server again so files are stable on disk
  echo "--- Stopping server for measurement ---"
  kill_server
  wait_for_port_free
  sleep 2

  # 7. Measure
  measure_disk "$label"
}

echo "========================================================"
echo " TSM File Size Benchmark: ALP vs Gorilla"
echo " Dataset: 500 hosts × 259,200 min × 5 fields = 648M pts"
echo "========================================================"

# Run both benchmarks
run_benchmark "ALP"
run_benchmark "GORILLA"

# ── Final comparison ──────────────────────────────────────────────

echo ""
echo "################################################################"
echo "  FINAL COMPARISON"
echo "################################################################"
echo ""
echo "                    ALP             Gorilla         Ratio"
echo "  ─────────────────────────────────────────────────────────"
printf "  TSM files:    %12s    %12s    %.2fx\n" \
  "$(numfmt --to=iec-i --suffix=B $ALP_tsm)" \
  "$(numfmt --to=iec-i --suffix=B $GORILLA_tsm)" \
  "$(echo "scale=2; $GORILLA_tsm / $ALP_tsm" | bc)"
printf "  WAL residual: %12s    %12s\n" \
  "$(numfmt --to=iec-i --suffix=B $ALP_wal)" \
  "$(numfmt --to=iec-i --suffix=B $GORILLA_wal)"
printf "  Index:        %12s    %12s\n" \
  "$(numfmt --to=iec-i --suffix=B $ALP_index)" \
  "$(numfmt --to=iec-i --suffix=B $GORILLA_index)"
printf "  Total:        %12s    %12s    %.2fx\n" \
  "$(numfmt --to=iec-i --suffix=B $ALP_total)" \
  "$(numfmt --to=iec-i --suffix=B $GORILLA_total)" \
  "$(echo "scale=2; $GORILLA_total / $ALP_total" | bc)"
echo ""
echo "  ALP TSM compression:     $(echo "scale=2; 648000000 * 8 / $ALP_tsm" | bc)x"
echo "  Gorilla TSM compression: $(echo "scale=2; 648000000 * 8 / $GORILLA_tsm" | bc)x"
echo ""

# Restore ALP as default
set_compression "ALP"
echo "  (Restored FLOAT_COMPRESSION = ALP)"
echo ""
