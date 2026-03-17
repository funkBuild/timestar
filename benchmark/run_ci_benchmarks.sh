#!/usr/bin/env bash
# CI Benchmark Runner — runs curated CPU-bound benchmarks and outputs JSON results.
# Usage: ./benchmark/run_ci_benchmarks.sh [build_dir]
#   build_dir: path to cmake build directory (default: ./build)
#
# Output: benchmark/ci_results.json

set -euo pipefail

BUILD_DIR="${1:-./build}"
RESULTS_FILE="benchmark/ci_results.json"
ITERATIONS=3

if [ ! -d "$BUILD_DIR" ]; then
    echo "ERROR: Build directory '$BUILD_DIR' not found. Build first." >&2
    exit 1
fi

echo "TimeStar CI Benchmark Suite"
echo "==========================="
echo "Build dir: $BUILD_DIR"
echo "Iterations: $ITERATIONS"
echo ""

declare -A results

# Run a benchmark and extract a metric via regex.
# Usage: run_bench "name" "command" "grep_pattern" "sed_extract"
run_bench() {
    local name="$1"
    local cmd="$2"
    local pattern="$3"
    local extract="$4"
    local sum=0
    local count=0

    echo -n "  Running $name... "

    for i in $(seq 1 $ITERATIONS); do
        local value
        # Strip ANSI escape codes before parsing; timeout 60s per run
        value=$(timeout 60 bash -c "$cmd" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -E "$pattern" | head -1 | sed -E "$extract" | tr -d '[:space:]')
        if [ -n "$value" ] && [ "$value" != "0" ]; then
            sum=$(echo "$sum + $value" | bc -l)
            count=$((count + 1))
        fi
    done

    if [ $count -gt 0 ]; then
        local avg
        avg=$(printf "%.2f" "$(echo "scale=4; $sum / $count" | bc -l)")
        results["$name"]="$avg"
        echo "${avg}"
    else
        results["$name"]="0"
        echo "FAILED (no output)"
    fi
}

echo "Running benchmarks (${ITERATIONS}x each, averaged)..."
echo ""

# --- Encoder benchmarks ---

# Integer encoder throughput (sorted timestamps — the primary use case)
run_bench "integer_encode_MBps" \
    "$BUILD_DIR/test/benchmark/encoder_benchmark" \
    "Encode throughput.*MB/s" \
    's/.*throughput:[^0-9]*([0-9.]+) MB\/s.*/\1/'

run_bench "integer_decode_MBps" \
    "$BUILD_DIR/test/benchmark/encoder_benchmark" \
    "Decode throughput.*MB/s" \
    's/.*throughput:[^0-9]*([0-9.]+) MB\/s.*/\1/'

# Bool encoder (BitPack — first data row, "All true")
run_bench "bool_bitpack_encode_ns_per_val" \
    "$BUILD_DIR/test/benchmark/bool_encoder_benchmark" \
    "BitPack.*PASS" \
    's/.*BitPack\s+([0-9.]+)\s+([0-9.]+)\s+.*/\1/'

run_bench "bool_bitpack_decode_ns_per_val" \
    "$BUILD_DIR/test/benchmark/bool_encoder_benchmark" \
    "BitPack.*PASS" \
    's/.*BitPack\s+([0-9.]+)\s+([0-9.]+)\s+.*/\2/'

echo ""
echo "Writing results to $RESULTS_FILE"

# Build JSON output
{
    echo "{"
    echo "  \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\","
    echo "  \"git_commit\": \"$(git describe --always --dirty 2>/dev/null || echo unknown)\","
    echo "  \"iterations\": $ITERATIONS,"
    echo "  \"metrics\": {"
    first=true
    for key in "${!results[@]}"; do
        if [ "$first" = true ]; then
            first=false
        else
            echo ","
        fi
        echo -n "    \"$key\": ${results[$key]}"
    done
    echo ""
    echo "  }"
    echo "}"
} > "$RESULTS_FILE"

echo "Done."
cat "$RESULTS_FILE"
