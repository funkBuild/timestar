#!/bin/bash

# Simplified profiling script that uses perf's built-in timeout
# Usage: ./profile_simple.sh [duration_seconds]

DURATION=${1:-30}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"

echo "=== Simple TimeStar CPU Profiling ==="
echo "Duration: ${DURATION} seconds"
echo ""

# Check for existing server
pkill -f timestar_http_server 2>/dev/null
sleep 1

# Start server
echo "Starting TimeStar server..."
cd ${BUILD_DIR}
./bin/timestar_http_server --port 8086 &
SERVER_PID=$!
sleep 2

if ! kill -0 ${SERVER_PID} 2>/dev/null; then
    echo "Failed to start server"
    exit 1
fi

echo "Server running with PID: ${SERVER_PID}"
echo ""

# Start load generator in background
echo "Starting load generator..."
(
    end=$(($(date +%s) + ${DURATION}))
    count=0
    while [ $(date +%s) -lt $end ]; do
        # Send a simple write request
        curl -s -X POST http://localhost:8086/write \
            -H "Content-Type: application/json" \
            -d "{
                \"measurement\": \"test\",
                \"fields\": {\"value\": $RANDOM},
                \"timestamp\": $(date +%s%N)
            }" > /dev/null 2>&1 &

        count=$((count + 1))

        # Progress indicator every 100 requests
        if [ $((count % 100)) -eq 0 ]; then
            echo "  Sent ${count} requests..."
        fi

        # 10ms delay between requests
        sleep 0.01
    done
    echo "  Load generation complete (${count} total requests)"
) &
LOAD_PID=$!

# Use perf record with built-in sleep for exact duration
echo "Recording CPU profile for ${DURATION} seconds..."
echo "(This will stop automatically)"
echo ""

# Record for exactly DURATION seconds using perf's sleep
sudo perf record -F 999 -p ${SERVER_PID} -g --call-graph dwarf -o perf.data -- sleep ${DURATION}

echo ""
echo "Profiling complete. Stopping services..."

# Kill load generator if still running
kill ${LOAD_PID} 2>/dev/null
wait ${LOAD_PID} 2>/dev/null

# Stop server
kill ${SERVER_PID} 2>/dev/null
wait ${SERVER_PID} 2>/dev/null

# Check if FlameGraph exists
if [ ! -d "${SCRIPT_DIR}/FlameGraph" ]; then
    echo "Installing FlameGraph tools..."
    cd ${SCRIPT_DIR}
    git clone https://github.com/brendangregg/FlameGraph.git
fi

echo ""
echo "Generating flame graph..."

# Generate flame graph
sudo perf script > out.perf
${SCRIPT_DIR}/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
${SCRIPT_DIR}/FlameGraph/flamegraph.pl \
    --title "TimeStar Insert CPU Profile (${DURATION}s)" \
    --colors hot \
    --width 1800 \
    out.folded > timestar_cpu_profile.svg

# Cleanup
sudo rm -f perf.data perf.data.old
rm -f out.perf out.folded

echo ""
echo "=== Complete ==="
echo "Flame graph saved as: timestar_cpu_profile.svg"
echo ""
echo "View with: firefox timestar_cpu_profile.svg"
echo ""

# Show quick summary
echo "Top CPU consumers:"
sudo perf report --stdio 2>/dev/null | grep -A 10 "Overhead.*Symbol" | head -15