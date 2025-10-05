#!/bin/bash

# Robust profiling script for TSDB insert path with proper timeout handling
# Usage: ./profile_insert_fixed.sh [duration_seconds]

DURATION=${1:-30}  # Default 30 seconds
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
FLAMEGRAPH_DIR="${SCRIPT_DIR}/FlameGraph"

echo "=== TSDB Insert Path Profiling ==="
echo "Duration: ${DURATION} seconds"
echo ""

# Check if FlameGraph tools exist
if [ ! -d "${FLAMEGRAPH_DIR}" ]; then
    echo "FlameGraph tools not found. Cloning..."
    cd ${SCRIPT_DIR}
    git clone https://github.com/brendangregg/FlameGraph.git
fi

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up processes..."

    # Kill load generator
    if [ ! -z "${LOAD_PID}" ]; then
        kill ${LOAD_PID} 2>/dev/null
        pkill -f "insert_load.py" 2>/dev/null
    fi

    # Stop perf
    if [ ! -z "${PERF_PID}" ]; then
        sudo kill -INT ${PERF_PID} 2>/dev/null
    fi

    # Stop server
    if [ ! -z "${SERVER_PID}" ]; then
        kill ${SERVER_PID} 2>/dev/null
        wait ${SERVER_PID} 2>/dev/null
    fi

    # Make sure all curl processes are dead
    pkill -f "curl.*8086/write" 2>/dev/null

    # Remove temp files
    rm -f /tmp/insert_load.py /tmp/simple_insert.sh
}

# Set trap for cleanup on exit
trap cleanup EXIT INT TERM

# Check if the server is already running
if pgrep -f tsdb_http_server > /dev/null; then
    echo "TSDB server already running. Please stop it first:"
    echo "  pkill -f tsdb_http_server"
    exit 1
fi

# Start the TSDB server
echo "Starting TSDB server..."
cd ${BUILD_DIR}
./bin/tsdb_http_server --port 8086 &
SERVER_PID=$!
echo "Server PID: ${SERVER_PID}"

# Wait for server to start
sleep 2

# Verify server is running
if ! kill -0 ${SERVER_PID} 2>/dev/null; then
    echo "Failed to start TSDB server"
    exit 1
fi

# Create simple load generator script
cat > /tmp/simple_insert.sh << 'EOF'
#!/bin/bash
duration=$1
start_time=$(date +%s)
count=0

echo "Generating insert load for ${duration} seconds..."

while true; do
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))

    if [ $elapsed -ge $duration ]; then
        echo "Load generation complete after ${elapsed} seconds (${count} requests)"
        break
    fi

    # Send batch write request
    curl -s -X POST http://localhost:8086/write \
        -H "Content-Type: application/json" \
        -d '{
            "writes": [
                {"measurement": "cpu", "tags": {"host": "server1"}, "fields": {"usage": 45.2}, "timestamp": '$(date +%s%N)'},
                {"measurement": "cpu", "tags": {"host": "server2"}, "fields": {"usage": 67.8}, "timestamp": '$(date +%s%N)'},
                {"measurement": "memory", "tags": {"host": "server1"}, "fields": {"used": 2048}, "timestamp": '$(date +%s%N)'},
                {"measurement": "memory", "tags": {"host": "server2"}, "fields": {"used": 4096}, "timestamp": '$(date +%s%N)'},
                {"measurement": "disk", "tags": {"host": "server1", "device": "sda"}, "fields": {"usage": 78.5}, "timestamp": '$(date +%s%N)'}
            ]
        }' > /dev/null 2>&1

    count=$((count + 1))

    # Small sleep to avoid overwhelming
    sleep 0.01
done
EOF

chmod +x /tmp/simple_insert.sh

echo ""
echo "Starting performance recording for ${DURATION} seconds..."
echo "Recording will automatically stop after the duration."
echo ""

# Start perf with a timeout - record for exactly DURATION seconds
sudo timeout $((DURATION + 2)) perf record -F 999 -p ${SERVER_PID} -g --call-graph dwarf -o perf.data &
PERF_PID=$!

# Give perf time to attach
sleep 1

# Start load generator with timeout
timeout ${DURATION} /tmp/simple_insert.sh ${DURATION} &
LOAD_PID=$!

# Wait for the load generator to complete
echo "Profiling in progress..."
wait ${LOAD_PID}

# Give perf a bit more time to finish
sleep 2

# Make sure perf has stopped
sudo pkill -INT -f "perf record.*${SERVER_PID}" 2>/dev/null

echo ""
echo "Stopping server..."
kill ${SERVER_PID} 2>/dev/null
wait ${SERVER_PID} 2>/dev/null

echo ""
echo "Generating flame graph..."

# Check if we have perf data
if [ ! -f perf.data ]; then
    echo "Error: perf.data not found. Profiling may have failed."
    exit 1
fi

# Generate the flame graph
sudo perf script -i perf.data > out.perf

# Check if we got any samples
if [ ! -s out.perf ]; then
    echo "Warning: No samples collected. Try running with more load or longer duration."
else
    ${FLAMEGRAPH_DIR}/stackcollapse-perf.pl out.perf > out.folded

    # Generate detailed flame graph
    ${FLAMEGRAPH_DIR}/flamegraph.pl \
        --title "TSDB Insert Path CPU Profile (${DURATION}s @ 999Hz)" \
        --colors hot \
        --width 1800 \
        --height 800 \
        --minwidth 0.5 \
        out.folded > tsdb_insert_flamegraph.svg

    echo "Flame graph generated: tsdb_insert_flamegraph.svg"

    # Show summary statistics
    echo ""
    echo "Top 10 functions by CPU usage:"
    sudo perf report -i perf.data --stdio --no-header | head -15
fi

# Cleanup temp files (trap will handle the rest)
sudo rm -f perf.data perf.data.old
rm -f out.perf out.folded

echo ""
echo "=== Profiling Complete ==="
echo ""
echo "To view the flame graph:"
echo "  firefox ${SCRIPT_DIR}/tsdb_insert_flamegraph.svg"
echo ""
echo "Or use Python's HTTP server:"
echo "  cd ${SCRIPT_DIR} && python3 -m http.server 8000"
echo "  Then open: http://localhost:8000/tsdb_insert_flamegraph.svg"