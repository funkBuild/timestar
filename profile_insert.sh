#!/bin/bash

# Script to profile TSDB insert path and generate flame graph
# Usage: ./profile_insert.sh [duration_seconds] [sample_frequency]

DURATION=${1:-30}  # Default 30 seconds
FREQ=${2:-999}     # Default 999 Hz sampling frequency
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
FLAMEGRAPH_DIR="${SCRIPT_DIR}/FlameGraph"

echo "=== TSDB Insert Path Profiling ==="
echo "Duration: ${DURATION} seconds"
echo "Sample frequency: ${FREQ} Hz"
echo ""

# Check if the server is already running
if pgrep -f tsdb_http_server > /dev/null; then
    echo "TSDB server already running. Please stop it first."
    exit 1
fi

# Start the TSDB server in background
echo "Starting TSDB server..."
cd ${BUILD_DIR}
./bin/tsdb_http_server --port 8086 &
SERVER_PID=$!
echo "Server PID: ${SERVER_PID}"

# Wait for server to start
sleep 2

# Check if server started successfully
if ! kill -0 ${SERVER_PID} 2>/dev/null; then
    echo "Failed to start TSDB server"
    exit 1
fi

echo ""
echo "Starting performance recording..."
echo "The script will send continuous write requests while profiling."
echo ""

# Start perf recording
sudo perf record -F ${FREQ} -p ${SERVER_PID} -g -o perf.data --call-graph dwarf &
PERF_PID=$!

# Give perf time to attach
sleep 1

# Create a Python script for continuous writes
cat > /tmp/insert_load.py << 'EOF'
#!/usr/bin/env python3
import requests
import json
import time
import sys
import random
from datetime import datetime

def generate_batch(size=100):
    """Generate a batch of time series points"""
    current_time = int(time.time() * 1e9)  # Nanoseconds
    writes = []

    for i in range(size):
        point = {
            "measurement": f"benchmark_metric_{i % 10}",
            "tags": {
                "host": f"server-{i % 5}",
                "region": f"region-{i % 3}",
                "datacenter": f"dc-{i % 2}",
                "service": f"service-{i % 4}"
            },
            "fields": {
                "cpu_usage": random.uniform(0, 100),
                "memory_usage": random.uniform(0, 100),
                "disk_io": random.randint(0, 10000),
                "network_bytes": random.randint(0, 1000000),
                "response_time": random.uniform(0.1, 100.0),
                "error_count": random.randint(0, 10),
                "request_count": random.randint(100, 10000)
            },
            "timestamp": current_time + (i * 1000000)  # 1ms apart
        }
        writes.append(point)

    return {"writes": writes}

def main():
    url = "http://localhost:8086/write"
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    batch_size = 100

    print(f"Sending continuous write requests for {duration} seconds...")
    print(f"Batch size: {batch_size} points per request")

    start_time = time.time()
    request_count = 0
    point_count = 0
    errors = 0

    while (time.time() - start_time) < duration:
        try:
            batch_data = generate_batch(batch_size)
            response = requests.post(url, json=batch_data, timeout=1)

            if response.status_code == 200:
                request_count += 1
                point_count += batch_size

                # Print progress every 10 requests
                if request_count % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = point_count / elapsed if elapsed > 0 else 0
                    print(f"  Sent {point_count} points in {request_count} requests ({rate:.0f} points/sec)")
            else:
                errors += 1
                print(f"  Error: {response.status_code}")

        except Exception as e:
            errors += 1
            if errors <= 5:  # Only print first 5 errors
                print(f"  Request error: {e}")

        # Small delay to avoid overwhelming the server
        time.sleep(0.001)  # 1ms between batches

    elapsed = time.time() - start_time
    print(f"\nCompleted in {elapsed:.1f} seconds")
    print(f"Total requests: {request_count}")
    print(f"Total points: {point_count}")
    print(f"Average rate: {point_count/elapsed:.0f} points/sec")
    print(f"Errors: {errors}")

if __name__ == "__main__":
    main()
EOF

chmod +x /tmp/insert_load.py

# Run the load generator
echo "Generating insert load..."
python3 /tmp/insert_load.py ${DURATION} &
LOAD_PID=$!

# Wait for the duration
sleep ${DURATION}

# Stop the load generator
kill ${LOAD_PID} 2>/dev/null

# Stop perf recording
echo ""
echo "Stopping performance recording..."
sudo kill -INT ${PERF_PID}
sleep 2

# Stop the server
echo "Stopping TSDB server..."
kill ${SERVER_PID}
wait ${SERVER_PID} 2>/dev/null

echo ""
echo "Generating flame graph..."

# Generate the flame graph
sudo perf script -i perf.data > out.perf
${FLAMEGRAPH_DIR}/stackcollapse-perf.pl out.perf > out.folded
${FLAMEGRAPH_DIR}/flamegraph.pl out.folded > tsdb_insert_flamegraph.svg

# Also generate a flame graph with better colors and title
${FLAMEGRAPH_DIR}/flamegraph.pl \
    --title "TSDB Insert Path CPU Profile (${DURATION}s @ ${FREQ}Hz)" \
    --colors hot \
    --width 1600 \
    --height 600 \
    --minwidth 0.5 \
    out.folded > tsdb_insert_flamegraph_detailed.svg

# Clean up temporary files
rm -f /tmp/insert_load.py
sudo rm -f perf.data
rm -f out.perf out.folded

echo ""
echo "=== Profiling Complete ==="
echo "Flame graphs generated:"
echo "  - tsdb_insert_flamegraph.svg (standard)"
echo "  - tsdb_insert_flamegraph_detailed.svg (detailed with hot colors)"
echo ""
echo "Open in a web browser to explore:"
echo "  firefox tsdb_insert_flamegraph_detailed.svg"
echo ""

# Show top functions by CPU usage
echo "Top 10 functions by CPU samples:"
sudo perf report -i perf.data.old --no-header --stdio 2>/dev/null | head -20 || true