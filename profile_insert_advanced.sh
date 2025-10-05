#!/bin/bash

# Advanced profiling script with multiple profiling options
# Usage: ./profile_insert_advanced.sh [mode] [duration]
# Modes: cpu (default), offcpu, cache, branch, instructions

MODE=${1:-cpu}
DURATION=${2:-30}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
FLAMEGRAPH_DIR="${SCRIPT_DIR}/FlameGraph"

echo "=== Advanced TSDB Insert Path Profiling ==="
echo "Mode: ${MODE}"
echo "Duration: ${DURATION} seconds"
echo ""

# Check dependencies
if ! command -v perf &> /dev/null; then
    echo "Error: perf not found. Install with: sudo apt-get install linux-tools-common linux-tools-generic"
    exit 1
fi

# Function to run the server and load
run_server_with_load() {
    local server_pid
    local load_pid

    # Start server
    echo "Starting TSDB server..."
    cd ${BUILD_DIR}
    ./bin/tsdb_http_server --port 8086 &
    server_pid=$!
    sleep 2

    if ! kill -0 ${server_pid} 2>/dev/null; then
        echo "Failed to start server"
        return 1
    fi

    # Create simple load generator
    cat > /tmp/simple_load.sh << 'EOF'
#!/bin/bash
duration=$1
end_time=$(($(date +%s) + duration))

echo "Generating load for ${duration} seconds..."
while [ $(date +%s) -lt $end_time ]; do
    curl -s -X POST http://localhost:8086/write \
        -H "Content-Type: application/json" \
        -d '{
            "measurement": "cpu",
            "tags": {"host": "server01", "region": "us-west"},
            "fields": {"usage": '"$(shuf -i 0-100 -n 1)"'.0},
            "timestamp": '"$(date +%s%N)"'
        }' > /dev/null &

    # Send 10 parallel requests
    if [ $(($(date +%s) % 2)) -eq 0 ]; then
        for i in {1..10}; do
            curl -s -X POST http://localhost:8086/write \
                -H "Content-Type: application/json" \
                -d '{
                    "writes": [
                        {"measurement": "metric'$i'", "tags": {"host": "h'$i'"}, "fields": {"value": '$i'.5}, "timestamp": '"$(date +%s%N)"'}
                    ]
                }' > /dev/null &
        done
    fi

    sleep 0.01
done
wait
echo "Load generation complete"
EOF
    chmod +x /tmp/simple_load.sh

    # Start load in background
    /tmp/simple_load.sh ${DURATION} &
    load_pid=$!

    echo ${server_pid} ${load_pid}
}

# Different profiling modes
case ${MODE} in
    cpu)
        echo "CPU profiling mode - tracking CPU cycles and call stacks"

        # Start server and load
        read server_pid load_pid <<< $(run_server_with_load)

        # Profile CPU
        echo "Recording CPU profile..."
        sudo perf record -F 999 -p ${server_pid} -g --call-graph dwarf \
            -o perf_cpu.data -- sleep ${DURATION}

        # Generate flame graph
        sudo perf script -i perf_cpu.data > out_cpu.perf
        ${FLAMEGRAPH_DIR}/stackcollapse-perf.pl out_cpu.perf > out_cpu.folded
        ${FLAMEGRAPH_DIR}/flamegraph.pl --title "TSDB Insert CPU Profile" \
            --colors hot --width 1800 \
            out_cpu.folded > tsdb_cpu_flamegraph.svg

        echo "Generated: tsdb_cpu_flamegraph.svg"
        ;;

    offcpu)
        echo "Off-CPU profiling mode - tracking blocking and waiting"

        # Need BPF for off-CPU profiling
        if [ ! -f /usr/share/bcc/tools/offcputime ]; then
            echo "Error: BCC tools not found. Install with: sudo apt-get install bpfcc-tools"
            exit 1
        fi

        # Start server and load
        read server_pid load_pid <<< $(run_server_with_load)

        # Profile Off-CPU
        echo "Recording Off-CPU profile..."
        sudo /usr/share/bcc/tools/offcputime -df -p ${server_pid} ${DURATION} > offcpu.stacks

        # Generate flame graph
        ${FLAMEGRAPH_DIR}/flamegraph.pl --title "TSDB Insert Off-CPU Profile" \
            --colors blue --countname "us" --width 1800 \
            offcpu.stacks > tsdb_offcpu_flamegraph.svg

        echo "Generated: tsdb_offcpu_flamegraph.svg"
        ;;

    cache)
        echo "Cache profiling mode - tracking L1/L2/L3 cache misses"

        # Start server and load
        read server_pid load_pid <<< $(run_server_with_load)

        # Profile cache events
        echo "Recording cache profile..."
        sudo perf record -e cache-misses,cache-references,L1-dcache-load-misses,L1-dcache-loads \
            -p ${server_pid} -g --call-graph dwarf \
            -o perf_cache.data -- sleep ${DURATION}

        # Generate report
        sudo perf report -i perf_cache.data --stdio > cache_report.txt

        echo "Generated: cache_report.txt"

        # Also create a flame graph for cache misses
        sudo perf script -i perf_cache.data > out_cache.perf
        ${FLAMEGRAPH_DIR}/stackcollapse-perf.pl out_cache.perf > out_cache.folded
        ${FLAMEGRAPH_DIR}/flamegraph.pl --title "TSDB Insert Cache Misses" \
            --colors mem --width 1800 \
            out_cache.folded > tsdb_cache_flamegraph.svg

        echo "Generated: tsdb_cache_flamegraph.svg"
        ;;

    branch)
        echo "Branch profiling mode - tracking branch predictions"

        # Start server and load
        read server_pid load_pid <<< $(run_server_with_load)

        # Profile branch events
        echo "Recording branch profile..."
        sudo perf record -e branch-misses,branch-instructions \
            -p ${server_pid} -g --call-graph dwarf \
            -o perf_branch.data -- sleep ${DURATION}

        # Generate report
        sudo perf report -i perf_branch.data --stdio > branch_report.txt

        echo "Generated: branch_report.txt"
        ;;

    instructions)
        echo "Instruction profiling mode - tracking IPC and instruction counts"

        # Start server and load
        read server_pid load_pid <<< $(run_server_with_load)

        # Profile instruction events
        echo "Recording instruction profile..."
        sudo perf stat -p ${server_pid} \
            -e cycles,instructions,cache-references,cache-misses,branches,branch-misses \
            -d -d -d \
            sleep ${DURATION} 2>&1 | tee instruction_stats.txt

        echo "Generated: instruction_stats.txt"
        ;;

    *)
        echo "Unknown mode: ${MODE}"
        echo "Available modes: cpu, offcpu, cache, branch, instructions"
        exit 1
        ;;
esac

# Cleanup
if [ ! -z "${server_pid}" ]; then
    kill ${server_pid} 2>/dev/null
fi
if [ ! -z "${load_pid}" ]; then
    kill ${load_pid} 2>/dev/null
fi
wait

# Clean temporary files
rm -f /tmp/simple_load.sh out_*.perf out_*.folded perf_*.data perf.data.old

echo ""
echo "=== Profiling Complete ==="
echo "View flame graphs with: firefox tsdb_*_flamegraph.svg"
echo "View reports with: less *_report.txt"