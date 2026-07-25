#!/bin/bash
# GATE (write-scaleout Phase 3): a rolling leadership rebalance under sustained writes
# must cost LATENCY, not client errors.
#
# Node 3 starts LAST, so nodes 1-2 hold ~2048 leaderships each against a fair share of
# 1365 and every rebalance call has real work to do. This matters: /cluster/rebalance-
# leadership only sheds leadership held ABOVE fair share, so storming an already-balanced
# cluster initiates ZERO transfers and the gate proves nothing. `transfers_initiated` is
# summed and ASSERTED for exactly that reason -- a vacuous run FAILS.
#
# Usage: rolling_rebalance_gate.sh [SERVER_BINARY]
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
BENCH="$BUILD_DIR/bin/timestar_insert_bench"
[ -x "$BENCH" ] || { echo "no insert bench at $BENCH"; exit 2; }
PORTS="49210 49211 49212"
MIN_TRANSFERS="${GATE_MIN_TRANSFERS:-200}"

kill_cluster 492
require_ports_free 49210 49211 49212
for i in 1 2 3; do rm -rf "/tmp/tsgate_rr$i"; mkdir -p "/tmp/tsgate_rr$i"; done
PEERS="127.0.0.1:49210,127.0.0.1:49211,127.0.0.1:49212"
start_node() {
    env TIMESTAR_DATA_DIR="/tmp/tsgate_rr$1" TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_NODE_ID=$1 TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port $((49209 + $1)) --smp 4 >>"/tmp/tsgate_rr$1/s.log" 2>&1 &
}
trap 'kill_cluster 492' EXIT

# Two-node phase: a 2-of-3 quorum elects leaders for every VShard between nodes 1 and 2.
start_node 1; start_node 2
for _ in $(seq 1 90); do
    sleep 2
    A=$(status_field "$(cluster_status 49210)" vshards_led)
    B=$(status_field "$(cluster_status 49211)" vshards_led)
    X=$(status_field "$(cluster_status 49210)" vshards_leaderless)
    [ -z "$A" ] && continue
    [ "$(( ${A:-0} + ${B:-0} ))" -ge 4080 ] && [ "${X:-9999}" -le 8 ] && break
done
echo "  before node 3 joins: led=[$(status_field "$(cluster_status 49210)" vshards_led) $(status_field "$(cluster_status 49211)" vshards_led)] (fair share 1365)"

start_node 3
sleep 8
echo "  node 3 joined with ~no leadership: led=[$(status_field "$(cluster_status 49210)" vshards_led) $(status_field "$(cluster_status 49211)" vshards_led) $(status_field "$(cluster_status 49212)" vshards_led)]"

echo "=== rebalance storm under sustained writes ==="
( timeout 300 "$BENCH" --server-port 49210 -c 4 --batches 600 --batch-size 10000 --verify 0 \
    --warmup 5 --connections 8 --hosts 1000 --racks 2 >/tmp/tsgate_rr_bench.txt 2>&1 ) &
BENCHPID=$!
sleep 2
TRANSFERS=0; CALLS=0
while kill -0 $BENCHPID 2>/dev/null; do
    for p in $PORTS; do
        R=$(curl -s -m5 -X POST "http://127.0.0.1:$p/cluster/rebalance-leadership?max=512" 2>/dev/null)
        N=$(printf '%s' "$R" | grep -o '"transfers_initiated":[0-9]*' | cut -d: -f2)
        TRANSFERS=$((TRANSFERS + ${N:-0})); CALLS=$((CALLS + 1))
    done
    sleep 0.2
done
wait $BENCHPID
echo "  rebalance calls: $CALLS"
grep -E "Requests:|First error|Throughput|batch latency" /tmp/tsgate_rr_bench.txt
echo "  final led=[$(status_field "$(cluster_status 49210)" vshards_led) $(status_field "$(cluster_status 49211)" vshards_led) $(status_field "$(cluster_status 49212)" vshards_led)]"

# THE anti-vacuity assertion: without real transfers this gate tests nothing.
assert_ge "leadership transfers initiated mid-bench" "$TRANSFERS" "$MIN_TRANSFERS"

HTTP_ERRS=$(grep -o '[0-9]* HTTP errors' /tmp/tsgate_rr_bench.txt | head -1 | cut -d' ' -f1)
CONN_FAILS=$(grep -o '[0-9]* connection failures' /tmp/tsgate_rr_bench.txt | head -1 | cut -d' ' -f1)
assert_eq "client HTTP errors" "${HTTP_ERRS:-missing}" 0
assert_eq "client connection failures" "${CONN_FAILS:-missing}" 0
assert_eq "server-side 500s" "$(cat /tmp/tsgate_rr*/s.log | grep -c 'Error handling write request')" 0
assert_eq "node crashes" "$(grep -l 'Segmentation fault' /tmp/tsgate_rr*/s.log 2>/dev/null | wc -l)" 0

# The cluster must go QUIET once the storm stops. At RF == N the balancer converges, so
# leadership settling is a real property -- and it is the one that catches a transfer
# mechanism that has become slow or lossy. A CheckQuorum-style regression (transfers
# needing a full election timeout, each with a leaderless window) shows up here as
# leadership still moving in bulk long after the last rebalance call.
# (registers its own gate_fail if leadership never stops moving)
wait_leadership_settled "$PORTS" 40

gate_exit
