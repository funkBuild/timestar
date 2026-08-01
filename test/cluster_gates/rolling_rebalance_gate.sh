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
PORTS="19220 19221 19222"
MIN_TRANSFERS="${GATE_MIN_TRANSFERS:-200}"

kill_cluster 1922
require_ports_free 19220 19221 19222
fresh_gate_data_dirs /tmp/tsgate_rb1 /tmp/tsgate_rb2 /tmp/tsgate_rb3 || exit 2
PEERS="127.0.0.1:19220,127.0.0.1:19221,127.0.0.1:19222"
start_node() {
    env $GATE_SERVER_ENV TIMESTAR_DATA_DIR="/tmp/tsgate_rb$1" TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_UUID=00112233445566778899aabbccddeeff TIMESTAR_CLUSTER_DEVELOPMENT_ALLOW_INSECURE_TRANSPORT=true TIMESTAR_CLUSTER_NODE_ID=$1 TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port $((19219 + $1)) --smp 4 --memory "$GATE_SERVER_MEMORY" >>"/tmp/tsgate_rb$1/s.log" 2>&1 &
}
trap 'gate_cleanup 1922 /tmp/tsgate_rb1 /tmp/tsgate_rb2 /tmp/tsgate_rb3' EXIT

# Two-node phase: a 2-of-3 quorum elects leaders for every VShard between nodes 1 and 2.
start_node 1; start_node 2
for _ in $(seq 1 90); do
    sleep 2
    A=$(status_field "$(cluster_status 19220)" vshards_led)
    B=$(status_field "$(cluster_status 19221)" vshards_led)
    X=$(status_field "$(cluster_status 19220)" vshards_leaderless)
    [ -z "$A" ] && continue
    [ "$(( ${A:-0} + ${B:-0} ))" -ge 4080 ] && [ "${X:-9999}" -le 8 ] && break
done
echo "  before node 3 joins: led=[$(status_field "$(cluster_status 19220)" vshards_led) $(status_field "$(cluster_status 19221)" vshards_led)] (fair share 1365)"

start_node 3
sleep 8
echo "  node 3 joined with ~no leadership: led=[$(status_field "$(cluster_status 19220)" vshards_led) $(status_field "$(cluster_status 19221)" vshards_led) $(status_field "$(cluster_status 19222)" vshards_led)]"

echo "=== rebalance storm under sustained writes ==="
( timeout 300 "$BENCH" --server-port 19220 -c 4 --batches 600 --batch-size 10000 --verify 0 \
    --warmup 5 --connections 8 --hosts 1000 --racks 2 >/tmp/tsgate_rb_bench.txt 2>&1 ) &
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
grep -E "Requests:|First error|Throughput|batch latency" /tmp/tsgate_rb_bench.txt
echo "  final led=[$(status_field "$(cluster_status 19220)" vshards_led) $(status_field "$(cluster_status 19221)" vshards_led) $(status_field "$(cluster_status 19222)" vshards_led)]"

# THE anti-vacuity assertion: without real transfers this gate tests nothing.
assert_ge "leadership transfers initiated mid-bench" "$TRANSFERS" "$MIN_TRANSFERS"

HTTP_ERRS=$(grep -o '[0-9]* HTTP errors' /tmp/tsgate_rb_bench.txt | head -1 | cut -d' ' -f1)
CONN_FAILS=$(grep -o '[0-9]* connection failures' /tmp/tsgate_rb_bench.txt | head -1 | cut -d' ' -f1)
assert_eq "client HTTP errors" "${HTTP_ERRS:-missing}" 0
assert_eq "client connection failures" "${CONN_FAILS:-missing}" 0
assert_eq "server-side 500s" "$(cat /tmp/tsgate_rb*/s.log | grep -c 'Error handling write request')" 0
assert_eq "node crashes" "$(grep -l 'Segmentation fault' /tmp/tsgate_rb*/s.log 2>/dev/null | wc -l)" 0

# The cluster must go QUIET once the storm stops. At RF == N the balancer converges, so
# leadership settling is a real property -- and it is the one that catches a transfer
# mechanism that has become slow or lossy. A CheckQuorum-style regression (transfers
# needing a full election timeout, each with a leaderless window) shows up here as
# leadership still moving in bulk long after the last rebalance call.
# (registers its own gate_fail if leadership never stops moving)
wait_leadership_settled "$PORTS" 40

gate_exit
