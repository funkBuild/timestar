#!/bin/bash
# GATE (write-scaleout 3a/3b): the DEPOSED PRIMARY -- a node that is alive and hosts a
# VShard's group but no longer LEADS it, with no placement change. Writes routed to it
# must succeed via the leader hint + retry, not spin until the budget is spent.
#
# FIVE nodes at RF=3, deliberately: at RF=3 on three nodes every node hosts every group,
# so the router's LeaderResolver always knows the real leader locally and this path is
# UNREACHABLE -- a 3-node run passes on a binary with no leader hint at all. With five
# nodes a coordinator hosts only ~3/5 of the VShards and falls back to the PLACEMENT
# PRIMARY for the rest; once leadership balances, most of those primaries are deposed.
#
# Measured on this gate: pre-Phase-3 207-216/300 accepted (~29% HTTP 500); with 3a+3b,
# 300/300 and 0 5xx.
#
# Usage: deposed_primary_gate.sh [SERVER_BINARY]
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
PORTS="49310 49311 49312 49313 49314"
WRITES="${GATE_WRITES:-300}"

kill_cluster 493
require_ports_free 49310 49311 49312 49313 49314
for i in 1 2 3 4 5; do rm -rf "/tmp/tsgate_dp$i"; mkdir -p "/tmp/tsgate_dp$i"; done
PEERS="127.0.0.1:49310,127.0.0.1:49311,127.0.0.1:49312,127.0.0.1:49313,127.0.0.1:49314"
for i in 1 2 3 4 5; do
    env TIMESTAR_DATA_DIR="/tmp/tsgate_dp$i" TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_NODE_ID=$i TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port $((49309 + i)) --smp 2 >"/tmp/tsgate_dp$i/s.log" 2>&1 &
done
trap 'kill_cluster 493' EXIT

wait_all_led "$PORTS" 4096 150 || gate_exit

HOSTED=$(status_field "$(cluster_status 49310)" vshards_hosted)
echo "  node 1 hosts $HOSTED of 4096 VShards (it must FORWARD the rest by placement primary)"
# If the coordinator hosts everything the gate is vacuous -- it cannot reach the
# stale-primary path at all. This is the check that makes a 3-node run FAIL rather than
# silently pass.
assert_le "vshards_hosted on the coordinator" "$HOSTED" 3500

# Drive leadership away from the placement primaries. Primaries do not move, so most
# VShards end up led by someone other than their primary: the deposed state.
for _ in $(seq 1 10); do
    for p in $PORTS; do
        curl -s -m5 -X POST "http://127.0.0.1:$p/cluster/rebalance-leadership?max=512" >/dev/null 2>&1
    done
    sleep 1
done
sleep 3
LEDS=""; for p in $PORTS; do LEDS="$LEDS $(status_field "$(cluster_status "$p")" vshards_led)"; done
echo "  leadership after rebalance (fair share 819): [$LEDS]"

echo "=== $WRITES writes to node 1, against deposed primaries ==="
BASE_TS=1700000000000000000
OK=0; E5XX=0; OTHER=0
for i in $(seq 0 $((WRITES - 1))); do
    CODE=$(curl -s -m10 -o /tmp/tsgate_dp_resp.txt -w '%{http_code}' -X POST http://127.0.0.1:49310/write \
        -H 'Content-Type: application/json' \
        -d "{\"measurement\":\"deposed\",\"tags\":{\"host\":\"h$i\"},\"fields\":{\"v\":1.0},\"timestamp\":$((BASE_TS + i * 1000000000))}")
    case "$CODE" in
        2*) OK=$((OK + 1)) ;;
        5*) E5XX=$((E5XX + 1)); [ "$E5XX" = "1" ] && echo "  first 5xx ($CODE): $(head -c 200 /tmp/tsgate_dp_resp.txt)" ;;
        *)  OTHER=$((OTHER + 1)) ;;
    esac
done
echo "  result: $OK accepted, $E5XX 5xx, $OTHER other"
assert_eq "writes accepted" "$OK" "$WRITES"
assert_eq "5xx responses" "$E5XX" 0
assert_eq "non-HTTP failures" "$OTHER" 0
assert_eq "server-side 500s" "$(cat /tmp/tsgate_dp*/s.log | grep -c 'Error handling write request')" 0
assert_eq "node crashes" "$(grep -l 'Segmentation fault' /tmp/tsgate_dp*/s.log 2>/dev/null | wc -l)" 0

gate_exit
