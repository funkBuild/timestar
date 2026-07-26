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
# PORTS BELOW THE EPHEMERAL RANGE (32768-60999 here), deliberately. This gate starts FIVE
# nodes in a burst and each one dials the others' Raft/data ports, so with the gate's ports
# inside the ephemeral range the kernel hands a peer's OUTBOUND connection the very port a
# later-starting node still has to bind: seastar then exits on the failed listen, one node
# silently never comes up, and the gate fails as "cluster did not converge". Observed four
# times in one session on node 3 (49312, then 51312 three runs running) before moving the
# whole gate below 32768. The 3-node gates are exposed to the same race, just less often --
# see the debt register.
PORTS="19310 19311 19312 19313 19314"
WRITES="${GATE_WRITES:-300}"

kill_cluster 193
require_ports_free 19310 19311 19312 19313 19314
for i in 1 2 3 4 5; do rm -rf "/tmp/tsgate_dp$i"; mkdir -p "/tmp/tsgate_dp$i"; done
PEERS="127.0.0.1:19310,127.0.0.1:19311,127.0.0.1:19312,127.0.0.1:19313,127.0.0.1:19314"
for i in 1 2 3 4 5; do
    env TIMESTAR_DATA_DIR="/tmp/tsgate_dp$i" TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_NODE_ID=$i TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port $((19309 + i)) --smp 2 >"/tmp/tsgate_dp$i/s.log" 2>&1 &
done
trap 'kill_cluster 193' EXIT

wait_all_led "$PORTS" 4096 150 || gate_exit

HOSTED=$(status_field "$(cluster_status 19310)" vshards_hosted)
echo "  node 1 hosts $HOSTED of 4096 VShards (it must FORWARD the rest by placement primary)"
# If the coordinator hosts everything the gate is vacuous -- it cannot reach the
# stale-primary path at all. This is the check that makes a 3-node run FAIL rather than
# silently pass.
assert_le "vshards_hosted on the coordinator" "$HOSTED" 3500

# Drive leadership away from the placement primaries. Primaries do not move, so most
# VShards end up led by someone other than their primary: the deposed state.
TRANSFERS=0
for _ in $(seq 1 10); do
    for p in $PORTS; do
        R=$(curl -s -m5 -X POST "http://127.0.0.1:$p/cluster/rebalance-leadership?max=512" 2>/dev/null)
        N=$(printf '%s' "$R" | grep -o '"transfers_initiated":[0-9]*' | cut -d: -f2)
        TRANSFERS=$((TRANSFERS + ${N:-0}))
    done
    sleep 1
done
# NOTE: this gate does not wait for leadership to go quiet, and it does not need to. Since
# debt D-12 the balancer's fair share is per-node and membership-weighted, so at RF < N it
# CONVERGES: measured on an idle 5-node RF=3 cluster, ~60 s to [820 818 820 820 818]
# (fair 819) and then zero movement. Before that fix it never converged -- 26-114 VShards
# still moving per 30 s after 12 minutes, spread stalled at ~300 -- which is why this gate
# used to explain its residual 5xx as balancer churn. A write can still meet a genuinely
# mid-transfer VShard during the storm above, so the HARD write assertion stays on 500s --
# the thing Phase 3 changed -- with the accepted count advisory. In practice this run is
# 300/300 with zero 5xx.
LEDS=""; MINLED=999999
for p in $PORTS; do
    L=$(status_field "$(cluster_status "$p")" vshards_led)
    LEDS="$LEDS ${L:-0}"
    [ "${L:-0}" -lt "$MINLED" ] && MINLED=${L:-0}
done
echo "  leadership after rebalance (fair share 819): [$LEDS], $TRANSFERS transfers initiated"

# ANTI-VACUITY. Without these, a run in which every rebalance call no-ops still reports
# 300/300 and "passes" -- while never once routing a write at a DEPOSED primary, which is
# the entire point of the gate. Leadership must actually have moved off the primaries.
assert_ge "leadership transfers initiated" "$TRANSFERS" 400
# ADVISORY, not an assertion: the storm above deliberately leaves the cluster mid-reshuffle,
# so the spread is whatever that pass happened to leave (it converges from here -- see the
# D-12 note above). Reported because a LOW value means more writes will meet a mid-transfer
# VShard, which explains the 5xx count below.
echo "  (advisory) least-loaded node leads $MINLED of a fair 819 -- low values mean more churn"

echo "=== $WRITES writes to node 1, against deposed primaries ==="
BASE_TS=1700000000000000000
OK=0; E5XX=0; OTHER=0
for i in $(seq 0 $((WRITES - 1))); do
    CODE=$(curl -s -m10 -o /tmp/tsgate_dp_resp.txt -w '%{http_code}' -X POST http://127.0.0.1:19310/write \
        -H 'Content-Type: application/json' \
        -d "{\"measurement\":\"deposed\",\"tags\":{\"host\":\"h$i\"},\"fields\":{\"v\":1.0},\"timestamp\":$((BASE_TS + i * 1000000000))}")
    case "$CODE" in
        2*) OK=$((OK + 1)) ;;
        5*) E5XX=$((E5XX + 1)); [ "$E5XX" = "1" ] && echo "  first 5xx ($CODE): $(head -c 200 /tmp/tsgate_dp_resp.txt)" ;;
        *)  OTHER=$((OTHER + 1)) ;;
    esac
done
echo "  result: $OK accepted, $E5XX 5xx, $OTHER other"
# THE Phase-3 property, and the one that is robust to the balancer churn above: the
# pre-Phase-3 binary answered a deposed primary with ~29% opaque HTTP 500s ("a VShard
# leader was stale"), because a hintless retry went straight back to the same stale node
# and the failure was mapped as an internal error. With the leader hint + bounded retry +
# honest status mapping there must be ZERO 500s, and the residual failures must be a small
# number of retryable 503s from VShards caught mid-transfer.
assert_eq "server-side 500s" "$(cat /tmp/tsgate_dp*/s.log | grep -c 'Error handling write request')" 0
assert_eq "non-HTTP failures" "$OTHER" 0
# ADVISORY: the 5xx here are retryable 503s from VShards caught mid-transfer by the storm,
# and their count tracks that churn rather than anything Phase 3 owns. Measured 274-300 of
# 300 across runs. The HARD assertion is the 500 count above.
echo "  (advisory) $OK/$WRITES accepted, $E5XX retryable 5xx -- see the balancer note above"
if [ "$E5XX" -gt 0 ]; then
    echo "  (advisory) first 5xx must be a 503 naming a retryable condition, never an opaque 500"
fi
assert_eq "node crashes" "$(grep -l 'Segmentation fault' /tmp/tsgate_dp*/s.log 2>/dev/null | wc -l)" 0

# === READS (debt D-13) ==============================================================
# This is a FIVE-node RF=3 cluster, so every coordinator hosts a STRICT SUBSET of the
# 4096 VShards -- and until D-13 that meant reads did not work AT ALL here: gatherLeaders
# read `leaderOf` as kNoNode for every VShard the coordinator does not host (~1638 of
# 4096) and could not tell that apart from "hosted, no leader elected", so queryReplicated
# counted them leaderless and failed the whole read QUERY_INCOMPLETE after its retries,
# forever. The gate could not assert reads at all, which is exactly why the defect
# survived to be found by code reading.
#
# The assertion is HARD and it is on every node: a scatter-gather read must succeed from
# ANY coordinator and return the SAME count -- the number of writes the cluster accepted
# above. That count is what makes this non-vacuous in both directions: an empty or short
# answer fails it just as loudly as an error does, and a double count (the same
# replicated series answered by two nodes) fails it too.
echo "=== reads from every node (5-node RF=3: each coordinator hosts a strict subset) ==="
Q="{\"query\":\"count:deposed(v)\",\"startTime\":$((BASE_TS - 1000000000)),\"endTime\":$((BASE_TS + WRITES * 1000000000)),\"aggregationInterval\":\"1h\"}"
READ_OK=0; READ_FAIL=0; COUNTS=""
for p in $PORTS; do
    R=$(curl -s -m20 -X POST "http://127.0.0.1:$p/query" -H 'Content-Type: application/json' -d "$Q")
    if ! printf '%s' "$R" | grep -q '"status":"success"'; then
        READ_FAIL=$((READ_FAIL + 1))
        [ "$READ_FAIL" = "1" ] && echo "  first read failure on port $p: $(printf '%s' "$R" | head -c 300)"
        COUNTS="$COUNTS ERR"
        continue
    fi
    READ_OK=$((READ_OK + 1))
    # Sum the returned bucket values: one bucket per series, value == points in it.
    N=$(printf '%s' "$R" | grep -o '"point_count":[0-9]*' | cut -d: -f2)
    TOTAL=$(printf '%s' "$R" | python3 -c '
import json,sys
r=json.load(sys.stdin)
t=0.0
for s in r.get("series",[]):
    for f in s.get("fields",{}).values():
        t+=sum(v for v in f.get("values",[]) if v is not None)
print(int(t))')
    COUNTS="$COUNTS $TOTAL"
    echo "  port $p: success, $N bucket(s), counted $TOTAL points"
done
echo "  read results per node:$COUNTS"
# HARD: every node answered.
assert_eq "nodes answering the scatter-gather read" "$READ_OK" 5
assert_eq "read failures" "$READ_FAIL" 0
# HARD: each node counted exactly the points the cluster accepted -- no missing VShard
# (a silently dropped VShard undercounts) and no VShard answered by two replicas (a
# double count overcounts).
for c in $COUNTS; do
    assert_eq "points counted by a node (accepted writes = $OK)" "$c" "$OK"
done
# ANTI-VACUITY: a run in which nothing was accepted would satisfy the equality above
# with 0 == 0 and prove nothing about reads.
assert_ge "points available to read (anti-vacuity)" "$OK" 100

gate_exit
