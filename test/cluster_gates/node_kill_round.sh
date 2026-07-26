#!/bin/bash
# MEASUREMENT + GATE (debt D-14): `kill -9` of one node MID-BENCH on a 3-node RF=3
# cluster. This is HA round (a) from the plan doc, scripted -- it was a hand-run
# procedure, which is why its 503 band ("33/200", "137/400", "333/900") could only ever
# be quoted, never re-measured.
#
# WHAT IT MEASURES. A 2-of-3 quorum survives one node leaving; what does not survive is
# the LEADERSHIP that node held (~1/3 of 4096 groups). Each of those groups needs a fresh
# election, and a batch fails if ANY of its ~1000 slices misses the write deadline, so the
# client-visible error rate during the outage is the per-VShard unavailability multiplied
# by batch fan-out. The number this prints IS that band.
#
# WHAT IT HARD-ASSERTS (the contract, which is not the band):
#   * zero server-side 500s and zero crashes -- every failure is a retryable 503;
#   * no acknowledged loss: every probe write ACKED while the node was down is readable
#     afterwards, on the surviving nodes;
#   * anti-vacuity: the bench must actually have been running when the kill landed, and
#     enough batches must have been accepted for the run to mean anything.
#
# The 503 count is REPORTED, not asserted, for the same reason every other gate treats it
# as advisory: it is run-to-run variable and it moves with machine speed. Compare two
# binaries by running this against each, in the same session, on an idle box.
#
# Usage: node_kill_round.sh [SERVER_BINARY]
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
BENCH="$BUILD_DIR/bin/timestar_insert_bench"
[ -x "$BENCH" ] || { echo "no insert bench at $BENCH"; exit 2; }
PORTS="49610 49611 49612"
BATCHES="${GATE_BATCHES:-400}"
PROBES="${GATE_PROBES:-50}"

FREE_GB=$(df -BG --output=avail /tmp | tail -1 | tr -dc '0-9')
if [ "${FREE_GB:-0}" -lt 20 ]; then
    echo "ABORT: only ${FREE_GB}G free on /tmp; this round needs ~20G (see the plan doc's MEASUREMENT HAZARD)" >&2
    exit 2
fi

kill_cluster 496
require_ports_free $PORTS
for i in 1 2 3; do rm -rf "/tmp/tsgate_nk$i"; mkdir -p "/tmp/tsgate_nk$i"; done
PEERS="127.0.0.1:49610,127.0.0.1:49611,127.0.0.1:49612"
start_node() {
    env TIMESTAR_DATA_DIR="/tmp/tsgate_nk$1" TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_NODE_ID=$1 TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port $((49609 + $1)) --smp 4 >>"/tmp/tsgate_nk$1/s.log" 2>&1 &
}
trap 'kill_cluster 496' EXIT

for i in 1 2 3; do start_node $i; done
wait_all_led "$PORTS" 4096 120 || gate_exit
# Balance leadership first. The first node to start wins every election, so on a fresh
# cluster node 3 may lead almost nothing -- and killing a node that leads nothing measures
# nothing. This is the same anti-vacuity the fault-injection gate needs and for the same
# reason.
for _ in $(seq 1 12); do
    for p in $PORTS; do curl -s -m5 -X POST "http://127.0.0.1:$p/cluster/rebalance-leadership?max=512" >/dev/null 2>&1; done
    sleep 2
done
wait_leadership_settled "$PORTS" 40 || gate_exit
LED3=$(status_field "$(cluster_status 49612)" vshards_led)
echo "  node 3 leads $LED3 of 4096 before the kill"
assert_ge "leadership on the node about to be killed (anti-vacuity)" "${LED3:-0}" 800

echo "=== bench $BATCHES x 10k against node 1; kill -9 node 3 at t+3s ==="
"$BENCH" --server-port 49610 -c 4 --batches "$BATCHES" --batch-size 10000 --verify 0 \
    --warmup 5 --connections 8 --hosts 1000 --racks 2 >/tmp/tsgate_nk_bench.txt 2>&1 &
BENCH_PID=$!
sleep 3
# The bench must still be running when the kill lands, or the round measures a healthy
# cluster and reports a triumphant zero.
if ! kill -0 "$BENCH_PID" 2>/dev/null; then
    gate_fail "the bench finished before the kill landed -- the round is vacuous (raise GATE_BATCHES)"
    gate_exit
fi
pkill -u "$(id -u)" -9 -f -- "--port 49612"
KILL_T=$(date +%s)
echo "  node 3 killed at t+3s"

# Probe writes DURING the outage, against a surviving node: these are the ones whose acks
# the no-loss assertion is about.
BASE_TS=1700000000000000000
PROBE_OK=0; PROBE_5XX=0
for i in $(seq 0 $((PROBES - 1))); do
    CODE=$(curl -s -m10 -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:49610/write \
        -H 'Content-Type: application/json' \
        -d "{\"measurement\":\"nkprobe\",\"tags\":{\"p\":\"p$i\"},\"fields\":{\"v\":1.0},\"timestamp\":$((BASE_TS + i * 1000000000))}")
    case "$CODE" in
        2*) PROBE_OK=$((PROBE_OK + 1)) ;;
        5*) PROBE_5XX=$((PROBE_5XX + 1)) ;;
    esac
done
wait $BENCH_PID 2>/dev/null
BENCH_END=$(date +%s)
grep -E "Requests:|Throughput" /tmp/tsgate_nk_bench.txt | sed 's/^/  /'
OK_REQS=$(grep -oE '[0-9]+ OK' /tmp/tsgate_nk_bench.txt | head -1 | cut -d' ' -f1)
HTTP_ERRS=$(grep -o '[0-9]* HTTP errors' /tmp/tsgate_nk_bench.txt | head -1 | cut -d' ' -f1)

echo "  === D-14 BAND: ${HTTP_ERRS:-?} of $BATCHES bench batches failed (${OK_REQS:-?} OK) during a $((BENCH_END - KILL_T))s post-kill window ==="
echo "  probe writes during the outage: $PROBE_OK/$PROBES acked, $PROBE_5XX 5xx"
echo "  failure kinds seen by the bench:"
grep -oE '\(last: [a-z-]+\)' /tmp/tsgate_nk_bench.txt | sort | uniq -c | sed 's/^/    /'

# The contract, hard-asserted.
assert_eq "server-side 500s" "$(cat /tmp/tsgate_nk*/s.log | grep -c 'Error handling write request')" 0
assert_eq "node crashes" "$(grep -l 'Segmentation fault' /tmp/tsgate_nk*/s.log 2>/dev/null | wc -l)" 0
assert_ge "batches accepted (anti-vacuity)" "${OK_REQS:-0}" "$((BATCHES / 4))"

# NO ACKNOWLEDGED LOSS: every probe write that was ACKED while the node was down must be
# readable from both survivors.
#
# ANTI-VACUITY FIRST: `readable == PROBE_OK` passes trivially at 0 == 0, i.e. a run in which
# every probe FAILED would report "no loss" having stored nothing. The floor makes the
# assertion mean something. Measured on the fixed binary: 42 of 50 acked (the rest are the
# honest bounded 503s of the failover window), so 10 leaves plenty of room for a slower box
# while still failing a run that acked essentially nothing.
assert_ge "probe writes acked during the outage (anti-vacuity)" "$PROBE_OK" 10
sleep 3
for p in 49610 49611; do
    R=$(curl -s -m20 -X POST "http://127.0.0.1:$p/query" -H 'Content-Type: application/json' \
        -d "{\"query\":\"count:nkprobe(v)\",\"startTime\":$((BASE_TS - 1000000000)),\"endTime\":$((BASE_TS + PROBES * 1000000000)),\"aggregationInterval\":\"1h\"}")
    # A FAILED read must not be scored as "no data": that is the whole QUERY_INCOMPLETE
    # contract, and an earlier version of this script counted an error response as 0 points
    # and reported it as data loss.
    if ! printf '%s' "$R" | grep -q '"status":"success"'; then
        gate_fail "read on node at $p did not succeed: $(printf '%s' "$R" | head -c 200)"
        continue
    fi
    N=$(printf '%s' "$R" | python3 -c '
import json,sys
r=json.load(sys.stdin)
t=0.0
for s in r.get("series",[]):
    for f in s.get("fields",{}).values():
        t+=sum(v for v in f.get("values",[]) if v is not None)
print(int(t))')
    assert_eq "acked probe points readable on node at $p" "$N" "$PROBE_OK"
done

gate_exit
