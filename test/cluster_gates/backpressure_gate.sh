#!/bin/bash
# GATE (write-scaleout 3d): the per-shard in-flight write bound must degrade to explicit
# 503 + Retry-After pushback -- never to 500s, never to timeouts -- and throughput must
# recover once concurrency drops.
#
# The budget is driven down via TIMESTAR_CLUSTER_WRITE_INFLIGHT_BYTES; at the 32 MiB
# default the canonical bench never approaches it (~2 MB/shard in flight). The load is the
# canonical single-point array payload, NOT a large {"writes":[...]} batch: 40 concurrent
# 1.3 MB json-batches segfault the node on the pre-Phase-3 binary too, so that shape is a
# separate pre-existing bug and cannot be the recovery vehicle here.
#
# Usage: backpressure_gate.sh [SERVER_BINARY] [BUDGET_BYTES]
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
LIMIT="${2:-2097152}"   # ~6 canonical batches per shard: 24 connections trip it, 4 do not
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
BENCH="$BUILD_DIR/bin/timestar_insert_bench"
[ -x "$BENCH" ] || { echo "no insert bench at $BENCH"; exit 2; }
PORTS="49210 49211 49212"

kill_cluster 492
require_ports_free 49210 49211 49212
for i in 1 2 3; do rm -rf "/tmp/tsgate_bp$i"; mkdir -p "/tmp/tsgate_bp$i"; done
PEERS="127.0.0.1:49210,127.0.0.1:49211,127.0.0.1:49212"
for i in 1 2 3; do
    env TIMESTAR_CLUSTER_WRITE_INFLIGHT_BYTES="$LIMIT" TIMESTAR_DATA_DIR="/tmp/tsgate_bp$i" \
        TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_NODE_ID=$i TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port $((49209 + i)) --smp 4 >"/tmp/tsgate_bp$i/s.log" 2>&1 &
done
trap 'kill_cluster 492' EXIT

wait_balanced "$PORTS" 4096 3 90 || gate_exit

# The effective budget must be LOGGED at startup -- a mis-set value has to be visible in
# the boot log rather than inferred from a wall of 503s.
assert_ge "startup log names the in-flight budget" \
    "$(cat /tmp/tsgate_bp*/s.log | grep -c "cluster write in-flight budget: $LIMIT")" 1

run_bench() { # $1 = connections, $2 = batches
    timeout 300 "$BENCH" --server-port 49210 -c 4 --batches "$2" --batch-size 10000 --verify 0 \
        --warmup 3 --connections "$1" --hosts 1000 --racks 2 2>&1
}
errs_of() { grep -o '[0-9]* HTTP errors' <<<"$1" | head -1 | cut -d' ' -f1; }
oks_of()  { grep -o 'Requests: *[0-9]*' <<<"$1" | head -1 | grep -o '[0-9]*'; }

echo "=== A: 24 connections against a $LIMIT-byte/shard budget (must PUSH BACK) ==="
# The bench reports status TEXT only, so the Retry-After header is probed with small
# single-point writes fired WHILE the bench holds the budget full. Deliberately not a
# burst of large {"writes":[...]} batches: that shape crashes the node (a pre-existing
# defect, reproduced on the pre-Phase-3 binary -- see the plan doc), and a gate must not
# depend on the thing it is not testing.
#
# The probe payload must be big enough to be REFUSED: admission is all-or-nothing on the
# batch's own size, so a single-point write slips into the headroom under a nearly-full
# budget (correctly -- a 60-byte write is not the memory problem).
#
# It is deliberately BYTE-heavy but SERIES-light: 6000 timestamps on FOUR series, not 6000
# distinct ones. A probe with thousands of unique series triggers a metadata/day-bitmap
# storm that kills the node inside CRoaring (`roaring_bitmap_add` dereferencing a failed
# allocation, right after seastar's memory-pressure dump) -- a pre-existing defect in the
# INDEX path, nothing to do with write routing, and not what this gate is testing. Same
# bytes, no index storm.
python3 - >/tmp/tsgate_bp_probe.json <<'PY'
import json
w = [{"measurement": "bpp", "tags": {"host": "h%d" % (i % 4)}, "fields": {"v": float(i)},
      "timestamp": 1700000000000000000 + i * 1000000} for i in range(6000)]
print(json.dumps({"writes": w}))
PY
rm -f /tmp/tsgate_bp_h_*.txt
( run_bench 24 200 >/tmp/tsgate_bp_a.txt 2>&1 ) &
BENCHPID=$!
sleep 4
for k in $(seq 1 20); do
    curl -s -m20 -o /dev/null -D "/tmp/tsgate_bp_h_$k.txt" -X POST http://127.0.0.1:49210/write \
        -H 'Content-Type: application/json' --data-binary @/tmp/tsgate_bp_probe.json >/dev/null 2>&1
done
wait $BENCHPID
rm -f /tmp/tsgate_bp_probe.json
A=$(cat /tmp/tsgate_bp_a.txt)
grep -E "Requests:|First error|Throughput" <<<"$A"
assert_ge "batches rejected under overload" "$(errs_of "$A")" 1
assert_ge "server-side 'shard write buffer full' rejections" \
    "$(cat /tmp/tsgate_bp*/s.log | grep -c 'shard write buffer full')" 1
assert_eq "server-side 500s under overload" \
    "$(cat /tmp/tsgate_bp*/s.log | grep -c 'Error handling write request')" 0

N503=$(grep -l ' 503 ' /tmp/tsgate_bp_h_*.txt 2>/dev/null | wc -l)
NRETRY=$(grep -il '^retry-after:' /tmp/tsgate_bp_h_*.txt 2>/dev/null | wc -l)
N500=$(grep -l ' 500 ' /tmp/tsgate_bp_h_*.txt 2>/dev/null | wc -l)
echo "  header probe during overload: $N503 x 503, $NRETRY carrying Retry-After, $N500 x 500"
assert_ge "503 responses observed while the bound was tripped" "$N503" 1
assert_eq "503s WITHOUT Retry-After" "$((N503 - NRETRY))" 0
assert_eq "500 responses in the probe" "$N500" 0
rm -f /tmp/tsgate_bp_h_*.txt /tmp/tsgate_bp_a.txt

echo "=== B: same cluster and budget, 4 connections (must RECOVER) ==="
B=$(run_bench 4 200)
grep -E "Requests:|First error|Throughput" <<<"$B"
assert_eq "client HTTP errors after the load drops" "$(errs_of "$B")" 0
assert_ge "batches accepted after the load drops" "$(oks_of "$B")" 200

echo "=== C: a single write still succeeds ==="
CODE=$(curl -s -m10 -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:49210/write \
    -H 'Content-Type: application/json' \
    -d '{"measurement":"bp2","tags":{"host":"x"},"fields":{"v":1.5},"timestamp":1700000900000000000}')
assert_eq "single write status" "$CODE" 200
assert_eq "node crashes" "$(grep -l 'Segmentation fault' /tmp/tsgate_bp*/s.log 2>/dev/null | wc -l)" 0

gate_exit
