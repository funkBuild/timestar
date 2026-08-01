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
LIMIT="${2:-1000000}"   # ~3 canonical batches per shard: 12 connections trip it, 1 does not
#
# The budget must be small enough that ONE extra concurrent request exceeds it, because a
# single HTTP request cannot trip a large one: the JSON batch size is itself capped by the
# handler (a 160k-entry batch is a 400, not a 503), and a multi-series batch splits its
# charge across shards. A 2 MiB budget is therefore only reachable by many-way concurrency,
# which cannot be aimed at a particular shard from curl.
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
BENCH="$BUILD_DIR/bin/timestar_insert_bench"
[ -x "$BENCH" ] || { echo "no insert bench at $BENCH"; exit 2; }
PORTS="19210 19211 19212"

kill_cluster 1921
require_ports_free 19210 19211 19212
fresh_gate_data_dirs /tmp/tsgate_bp1 /tmp/tsgate_bp2 /tmp/tsgate_bp3 || exit 2
PEERS="127.0.0.1:19210,127.0.0.1:19211,127.0.0.1:19212"
for i in 1 2 3; do
    env $GATE_SERVER_ENV TIMESTAR_CLUSTER_WRITE_INFLIGHT_BYTES="$LIMIT" TIMESTAR_DATA_DIR="/tmp/tsgate_bp$i" \
        TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_UUID=00112233445566778899aabbccddeeff TIMESTAR_CLUSTER_DEVELOPMENT_ALLOW_INSECURE_TRANSPORT=true TIMESTAR_CLUSTER_NODE_ID=$i TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port $((19209 + i)) --smp 4 >"/tmp/tsgate_bp$i/s.log" 2>&1 &
done
trap 'gate_cleanup 1921 /tmp/tsgate_bp1 /tmp/tsgate_bp2 /tmp/tsgate_bp3' EXIT

wait_balanced "$PORTS" 4096 3 90 || gate_exit
wait_healthy "$PORTS" 60 || gate_exit

# The effective budget must be LOGGED at startup -- a mis-set value has to be visible in
# the boot log rather than inferred from a wall of 503s.
assert_ge "startup log names the in-flight budget" \
    "$(cat /tmp/tsgate_bp*/s.log | grep -c "cluster write in-flight budget: $LIMIT")" 1

run_bench() { # $1 = connections, $2 = batches, $3 = persistent transcript
    timeout 300 "$BENCH" --server-port 19210 -c 4 --batches "$2" --batch-size 10000 --verify 0 \
        --warmup 3 --connections "$1" --hosts 1000 --racks 2 >"$3" 2>&1
    BENCH_RC=$?
}
errs_of() { grep -o '[0-9]* HTTP errors' <<<"$1" | head -1 | cut -d' ' -f1; }
oks_of()  { grep -o 'Requests: *[0-9]*' <<<"$1" | head -1 | grep -o '[0-9]*'; }

echo "=== A0: header shape of a rejection (deterministic, no bench) ==="
# Admission is all-or-nothing and admits any batch on an IDLE shard, so a rejection is
# forced by CONCURRENCY, not by size: several probes at once, one admitted, the rest
# refused.
#
# SIXTEEN of them, deliberately: the budget is charged on the REQUEST shard (the one the
# HTTP connection landed on), not on the owning shard, so four probes across four shards
# never meet. With four shards, sixteen concurrent requests collide by pigeonhole.
#
# ONE series each, deliberately. The charge follows the series to ITS owning shard, so a
# multi-series probe splits its bytes across shards and each shard sees only a fraction --
# a 4-series 40k-point probe charged ~160 KB per shard against a 2 MiB bound and was
# (correctly) admitted, which is what made an earlier version of this check silently pass
# nothing. A single series puts the whole ~2.6 MB on one shard. Size against the RESIDENT
# estimate the bound is charged in (~16 B/point plus the key), not the JSON body -- and
# under the handler's own batch-entry cap, which rejects an over-large request with 400
# long before admission sees it.
#
# BYTE-heavy but SERIES-light on purpose: a probe with thousands of distinct series
# triggers a metadata/day-bitmap storm that kills the node inside CRoaring
# (`roaring_bitmap_add` dereferencing a failed allocation, right after seastar's
# memory-pressure dump) -- a pre-existing defect in the INDEX path, unrelated to write
# routing, and not what this gate tests.
python3 - >/tmp/tsgate_bp_probe.json <<'PY'
import json
w = [{"measurement": "bpp", "tags": {"host": "h0"}, "fields": {"v": float(i)},
      "timestamp": 1700000000000000000 + i * 1000000} for i in range(20000)]
print(json.dumps({"writes": w}))
PY
rm -f /tmp/tsgate_bp_h_*.txt
seq 1 16 | xargs -P 16 -I{} curl -s -m60 -o /dev/null -D "/tmp/tsgate_bp_h_{}.txt" \
    -X POST http://127.0.0.1:19210/write -H 'Content-Type: application/json' \
    --data-binary @/tmp/tsgate_bp_probe.json >/dev/null 2>&1
rm -f /tmp/tsgate_bp_probe.json
echo "  probe statuses: $(grep -h '^HTTP/1.1' /tmp/tsgate_bp_h_*.txt 2>/dev/null | tr -d '\r' | sort | uniq -c | tr '\n' ' ')"
N503=$(grep -l ' 503 ' /tmp/tsgate_bp_h_*.txt 2>/dev/null | wc -l)
NRETRY=$(grep -il '^retry-after:' /tmp/tsgate_bp_h_*.txt 2>/dev/null | wc -l)
N500=$(grep -l ' 500 ' /tmp/tsgate_bp_h_*.txt 2>/dev/null | wc -l)
echo "  $N503 x 503, $NRETRY carrying Retry-After, $N500 x 500"
assert_ge "503 responses when concurrent batches exceed the bound" "$N503" 1
assert_eq "503s WITHOUT Retry-After" "$((N503 - NRETRY))" 0
assert_eq "500 responses in the probe" "$N500" 0
rm -f /tmp/tsgate_bp_h_*.txt

# The large concurrent probes can leave a short node-local apply tail even when
# every HTTP request was refused/ambiguous. The benchmark has its own /health
# preflight; wait for the cluster-aware contract instead of mistaking transient
# non-readiness for an empty overload campaign.
wait_healthy "$PORTS" 60 || gate_exit

echo "=== A: 12 connections against a $LIMIT-byte/shard budget (must PUSH BACK) ==="
run_bench 12 200 /tmp/tsgate_bp_overload_bench.txt
A_RC=$BENCH_RC
A=$(</tmp/tsgate_bp_overload_bench.txt)
grep -E "Requests:|First error|Throughput" <<<"$A"
assert_eq "overload benchmark completed" "$A_RC" 0
assert_ge "batches rejected under overload" "$(errs_of "$A")" 1
assert_ge "server-side 'shard write buffer full' rejections" \
    "$(cat /tmp/tsgate_bp*/s.log | grep -c 'shard write buffer full')" 1
assert_eq "server-side 500s under overload" \
    "$(cat /tmp/tsgate_bp*/s.log | grep -c 'Error handling write request')" 0

echo "=== C: a single write still succeeds at the tiny budget ==="
CODE=$(curl -s -m10 -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:19210/write \
    -H 'Content-Type: application/json' \
    -d '{"measurement":"bp2","tags":{"host":"x"},"fields":{"v":1.5},"timestamp":1700000900000000000}')
assert_eq "single write status" "$CODE" 200
assert_eq "node crashes under overload" "$(grep -l 'Segmentation fault' /tmp/tsgate_bp*/s.log 2>/dev/null | wc -l)" 0

echo "=== B: SAME cluster restarted at the DEFAULT budget (must run clean) ==="
# Recovery is measured at the default budget on a fresh start, not by lowering the load on
# the artificial one. Two reasons the artificial budget cannot show it: the bench keeps
# several batches in flight even at --connections 1 (it pipelines per client core), and a
# budget small enough for 16 curls to trip is far below what one bench connection holds --
# so "the load dropped" is not expressible there. What an operator actually needs to know
# is that the DEFAULT budget never gets in the way, which is what this measures.
kill_cluster 1921
require_ports_free 19210 19211 19212
fresh_gate_data_dirs /tmp/tsgate_bp1 /tmp/tsgate_bp2 /tmp/tsgate_bp3 || exit 2
for i in 1 2 3; do
    env $GATE_SERVER_ENV TIMESTAR_DATA_DIR="/tmp/tsgate_bp$i" \
        TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_UUID=00112233445566778899aabbccddeeff TIMESTAR_CLUSTER_DEVELOPMENT_ALLOW_INSECURE_TRANSPORT=true TIMESTAR_CLUSTER_NODE_ID=$i TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port $((19209 + i)) --smp 4 >"/tmp/tsgate_bp$i/s.log" 2>&1 &
done
wait_balanced "$PORTS" 4096 3 90 || gate_exit
wait_healthy "$PORTS" 60 || gate_exit
assert_ge "startup log names the DEFAULT budget" \
    "$(cat /tmp/tsgate_bp*/s.log | grep -c 'in-flight budget: 33554432 bytes/shard (default)')" 1
run_bench 8 200 /tmp/tsgate_bp_default_bench.txt
B_RC=$BENCH_RC
B=$(</tmp/tsgate_bp_default_bench.txt)
grep -E "Requests:|First error|Throughput" <<<"$B"
assert_eq "default-budget benchmark completed" "$B_RC" 0
assert_eq "client HTTP errors at the default budget" "$(errs_of "$B")" 0
assert_ge "batches accepted at the default budget" "$(oks_of "$B")" 200
assert_eq "server-side rejections at the default budget" \
    "$(cat /tmp/tsgate_bp*/s.log | grep -c 'shard write buffer full')" 0
assert_eq "node crashes at the default budget" \
    "$(grep -l 'Segmentation fault' /tmp/tsgate_bp*/s.log 2>/dev/null | wc -l)" 0

gate_exit
