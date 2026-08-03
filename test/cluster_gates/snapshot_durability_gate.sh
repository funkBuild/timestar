#!/bin/bash
# P1 SNAPSHOT-DURABILITY GATE: compact a real hot VShard, kill -9 every
# replica, restart over the compacted journals, and require every acknowledged
# point exactly once.
#
# The old version drove three 20-GiB-class load arms to force a handful of
# VShards over their snapshot threshold. This bounded shape targets one series
# directly: 128 awaited writes create 128 Raft entries in one VShard, a 4-KiB
# WAL threshold materializes TSM data, and an eight-entry trigger compacts it.
# It therefore exercises the same snapshot/replay boundary with one-reactor,
# 1-GiB nodes and low-volume build/tmp storage.
#
# Usage: snapshot_durability_gate.sh [SERVER_BINARY]

GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-1G}"
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }

PORTS="19710 19711 19712"
PREFIX=1971
PEERS="127.0.0.1:19710,127.0.0.1:19711,127.0.0.1:19712"
ROOT="$GATE_TMP_ROOT"
DIR1="$ROOT/tsgate_sd1"
DIR2="$ROOT/tsgate_sd2"
DIR3="$ROOT/tsgate_sd3"
WORK="$ROOT/tsgate_sd_work"
DIRS="$DIR1 $DIR2 $DIR3 $WORK"
WRITES="${GATE_WRITES:-128}"
SNAP_ENTRIES="${GATE_SNAPSHOT_ENTRIES:-8}"
WAL_THRESHOLD="${GATE_WAL_THRESHOLD:-4096}"
declare -a NODE_PIDS

mkdir -p "$ROOT"
kill_cluster "$PREFIX"
require_ports_free $PORTS
fresh_gate_data_dirs $DIRS || exit 2
trap 'gate_cleanup 1971 "$DIR1" "$DIR2" "$DIR3" "$WORK"' EXIT

node_dir() {
    case "$1" in
        1) echo "$DIR1" ;;
        2) echo "$DIR2" ;;
        3) echo "$DIR3" ;;
        *) return 2 ;;
    esac
}

start_node() {
    local node="$1" dir port
    dir=$(node_dir "$node")
    port=$((19709 + node))
    env $GATE_SERVER_ENV TMPDIR="$WORK" TIMESTAR_DATA_DIR="$dir" \
        TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 \
        TIMESTAR_CLUSTER_UUID=00112233445566778899aabbccddeeff \
        TIMESTAR_CLUSTER_DEVELOPMENT_ALLOW_INSECURE_TRANSPORT=true \
        TIMESTAR_CLUSTER_NODE_ID="$node" TIMESTAR_CLUSTER_PEERS="$PEERS" \
        TIMESTAR_CLUSTER_SNAPSHOT_ENTRIES="$SNAP_ENTRIES" \
        TIMESTAR_CLUSTER_SNAPSHOT_BYTES=0 \
        TIMESTAR_CLUSTER_SNAPSHOT_MIN_INTERVAL_S=3600 \
        TIMESTAR_WAL_SIZE_THRESHOLD="$WAL_THRESHOLD" \
        "$BIN" --port "$port" --smp 1 --memory "$GATE_SERVER_MEMORY" \
        >>"$WORK/node${node}.log" 2>&1 &
    NODE_PIDS[$node]=$!
}

for node in 1 2 3; do start_node "$node"; done
wait_all_led "$PORTS" 4096 180 || gate_exit
wait_leadership_settled "$PORTS" 40 || gate_exit

echo "=== write one hot VShard through a real RF=3 cluster ==="
TS=1785712000000000000
ACKED=0
for i in $(seq 1 "$WRITES"); do
    port=$((19710 + (i % 3)))
    code=$(curl -sS -m15 -o "$WORK/write.json" -w '%{http_code}' \
        -X POST "http://127.0.0.1:$port/write" -H 'Content-Type: application/json' \
        -d "{\"measurement\":\"snapshot_durable\",\"tags\":{\"probe\":\"one\"},\"fields\":{\"value\":$i.5},\"timestamp\":$((TS + i))}")
    [ "$code" = "200" ] && ACKED=$((ACKED + 1))
done
assert_eq "writes acknowledged" "$ACKED" "$WRITES"

echo "=== require a durable snapshot on every replica ==="
SNAPSHOT_READY=0
for _ in $(seq 1 90); do
    SNAPSHOT_READY=1
    for port in $PORTS; do
        taken=$(status_field "$(cluster_status "$port")" snapshots_taken)
        [ "${taken:-0}" -ge 1 ] || SNAPSHOT_READY=0
    done
    [ "$SNAPSHOT_READY" = "1" ] && break
    sleep 2
done
assert_eq "all replicas took a snapshot" "$SNAPSHOT_READY" 1
for port in $PORTS; do
    status=$(cluster_status "$port")
    echo "  :$port snapshots=$(status_field "$status" snapshots_taken)" \
         "skipped_unflushed=$(status_field "$status" snapshots_skipped_unflushed)" \
         "refused_too_large=$(status_field "$status" snapshots_refused_too_large)"
done

TSM_REPLICAS=0
for dir in "$DIR1" "$DIR2" "$DIR3"; do
    find "$dir" -type f -name '*.tsm' -print -quit 2>/dev/null | grep -q . && TSM_REPLICAS=$((TSM_REPLICAS + 1))
done
assert_eq "replicas with materialized TSM data" "$TSM_REPLICAS" 3

# Empty topology-only snapshots are currently 162 bytes. Require all replicas
# to hold the same larger data snapshot so an aggregate snapshot counter cannot
# be satisfied by unrelated automatic leadership history.
HOT_GROUP=""
HOT_SNAPSHOT_REPLICAS=0
for dir in "$DIR1" "$DIR2" "$DIR3"; do
    group=$(find "$dir/cluster_raft" -type f -path '*/snapshot_sidecars/snapshot_v1_g*.bin' \
        -size +256c -printf '%f\n' 2>/dev/null | sed -n 's/^snapshot_v1_g\([0-9]*\)_.*/\1/p' | head -1)
    if [ -n "$group" ]; then
        HOT_SNAPSHOT_REPLICAS=$((HOT_SNAPSHOT_REPLICAS + 1))
        if [ -z "$HOT_GROUP" ]; then HOT_GROUP="$group"; elif [ "$group" != "$HOT_GROUP" ]; then HOT_GROUP="mismatch"; fi
    fi
done
assert_eq "replicas holding one common non-empty VShard snapshot" "$HOT_SNAPSHOT_REPLICAS" 3
if [ -n "$HOT_GROUP" ] && [ "$HOT_GROUP" != "mismatch" ]; then
    gate_ok "all replicas compacted non-empty VShard $HOT_GROUP"
else
    gate_fail "non-empty replica snapshots do not identify one common VShard"
fi

echo "=== kill -9 all replicas and restart over compacted journals ==="
kill_cluster "$PREFIX"
for node in 1 2 3; do start_node "$node"; done
wait_all_led "$PORTS" 4096 240 || gate_exit

WORST=999999
BEST=0
READS=""
for port in $PORTS; do
    count=0
    for _ in $(seq 1 30); do
        response=$(curl -sS -m20 -X POST "http://127.0.0.1:$port/query" \
            -H 'Content-Type: application/json' \
            -d "{\"query\":\"count:snapshot_durable(value){probe:one}\",\"startTime\":$((TS - 1)),\"endTime\":$((TS + WRITES + 1))}")
        count=$(printf '%s' "$response" | grep -o '"point_count":[0-9]*' | head -1 | cut -d: -f2)
        count=${count:-0}
        [ "$count" -ge "$ACKED" ] && break
        sleep 2
    done
    READS="$READS $count"
    [ "$count" -lt "$WORST" ] && WORST=$count
    [ "$count" -gt "$BEST" ] && BEST=$count
done
echo "  readback:$READS of $ACKED acknowledged"

LOGS="$WORK/node1.log $WORK/node2.log $WORK/node3.log"
RECOVERED=$(grep -h -c 'recovered from a compacted journal' $LOGS | awk '{s+=$1} END{print s+0}')
REFUSED=$(grep -h -c 'snapshot recovery not yet wired' $LOGS | awk '{s+=$1} END{print s+0}')
TOO_BIG=$(grep -h -c 'NOT compacting VShard' $LOGS | awk '{s+=$1} END{print s+0}')
SERVER_500=$(grep -h -c 'Error handling write request' $LOGS | awk '{s+=$1} END{print s+0}')
CRASHES=$(grep -l -E 'Segmentation fault|Aborting on shard|std::bad_alloc' $LOGS 2>/dev/null | wc -l)

assert_ge "compacted-journal recoveries" "$RECOVERED" 3
assert_eq "snapshot recovery refusals" "$REFUSED" 0
assert_eq "snapshots refused as too large" "$TOO_BIG" 0
assert_eq "every acknowledged point readable after restart" "$WORST" "$ACKED"
assert_le "no point fabricated or double-counted" "$BEST" "$WRITES"
assert_eq "server-side 500s" "$SERVER_500" 0
assert_eq "node crashes" "$CRASHES" 0

gate_exit
