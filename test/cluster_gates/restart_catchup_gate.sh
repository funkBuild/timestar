#!/bin/bash
# P1 EMPTY-NODE CATCH-UP GATE: remove a stopped voter's entire durable root,
# compact a live VShard past that voter's log, append a post-snapshot suffix,
# and require the returning empty node to install the snapshot and catch up.
#
# The hot-series shape replaces the former four-million-point load campaign.
# It gives one VShard enough exact Raft entries and flushed TSM data to force
# compaction while three one-reactor processes remain inside 1-GiB limits. All
# durable state, responses, logs, and process temporaries live under build/tmp.
#
# Usage: restart_catchup_gate.sh [SERVER_BINARY]

GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-1G}"
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }

PORTS="19510 19511 19512"
PREFIX=1951
PEERS="127.0.0.1:19510,127.0.0.1:19511,127.0.0.1:19512"
ROOT="$GATE_TMP_ROOT"
DIR1="$ROOT/tsgate_cu1"
DIR2="$ROOT/tsgate_cu2"
DIR3="$ROOT/tsgate_cu3"
WORK="$ROOT/tsgate_cu_work"
DIRS="$DIR1 $DIR2 $DIR3 $WORK"
PREFIX_WRITES="${GATE_PREFIX_WRITES:-96}"
SUFFIX_WRITES="${GATE_SUFFIX_WRITES:-8}"
SNAP_ENTRIES="${GATE_SNAPSHOT_ENTRIES:-80}"
WAL_THRESHOLD="${GATE_WAL_THRESHOLD:-4096}"
declare -a NODE_PIDS

mkdir -p "$ROOT"
kill_cluster "$PREFIX"
require_ports_free $PORTS
fresh_gate_data_dirs $DIRS || exit 2
trap 'gate_cleanup 1951 "$DIR1" "$DIR2" "$DIR3" "$WORK"' EXIT

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
    port=$((19509 + node))
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

write_point() {
    local ordinal="$1" preferred_port="$2" attempt port code
    for attempt in $(seq 0 5); do
        port=$((19510 + ((preferred_port - 19510 + attempt) % 2)))
        code=$(curl -sS -m15 -o "$WORK/write.json" -w '%{http_code}' \
            -X POST "http://127.0.0.1:$port/write" -H 'Content-Type: application/json' \
            -d "{\"measurement\":\"empty_catchup\",\"tags\":{\"probe\":\"one\"},\"fields\":{\"value\":$ordinal.5},\"timestamp\":$((TS + ordinal))}")
        [ "$code" = "200" ] && return 0
        case "$code" in 000|503) sleep 0.1 ;; *) return 1 ;; esac
    done
    return 1
}

survivor_counter_sum() {
    local field="$1" a b
    a=$(status_field "$(cluster_status 19510)" "$field")
    b=$(status_field "$(cluster_status 19511)" "$field")
    echo $(( ${a:-0} + ${b:-0} ))
}

query_count() {
    local port="$1" response count
    response=$(curl -sS -m20 -X POST "http://127.0.0.1:$port/query" \
        -H 'Content-Type: application/json' \
        -d "{\"query\":\"count:empty_catchup(value){probe:one}\",\"startTime\":$((TS - 1)),\"endTime\":$((TS + PREFIX_WRITES + SUFFIX_WRITES + 1))}")
    count=$(printf '%s' "$response" | grep -o '"point_count":[0-9]*' | head -1 | cut -d: -f2)
    echo "${count:-0}"
}

for node in 1 2 3; do start_node "$node"; done
wait_all_led "$PORTS" 4096 180 || gate_exit
wait_leadership_settled "$PORTS" 40 || gate_exit

echo "=== stop node 3 and prove its durable root is absent ==="
kill -9 "${NODE_PIDS[3]}" 2>/dev/null
wait "${NODE_PIDS[3]}" 2>/dev/null
NODE_PIDS[3]=""
remove_gate_data_dirs "$DIR3" || exit 2
if [ -e "$DIR3" ]; then
    echo "ABORT: node 3 durable root survived verified removal" >&2
    exit 2
fi
gate_ok "node 3 durable root is absent while the process is stopped"
mkdir -p "$DIR3"
if find "$DIR3" -mindepth 1 -print -quit | grep -q .; then
    echo "ABORT: recreated node 3 root is not empty" >&2
    exit 2
fi
gate_ok "node 3 restart root is newly created and empty"

wait_all_led "19510 19511" 4096 120 || gate_exit
wait_leadership_settled "19510 19511" 40 || gate_exit

echo "=== create and compact a hot prefix while node 3 is absent ==="
TS=1785712200000000000
ACKED=0
for i in $(seq 1 "$PREFIX_WRITES"); do
    port=$((19510 + (i % 2)))
    write_point "$i" "$port" && ACKED=$((ACKED + 1))
done
assert_eq "prefix writes acknowledged" "$ACKED" "$PREFIX_WRITES"

SNAPSHOT_READY=0
for _ in $(seq 1 90); do
    taken1=$(status_field "$(cluster_status 19510)" snapshots_taken)
    taken2=$(status_field "$(cluster_status 19511)" snapshots_taken)
    if [ "${taken1:-0}" -ge 1 ] && [ "${taken2:-0}" -ge 1 ]; then
        SNAPSHOT_READY=1
        break
    fi
    sleep 2
done
assert_eq "both survivors compacted the missing voter's prefix" "$SNAPSHOT_READY" 1
TSM_SURVIVORS=0
for dir in "$DIR1" "$DIR2"; do
    find "$dir" -type f -name '*.tsm' -print -quit 2>/dev/null | grep -q . && TSM_SURVIVORS=$((TSM_SURVIVORS + 1))
done
assert_eq "survivors with materialized TSM data" "$TSM_SURVIVORS" 2

# Empty topology-only snapshots are currently 162 bytes. The hot snapshot is
# larger because it carries the exact catalog and TSM object. Require both
# donors to have compacted that same non-empty group; an aggregate snapshot
# counter can otherwise be satisfied by unrelated balancing churn.
HOT_GROUP1=$(find "$DIR1/cluster_raft" -type f -path '*/snapshot_sidecars/snapshot_v1_g*.bin' \
    -size +256c -printf '%f\n' 2>/dev/null | sed -n 's/^snapshot_v1_g\([0-9]*\)_.*/\1/p' | head -1)
HOT_GROUP2=$(find "$DIR2/cluster_raft" -type f -path '*/snapshot_sidecars/snapshot_v1_g*.bin' \
    -size +256c -printf '%f\n' 2>/dev/null | sed -n 's/^snapshot_v1_g\([0-9]*\)_.*/\1/p' | head -1)
if [ -n "$HOT_GROUP1" ] && [ "$HOT_GROUP1" = "$HOT_GROUP2" ]; then
    gate_ok "both survivors hold the non-empty snapshot for VShard $HOT_GROUP1"
else
    gate_fail "non-empty donor snapshots disagree or are absent (node1=${HOT_GROUP1:-none}, node2=${HOT_GROUP2:-none})"
    gate_exit
fi

echo "=== add an exact delete and retained suffix after the snapshot ==="
DELETE_ISSUED_MS=$(date +%s)000
DELETE_CODE=000
for _ in $(seq 1 20); do
    DELETE_CODE=$(curl -sS -m15 -o "$WORK/delete.json" -w '%{http_code}' \
        -X POST 'http://127.0.0.1:19510/delete' -H 'Content-Type: application/json' \
        -H 'Idempotency-Key: 11111111111111111111111111111111' \
        -H "Idempotency-Key-Timestamp: $DELETE_ISSUED_MS" \
        -d "{\"series\":\"empty_catchup,probe=one value\",\"startTime\":$((TS + 1)),\"endTime\":$((TS + 1))}")
    [ "$DELETE_CODE" = "200" ] && break
    case "$DELETE_CODE" in 000|503) ;; *) break ;; esac
    sleep 0.1
done
assert_eq "post-snapshot exact delete HTTP status" "$DELETE_CODE" 200

for i in $(seq 1 "$SUFFIX_WRITES"); do
    ordinal=$((PREFIX_WRITES + i))
    port=$((19510 + (i % 2)))
    write_point "$ordinal" "$port" && ACKED=$((ACKED + 1))
done
assert_eq "all prefix and suffix writes acknowledged" "$ACKED" "$((PREFIX_WRITES + SUFFIX_WRITES))"
EXPECTED=$((PREFIX_WRITES + SUFFIX_WRITES - 1))

# Prove the exact-series spelling hit real data before recovery begins. A 200
# response alone only proves that a target was replicated; deleting a malformed
# but syntactically valid series key is an intentional no-op.
SURVIVOR_COUNT=0
for _ in $(seq 1 12); do
    SURVIVOR_COUNT=$(query_count 19510)
    [ "$SURVIVOR_COUNT" -eq "$EXPECTED" ] && break
    sleep 2
done
assert_eq "survivors applied the exact post-snapshot delete" "$SURVIVOR_COUNT" "$EXPECTED"

# Compaction while node 3 is intentionally absent may start and then abandon a
# transfer to that dead voter. Measure the recovery window separately: after the
# empty process starts, at least one new chunk must be sent and retry exhaustion
# must remain bounded. A failed attempt is safe and restarts from offset zero;
# successful installation plus exact readback are the correctness conditions.
CHUNKS_BEFORE_REJOIN=$(survivor_counter_sum snapshot_chunks_sent)
ABANDONED_BEFORE_REJOIN=$(survivor_counter_sum snapshot_transfers_abandoned)

echo "=== restart the empty node and require snapshot installation plus suffix catch-up ==="
start_node 3
INSTALLED=0
for _ in $(seq 1 180); do
    installed=$(status_field "$(cluster_status 19512)" snapshots_installed)
    if [ "${installed:-0}" -ge 1 ]; then
        INSTALLED=1
        break
    fi
    sleep 2
done
assert_eq "empty node installed a transferred snapshot" "$INSTALLED" 1

# Returning from a completely empty root creates the maximum possible leader
# deficit. The periodic balancer uses jittered, bounded passes and only targets
# replicas that exactly match the donor's log while recovery is in progress;
# allow the same six-minute envelope as snapshot installation before judging
# the idle cluster stable.
wait_leadership_settled "$PORTS" 180 || gate_exit
CAUGHT=0
for _ in $(seq 1 180); do
    ok=1
    for port in 19510 19511; do
        status=$(cluster_status "$port")
        led=$(status_field "$status" vshards_led)
        peer3=$(printf '%s' "$status" | grep -o '"3":[0-9]*' | head -1 | cut -d: -f2)
        [ -n "$led" ] && [ "${peer3:-0}" -ge "$led" ] || ok=0
    done
    if [ "$ok" = "1" ]; then CAUGHT=1; break; fi
    sleep 2
done
assert_eq "empty node caught up on every group led by either survivor" "$CAUGHT" 1

COUNT=0
for _ in $(seq 1 12); do
    COUNT=$(query_count 19512)
    [ "$COUNT" -eq "$EXPECTED" ] && break
    sleep 2
done
assert_eq "snapshot prefix plus retained suffix minus exact delete" "$COUNT" "$EXPECTED"

CHUNKS=$(survivor_counter_sum snapshot_chunks_sent)
UNDELIVERABLE=$(survivor_counter_sum snapshots_undeliverable)
ABANDONED=$(survivor_counter_sum snapshot_transfers_abandoned)
CHUNKS_AFTER_REJOIN=$((CHUNKS - CHUNKS_BEFORE_REJOIN))
ABANDONED_AFTER_REJOIN=$((ABANDONED - ABANDONED_BEFORE_REJOIN))
LOGS="$WORK/node1.log $WORK/node2.log $WORK/node3.log"
SERVER_500=$(grep -h -c 'Error handling write request' $LOGS | awk '{s+=$1} END{print s+0}')
CRASHES=$(grep -l -E 'Segmentation fault|Aborting on shard|std::bad_alloc' $LOGS 2>/dev/null | wc -l)

assert_ge "InstallSnapshot chunks sent after empty-node restart" "$CHUNKS_AFTER_REJOIN" 1
assert_eq "undeliverable snapshots" "$UNDELIVERABLE" 0
assert_le "snapshot transfers abandoned after empty-node restart" "$ABANDONED_AFTER_REJOIN" 8
assert_eq "server-side 500s" "$SERVER_500" 0
assert_eq "node crashes" "$CRASHES" 0

gate_exit
