#!/bin/bash
# P1 HOMOGENEOUS-V1 GATE: an unsupported peer or persisted format must fail
# closed before application traffic or partial recovery can be served.
#
# This is deliberately not a compatibility test.  The unsupported value is
# introduced only by the harness; production continues to define and emit v1.
# The live arm corrupts the version field of a real, acknowledged WAL, then
# proves startup neither serves HTTP nor rewrites the rejected source.
#
# One Seastar process runs at a time, capped at 1 GiB.  Its durable root, logs,
# test temporaries, and retained diagnostic tail all live under build/tmp.
#
# Usage: homogeneous_v1_rejection_gate.sh [SERVER_BINARY]

GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-1G}"
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
UNIT="$BUILD_DIR/test/timestar_test"
SOCKET="$BUILD_DIR/test/timestar_cluster_socket_test"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
[ -x "$UNIT" ] || { echo "no test binary at $UNIT"; exit 2; }
[ -x "$SOCKET" ] || { echo "no socket test binary at $SOCKET"; exit 2; }

PORT=19890
PREFIX=1989
ROOT="$BUILD_DIR/tmp/tsgate_v1_reject"
WORK="$BUILD_DIR/tmp/tsgate_v1_work"
TEST_TMP="$WORK/test_tmp"
LOG="$WORK/server.log"
TAILS="$BUILD_DIR/tmp/tsgate_${PREFIX}_tails.log"
SERVER_PID=""

stop_server() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -9 "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    fi
    SERVER_PID=""
}

cleanup() {
    stop_server
    if [ -f "$LOG" ]; then
        { echo "=== $LOG (last 300 lines) ==="; tail -n 300 "$LOG"; } >"$TAILS"
    fi
    if [ "${GATE_KEEP_DATA:-0}" = "1" ]; then
        echo "  GATE_KEEP_DATA=1: leaving $ROOT and $WORK in place"
    else
        remove_gate_data_dirs "$ROOT" "$WORK" || true
    fi
}
trap cleanup EXIT

mkdir -p "$BUILD_DIR/tmp"
kill_cluster "$PREFIX"
require_ports_free "$PORT"
fresh_gate_data_dirs "$ROOT" "$WORK" || exit 2
mkdir -p "$TEST_TMP"

echo "=== exact-v1 codecs and real-socket handshake ==="
CODEC_FILTER='WriteRecordV1.IsSelfIdentifyingAndRejectsOtherVersions:RaftCodecTest.RequiresExplicitV1Marker:JournalSegmentTest.HeaderRejectsBadMagicVersionAndTruncation:SnapshotPayloadV1.RejectsMalformedFrames:ControlCommandV1.RejectsTruncationCorruptionAndTrailingBytes:Group0SnapshotV1.RoundTripsAndRejectsMalformedState'
if TMPDIR="$TEST_TMP" "$UNIT" --gtest_filter="$CODEC_FILTER" \
    --smp 1 --memory "$GATE_SERVER_MEMORY" --overprovisioned; then
    gate_ok "current wire, command, snapshot, and journal codecs reject non-v1 input"
else
    gate_fail "exact-v1 codec rejection suite"
fi

if TMPDIR="$TEST_TMP" "$SOCKET" \
    --gtest_filter=ShardRaftPlaneTest.DataPlaneRejectsUnknownPeerVersionBeforeServingTraffic \
    --smp 1 --memory "$GATE_SERVER_MEMORY" --overprovisioned; then
    gate_ok "data-plane rejects unknown peer versions before the application verb"
else
    gate_fail "exact-v1 data-plane socket rejection"
fi

start_server() {
    env $GATE_SERVER_ENV \
        TMPDIR="$TEST_TMP" TIMESTAR_DATA_DIR="$ROOT" TIMESTAR_WAL_SIZE_THRESHOLD=1073741824 \
        "$BIN" --port "$PORT" --smp 1 --memory "$GATE_SERVER_MEMORY" --overprovisioned \
        >>"$LOG" 2>&1 &
    SERVER_PID=$!
}

wait_serving() {
    local polls="${1:-60}"
    for _ in $(seq 1 "$polls"); do
        if curl -fsS -m2 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
            return 0
        fi
        kill -0 "$SERVER_PID" 2>/dev/null || return 1
        sleep 1
    done
    return 1
}

echo "=== create a real acknowledged v1 WAL ==="
start_server
if wait_serving 60; then
    gate_ok "fresh v1 server reached HTTP readiness"
else
    gate_fail "fresh v1 server did not reach HTTP readiness"
    gate_exit
fi

WRITE_CODE=$(curl -sS -m10 -o "$WORK/write.json" -w '%{http_code}' \
    -X POST "http://127.0.0.1:$PORT/write" -H 'Content-Type: application/json' \
    -d '{"measurement":"v1_rejection","tags":{"source":"gate"},"fields":{"value":1.0},"timestamp":1785711600000000000}')
assert_eq "seed write HTTP status" "$WRITE_CODE" 200
stop_server

WAL=$(find "$ROOT/shard_0" -maxdepth 1 -type f -name '*.wal' -print -quit 2>/dev/null)
if [ -z "$WAL" ] || [ ! -f "$WAL" ]; then
    gate_fail "acknowledged write left no crash-recoverable WAL"
    gate_exit
fi

V1_HEADER=$(od -An -t x1 -N8 "$WAL" | tr -d ' \n')
assert_eq "source WAL header" "$V1_HEADER" 5453574c01000000

echo "=== replace only the persisted version field with an unsupported value ==="
printf '\002\000\000\000' | dd of="$WAL" bs=1 seek=4 conv=notrunc status=none
V2_HEADER=$(od -An -t x1 -N8 "$WAL" | tr -d ' \n')
assert_eq "rejected WAL header" "$V2_HEADER" 5453574c02000000
REJECTED_CKSUM=$(cksum "$WAL")

: >"$LOG"
start_server
SERVED=0
EXITED=0
EXIT_CODE=0
for _ in $(seq 1 60); do
    if curl -fsS -m1 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
        SERVED=1
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        wait "$SERVER_PID"
        EXIT_CODE=$?
        SERVER_PID=""
        EXITED=1
        break
    fi
    sleep 1
done

assert_eq "unknown-version startup served HTTP" "$SERVED" 0
assert_eq "unknown-version startup exited" "$EXITED" 1
if [ "$EXIT_CODE" -ne 0 ]; then
    gate_ok "unknown-version startup exit code = $EXIT_CODE (non-zero)"
else
    gate_fail "unknown-version startup exit code = 0"
fi
if grep -q 'WAL recovery: unsupported version 2' "$LOG"; then
    gate_ok "startup named the unsupported WAL version"
else
    gate_fail "startup log did not name unsupported WAL version 2"
fi
assert_eq "rejected WAL preserved byte-for-byte" "$(cksum "$WAL")" "$REJECTED_CKSUM"

gate_exit
