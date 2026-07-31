#!/bin/bash
# GATE (write-scaleout Phase 4c): a BURST OF TCP CONNECTION RESETS between two live nodes
# must cost latency, not client errors -- and must lose and duplicate nothing.
#
# This injects the actual fault [D6] is a theory about instead of hoping ambient load
# produces one. A userspace proxy (tcp_reset_proxy.py) carries one peer's traffic, and is
# told to RST every connection it holds, repeatedly, for the middle of a bench run.
#
# WHY A RESET AND NOT A KILL. They are different faults and only one of them is [D6]. A
# killed node refuses connections, so reconnects fail and the write SHOULD eventually fail
# closed -- that is the node-kill gate. Here the peer is HEALTHY and its listener stays
# open the whole time; only established connections die. seastar's rpc::client never
# re-dials, so the transport has to notice and replace the client, and the write retry has
# to OUTLIVE the reconnect backoff it does that behind. Pre-4a it did not: six attempts
# 20 ms apart all fit inside one 200 ms backoff window, so every attempt fast-failed on the
# same dead socket and the client got a 5xx.
#
# TOPOLOGY. Nodes 1 and 2 are told node 3 lives at 127.0.0.2 (the proxy); node 3 is told
# it lives at 127.0.0.1 (where it actually binds). Same node ids, same placement, same
# replication -- only the ADDRESS nodes 1 and 2 dial differs, which is the only way to put
# a proxy in front of a peer whose HTTP/data/Raft ports are a fixed offset apart and whose
# own bind address comes from the same list. The DATA-PLANE and RAFT ports are proxied
# (one peer address serves both); only the DATA-PLANE port is reset.
#
# THE PROXY IS A HANDICAP, AND THE GATE SAYS SO. A python forwarder is far slower than the
# kernel path it replaces, so absolute throughput here is meaningless. The gate measures a
# QUIET baseline through the same proxy first, and asserts the dip RELATIVE TO THAT.
#
# Usage: fault_injection_gate.sh [SERVER_BINARY]
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
BENCH="$BUILD_DIR/bin/timestar_insert_bench"
[ -x "$BENCH" ] || { echo "no insert bench at $BENCH"; exit 2; }
command -v python3 >/dev/null || { echo "python3 required for the reset proxy"; exit 2; }

PORTS="19410 19411 19412"
# Minimum resets the run must actually have injected. Without this the gate is vacuous:
# a proxy that never fired, or a bench that finished before the storm started, would pass.
#
# THESE FLOORS ARE SET AGAINST THE OBSERVED STORM, not against zero (debt D-4). They used
# to be 8 and 8 -- about 5% of what a real run injects (147 rounds destroying 392-400
# connections), which is barely more than the vacuity check they replaced: a storm that
# fired 9 times would have satisfied them while proving almost nothing about a burst.
# They are now ~50% of observed, which is the largest fraction that still leaves room for
# a slower box (the resetter fires on a fixed 0.3 s clock while the bench length is
# machine-dependent, so a machine that finishes the bench in half the time legitimately
# injects half the rounds). A run that comes in under these is not a pass and is not a
# failure of the property either -- it is a run that did not test it, and it must say so.
#
# If a genuinely slower/faster box needs a different number, override rather than edit:
# GATE_MIN_RESET_ROUNDS / GATE_MIN_RESET_CONNS. Record the observed counts when you do.
MIN_RESET_ROUNDS="${GATE_MIN_RESET_ROUNDS:-70}"
MIN_RESET_CONNS="${GATE_MIN_RESET_CONNS:-180}"
# The dip bound, as a percentage of the quiet-through-the-proxy baseline.
MIN_DIP_PCT="${GATE_MIN_DIP_PCT:-40}"

cleanup() {
    [ -n "${PROXY_PID:-}" ] && kill -9 "$PROXY_PID" 2>/dev/null
    kill_cluster 1941
}
kill_cluster 1941
require_ports_free 19410 19411 19412
for i in 1 2 3; do rm -rf "/tmp/tsgate_fi$i"; mkdir -p "/tmp/tsgate_fi$i"; done
trap cleanup EXIT

# Node 3 is reached through 127.0.0.2 by everyone EXCEPT itself.
PEERS_VIA_PROXY="127.0.0.1:19410,127.0.0.1:19411,127.0.0.2:19412"
PEERS_DIRECT="127.0.0.1:19410,127.0.0.1:19411,127.0.0.1:19412"
start_node() { # $1 = node id, $2 = peers list
    env TIMESTAR_DATA_DIR="/tmp/tsgate_fi$1" TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_NODE_ID=$1 TIMESTAR_CLUSTER_PEERS="$2" \
        "$BIN" --port $((19409 + $1)) --smp 4 >>"/tmp/tsgate_fi$1/s.log" 2>&1 &
}

# Only the DATA-PLANE and RAFT ports are proxied. Node 3's HTTP listener binds
# 0.0.0.0:19412 (it is not a cluster-plane address), so a proxy on 127.0.0.2:19412 would
# collide with it and node 3 would exit on the failed bind -- which is exactly what the
# first version of this gate did. The cluster planes only ever dial port+1000 and
# port+2000, so leaving the HTTP port unproxied costs nothing.
echo "=== proxy: 127.0.0.2:{20412,21412} -> 127.0.0.1:{20412,21412} ==="
PROXY_LOG=/tmp/tsgate_fi_proxy.log
: >"$PROXY_LOG"
python3 ./tcp_reset_proxy.py \
    --map 127.0.0.2:20412:127.0.0.1:20412:reset \
    --map 127.0.0.2:21412:127.0.0.1:21412 \
    >"$PROXY_LOG" 2>&1 &
PROXY_PID=$!
for _ in $(seq 1 30); do grep -q READY "$PROXY_LOG" && break; sleep 0.2; done
grep -q READY "$PROXY_LOG" || { echo "ABORT: proxy did not come up"; cat "$PROXY_LOG"; exit 2; }
grep -q RESETTABLE "$PROXY_LOG" || { echo "ABORT: no resettable mapping"; exit 2; }

start_node 1 "$PEERS_VIA_PROXY"
start_node 2 "$PEERS_VIA_PROXY"
start_node 3 "$PEERS_DIRECT"
wait_all_led "$PORTS" 4096 120 || gate_exit

# Leadership must be SPREAD before the fault matters. The first node to start wins every
# election, and a converged-but-skewed cluster can leave node 3 leading ~3% of the
# VShards -- so almost nothing crosses the proxy and resetting it proves nothing. Storm
# the rebalance endpoint until every node is near its fair share, then let it settle.
echo "=== balancing leadership so a fair share of traffic crosses the proxy ==="
for _ in $(seq 1 12); do
    for p in $PORTS; do curl -s -m5 -X POST "http://127.0.0.1:$p/cluster/rebalance-leadership?max=2048" >/dev/null 2>&1; done
    sleep 1
done
wait_balanced "$PORTS" 4096 3 60 || gate_exit
NODE3_LED=$(status_field "$(cluster_status 19412)" vshards_led)
echo "  node 3 (behind the proxy) leads $NODE3_LED VShards"
assert_ge "VShards led behind the proxy (traffic that must cross the fault)" "${NODE3_LED:-0}" 800

# ---------------------------------------------------------------------------
# Baseline: the same load through the same proxy, with NO resets. Everything below is
# measured against this, not against an unproxied number.
echo "=== baseline (proxy in path, no faults) ==="
timeout 300 "$BENCH" --server-port 19410 -c 4 --batches 2000 --batch-size 10000 --verify 0 \
    --warmup 5 --connections 4 --hosts 1000 --racks 2 >/tmp/tsgate_fi_base.txt 2>&1
grep -E "Requests:|Throughput|batch latency" /tmp/tsgate_fi_base.txt
BASE_TPUT=$(grep -oE 'Throughput:[[:space:]]*[0-9.]+' /tmp/tsgate_fi_base.txt | head -1 | grep -oE '[0-9.]+')
BASE_ERRS=$(grep -o '[0-9]* HTTP errors' /tmp/tsgate_fi_base.txt | head -1 | cut -d' ' -f1)
echo "  baseline throughput: ${BASE_TPUT:-?} (errors ${BASE_ERRS:-?})"
assert_eq "baseline client HTTP errors (the proxy alone must break nothing)" "${BASE_ERRS:-missing}" 0

# ---------------------------------------------------------------------------
# The known-count durability probe. A fixed set of points is written DURING the
# turbulence, one per request, so every ack is individually attributable; afterwards the
# range is read back and must contain EXACTLY the acked points -- no loss, no duplicates.
BASE_TS=1750000000000000000
PROBE=200

echo "=== reset storm under sustained writes ==="
# Three independent things run at once, deliberately decoupled: the bench (bulk load),
# the RESETTER (the fault), and the probe (the countable writes). An earlier version
# gated the resetter on the bench still running and the bench finished in under a second,
# so the storm never fired -- the anti-vacuity assertions below exist because of that.
rm -f /tmp/tsgate_fi_stop
( timeout 300 "$BENCH" --server-port 19410 -c 4 --batches 2000 --batch-size 10000 --verify 0 \
    --warmup 5 --connections 4 --hosts 1000 --racks 2 >/tmp/tsgate_fi_storm.txt 2>&1 ) &
BENCHPID=$!
( ROUNDS=0
  while [ ! -f /tmp/tsgate_fi_stop ]; do
      kill -USR1 "$PROXY_PID" 2>/dev/null && ROUNDS=$((ROUNDS + 1))
      sleep 0.3
  done
  echo "$ROUNDS" >/tmp/tsgate_fi_rounds ) &
RESETPID=$!
sleep 1

PROBE_OK=0; PROBE_5XX=0; PROBE_OTHER=0
i=0
while [ "$i" -lt "$PROBE" ]; do
    CODE=$(curl -s -m10 -o /tmp/tsgate_fi_resp.txt -w '%{http_code}' -X POST http://127.0.0.1:19410/write \
        -H 'Content-Type: application/json' \
        -d "{\"measurement\":\"faultprobe\",\"tags\":{\"host\":\"p$i\"},\"fields\":{\"v\":1.0},\"timestamp\":$((BASE_TS + i * 1000000000))}")
    case "$CODE" in
        2*) PROBE_OK=$((PROBE_OK + 1)) ;;
        5*) PROBE_5XX=$((PROBE_5XX + 1))
            [ "$PROBE_5XX" = "1" ] && echo "  first probe 5xx ($CODE): $(head -c 200 /tmp/tsgate_fi_resp.txt)" ;;
        *)  PROBE_OTHER=$((PROBE_OTHER + 1)) ;;
    esac
    i=$((i + 1))
done
wait $BENCHPID
touch /tmp/tsgate_fi_stop
wait $RESETPID
ROUNDS=$(cat /tmp/tsgate_fi_rounds 2>/dev/null || echo 0)
echo "  probe writes: $PROBE_OK ok, $PROBE_5XX 5xx, $PROBE_OTHER other (of $i attempted)"
grep -E "Requests:|First error|Throughput|batch latency" /tmp/tsgate_fi_storm.txt

RESET_CONNS=$(awk '/^RESET /{s+=$2} END{print s+0}' "$PROXY_LOG")
echo "  reset rounds: $ROUNDS, connections destroyed: $RESET_CONNS"

# ---------------------------------------------------------------------------
# THE ANTI-VACUITY ASSERTIONS. Without a real storm this gate proves nothing: a proxy that
# never fired, or one that fired while no connection was open, would otherwise pass.
assert_ge "reset rounds injected" "$ROUNDS" "$MIN_RESET_ROUNDS"
assert_ge "peer connections actually destroyed" "$RESET_CONNS" "$MIN_RESET_CONNS"

# THE PROPERTY. A reset burst against a LIVE peer must be absorbed entirely by the
# transport's reconnect plus the 4a retry pacing, inside the 1.5s write deadline.
STORM_ERRS=$(grep -o '[0-9]* HTTP errors' /tmp/tsgate_fi_storm.txt | head -1 | cut -d' ' -f1)
STORM_CONN=$(grep -o '[0-9]* connection failures' /tmp/tsgate_fi_storm.txt | head -1 | cut -d' ' -f1)
assert_eq "bench client HTTP errors across the reset storm" "${STORM_ERRS:-missing}" 0
assert_eq "bench client connection failures" "${STORM_CONN:-missing}" 0
assert_eq "probe 5xx across the reset storm" "$PROBE_5XX" 0
assert_eq "probe non-HTTP failures" "$PROBE_OTHER" 0
assert_eq "server-side 500s" "$(cat /tmp/tsgate_fi*/s.log | grep -c 'Error handling write request')" 0
assert_eq "node crashes" "$(grep -l 'Segmentation fault' /tmp/tsgate_fi*/s.log 2>/dev/null | wc -l)" 0

# BOUNDED DIP, against the proxied baseline (see the header).
STORM_TPUT=$(grep -oE 'Throughput:[[:space:]]*[0-9.]+' /tmp/tsgate_fi_storm.txt | head -1 | grep -oE '[0-9.]+')
if [ -n "${BASE_TPUT:-}" ] && [ -n "${STORM_TPUT:-}" ]; then
    PCT=$(awk -v a="$STORM_TPUT" -v b="$BASE_TPUT" 'BEGIN{ if (b+0==0) print 0; else printf "%d", 100*a/b }')
    echo "  throughput under the storm: $STORM_TPUT vs baseline $BASE_TPUT ($PCT%)"
    assert_ge "throughput retained vs the proxied baseline (%)" "$PCT" "$MIN_DIP_PCT"
else
    gate_fail "could not parse throughput from one of the bench runs"
fi

# ---------------------------------------------------------------------------
# NO LOSS, NO DUP. Every acked probe point must be readable, exactly once, on EVERY node
# -- the ack contract says an ack means a durable quorum commit, so a follower read after
# the storm must agree.
echo "=== read-back on every node ==="
sleep 3
for p in $PORTS; do
    RESP=$(curl -s -m20 -X POST "http://127.0.0.1:$p/query" -H 'Content-Type: application/json' \
        -d "{\"query\":\"count:faultprobe(v){}\",\"startTime\":$BASE_TS,\"endTime\":$((BASE_TS + PROBE * 1000000000))}")
    N=$(printf '%s' "$RESP" | grep -o '"point_count":[0-9]*' | cut -d: -f2)
    echo "  node on :$p -> point_count=${N:-<none>}"
    assert_eq "acked probe points readable on :$p" "${N:-missing}" "$PROBE_OK"
done

gate_exit
