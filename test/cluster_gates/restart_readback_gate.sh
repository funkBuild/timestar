#!/bin/bash
# GATE (debt D-36): AN ACKNOWLEDGED WRITE MUST BE READABLE AFTER A RESTART, INCLUDING A
# RESTART THAT FOLLOWS A HEAVY WRITE CAMPAIGN.
#
# WHY THIS EXISTS SEPARATELY FROM snapshot_durability_gate.sh. That gate found the defect
# but could not name it: it reads back exactly ONCE, so "175 of 200 acked points" is
# equally consistent with 25 points LOST and with 25 points sitting durably in a recovered
# journal that apply() has not reached yet. Those are completely different bugs -- one is
# unrecoverable, the other is a liveness/visibility failure that heals on its own -- and no
# single read can tell them apart.
#
# THE MEASUREMENT, and it is one line of difference from that gate: read back REPEATEDLY.
#
#   * a count that starts short and CLIMBS to the acked total is an APPLY STALL: the data
#     is on disk, the cluster is replaying, and the ack contract is violated only in the
#     sense that the promise is temporarily unreadable;
#   * a count that starts short and STAYS short across the whole window is LOSS.
#
# The gate asserts the invariant at the END of the window (every acked point readable) and
# REPORTS the trajectory, so a regression is visible as either "never converged" (loss) or
# "took much longer to converge" (a worse stall).
#
# WHAT IT WATCHES WHILE IT WAITS, from /cluster/status (added with this gate):
#   apply_lag_entries   committed-but-unapplied entries summed over hosted groups. This is
#                       the ack-contract gap made visible: > 0 means the node is holding
#                       promises it cannot currently answer.
#   apply_failures      committed applies that threw. A stall counter, not a loss one --
#                       the drain is retried and re-apply is idempotent -- but a group
#                       whose apply keeps throwing never becomes readable.
#   tick_errors         ticks that threw (a tick drives the whole Ready drain). A tick
#                       failure is isolated to its own group; it used to abort the whole
#                       pass, starving every higher group id of its tick.
#
# ONE AT A TIME (test/cluster_gates/README.md) and ports BELOW the ephemeral range
# (D-27): a gate that picks inside it races the kernel for its own listen ports.
#
# Usage: restart_readback_gate.sh [SERVER_BINARY]
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
BENCH="$BUILD_DIR/bin/timestar_insert_bench"
[ -x "$BENCH" ] || { echo "no insert bench at $BENCH"; exit 2; }

PORTS="19730 19731 19732"
PROBES="${GATE_PROBES:-200}"
BATCHES="${GATE_BATCHES:-150}"
# How long to keep re-reading after the restart before calling it loss. Generous on
# purpose: the question this gate answers is "does it EVER become readable", and a
# too-short window answers "loss" for a stall.
READ_POLLS="${GATE_READ_POLLS:-24}"
READ_INTERVAL="${GATE_READ_INTERVAL:-10}"

FREE_GB=$(df -BG --output=avail /tmp | tail -1 | tr -dc '0-9')
if [ "${FREE_GB:-0}" -lt 30 ]; then
    echo "ABORT: only ${FREE_GB}G free on /tmp; this gate needs ~30G (see the plan doc's MEASUREMENT HAZARD)" >&2
    exit 2
fi

trap 'kill_cluster 1973' EXIT

PEERS="127.0.0.1:19730,127.0.0.1:19731,127.0.0.1:19732"
start_node() {
    env TIMESTAR_DATA_DIR="/tmp/tsgate_rr$1" TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_NODE_ID=$1 TIMESTAR_CLUSTER_PEERS="$PEERS" \
        TIMESTAR_WAL_SIZE_THRESHOLD="${GATE_WAL_THRESHOLD:-2097152}" \
        "$BIN" --port $((19729 + $1)) --smp 4 >>"/tmp/tsgate_rr$1/s.log" 2>&1 &
}

# status_sum FIELD -- summed across the three nodes.
status_sum() {
    local total=0 p v
    for p in $PORTS; do
        v=$(status_field "$(cluster_status "$p")" "$1")
        total=$((total + ${v:-0}))
    done
    echo "$total"
}

kill_cluster 1973
require_ports_free $PORTS
for i in 1 2 3; do rm -rf "/tmp/tsgate_rr$i"; mkdir -p "/tmp/tsgate_rr$i"; done

echo "=== phase 1: bring up an RF=3 cluster and load it heavily ==="
for i in 1 2 3; do start_node $i; done
wait_all_led "$PORTS" 4096 120 || gate_exit
wait_leadership_settled "$PORTS" 40 || gate_exit

"$BENCH" --server-port 19730 -c 4 --batches "$BATCHES" --batch-size 10000 --verify 0 \
    --warmup 3 --connections 8 --hosts 500 --racks 2 >/tmp/tsgate_rr_bench.txt 2>&1
grep -E "Requests:|Throughput" /tmp/tsgate_rr_bench.txt | sed 's/^/    /'

echo "=== phase 2: 200 acked probe writes ==="
# One point per probe, distinct series AND distinct timestamp, each write awaited -- so
# every 2xx is a promise the cluster made and must keep. Round-robin across coordinators so
# the promises are not all made by one node.
TS=$(date +%s)000000000
ACKED=0
for i in $(seq 1 "$PROBES"); do
    P=$(echo "$PORTS" | tr ' ' '\n' | sed -n "$(( (i % 3) + 1 ))p")
    CODE=$(curl -s -m10 -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$P/write" \
        -H 'Content-Type: application/json' \
        -d "{\"measurement\":\"durable\",\"tags\":{\"probe\":\"p$i\"},\"fields\":{\"value\":$i.5},\"timestamp\":$((TS + i))}")
    [ "$CODE" = "200" ] && ACKED=$((ACKED + 1))
done
echo "    $ACKED of $PROBES probe writes acknowledged"
PRE_LAG=$(status_sum apply_lag_entries)
echo "    pre-kill apply_lag_entries=$PRE_LAG"

echo "=== phase 3: kill -9 the whole cluster and restart it ==="
kill_cluster 1973
sleep 2
for i in 1 2 3; do start_node $i; done
wait_all_led "$PORTS" 4096 180 || gate_exit

echo "=== phase 4: read back REPEATEDLY -- a climbing count is a stall, a flat one is loss ==="
readback() { # -> echoes the per-node counts, sets WORST
    local p READ N
    WORST=999999
    RESULT=""
    for p in $PORTS; do
        READ=$(curl -s -m30 -X POST "http://127.0.0.1:$p/query" -H 'Content-Type: application/json' \
            -d "{\"query\":\"count:durable(value){}\",\"startTime\":$((TS - 1000000000)),\"endTime\":$((TS + PROBES + 1000000000))}")
        N=$(printf '%s' "$READ" | grep -o '"point_count":[0-9]*' | head -1 | cut -d: -f2)
        N=${N:-0}
        RESULT="$RESULT $N"
        [ "$N" -lt "$WORST" ] && WORST=$N
    done
}

FIRST_WORST=""
CONVERGED_AT=""
for poll in $(seq 1 "$READ_POLLS"); do
    readback
    LAG=$(status_sum apply_lag_entries)
    BEHIND=$(status_sum apply_groups_behind)
    AFAIL=$(status_sum apply_failures)
    TERR=$(status_sum tick_errors)
    ELAPSED=$(( (poll - 1) * READ_INTERVAL ))
    echo "    t+${ELAPSED}s readback:$RESULT of $ACKED acked (worst $WORST)" \
         "apply_lag=$LAG groups_behind=$BEHIND apply_failures=$AFAIL" \
         "tick_errors=$TERR"
    [ -z "$FIRST_WORST" ] && FIRST_WORST=$WORST
    if [ "$WORST" -ge "$ACKED" ] && [ -z "$CONVERGED_AT" ]; then
        CONVERGED_AT=$ELAPSED
        break
    fi
    sleep "$READ_INTERVAL"
done

echo "=== VERDICT ==="
echo "  first readback (worst node): $FIRST_WORST of $ACKED acked"
if [ -n "$CONVERGED_AT" ]; then
    if [ "$FIRST_WORST" -ge "$ACKED" ]; then
        echo "  every acked point was readable on the FIRST read -- no stall, no loss"
    else
        echo "  APPLY STALL, not loss: the count climbed from $FIRST_WORST to $ACKED and converged" \
             "${CONVERGED_AT}s after the restart. The data was durable the whole time; it was not APPLIED."
    fi
else
    echo "  LOSS (or a stall longer than $((READ_POLLS * READ_INTERVAL))s): the count never reached $ACKED."
fi

# WHY the applies failed, not just how many. Rate-limited at the source, so this is a
# handful of lines even when the counter is in the tens of thousands.
echo "  apply-failure reasons (rate-limited sample):"
cat /tmp/tsgate_rr*/s.log | grep -o 'could not apply committed entry.*' | sed 's/^/    /' | sort | uniq -c | sort -rn | head -5
SVR500=$(cat /tmp/tsgate_rr*/s.log | grep -c 'Error handling write request')
CRASH=$(grep -l 'Segmentation fault' /tmp/tsgate_rr*/s.log 2>/dev/null | wc -l)
BACKLOG=$(cat /tmp/tsgate_rr*/s.log | grep -ciE 'ingest backlog|backlogged')
echo "  server 500s=$SVR500 crashes=$CRASH ingest_backlog_lines=$BACKLOG"

# ANTI-VACUITY: without acked probes there is no promise to keep.
assert_ge "probe writes acknowledged" "$ACKED" "$PROBES"
# THE INVARIANT: ack => durable quorum commit, so every acked point must be readable after
# the whole cluster is kill -9'd and restarted. Asserted at the END of the read window --
# see the header for why a single read cannot carry this assertion.
assert_ge "every acked point readable after kill -9 (within the read window)" "$WORST" "$ACKED"
assert_le "nothing fabricated or double-counted" "$WORST" "$PROBES"
assert_eq "node crashes" "$CRASH" 0
assert_eq "server-side 500s" "$SVR500" 0

for i in 1 2 3; do rm -rf "/tmp/tsgate_rr$i"; done
gate_exit
