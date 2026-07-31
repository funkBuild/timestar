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
# K STORMS AND AN ERROR BUDGET, NOT ONE STORM AND A ZERO (debt D-21). The single-storm
# zero-error assertion was not reproducible run to run IN EITHER DIRECTION, so neither a
# failing run nor a passing one identified anything. MEASURED on this box, six consecutive
# single-storm runs of the pre-D-21 gate against one unchanged HEAD binary:
#
#     run  1     2     3     4         (5 and 6 were cut short by a harness timeout)
#     err  0     1     0     26        <- the 26 is a VOID run; see below
#     rounds 145 148   146   199
#     conns  392 397   395   471
#     base   5.16M 5.05M 4.98M 0.45M pts/s, baseline errors 0 0 0 214
#
# So the storm draws are 0, 1, 0 and the fourth run is not a draw at all: its FAULT-FREE
# baseline through the proxy collapsed to 9% of throughput with 214 client errors, and the
# storm that followed inherited a sick cluster AND a longer bench (the resetter fires on a
# fixed 0.3 s clock, so a slower bench takes MORE rounds -- 199 against the usual 145-148).
#
# THE K-STORM FORM IS NOW MEASURED TOO -- three consecutive runs of THIS text, K=3, against
# one unchanged HEAD binary (2fdde50 + the sizing below), each run a fresh cluster:
#
#     run          A              B              C
#     errors       [0 0 0] = 0    [1 0 0] = 1    [1 0 1] = 2
#     rounds       79 79 76       77 77 76       79 80 84
#     conns        228 231 224    221 223 219    226 243 230
#     baseline     5.15 M         5.16 M         5.06 M pts/s, 0 errors on all three
#     dip          86 86 87 %     88 87 87 %     87 84 88 %
#
# NINE storm draws: six zeros and three ones, and NO STORM DREW MORE THAN ONE. That is the
# distribution the budget is set against -- not a guess, and the reason the budget is on the
# TOTAL: a per-storm zero would have failed run B and run C on the same binary that passed
# run A, which is precisely the non-reproducibility D-21 filed.
#
# The two failure modes are separated rather than averaged:
#
#   * A DIRTY BASELINE VOIDS THE RUN (exit 3), it does not fail it. The control arm failed,
#     so the run says nothing about the fault -- reporting that as a property failure is
#     what made a single red run uninformative. Re-draw it (and read the README's
#     self-amplifying disk note first: that is one known cause).
#   * THE PROPERTY IS ASSERTED OVER K STORMS AGAINST A BUDGET. The storm runs
#     GATE_STORM_ROUNDS times (default 3) against the same cluster and the gate asserts on
#     the TOTAL client errors, with the per-storm vector printed so the distribution is
#     visible rather than inferred. The default budget sits above the observed run totals
#     (0, 1, 2 above) and far below what a binary without the 4a fix produces -- see
#     `fault_injection_ab.sh`, which measures both arms under an identical storm and is
#     where the separation is actually asserted.
#
# The residual variance is environmental and whole-run (that void baseline), which K storms
# inside one run cannot average away -- which is exactly why the void rule is separate.
#
# Usage: fault_injection_gate.sh [SERVER_BINARY]
#   GATE_STORM_ROUNDS=K        storms per run (default 3)
#   GATE_MAX_STORM_ERRORS=N    total client-error budget across all K (default 3)
#   exit 0 = pass, 1 = property failed, 2 = setup refused, 3 = VOID (re-draw)
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
# to be 8 and 8 -- about 5% of what a real run injects, which is barely more than the
# vacuity check they replaced: a storm that fired 9 times would have satisfied them while
# proving almost nothing about a burst. They are ~50% of observed, which is the largest
# fraction that still leaves room for a slower box (the resetter fires on a fixed 0.3 s
# clock while the bench length is machine-dependent, so a machine that finishes the bench
# in half the time legitimately injects half the rounds). A run that comes in under these
# is not a pass and is not a failure of the property either -- it is a run that did not
# test it, and it must say so.
#
# They came down from 70/180 with the bench size (see BENCH_BATCHES below): 70/180 was
# ~50% of a 2000-batch storm, and K storms of that size do not fit this box's tmpfs.
# Per-storm observations at the current 1000 batches are in the header's table.
#
# If a genuinely slower/faster box needs a different number, override rather than edit:
# GATE_MIN_RESET_ROUNDS / GATE_MIN_RESET_CONNS. Record the observed counts when you do.
MIN_RESET_ROUNDS="${GATE_MIN_RESET_ROUNDS:-35}"
MIN_RESET_CONNS="${GATE_MIN_RESET_CONNS:-100}"
# HOW BIG EACH BENCH IS, and why K STORMS FORCED IT DOWN. Every bench here writes
# `batches * batch-size * 10 fields` points into a cluster that keeps all of them at RF=3,
# and NOTHING is deleted between storms -- the K storms share one cluster by design, so
# each one lands on top of its predecessors. At the 2000 batches this gate used when it ran
# ONE storm that is 200 M points per bench, and MEASURED here: baseline + 2 storms filled
# 34 G of the 62 G tmpfs, with the third storm still to come. The run was killed at that
# point; a fourth bench would have taken it past the quota, and a /tmp quota exhaustion on
# this box does not fail the gate politely -- "Disk quota exceeded" reaches every process
# including the shell driving it.
#
# So the K-storm restructure and the old bench size are not simultaneously affordable, and
# the bench size is the right thing to give up: storm INTENSITY is set by the resetter (it
# destroys EVERY live connection on every round, 0.3 s apart), not by how long the bench
# runs. A shorter bench buys fewer ROUNDS, not weaker ones.
#
# 1000 batches => 100 M points/bench, ~4 benches/run => ~25 G peak, leaving the README's
# headroom intact. The floors above move with it: 146-155 rounds / 400-431 connections
# measured per storm at 2000 batches, so ~73/200 at 1000, and the floors are ~50% of that
# (the resetter fires on a wall clock while bench length is machine-dependent, so a faster
# box legitimately injects fewer rounds).
BENCH_BATCHES="${GATE_BENCH_BATCHES:-1000}"
# The dip bound, as a percentage of the quiet-through-the-proxy baseline.
MIN_DIP_PCT="${GATE_MIN_DIP_PCT:-40}"
# D-21: K storms per run, and a budget on their TOTAL rather than a zero on one draw. The
# floors above are PER STORM and are multiplied by K below, so a run that injects a full
# storm K times is what satisfies them -- three anaemic storms cannot add up to one real
# one.
STORM_ROUNDS="${GATE_STORM_ROUNDS:-3}"
MAX_STORM_ERRORS="${GATE_MAX_STORM_ERRORS:-3}"
# THE COMBINED MODE (debt D-19's second half), off by default. Set by
# `combined_fault_rebalance_gate.sh`, which is where its floors and budget live: a
# leadership rebalance storm runs THROUGH each reset storm, at a raised connection count.
# It is a knob rather than a second script so the proxy topology, the balance-first step
# and the known-count probe are not duplicated and cannot drift apart.
REBALANCE_STORM="${GATE_REBALANCE_STORM:-0}"
MIN_COMBINED_CALLS="${GATE_MIN_COMBINED_CALLS:-0}"
CONNECTIONS="${GATE_CONNECTIONS:-4}"
# A baseline slower than this VOIDS the run rather than failing it (see the header): the
# fault-free control through the proxy runs 4.98-5.16 M pts/s here, and the one collapse
# observed came in at 0.45 M. The floor is ~40% of the low end -- far enough below the
# observed band to never fire on an honestly slower box, far enough above the collapse to
# catch it.
MIN_BASE_TPUT="${GATE_MIN_BASE_TPUT:-2000000}"

# VOID is not a pass and not a failure: it is a run that did not test the property. It gets
# its own exit code so a caller (fault_injection_ab.sh, CI) can re-draw instead of filing a
# regression.
gate_void() {
    echo "  VOID: $*" >&2
    echo "GATE VOID -- this run did not test the property; re-draw it (see the header)"
    exit 3
}

cleanup() {
    [ -n "${PROXY_PID:-}" ] && kill -9 "$PROXY_PID" 2>/dev/null
    gate_cleanup 1941 /tmp/tsgate_fi1 /tmp/tsgate_fi2 /tmp/tsgate_fi3
    rm -f /tmp/tsgate_fi_base.txt /tmp/tsgate_fi_storm.txt /tmp/tsgate_fi_resp.txt \
        /tmp/tsgate_fi_rounds /tmp/tsgate_fi_stop
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
    # IN COMBINED MODE THIS STOPS EARLY, ON PURPOSE, and it is the difference between a
    # combined gate and two gates in a trenchcoat. `/cluster/rebalance-leadership` only
    # sheds leadership held ABOVE fair share, so balancing to fair share here leaves the
    # rebalance storm below with NOTHING TO DO -- measured exactly that way: 129 transfers
    # in the first storm and ZERO in the second, i.e. half the run was an ordinary reset
    # gate. Stopping as soon as enough traffic crosses the proxy preserves the imbalance
    # the storm then works through, while the >= 800 assertion below still holds from the
    # first storm onward.
    if [ "$REBALANCE_STORM" = "1" ]; then
        N3=$(status_field "$(cluster_status 19412)" vshards_led)
        [ "${N3:-0}" -ge 900 ] && { echo "  combined mode: stopping the pre-balance at node 3 = $N3 (imbalance kept for the storm)"; break; }
    fi
done
if [ "$REBALANCE_STORM" != "1" ]; then
    wait_balanced "$PORTS" 4096 3 60 || gate_exit
fi
NODE3_LED=$(status_field "$(cluster_status 19412)" vshards_led)
echo "  node 3 (behind the proxy) leads $NODE3_LED VShards"
assert_ge "VShards led behind the proxy (traffic that must cross the fault)" "${NODE3_LED:-0}" 800

# ---------------------------------------------------------------------------
# Baseline: the same load through the same proxy, with NO resets. Everything below is
# measured against this, not against an unproxied number.
echo "=== baseline (proxy in path, no faults) ==="
timeout 300 "$BENCH" --server-port 19410 -c 4 --batches "$BENCH_BATCHES" --batch-size 10000 --verify 0 \
    --warmup 5 --connections 4 --hosts 1000 --racks 2 >/tmp/tsgate_fi_base.txt 2>&1
# "First error" is printed here too (D-21): when the baseline DOES break, its signature is
# the only evidence of why, and the run that first showed this had none recorded.
grep -E "Requests:|First error|Throughput|batch latency" /tmp/tsgate_fi_base.txt
BASE_TPUT=$(grep -oE 'Throughput:[[:space:]]*[0-9.]+' /tmp/tsgate_fi_base.txt | head -1 | grep -oE '[0-9.]+')
BASE_ERRS=$(grep -o '[0-9]* HTTP errors' /tmp/tsgate_fi_base.txt | head -1 | cut -d' ' -f1)
echo "  baseline throughput: ${BASE_TPUT:-?} (errors ${BASE_ERRS:-?})"
# THE CONTROL ARM, AND IT VOIDS RATHER THAN FAILS (debt D-21). No fault has been injected
# yet, so a broken baseline is a broken environment -- disk headroom, a starved proxy, a
# co-tenant -- and everything downstream of it measures that instead of the server. Failing
# here would be indistinguishable from the property failing, which is precisely the
# confusion that left a red run uninformative.
[ "${BASE_ERRS:-missing}" = "0" ] || gate_void "the fault-free baseline through the proxy took ${BASE_ERRS} client errors"
if [ -n "${BASE_TPUT:-}" ]; then
    awk -v t="$BASE_TPUT" -v m="$MIN_BASE_TPUT" 'BEGIN{exit !(t+0 < m+0)}' &&
        gate_void "the fault-free baseline ran at $BASE_TPUT pts/s, under the ${MIN_BASE_TPUT} floor"
else
    gate_void "could not parse a throughput from the baseline run"
fi
gate_ok "baseline clean through the proxy: $BASE_TPUT pts/s, 0 client errors"

# ---------------------------------------------------------------------------
# The known-count durability probe. A fixed set of points is written DURING the
# turbulence, one per request, so every ack is individually attributable; afterwards the
# range is read back and must contain every acked point -- no loss.
BASE_TS=1750000000000000000
PROBE=200

# K STORMS AGAINST THE SAME CLUSTER (debt D-21). Each iteration is an independent burst:
# its own bench, its own resetter, its own 200-point probe in its own timestamp window, and
# its own read-back. The totals are what the property is asserted on; the per-storm vector
# is printed so the distribution is visible in the log rather than inferred from a pass/fail.
TOT_ROUNDS=0; TOT_CONNS=0; TOT_BENCH_ERRS=0; TOT_PROBE_5XX=0; TOT_PROBE_OTHER=0; TOT_CONN_FAILS=0
TOT_TRANSFERS=0; TOT_REBAL_CALLS=0
WORST_PCT=100
ERR_VECTOR=""; ROUND_VECTOR=""; TPUT_VECTOR=""
PREV_CONNS=0

storm=1
while [ "$storm" -le "$STORM_ROUNDS" ]; do
    echo "=== reset storm $storm/$STORM_ROUNDS under sustained writes ==="
    # Each storm gets its own probe window, 1000 s past the previous one, so a read-back
    # can attribute points to the storm that wrote them. Reusing one window would let a
    # later storm's points cover an earlier storm's loss.
    STORM_TS=$((BASE_TS + (storm - 1) * 1000 * 1000000000))
    # Three independent things run at once, deliberately decoupled: the bench (bulk load),
    # the RESETTER (the fault), and the probe (the countable writes). An earlier version
    # gated the resetter on the bench still running and the bench finished in under a
    # second, so the storm never fired -- the anti-vacuity assertions below exist because
    # of that.
    rm -f /tmp/tsgate_fi_stop /tmp/tsgate_fi_rebal
    ( timeout 300 "$BENCH" --server-port 19410 -c 4 --batches "$BENCH_BATCHES" --batch-size 10000 --verify 0 \
        --warmup 5 --connections "$CONNECTIONS" --hosts 1000 --racks 2 >/tmp/tsgate_fi_storm.txt 2>&1 ) &
    BENCHPID=$!
    ( ROUNDS=0
      while [ ! -f /tmp/tsgate_fi_stop ]; do
          kill -USR1 "$PROXY_PID" 2>/dev/null && ROUNDS=$((ROUNDS + 1))
          sleep 0.3
      done
      echo "$ROUNDS" >/tmp/tsgate_fi_rounds ) &
    RESETPID=$!
    # THE FOURTH THING, when this is the COMBINED gate (debt D-19's second half): a
    # leadership rebalance storm running THROUGH the reset storm. Two faults at once is not
    # the sum of two gates -- a write whose slice is refused by a group mid-transfer has to
    # be re-dispatched over a transport that is simultaneously losing its connections, and
    # the retry budget is shared between both. `combined_fault_rebalance_gate.sh` is the
    # entry point; here it is a knob so the topology, proxy and probe are not duplicated.
    REBALPID=""
    if [ "$REBALANCE_STORM" = "1" ]; then
        ( T=0; C=0
          while [ ! -f /tmp/tsgate_fi_stop ]; do
              for p in $PORTS; do
                  R=$(curl -s -m5 -X POST "http://127.0.0.1:$p/cluster/rebalance-leadership?max=512" 2>/dev/null)
                  N=$(printf '%s' "$R" | grep -o '"transfers_initiated":[0-9]*' | cut -d: -f2)
                  T=$((T + ${N:-0})); C=$((C + 1))
              done
              sleep 0.2
          done
          echo "$T $C" >/tmp/tsgate_fi_rebal ) &
        REBALPID=$!
    fi
    sleep 1

    PROBE_OK=0; PROBE_5XX=0; PROBE_OTHER=0
    i=0
    while [ "$i" -lt "$PROBE" ]; do
        CODE=$(curl -s -m10 -o /tmp/tsgate_fi_resp.txt -w '%{http_code}' -X POST http://127.0.0.1:19410/write \
            -H 'Content-Type: application/json' \
            -d "{\"measurement\":\"faultprobe\",\"tags\":{\"host\":\"p$i\"},\"fields\":{\"v\":1.0},\"timestamp\":$((STORM_TS + i * 1000000000))}")
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
    if [ -n "$REBALPID" ]; then
        wait $REBALPID
        STORM_TRANSFERS=0; STORM_CALLS=0
        [ -s /tmp/tsgate_fi_rebal ] && read -r STORM_TRANSFERS STORM_CALLS </tmp/tsgate_fi_rebal
        TOT_TRANSFERS=$((TOT_TRANSFERS + STORM_TRANSFERS))
        TOT_REBAL_CALLS=$((TOT_REBAL_CALLS + STORM_CALLS))
        echo "  rebalance: $STORM_TRANSFERS transfers over $STORM_CALLS calls, DURING the reset storm"
    fi
    ROUNDS=$(cat /tmp/tsgate_fi_rounds 2>/dev/null || echo 0)
    echo "  probe writes: $PROBE_OK ok, $PROBE_5XX 5xx, $PROBE_OTHER other (of $i attempted)"
    grep -E "Requests:|First error|Throughput|batch latency" /tmp/tsgate_fi_storm.txt

    # The proxy's RESET lines are CUMULATIVE across the whole run, so this storm's share is
    # the delta. Summing the file per storm would count every earlier storm again and make
    # the per-storm floors trivially satisfiable by the last iteration.
    ALL_CONNS=$(awk '/^RESET /{s+=$2} END{print s+0}' "$PROXY_LOG")
    RESET_CONNS=$((ALL_CONNS - PREV_CONNS))
    PREV_CONNS=$ALL_CONNS
    echo "  reset rounds: $ROUNDS, connections destroyed: $RESET_CONNS"

    STORM_ERRS=$(grep -o '[0-9]* HTTP errors' /tmp/tsgate_fi_storm.txt | head -1 | cut -d' ' -f1)
    STORM_CONN=$(grep -o '[0-9]* connection failures' /tmp/tsgate_fi_storm.txt | head -1 | cut -d' ' -f1)
    STORM_TPUT=$(grep -oE 'Throughput:[[:space:]]*[0-9.]+' /tmp/tsgate_fi_storm.txt | head -1 | grep -oE '[0-9.]+')
    # A bench that produced no parseable counter at all is a harness failure, not a zero.
    [ -n "${STORM_ERRS:-}" ] && [ -n "${STORM_TPUT:-}" ] ||
        gate_void "storm $storm produced no parseable bench result (timeout? see /tmp/tsgate_fi_storm.txt)"

    TOT_ROUNDS=$((TOT_ROUNDS + ROUNDS))
    TOT_CONNS=$((TOT_CONNS + RESET_CONNS))
    TOT_BENCH_ERRS=$((TOT_BENCH_ERRS + STORM_ERRS))
    TOT_CONN_FAILS=$((TOT_CONN_FAILS + ${STORM_CONN:-0}))
    TOT_PROBE_5XX=$((TOT_PROBE_5XX + PROBE_5XX))
    TOT_PROBE_OTHER=$((TOT_PROBE_OTHER + PROBE_OTHER))
    ERR_VECTOR="$ERR_VECTOR $((STORM_ERRS + PROBE_5XX))"
    ROUND_VECTOR="$ROUND_VECTOR $ROUNDS/$RESET_CONNS"

    PCT=$(awk -v a="$STORM_TPUT" -v b="$BASE_TPUT" 'BEGIN{ if (b+0==0) print 0; else printf "%d", 100*a/b }')
    TPUT_VECTOR="$TPUT_VECTOR ${PCT}%"
    [ "$PCT" -lt "$WORST_PCT" ] && WORST_PCT=$PCT
    echo "  throughput under storm $storm: $STORM_TPUT vs baseline $BASE_TPUT ($PCT%)"

    # NO LOSS. Every acked probe point must be readable on EVERY node -- an ack means a
    # durable quorum commit, so a follower read after the storm must agree.
    #
    # THE BOUND IS A BAND, NOT AN EQUALITY, and the difference is the ack contract rather
    # than sloppiness. A retryable 5xx means UNKNOWN, not "did not happen": the slice may
    # have committed before the error was raised. Measured exactly that way -- a run with 3
    # probe 5xx read back 200 of 200 attempted, and the old `== PROBE_OK` assertion called
    # that a failure three times over. What must never happen is FEWER than the acked count
    # (loss) or MORE than were attempted (fabrication or a double count).
    sleep 3
    for p in $PORTS; do
        RESP=$(curl -s -m20 -X POST "http://127.0.0.1:$p/query" -H 'Content-Type: application/json' \
            -d "{\"query\":\"count:faultprobe(v){}\",\"startTime\":$STORM_TS,\"endTime\":$((STORM_TS + PROBE * 1000000000))}")
        N=$(printf '%s' "$RESP" | grep -o '"point_count":[0-9]*' | cut -d: -f2)
        echo "  storm $storm read-back on :$p -> point_count=${N:-<none>}"
        assert_ge "storm $storm: acked probe points readable on :$p (acked $PROBE_OK)" "${N:-0}" "$PROBE_OK"
        assert_le "storm $storm: probe points readable on :$p (attempted $PROBE)" "${N:-999999}" "$PROBE"
    done

    storm=$((storm + 1))
done

# ---------------------------------------------------------------------------
echo "=== $STORM_ROUNDS storms: errors[$ERR_VECTOR ] rounds/conns[$ROUND_VECTOR ] throughput[$TPUT_VECTOR ] ==="

# MACHINE-READABLE TOTALS, printed BEFORE the assertions so they survive a FAILING run --
# which is the case that matters, because `fault_injection_ab.sh`'s whole job is to read
# the numbers off an arm that is EXPECTED to fail.
#
# It used to scrape the assertion lines instead, and that silently did not work: the
# assertion is named "client errors across the reset storms (bench + probe, K storms)"
# while the A/B looked for "client errors across the reset storms = ", so the `grep -F`
# never matched and BOTH arms parsed as empty -- the A/B then compared `${R_TOTAL:-0}`
# against `${H_TOTAL:-999}` and reported a separation it had not measured. Assertion text
# is prose and will be reworded again; these lines are the contract.
echo "GATE_METRIC storm_errors_total $((TOT_BENCH_ERRS + TOT_PROBE_5XX))"
echo "GATE_METRIC storm_bench_errors $TOT_BENCH_ERRS"
echo "GATE_METRIC storm_probe_5xx $TOT_PROBE_5XX"
echo "GATE_METRIC reset_rounds_total $TOT_ROUNDS"
echo "GATE_METRIC reset_conns_total $TOT_CONNS"
echo "GATE_METRIC storm_count $STORM_ROUNDS"
echo "GATE_METRIC worst_storm_pct $WORST_PCT"
echo "GATE_METRIC rebalance_transfers $TOT_TRANSFERS"
echo "GATE_METRIC rebalance_calls $TOT_REBAL_CALLS"
echo "GATE_METRIC bench_connections $CONNECTIONS"

# THE ANTI-VACUITY ASSERTIONS. Without a real storm this gate proves nothing: a proxy that
# never fired, or one that fired while no connection was open, would otherwise pass. The
# floors are PER STORM and scaled by K here, so K feeble storms cannot substitute for one
# real one.
assert_ge "reset rounds injected" "$TOT_ROUNDS" "$((MIN_RESET_ROUNDS * STORM_ROUNDS))"
assert_ge "peer connections actually destroyed" "$TOT_CONNS" "$((MIN_RESET_CONNS * STORM_ROUNDS))"
# In combined mode the SECOND fault needs its own anti-vacuity floor, and it is on the CALLS
# rather than on the transfers -- which is a MEASUREMENT, not a weakening.
#
# The obvious assertion (transfers >= N) fails on correct behaviour here. Measured over two
# runs: with the cluster deliberately left imbalanced (1536/1536/1024 against a fair 1365)
# and 420 rebalance calls issued THROUGH the reset storms, `transfers_initiated` came to
# 0 and 13. The same endpoint on a healthy transport moves thousands (2175-2901 in
# `skewed_rebalance_gate.sh`). The difference is the fault: the only node below fair share
# is the one behind the proxy, its acks are being destroyed every 0.3 s, so D-1's liveness
# filter reads it as dead and REFUSES to hand it groups. A balancer that shipped leadership
# to a peer whose connection is being reset is precisely what D-1 exists to prevent.
#
# So the anti-vacuity check is that the storm loop actually ran, and the transfer count is
# reported as the finding. (D-18 records D-1's production effect as reasoned and
# unit-pinned rather than measured; this is the first live measurement of the filter
# ACTING, and it is on the register's D-18 row.)
if [ "$REBALANCE_STORM" = "1" ]; then
    assert_ge "rebalance calls issued DURING the reset storms" "$TOT_REBAL_CALLS" "$MIN_COMBINED_CALLS"
    echo "  (finding) $TOT_TRANSFERS leadership transfers initiated across $TOT_REBAL_CALLS calls --"
    echo "            near-zero is CORRECT: the only under-share node is the one behind the"
    echo "            reset proxy, and D-1's liveness filter refuses a peer that is not acking."
fi

# THE PROPERTY, as an aggregate over K draws (debt D-21). A reset burst against a LIVE peer
# must be absorbed by the transport's reconnect plus the 4a retry pacing, inside the 1.5 s
# write deadline. The budget is on the TOTAL because a single draw is not reproducible in
# either direction; the per-storm vector above is what to read when it is exceeded.
assert_le "client errors across the reset storms (bench + probe, $STORM_ROUNDS storms)" \
    "$((TOT_BENCH_ERRS + TOT_PROBE_5XX))" "$MAX_STORM_ERRORS"
# NON-HTTP failures are not budgeted: a client that could not complete a request at all is
# a different class from a server that answered 503 -- the peer under fault is HEALTHY and
# its listener never closes, so the coordinator the client talks to has no reason to drop a
# connection. None has ever been observed here, in either arm of the A/B.
assert_eq "probe non-HTTP failures" "$TOT_PROBE_OTHER" 0
assert_eq "bench client connection failures" "$TOT_CONN_FAILS" 0
# NEITHER IS A 500. The whole point of 4a is that this fault is retryable and says so;
# an opaque 500 is a defect at any rate.
assert_eq "server-side 500s" "$(cat /tmp/tsgate_fi*/s.log | grep -c 'Error handling write request')" 0
assert_eq "node crashes" "$(grep -l 'Segmentation fault' /tmp/tsgate_fi*/s.log 2>/dev/null | wc -l)" 0

# BOUNDED DIP, against the proxied baseline (see the header), on the WORST storm.
assert_ge "throughput retained vs the proxied baseline, worst storm (%)" "$WORST_PCT" "$MIN_DIP_PCT"

gate_exit
