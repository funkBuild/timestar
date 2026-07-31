#!/bin/bash
# GATE (debt D-18): a leadership rebalance under a SKEWED write workload -- the load
# concentrated on a HANDFUL of Raft groups instead of spread over all 4096 -- must still
# cost latency, not client errors.
#
# WHY THIS IS NOT rolling_rebalance_gate.sh WITH DIFFERENT NUMBERS. That gate offers
# `--hosts 1000 --racks 2`, i.e. 20 000 series over 4096 groups, so every group carries a
# trickle and at the instant the balancer arms a transfer a given group typically has
# NOTHING in flight: matchIndex has already caught lastIndex. The behaviour D-1 is about --
# matchIndex trailing lastIndex CONTINUOUSLY on a group under sustained writes -- is
# therefore unreachable there, which is exactly why that gate's numbers came out
# byte-identical on binaries with and without the D-1 fix, and why D-18 was filed.
#
# Here `--hosts 4 --racks 1` gives 4 tagsets x 10 field names = 40 series, so the WHOLE
# offered load lands on at most 40 of 4096 groups (~1%). Each of those groups sees roughly
# 100x the per-group proposal rate of the rolling gate and is continuously mid-append while
# the rebalance storm runs. That is the state D-1's lag filter exists for, and the state a
# transfer armed at a peer that has not acked the tail produces D-20's abandon window in.
#
# WHAT IT ASSERTS, and it is deliberately the same PROPERTY as the rolling gate on a
# different workload, not a new one: writes stay available (no client HTTP errors, no
# opaque 500s, no crashes) while leadership genuinely moves (an anti-vacuity floor on
# `transfers_initiated` AND on the leadership the joining node ends up with), and a
# known-count probe written INTO THE HOT GROUPS during the storm is fully readable
# afterwards on every node.
#
# IT RUNS A CONTROL BENCH FIRST, AND THAT IS NOT CEREMONY -- it is what the first
# calibration run made necessary. Concentrating the rolling gate's offered load
# (8 connections x 100 000 points per batch) onto 39 groups SATURATES them: measured 210 of
# 400 batches refused with retryable 503s, median batch latency 1507 ms against the router's
# 1500 ms deadline, 359 k pts/s where the spread load does 5 M, and 30 G of tmpfs consumed
# in 95 s. Every one of those errors was the SKEW, not the rebalance, and a gate that
# asserted zero errors under the storm alone would have reported an overload as a
# regression. So the load is sized to sit BELOW the hot groups' ceiling and the control
# proves it does: an error in the control ABORTS (exit 3) rather than failing, exactly as
# the fault gate's fault-free baseline does, and the storm arm's numbers are read against
# the control's rather than against absolutes.
#
# THE CONCENTRATION IS MEASURED PER VSHARD, not assumed. `--hosts 4` is an input; what the
# gate checks is the OUTPUT -- the per-VShard journal directories, of which the ones that
# actually took traffic must be a SMALL set. Without that check a future change to the
# bench's series mapping (or to the VShard hash) could silently spread the load again and
# this gate would quietly become a second copy of the rolling one, which is the precise
# failure D-18 describes.
#
# WHAT IT DOES NOT PROVE, stated plainly because D-18 asks for it: that leadership of THOSE
# SPECIFIC groups moved. Nothing exposes per-VShard leadership -- `/cluster/status` reports
# `vshards_led` as a COUNT and `/cluster/rebalance-leadership` reports only a before/after
# count, so from outside the process a hot group and an idle one are indistinguishable. The
# gate therefore asserts hot-group concentration and AGGREGATE leadership movement, and the
# residual is one read-only endpoint away: a `GET /cluster/vshards?led=1` returning the ids
# this node leads would let this script intersect them with the hot set measured below and
# close D-18's wording exactly. Until then the hot groups' own leadership is inferred from
# the fact that the balancer's cursor sweeps all 4096.
#
# WHAT IT FOUND, which is the reason it exists, and it is a COMPOSITION of two failures
# rather than either one. Under the SPREAD load the rolling gate offers, an identical storm
# produces ZERO client errors. Under this one it produces a handful of retryable 503s.
# Reading only the node log, the story looks like the abandon window:
#
#   cluster: this node LEADS VShard 1304 and refused a proposal (1 such refusals so far);
#   a leadership transfer is in flight
#
# Those refusals are real and this gate counts them. But they are NOT what failed the
# batches, and the gate's own output says so: every give-up reports `(last: transport)`,
# which is `writeFailureName(WriteFailure::Transport)` -- a LeaderRefused give-up would
# print `leader-refused-mid-transfer`. What actually happens is both, in order:
#
#   1. A rebalance-armed group refuses the slice. LeaderRefused is election-SHAPED, so
#      `ReplicatedBatchWriteRouter` widens the budget for the next decision from
#      kMaxAttempts/kDeadline (6 attempts, 1.5 s) to kMaxElectionAttempts/kElectionDeadline
#      (16 attempts, 6 s) -- the batch is now waiting out what might be an election.
#   2. Inside that widened window a slice fails on the TRANSPORT (an attempt timing out
#      against a hot group that is saturated, here; no fault is injected). `electionWaitOnly`
#      is recomputed per attempt and is deliberately NOT sticky, so the budget collapses
#      back to 6 attempts in the same breath -- and the attempt counter is already past 6,
#      so the give-up is IMMEDIATE, with no further retry, reporting `last: transport`.
#
# The attempt counts are the proof and they are asserted below: 8, 9, 13 and 14 attempts
# were observed, all of them > kMaxAttempts (6), which is reachable ONLY if earlier attempts
# were election-shaped. So "the 1.5 s deadline expired" is the wrong citation -- the
# governing bound while the batch waits is kElectionDeadline (6 s) / 16 attempts, and the
# 1.5 s figure only becomes relevant at the instant it kills the batch.
#
# IS IT A REGRESSION FROM D-20? THIS GATE CANNOT TELL, and that is the measured answer
# rather than the argued one. A binary with the pre-D-20 window restored (kRaftTransferTicks
# back to kRaftElectionTicksMin - 1 = 2.48 s, both D-20 fence layers disarmed) was built and
# run against this gate, interleaved with the current binary on the same box in the same
# session:
#
#     arm            503s of 500   transfers   joiner led   retained
#     1 s (current)  17            2581        1365         22 %
#     2.48 s (pre)   16            3522        1254         31 %
#     1 s (current)  11            2307        1365         14 %
#     2.48 s (pre)   8             1961        1364         24 %
#
# The pre-D-20 draws (8 and 16) sit INSIDE the current binary's own spread on the identical
# configuration (0, 5, 6, 24, 13, 0, 17, 11 across eight runs). So the window is not
# separable here in either direction: there is no evidence the current binary is worse --
# which is what a regression would look like -- and equally no evidence for the "the old
# window produces strictly more" reasoning this comment used to assert as measured. The
# claim is withdrawn, not restated: D-20 rests on its unit tests and on the timing
# assertions, NOT on this gate.
#
# What a sensitive measurement needs is the counter the server already keeps and does not
# publish: `ReplicatedVShardHost::proposeRefusedWhileLeader()` counts exactly these
# refusals, but it is not on `/cluster/status`, so from outside the only view is a WARN line
# rate-limited to one per node per 5 s. The two arms' refusal counters differed by 165 to 1
# on one pairing and not at all on the other, which is the instrument failing, not the
# window behaving inconsistently. Publishing that counter would make this a real
# measurement; it is the same missing-operator-surface residual as the per-VShard
# leadership one above.
#
# It is NOT loss either way: the probe reads back 150/150 on every node on every run of
# both arms.
#
# Usage: skewed_rebalance_gate.sh [SERVER_BINARY]
#   GATE_MIN_TRANSFERS=N   anti-vacuity floor on transfers_initiated (default 800)
#   GATE_MAX_STORM_5XX=N   ceiling on retryable 503s under the storm (default 100 of 500)
#   GATE_MIN_DIP_PCT=N     throughput floor vs the control, percent (default 10)
#   GATE_HOSTS=N           simulated hosts; the skew knob (default 4 => 40 series)
#   GATE_KEEP_DATA=1       keep the data dirs (they are deleted on exit by default)
#   exit 0 = pass, 1 = property failed, 2 = setup refused, 3 = VOID (control arm failed)
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
BENCH="$BUILD_DIR/bin/timestar_insert_bench"
[ -x "$BENCH" ] || { echo "no insert bench at $BENCH"; exit 2; }

PORTS="19240 19241 19242"
# Anti-vacuity: without real transfers this gate is just a skewed write bench. Measured
# 2175 / 2298 / 2901 / 2196 across the four calibration runs (105-228 endpoint calls), so
# the floor is ~a third of the lowest.
MIN_TRANSFERS="${GATE_MIN_TRANSFERS:-800}"
HOSTS="${GATE_HOSTS:-4}"
# THE LOAD IS SIZED TO THE 39 GROUPS IT LANDS ON, not copied from the rolling gate. See the
# header: the rolling gate's `--connections 8 --batch-size 10000` concentrated here
# saturates them (210/400 batches refused, median latency at the 1500 ms write deadline,
# 30 G of tmpfs in 95 s). 4 connections x 20 000 points per batch keeps every hot group
# continuously mid-append -- which is the state this gate exists to create -- without
# driving it into its own ceiling. Measured peak with the control arm included: 4.6 G.
BATCHES="${GATE_BATCHES:-500}"
BATCH_SIZE="${GATE_BATCH_SIZE:-2000}"
CONNECTIONS="${GATE_CONNECTIONS:-4}"
PROBE="${GATE_PROBE:-150}"
# The joining node must end up with a REAL share, not a token one. Fair share of 4096 over
# three nodes is 1365, and the storm reaches it: 1364-1365 on all four calibration runs
# (1352 on the oversized first one). The floor is 900, i.e. 66% of fair share -- far below
# what a working balancer reaches and far above the handful a broken one would.
MIN_JOINER_LED="${GATE_MIN_JOINER_LED:-900}"
# A VShard journal bigger than this took real traffic. An idle group's directory holds a
# HardState record and nothing else (tens of KB); a hot one holds the whole campaign.
HOT_KB="${GATE_HOT_KB:-1024}"
# The hot set must stay SMALL -- this is what makes the workload skewed rather than merely
# different. 40 series can touch at most 40 groups, and every run has measured exactly 39,
# so the bound is 60: tight enough to FALSIFY (160 could not be reached by 40 series under
# any hash and so asserted nothing) while leaving room for a hash change that collides
# differently.
MAX_HOT_VSHARDS="${GATE_MAX_HOT_VSHARDS:-60}"
# THE RETRYABLE-503 CEILING AND THE DIP FLOOR, both CALIBRATED, both bands rather than
# absolutes -- see the long note at the assertions for why a zero would be wrong here.
# Three runs of this exact configuration (500 batches x 20 000 points, 4 connections):
#
#     run                 2      3      4      5      6      7      8      9
#     retained            29 %   16 %   22 %   44 %   30 %   43 %   22 %   14 %
#     retryable 503s      6      5      24     0      13     0      17     11
#     transfers           2175   2298   2901   2196   2532   2201   2581   2307
#     hot VShards         39     39     39     39     39     39     39     39
#     joiner led          1364   1364   1364   1365   1364   1364   1365   1365
#
#     control             1.32-3.43 M pts/s, ZERO client errors on all eight
#     probe read-back     150/150 on every node, all eight runs
#     failure class       transport at attempt 8 wherever there were errors at all
#
# (A reviewer's independent run of the same configuration drew 20, inside this band.)
#
# THE CEILING IS A COARSE TRIPWIRE, NOT A TIGHT BOUND, and it is set that way on purpose --
# D-21's whole lesson is that a threshold fitted to a handful of draws of a heavy-tailed
# count gets crossed by the good binary. Identical input has drawn 0, 0, 5, 6, 11, 13, 17,
# 20 and 24 here;
# a ~2x-the-worst ceiling (50) is exactly the shape that failed for D-21 at 3, so it is 100
# -- 20 % of the batches, still an order of magnitude below the saturation regime the
# oversized first calibration run produced (210 of 400, 52 %).
#
# The tight, falsifiable assertion is the FAILURE CLASS below, not this number: a give-up
# must name a retryable class, and the (class, attempt-count) pair distinguishes the
# election-extension composition from a plain transport failure. That is what would catch a
# change in KIND; the ceiling only catches a change in ORDER OF MAGNITUDE.
MAX_STORM_5XX="${GATE_MAX_STORM_5XX:-100}"
MIN_DIP_PCT="${GATE_MIN_DIP_PCT:-10}"

kill_cluster 1924
require_ports_free 19240 19241 19242
for i in 1 2 3; do rm -rf "/tmp/tsgate_sk$i"; mkdir -p "/tmp/tsgate_sk$i"; done
PEERS="127.0.0.1:19240,127.0.0.1:19241,127.0.0.1:19242"
start_node() {
    env TIMESTAR_DATA_DIR="/tmp/tsgate_sk$1" TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_NODE_ID=$1 TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port $((19239 + $1)) --smp 4 >>"/tmp/tsgate_sk$1/s.log" 2>&1 &
}
trap 'gate_cleanup 1924 /tmp/tsgate_sk1 /tmp/tsgate_sk2 /tmp/tsgate_sk3; rm -f /tmp/tsgate_sk_bench.txt /tmp/tsgate_sk_control.txt /tmp/tsgate_sk_resp.txt /tmp/tsgate_sk_transfers' EXIT

# Two-node phase: a 2-of-3 quorum elects leaders for every VShard between nodes 1 and 2, so
# node 3 joins with nothing and every rebalance call has real work to do. (Storming an
# already-balanced cluster initiates ZERO transfers and proves nothing -- the same reason
# rolling_rebalance_gate.sh starts its third node last.)
start_node 1; start_node 2
for _ in $(seq 1 90); do
    sleep 2
    A=$(status_field "$(cluster_status 19240)" vshards_led)
    B=$(status_field "$(cluster_status 19241)" vshards_led)
    X=$(status_field "$(cluster_status 19240)" vshards_leaderless)
    [ -z "$A" ] && continue
    [ "$(( ${A:-0} + ${B:-0} ))" -ge 4080 ] && [ "${X:-9999}" -le 8 ] && break
done
echo "  before node 3 joins: led=[$(status_field "$(cluster_status 19240)" vshards_led) $(status_field "$(cluster_status 19241)" vshards_led)] (fair share 1365)"

start_node 3
sleep 8
echo "  node 3 joined with ~no leadership: led=[$(status_field "$(cluster_status 19240)" vshards_led) $(status_field "$(cluster_status 19241)" vshards_led) $(status_field "$(cluster_status 19242)" vshards_led)]"

# ---------------------------------------------------------------------------
# THE CONTROL: the identical skewed load with NO rebalance running. Everything below is
# measured against this. An error here is not a property failure -- it means the load is
# too heavy for this box's hot groups, so the storm arm would be measuring saturation.
# VOID (exit 3) says so, and re-sizing is `GATE_CONNECTIONS` / `GATE_BATCH_SIZE`.
gate_void() {
    echo "  VOID: $*" >&2
    echo "GATE VOID -- this run did not test the property; the control arm failed"
    exit 3
}
echo "=== control: the same skewed load with NO rebalance ($HOSTS hosts x 1 rack x 10 fields) ==="
timeout 300 "$BENCH" --server-port 19240 -c 4 --batches "$BATCHES" --batch-size "$BATCH_SIZE" --verify 0 \
    --warmup 5 --connections "$CONNECTIONS" --hosts "$HOSTS" --racks 1 >/tmp/tsgate_sk_control.txt 2>&1
grep -E "Requests:|First error|Throughput|batch latency" /tmp/tsgate_sk_control.txt
CTL_ERRS=$(grep -o '[0-9]* HTTP errors' /tmp/tsgate_sk_control.txt | head -1 | cut -d' ' -f1)
CTL_TPUT=$(grep -oE 'Throughput:[[:space:]]*[0-9.]+' /tmp/tsgate_sk_control.txt | head -1 | grep -oE '[0-9.]+')
[ "${CTL_ERRS:-missing}" = "0" ] ||
    gate_void "the rebalance-free control took ${CTL_ERRS:-<unparseable>} client errors -- the skewed load is saturating its ${HOSTS}x10 groups on this box, so the storm arm would measure that. Lower GATE_CONNECTIONS or GATE_BATCH_SIZE."
[ -n "${CTL_TPUT:-}" ] || gate_void "could not parse a throughput from the control run"
gate_ok "control clean under the skew: $CTL_TPUT pts/s, 0 client errors"

# ---------------------------------------------------------------------------
echo "=== rebalance storm under the SAME skewed load ==="
BASE_TS=1760000000000000000
( timeout 300 "$BENCH" --server-port 19240 -c 4 --batches "$BATCHES" --batch-size "$BATCH_SIZE" --verify 0 \
    --warmup 5 --connections "$CONNECTIONS" --hosts "$HOSTS" --racks 1 >/tmp/tsgate_sk_bench.txt 2>&1 ) &
BENCHPID=$!
sleep 2

# The storm runs in its own subshell so the probe below can run against it concurrently:
# the point of this gate is a write meeting a group that is BOTH mid-append and
# mid-transfer, which needs all three things live at once.
rm -f /tmp/tsgate_sk_transfers
( T=0; C=0
  while kill -0 $BENCHPID 2>/dev/null; do
      for p in $PORTS; do
          R=$(curl -s -m5 -X POST "http://127.0.0.1:$p/cluster/rebalance-leadership?max=512" 2>/dev/null)
          N=$(printf '%s' "$R" | grep -o '"transfers_initiated":[0-9]*' | cut -d: -f2)
          T=$((T + ${N:-0})); C=$((C + 1))
      done
      sleep 0.2
  done
  echo "$T $C" >/tmp/tsgate_sk_transfers ) &
STORMPID=$!

# THE PROBE GOES INTO A HOT GROUP, and that is the whole reason it names the bench's own
# series rather than a private one. The VShard is chosen by a hash of the FULL series key
# (measurement + tags + field), so a probe with its own measurement would land on a
# quiet group and measure nothing this gate is about. `server.metrics` /
# {host-01, rack-1} / cpu_usage is the bench's first series, so these writes queue behind
# its proposals in the same Raft log. The timestamps sit far past the bench's window so the
# read-back below can count them exactly.
PROBE_OK=0; PROBE_5XX=0; PROBE_OTHER=0
i=0
while [ "$i" -lt "$PROBE" ]; do
    CODE=$(curl -s -m10 -o /tmp/tsgate_sk_resp.txt -w '%{http_code}' -X POST http://127.0.0.1:19240/write \
        -H 'Content-Type: application/json' \
        -d "{\"measurement\":\"server.metrics\",\"tags\":{\"host\":\"host-01\",\"rack\":\"rack-1\"},\"fields\":{\"cpu_usage\":1.0},\"timestamp\":$((BASE_TS + i * 1000000000))}")
    case "$CODE" in
        2*) PROBE_OK=$((PROBE_OK + 1)) ;;
        5*) PROBE_5XX=$((PROBE_5XX + 1))
            [ "$PROBE_5XX" = "1" ] && echo "  first probe 5xx ($CODE): $(head -c 200 /tmp/tsgate_sk_resp.txt)" ;;
        *)  PROBE_OTHER=$((PROBE_OTHER + 1)) ;;
    esac
    i=$((i + 1))
done
wait $BENCHPID
wait $STORMPID
# A missing file means the storm subshell died; do not let `set -u` turn that into an
# obscure unbound-variable abort three lines later -- the floor below will report 0
# transfers, which is the honest result.
TRANSFERS=0; CALLS=0
[ -s /tmp/tsgate_sk_transfers ] && read -r TRANSFERS CALLS </tmp/tsgate_sk_transfers

echo "  rebalance calls: $CALLS, transfers initiated: $TRANSFERS"
echo "  probe writes into the hot group: $PROBE_OK ok, $PROBE_5XX 5xx, $PROBE_OTHER other (of $PROBE)"
grep -E "Requests:|First error|Throughput|batch latency" /tmp/tsgate_sk_bench.txt
LED1=$(status_field "$(cluster_status 19240)" vshards_led)
LED2=$(status_field "$(cluster_status 19241)" vshards_led)
LED3=$(status_field "$(cluster_status 19242)" vshards_led)
echo "  final led=[$LED1 $LED2 $LED3] (fair share 1365)"

# ---------------------------------------------------------------------------
# HOW SKEWED WAS IT, PER VSHARD. Measured, not assumed -- see the header. Each VShard keeps
# its own Raft journal directory, so a group's traffic is its directory's size.
HOT=0; TOTAL_VS=0
JROOT=/tmp/tsgate_sk1/cluster_raft
if [ -d "$JROOT" ] && [ -d "$JROOT/vshard_0" ]; then
    TOTAL_VS=$(find "$JROOT" -maxdepth 1 -name 'vshard_*' -type d | wc -l)
    HOT=$(du -sk "$JROOT"/vshard_* 2>/dev/null | awk -v t="$HOT_KB" '$1 > t' | wc -l)
    echo "  per-VShard journals: $HOT of $TOTAL_VS carry more than ${HOT_KB} KB"
    echo "  largest journals: $(du -sk "$JROOT"/vshard_* 2>/dev/null | sort -rn | head -3 |
        awk '{n=split($2,p,"/"); printf "%s=%sKB ", p[n], $1}')"
    # BOTH DIRECTIONS. Zero hot groups means the load never landed (or the layout moved and
    # this measurement is silently reading nothing); more than the bound means the workload
    # is not skewed and this gate has become a duplicate of the rolling one.
    assert_ge "VShards that took real traffic (anti-vacuity)" "$HOT" 1
    assert_le "VShards that took real traffic (the SKEW: $HOSTS hosts x 10 fields)" "$HOT" "$MAX_HOT_VSHARDS"
else
    # TIMESTAR_CLUSTER_SHARED_JOURNAL=1 puts every group in one per-shard journal, so there
    # is nothing per-VShard to measure. Say so rather than asserting on an empty count.
    echo "  (per-VShard journals absent -- shared-journal layout? skew not measured)"
fi

# ---------------------------------------------------------------------------
# THE PROPERTY. Same as the rolling gate's, on the workload it cannot reach.
assert_ge "leadership transfers initiated mid-bench" "$TRANSFERS" "$MIN_TRANSFERS"
assert_ge "leadership the joining node ended up with (fair share 1365)" "${LED3:-0}" "$MIN_JOINER_LED"

HTTP_ERRS=$(grep -o '[0-9]* HTTP errors' /tmp/tsgate_sk_bench.txt | head -1 | cut -d' ' -f1)
CONN_FAILS=$(grep -o '[0-9]* connection failures' /tmp/tsgate_sk_bench.txt | head -1 | cut -d' ' -f1)
STORM_TPUT=$(grep -oE 'Throughput:[[:space:]]*[0-9.]+' /tmp/tsgate_sk_bench.txt | head -1 | grep -oE '[0-9.]+')
# LATENCY IS RECORDED AND DELIBERATELY NOT BOUNDED HERE, which is a stated gap rather than
# an oversight. Measured p99 under the storm reaches 3460 ms against a control p99 of 88 ms
# -- a 39x tail -- and a batch that exceeds the router's budget already surfaces as one of
# the 503s bounded above, so a p99 bound would mostly re-assert the error ceiling. Bounding
# the tail honestly needs its own calibration across more runs than four; until then the
# numbers are printed so a drift is visible.
# `sed` and not a second `grep -oE '[0-9.]+'`: the LABEL contains digits, so that pipeline
# returns "99" from "p99" before the value and the variable ends up holding two lines.
CTL_P99=$(grep -oE 'p99=[[:space:]]*[0-9.]+' /tmp/tsgate_sk_control.txt | head -1 | sed 's/.*p99=[[:space:]]*//')
STORM_P99=$(grep -oE 'p99=[[:space:]]*[0-9.]+' /tmp/tsgate_sk_bench.txt | head -1 | sed 's/.*p99=[[:space:]]*//')
echo "  batch latency p99: control ${CTL_P99:-?} ms -> storm ${STORM_P99:-?} ms (NOT bounded by this gate)"
STORM_PCT=$(awk -v a="${STORM_TPUT:-0}" -v b="${CTL_TPUT:-0}" 'BEGIN{ if (b+0==0) print 0; else printf "%d", 100*a/b }')
echo "  throughput under the storm: ${STORM_TPUT:-?} vs control ${CTL_TPUT:-?} (${STORM_PCT}%)"
# THE CLIENT-ERROR BOUND IS A BAND, NOT A ZERO, AND THAT IS THIS GATE'S FINDING (D-18).
# `rolling_rebalance_gate.sh` asserts ZERO client errors under an identical storm and gets
# it, because its 20 000 series spread over 4096 groups make any single group idle at the
# moment a transfer is armed. Concentrate the load onto 39 groups and the same storm DOES
# produce retryable 503s -- measured, with the mechanism named verbatim in the node log:
#
#   cluster: this node LEADS VShard 1304 and refused a proposal (1 such refusals so far);
#   a leadership transfer is in flight
#
# That is the D-20 abandon window (a group refuses proposals while a transfer it armed is
# outstanding) meeting a group that is never idle, which is exactly the state D-18 says
# nothing measured. It is NOT a regression from D-20 -- that row SHORTENED the window from
# one election timeout to 1 s, so it strictly reduces this -- and it is not loss: the
# refusals are retryable 503s, the probe reads back 150/150 on every node, and the router
# retried 8-14 times before its 1.5 s deadline ran out.
#
# So the gate asserts what actually holds: no opaque 500s, no loss, and a BOUNDED number of
# retryable refusals. A zero here would be a gate that fails on correct behaviour, and an
# unbounded band would not notice the window growing again.
assert_le "client HTTP errors (retryable 503s under the storm)" "${HTTP_ERRS:-999}" "$MAX_STORM_5XX"

# THE FAILURE CLASS IS THE FALSIFIABLE PART, and it is what the count ceiling above is not.
# `RetryableWriteError` names the class of the LAST failure and the attempt it gave up on,
# so the composition in the header can be read straight off a run instead of inferred.
# `head -1` on BOTH, and it is not defensive padding: the bench prints the error body TWICE
# on one line (the `message` and `error` fields of the JSON both carry it), so `grep -o`
# returns two matches from a single `grep -m1` line and the variable ends up holding
# "transport\ntransport". That got as far as a run, where the `case` below matched neither
# alternative and failed the gate with a truncated message.
ERR_CLASS=$(grep -m1 -oE '\(last: [a-z-]+\)' /tmp/tsgate_sk_bench.txt | head -1 | tr -d '()' | cut -d' ' -f2)
ERR_ATTEMPTS=$(grep -m1 -oE 'after [0-9]+ attempt\(s\)' /tmp/tsgate_sk_bench.txt | head -1 | grep -oE '[0-9]+')
# The refusal line carries a RUNNING COUNT ("N such refusals so far") and is not emitted on
# every refusal, so counting LINES undercounts badly -- one line was logged against 13
# client errors. Take the largest counter any node reported instead.
REFUSALS=$(cat /tmp/tsgate_sk*/s.log 2>/dev/null | grep -oE '\(([0-9]+) such refusals so far\)' |
    grep -oE '[0-9]+' | sort -rn | head -1)
REFUSALS="${REFUSALS:-0}"
echo "  failure class: ${ERR_CLASS:-<none, no client errors>} after ${ERR_ATTEMPTS:-0} attempts;"
echo "  abandon-window refusals (largest per-node counter logged): $REFUSALS"
if [ -n "${ERR_CLASS:-}" ]; then
    # HARD: a give-up must name a RETRYABLE class. `fatal` or `unassigned` here would mean
    # the storm is producing something the client cannot retry, which is a different and
    # much worse property than the bounded 503s this gate tolerates.
    case "$ERR_CLASS" in
        transport|leader-refused-mid-transfer|not-leader|leadership-lost|overloaded)
            gate_ok "first failure carries a retryable class ($ERR_CLASS)" ;;
        *)  gate_fail "first failure class is '$ERR_CLASS', which is not retryable -- the storm is producing a failure the client cannot retry" ;;
    esac
    # THE COMPOSITION, asserted where it is decidable. `transport` past kMaxAttempts (6) is
    # reachable ONLY through an election-shaped extension, so the pair (class, attempts)
    # distinguishes "a refusal widened the budget and a transport blip then collapsed it"
    # from "a plain transport failure inside the base budget". Both are legitimate; the gate
    # reports WHICH, so the header's story cannot quietly stop being true.
    if [ "$ERR_CLASS" = "transport" ] && [ "${ERR_ATTEMPTS:-0}" -gt 6 ]; then
        gate_ok "composition confirmed: transport give-up at attempt $ERR_ATTEMPTS, past kMaxAttempts 6, so an election-shaped failure widened the budget first"
    elif [ "$ERR_CLASS" = "transport" ]; then
        echo "  (note) transport give-up at attempt ${ERR_ATTEMPTS:-?}, INSIDE the base budget --"
        echo "         this run's failures are not the election-extension composition the header describes"
    fi
fi
echo "GATE_METRIC first_error_class ${ERR_CLASS:-none}"
echo "GATE_METRIC first_error_attempts ${ERR_ATTEMPTS:-0}"
echo "GATE_METRIC leader_refusals $REFUSALS"
assert_eq "client connection failures" "${CONN_FAILS:-missing}" 0
# THE COST MUST BE LATENCY, AND BOUNDED. Against the control rather than an absolute, for
# the same reason the fault gate measures its dip through its own proxy: the skew already
# costs an order of magnitude of throughput and that is the workload, not the storm.
assert_ge "throughput retained vs the rebalance-free control (%)" "${STORM_PCT:-0}" "$MIN_DIP_PCT"
assert_eq "probe non-HTTP failures" "$PROBE_OTHER" 0
assert_eq "server-side 500s" "$(cat /tmp/tsgate_sk*/s.log | grep -c 'Error handling write request')" 0
assert_eq "node crashes" "$(grep -l 'Segmentation fault' /tmp/tsgate_sk*/s.log 2>/dev/null | wc -l)" 0
# ADVISORY, like the deposed-primary gate's: a probe write can meet a VShard genuinely
# mid-transfer and get a retryable 503, which is the storm's doing rather than a defect.
# The HARD assertion is the 500 count above.
echo "  (advisory) $PROBE_OK/$PROBE probe writes accepted into the hot group, $PROBE_5XX retryable 5xx"

# THE READ-BACK COMES AFTER LEADERSHIP SETTLES, not three seconds after the storm stops.
# Measured on the first calibration run: read straight after the storm, all three nodes
# answered with no `point_count` at all -- the storm leaves VShards mid-transfer and a read
# that meets one fails QUERY_INCOMPLETE, which is the rolling gate's subject, not this
# one's. Settling first also doubles as the transfer-mechanism check the rolling gate makes.
# (registers its own gate_fail if leadership never stops moving)
wait_leadership_settled "$PORTS" 40

# NO LOSS ON THE HOT GROUP. Every acked probe point must be readable from every node.
# The bound is a band for the same reason the fault gate's is: a retryable 5xx means
# UNKNOWN, so more than the acked count and no more than the attempted count is correct.
for p in $PORTS; do
    RESP=$(curl -s -m30 -X POST "http://127.0.0.1:$p/query" -H 'Content-Type: application/json' \
        -d "{\"query\":\"count:server.metrics(cpu_usage){host:host-01,rack:rack-1}\",\"startTime\":$BASE_TS,\"endTime\":$((BASE_TS + PROBE * 1000000000))}")
    N=$(printf '%s' "$RESP" | grep -o '"point_count":[0-9]*' | cut -d: -f2)
    echo "  hot-group read-back on :$p -> point_count=${N:-<none>}"
    # A read that did not answer at all must show WHY. Without this the first calibration
    # run reported "= 0, expected >= 150" three times with no way to tell a lost write from
    # a failed query -- which are opposite diagnoses.
    [ -n "${N:-}" ] || echo "    response: $(printf '%s' "$RESP" | head -c 300)"
    assert_ge "acked probe points readable on :$p (acked $PROBE_OK)" "${N:-0}" "$PROBE_OK"
    assert_le "probe points readable on :$p (attempted $PROBE)" "${N:-999999}" "$PROBE"
done

echo "GATE_METRIC transfers_initiated $TRANSFERS"
echo "GATE_METRIC rebalance_calls $CALLS"
echo "GATE_METRIC hot_vshards $HOT"
echo "GATE_METRIC joiner_led ${LED3:-0}"
echo "GATE_METRIC probe_ok $PROBE_OK"
echo "GATE_METRIC probe_5xx $PROBE_5XX"
echo "GATE_METRIC control_tput ${CTL_TPUT:-0}"
echo "GATE_METRIC storm_tput ${STORM_TPUT:-0}"
echo "GATE_METRIC storm_pct ${STORM_PCT:-0}"
echo "GATE_METRIC storm_http_errors ${HTTP_ERRS:-0}"

gate_exit
