#!/bin/bash
# GATE (debt D-6): kill -9 the WHOLE cluster and prove that TAKING SNAPSHOTS DOES NOT COST
# DURABILITY. Same-session A/B: the identical run once with the snapshot trigger OFF and
# once with it ON, comparing what each reads back.
#
# TWO PHASES, AND THE FIRST ONE IS THE FENCE.
#
# PHASE 1 (FENCE, hard-asserted): a LIGHT load -- just enough to roll TSM and trigger
# snapshots -- then 200 acked probes, kill -9, restart, and EVERY ACKED POINT MUST BE
# READABLE. That is the real invariant, and in this regime it holds, so asserting it is a
# fence rather than a coin toss.
#
# PHASE 2 (A/B, advisory): the same run under a HEAVY load, once with the trigger off and
# once on, reported for context. The absolute bar did NOT hold there on the binary this gate
# was written against, because of a PRE-EXISTING load-dependent defect (debt D-36) that has
# nothing to do with snapshots -- and its magnitude swung run to run, so asserting the A/B
# alone would both fail without a regression and pass with one (review F5). Measured across
# runs on THAT binary:
#
#     heavy load + snapshots ON      194-195 of 200 acked
#     heavy load + snapshots OFF     175-199 of 200 acked   (183/193/199 and 175/175/175
#                                                            observed on the same binary)
#     LIGHT load, either way         200 of 200 acked
#
# The spread on the control arm is the point: it is wider than any regression this gate
# would be looking for. Phase 2 is kept because it is the evidence that the shortfall is not
# snapshot-related -- the snapshot arm is never the worse one -- not because it is a bar.
#
# D-36 IS NOW CLOSED (`966baf8`), and knowing what it was changes how to read phase 2. It was
# never loss: the points were durable in the recovered journals and simply not APPLIED yet, and
# this gate reads back exactly once, so it was measuring apply lag and calling it durability.
# The fix fences a cluster read on the node's own apply lag, so a read either waits or fails
# closed rather than answering short. Phase 2's absolute bar should therefore now be MET -- but
# it is still reported rather than asserted here, because the sample that closed D-36 came from
# `restart_readback_gate.sh` (which re-reads, and is the gate that actually holds that
# invariant to account) and because the control arm's historical spread is not evidence about
# the current binary. If phase 2 starts falling short again, that is a D-36 REGRESSION and the
# readback gate is where to confirm it.
#
# MEASURED after the fix: BOTH heavy arms 200/200/200, 37 snapshots taken, 37 compacted-journal
# recoveries, crashes 0 -- the first run on which the "absolute bar is met" branch below has
# ever been taken. ONE run is not grounds to promote it to an assertion on a bar this gate has
# never held before; two consecutive clean runs would be.
#
# WHAT D-6 COULD HAVE BROKEN, and what this therefore isolates. A snapshot splits recovery
# into two halves that must AGREE: everything at or below the compacted boundary comes from
# the LOCAL Engine's TSM files, everything above it from the retained log suffix. A boundary
# one entry too high loses the unflushed remainder of a partially-flushed entry; a boundary
# above an UNCONVERTED store loses that store entirely (both are real and both are guarded
# in `snapshotVShard` -- the second one cost 7 of 200 points on this gate's first run and is
# why `hasUnconvertedStores` exists); a recovery that re-installs the payload
# double-registers its TSM files and reads back ~2x. The A/B catches all three, because all
# three make the snapshot arm WORSE than the control arm.
#
# Before D-6 this gate could not be written at all: `addVShard` threw "snapshot recovery not
# yet wired" on a compacted journal, so the FIRST restart after a snapshot was a fail-closed
# startup.
#
# Usage: snapshot_durability_gate.sh [SERVER_BINARY]
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
BENCH="$BUILD_DIR/bin/timestar_insert_bench"
[ -x "$BENCH" ] || { echo "no insert bench at $BENCH"; exit 2; }

# Ports BELOW the kernel's ephemeral range (see D-27 / require_ports_free): a gate that
# picks inside it races the kernel for its own listen ports.
PORTS="19710 19711 19712"
PROBES="${GATE_PROBES:-200}"
BATCHES="${GATE_BATCHES:-150}"

FREE_GB=$(df -BG --output=avail /tmp | tail -1 | tr -dc '0-9')
if [ "${FREE_GB:-0}" -lt 20 ]; then
    echo "ABORT: only ${FREE_GB}G free on /tmp; this gate needs ~20G (see the plan doc's MEASUREMENT HAZARD)" >&2
    exit 2
fi

# SNAPSHOTS MUST ACTUALLY HAPPEN in the subject arm, or the A/B compares nothing. Both knobs
# are needed and the reasons are recorded in restart_catchup_gate.sh's header: the entry
# threshold has to be low because a VShard only gets an entry from the batches that touch it
# (one of 4096 groups sees very few), and the memory store has to actually ROLL or there is
# no flushed data to snapshot at all.
SNAP_ON="${GATE_SNAPSHOT_ENTRIES:-4}"
# The control arm must disable BOTH halves of the policy (review F6): leaving the 64 MiB
# byte threshold live would let a busy group snapshot anyway and the "control" would not be
# one.
SNAP_OFF=100000000
SNAP_OFF_BYTES=1099511627776  # 1 TiB, i.e. unreachable
LIGHT_BATCHES="${GATE_LIGHT_BATCHES:-20}"
LIGHT_WAL="${GATE_LIGHT_WAL:-262144}"
WAL_THRESHOLD="${GATE_WAL_THRESHOLD:-2097152}"

trap 'gate_cleanup 1971 /tmp/tsgate_sd1 /tmp/tsgate_sd2 /tmp/tsgate_sd3' EXIT

# run_arm NAME SNAPSHOT_ENTRIES BATCHES WAL_THRESHOLD SNAPSHOT_BYTES
#   -> sets ARM_MIN, ARM_READS, ARM_TAKEN, ARM_RECOVERED, ARM_ACKED, ARM_BACKLOG, ...
run_arm() {
    local name="$1" snap="$2" batches="${3:-$BATCHES}" wal="${4:-$WAL_THRESHOLD}" snapbytes="${5:-}"
    kill_cluster 1971
    require_ports_free $PORTS
    fresh_gate_data_dirs /tmp/tsgate_sd1 /tmp/tsgate_sd2 /tmp/tsgate_sd3 || return 1
    local PEERS="127.0.0.1:19710,127.0.0.1:19711,127.0.0.1:19712"
    start_node() {
        env $GATE_SERVER_ENV TIMESTAR_DATA_DIR="/tmp/tsgate_sd$1" TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
            TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_UUID=00112233445566778899aabbccddeeff TIMESTAR_CLUSTER_DEVELOPMENT_ALLOW_INSECURE_TRANSPORT=true TIMESTAR_CLUSTER_NODE_ID=$1 TIMESTAR_CLUSTER_PEERS="$PEERS" \
            TIMESTAR_CLUSTER_SNAPSHOT_ENTRIES="$snap" TIMESTAR_CLUSTER_SNAPSHOT_MIN_INTERVAL_S=2 \
            ${snapbytes:+TIMESTAR_CLUSTER_SNAPSHOT_BYTES="$snapbytes"} \
            TIMESTAR_WAL_SIZE_THRESHOLD="$wal" \
            "$BIN" --port $((19709 + $1)) --smp 4 --memory "$GATE_SERVER_MEMORY" >>"/tmp/tsgate_sd$1/s.log" 2>&1 &
    }
    echo "=== ARM: $name (snapshot entry threshold $snap) ==="
    for i in 1 2 3; do start_node $i; done
    wait_all_led "$PORTS" 4096 120 || return 1
    wait_leadership_settled "$PORTS" 40 || return 1

    # Load, so the snapshot producer has flushed data to compact -- and so the arms share
    # the same (load-dependent) exposure to the pre-existing defect above.
    "$BENCH" --server-port 19710 -c 4 --batches "$batches" --batch-size 10000 --verify 0 \
        --warmup 3 --connections 8 --hosts 500 --racks 2 >/tmp/tsgate_sd_bench.txt 2>&1
    grep -E "Requests:|Throughput" /tmp/tsgate_sd_bench.txt | sed 's/^/    /'

    ARM_TAKEN=0
    for _ in $(seq 1 15); do
        ARM_TAKEN=0
        for p in $PORTS; do ARM_TAKEN=$((ARM_TAKEN + $(status_field "$(cluster_status "$p")" snapshots_taken))); done
        [ "${ARM_TAKEN:-0}" -gt 0 ] && break
        sleep 2
    done
    for p in $PORTS; do
        S=$(cluster_status "$p")
        echo "    node $p: sweeps=$(status_field "$S" snapshot_sweeps)" \
             "max_entries_since=$(status_field "$S" snapshot_max_entries_since)" \
             "taken=$(status_field "$S" snapshots_taken)" \
             "skipped_unflushed=$(status_field "$S" snapshots_skipped_unflushed)" \
             "skipped_pending_conversion=$(status_field "$S" snapshots_skipped_pending_conversion)"
    done

    # ACKED PROBE WRITES. One point per probe, distinct timestamp and series, each write
    # awaited -- so every 2xx is a promise the cluster made and must keep. Round-robin
    # across coordinators so the promises are not all made by one node.
    local TS ACKED=0 i P CODE
    TS=$(date +%s)000000000
    for i in $(seq 1 "$PROBES"); do
        P=$(echo "$PORTS" | tr ' ' '\n' | sed -n "$(( (i % 3) + 1 ))p")
        CODE=$(curl -s -m10 -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$P/write" \
            -H 'Content-Type: application/json' \
            -d "{\"measurement\":\"durable\",\"tags\":{\"probe\":\"p$i\"},\"fields\":{\"value\":$i.5},\"timestamp\":$((TS + i))}")
        [ "$CODE" = "200" ] && ACKED=$((ACKED + 1))
    done
    ARM_ACKED=$ACKED
    echo "    $ACKED of $PROBES probe writes acknowledged"
    # A few more sweeps, so a boundary can land ON TOP of the probes: some below it
    # (recovered from TSM), some above it (recovered by log replay) -- the interaction the
    # gate is for.
    sleep 12
    local AFTER=0
    for p in $PORTS; do AFTER=$((AFTER + $(status_field "$(cluster_status "$p")" snapshots_taken))); done
    echo "    snapshots taken: $ARM_TAKEN before the probes, $AFTER after"
    ARM_TAKEN=$AFTER

    kill_cluster 1971
    sleep 2
    for i in 1 2 3; do start_node $i; done
    wait_all_led "$PORTS" 4096 180 || return 1

    ARM_RECOVERED=$(cat /tmp/tsgate_sd*/s.log | grep -c 'recovered from a compacted journal')
    ARM_MIN=999999
    ARM_READS=""
    for p in $PORTS; do
        local READ N
        READ=$(curl -s -m30 -X POST "http://127.0.0.1:$p/query" -H 'Content-Type: application/json' \
            -d "{\"query\":\"count:durable(value){}\",\"startTime\":$((TS - 1000000000)),\"endTime\":$((TS + PROBES + 1000000000))}")
        N=$(printf '%s' "$READ" | grep -o '"point_count":[0-9]*' | head -1 | cut -d: -f2)
        N=${N:-0}
        ARM_READS="$ARM_READS $N"
        [ "$N" -lt "$ARM_MIN" ] && ARM_MIN=$N
    done
    echo "    readback:$ARM_READS of $ARM_ACKED acked (worst $ARM_MIN); compacted-journal recoveries=$ARM_RECOVERED"
    ARM_500=$(cat /tmp/tsgate_sd*/s.log | grep -c 'Error handling write request')
    ARM_CRASH=$(grep -l 'Segmentation fault' /tmp/tsgate_sd*/s.log 2>/dev/null | wc -l)
    ARM_QUOTA=$(cat /tmp/tsgate_sd*/s.log | grep -c 'Disk quota exceeded')
    ARM_TOOBIG=$(cat /tmp/tsgate_sd*/s.log | grep -c 'NOT compacting VShard')
    ARM_REFUSED=$(cat /tmp/tsgate_sd*/s.log | grep -c 'snapshot recovery not yet wired')
    # Evidence for the D-36 mechanism, reported in both arms so the attribution stays
    # checkable rather than remembered.
    ARM_BACKLOG=$(cat /tmp/tsgate_sd*/s.log | grep -ciE 'ingest backlog|backlogged')
    echo "    server 500s=$ARM_500 crashes=$ARM_CRASH quota=$ARM_QUOTA too_big=$ARM_TOOBIG" \
         "recovery_refusals=$ARM_REFUSED ingest_backlog_lines=$ARM_BACKLOG"
    return 0
}

# ---- PHASE 1: THE FENCE ----
# Light load: enough rollovers to give the producer flushed data to compact, small enough
# that restart replay does not trip the D-36 confounder. Here the absolute bar HOLDS, so it
# is asserted.
run_arm "FENCE, light load, snapshots ON" "$SNAP_ON" "$LIGHT_BATCHES" "$LIGHT_WAL" || gate_exit
FENCE_MIN=$ARM_MIN; FENCE_READS="$ARM_READS"; FENCE_TAKEN=$ARM_TAKEN; FENCE_ACKED=$ARM_ACKED
FENCE_RECOVERED=$ARM_RECOVERED
echo "=== FENCE ==="
echo "  light load + compaction: readback$FENCE_READS of $FENCE_ACKED acked, $FENCE_TAKEN snapshots taken"
# ANTI-VACUITY first: without a snapshot taken AND a compacted journal recovered, the fence
# is just a kill -9 test and says nothing about D-6.
assert_ge "FENCE: snapshots taken" "$FENCE_TAKEN" 1
assert_ge "FENCE: VShards recovered from a compacted journal" "$FENCE_RECOVERED" 1
assert_eq "FENCE: snapshot-recovery refusals" "$ARM_REFUSED" 0
assert_eq "FENCE: snapshots refused as too large" "$ARM_TOOBIG" 0
# THE INVARIANT, hard-asserted: ack => durable quorum commit, so every acked point must be
# readable after the whole cluster is kill -9'd and restarted over compacted journals.
assert_ge "FENCE: every acked point readable after kill -9 over compacted journals" "$FENCE_MIN" "$FENCE_ACKED"
assert_le "FENCE: nothing fabricated or double-counted" "$FENCE_MIN" "$PROBES"

# ---- PHASE 2, CONTROL ARM: no compaction at all ----
run_arm "control, heavy load, snapshots OFF" "$SNAP_OFF" "$BATCHES" "$WAL_THRESHOLD" "$SNAP_OFF_BYTES" || gate_exit
CTRL_MIN=$ARM_MIN; CTRL_READS="$ARM_READS"; CTRL_TAKEN=$ARM_TAKEN; CTRL_ACKED=$ARM_ACKED
CTRL_BACKLOG=$ARM_BACKLOG
assert_eq "control arm took no snapshots (it is the control)" "$CTRL_TAKEN" 0

# ---- PHASE 2, SUBJECT ARM: the snapshot producer running ----
run_arm "subject, heavy load, snapshots ON" "$SNAP_ON" || gate_exit
SUBJ_MIN=$ARM_MIN; SUBJ_READS="$ARM_READS"; SUBJ_TAKEN=$ARM_TAKEN; SUBJ_ACKED=$ARM_ACKED

echo "=== A/B (advisory -- see the header and debt D-36) ==="
echo "  control (no compaction):  readback$CTRL_READS of $CTRL_ACKED acked, worst $CTRL_MIN"
echo "  subject (compacting):     readback$SUBJ_READS of $SUBJ_ACKED acked, worst $SUBJ_MIN," \
     "$SUBJ_TAKEN snapshots taken"

# ANTI-VACUITY for phase 2 as well.
assert_ge "snapshots taken in the subject arm" "$SUBJ_TAKEN" 1
assert_ge "VShards recovered from a compacted journal (subject arm)" "$ARM_RECOVERED" 1
assert_eq "snapshot-recovery refusals" "$ARM_REFUSED" 0
assert_eq "snapshots refused as too large" "$ARM_TOOBIG" 0

# ADVISORY, NOT ASSERTED (review F5): the control arm's own spread (175-199 across runs on
# one binary) is wider than any regression this comparison could detect, so asserting it
# would fail without a regression and pass with one. The FENCE above is what actually holds
# D-6 to account; this is the evidence that the residual loss is not snapshot-related.
if [ "$SUBJ_MIN" -ge "$CTRL_MIN" ]; then
    echo "  ADVISORY: compaction is not the worse arm ($SUBJ_MIN >= $CTRL_MIN), consistent with D-36 being unrelated"
else
    echo "  ADVISORY: compaction read back FEWER points this run ($SUBJ_MIN < $CTRL_MIN). One draw is not a verdict --" \
         "the control arm alone spans 175-199 across runs. Re-run before concluding anything, and check the FENCE."
fi
# Still hard-asserted, because it is not noise-shaped: a re-installed snapshot would
# double-register its TSM files and read back ~2x.
assert_le "acked points readable WITH compaction (<= sent, nothing fabricated/doubled)" "$SUBJ_MIN" "$PROBES"

# THE ABSOLUTE INVARIANT, reported and NOT asserted -- see the header. It is violated on
# this binary by a pre-existing, load-dependent defect (D-36) in BOTH arms, and asserting it
# here would attribute that to D-6.
if [ "$SUBJ_MIN" -lt "$SUBJ_ACKED" ]; then
    echo "  ADVISORY (debt D-36 REGRESSION -- D-36 is CLOSED, so this should no longer happen; NOT a D-6 finding):" \
         "under HEAVY load the absolute bar is not met --" \
         "$SUBJ_MIN of $SUBJ_ACKED acked points readable with compaction," \
         "$CTRL_MIN of $CTRL_ACKED without it. ack => durable quorum commit means this must be" \
         "$SUBJ_ACKED; the control arm shows it is not snapshot-related. Ingest-backlog log lines:" \
         "control=$CTRL_BACKLOG subject=$ARM_BACKLOG."
else
    gate_ok "the absolute bar is met: every acked point readable with compaction"
fi

assert_eq "server-side 500s" "$ARM_500" 0
assert_eq "node crashes" "$ARM_CRASH" 0
assert_eq "journal quota fences" "$ARM_QUOTA" 0

gate_exit
