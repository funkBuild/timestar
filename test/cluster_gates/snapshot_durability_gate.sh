#!/bin/bash
# GATE (debt D-6): kill -9 the WHOLE cluster and prove that TAKING SNAPSHOTS DOES NOT COST
# DURABILITY. Same-session A/B: the identical run once with the snapshot trigger OFF and
# once with it ON, comparing what each reads back.
#
# WHY AN A/B AND NOT AN ABSOLUTE BAR. An absolute "every acked point readable" is the right
# invariant and this gate reports it -- but it is VIOLATED ON THIS BINARY BY A PRE-EXISTING,
# LOAD-DEPENDENT DEFECT that has nothing to do with snapshots, and measuring it as an
# absolute would attribute someone else's bug to D-6 (or, worse, hide a real D-6 regression
# inside the noise). Measured on this box, three configurations:
#
#     load phase + snapshots ON      194 / 194 / 194  of 200 acked
#     load phase + snapshots OFF     183 / 193 / 199  of 200 acked   <-- WORSE, and no
#                                                                        snapshot was taken
#     NO load phase, snapshots OFF   200 / 200 / 200  of 200 acked
#
# So kill -9 + full log replay is sound on its own; the loss appears only when a heavy write
# campaign precedes the probes, and the SNAPSHOT run is the better of the two. Filed as
# debt D-36 -- the prime suspect is restart replay of ~1.5M points tripping
# `rejectIfIngestBacklogged` inside apply (EngineDataStateMachine's header already warns
# "do not read this as never fires"), which leaves an entry in the log and un-applied.
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
SNAP_OFF=100000000  # unreachable at this scale -> the control arm compacts nothing
WAL_THRESHOLD="${GATE_WAL_THRESHOLD:-2097152}"

trap 'kill_cluster 197' EXIT

# run_arm NAME SNAPSHOT_ENTRIES -> sets ARM_MIN, ARM_READS, ARM_TAKEN, ARM_RECOVERED
run_arm() {
    local name="$1" snap="$2"
    kill_cluster 197
    require_ports_free $PORTS
    for i in 1 2 3; do rm -rf "/tmp/tsgate_sd$i"; mkdir -p "/tmp/tsgate_sd$i"; done
    local PEERS="127.0.0.1:19710,127.0.0.1:19711,127.0.0.1:19712"
    start_node() {
        env TIMESTAR_DATA_DIR="/tmp/tsgate_sd$1" TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
            TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_NODE_ID=$1 TIMESTAR_CLUSTER_PEERS="$PEERS" \
            TIMESTAR_CLUSTER_SNAPSHOT_ENTRIES="$snap" TIMESTAR_CLUSTER_SNAPSHOT_MIN_INTERVAL_S=2 \
            TIMESTAR_WAL_SIZE_THRESHOLD="$WAL_THRESHOLD" \
            "$BIN" --port $((19709 + $1)) --smp 4 >>"/tmp/tsgate_sd$1/s.log" 2>&1 &
    }
    echo "=== ARM: $name (snapshot entry threshold $snap) ==="
    for i in 1 2 3; do start_node $i; done
    wait_all_led "$PORTS" 4096 120 || return 1
    wait_leadership_settled "$PORTS" 40 || return 1

    # Load, so the snapshot producer has flushed data to compact -- and so the arms share
    # the same (load-dependent) exposure to the pre-existing defect above.
    "$BENCH" --server-port 19710 -c 4 --batches "$BATCHES" --batch-size 10000 --verify 0 \
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

    kill_cluster 197
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

# ---- CONTROL ARM: no compaction at all ----
run_arm "control, snapshots OFF" "$SNAP_OFF" || gate_exit
CTRL_MIN=$ARM_MIN; CTRL_READS="$ARM_READS"; CTRL_TAKEN=$ARM_TAKEN; CTRL_ACKED=$ARM_ACKED
CTRL_BACKLOG=$ARM_BACKLOG
assert_eq "control arm took no snapshots (it is the control)" "$CTRL_TAKEN" 0

# ---- SUBJECT ARM: the snapshot producer running ----
run_arm "subject, snapshots ON" "$SNAP_ON" || gate_exit
SUBJ_MIN=$ARM_MIN; SUBJ_READS="$ARM_READS"; SUBJ_TAKEN=$ARM_TAKEN; SUBJ_ACKED=$ARM_ACKED

echo "=== A/B ==="
echo "  control (no compaction):  readback$CTRL_READS of $CTRL_ACKED acked, worst $CTRL_MIN"
echo "  subject (compacting):     readback$SUBJ_READS of $SUBJ_ACKED acked, worst $SUBJ_MIN," \
     "$SUBJ_TAKEN snapshots taken"

# ANTI-VACUITY: without snapshots and without a compacted journal being RECOVERED, the
# subject arm is just the control arm and proves nothing about D-6.
assert_ge "snapshots taken in the subject arm" "$SUBJ_TAKEN" 1
assert_ge "VShards recovered from a compacted journal" "$ARM_RECOVERED" 1
assert_eq "snapshot-recovery refusals" "$ARM_REFUSED" 0
assert_eq "snapshots refused as too large" "$ARM_TOOBIG" 0

# THE D-6 ASSERTION: compacting must not cost durability. Every way D-6 could lose data --
# a boundary above unflushed or unconverted data, a re-installed payload -- makes the
# subject arm strictly worse than the control.
assert_ge "acked points readable WITH compaction (>= without, i.e. compaction costs nothing)" \
    "$SUBJ_MIN" "$CTRL_MIN"
# Nothing fabricated or double-counted either (a re-installed snapshot would read back ~2x).
assert_le "acked points readable WITH compaction (<= sent, nothing fabricated/doubled)" "$SUBJ_MIN" "$PROBES"

# THE ABSOLUTE INVARIANT, reported and NOT asserted -- see the header. It is violated on
# this binary by a pre-existing, load-dependent defect (D-36) in BOTH arms, and asserting it
# here would attribute that to D-6.
if [ "$SUBJ_MIN" -lt "$SUBJ_ACKED" ]; then
    echo "  ADVISORY (debt D-36, PRE-EXISTING, not a D-6 regression): the absolute bar is not met --" \
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
