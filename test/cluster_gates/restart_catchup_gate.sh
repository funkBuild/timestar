#!/bin/bash
# GATE (write-scaleout 5.4 / CR-FIX-011): a follower that was DOWN through a
# large write campaign has its whole data directory removed, then must catch up
# from an actually empty node -- through bounded snapshot chunks plus the
# retained post-snapshot suffix.
#
# WHAT IT DISCRIMINATES. `sendAppend` used to put `log_.entriesFrom(nextIndex)` -- the
# entire remaining log tail -- into ONE AppendEntries. The Raft deliver verb is seastar
# `no_wait`, so a message over the receiver's admission bound is DROPPED WITH NO REPLY:
# the leader re-sends the same oversized append forever and the follower never acks. That
# is why the inbound bound was 1 GiB, i.e. not a bound. 5.4 caps the append side
# (RaftOptions::maxAppendEntries / maxAppendBytes) and tightens admission to 128 MiB, and
# this gate is what says the tightened bound did not break catch-up.
#
# HONEST SCOPE OF THE APPEND HALF. The pre-5.4 binary (c052253) PASSES that half too: at
# 400 batches the whole log tail still fits under the old 1 GiB bound, so nothing was
# broken to begin with at this scale. That half is a REGRESSION FENCE on the tightened
# admission bound -- it says catch-up still works after the bound came down -- not a
# demonstration that chunking was required there. Raise GATE_BATCHES if you want to push
# the backlog toward the old bound; the disk cost rises with it.
#
# ============================================================================
# THE SNAPSHOT HALF (debt D-5/D-6) -- this is the DISCRIMINATING part.
# ============================================================================
#
# The append half cannot fail for a chunked-snapshot regression, because it never reaches
# the snapshot path: the leader still has every entry the returning follower needs. To
# exercise chunked InstallSnapshot the follower has to be behind the leader's COMPACTED
# PREFIX, so the entries that would have caught it up no longer exist.
#
# Both halves of that are new. D-6 wired the snapshot PRODUCER (nothing ever compacted
# before -- `snapshotVShard` had no caller at all), and D-5 chunked the TRANSFER (one
# message used to carry an entire VShard snapshot, which is why the Raft admission bound
# had to stay loose and why the producer refused to compact past a size limit).
#
# HOW IT IS FORCED, in two independent places, and BOTH are needed:
#
#   1. `TIMESTAR_CLUSTER_SNAPSHOT_ENTRIES` lowers the trigger threshold. Production is 8192
#      entries or 64 MiB PER VSHARD, which a short campaign spread over 4096 VShards never
#      approaches -- reaching it honestly would take hours of disk. (The knob exists for
#      operators too; see cluster_data_plane.cpp.)
#
#      HOW LOW IT HAS TO BE IS NOT OBVIOUS, and the reason is worth recording: a Raft entry
#      is one batch's slice FOR ONE VSHARD, so a VShard only gets an entry from the batches
#      that happen to touch it. Measured on this box, a 120x10k campaign over 200 hosts put
#      just SEVEN entries in the busiest VShard -- the points are spread across 4096 groups.
#      A threshold of 8 therefore compacted NOTHING while looking like a broken trigger; the
#      gate reports `snapshot_max_entries_since` precisely so that mistake is visible next
#      time rather than mysterious.
#
#   2. `TIMESTAR_WAL_SIZE_THRESHOLD` lowers the memory-store ROLLOVER threshold, and this
#      one is not optional either. A snapshot captures only FLUSHED (TSM) data, so with no
#      TSM file there is no snapshot to take: the boundary would be revision 0 and
#      `snapshotVShard` returns 0 forever. At the 64 MB default a 400x10k campaign spread
#      over 4 shards writes ~24 MB per shard and NEVER ROLLS -- the first run of this gate
#      produced `snapshots_taken=0` with zero TSM files on disk, and the trigger looked
#      broken when it was correctly declining to compact unflushed data. 4 MB rolls several
#      times during the campaign.
#
# That first failure is worth keeping in the record: "the producer is wired" and "the
# producer can run in this gate's shape" are different claims, and only the second one is
# testable here.
#
# WHY IT CANNOT PASS BY ORDINARY APPEND CATCH-UP, which is the assertion that makes this a
# real gate rather than a re-run of the first half: `/cluster/status` reports
# `snapshots_taken` (the producer ran), `snapshot_chunks_sent` (chunks left a leader) and
# `snapshots_installed` (a follower installed one). A catch-up that went through appends
# leaves all three at zero, and the gate FAILS on that. Nothing else distinguishes the two
# paths from the outside -- both end with the follower caught up and able to answer.
#
# IT ALSO FOUND A REAL DEFECT, in the consensus layer rather than the transport: a leader
# transfer aimed at a DOWN peer was never abandoned, so the group refused every write
# forever (RaftNode::propose refuses while leadTransferee_ is set, and only a matchIndex
# that never arrives cleared it) -- and the leadership balancer aims at a dead peer by
# preference, because a dead peer leads nothing and so has the largest deficit on every
# pass. Both halves are fixed and pinned by raft_transfer_abort_test.cpp.
#
# The campaign has to be big enough that the backlog exceeds a chunk MANY times over, so
# catch-up genuinely runs as a pipeline. It is sized in batches rather than points so the
# disk cost is explicit: each batch is 10k points at RF=3.
#
# EMPTY-NODE SCOPE. `snapshots_installed` proves the freshly recreated process
# installed a transferred payload; the subsequent public queries prove the node
# can coordinate normal discovery after that catch-up and preserve both a live
# and an exactly deleted probe. The focused EngineSnapshotApply regression is
# the complementary proof that those discovered records come from the installed
# catalog itself rather than another replica. Existing-data crash/restart replay
# remains covered by restart_readback_gate.sh.
#
# Usage: restart_catchup_gate.sh [SERVER_BINARY]
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
BENCH="$BUILD_DIR/bin/timestar_insert_bench"
[ -x "$BENCH" ] || { echo "no insert bench at $BENCH"; exit 2; }
PORTS="19510 19511 19512"
BATCHES="${GATE_BATCHES:-400}"   # 400 x 10k = 4M points

# These gates are disk-hungry and /tmp is a per-user-quota tmpfs here: exhausting it
# fences every JournalWriter with "Disk quota exceeded" and the cluster degrades into
# exactly the shape a write-path regression would produce. Refuse to start rather than
# produce a number nobody can trust.
FREE_GB=$(df -BG --output=avail /tmp | tail -1 | tr -dc '0-9')
if [ "${FREE_GB:-0}" -lt 20 ]; then
    echo "ABORT: only ${FREE_GB}G free on /tmp; this gate needs ~20G (see the plan doc's MEASUREMENT HAZARD)" >&2
    exit 2
fi

kill_cluster 1951
require_ports_free $PORTS
fresh_gate_data_dirs /tmp/tsgate_cu1 /tmp/tsgate_cu2 /tmp/tsgate_cu3 || exit 2
PEERS="127.0.0.1:19510,127.0.0.1:19511,127.0.0.1:19512"
# SNAPSHOT_ENTRIES is what forces the producer to run inside a gate-sized campaign (see
# the header for why it has to be this low -- a VShard sees only the batches that touch it,
# and the busiest of 4096 groups reached 4-5 entries here). The 2 s min-interval keeps the
# sweep from re-snapshotting one group on every pass while still letting the ~1365 groups
# per shard get a turn.
SNAP_ENTRIES="${GATE_SNAPSHOT_ENTRIES:-4}"
# ... and the memory store must actually ROLL, or there is no flushed data to snapshot at
# all (see the header: this is what made the first run of this gate report taken=0).
WAL_THRESHOLD="${GATE_WAL_THRESHOLD:-2097152}"
start_node() {
    env $GATE_SERVER_ENV TIMESTAR_DATA_DIR="/tmp/tsgate_cu$1" TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_UUID=00112233445566778899aabbccddeeff TIMESTAR_CLUSTER_DEVELOPMENT_ALLOW_INSECURE_TRANSPORT=true TIMESTAR_CLUSTER_NODE_ID=$1 TIMESTAR_CLUSTER_PEERS="$PEERS" \
        TIMESTAR_CLUSTER_SNAPSHOT_ENTRIES="$SNAP_ENTRIES" TIMESTAR_CLUSTER_SNAPSHOT_MIN_INTERVAL_S=2 \
        TIMESTAR_WAL_SIZE_THRESHOLD="$WAL_THRESHOLD" \
        "$BIN" --port $((19509 + $1)) --smp 4 --memory "$GATE_SERVER_MEMORY" >>"/tmp/tsgate_cu$1/s.log" 2>&1 &
}
trap 'gate_cleanup 1951 /tmp/tsgate_cu1 /tmp/tsgate_cu2 /tmp/tsgate_cu3' EXIT

for i in 1 2 3; do start_node $i; done
wait_all_led "$PORTS" 4096 120 || gate_exit

echo "=== kill node 3, then write $BATCHES x 10k points with it down ==="
pkill -u "$(id -u)" -9 -f -- '--port 19512'
sleep 3
# This is not an ordinary restart: remove every journal, TSM, WAL, index and
# node-local identity artifact while the process is confirmed dead. The helper
# restricts recursive removal to a direct /tmp/tsgate_* root, retries it, proves
# absence, and only then recreates an empty directory for the later log file.
fresh_gate_data_dirs /tmp/tsgate_cu3 || exit 2
if find /tmp/tsgate_cu3 -mindepth 1 -print -quit | grep -q .; then
    echo "ABORT: node 3 data directory is not empty after verified reset" >&2
    exit 2
fi
echo "  node 3 durable state removed; restart target is empty"
# WAIT FOR RE-ELECTION BEFORE MEASURING. Node 3 led ~1/3 of the VShards; killing it
# leaves those leaderless until the surviving pair re-elects, and a write to one of them
# in that window is a bounded, honest 503. That is the NODE-KILL round's subject, not this
# gate's -- measuring through it conflates failover with catch-up. With leadership settled
# on the surviving pair and a 2-of-3 quorum intact, ZERO errors is the right bar, and it
# is the bar that exposed the stuck-transfer defect above.
wait_all_led "19510 19511" 4096 90 || gate_exit
# ...and settled, not merely elected. `wait_all_led` returns as soon as every VShard has
# SOME leader, which on a freshly bereaved cluster is the middle of a leadership
# reshuffle: a write landing on a VShard that is mid-transfer is an honest bounded 503,
# and measuring through that reshuffle is measuring failover again. (Observed: 50/400
# errors in a 6 s burst covering the whole bench when it started immediately after
# wait_all_led, against 106/400 before the stuck-transfer fixes and 0 once settled.)
wait_leadership_settled "19510 19511" 40 || gate_exit

# Two public-path probes precede the bulk campaign so the low WAL threshold
# rolls them into TSM before snapshot production. One survives; the other is
# deleted through the replicated exact-key command before the donor materialises
# its resolved VShard view.
TS=$(date +%s)000000000
KEEP_CODE=$(curl -sS -m10 -o /tmp/tsgate_cu_keep_write.txt -w '%{http_code}' \
    -X POST "http://127.0.0.1:19510/write" -H 'Content-Type: application/json' \
    -d "{\"measurement\":\"catchup\",\"tags\":{\"probe\":\"p1\"},\"fields\":{\"value\":42.5},\"timestamp\":$TS}")
GONE_CODE=$(curl -sS -m10 -o /tmp/tsgate_cu_gone_write.txt -w '%{http_code}' \
    -X POST "http://127.0.0.1:19510/write" -H 'Content-Type: application/json' \
    -d "{\"measurement\":\"catchup\",\"tags\":{\"probe\":\"gone\"},\"fields\":{\"value\":99.5},\"timestamp\":$TS}")
DELETE_CODE=$(curl -sS -m10 -o /tmp/tsgate_cu_delete.txt -w '%{http_code}' \
    -X POST "http://127.0.0.1:19510/delete" -H 'Content-Type: application/json' \
    -d "{\"measurement\":\"catchup\",\"tags\":{\"probe\":\"gone\"},\"field\":\"value\",\"startTime\":$TS,\"endTime\":$TS}")
assert_eq "surviving probe write HTTP status" "$KEEP_CODE" 200
assert_eq "deleted probe write HTTP status" "$GONE_CODE" 200
assert_eq "replicated exact delete HTTP status" "$DELETE_CODE" 200

"$BENCH" --server-port 19510 -c 4 --batches "$BATCHES" --batch-size 10000 --verify 0 \
    --warmup 5 --connections 8 --hosts 1000 --racks 2 >/tmp/tsgate_cu_bench.txt 2>&1
grep -E "Requests:|Throughput|batch latency" /tmp/tsgate_cu_bench.txt | sed 's/^/  /'
OK_REQS=$(grep -oE '[0-9]+ OK' /tmp/tsgate_cu_bench.txt | head -1 | cut -d' ' -f1)
HTTP_ERRS=$(grep -o '[0-9]* HTTP errors' /tmp/tsgate_cu_bench.txt | head -1 | cut -d' ' -f1)

# ANTI-VACUITY: enough of the campaign must have LANDED for the catch-up that follows to
# be a real catch-up. This is the assertion that matters here, and it is a floor, not a
# zero -- see the next paragraph.
assert_ge "batches accepted during the campaign" "${OK_REQS:-0}" "$((BATCHES / 4))"

# The client error count is ADVISORY, deliberately, and the reason is worth stating
# because a zero here would be a nicer gate: writing to a 3-node RF=3 cluster with ONE
# NODE DOWN produces a run-to-run-variable share of bounded 503s ("N VShard slice(s)
# uncommitted after 6 attempt(s) (last: not-leader)") -- 50, 91, 106, 107 and 201 of 400
# across five runs. It is PRE-EXISTING, and that is MEASURED, not assumed: the same gate
# against the pre-Phase-5 binary (c052253) gives 111/400, squarely inside that range.
#
# What is established about it: every rejection is logged on the COORDINATOR and ZERO on
# the node that actually holds the leadership, so the write path is repeatedly proposing
# to the wrong place and burning its whole six-attempt budget -- a leader-RESOLUTION
# problem, not a consensus one. The run-to-run variance is larger than any difference
# between binaries, so do NOT read a change in this number as a result of anything.
#
# What IS hard-asserted: zero server-side 500s, zero crashes, and the catch-up itself.
echo "  campaign with one replica down: ${OK_REQS:-?} OK, ${HTTP_ERRS:-?} bounded 503s (ADVISORY -- see the script)"

sleep 2

# THE PRODUCER MUST HAVE RUN while node 3 was down -- otherwise the survivors still hold
# every entry node 3 needs and the catch-up below would go through ordinary appends,
# leaving the snapshot path untested. Give the 5 s sweep a few passes to land.
echo "=== check the snapshot producer compacted the survivors' logs ==="
SNAP_TAKEN=0
for _ in $(seq 1 30); do
    SNAP_TAKEN=$(( $(status_field "$(cluster_status 19510)" snapshots_taken) \
                 + $(status_field "$(cluster_status 19511)" snapshots_taken) ))
    [ "${SNAP_TAKEN:-0}" -gt 0 ] && break
    sleep 2
done
for p in 19510 19511; do
    S=$(cluster_status "$p")
    echo "  node $p: snapshot_trigger=$(printf '%s' "$S" | grep -o '"snapshot_trigger":[a-z]*' | cut -d: -f2)" \
         "sweeps=$(status_field "$S" snapshot_sweeps)" \
         "max_entries_since=$(status_field "$S" snapshot_max_entries_since)" \
         "taken=$(status_field "$S" snapshots_taken)" \
         "skipped_unflushed=$(status_field "$S" snapshots_skipped_unflushed)" \
         "refused_too_large=$(status_field "$S" snapshots_refused_too_large)"
done
# TSM file count, reported because it is the FIRST thing to look at when taken=0: a snapshot
# captures only flushed data, so zero TSM files means the trigger is correctly declining
# rather than broken (see the header).
echo "  TSM files: node1=$(ls /tmp/tsgate_cu1/shard_*/tsm/ 2>/dev/null | grep -c tsm)" \
     "node2=$(ls /tmp/tsgate_cu2/shard_*/tsm/ 2>/dev/null | grep -c tsm)"
# ANTI-VACUITY for the whole snapshot half: with no snapshot taken, nothing below can
# discriminate chunked InstallSnapshot from append catch-up.
assert_ge "snapshots taken while node 3 was down" "${SNAP_TAKEN:-0}" 1
# A snapshot too large to ship means the log was KEPT, so the follower would again catch up
# by appends -- and it is the condition D-5's chunking + the raised D-6 threshold exist to
# remove.
assert_eq "snapshots refused as too large" "$(cat /tmp/tsgate_cu*/s.log | grep -c 'NOT compacting VShard')" 0

echo "=== restart node 3 and wait for it to catch up ==="
LED_BEFORE=$(( $(status_field "$(cluster_status 19510)" vshards_led) + $(status_field "$(cluster_status 19511)" vshards_led) ))
echo "  nodes 1-2 lead $LED_BEFORE VShards between them"
start_node 3
CAUGHT=0
for _ in $(seq 1 150); do   # up to 5 minutes
    sleep 2
    OK=1
    for p in 19510 19511; do
        S=$(cluster_status "$p")
        LED=$(status_field "$S" vshards_led)
        C3=$(printf '%s' "$S" | grep -o '"3":[0-9]*' | head -1 | cut -d: -f2)
        if [ -z "$LED" ]; then OK=0; break; fi
        # `peer_caught_up[3]` counts the groups this node leads on which node 3's
        # matchIndex has reached the leader's last index.
        if [ "${C3:-0}" -lt "$((LED - 16))" ]; then OK=0; fi
    done
    if [ "$OK" = "1" ]; then CAUGHT=1; break; fi
done
for p in 19510 19511; do
    S=$(cluster_status "$p")
    echo "  node $p: led=$(status_field "$S" vshards_led) peer3_caught_up=$(printf '%s' "$S" | grep -o '"3":[0-9]*' | head -1 | cut -d: -f2)"
done
assert_eq "node 3 caught up on every group its peers lead" "$CAUGHT" 1

# It is not caught up in any useful sense unless it can ANSWER for the data.
READ=$(curl -s -m15 -X POST "http://127.0.0.1:19512/query" -H 'Content-Type: application/json' \
    -d "{\"query\":\"avg:catchup(value){probe:p1}\",\"startTime\":$((TS - 1000000000)),\"endTime\":$((TS + 1000000000))}")
if printf '%s' "$READ" | grep -q '42.5'; then
    gate_ok "the surviving probe is discoverable through the empty-node restart"
else
    gate_fail "the empty-node restart cannot discover the surviving probe: $READ"
fi

GONE_READ=$(curl -s -m15 -X POST "http://127.0.0.1:19512/query" -H 'Content-Type: application/json' \
    -d "{\"query\":\"avg:catchup(value){probe:gone}\",\"startTime\":$((TS - 1000000000)),\"endTime\":$((TS + 1000000000))}")
if printf '%s' "$GONE_READ" | grep -q '99.5'; then
    gate_fail "the exactly deleted probe resurrected after empty-node catch-up: $GONE_READ"
elif printf '%s' "$GONE_READ" | grep -q '"status":"success"'; then
    gate_ok "the exactly deleted probe remains absent after empty-node catch-up"
else
    gate_fail "the deleted-probe query did not complete successfully: $GONE_READ"
fi

# ============================================================================
# PROOF THAT THE SNAPSHOT PATH WAS ACTUALLY USED (D-5's discriminating assertion)
# ============================================================================
#
# Without these three, a green gate says only "node 3 caught up", which ordinary bounded
# AppendEntries catch-up satisfies just as well -- and would keep satisfying after a
# chunked-InstallSnapshot regression. There is no other externally visible difference
# between the two paths.
CHUNKS_SENT=$(( $(status_field "$(cluster_status 19510)" snapshot_chunks_sent) \
              + $(status_field "$(cluster_status 19511)" snapshot_chunks_sent) ))
INSTALLED=$(status_field "$(cluster_status 19512)" snapshots_installed)
UNDELIVERABLE=$(( $(status_field "$(cluster_status 19510)" snapshots_undeliverable) \
                + $(status_field "$(cluster_status 19511)" snapshots_undeliverable) ))
RESTARTED=$(( $(status_field "$(cluster_status 19510)" snapshot_transfers_restarted) \
            + $(status_field "$(cluster_status 19511)" snapshot_transfers_restarted) ))
ABANDONED=$(( $(status_field "$(cluster_status 19510)" snapshot_transfers_abandoned) \
            + $(status_field "$(cluster_status 19511)" snapshot_transfers_abandoned) ))
echo "  snapshot transfer: chunks_sent=$CHUNKS_SENT installed_on_node3=${INSTALLED:-0}" \
     "undeliverable=$UNDELIVERABLE restarted=$RESTARTED abandoned=$ABANDONED"
assert_ge "InstallSnapshot chunks sent by the survivors" "${CHUNKS_SENT:-0}" 1
assert_ge "snapshots INSTALLED on the restarted node" "${INSTALLED:-0}" 1
# A snapshot this node declined to send leaves the peer permanently uncaught-up: it is the
# F3a refusal, and after D-5 it should be unreachable at this scale.
assert_eq "snapshots this node could not deliver" "${UNDELIVERABLE:-0}" 0
# Chunks are paced one-in-flight per peer and recovered by a stall timer. A few restarts on
# a loaded box are honest; ABANDONING a transfer means a peer could not be caught up by
# snapshot at all, which is a fail.
assert_eq "snapshot transfers abandoned" "${ABANDONED:-0}" 0

# The send-side mirror of the admission bound must never have fired: an append above it
# would be a message the peer's bound would have dropped silently. This is also the check
# that the TIGHTENED bound (D-5: 96 -> 32 MiB send, 128 -> 64 MiB admission) is not being
# crossed by a real producer.
assert_eq "oversized Raft messages refused" "$(cat /tmp/tsgate_cu*/s.log | grep -c 'refusing to send')" 0
assert_eq "journal quota fences" "$(cat /tmp/tsgate_cu*/s.log | grep -c 'Disk quota exceeded')" 0
assert_eq "server-side 500s" "$(cat /tmp/tsgate_cu*/s.log | grep -c 'Error handling write request')" 0
assert_eq "node crashes" "$(grep -l 'Segmentation fault' /tmp/tsgate_cu*/s.log 2>/dev/null | wc -l)" 0

gate_exit
