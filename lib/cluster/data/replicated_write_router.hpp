#pragma once

#include "node_store.hpp"  // NodeTransport, ProposeSink, ProposeOutcome
#include "vshard_directory.hpp"
#include "write_errors.hpp"
#include "write_record.hpp"

#include <chrono>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

namespace timestar::data {

// The RF=3 write router (integration plan M3): routes a WriteBatch to each VShard's Raft
// LEADER to be replicated -- the local ProposeSink (this node's ReplicatedVShardHost)
// when this node leads the VShard, else proposeWrite RPC to the leader node.
//
// FAILOVER (write-scaleout 3a/3b). The initial leader guess is the current Raft leader if
// this node knows it, else the placement primary. When that guess is wrong the rejecting
// node now returns WHO leads the VShard (SliceReject::leaderHint), and the router retries
// the REJECTED SLICES ONLY against the corrected map, bounded by attempts and a deadline.
//
// That closes the v1 gap: a `false` used to carry no hint at all, so the model invariant
// "placement primary == Raft leader" had one uncovered case -- a primary that is alive
// but no longer leader with NO placement change -- in which every retry went straight
// back to the same stale primary. That case is not exotic; it is what a routine
// leadership rebalance produces, and it was measured as client-visible 5xx.
//
// ACK CONTRACT (unchanged, and the retry is built to preserve it):
//   * ack => durable quorum commit. A slice is only ever dropped from the retry set when
//     the leader reported it COMMITTED.
//   * no silent partial batch: if any slice is still uncommitted when the budget is
//     spent, the WHOLE write fails with RetryableWriteError.
//   * fail-closed: an unassigned VShard rejects the batch BEFORE anything is dispatched
//     (UnassignedVShardError), so a batch that cannot be routed in full never commits in
//     part.
//   * no abandoned futures: every dispatched propose is awaited, on every attempt.
//
// RETRY SAFETY: re-proposing a slice whose first attempt actually committed (an ambiguous
// transport failure or a leadership loss after append) re-applies it at a NEW log index,
// hence a NEW, higher revision. That is value-idempotent under LWW -- identical bytes --
// and the full audit, including the one interleaving case and why it is a legal
// linearization rather than a lost ack, is in write_errors.hpp.
//
// Contrast with the M2 NodeWriteRouter (RF=1): that applies directly on the owner; this
// REPLICATES through Raft on the leader, acking only on durable quorum commit.
class ReplicatedBatchWriteRouter {
public:
    // Retry budget. Sized against the write path's analogue on the read side
    // (cluster_data_plane.cpp's kLeaderRetries: 4 x 25ms), but a little longer, because a
    // write must wait for the NEW leader to be elected AND to commit, not just to answer
    // a ReadIndex. A leadership transfer completes in single-digit milliseconds, so this
    // converts the transfer window into a latency bump; a genuinely dead peer still fails
    // the write inside the deadline, and fail-closed is preserved.
    static constexpr unsigned kMaxAttempts = 6;  // 1 initial + 5 retries
    // The pause between attempts is no longer a single number: it is chosen per failure
    // CLASS by `writeFailureRetryDelay` (write_errors.hpp, write-scaleout 4a), because a
    // stale leader and a dead connection want opposite pacing. A NotLeader retry goes to a
    // different node and wants to be immediate; a Transport failure must OUTLAST the
    // transport's own reconnect backoff or every attempt fast-fails on the same dead
    // client. `kRetryDelay` is what the leader-shaped classes still use, and remains the
    // base the geometric schedule grows from.
    static constexpr auto kRetryDelay = kWriteRetryDelayBase;
    static constexpr auto kDeadline = std::chrono::milliseconds(1500);
    // Per-ATTEMPT bound, pushed down into the RPC itself (write-scaleout 3f). The overall
    // deadline alone is not enough: it is only checked BETWEEN attempts, so an attempt
    // against a peer that accepts the connection and then goes silent never returns, the
    // batch holds its WriteAdmission charge indefinitely, and the shard starts 503-ing
    // behind it. Sized well above the measured p99 batch latency (~180 ms) so a merely
    // slow quorum round is never cut off, and small enough that the overall deadline
    // still admits several attempts.
    static constexpr auto kAttemptTimeout = std::chrono::milliseconds(600);

    // THE ELECTION WINDOW (debt D-14). A batch whose only remaining problem is that some
    // group has no leader yet is waiting on a Raft election, and an election takes 2.5-5 s
    // at the production timing while the deadline above is 1.5 s. So the batch failed --
    // not because anything was wrong with it, but because it asked before the cluster had
    // finished choosing. With ~1000 VShards per batch and ~1/3 of groups re-electing after
    // a node dies, essentially every batch in that window failed: that is the measured
    // one-node-down 503 band (33/200, 137/400, 333/900).
    //
    // When -- and ONLY when -- every failure of an attempt is election-shaped
    // (`isElectionWaitFailure`), the batch may keep retrying to this longer deadline, with
    // a bigger attempt budget to match the 400 ms election-class pacing.
    //
    // WHAT THIS DELIBERATELY DOES NOT DO:
    //   * it does NOT extend for `Transport`. A dead or reset connection is not waiting
    //     for an election; the 4a schedule already spans the reconnect window inside the
    //     1.5 s deadline, and stretching it would hold a batch (and its in-flight-bytes
    //     charge) for 6 s against a peer that is simply gone. [D6] keeps its own budget.
    //     The test is applied PER ATTEMPT, so a batch that meets a transport failure at
    //     any point immediately reverts to the 1.5 s deadline.
    //   * it does NOT extend for `Overloaded`: the cure there is for something to drain,
    //     and waiting 6 s while holding admission bytes is the opposite of that.
    //   * it does NOT make a dead cluster wait forever. Quorum loss keeps every group
    //     leaderless, which IS election-shaped, so the batch waits out this cap and then
    //     fails closed with the usual RetryableWriteError -- 6 s, not 1.5 s, and not
    //     unbounded.
    //
    // THE COST, stated because it is real: during a failover, batches that used to fail at
    // 1.5 s now succeed at up to ~5 s, so p99 write latency during the window is worse and
    // each waiting batch holds its WriteAdmission charge for longer, which can push
    // CONCURRENT writes into 503-Overloaded sooner. That is the right trade for a write
    // path whose failure mode is "the client must retry the whole batch", but it is a
    // trade.
    static constexpr auto kElectionDeadline = std::chrono::milliseconds(6000);
    // 3 fast retries (20 ms) + the rest at kElectionRetryDelay (400 ms) spans ~5.3 s, so
    // the deadline above is what actually bounds the wait, not the attempt count.
    static constexpr unsigned kMaxElectionAttempts = 16;
    static_assert(kElectionDeadline > kDeadline, "the election window must extend the base deadline");
    static_assert(kMaxElectionAttempts > kMaxAttempts, "an extended deadline needs attempts left to use it");

    // THE COUPLING, checked by the compiler rather than by comment, and against the REAL
    // attempt count: the pauses of a full Transport-class budget must outlast the
    // transport's own reconnect backoff, or every attempt fast-fails on the same dead
    // client and a TCP blip becomes a client 5xx ([D6], write-scaleout 4a).
    //
    // Both sides are taken at their PESSIMAL jittered values -- shortest possible retry
    // span against longest possible backoff window -- because the nominal comparison
    // (620 ms vs 200 ms, three windows) is not what has to hold; the unlucky draw is.
    // Pessimal is 465 ms vs 300 ms, i.e. at least one guaranteed re-dial inside the
    // budget even on the worst draw, against ZERO before 4a.
    //
    // It is asserted HERE and not in write_errors.hpp because kMaxAttempts lives here:
    // asserting against a literal 6 let someone drop kMaxAttempts to 4, restore [D6], and
    // still compile.
    static_assert(worstCaseWriteRetrySpan(WriteFailure::Transport, kMaxAttempts) > cluster::worstCaseReconnectBackoff(),
                  "the write retry schedule must outlast the transport reconnect window even on the "
                  "worst jitter draw [write-scaleout 4a]");

    ReplicatedBatchWriteRouter(const VShardDirectory& dir, ProposeSink& local, NodeTransport& client,
                               const LeaderResolver& leaders)
        : dir_(dir), local_(local), client_(client), leaders_(leaders) {}

    // Route + replicate. Resolves when every VShard-leader group has durably committed on
    // quorum. Throws UnassignedVShardError if any series' VShard is unassigned (before
    // any dispatch), RetryableWriteError if slices remain uncommitted after the bounded
    // retries, and propagates a genuinely fatal error (journal I/O, an oversized frame)
    // unchanged.
    seastar::future<> write(WriteBatch batch);
    // Same, for a batch the caller already split by VShard (write-scaleout 2b): the
    // production path splits ONCE at ingress and every layer below only re-buckets those
    // groups. Identical semantics; the WriteBatch overload just splits first.
    seastar::future<> write(VShardBatches groups);

private:
    // Ask the local Raft plane to un-hibernate the groups that still believe `node` leads
    // them, after an RPC to it failed on the transport (debt D-14). RATE-LIMITED per node:
    // the wake is O(groups on this shard) and its effect lasts several seconds, so calling
    // it on every failed attempt of every concurrent batch would be pure overhead.
    void wakeGroupsBehind(NodeId node);

    const VShardDirectory& dir_;
    ProposeSink& local_;
    NodeTransport& client_;
    const LeaderResolver& leaders_;
    // Last time we asked the plane to wake groups behind a given peer.
    std::map<NodeId, seastar::lowres_clock::time_point> lastWake_;
    static constexpr auto kWakeInterval = std::chrono::milliseconds(500);
};

}  // namespace timestar::data
