#pragma once

#include "node_store.hpp"  // NodeTransport, ProposeSink, ProposeOutcome
#include "vshard_directory.hpp"
#include "write_errors.hpp"
#include "write_record.hpp"

#include <chrono>
#include <seastar/core/future.hh>

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
    const VShardDirectory& dir_;
    ProposeSink& local_;
    NodeTransport& client_;
    const LeaderResolver& leaders_;
};

}  // namespace timestar::data
