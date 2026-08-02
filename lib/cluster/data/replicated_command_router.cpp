#include "replicated_command_router.hpp"

#include "../reconnect_policy.hpp"

#include <algorithm>
#include <exception>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <set>
#include <stdexcept>

namespace timestar::data {

seastar::future<> ReplicatedCommandRouter::propose(uint16_t vshard, ReplicatedCommand command) {
    // Reject an internally malformed routing request before it can enter a local
    // Raft log. The wire ingress and state machine repeat this check, but the
    // originating-node path reaches the local sink directly and therefore needs
    // its own preflight boundary.
    bool targetsVShard = true;
    if (const auto* batch = std::get_if<DeleteRangeBatch>(&command)) {
        targetsVShard = !batch->targets.empty();
        for (const auto& target : batch->targets)
            targetsVShard =
                targetsVShard && timestar::virtualShard(SeriesId128::fromSeriesKey(target.seriesKey)) == vshard;
    } else if (auto* w = std::get_if<WriteBatch>(&command)) {
        targetsVShard = !w->series.empty();
        for (auto& series : w->series)
            targetsVShard = targetsVShard && vshardOf(series) == vshard;
    }
    if (!targetsVShard)
        throw std::invalid_argument("ReplicatedCommandRouter: command does not belong to requested VShard");

    const NodeId owner = dir_.ownerOf(vshard);
    if (owner == kNoNode)
        throw UnassignedVShardError("ReplicatedCommandRouter: VShard " + std::to_string(vshard) +
                                    " is unassigned in the current placement map");

    const NodeId self = dir_.self();
    NodeId hint = kNoNode;
    NodeId unreachable = kNoNode;
    const auto started = seastar::lowres_clock::now();
    const auto deadline = started + ReplicatedBatchWriteRouter::kDeadline;
    const auto electionDeadline = started + ReplicatedBatchWriteRouter::kElectionDeadline;
    WriteFailure lastKind = WriteFailure::None;

    for (unsigned attempt = 1;; ++attempt) {
        NodeId leader = hint;
        if (leader == kNoNode)
            leader = leaders_.leaderOf(vshard);
        if (leader == kNoNode)
            leader = owner;

        const bool local = leader == self;
        const auto attemptDeadline =
            std::min(deadline, seastar::lowres_clock::now() + ReplicatedBatchWriteRouter::kAttemptTimeout);
        ProposeOutcome out;
        try {
            // Keep `command` in this frame. An ambiguous timeout may require a
            // second proposal, and moving away the only copy would make the retry
            // silently different from the first attempt.
            if (local)
                out = co_await local_.proposeCommandHinted(vshard, command, attemptDeadline);
            else
                out = co_await client_.proposeCommandHinted(leader, vshard, command, attemptDeadline);
        } catch (...) {
            const auto ep = std::current_exception();
            const WriteFailure kind = local ? classifyLocalWriteFailure(ep) : classifyRemoteWriteFailure(ep);
            if (!isRetryableWriteFailure(kind))
                std::rethrow_exception(ep);
            out.rejects.push_back(SliceReject{vshard, kNoNode, kind});
            if (!local && kind == WriteFailure::Transport)
                unreachable = leader;
        }

        if (out.committed ||
            std::find(out.committedVShards.begin(), out.committedVShards.end(), vshard) != out.committedVShards.end())
            co_return;

        // A reject is advisory. Only one naming the VShard we sent may steer
        // or classify this retry; a malformed peer reply cannot redirect an
        // unrelated group. Silence is conservatively a hintless not-leader.
        lastKind = WriteFailure::NotLeader;
        NodeId nextHint = kNoNode;
        for (const auto& reject : out.rejects) {
            if (reject.vshard != vshard)
                continue;
            lastKind = reject.kind;
            if (reject.leaderHint != kNoNode && reject.leaderHint != leader)
                nextHint = reject.leaderHint;
        }
        if (lastKind == WriteFailure::Expired)
            throw DeleteReceiptExpiredError("ReplicatedCommandRouter: delete idempotency window expired for VShard " +
                                            std::to_string(vshard));
        if (!isRetryableWriteFailure(lastKind))
            throw std::runtime_error("ReplicatedCommandRouter: terminal proposal failure for VShard " +
                                     std::to_string(vshard));
        hint = nextHint;

        const auto pause = cluster::jitteredDelay(writeFailureRetryDelay(lastKind, attempt), kWriteRetryJitterPercent);
        const bool electionWait = isElectionWaitFailure(lastKind);
        const auto effectiveDeadline = electionWait ? electionDeadline : deadline;
        const unsigned maxAttempts =
            electionWait ? ReplicatedBatchWriteRouter::kMaxElectionAttempts : ReplicatedBatchWriteRouter::kMaxAttempts;
        const auto now = seastar::lowres_clock::now();
        if (attempt >= maxAttempts || now >= effectiveDeadline || (effectiveDeadline - now) <= pause) {
            if (unreachable != kNoNode)
                local_.wakeGroupsLedBy(unreachable);
            throw RetryableWriteError("ReplicatedCommandRouter: VShard " + std::to_string(vshard) +
                                      " uncommitted after " + std::to_string(attempt) +
                                      " attempt(s) (last: " + writeFailureName(lastKind) + "); retry the mutation");
        }
        co_await seastar::sleep(pause);
    }
}

}  // namespace timestar::data
