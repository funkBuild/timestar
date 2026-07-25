#include "replicated_write_router.hpp"

#include "../../core/placement_table.hpp"  // virtualShard
#include "../../utils/logger.hpp"

#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

namespace timestar::data {

seastar::future<> ReplicatedBatchWriteRouter::write(WriteBatch batch) {
    // Entry for callers that still hold an unsplit batch (RF=3 tests, the RF=1-shaped
    // seams). The production path arrives already split -- see the overload.
    return write(splitByVShard(std::move(batch)));
}

seastar::future<> ReplicatedBatchWriteRouter::write(VShardBatches groups) {
    // FAIL-CLOSED FIRST, for the WHOLE batch, before anything is dispatched: a single
    // unassigned VShard rejects the write with zero slices in flight. Doing this per
    // group inside the dispatch loop (as the pre-3d code effectively did, throwing from
    // the middle of the bucketing pass) could leave earlier groups already replicating,
    // so a batch that was never routable could still commit in part.
    for (const auto& g : groups) {
        if (dir_.ownerOf(g.first) == kNoNode)
            throw UnassignedVShardError("ReplicatedBatchWriteRouter: VShard " + std::to_string(g.first) +
                                        " is unassigned in the current placement map");
    }

    const NodeId self = dir_.self();
    // `groups` stays OWNED here for the whole call, retries included; the dispatch below
    // borrows it (VShardBatchView) and every future is awaited before this frame returns,
    // so no view ever outlives its groups.
    VShardBatchView outstanding = viewOf(groups);
    // Leader corrections learned from rejects, applied on the NEXT attempt. This is the
    // 3a payoff: without it a deposed-but-alive primary is re-selected every time and the
    // retry budget is spent re-asking the same wrong node.
    std::map<uint16_t, NodeId> hints;
    const auto deadline = seastar::lowres_clock::now() + kDeadline;
    WriteFailure lastKind = WriteFailure::None;

    for (unsigned attempt = 1;; ++attempt) {
        // Bucket the outstanding groups by the leader NODE we now believe leads them:
        // a hint from a reject wins; else this node's live Raft view; else the placement
        // primary (which a stale primary answers with the real leader, closing the loop).
        VShardBatchView localView;
        std::map<NodeId, VShardBatchView> byLeader;
        for (const auto* g : outstanding) {
            NodeId leader = kNoNode;
            if (auto h = hints.find(g->first); h != hints.end())
                leader = h->second;
            if (leader == kNoNode)
                leader = leaders_.leaderOf(g->first);
            if (leader == kNoNode)
                leader = dir_.ownerOf(g->first);
            if (leader == self)
                localView.push_back(g);
            else
                byLeader[leader].push_back(g);
        }

        // Dispatch every target CONCURRENTLY: local groups through this node's Raft
        // propose sink, remote groups via one hinted proposeWrite per leader.
        std::vector<seastar::future<ProposeOutcome>> pending;
        std::vector<NodeId> pendingTargets;                // parallel to `pending`
        std::vector<const VShardBatchView*> pendingViews;  // parallel to `pending`
        pending.reserve(byLeader.size() + 1);
        pendingTargets.reserve(byLeader.size() + 1);
        pendingViews.reserve(byLeader.size() + 1);
        if (!localView.empty()) {
            pendingTargets.push_back(self);
            pendingViews.push_back(&localView);
            pending.push_back(local_.proposeVShardBatchesHinted(localView));
        }
        for (auto& [leader, lview] : byLeader) {
            pendingTargets.push_back(leader);
            pendingViews.push_back(&lview);
            pending.push_back(client_.proposeWriteHinted(leader, lview));
        }

        // Await ALL (never abandon an in-flight replication) and collect the slices that
        // did NOT commit, together with any corrected leader.
        std::map<uint16_t, const VShardBatchGroup*> failed;
        std::map<uint16_t, NodeId> nextHints;
        std::exception_ptr fatalErr;
        for (size_t i = 0; i < pending.size(); ++i) {
            const bool isLocal = pendingTargets[i] == self;
            try {
                ProposeOutcome out = co_await std::move(pending[i]);
                if (out.committed)
                    continue;
                size_t matched = 0;
                for (const auto& r : out.rejects) {
                    lastKind = r.kind;
                    for (const auto* g : *pendingViews[i])
                        if (g->first == r.vshard) {
                            failed[r.vshard] = g;
                            ++matched;
                        }
                    if (r.leaderHint != kNoNode)
                        nextHints[r.vshard] = r.leaderHint;
                }
                // `committed == false` with no reject we can attribute (an empty list, or
                // VShards not in the view) must NOT read as success -- that is the silent
                // partial ack the whole contract exists to prevent. Fall back to retrying
                // the entire target's slice.
                if (matched == 0) {
                    if (lastKind == WriteFailure::None)
                        lastKind = WriteFailure::NotLeader;
                    for (const auto* g : *pendingViews[i])
                        failed[g->first] = g;
                }
            } catch (...) {
                const auto kind = isLocal ? classifyLocalWriteFailure(std::current_exception())
                                          : classifyRemoteWriteFailure(std::current_exception());
                if (!isRetryableWriteFailure(kind)) {
                    // A fatal failure is NOT retried and NOT downgraded: it propagates
                    // exactly as it was thrown, so an oversized frame stays a 413 and a
                    // journal fault stays visible.
                    if (!fatalErr)
                        fatalErr = std::current_exception();
                    continue;
                }
                lastKind = kind;
                for (const auto* g : *pendingViews[i])
                    failed[g->first] = g;
            }
        }
        if (fatalErr)
            std::rethrow_exception(fatalErr);
        if (failed.empty())
            co_return;  // every slice durably committed on quorum

        // Slices remain. Retry ONLY those -- a slice that committed is never re-proposed,
        // which keeps a leadership blip on one VShard from re-running the whole batch.
        if (attempt >= kMaxAttempts || seastar::lowres_clock::now() >= deadline)
            throw RetryableWriteError("ReplicatedBatchWriteRouter: " + std::to_string(failed.size()) +
                                      " VShard slice(s) uncommitted after " + std::to_string(attempt) +
                                      " attempt(s) (last: " + writeFailureName(lastKind) + "); retry the write");
        outstanding.clear();
        outstanding.reserve(failed.size());
        for (const auto& [vs, g] : failed)
            outstanding.push_back(g);
        hints = std::move(nextHints);
        co_await seastar::sleep(kRetryDelay);
    }
}

}  // namespace timestar::data
