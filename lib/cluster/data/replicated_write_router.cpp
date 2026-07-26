#include "replicated_write_router.hpp"

#include "../../core/placement_table.hpp"  // virtualShard
#include "../../utils/logger.hpp"
#include "../reconnect_policy.hpp"  // jitteredDelay

#include <algorithm>
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
        // The pause BEFORE the next attempt, chosen by the failure classes this attempt
        // actually saw (write-scaleout 4a). Taking the MAXIMUM over the classes matters
        // for a mixed batch: if any slice failed on the transport, the whole retry must
        // wait out the reconnect window, or that slice burns the budget fast-failing on a
        // dead client while the not-leader slices are the only ones making progress.
        std::chrono::milliseconds retryDelay{0};
        auto noteKind = [&retryDelay, attempt](WriteFailure k) {
            retryDelay = std::max(retryDelay, writeFailureRetryDelay(k, attempt));
        };
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
        // The attempt's own deadline: never past the overall one, and never longer than a
        // single attempt is allowed to take. It is computed BEFORE any dispatch and passed
        // to EVERY target, local included. The local leg needs it just as much as a remote
        // one -- arguably more: a locally-led VShard that has lost quorum suspends its Raft
        // waiter forever (see RaftGroup::proposeAndAwaitApplied), which is the unbounded
        // version of the remote black-hole this deadline exists for, and a deadline checked
        // only BETWEEN attempts cannot stop an attempt that never returns.
        const auto attemptDeadline =
            std::min(deadline, seastar::lowres_clock::now() + ReplicatedBatchWriteRouter::kAttemptTimeout);
        if (!localView.empty()) {
            pendingTargets.push_back(self);
            pendingViews.push_back(&localView);
            pending.push_back(local_.proposeVShardBatchesHinted(localView, attemptDeadline));
        }
        for (auto& [leader, lview] : byLeader) {
            pendingTargets.push_back(leader);
            pendingViews.push_back(&lview);
            pending.push_back(client_.proposeWriteHinted(leader, lview, attemptDeadline));
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
                // COMMITTED-SET, not reject-set (see ProposeOutcome). Everything this
                // target was asked for is uncommitted UNLESS it is explicitly named
                // committed. Deriving the uncommitted set by subtracting `rejects` acked
                // slices that were never replicated whenever a sink answered with a
                // strict-subset reject list -- which ReplicatedVShardHost's membership
                // check does by construction, having proposed nothing at all.
                //
                // Only VShards WE dispatched to this target can be crossed off, so a
                // reply naming VShards outside the view (a buggy or hostile peer) cannot
                // remove anything from the retry set.
                std::set<uint16_t> committedHere(out.committedVShards.begin(), out.committedVShards.end());
                for (const auto* g : *pendingViews[i])
                    if (!committedHere.count(g->first))
                        failed[g->first] = g;
                // Rejects are advisory: they carry the leader hint and the reason, and
                // nothing else. A hint is accepted only for a VShard THIS target was
                // actually asked about -- checking against the accumulated `failed` map
                // would let a peer processed later inject a hint for a slice a DIFFERENT
                // target failed, steering someone else's retry.
                std::set<uint16_t> askedHere;
                for (const auto* g : *pendingViews[i])
                    askedHere.insert(g->first);
                for (const auto& r : out.rejects) {
                    lastKind = r.kind;
                    noteKind(r.kind);
                    if (r.leaderHint != kNoNode && askedHere.count(r.vshard))
                        nextHints[r.vshard] = r.leaderHint;
                }
                if (lastKind == WriteFailure::None)
                    lastKind = WriteFailure::NotLeader;
                // A target that named no reason at all is treated as a plain not-leader:
                // the slices are uncommitted and the retry goes elsewhere immediately.
                noteKind(WriteFailure::NotLeader);
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
                noteKind(kind);
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
        //
        // The pause is jittered so that N concurrent batches (and, at RF=3, N shards x N
        // peers) do not all re-dial the same peer on the same 20/40/80 ms grid -- the
        // write-side half of the herd 4b jitters on the transport side.
        const auto pause = cluster::jitteredDelay(retryDelay <= std::chrono::milliseconds(0) ? kRetryDelay : retryDelay,
                                                  kWriteRetryJitterPercent);
        const auto now = seastar::lowres_clock::now();
        // Give up if the budget is spent OR IF THE PAUSE ITSELF WOULD OUTLAST IT. The
        // second half matters now that pauses reach 320 ms: without it a pause starting at
        // t=1499 ms sleeps past the deadline and then dispatches an attempt that the
        // between-attempts check is guaranteed to reject, so the caller waits ~400 ms
        // longer than the deadline it was promised for an answer that cannot change.
        if (attempt >= kMaxAttempts || now >= deadline || (deadline - now) <= pause)
            throw RetryableWriteError("ReplicatedBatchWriteRouter: " + std::to_string(failed.size()) +
                                      " VShard slice(s) uncommitted after " + std::to_string(attempt) +
                                      " attempt(s) (last: " + writeFailureName(lastKind) + "); retry the write");
        outstanding.clear();
        outstanding.reserve(failed.size());
        for (const auto& [vs, g] : failed)
            outstanding.push_back(g);
        hints = std::move(nextHints);
        co_await seastar::sleep(pause);
    }
}

}  // namespace timestar::data
