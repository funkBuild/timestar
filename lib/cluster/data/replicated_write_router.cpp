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

// Called ONLY when a write has exhausted its budget with a peer still unreachable, which
// is the distinction that keeps [D6] intact (write-scaleout 4c).
//
// A RESET connection to a HEALTHY peer is absorbed inside the retry budget -- that is
// exactly what 4a's schedule is for -- so it never reaches here and never touches Raft. A
// peer that is really GONE fails the batch, and only then do we schedule one prompt
// check-tick for the groups behind it. Calls are coalesced for 500 ms per peer so
// concurrent give-ups cannot repeat the O(groups) selection, but sustained proof that a
// peer is dead may schedule later prompt passes. That repetition is load-bearing when a
// registry pass itself runs longer than its nominal timer interval: a full-election
// cooldown measured 51 s of all-group recovery against the 30 s SLO.
//
// Waking on every failed ATTEMPT instead was measured to cost errors on the reset gate: it
// puts ~1364 groups (a third of the map) on full-rate ticking for 8 s, and a follower
// ticking at full rate through a storm of dropped heartbeats times out its election and
// campaigns against a leader that is perfectly alive. 151 reset rounds -> 3 bench 503s
// carrying the [D6] signature, against 0 with the wake at give-up time.
void ReplicatedBatchWriteRouter::wakeGroupsBehind(NodeId node) {
    if (node == kNoNode)
        return;
    const auto now = seastar::lowres_clock::now();
    auto it = lastWake_.find(node);
    if (it != lastWake_.end() && now - it->second < kWakeInterval)
        return;
    // Local and synchronous: the router and the Raft groups it is talking about live on the
    // SAME shard (ReplicatedDataPlane owns both), so this needs no cross-shard hop and
    // cannot suspend. Other shards' routers wake their own groups when their own slices
    // fail, which is what makes per-shard coverage complete without a fan-out.
    const size_t woken = local_.wakeGroupsLedBy(node);
    if (woken > 0) {
        // A zero result is not a wake and must not suppress a later one: the local leader
        // view can change between requests as Raft messages arrive.
        lastWake_[node] = now;
        timestar::http_log.info(
            "cluster: node {} unreachable for writes; scheduled one prompt Raft check for {} "
            "group(s) that still believe it leads them",
            node, woken);
    }
}

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
    // Peers that failed on the transport during this write. Used only if the write gives
    // up -- a blip absorbed by the retry budget must leave no trace (see wakeGroupsBehind).
    std::set<NodeId> unreachable;
    const auto started = seastar::lowres_clock::now();
    const auto deadline = started + kDeadline;
    const auto electionDeadline = started + kElectionDeadline;
    WriteFailure lastKind = WriteFailure::None;

    for (unsigned attempt = 1;; ++attempt) {
        // The pause BEFORE the next attempt, chosen by the failure classes this attempt
        // actually saw (write-scaleout 4a). Taking the MAXIMUM over the classes matters
        // for a mixed batch: if any slice failed on the transport, the whole retry must
        // wait out the reconnect window, or that slice burns the budget fast-failing on a
        // dead client while the not-leader slices are the only ones making progress.
        std::chrono::milliseconds retryDelay{0};
        // Is EVERY failure of this attempt one that a wait can cure (debt D-14)? Decided
        // per attempt, and pessimistically: one transport failure anywhere in the batch
        // puts the whole batch back on the 1.5 s deadline, because that slice is not
        // waiting for an election and the [D6] schedule already covers it.
        bool electionWaitOnly = true;
        auto noteKind = [&retryDelay, &electionWaitOnly, attempt](WriteFailure k) {
            retryDelay = std::max(retryDelay, writeFailureRetryDelay(k, attempt));
            if (!isElectionWaitFailure(k))
                electionWaitOnly = false;
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
                // Which of OUR uncommitted slices did this target actually give a reason
                // for? Tracked so the remainder can be classified EXPLICITLY below
                // (debt D-28) instead of silently keeping whatever the last named reject
                // said.
                std::set<uint16_t> explainedHere;
                for (const auto& r : out.rejects) {
                    // A REJECT FOR A VSHARD WE NEVER SENT IS NOT EVIDENCE ABOUT ANYTHING.
                    // The hint and the committed set were already gated on `askedHere`;
                    // the KIND was not, so a buggy or hostile peer could fold a failure
                    // class -- and with it the retry pacing and the election window -- out
                    // of a slice this dispatch has no relationship to. Every use of a
                    // reply is now scoped to what this target was actually asked.
                    if (!askedHere.count(r.vshard))
                        continue;
                    lastKind = r.kind;
                    noteKind(r.kind);
                    if (!committedHere.count(r.vshard))
                        explainedHere.insert(r.vshard);
                    // A HINT NAMING THE REJECTER IS NOT A HINT (write-scaleout 5 review,
                    // F1). A target that just refused a slice and then says "the leader is
                    // me" sends the retry straight back to itself: the slice re-buckets to
                    // the same place and every remaining attempt asks the same refusing
                    // group again, with no RPC and no possibility of a different answer.
                    // Dropping it makes the next attempt RE-RESOLVE from the live leader
                    // view instead, which is the only thing that can change. (The local
                    // sink already refrains from naming itself; this is the general rule,
                    // and it covers a remote peer that names itself too.)
                    // (The `askedHere` half of that rule is now the loop guard above.)
                    if (r.leaderHint != kNoNode && r.leaderHint != pendingTargets[i])
                        nextHints[r.vshard] = r.leaderHint;
                }
                // EVERY UNCOMMITTED SLICE GETS A CLASS, AND IT IS NEVER ONE IT INHERITED
                // (debt D-28). The kinds above are recorded per REJECT, so a slice this
                // target left uncommitted AND unnamed used to carry whatever the last
                // named reject happened to say -- an election-shaped label bought by
                // omission, which is exactly what the 6 s D-14 window must not be
                // reachable by. There are two silences and they mean different things:
                //
                //   * The target named NOTHING AT ALL. It reported one uniform answer
                //     about the whole dispatch and gave no reason. Treat that as plain
                //     NotLeader, so the retry
                //     re-resolves and goes elsewhere at once. Unchanged, and it must stay
                //     INSIDE the "named nothing" case -- applying it unconditionally
                //     MANUFACTURED the label, so a 503 said "not-leader" whatever the
                //     target actually reported.
                //
                //   * The target explained SOME slices and not others. It has demonstrated
                //     it can explain, so its silence about the rest is genuinely unknown
                //     -- not evidence of an election. Those get `Transport`, the ambiguous
                //     retryable default: still retried (an unclassified slice is still
                //     uncommitted, so no ack can be manufactured by it), never
                //     election-shaped, and paced on the transport schedule.
                //
                // Both in-tree sinks name every slice they fail or none of them, so the
                // second arm is unreachable today; it exists so that a future sink
                // reporting a strict subset cannot silently buy the election window.
                //
                // The test is `rejects.empty()` and not "explained none of ours" on
                // purpose: a target whose rejects name ONLY VShards we never sent it (the
                // loop above ignores those entirely) is a peer talking about something
                // else, so it takes the ambiguous
                // arm rather than the election-shaped one.
                std::vector<uint16_t> unexplained;
                for (const auto* g : *pendingViews[i])
                    if (!committedHere.count(g->first) && !explainedHere.count(g->first))
                        unexplained.push_back(g->first);
                if (!unexplained.empty()) {
                    const WriteFailure unnamedKind =
                        out.rejects.empty() ? WriteFailure::NotLeader : WriteFailure::Transport;
                    lastKind = unnamedKind;
                    noteKind(unnamedKind);
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
                noteKind(kind);
                // Remember WHO was unreachable; whether to wake the groups behind it is
                // decided at give-up time, not here (see wakeGroupsBehind).
                if (kind == WriteFailure::Transport && !isLocal) {
                    unreachable.insert(pendingTargets[i]);
                    if (remoteTransportFailures_++ % 1024 == 0) {
                        try {
                            std::rethrow_exception(std::current_exception());
                        } catch (const std::exception& e) {
                            timestar::http_log.warn(
                                "cluster: forwarded write transport to node {} failed: {} (occurrence {})",
                                pendingTargets[i], e.what(), remoteTransportFailures_);
                        } catch (...) {
                            timestar::http_log.warn(
                                "cluster: forwarded write transport to node {} failed with a non-standard exception "
                                "(occurrence {})",
                                pendingTargets[i], remoteTransportFailures_);
                        }
                    }
                }
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
        // The budget for THIS decision: the election window only while every failure of
        // the attempt just finished was election-shaped (debt D-14). Nothing here is
        // sticky -- a later transport failure drops straight back to the base deadline and
        // the base attempt count, so a batch cannot buy 6 s of patience with one
        // leaderless slice and then spend it on a dead peer.
        const auto effDeadline = electionWaitOnly ? electionDeadline : deadline;
        const unsigned effMaxAttempts = electionWaitOnly ? kMaxElectionAttempts : kMaxAttempts;
        // Give up if the budget is spent OR IF THE PAUSE ITSELF WOULD OUTLAST IT. The
        // second half matters now that pauses reach 320 ms: without it a pause starting at
        // t=1499 ms sleeps past the deadline and then dispatches an attempt that the
        // between-attempts check is guaranteed to reject, so the caller waits ~400 ms
        // longer than the deadline it was promised for an answer that cannot change.
        if (attempt >= effMaxAttempts || now >= effDeadline || (effDeadline - now) <= pause) {
            // GIVING UP. This is the moment a transport failure means "that peer is gone",
            // not "that connection blipped" -- see wakeGroupsBehind for why the difference
            // decides whether we touch the Raft groups at all.
            for (NodeId n : unreachable)
                wakeGroupsBehind(n);
            throw RetryableWriteError("ReplicatedBatchWriteRouter: " + std::to_string(failed.size()) +
                                      " VShard slice(s) uncommitted after " + std::to_string(attempt) +
                                      " attempt(s) (last: " + writeFailureName(lastKind) + "); retry the write");
        }
        outstanding.clear();
        outstanding.reserve(failed.size());
        for (const auto& [vs, g] : failed)
            outstanding.push_back(g);
        hints = std::move(nextHints);
        co_await seastar::sleep(pause);
    }
}

}  // namespace timestar::data
