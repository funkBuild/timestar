#pragma once

#include "../raft/raft_types.hpp"  // NodeId, LogIndex, kNoNode

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <map>
#include <utility>
#include <vector>

// THE LEADERSHIP BALANCER'S ARITHMETIC, EXTRACTED PURE (debt D-22).
//
// `ShardRaftPlane::rebalance` is not unit-testable: it needs a whole
// `ReplicatedDataPlane`, a journal per VShard and a live Raft tick. So the decisions it
// makes -- fair share, who is a candidate, how much budget a pass has, what a transfer
// costs -- were verified analytically and on a 5-node cluster (debt D-12) and pinned by
// NOTHING, which is how a future edit could silently restore the ~40 %-low target with
// every 3-node gate still green (they cannot see it by construction: at RF == N the two
// formulas agree).
//
// Everything here is a pure function of its arguments, in the shape `planReadRouting`
// took for D-13. The loop keeps only what genuinely needs the plane: reading the live
// Raft view into a survey, and awaiting `transferLeadership`. It holds no arithmetic of
// its own, so these tests are tests of the balancer and not of a copy of it.
//
// The transfer-eligibility predicate below arrived first, for the same reason under a
// different name (debt D-23): `raft_peer_liveness_test` re-implemented it by hand, so the
// test and the balancer could drift apart while every test stayed green -- the copy kept
// pinning the OLD rule and nothing pinned the new one.
namespace timestar::data {

using timestar::raft::kNoNode;
using timestar::raft::LogIndex;
using timestar::raft::NodeId;

// May `target` be handed leadership of a group right now? (Extracted so the balancer's
// candidate loop can try the NEXT peer when this one is ineligible instead of giving up
// on the group -- see LeadershipBalancePass::chooseTarget. Extracted a second time, as
// this SCALAR overload, so the tests can exercise the real predicate rather than a
// hand-written copy of it -- debt D-23.)
//
// NEVER hand leadership to a peer that is not CAUGHT UP on this group.
//
// A leader with a transfer in flight refuses every proposal until the
// transferee acks up to lastIndex, so targeting a peer that cannot ack costs
// the group its write availability for the whole transfer window. And a DEAD
// peer is the MOST attractive target the loop has: it leads nothing, so its
// deficit is the largest on every pass. Measured on the restart-catch-up
// gate: with one of three nodes down, ~26% of writes failed with
// "1 VShard slice(s) uncommitted ... (last: not-leader)" for as long as it
// stayed down, against a perfectly healthy 2-of-3 quorum.
//
// RaftNode::tick abandons a transfer after `transferTimeout` -- two
// heartbeat intervals, 1 s at the production cadence, since D-20 shortened
// it from one election timeout -- so this is no longer unbounded and no
// longer outlasts the write deadline. A bounded write outage repeated every
// balancer pass is still an outage, though, and a caught-up target is also
// the only one that transfers IMMEDIATELY (transferLeadership sends
// TimeoutNow at once rather than waiting on a catch-up round trip).
//
// THE GUARD USED TO BE EXACT EQUALITY (matchIndex == lastIndex) and that made
// the balancer LOAD-DEPENDENT (debt D-1). On a group taking writes, matchIndex
// trails lastIndex by whatever is in flight essentially all of the time -- the
// leader appends locally, then replicates -- so a perfectly healthy peer failed
// the test on nearly every pass. The balancer converged when the cluster was
// quiet and could only move COLD groups when it was busy, which is exactly
// backwards: hot groups are the ones whose leadership placement matters.
//
// Two independent gates replace it, and BOTH must hold:
//
//   1. LAG BOUND -- within kMaxTransferLagEntries of our log end. This is the
//      "the handoff will complete promptly" half. 64 entries is about one
//      AppendEntries round trip of in-flight work at this batch size, so a
//      peer keeping up passes continuously while one genuinely behind (a
//      restarted node still streaming its backlog) does not. TimeoutNow then
//      fires on the very next ack rather than after a catch-up campaign.
//
//   2. LIVENESS -- replied within the LAST HEARTBEAT ROUND. This is the "the
//      target is actually there" half, and it is the one a pure lag bound
//      CANNOT provide. On an IDLE group a dead peer sits at lag ZERO forever:
//      it was caught up when it died and lastIndex never moves again, so it
//      passes any delta check, including the old exact-equality one. (That
//      hole is therefore not new here -- it is pre-existing, and this closes
//      it.) The heartbeat gives an independent decaying signal: the leader
//      bcastAppends every heartbeatTimeout ticks whether or not there is
//      anything to send, and a live peer answers within an RTT.
//
// THE LIVENESS WINDOW IS ONE HEARTBEAT ROUND, AND IT IS THIS TIGHT FOR A
// MEASURED REASON. It was three rounds (1.5 s at the production 25-tick
// heartbeat) and `fault_injection_gate.sh` regressed: 1 of 2000 bench writes
// failed `RetryableWriteError ... (last: transport)` where the same tree
// without this change took a marginally HEAVIER storm (167 rounds / 457
// connections vs 165 / 442) with zero. The mechanism is specific and it is
// the balancer loop's: the periodic balancer could target a peer whose connection
// had just been RST, because the ack clock had not yet decayed past three rounds
// and the lag bound still held. `transferLeadership` then pins
// `leadTransferee_` and the group refuses EVERY proposal until the transferee
// acks -- and the abandon bound was, WHEN THAT WAS MEASURED, one ELECTION
// timeout (2.5-5 s), far longer than the 1.5 s write deadline. So one
// mis-aimed transfer was one failed batch. The old exact-equality guard was
// accidentally immune: a peer whose acks stopped fell behind a growing
// lastIndex immediately. D-20 has since cut the abandon bound to 1 s, which
// makes a mis-aimed transfer a retry rather than a failed batch -- but that
// is a REASON THIS FILTER STAYS TIGHT, not a licence to relax it: the gate
// result above is the only measurement anyone has of this loop, and it was
// taken at three rounds.
//
// One round is the tightest bound a HEALTHY peer still satisfies -- it answers
// every heartbeat within an RTT, so its clock resets long before the next
// round -- and it cuts the post-reset eligibility window from 1.5 s to 0.5 s.
//
// Residual exposure, bounded and now deliberate: a peer that dies inside the
// current heartbeat round can still be targeted on a pass that races it. That
// costs the group its proposals for one `transferTimeout` (1 s at production
// cadence, was one election timeout before D-20), after which RaftNode::tick
// abandons the transfer (§3.10) and writes resume; the peer then reads stale
// on every later pass and is skipped. One bounded window on the pass that
// races the death, not a window per pass forever -- and since D-20 that
// window fits inside the write deadline, so the batch that races it retries
// into the resumed leader instead of failing.
inline constexpr LogIndex kMaxTransferLagEntries = 64;

inline bool transferrableTo(LogIndex lastIndex, LogIndex matchIndex, uint64_t ticksSinceAck,
                            uint64_t heartbeatTimeout, bool requireExactMatch = false) {
    const LogIndex maxLagEntries = requireExactMatch ? 0 : kMaxTransferLagEntries;
    const bool caughtUp = matchIndex >= lastIndex || (lastIndex - matchIndex) <= maxLagEntries;
    const bool live = ticksSinceAck <= heartbeatTimeout;
    return caughtUp && live;
}

// A live peer that is still behind is recovering and automatic leadership
// movement must yield to it. A dead peer must not freeze balancing among the
// surviving quorum; liveness filtering already prevents selecting it.
inline bool automaticBalancePeerReady(LogIndex lastIndex, LogIndex matchIndex, uint64_t ticksSinceAck,
                                      uint64_t heartbeatTimeout) {
    return ticksSinceAck > heartbeatTimeout || matchIndex >= lastIndex;
}

// A FAIR SHARE IS A SUM OF 1/|voters| IN DOUBLE, AND MUST BE CONSUMED WITH A TOLERANCE.
//
// This is a REAL DEFECT that predates the extraction (the pre-extraction loop had it
// identically) and that the extraction is what made findable, because it only shows up
// when someone can call the arithmetic directly. Six additions of 1/3 sum to
// 1.9999999999999998, and truncating that with `static_cast<size_t>` yields ONE where the
// true share is TWO. At RF=3, 102 of the 200 multiples of 3 up to 600 land strictly below
// their true integer this way (at RF=5, 79 of 200).
//
// What it costs, driven through this function: 6 groups over 3 voters with leadership
// [3, 2, 1] and self leading 3. The true shares are 2 each, so the answer is to move ONE
// group to the node holding 1. Truncating instead gives that node a deficit of
// `1 - 1 == 0` -- chooseTarget then skips it as satisfied -- and a budget of `3 - 1 == 2`
// that nothing can spend. The pass transfers NOTHING and the [3, 2, 1] imbalance is
// permanent, on every pass, forever. That is the same *shape* of never-converging
// behaviour D-12 measured, and it is a plausible-but-UNPROVEN explanation of D-12's
// residual 2-3 spread; nobody has re-run a cluster to check.
//
// The fix is to floor with a tolerance rather than to truncate, and to compare with the
// same tolerance so a node exactly AT its share is never read as above or below it. The
// tolerance is derived, not picked: the accumulated error is bounded by n * epsilon, which
// over the 4096 VShards a shard can host is ~3e-11 (measured: 3.1e-11 at n = 4095), while
// the smallest gap that can distinguish two real shares is 1/|voters| >= 1/N, of order
// 0.1 for any replication factor anyone runs. 1e-6 sits five orders above the noise and
// five below the signal. It is NOT a substitute for exact arithmetic if voter sets ever
// grow into the thousands.
inline constexpr double kShareTolerance = 1e-6;

// The number of WHOLE groups a share of `share` is worth. floor, so a share of 6.67 is
// six -- but a share that is a whole number spelled 1.9999999999999998 is two.
inline size_t shareFloor(double share) {
    if (share <= 0.0)
        return 0;
    return static_cast<size_t>(std::floor(share + kShareTolerance));
}

// Is `have` strictly BELOW `share` -- i.e. does this node genuinely want more groups?
// A node holding exactly its share must answer false, whichever way the last bit fell.
inline bool belowShare(size_t have, double share) {
    return static_cast<double>(have) + kShareTolerance < share;
}

// One surveyed group: what the live Raft view says about a VShard this shard hosts.
// `leader == kNoNode` means hosted but with no elected leader -- it still counts toward
// every voter's fair share (we replicate it, so it is leadership that exists to be won)
// but it is nobody's led count.
struct BalanceGroup {
    uint16_t vshard = 0;
    std::vector<NodeId> voters;
    NodeId leader = kNoNode;
};

// One rebalancing pass over a shard's surveyed groups: who may receive leadership, how
// much this pass may give away, and the round-robin cursor that spreads it.
class LeadershipBalancePass {
public:
    static constexpr size_t kNoTarget = static_cast<size_t>(-1);

    // Fair share of the surveyed set, per node: Σ 1/|voters(g)| over the groups we host
    // that the node is a voter of. See planLeadershipBalance for why it is not totalLed/N.
    std::map<NodeId, double> expected;
    // Leadership we can SEE over that same set (leaderless groups are in nobody's count).
    std::map<NodeId, size_t> led;
    // Indices into the surveyed groups WE lead, in survey order.
    std::vector<size_t> mine;
    // (peer, how many groups it is short of its fair share). A peer at or above share is
    // not here at all. A deficit may be ZERO -- a peer whose shortfall rounds away still
    // occupies a slot, and therefore still shifts the round-robin cursor's modulus.
    std::vector<std::pair<NodeId, size_t>> targets;
    // How many groups this pass may give away: what we hold above our own share, capped
    // by the caller's maxTransfers.
    size_t budget = 0;

    // Is there anything to do? A pass that is not viable must transfer nothing -- the
    // three ways to get here are "we are at or below our own share", "no peer is below
    // its share", and "the caller asked for no transfers".
    bool viable() const { return budget > 0 && !targets.empty(); }
    bool exhausted() const { return done_ >= budget; }
    size_t done() const { return done_; }
    size_t cursor() const { return cursor_; }
    NodeId targetAt(size_t idx) const { return targets[idx].first; }
    size_t deficitAt(size_t idx) const { return targets[idx].second; }

    // Pick the first ELIGIBLE target for a group with these `voters`, starting from the
    // round-robin cursor, rather than taking whatever the cursor points at and giving up
    // on the group if it does not fit. Returns kNoTarget when no peer can take it now.
    //
    // A NON-VOTER OF THIS GROUP IS NOT A CANDIDATE AT ALL (debt D-12).
    // `RaftNode::transferLeadership` returns silently for a target that is not a
    // voter, so aiming at one is a NO-OP that the loop nonetheless counted as a
    // transfer: at RF=3 on 5 nodes, 2 of the 4 peers replicate any given group,
    // so about half of every pass's budget was spent on transfers that could not
    // happen -- and `transfers_initiated` reported them. MEASURED before the fix
    // on an idle 5-node RF=3 cluster: 114 961 transfers "initiated" over five
    // minutes of operator storms against roughly 1 500 VShards that actually
    // changed hands.
    //
    // `eligible(cand)` is the caller's live check -- `transferrableTo` against the real
    // group -- and is asked LAST, because it is the only one that touches Raft.
    template <class Eligible>
    size_t chooseTarget(const std::vector<NodeId>& voters, Eligible&& eligible) const {
        if (targets.empty())
            return kNoTarget;
        for (size_t k = 0; k < targets.size(); ++k) {
            const size_t idx = (cursor_ + k) % targets.size();
            const auto& [cand, deficit] = targets[idx];
            if (deficit == 0)
                continue;
            if (std::find(voters.begin(), voters.end(), cand) == voters.end())
                continue;
            if (!eligible(cand))
                continue;
            return idx;
        }
        return kNoTarget;
    }

    // Account for one attempted transfer to `targets[idx]`.
    //
    // COUNT WHAT WAS ARMED, NOT WHAT WAS ASKED (debt D-24). The candidate filter in
    // chooseTarget refuses non-voters and peers that are not caught up, but
    // `RaftNode::transferLeadership` has one more silent early return the balancer
    // cannot see: the F2 re-arm guard, which ignores a repeat request for the
    // transfer ALREADY in flight to that same target. Counting it inflated
    // `transfers_initiated`, which `deposed_primary_gate.sh` asserts an
    // anti-vacuity FLOOR on -- an inflated counter makes that assertion weaker
    // than it reads, in the direction that hides a balancer doing nothing.
    //
    // WHICH CALLER ACTUALLY REACHES THAT GUARD: not the periodic timer. That pass
    // runs every 5 s and never overlaps itself, and since D-20 the abandon window
    // is 1 s, so `leadTransferee_` is clear again by the next pass. It is the
    // OPERATOR endpoint -- `POST /cluster/rebalance-leadership`, which calls
    // `rebalanceLeadership` directly, bounded by nothing -- that can ask again
    // inside a live window. (Before D-20 the window was 2.5-5 s and the periodic
    // pass reached it too, which is how the inflation was found.)
    //
    // `deficit` is charged on the same condition: an unarmed transfer moves no
    // leadership, so spending the target's deficit on it would make the rest of
    // the pass skip a peer that still needs groups.
    //
    // THE CURSOR ADVANCES EITHER WAY. It is a fairness device, not an accounting one:
    // holding it still on an unarmed attempt would re-offer the same peer the same
    // groups for the rest of the pass.
    void recordAttempt(size_t idx, bool armed) {
        if (armed) {
            --targets[idx].second;
            ++done_;
        }
        ++cursor_;
    }

private:
    size_t done_ = 0;
    size_t cursor_ = 0;
};

// Plan one rebalancing pass over `groups` -- the groups THIS shard hosts, surveyed in
// VShard order.
//
// FAIR SHARE IS PER-NODE AND MEMBERSHIP-WEIGHTED, NOT totalLed/N (debt D-12). A node
// can only lead a group it REPLICATES, so at RF < N the nodes are not
// interchangeable: of the groups this shard hosts, we are a voter in all of them and
// a given peer in only some. The old `fair = totalLed / peers.size()` divided our
// hosted count by the whole cluster, which at RF=3 on 5 nodes is ~40% of the truth --
// so every node believed itself permanently above fair share and shed leadership on
// every pass, forever. MEASURED on an idle 5-node RF=3 cluster before this fix: after
// 12 minutes with no writes the balancer was still moving 26-114 VShards per 30 s and
// the spread had stalled at ~300 of a fair 819 (min 666, max 980) instead of closing.
//
// The correct expectation for node v over the groups we host is the sum of 1/|voters|
// over those groups v is a voter of: with uniform RF it is |hosted|/RF for us and
// |hosted ∩ hosted(v)|/RF for a peer. At RF == N every group has every node as a voter,
// so expected[v] == hosted/N for every v -- the old formula's shape, and behaviourally
// the same target, which is why RF == N (production, the 3-node gates) is unchanged.
// It is not literally the same arithmetic: the old divisor counted only groups that had
// a LEADER (leaderless ones were skipped before the count) and divided in integers,
// where this counts every hosted group and divides in doubles. Both differences are
// sub-one-VShard on a converged cluster and neither changes which side of fair share a
// node lands on -- the 3-node gates measure identically before and after -- but "reduces
// exactly to the old formula" would be too strong a claim.
inline LeadershipBalancePass planLeadershipBalance(const std::vector<BalanceGroup>& groups, NodeId self,
                                                   const std::vector<NodeId>& peers, size_t maxTransfers) {
    LeadershipBalancePass p;
    if (maxTransfers == 0 || peers.empty())
        return p;  // budget stays 0: not viable
    for (size_t i = 0; i < groups.size(); ++i) {
        const BalanceGroup& g = groups[i];
        if (g.voters.empty())
            continue;
        for (NodeId v : g.voters)
            p.expected[v] += 1.0 / static_cast<double>(g.voters.size());
        // A group we do not REPLICATE tells us nothing and can never be ours to give
        // away, so the survey must not contain one; a group we replicate but that has no
        // elected leader is nobody's led count, but it IS in everyone's fair share.
        if (g.leader == kNoNode)
            continue;
        ++p.led[g.leader];
        if (g.leader == self)
            p.mine.push_back(i);
    }
    const double fairSelf = p.expected.count(self) ? p.expected.at(self) : 0.0;
    // Shed only if we are above our share by more than the tolerance -- a node holding
    // EXACTLY its share (the converged state, and the one whose last bit falls short)
    // must read as converged and stop.
    if (static_cast<double>(p.mine.size()) <= fairSelf + kShareTolerance)
        return p;  // at or below our own share: shed nothing

    for (NodeId id : peers) {
        if (id == self)
            continue;
        const double fairPeer = p.expected.count(id) ? p.expected.at(id) : 0.0;
        const size_t have = p.led.count(id) ? p.led.at(id) : 0;
        if (belowShare(have, fairPeer))
            p.targets.push_back({id, shareFloor(fairPeer) - have});
    }
    if (p.targets.empty())
        return p;
    p.budget = std::min(maxTransfers, p.mine.size() - shareFloor(fairSelf));
    return p;
}

}  // namespace timestar::data
