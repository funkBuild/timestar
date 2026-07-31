// THE LEADERSHIP BALANCER'S ARITHMETIC (debt D-22, adopting D-24's assertions).
//
// WHAT THIS PINS, AND WHY IT DID NOT EXIST BEFORE. `ShardRaftPlane::rebalance` decides
// how leadership is spread across a cluster: what a node's fair share of the groups it
// hosts is, which peers may receive leadership of which group, how many groups one pass
// may move, and what a transfer costs the target's deficit. Every one of those was
// verified analytically in the D-12 row and measured on a live 5-node RF=3 cluster, and
// NOTHING in the suite pinned any of it -- the loop needs a whole ReplicatedDataPlane, a
// journal per VShard and a live Raft tick, so it was not unit-testable at all.
//
// That mattered in a specific direction: the 3-node gates CANNOT see the D-12 defect by
// construction. At RF == N every node is a voter of every group, so the membership-weighted
// share and the old `totalLed / N` agree exactly; the ~40 %-low target only appears at
// RF < N. A future edit could therefore restore it with every gate still green.
//
// So the arithmetic is extracted pure (lib/cluster/data/leadership_balance.hpp) in the
// shape planReadRouting took for D-13, and this file tests THE REAL FUNCTIONS. The loop
// keeps only what needs the live plane -- surveying the Raft view and awaiting the
// transfer -- which is itself asserted below, because "the tests test the balancer" is
// only true while the balancer holds no arithmetic of its own.
#include "../../../lib/cluster/data/leadership_balance.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <numeric>
#include <set>
#include <sstream>
#include <string>
#include <vector>

using timestar::data::BalanceGroup;
using timestar::data::LeadershipBalancePass;
using timestar::data::planLeadershipBalance;
using timestar::raft::kNoNode;
using timestar::raft::NodeId;

namespace {

constexpr NodeId kSelf = 1;

// Every candidate is eligible: isolates the ARITHMETIC from the liveness predicate, which
// raft_peer_liveness_test.cpp owns.
auto anyoneEligible() {
    return [](NodeId) { return true; };
}

// The groups THIS shard hosts, as the survey hands them over. `self` is a voter of all of
// them by construction -- a group we do not replicate never reaches the planner.
//
// `peerPairs` walks the 2-of-4 voter combinations so each peer replicates the same number
// of groups, which is the uniform-RF shape a real placement map produces.
std::vector<BalanceGroup> hostedGroups(size_t count, const std::vector<std::vector<NodeId>>& peerPairs,
                                       const std::vector<NodeId>& leaders) {
    std::vector<BalanceGroup> gs;
    for (size_t i = 0; i < count; ++i) {
        BalanceGroup g;
        g.vshard = static_cast<uint16_t>(i);
        g.voters.push_back(kSelf);
        for (NodeId p : peerPairs[i % peerPairs.size()])
            g.voters.push_back(p);
        g.leader = leaders[i % leaders.size()];
        gs.push_back(std::move(g));
    }
    return gs;
}

// RF == N: every node is a voter of every group.
std::vector<BalanceGroup> fullMembership(size_t count, const std::vector<NodeId>& nodes,
                                         const std::vector<NodeId>& leaders) {
    std::vector<BalanceGroup> gs;
    for (size_t i = 0; i < count; ++i)
        gs.push_back(BalanceGroup{static_cast<uint16_t>(i), nodes, leaders[i % leaders.size()]});
    return gs;
}

double totalExpected(const LeadershipBalancePass& p) {
    double sum = 0;
    for (const auto& [node, share] : p.expected)
        sum += share;
    return sum;
}

// The formula D-12 replaced, computed here so the tests can state what it WOULD have
// decided. It is a historical reference value, never the thing under test.
double oldFairShare(const std::vector<BalanceGroup>& groups, size_t nodeCount) {
    size_t totalLed = 0;
    for (const auto& g : groups)
        if (g.leader != kNoNode)
            ++totalLed;
    return static_cast<double>(totalLed / nodeCount);  // integer division, as it was
}

}  // namespace

// --- fair share ----------------------------------------------------------------------

TEST(LeadershipBalanceTest, ExpectedSharesSumToTheHostedGroupCount) {
    // THE CONSERVATION PROPERTY. Leadership of each hosted group goes to exactly one node,
    // so the fair shares of all nodes must add up to the number of groups -- no more (or
    // every node is permanently below share and nobody ever sheds) and no less (or every
    // node is permanently above share and sheds forever, which is D-12's live symptom).
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}, {3, 4}, {4, 5}, {5, 2}};
    auto groups = hostedGroups(20, pairs, {1, 1, 2, 3, 4, 5, 1, 2, 3, 4});
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3, 4, 5}, 100);
    EXPECT_NEAR(totalExpected(p), 20.0, 1e-9);

    // ... and it holds with a leaderless group in the survey, and with mixed RF.
    groups[3].leader = kNoNode;
    groups[7].voters = {kSelf, 2, 3, 4, 5};  // one group replicated everywhere
    auto q = planLeadershipBalance(groups, kSelf, {1, 2, 3, 4, 5}, 100);
    EXPECT_NEAR(totalExpected(q), 20.0, 1e-9) << "a leaderless group is still leadership that exists to be won";
}

TEST(LeadershipBalanceTest, AtRfEqualsNEveryNodeExpectsHostedOverN) {
    // WHY THE 3-NODE GATES ARE UNAFFECTED BY D-12, stated as a test. At RF == N every
    // group has every node as a voter, so the membership-weighted share collapses to
    // hosted/N for every node -- the old formula's value. This is the property that makes
    // "the gates measured no change" evidence of correctness rather than of blindness.
    for (const std::vector<NodeId>& nodes : {std::vector<NodeId>{1, 2, 3}, std::vector<NodeId>{1, 2, 3, 4, 5}}) {
        const size_t n = nodes.size();
        auto groups = fullMembership(30, nodes, nodes);
        auto p = planLeadershipBalance(groups, kSelf, nodes, 100);
        for (NodeId v : nodes)
            EXPECT_NEAR(p.expected.at(v), 30.0 / static_cast<double>(n), 1e-9)
                << "node " << v << " at RF == N == " << n;
    }
}

TEST(LeadershipBalanceTest, AtRfBelowNAConvergedNodeShedsNothingThoughTheOldFormulaWouldHave) {
    // THE D-12 DEFECT ITSELF, in the shape it was measured. 5 nodes, RF=3, 20 hosted
    // groups. We are a voter of all 20 (we host them), so our share is 20/3 = 6.67; each
    // peer replicates 10 of them, so its share is 10/3 = 3.33. The shares sum to 20.
    //
    // We lead 6 -- BELOW our share, i.e. converged -- and must shed nothing. The old
    // `totalLed / peers.size()` said 20/5 = 4, so it would have called us 2 above share
    // and moved leadership away on this pass, and on every pass after it, forever.
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}, {3, 4}, {4, 5}, {5, 2}};
    auto groups = hostedGroups(20, pairs, {1, 1, 1, 2, 3, 4, 5, 2, 3, 4, 1, 1, 1, 5, 2, 3, 4, 5, 2, 3});
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3, 4, 5}, 100);

    EXPECT_NEAR(p.expected.at(kSelf), 20.0 / 3.0, 1e-9);
    for (NodeId peer : {2, 3, 4, 5})
        EXPECT_NEAR(p.expected.at(peer), 10.0 / 3.0, 1e-9) << "peer " << peer << " replicates half the groups";
    ASSERT_EQ(p.mine.size(), 6u);
    EXPECT_FALSE(p.viable()) << "we hold 6 of a fair 6.67 -- there is nothing to shed";

    // The reference value, so the test states the size of the error rather than implying it.
    EXPECT_LT(oldFairShare(groups, 5), static_cast<double>(p.mine.size()))
        << "the old formula must have wanted to shed here, or this case proves nothing";
    EXPECT_NEAR(oldFairShare(groups, 5) / p.expected.at(kSelf), 0.6, 0.01) << "~40% low, as the D-12 row records";
}

TEST(LeadershipBalanceTest, ANodeThatReplicatesNothingWeHostIsNeverATarget) {
    // A peer in the cluster's node list that is a voter of none of OUR groups has a fair
    // share of zero over this survey, so it cannot be below its share and cannot receive
    // leadership. (It leads groups on other shards; this shard has nothing to give it.)
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}};
    auto groups = hostedGroups(9, pairs, {1, 1, 1, 1, 1, 1, 1, 2, 3});
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3, 4}, 100);
    EXPECT_EQ(p.expected.count(4), 0u);
    for (size_t i = 0; i < p.targets.size(); ++i)
        EXPECT_NE(p.targetAt(i), 4u) << "node 4 replicates none of our groups";
}

TEST(LeadershipBalanceTest, ALeaderlessGroupIsInNobodysLedCountButInEveryVotersShare) {
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}};
    auto groups = hostedGroups(6, pairs, {1, 1, 1, 1, 1, 1});
    groups[5].leader = kNoNode;
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3}, 100);
    EXPECT_EQ(p.mine.size(), 5u) << "we cannot give away a group we do not lead";
    EXPECT_EQ(p.led.count(kNoNode), 0u) << "'nobody' must never appear as a leader";
    EXPECT_NEAR(p.expected.at(kSelf), 2.0, 1e-9) << "6 groups / 3 voters -- the leaderless one included";
}

// --- budget ---------------------------------------------------------------------------

TEST(LeadershipBalanceTest, TheBudgetIsWhatWeHoldAboveFairShareCappedByMaxTransfers) {
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}};
    auto groups = hostedGroups(9, pairs, {1, 1, 1, 1, 1, 1, 1, 1, 2});
    // Share is 9/3 = 3 for each of the three voters; we lead 8, peer 2 leads 1, peer 3 none.
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3}, 100);
    ASSERT_TRUE(p.viable());
    EXPECT_EQ(p.budget, 8u - 3u);
    EXPECT_EQ(p.deficitAt(0), 2u) << "peer 2 leads 1 of a fair 3";
    EXPECT_EQ(p.deficitAt(1), 3u) << "peer 3 leads none of a fair 3";

    auto capped = planLeadershipBalance(groups, kSelf, {1, 2, 3}, 2);
    EXPECT_EQ(capped.budget, 2u) << "the caller's cap wins when it is smaller";
    EXPECT_TRUE(capped.viable());
}

TEST(LeadershipBalanceTest, APassWithNothingToDoIsNotViable) {
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}};
    // (a) we are at our own share.
    auto even = hostedGroups(9, pairs, {1, 1, 1, 2, 2, 2, 3, 3, 3});
    EXPECT_FALSE(planLeadershipBalance(even, kSelf, {1, 2, 3}, 100).viable());
    // (b) we are above share but no peer is below its own (they lead groups on other
    //     shards; over THIS survey nobody is short).
    auto noRoom = hostedGroups(3, pairs, {1, 2, 3});
    auto p = planLeadershipBalance(noRoom, kSelf, {1, 2, 3}, 100);
    EXPECT_TRUE(p.targets.empty());
    EXPECT_FALSE(p.viable());
    // (c) degenerate callers.
    auto skewed = hostedGroups(9, pairs, {1, 1, 1, 1, 1, 1, 1, 1, 2});
    EXPECT_FALSE(planLeadershipBalance(skewed, kSelf, {1, 2, 3}, 0).viable()) << "maxTransfers 0";
    EXPECT_FALSE(planLeadershipBalance(skewed, kSelf, {}, 100).viable()) << "no peers";
    EXPECT_FALSE(planLeadershipBalance({}, kSelf, {1, 2, 3}, 100).viable()) << "no hosted groups";
    // ... and a viable pass really is viable, or (a)-(c) prove nothing.
    EXPECT_TRUE(planLeadershipBalance(skewed, kSelf, {1, 2, 3}, 100).viable());
}

// --- target choice ----------------------------------------------------------------------

TEST(LeadershipBalanceTest, ANonVoterOfTheGroupIsNeverChosenAsItsTarget) {
    // THE THIRD D-12 PROPERTY. `RaftNode::transferLeadership` returns silently for a
    // target that is not a voter of that group, so aiming at one moves nothing while
    // spending the pass's budget -- 114 961 transfers "initiated" against ~1 500 that
    // happened, on the 5-node measurement.
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}, {4, 5}};
    auto groups = hostedGroups(12, pairs, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 4});
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3, 4, 5}, 100);
    ASSERT_TRUE(p.viable());
    ASSERT_GE(p.targets.size(), 2u);

    for (size_t gi : p.mine) {
        const auto& voters = groups[gi].voters;
        const size_t chosen = p.chooseTarget(voters, anyoneEligible());
        if (chosen == LeadershipBalancePass::kNoTarget)
            continue;
        EXPECT_NE(std::find(voters.begin(), voters.end(), p.targetAt(chosen)), voters.end())
            << "chose node " << p.targetAt(chosen) << " for a group it does not replicate";
    }
    // Anti-vacuity: a target that IS a voter is reachable for this group, so the loop
    // above is not passing by choosing nobody.
    EXPECT_NE(p.chooseTarget(groups[p.mine.front()].voters, anyoneEligible()), LeadershipBalancePass::kNoTarget);
}

TEST(LeadershipBalanceTest, AnIneligibleCandidateAdvancesToTheNextRatherThanSkippingTheGroup) {
    // The other half of D-12's target choice: try candidates IN TURN from the cursor
    // rather than taking whatever the cursor points at and abandoning the group if it does
    // not fit. Here the cursor points at a peer that does not replicate this group.
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}, {4, 5}};
    auto groups = hostedGroups(12, pairs, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 4});
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3, 4, 5}, 100);
    ASSERT_TRUE(p.viable());

    const std::vector<NodeId> voters = {kSelf, 4, 5};  // targets[0] is peer 2 -- not a voter here
    ASSERT_EQ(p.targetAt(0), 2u);
    const size_t chosen = p.chooseTarget(voters, anyoneEligible());
    ASSERT_NE(chosen, LeadershipBalancePass::kNoTarget) << "the group must not be skipped because the cursor missed";
    EXPECT_TRUE(p.targetAt(chosen) == 4u || p.targetAt(chosen) == 5u);

    // ... and when NO candidate fits, the group is genuinely left alone.
    EXPECT_EQ(p.chooseTarget({kSelf, 9}, anyoneEligible()), LeadershipBalancePass::kNoTarget);
    EXPECT_EQ(p.chooseTarget(voters, [](NodeId) { return false; }), LeadershipBalancePass::kNoTarget)
        << "an eligibility refusal (a dead or lagging peer) leaves the group untransferred";
}

TEST(LeadershipBalanceTest, ATargetWhoseDeficitIsSpentDropsOutOfTheRunning) {
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}};
    auto groups = hostedGroups(9, pairs, {1, 1, 1, 1, 1, 1, 1, 1, 2});
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3}, 100);
    ASSERT_EQ(p.targets.size(), 2u);
    ASSERT_EQ(p.deficitAt(0), 2u);

    const std::vector<NodeId> voters = {kSelf, 2, 3};
    for (int i = 0; i < 2; ++i) {
        const size_t idx = p.chooseTarget(voters, [](NodeId n) { return n == 2; });
        ASSERT_EQ(p.targetAt(idx), 2u);
        p.recordAttempt(idx, /*armed=*/true);
    }
    EXPECT_EQ(p.deficitAt(0), 0u);
    EXPECT_EQ(p.chooseTarget(voters, [](NodeId n) { return n == 2; }), LeadershipBalancePass::kNoTarget)
        << "a peer that has reached its fair share must stop being offered groups";
    EXPECT_NE(p.chooseTarget(voters, anyoneEligible()), LeadershipBalancePass::kNoTarget)
        << "the OTHER peer still needs groups";
}

TEST(LeadershipBalanceTest, TheCursorSpreadsTransfersAcrossTargets) {
    // Round robin, not "fill the first target then the second": leadership handed to one
    // peer in a burst is leadership that peer must then re-shed.
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}};
    auto groups = hostedGroups(9, pairs, {1, 1, 1, 1, 1, 1, 1, 1, 1});
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3}, 100);
    ASSERT_EQ(p.targets.size(), 2u);

    const std::vector<NodeId> voters = {kSelf, 2, 3};
    std::vector<NodeId> order;
    while (!p.exhausted()) {
        const size_t idx = p.chooseTarget(voters, anyoneEligible());
        if (idx == LeadershipBalancePass::kNoTarget)
            break;
        order.push_back(p.targetAt(idx));
        p.recordAttempt(idx, /*armed=*/true);
    }
    ASSERT_GE(order.size(), 4u);
    EXPECT_EQ(order[0], 2u);
    EXPECT_EQ(order[1], 3u);
    EXPECT_EQ(order[2], 2u);
    EXPECT_EQ(order[3], 3u);
}

TEST(LeadershipBalanceTest, ThePassStopsAtItsBudget) {
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}};
    auto groups = hostedGroups(9, pairs, {1, 1, 1, 1, 1, 1, 1, 1, 1});
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3}, 2);
    ASSERT_EQ(p.budget, 2u);
    const std::vector<NodeId> voters = {kSelf, 2, 3};
    size_t attempted = 0;
    for (size_t gi = 0; gi < p.mine.size(); ++gi) {
        if (p.exhausted())
            break;
        const size_t idx = p.chooseTarget(voters, anyoneEligible());
        ASSERT_NE(idx, LeadershipBalancePass::kNoTarget);
        p.recordAttempt(idx, /*armed=*/true);
        ++attempted;
    }
    EXPECT_EQ(attempted, 2u);
    EXPECT_EQ(p.done(), 2u);
}

// --- accounting: ARMED transfers only (debt D-24) ---------------------------------------

TEST(LeadershipBalanceTest, OnlyAnArmedTransferChargesDoneAndTheTargetsDeficit) {
    // D-24's rule, adopted here as the register's row asks. `transferLeadership` has silent
    // early returns the balancer cannot see -- chiefly the F2 re-arm guard, reachable
    // through the unbounded `POST /cluster/rebalance-leadership` -- so what was ASKED is not
    // what was ARMED. Counting the ask inflated `transfers_initiated`, which two gates
    // assert anti-vacuity floors on, in the direction that hides a balancer doing nothing.
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}};
    auto groups = hostedGroups(9, pairs, {1, 1, 1, 1, 1, 1, 1, 1, 1});
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3}, 100);
    const std::vector<NodeId> voters = {kSelf, 2, 3};

    const size_t idx = p.chooseTarget(voters, anyoneEligible());
    const size_t before = p.deficitAt(idx);
    p.recordAttempt(idx, /*armed=*/false);
    EXPECT_EQ(p.done(), 0u) << "an unarmed transfer moved no leadership and must not be counted";
    EXPECT_EQ(p.deficitAt(idx), before) << "nor may it spend the target's deficit";

    // ... and an ARMED one to the same peer does charge both, so the checks above are not
    // vacuously true of a pass that charges nothing at all.
    const size_t idx2 = p.chooseTarget(voters, [](NodeId n) { return n == 2; });
    ASSERT_EQ(p.targetAt(idx2), 2u);
    const size_t before2 = p.deficitAt(idx2);
    p.recordAttempt(idx2, /*armed=*/true);
    EXPECT_EQ(p.done(), 1u);
    EXPECT_EQ(p.deficitAt(idx2), before2 - 1);
}

TEST(LeadershipBalanceTest, AnUnarmedAttemptLeavesTheTargetStillNeedingGroups) {
    // The deficit half matters as much as the counter: spending it on an unarmed transfer
    // makes the REST of the pass skip a peer that still needs groups -- the balancer then
    // reports work it did not do and converges more slowly than it thinks.
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}};
    auto groups = hostedGroups(9, pairs, {1, 1, 1, 1, 1, 1, 1, 1, 2});
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3}, 100);
    const std::vector<NodeId> voters = {kSelf, 2, 3};
    auto onlyPeer2 = [](NodeId n) { return n == 2; };
    ASSERT_EQ(p.deficitAt(0), 2u);

    for (int i = 0; i < 5; ++i) {
        const size_t idx = p.chooseTarget(voters, onlyPeer2);
        ASSERT_NE(idx, LeadershipBalancePass::kNoTarget) << "attempt " << i << ": peer 2 still needs 2 groups";
        ASSERT_EQ(p.targetAt(idx), 2u);
        p.recordAttempt(idx, /*armed=*/false);
    }
    EXPECT_EQ(p.done(), 0u);
    EXPECT_EQ(p.deficitAt(0), 2u) << "five unarmed asks must cost the target nothing";

    // ... and the very next ARMED one does charge, so the check above is not vacuous.
    const size_t idx = p.chooseTarget(voters, onlyPeer2);
    p.recordAttempt(idx, /*armed=*/true);
    EXPECT_EQ(p.done(), 1u);
    EXPECT_EQ(p.deficitAt(0), 1u);
}

TEST(LeadershipBalanceTest, TheCursorAdvancesOnAnUnarmedAttemptToo) {
    // The cursor is a FAIRNESS device, not an accounting one. Holding it still on an
    // unarmed attempt would re-offer the same peer the same groups for the rest of the
    // pass -- and the commonest reason a transfer is unarmed is that this very peer
    // already has one in flight.
    const std::vector<std::vector<NodeId>> pairs = {{2, 3}};
    auto groups = hostedGroups(9, pairs, {1, 1, 1, 1, 1, 1, 1, 1, 1});
    auto p = planLeadershipBalance(groups, kSelf, {1, 2, 3}, 100);
    const std::vector<NodeId> voters = {kSelf, 2, 3};

    const size_t first = p.chooseTarget(voters, anyoneEligible());
    ASSERT_EQ(p.targetAt(first), 2u);
    p.recordAttempt(first, /*armed=*/false);
    const size_t second = p.chooseTarget(voters, anyoneEligible());
    EXPECT_EQ(p.targetAt(second), 3u) << "the next group must be offered to the NEXT peer";
    EXPECT_EQ(p.done(), 0u) << "advancing the cursor is not the same as counting a transfer";
}

// --- the extraction is the balancer, not a copy of it ------------------------------------

TEST(LeadershipBalanceTest, TheBalancerLoopHoldsNoArithmeticOfItsOwn) {
    // WHAT MAKES EVERY TEST ABOVE A TEST OF THE BALANCER. The loop cannot be unit-tested
    // (it needs a whole ReplicatedDataPlane), so behaviour preservation rests on the loop
    // CONSUMING the extracted functions rather than re-deriving anything. If the
    // arithmetic creeps back into `rebalance`, these tests keep passing while the balancer
    // changes -- which is exactly the drift D-23 files against the liveness predicate.
#ifdef SHARD_RAFT_PLANE_SOURCE_PATH
    std::ifstream in(SHARD_RAFT_PLANE_SOURCE_PATH);
    ASSERT_TRUE(in.is_open()) << "could not open " << SHARD_RAFT_PLANE_SOURCE_PATH;
    std::ostringstream ss;
    ss << in.rdbuf();
    const std::string raw = ss.str();

    // Comments are stripped: this is a claim about the CODE, and a comment quoting the old
    // formula (this file's own header does) must not trip it.
    std::string code;
    std::istringstream lines(raw);
    for (std::string line; std::getline(lines, line);) {
        const size_t c = line.find("//");
        code += (c == std::string::npos ? line : line.substr(0, c));
        code += '\n';
    }

    for (const char* needle : {"data::planLeadershipBalance(groups, self, peers, maxTransfers)", "pass.chooseTarget(",
                               "pass.recordAttempt(chosen, armed)", "co_return pass.done();"})
        EXPECT_NE(code.find(needle), std::string::npos)
            << "rebalance no longer consumes the extracted plan: missing " << needle;

    for (const char* forbidden : {"1.0 / static_cast<double>", "--deficit", "fairSelf", "targets.push_back"})
        EXPECT_EQ(code.find(forbidden), std::string::npos)
            << "the balancer's arithmetic is back in the loop, where nothing pins it: " << forbidden;
#else
    GTEST_SKIP() << "SHARD_RAFT_PLANE_SOURCE_PATH not defined";
#endif
}
