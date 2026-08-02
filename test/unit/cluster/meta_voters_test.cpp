// Meta-voter auto-selection: spread across failure domains, deterministic,
// churn-minimizing. Pure -- no reactor.
#include "../../../lib/cluster/control/meta_voters.hpp"

#include <gtest/gtest.h>

using namespace timestar::control;

namespace {

std::map<NodeId, NodeRecord> makeNodes(std::vector<std::tuple<NodeId, std::string, NodeState>> specs) {
    std::map<NodeId, NodeRecord> m;
    for (auto& [id, dom, st] : specs) {
        NodeRecord r;
        r.raftId = id;
        r.uuid = "u" + std::to_string(id);
        r.failureDomain = dom;
        r.state = st;
        m[id] = r;
    }
    return m;
}

}  // namespace

TEST(MetaVotersTest, SingleNodeGroupOfOne) {
    auto nodes = makeNodes({{1, "rack-a", NodeState::Active}});
    EXPECT_EQ(selectMetaVoters(nodes, {}, 3), (std::vector<NodeId>{1}));
}

TEST(MetaVotersTest, GrowsAcrossDistinctDomains) {
    // Three nodes in three domains -> one voter per domain.
    auto nodes = makeNodes(
        {{1, "rack-a", NodeState::Active}, {2, "rack-b", NodeState::Active}, {3, "rack-c", NodeState::Active}});
    EXPECT_EQ(selectMetaVoters(nodes, {}, 3), (std::vector<NodeId>{1, 2, 3}));
}

TEST(MetaVotersTest, PrefersDomainDiversityOverPacking) {
    // rack-a has 3 nodes, rack-b and rack-c one each. A 3-voter set must take one
    // per domain (1,4,5), NOT three from rack-a.
    auto nodes = makeNodes({{1, "rack-a", NodeState::Active},
                            {2, "rack-a", NodeState::Active},
                            {3, "rack-a", NodeState::Active},
                            {4, "rack-b", NodeState::Active},
                            {5, "rack-c", NodeState::Active}});
    EXPECT_EQ(selectMetaVoters(nodes, {}, 3), (std::vector<NodeId>{1, 4, 5}));
}

TEST(MetaVotersTest, FewerDomainsThanTargetPacksSecondPass) {
    // Only two domains but target 3: one per domain first, then a second from the
    // domain with spare capacity.
    auto nodes = makeNodes(
        {{1, "rack-a", NodeState::Active}, {2, "rack-a", NodeState::Active}, {3, "rack-b", NodeState::Active}});
    auto v = selectMetaVoters(nodes, {}, 3);
    EXPECT_EQ(v, (std::vector<NodeId>{1, 2, 3}));
}

TEST(MetaVotersTest, SkipsNonActiveNodes) {
    auto nodes = makeNodes({{1, "rack-a", NodeState::Active},
                            {2, "rack-b", NodeState::Draining},
                            {3, "rack-c", NodeState::Removed},
                            {4, "rack-d", NodeState::Joining}});
    EXPECT_EQ(selectMetaVoters(nodes, {}, 3), (std::vector<NodeId>{1}));  // only node 1 is Active
}

TEST(MetaVotersTest, MinimizesChurnKeepingCurrentVoters) {
    // rack-a has nodes 1 and 5; if 1 is already a voter, keep it rather than
    // switching to 5.
    auto nodes = makeNodes({{1, "rack-a", NodeState::Active},
                            {5, "rack-a", NodeState::Active},
                            {2, "rack-b", NodeState::Active},
                            {3, "rack-c", NodeState::Active}});
    auto v = selectMetaVoters(nodes, /*current=*/{1, 2, 3}, 3);
    EXPECT_EQ(v, (std::vector<NodeId>{1, 2, 3}));  // no churn to 5
}

TEST(MetaVotersTest, Deterministic) {
    auto nodes = makeNodes({{3, "rack-c", NodeState::Active},
                            {1, "rack-a", NodeState::Active},
                            {2, "rack-b", NodeState::Active},
                            {4, "rack-a", NodeState::Active}});
    EXPECT_EQ(selectMetaVoters(nodes, {}, 3), selectMetaVoters(nodes, {}, 3));
}

TEST(MetaVotersTest, DiffDetection) {
    EXPECT_FALSE(metaVotersDiffer({1, 2, 3}, {3, 2, 1}));  // same set, different order
    EXPECT_TRUE(metaVotersDiffer({1, 2, 3}, {1, 2, 4}));
    EXPECT_TRUE(metaVotersDiffer({1}, {1, 2, 3}));
}
