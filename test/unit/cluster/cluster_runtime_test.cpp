// Integration M2: the static derived placement (ClusterRuntime). Every node builds
// the SAME VShardDirectory from the [cluster] config, so routing is coordination-
// free: deterministic owner per VShard, all 4096 assigned, self id + peer addresses
// resolved, and a misconfigured node fails to start rather than routing to a
// phantom owner.
#include "../../../lib/cluster/integration/cluster_runtime.hpp"
#include "../../../lib/core/vshard.hpp"

#include <gtest/gtest.h>

using namespace timestar::cluster;
using timestar::ClusterConfig;
using timestar::data::kNoNode;
using timestar::raft::NodeId;

namespace {
ClusterConfig cfg(uint16_t nodeId, std::vector<std::string> peers) {
    ClusterConfig c;
    c.enabled = true;
    c.node_id = nodeId;
    c.peers = std::move(peers);
    return c;
}
}  // namespace

TEST(ClusterRuntimeTest, DeterministicPlacementAllVShardsAssigned) {
    auto rt = ClusterRuntime::fromConfig(cfg(2, {"a:1", "b:2", "c:3"}));
    EXPECT_EQ(rt.selfId, 2);
    EXPECT_EQ(rt.map.epoch, 1u);
    auto dir = rt.directory();
    // Every VShard is owned by round-robin node (vs % 3) + 1, none unassigned.
    for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs) {
        NodeId expect = static_cast<NodeId>((vs % 3) + 1);
        ASSERT_NE(dir.ownerOf(vs), kNoNode) << vs;
        EXPECT_EQ(dir.ownerOf(vs), expect) << vs;
    }
    // All three nodes appear as owners.
    EXPECT_EQ(dir.ownerNodes(), (std::set<NodeId>{1, 2, 3}));
}

TEST(ClusterRuntimeTest, PeerAddressesAreOneBasedAndIncludeSelf) {
    auto rt = ClusterRuntime::fromConfig(cfg(1, {"host1:8081", "host2:8082"}));
    EXPECT_EQ(rt.peerAddresses.at(1), "host1:8081");
    EXPECT_EQ(rt.peerAddresses.at(2), "host2:8082");
    EXPECT_EQ(rt.peerAddresses.size(), 2u);
}

TEST(ClusterRuntimeTest, EveryNodeComputesTheSameMap) {
    // Two different nodes of the same cluster must agree on ownership.
    auto n1 = ClusterRuntime::fromConfig(cfg(1, {"a:1", "b:2", "c:3"}));
    auto n3 = ClusterRuntime::fromConfig(cfg(3, {"a:1", "b:2", "c:3"}));
    auto d1 = n1.directory();
    auto d3 = n3.directory();
    for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs)
        ASSERT_EQ(d1.ownerOf(vs), d3.ownerOf(vs)) << vs;
    EXPECT_NE(n1.selfId, n3.selfId);  // ...but each knows its own identity
}

ClusterConfig cfgRf(uint16_t nodeId, std::vector<std::string> peers, uint16_t rf) {
    ClusterConfig c;
    c.enabled = true;
    c.partitioned = true;
    c.node_id = nodeId;
    c.peers = std::move(peers);
    c.replication_factor = rf;
    return c;
}

TEST(ClusterRuntimeTest, Rf3PlacementGivesEveryVShardThreeDistinctReplicas) {
    auto rt = ClusterRuntime::fromConfig(cfgRf(1, {"a:1", "b:2", "c:3"}, 3));
    for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs) {
        const auto& reps = rt.map.placement.at(vs);
        ASSERT_EQ(reps.size(), 3u) << vs;
        std::set<NodeId> distinct(reps.begin(), reps.end());
        EXPECT_EQ(distinct.size(), 3u) << "vs " << vs << " has duplicate replicas";
        // Primary is (vs % 3) + 1; replicas are the next two wrapping.
        EXPECT_EQ(reps[0], static_cast<NodeId>((vs % 3) + 1));
    }
    // With RF=3 over 3 nodes, this node replicates EVERY VShard.
    EXPECT_EQ(rt.localReplicaGroups().size(), timestar::VIRTUAL_SHARD_COUNT);
}

TEST(ClusterRuntimeTest, LocalReplicaGroupsAreThisNodesVoterSets) {
    // RF=1 over 3 nodes: node 2 replicates only the VShards it owns (~1/3), with a
    // single-voter set {2}.
    auto rt = ClusterRuntime::fromConfig(cfgRf(2, {"a:1", "b:2", "c:3"}, 1));
    auto groups = rt.localReplicaGroups();
    EXPECT_GT(groups.size(), 0u);
    EXPECT_LT(groups.size(), timestar::VIRTUAL_SHARD_COUNT);  // not all -- partitioned
    for (const auto& [vs, voters] : groups) {
        EXPECT_EQ(voters, (std::vector<NodeId>{2}));
        EXPECT_EQ(rt.directory().ownerOf(vs), 2);  // node 2 owns these
    }
}

TEST(ClusterRuntimeTest, Rf5OverThreeNodesFailsClosed) {
    EXPECT_THROW(ClusterRuntime::fromConfig(cfgRf(1, {"a:1", "b:2", "c:3"}, 5)), std::invalid_argument);
}

TEST(ClusterRuntimeTest, MisconfigurationFailsClosed) {
    EXPECT_THROW(ClusterRuntime::fromConfig(cfg(1, {})), std::invalid_argument);       // empty peers
    EXPECT_THROW(ClusterRuntime::fromConfig(cfg(0, {"a:1"})), std::invalid_argument);  // node_id < 1
    EXPECT_THROW(ClusterRuntime::fromConfig(cfg(3, {"a:1", "b:2"})), std::invalid_argument);  // node_id > N
}
