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

TEST(ClusterRuntimeTest, MisconfigurationFailsClosed) {
    EXPECT_THROW(ClusterRuntime::fromConfig(cfg(1, {})), std::invalid_argument);       // empty peers
    EXPECT_THROW(ClusterRuntime::fromConfig(cfg(0, {"a:1"})), std::invalid_argument);  // node_id < 1
    EXPECT_THROW(ClusterRuntime::fromConfig(cfg(3, {"a:1", "b:2"})), std::invalid_argument);  // node_id > N
}
