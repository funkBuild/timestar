#include "../../../lib/cluster/data/write_record.hpp"
#include "../../../lib/cluster/integration/group0_startup.hpp"

#include <gtest/gtest.h>

using namespace timestar::cluster;

namespace {

timestar::control::ControlMap completeMap() {
    timestar::control::ControlMap map;
    map.epoch = 1;
    for (uint16_t vshard = 0; vshard < timestar::VIRTUAL_SHARD_COUNT; ++vshard)
        map.placement.emplace(vshard, std::vector<timestar::raft::NodeId>{1, 2, 3});
    return map;
}

timestar::control::NodeCapabilityAdvertisement capability(
    timestar::raft::NodeId id, std::string uuid, std::string address,
    std::string cluster = std::string(32, 'a')) {
    return {std::move(cluster),
            timestar::control::NodeRecord{id, std::move(uuid), std::move(address), "rack-a",
                                          timestar::control::NodeState::Active},
            {1, timestar::data::kWriteBatchFormatV7}};
}

timestar::control::LegacyReceiptInventoryAdvertisement receiptInventory(
    timestar::raft::NodeId id, std::string uuid, std::string address, uint64_t legacy = 3,
    uint64_t total = 4) {
    timestar::control::LegacyReceiptInventoryAdvertisement inventory{
        std::string(32, 'a'),
        timestar::control::NodeRecord{id, std::move(uuid), std::move(address), "rack-a",
                                      timestar::control::NodeState::Active},
        {}};
    inventory.entries.reserve(timestar::VIRTUAL_SHARD_COUNT);
    for (uint16_t vshard = 0; vshard < timestar::VIRTUAL_SHARD_COUNT; ++vshard)
        inventory.entries.push_back({vshard, legacy, total});
    return inventory;
}

}  // namespace

TEST(Group0StartupPolicyTest, FreshSeedNeverInitializesImplicitly) {
    auto decision = decideGroup0Startup(true, /*self=*/1, /*seed=*/1, false, false);
    EXPECT_EQ(decision.mode, Group0StartMode::AwaitExplicitBootstrap);
    EXPECT_FALSE(decision.host());
    EXPECT_TRUE(decision.initialVoters.empty());
}

TEST(Group0StartupPolicyTest, ExplicitBootstrapIsRestrictedToTheSeed) {
    auto decision = decideGroup0Startup(true, /*self=*/1, /*seed=*/1, true, false);
    EXPECT_EQ(decision.mode, Group0StartMode::Bootstrap);
    EXPECT_TRUE(decision.bootstrap());
    EXPECT_EQ(decision.initialVoters, (std::vector<timestar::raft::NodeId>{1}));
    EXPECT_THROW(decideGroup0Startup(true, 2, 1, true, false), std::invalid_argument);
    EXPECT_THROW(decideGroup0Startup(false, 1, 1, true, false), std::invalid_argument);
}

TEST(Group0StartupPolicyTest, ExistingJournalRecoversAndFreshNonSeedObserves) {
    auto recovery = decideGroup0Startup(true, /*self=*/1, /*seed=*/1, false, true);
    EXPECT_EQ(recovery.mode, Group0StartMode::Recover);
    EXPECT_TRUE(recovery.host());
    auto observer = decideGroup0Startup(true, /*self=*/2, /*seed=*/1, false, false);
    EXPECT_EQ(observer.mode, Group0StartMode::Observe);
    EXPECT_TRUE(observer.host());
    EXPECT_EQ(observer.initialVoters, (std::vector<timestar::raft::NodeId>{1}));
}

TEST(Group0StartupPolicyTest, DurableInitialServingMapMustMatchBoundStaticTopology) {
    const auto configured = completeMap();
    EXPECT_EQ(selectServingMapForStartup(configured, std::nullopt), configured);
    EXPECT_EQ(selectServingMapForStartup(configured, configured), configured);

    auto conflicting = configured;
    conflicting.placement.at(0) = {3, 2, 1};
    EXPECT_THROW(selectServingMapForStartup(configured, std::move(conflicting)), std::runtime_error);
    EXPECT_THROW(selectServingMapForStartup(timestar::control::ControlMap{}, std::nullopt), std::invalid_argument);
}

TEST(Group0StartupPolicyTest, CapabilityCollectionBindsEveryConfiguredNodeIdentity) {
    const std::string cluster(32, 'a');
    const std::map<timestar::raft::NodeId, std::string> peers = {
        {1, "node-1.example:8086"}, {2, "node-2.example:8086"}};
    std::map<timestar::raft::NodeId, timestar::control::NodeCapabilityAdvertisement> capabilities = {
        {1, capability(1, std::string(32, '1'), peers.at(1))},
        {2, capability(2, std::string(32, '2'), peers.at(2))}};

    const auto versions = validateNodeCapabilities(cluster, peers, capabilities);
    EXPECT_EQ(versions.size(), peers.size());
    EXPECT_EQ(versions.at(2).max, timestar::data::kWriteBatchFormatV7);

    auto invalid = capabilities;
    invalid.at(2).record.uuid = invalid.at(1).record.uuid;
    EXPECT_THROW(validateNodeCapabilities(cluster, peers, invalid), NodeCapabilityValidationError);
    invalid = capabilities;
    invalid.at(2).clusterUuid = std::string(32, 'b');
    EXPECT_THROW(validateNodeCapabilities(cluster, peers, invalid), NodeCapabilityValidationError);
    invalid = capabilities;
    invalid.at(2).record.address = "wrong.example:8086";
    EXPECT_THROW(validateNodeCapabilities(cluster, peers, invalid), NodeCapabilityValidationError);
    invalid.erase(2);
    EXPECT_NO_THROW(validateObservedNodeCapabilities(cluster, peers, invalid));
    invalid.at(1).clusterUuid = std::string(32, 'b');
    EXPECT_THROW(validateObservedNodeCapabilities(cluster, peers, invalid), NodeCapabilityValidationError)
        << "a missing peer must not hide a conflicting reply from an observed peer";
    invalid = capabilities;
    invalid.erase(2);
    EXPECT_THROW(validateNodeCapabilities(cluster, peers, invalid), NodeCapabilityValidationError);
}

TEST(Group0StartupPolicyTest, LegacyReceiptPreflightRequiresEveryAgreeingServingReplica) {
    const std::string cluster(32, 'a');
    const std::map<timestar::raft::NodeId, std::string> peers = {
        {1, "node-1.example:8086"}, {2, "node-2.example:8086"}, {3, "node-3.example:8086"}};
    std::map<timestar::raft::NodeId, timestar::control::LegacyReceiptInventoryAdvertisement> inventories = {
        {1, receiptInventory(1, std::string(32, '1'), peers.at(1))},
        {2, receiptInventory(2, std::string(32, '2'), peers.at(2))},
        {3, receiptInventory(3, std::string(32, '3'), peers.at(3))}};

    const auto summary = validateLegacyReceiptInventories(cluster, peers, completeMap(), inventories);
    EXPECT_EQ(summary.vshards, timestar::VIRTUAL_SHARD_COUNT);
    EXPECT_EQ(summary.maxLegacyReceipts, 3u);
    EXPECT_EQ(summary.maxTotalReceipts, 4u);

    auto invalid = inventories;
    invalid.at(2).entries.pop_back();
    EXPECT_THROW(validateLegacyReceiptInventories(cluster, peers, completeMap(), invalid),
                 NodeCapabilityValidationError);
    invalid = inventories;
    invalid.at(3).entries.at(7).legacyReceipts = 2;
    EXPECT_THROW(validateLegacyReceiptInventories(cluster, peers, completeMap(), invalid),
                 NodeCapabilityValidationError);
    invalid = inventories;
    invalid.at(2).entries.at(7).hasUnappliedEntries = true;
    EXPECT_THROW(validateLegacyReceiptInventories(cluster, peers, completeMap(), invalid),
                 NodeCapabilityValidationError);
    invalid = inventories;
    for (auto& entry : invalid.at(1).entries)
        entry = {entry.vshard, timestar::data::kMaxDeleteReceiptsPerVShard,
                 timestar::data::kMaxDeleteReceiptsPerVShard};
    for (auto& entry : invalid.at(2).entries)
        entry = {entry.vshard, timestar::data::kMaxDeleteReceiptsPerVShard,
                 timestar::data::kMaxDeleteReceiptsPerVShard};
    for (auto& entry : invalid.at(3).entries)
        entry = {entry.vshard, timestar::data::kMaxDeleteReceiptsPerVShard,
                 timestar::data::kMaxDeleteReceiptsPerVShard};
    EXPECT_THROW(validateLegacyReceiptInventories(cluster, peers, completeMap(), invalid),
                 NodeCapabilityValidationError);
}
