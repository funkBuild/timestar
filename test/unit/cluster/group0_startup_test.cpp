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

TEST(Group0StartupPolicyTest, DurableServingMapAdvancesStaticBootstrapWithoutAllowingEpochConflict) {
    const auto configured = completeMap();
    EXPECT_EQ(selectServingMapForStartup(configured, std::nullopt), configured);
    EXPECT_EQ(selectServingMapForStartup(configured, configured), configured);

    auto conflicting = configured;
    conflicting.placement.at(0) = {3, 2, 1};
    EXPECT_THROW(selectServingMapForStartup(configured, std::move(conflicting)), std::runtime_error);

    auto advanced = configured;
    advanced.epoch = 2;
    advanced.placement.at(0) = {3, 2, 1};
    EXPECT_EQ(selectServingMapForStartup(configured, advanced), advanced)
        << "a committed movement must survive restart instead of being rejected as non-static";

    auto configuredAhead = configured;
    configuredAhead.epoch = 3;
    EXPECT_THROW(selectServingMapForStartup(configuredAhead, advanced), std::runtime_error)
        << "a stale cache may not regress a newer bootstrap source";
    EXPECT_THROW(selectServingMapForStartup(timestar::control::ControlMap{}, std::nullopt), std::invalid_argument);
}
