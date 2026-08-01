#include "../../../lib/cluster/integration/group0_startup.hpp"

#include <gtest/gtest.h>

using namespace timestar::cluster;

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
