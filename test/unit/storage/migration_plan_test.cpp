#include "../../../lib/storage/migration_plan.hpp"

#include "../../../lib/core/placement_table.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace {

using timestar::MigrationPlan;
using timestar::VShardId;
// SeriesId128 is in the global namespace.

SeriesId128 sid(const std::string& key) {
    return SeriesId128::fromSeriesKey(key);
}

TEST(MigrationPlanTest, AssignsSeriesToItsDerivedVShard) {
    MigrationPlan plan;
    const auto s = sid("weather,loc=west temp");
    plan.assign(s);

    const VShardId expected{timestar::virtualShard(s)};
    EXPECT_EQ(plan.assignedCount(), 1u);
    EXPECT_EQ(plan.targetVShards(), (std::vector<VShardId>{expected}));
    ASSERT_EQ(plan.seriesForVShard(expected).size(), 1u);
    EXPECT_EQ(plan.seriesForVShard(expected)[0], s);
}

TEST(MigrationPlanTest, AssignIsIdempotent) {
    MigrationPlan plan;
    const auto s = sid("m,tag=a f");
    plan.assign(s);
    plan.assign(s);
    plan.assign(s);
    EXPECT_EQ(plan.assignedCount(), 1u) << "re-assigning a series must not double-count it";
    const VShardId vs{timestar::virtualShard(s)};
    EXPECT_EQ(plan.seriesForVShard(vs).size(), 1u);
}

TEST(MigrationPlanTest, OrphansAreQuarantinedAndCountedNeverAssigned) {
    MigrationPlan plan;
    plan.assign(sid("good,x=1 v"));
    plan.quarantineOrphan(sid("orphan,x=2 v"), "no metadata for series");
    plan.quarantineOrphan(sid("orphan,x=3 v"), "unreadable index entry");

    EXPECT_EQ(plan.assignedCount(), 1u);
    EXPECT_EQ(plan.orphanCount(), 2u);
    ASSERT_EQ(plan.orphans().size(), 2u);
    EXPECT_EQ(plan.orphans()[0].reason, "no metadata for series");
    // An orphan is never among the assigned target series.
    for (const auto vs : plan.targetVShards()) {
        for (const auto& s : plan.seriesForVShard(vs)) {
            EXPECT_NE(s, sid("orphan,x=2 v"));
            EXPECT_NE(s, sid("orphan,x=3 v"));
        }
    }
}

// A series is classified at most once: assign+quarantine of the same series does
// not double-count, and the first classification wins (mutual exclusion).
TEST(MigrationPlanTest, AssignAndQuarantineAreMutuallyExclusive) {
    {
        MigrationPlan plan;
        const auto s = sid("dual,x=1 v");
        plan.assign(s);
        plan.quarantineOrphan(s, "late orphan claim");  // ignored: already assigned
        EXPECT_EQ(plan.assignedCount(), 1u);
        EXPECT_EQ(plan.orphanCount(), 0u) << "an already-assigned series must not also be an orphan";
    }
    {
        MigrationPlan plan;
        const auto s = sid("dual,x=2 v");
        plan.quarantineOrphan(s, "orphan");
        plan.quarantineOrphan(s, "again");  // idempotent
        plan.assign(s);                     // ignored: already quarantined
        EXPECT_EQ(plan.orphanCount(), 1u) << "duplicate orphan quarantine must not double-count";
        EXPECT_EQ(plan.assignedCount(), 0u);
    }
}

TEST(MigrationPlanTest, FreeSpacePreconditionRequiresBothGenerationsPlusHeadroom) {
    // legacy 100 + rewritten 80 + headroom 20 = 200 required.
    EXPECT_TRUE(MigrationPlan::freeSpaceSufficient(100, 80, 200, 20));
    EXPECT_TRUE(MigrationPlan::freeSpaceSufficient(100, 80, 500, 20));
    EXPECT_FALSE(MigrationPlan::freeSpaceSufficient(100, 80, 199, 20)) << "one byte short must fail closed";
    EXPECT_FALSE(MigrationPlan::freeSpaceSufficient(100, 80, 0, 20));

    // Overflow must fail closed, not wrap to a passing sum.
    constexpr uint64_t big = std::numeric_limits<uint64_t>::max();
    EXPECT_FALSE(MigrationPlan::freeSpaceSufficient(big, 1, big, 0));
    EXPECT_FALSE(MigrationPlan::freeSpaceSufficient(big, 0, big, 1));
}

}  // namespace
