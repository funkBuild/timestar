#include "../../../lib/storage/revision_merge.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

using timestar::mergeByRevision;
using timestar::RevisionPoint;

using DP = RevisionPoint<double>;
using SP = RevisionPoint<std::string>;

std::vector<uint64_t> timestamps(const std::vector<DP>& v) {
    std::vector<uint64_t> ts;
    for (const auto& p : v)
        ts.push_back(p.timestamp);
    return ts;
}

TEST(RevisionMergeTest, EmptyInputs) {
    EXPECT_TRUE(mergeByRevision<double>({}).empty());
    EXPECT_TRUE(mergeByRevision<double>({{}, {}}).empty());
}

TEST(RevisionMergeTest, SingleRunPassesThrough) {
    std::vector<std::vector<DP>> runs = {{{10, 1.0, 5}, {20, 2.0, 6}, {30, 3.0, 7}}};
    auto out = mergeByRevision(runs);
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(timestamps(out), (std::vector<uint64_t>{10, 20, 30}));
    EXPECT_EQ(out[1].value, 2.0);
    EXPECT_EQ(out[1].revision, 6u);
}

TEST(RevisionMergeTest, DisjointTimestampsInterleaveSorted) {
    std::vector<std::vector<DP>> runs = {
        {{10, 1.0, 1}, {30, 3.0, 3}},
        {{20, 2.0, 2}, {40, 4.0, 4}},
    };
    auto out = mergeByRevision(runs);
    EXPECT_EQ(timestamps(out), (std::vector<uint64_t>{10, 20, 30, 40}));
}

TEST(RevisionMergeTest, OverlapHighestRevisionWinsPerTimestamp) {
    // Same timestamps in both runs; run B has higher revisions -> B wins.
    std::vector<std::vector<DP>> runs = {
        {{10, 1.0, 1}, {20, 2.0, 1}},  // older
        {{10, 100.0, 9}, {20, 200.0, 9}},
    };
    auto out = mergeByRevision(runs);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].value, 100.0);
    EXPECT_EQ(out[0].revision, 9u);
    EXPECT_EQ(out[1].value, 200.0);
}

TEST(RevisionMergeTest, MigratedFloorLosesToAssignedRevision) {
    // Migrated data (revision 0) must lose to any assigned write (revision >= 1).
    std::vector<std::vector<DP>> runs = {
        {{10, -1.0, 0}},  // migrated
        {{10, 42.0, 1}},  // assigned
    };
    auto out = mergeByRevision(runs);
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].value, 42.0);
    EXPECT_EQ(out[0].revision, 1u);
}

TEST(RevisionMergeTest, MixedOverlapAndDisjoint) {
    std::vector<std::vector<DP>> runs = {
        {{10, 1.0, 5}, {20, 2.0, 5}, {50, 5.0, 5}},
        {{20, 22.0, 8}, {30, 3.0, 8}},  // wins at 20 (rev 8 > 5)
    };
    auto out = mergeByRevision(runs);
    EXPECT_EQ(timestamps(out), (std::vector<uint64_t>{10, 20, 30, 50}));
    EXPECT_EQ(out[1].value, 22.0);  // ts 20 resolved to the higher revision
    EXPECT_EQ(out[1].revision, 8u);
}

TEST(RevisionMergeTest, EqualRevisionTieIsDeterministicLaterRunWins) {
    // A genuine tie cannot happen for distinct writes; if it does the values are
    // identical. Verify the resolution is deterministic (later run wins).
    std::vector<std::vector<DP>> runs = {
        {{10, 1.0, 7}},
        {{10, 2.0, 7}},  // later run, same revision
    };
    auto out = mergeByRevision(runs);
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].value, 2.0);
}

TEST(RevisionMergeTest, ThreeRunsResolveOnePointPerTimestamp) {
    std::vector<std::vector<DP>> runs = {
        {{10, 1.0, 1}, {20, 2.0, 1}},
        {{10, 10.0, 5}, {30, 3.0, 5}},
        {{10, 99.0, 3}, {20, 22.0, 9}},
    };
    auto out = mergeByRevision(runs);
    // Distinct timestamps: 10, 20, 30 (one each).
    EXPECT_EQ(timestamps(out), (std::vector<uint64_t>{10, 20, 30}));
    EXPECT_EQ(out[0].value, 10.0);  // ts10: max rev 5 (run 1)
    EXPECT_EQ(out[1].value, 22.0);  // ts20: max rev 9 (run 2)
    EXPECT_EQ(out[2].value, 3.0);   // ts30: only run 1
}

TEST(RevisionMergeTest, WorksForStringValues) {
    std::vector<std::vector<SP>> runs = {
        {{10, "old", 1}},
        {{10, "new", 2}, {20, "b", 2}},
    };
    auto out = mergeByRevision(runs);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].value, "new");
    EXPECT_EQ(out[1].value, "b");
}

}  // namespace
