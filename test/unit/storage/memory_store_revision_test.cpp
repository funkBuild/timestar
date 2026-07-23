#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/storage/memory_store.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace {

TimeStarInsert<double> makeInsert(std::vector<uint64_t> ts, std::vector<double> vals, std::vector<uint64_t> revs = {}) {
    TimeStarInsert<double> req("m", "f");
    req.timestamps = std::move(ts);
    req.values = std::move(vals);
    req.revisions = std::move(revs);
    return req;
}

TEST(MemoryStoreRevisionTest, RevisionsParallelOnOrderedInsert) {
    InMemorySeries<double> s;
    s.insert(makeInsert({10, 20, 30}, {1.0, 2.0, 3.0}, {5, 6, 7}));
    EXPECT_EQ(s.revisions, (std::vector<uint64_t>{5, 6, 7}));
    EXPECT_EQ(s.timestamps.size(), s.revisions.size());
}

TEST(MemoryStoreRevisionTest, UntrackedSeriesKeepsRevisionsEmpty) {
    InMemorySeries<double> s;
    s.insert(makeInsert({10, 20}, {1.0, 2.0}));  // no revisions
    EXPECT_TRUE(s.revisions.empty()) << "untracked series must not allocate a revision column";
}

TEST(MemoryStoreRevisionTest, DedupWithinBatchKeepsLastRevision) {
    InMemorySeries<double> s;
    // Duplicate timestamp 10, unsorted, last write (rev 9) must win value AND revision.
    s.insert(makeInsert({10, 20, 10}, {1.0, 2.0, 111.0}, {3, 4, 9}));
    ASSERT_EQ(s.timestamps.size(), 2u);
    EXPECT_EQ(s.timestamps, (std::vector<uint64_t>{10, 20}));
    EXPECT_EQ(s.values[0], 111.0);
    EXPECT_EQ(s.revisions[0], 9u) << "dedup kept the newest revision";
    EXPECT_EQ(s.revisions[1], 4u);
}

TEST(MemoryStoreRevisionTest, UnsortedBatchPermutesRevisionsWithValues) {
    InMemorySeries<double> s;
    s.insert(makeInsert({30, 10, 20}, {3.0, 1.0, 2.0}, {33, 11, 22}));
    EXPECT_EQ(s.timestamps, (std::vector<uint64_t>{10, 20, 30}));
    EXPECT_EQ(s.values, (std::vector<double>{1.0, 2.0, 3.0}));
    EXPECT_EQ(s.revisions, (std::vector<uint64_t>{11, 22, 33})) << "revisions followed the sort permutation";
}

TEST(MemoryStoreRevisionTest, OverwriteAcrossInsertsKeepsNewerRevision) {
    InMemorySeries<double> s;
    s.insert(makeInsert({10, 20, 30}, {1.0, 2.0, 3.0}, {1, 2, 3}));
    // Second insert overwrites ts 20 with a newer revision (merge path).
    s.insert(makeInsert({20}, {222.0}, {9}));
    ASSERT_EQ(s.timestamps.size(), 3u);
    EXPECT_EQ(s.values[1], 222.0);
    EXPECT_EQ(s.revisions, (std::vector<uint64_t>{1, 9, 3})) << "merge kept the newer point's revision at ts 20";
}

TEST(MemoryStoreRevisionTest, StrictAppendCarriesRevisions) {
    InMemorySeries<double> s;
    s.insert(makeInsert({10, 20}, {1.0, 2.0}, {1, 2}));
    s.insert(makeInsert({30, 40}, {3.0, 4.0}, {3, 4}));  // strictly later -> fast path
    EXPECT_EQ(s.timestamps, (std::vector<uint64_t>{10, 20, 30, 40}));
    EXPECT_EQ(s.revisions, (std::vector<uint64_t>{1, 2, 3, 4}));
}

TEST(MemoryStoreRevisionTest, UntrackedThenTrackedBackfillsFloor) {
    InMemorySeries<double> s;
    s.insert(makeInsert({10, 20}, {1.0, 2.0}));  // untracked
    s.insert(makeInsert({30}, {3.0}, {7}));      // now tracked
    ASSERT_EQ(s.revisions.size(), 3u);
    EXPECT_EQ(s.revisions, (std::vector<uint64_t>{0, 0, 7})) << "pre-existing points backfilled to migrated floor";
}

TEST(MemoryStoreRevisionTest, TrackedThenUntrackedIncomingIsFloor) {
    InMemorySeries<double> s;
    s.insert(makeInsert({10}, {1.0}, {5}));  // tracked
    s.insert(makeInsert({20}, {2.0}));       // incoming has no revision -> floor 0
    ASSERT_EQ(s.revisions.size(), 2u);
    EXPECT_EQ(s.revisions, (std::vector<uint64_t>{5, 0}));
}

}  // namespace
