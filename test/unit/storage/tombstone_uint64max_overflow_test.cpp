#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm_tombstone.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>

using namespace timestar;

// =============================================================================
// Bug #21: Tombstone integer overflow when endTime == UINT64_MAX
//
// In sortAndMergeEntries, the adjacency check "entry.startTime <= current.endTime + 1"
// overflows when current.endTime == UINT64_MAX, wrapping to 0 and breaking merging.
// Similarly in addTombstone, "mergedEnd + 1" and "ranges[i].second + 1" overflow.
// The fix adds explicit UINT64_MAX guard checks to prevent the overflow.
// =============================================================================

// ---------------------------------------------------------------------------
// Unit test: sortAndMergeEntries is private, but addTombstone calls it and
// uses the same merge logic. We test through addTombstone.
// ---------------------------------------------------------------------------
class TombstoneUint64MaxTest : public ::testing::Test {
protected:
    std::unique_ptr<TSMTombstone> tombstone;

    void SetUp() override {
        // Use a temp path; we won't actually flush to disk
        tombstone = std::make_unique<TSMTombstone>("/tmp/test_tombstone_uint64max.tomb");
    }
};

TEST_F(TombstoneUint64MaxTest, MergeAdjacentToMaxDoesNotOverflow) {
    SeriesId128 id("series_overflow_test");

    // Add a range ending at UINT64_MAX
    auto result1 = tombstone->addTombstone(id, 100, UINT64_MAX).get();
    EXPECT_TRUE(result1);

    // Add a range starting at 50 (overlapping with the previous)
    auto result2 = tombstone->addTombstone(id, 50, 99).get();
    EXPECT_TRUE(result2);

    // The ranges should merge into [50, UINT64_MAX]
    auto ranges = tombstone->getTombstoneRanges(id);
    ASSERT_EQ(ranges.size(), 1u) << "Overlapping ranges should merge to a single range";
    EXPECT_EQ(ranges[0].first, 50u);
    EXPECT_EQ(ranges[0].second, UINT64_MAX);
}

TEST_F(TombstoneUint64MaxTest, AdjacentRangesAtMaxMerge) {
    SeriesId128 id("series_adjacent_max");

    // Add two adjacent ranges where the first ends at UINT64_MAX - 1
    // and the second starts at UINT64_MAX
    auto result1 = tombstone->addTombstone(id, 100, UINT64_MAX - 1).get();
    EXPECT_TRUE(result1);

    auto result2 = tombstone->addTombstone(id, UINT64_MAX, UINT64_MAX).get();
    EXPECT_TRUE(result2);

    // Should merge to [100, UINT64_MAX]
    auto ranges = tombstone->getTombstoneRanges(id);
    ASSERT_EQ(ranges.size(), 1u) << "Adjacent ranges should merge";
    EXPECT_EQ(ranges[0].first, 100u);
    EXPECT_EQ(ranges[0].second, UINT64_MAX);
}

TEST_F(TombstoneUint64MaxTest, FullRangeDeleteWorks) {
    SeriesId128 id("series_full_range");

    // Delete the entire time range
    auto result = tombstone->addTombstone(id, 0, UINT64_MAX).get();
    EXPECT_TRUE(result);

    auto ranges = tombstone->getTombstoneRanges(id);
    ASSERT_EQ(ranges.size(), 1u);
    EXPECT_EQ(ranges[0].first, 0u);
    EXPECT_EQ(ranges[0].second, UINT64_MAX);

    // Any timestamp should be deleted
    EXPECT_TRUE(tombstone->isDeleted(id, 0));
    EXPECT_TRUE(tombstone->isDeleted(id, 1000));
    EXPECT_TRUE(tombstone->isDeleted(id, UINT64_MAX));
}

TEST_F(TombstoneUint64MaxTest, NonOverlappingRangesStaySeparate) {
    SeriesId128 id("series_separate");

    // Two non-overlapping, non-adjacent ranges
    auto result1 = tombstone->addTombstone(id, 0, 100).get();
    EXPECT_TRUE(result1);

    auto result2 = tombstone->addTombstone(id, 200, UINT64_MAX).get();
    EXPECT_TRUE(result2);

    auto ranges = tombstone->getTombstoneRanges(id);
    ASSERT_EQ(ranges.size(), 2u) << "Non-adjacent ranges should remain separate";
}

// ---------------------------------------------------------------------------
// Source-inspection test for the overflow guard in sortAndMergeEntries
// ---------------------------------------------------------------------------
class TombstoneUint64MaxSourceTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef TSM_TOMBSTONE_SOURCE_PATH
        std::ifstream file(TSM_TOMBSTONE_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif
        std::vector<std::string> paths = {
            "../lib/storage/tsm_tombstone.cpp",
            "../../lib/storage/tsm_tombstone.cpp",
        };
        for (const auto& path : paths) {
            std::ifstream f(path);
            if (f.is_open()) {
                std::stringstream ss;
                ss << f.rdbuf();
                sourceCode = ss.str();
                return;
            }
        }
    }
};

TEST_F(TombstoneUint64MaxSourceTest, SortAndMergeHasOverflowGuard) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load tsm_tombstone.cpp";

    // The sortAndMergeEntries function should check for UINT64_MAX
    auto pos = sourceCode.find("sortAndMergeEntries");
    ASSERT_NE(pos, std::string::npos);

    // Extract the function body
    auto braceStart = sourceCode.find('{', pos);
    ASSERT_NE(braceStart, std::string::npos);
    int depth = 1;
    size_t i = braceStart + 1;
    while (i < sourceCode.size() && depth > 0) {
        if (sourceCode[i] == '{') depth++;
        else if (sourceCode[i] == '}') depth--;
        i++;
    }
    auto body = sourceCode.substr(pos, i - pos);

    EXPECT_NE(body.find("UINT64_MAX"), std::string::npos)
        << "sortAndMergeEntries must check for UINT64_MAX to prevent overflow in adjacency check";
}

TEST_F(TombstoneUint64MaxSourceTest, AddTombstoneHasOverflowGuard) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load tsm_tombstone.cpp";

    auto pos = sourceCode.find("addTombstone");
    ASSERT_NE(pos, std::string::npos);

    auto braceStart = sourceCode.find('{', pos);
    ASSERT_NE(braceStart, std::string::npos);
    int depth = 1;
    size_t i = braceStart + 1;
    while (i < sourceCode.size() && depth > 0) {
        if (sourceCode[i] == '{') depth++;
        else if (sourceCode[i] == '}') depth--;
        i++;
    }
    auto body = sourceCode.substr(pos, i - pos);

    EXPECT_NE(body.find("UINT64_MAX"), std::string::npos)
        << "addTombstone must check for UINT64_MAX to prevent overflow in adjacency check";
}
