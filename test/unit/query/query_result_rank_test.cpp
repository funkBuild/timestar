#include <gtest/gtest.h>
#include "query_result.hpp"
#include "tsm_result.hpp"
#include <fstream>
#include <memory>
#include <string>
#include <vector>

// Helper to create a TSMBlock with given timestamps and values
static std::unique_ptr<TSMBlock<double>> makeRankTestBlock(
    const std::vector<uint64_t>& timestamps,
    const std::vector<double>& values) {
    auto block = std::make_unique<TSMBlock<double>>(timestamps.size());
    for (size_t i = 0; i < timestamps.size(); i++) {
        block->timestamps->push_back(timestamps[i]);
        block->values->push_back(values[i]);
    }
    return block;
}

class QueryResultRankTest : public ::testing::Test {};

// Test 1: Higher rank result's values should be preferred for overlapping timestamps.
// Result A (rank=1): timestamps [100, 200, 300], values [1.0, 2.0, 3.0]
// Result B (rank=5): timestamps [200, 300, 400], values [20.0, 30.0, 40.0]
// For ts=200 and ts=300, rank 5 > rank 1, so B's values should win.
// Expected: timestamps [100, 200, 300, 400], values [1.0, 20.0, 30.0, 40.0]
TEST_F(QueryResultRankTest, HigherRankPreferredForSameTimestamp) {
    std::vector<TSMResult<double>> tsmResults;

    // Result A: rank=1 (lower priority)
    tsmResults.emplace_back(1);
    auto blockA = makeRankTestBlock({100, 200, 300}, {1.0, 2.0, 3.0});
    tsmResults[0].appendBlock(blockA);

    // Result B: rank=5 (higher priority)
    tsmResults.emplace_back(5);
    auto blockB = makeRankTestBlock({200, 300, 400}, {20.0, 30.0, 40.0});
    tsmResults[1].appendBlock(blockB);

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    // Should have 4 unique timestamps
    ASSERT_EQ(qr.timestamps.size(), 4u);
    EXPECT_EQ(qr.timestamps[0], 100u);
    EXPECT_EQ(qr.timestamps[1], 200u);
    EXPECT_EQ(qr.timestamps[2], 300u);
    EXPECT_EQ(qr.timestamps[3], 400u);

    ASSERT_EQ(qr.values.size(), 4u);
    EXPECT_DOUBLE_EQ(qr.values[0], 1.0);   // ts=100: only in A
    EXPECT_DOUBLE_EQ(qr.values[1], 20.0);  // ts=200: B wins (rank 5 > 1)
    EXPECT_DOUBLE_EQ(qr.values[2], 30.0);  // ts=300: B wins (rank 5 > 1)
    EXPECT_DOUBLE_EQ(qr.values[3], 40.0);  // ts=400: only in B
}

// Test 2: Three results with different ranks and overlapping timestamps.
// For ts=200 which appears in all three, the highest rank (7) should win.
TEST_F(QueryResultRankTest, RankPriorityWithMultipleOverlaps) {
    std::vector<TSMResult<double>> tsmResults;

    // Result A: rank=3
    tsmResults.emplace_back(3);
    auto blockA = makeRankTestBlock({100, 200}, {1.0, 2.0});
    tsmResults[0].appendBlock(blockA);

    // Result B: rank=7 (highest)
    tsmResults.emplace_back(7);
    auto blockB = makeRankTestBlock({200, 300}, {20.0, 30.0});
    tsmResults[1].appendBlock(blockB);

    // Result C: rank=5
    tsmResults.emplace_back(5);
    auto blockC = makeRankTestBlock({200, 400}, {200.0, 400.0});
    tsmResults[2].appendBlock(blockC);

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    // Unique timestamps: 100, 200, 300, 400
    ASSERT_EQ(qr.timestamps.size(), 4u);
    EXPECT_EQ(qr.timestamps[0], 100u);
    EXPECT_EQ(qr.timestamps[1], 200u);
    EXPECT_EQ(qr.timestamps[2], 300u);
    EXPECT_EQ(qr.timestamps[3], 400u);

    ASSERT_EQ(qr.values.size(), 4u);
    EXPECT_DOUBLE_EQ(qr.values[0], 1.0);    // ts=100: only in A
    EXPECT_DOUBLE_EQ(qr.values[1], 20.0);   // ts=200: B wins (rank 7 > 5 > 3)
    EXPECT_DOUBLE_EQ(qr.values[2], 30.0);   // ts=300: only in B
    EXPECT_DOUBLE_EQ(qr.values[3], 400.0);  // ts=400: only in C
}

// Test 3: Non-overlapping timestamps -- rank is irrelevant, all values present.
TEST_F(QueryResultRankTest, NoOverlapRankIrrelevant) {
    std::vector<TSMResult<double>> tsmResults;

    // Result A: rank=1
    tsmResults.emplace_back(1);
    auto blockA = makeRankTestBlock({100, 200}, {1.0, 2.0});
    tsmResults[0].appendBlock(blockA);

    // Result B: rank=5
    tsmResults.emplace_back(5);
    auto blockB = makeRankTestBlock({300, 400}, {3.0, 4.0});
    tsmResults[1].appendBlock(blockB);

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    ASSERT_EQ(qr.timestamps.size(), 4u);
    EXPECT_EQ(qr.timestamps[0], 100u);
    EXPECT_EQ(qr.timestamps[1], 200u);
    EXPECT_EQ(qr.timestamps[2], 300u);
    EXPECT_EQ(qr.timestamps[3], 400u);

    ASSERT_EQ(qr.values.size(), 4u);
    EXPECT_DOUBLE_EQ(qr.values[0], 1.0);
    EXPECT_DOUBLE_EQ(qr.values[1], 2.0);
    EXPECT_DOUBLE_EQ(qr.values[2], 3.0);
    EXPECT_DOUBLE_EQ(qr.values[3], 4.0);
}

// Test 4: Two results with the SAME rank and overlapping timestamps.
// Either value is acceptable; just verify no crash and correct timestamp count.
TEST_F(QueryResultRankTest, SameRankFirstWins) {
    std::vector<TSMResult<double>> tsmResults;

    // Result A: rank=3
    tsmResults.emplace_back(3);
    auto blockA = makeRankTestBlock({100, 200, 300}, {1.0, 2.0, 3.0});
    tsmResults[0].appendBlock(blockA);

    // Result B: rank=3 (same rank)
    tsmResults.emplace_back(3);
    auto blockB = makeRankTestBlock({200, 300, 400}, {20.0, 30.0, 40.0});
    tsmResults[1].appendBlock(blockB);

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    // Should have 4 unique timestamps regardless of which value wins
    ASSERT_EQ(qr.timestamps.size(), 4u);
    EXPECT_EQ(qr.timestamps[0], 100u);
    EXPECT_EQ(qr.timestamps[1], 200u);
    EXPECT_EQ(qr.timestamps[2], 300u);
    EXPECT_EQ(qr.timestamps[3], 400u);

    ASSERT_EQ(qr.values.size(), 4u);
    // ts=100: only in A
    EXPECT_DOUBLE_EQ(qr.values[0], 1.0);
    // ts=200 and ts=300: same rank, either value is acceptable
    EXPECT_TRUE(qr.values[1] == 2.0 || qr.values[1] == 20.0);
    EXPECT_TRUE(qr.values[2] == 3.0 || qr.values[2] == 30.0);
    // ts=400: only in B
    EXPECT_DOUBLE_EQ(qr.values[3], 40.0);
}

// Test 5: Source-code inspection -- verify TSMIterationState struct contains a rank field.
TEST_F(QueryResultRankTest, RankFieldInIterationState) {
#ifdef QUERY_RESULT_SOURCE_PATH
    std::ifstream file(QUERY_RESULT_SOURCE_PATH);
    ASSERT_TRUE(file.is_open()) << "Could not open query_result.hpp at: "
                                << QUERY_RESULT_SOURCE_PATH;

    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());

    // Verify the TSMIterationState struct contains a rank field
    EXPECT_NE(content.find("uint64_t rank"), std::string::npos)
        << "TSMIterationState should contain a 'uint64_t rank' field";

    // Verify the merge loop uses rank in its comparison
    EXPECT_NE(content.find("state->rank > minRank"), std::string::npos)
        << "Merge loop should compare state->rank > minRank for rank-based priority";

    // Verify minRank is tracked
    EXPECT_NE(content.find("uint64_t minRank"), std::string::npos)
        << "Merge loop should track minRank for rank-based priority";
#else
    GTEST_SKIP() << "QUERY_RESULT_SOURCE_PATH not defined";
#endif
}
