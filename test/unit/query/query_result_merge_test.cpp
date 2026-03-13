#include "query_result.hpp"
#include "tsm_result.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

// Helper to create a TSMBlock with given timestamps and values
static std::unique_ptr<TSMBlock<double>> makeBlock(const std::vector<uint64_t>& timestamps,
                                                   const std::vector<double>& values) {
    auto block = std::make_unique<TSMBlock<double>>(timestamps.size());
    for (size_t i = 0; i < timestamps.size(); i++) {
        block->timestamps.push_back(timestamps[i]);
        block->values.push_back(values[i]);
    }
    return block;
}

class QueryResultMergeTest : public ::testing::Test {};

// Test that merging works when some TSMResults have no blocks (null getBlock(0)).
// Before the fix, this caused out-of-bounds access because the merge loops
// iterated using the original tsmResults count instead of blockIterState.size().
TEST_F(QueryResultMergeTest, NullBlocksSkippedWithoutOOB) {
    std::vector<TSMResult<double>> tsmResults;

    // Result 0: empty (no blocks) -- getBlock(0) returns nullptr
    tsmResults.emplace_back(0);

    // Result 1: has data
    tsmResults.emplace_back(1);
    auto block1 = makeBlock({100, 200, 300}, {1.0, 2.0, 3.0});
    tsmResults[1].appendBlock(block1);

    // Result 2: also empty
    tsmResults.emplace_back(2);

    // Result 3: has data
    tsmResults.emplace_back(3);
    auto block3 = makeBlock({150, 250}, {4.0, 5.0});
    tsmResults[3].appendBlock(block3);

    // Result 4: empty
    tsmResults.emplace_back(4);

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    // Should produce a sorted, deduplicated merge of both valid results:
    // timestamps: 100, 150, 200, 250, 300
    ASSERT_EQ(qr.timestamps.size(), 5u);
    EXPECT_EQ(qr.timestamps[0], 100u);
    EXPECT_EQ(qr.timestamps[1], 150u);
    EXPECT_EQ(qr.timestamps[2], 200u);
    EXPECT_EQ(qr.timestamps[3], 250u);
    EXPECT_EQ(qr.timestamps[4], 300u);

    ASSERT_EQ(qr.values.size(), 5u);
    EXPECT_DOUBLE_EQ(qr.values[0], 1.0);
    EXPECT_DOUBLE_EQ(qr.values[1], 4.0);
    EXPECT_DOUBLE_EQ(qr.values[2], 2.0);
    EXPECT_DOUBLE_EQ(qr.values[3], 5.0);
    EXPECT_DOUBLE_EQ(qr.values[4], 3.0);
}

// Test that duplicate timestamps across results are deduplicated
// (the merge takes the value from the result with the earliest min timestamp).
TEST_F(QueryResultMergeTest, DuplicateTimestampsDeduplicated) {
    std::vector<TSMResult<double>> tsmResults;

    // Result 0: empty
    tsmResults.emplace_back(0);

    // Result 1: timestamps 100, 200, 300
    tsmResults.emplace_back(1);
    auto block1 = makeBlock({100, 200, 300}, {10.0, 20.0, 30.0});
    tsmResults[1].appendBlock(block1);

    // Result 2: empty
    tsmResults.emplace_back(2);

    // Result 3: timestamps 200, 300, 400 (overlaps with result 1)
    tsmResults.emplace_back(3);
    auto block3 = makeBlock({200, 300, 400}, {21.0, 31.0, 41.0});
    tsmResults[3].appendBlock(block3);

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    // Merge deduplicates by advancing past timestamps <= min.
    // Expected unique timestamps: 100, 200, 300, 400
    ASSERT_EQ(qr.timestamps.size(), 4u);
    EXPECT_EQ(qr.timestamps[0], 100u);
    EXPECT_EQ(qr.timestamps[1], 200u);
    EXPECT_EQ(qr.timestamps[2], 300u);
    EXPECT_EQ(qr.timestamps[3], 400u);
}

// Test merging when the only valid result is NOT the first in the vector
// (null entries at the front). This specifically exercises the tsmResultIndex
// tracking: when advancing blocks, we must reference the correct TSMResult.
TEST_F(QueryResultMergeTest, ValidResultNotFirst) {
    std::vector<TSMResult<double>> tsmResults;

    // Results 0, 1, 2: all empty
    tsmResults.emplace_back(0);
    tsmResults.emplace_back(1);
    tsmResults.emplace_back(2);

    // Result 3: has data across two blocks
    tsmResults.emplace_back(3);
    auto blockA = makeBlock({100, 200}, {1.0, 2.0});
    auto blockB = makeBlock({300, 400}, {3.0, 4.0});
    tsmResults[3].appendBlock(blockA);
    tsmResults[3].appendBlock(blockB);

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

// Test with multiple valid results interleaved with empty ones,
// where valid results have multiple blocks each. This exercises
// block advancement using the correct tsmResultIndex.
TEST_F(QueryResultMergeTest, MultipleValidResultsMultipleBlocks) {
    std::vector<TSMResult<double>> tsmResults;

    // Result 0: empty
    tsmResults.emplace_back(0);

    // Result 1: two blocks
    tsmResults.emplace_back(1);
    auto b1a = makeBlock({100, 300}, {1.0, 3.0});
    auto b1b = makeBlock({500, 700}, {5.0, 7.0});
    tsmResults[1].appendBlock(b1a);
    tsmResults[1].appendBlock(b1b);

    // Result 2: empty
    tsmResults.emplace_back(2);

    // Result 3: two blocks
    tsmResults.emplace_back(3);
    auto b3a = makeBlock({200, 400}, {2.0, 4.0});
    auto b3b = makeBlock({600, 800}, {6.0, 8.0});
    tsmResults[3].appendBlock(b3a);
    tsmResults[3].appendBlock(b3b);

    // Result 4: empty
    tsmResults.emplace_back(4);

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    // All timestamps interleaved and sorted: 100, 200, 300, 400, 500, 600, 700, 800
    ASSERT_EQ(qr.timestamps.size(), 8u);
    for (size_t i = 0; i < 8; i++) {
        EXPECT_EQ(qr.timestamps[i], (i + 1) * 100u);
        EXPECT_DOUBLE_EQ(qr.values[i], static_cast<double>(i + 1));
    }
}

// Test that all results being empty produces an empty QueryResult
TEST_F(QueryResultMergeTest, AllEmptyResults) {
    std::vector<TSMResult<double>> tsmResults;
    tsmResults.emplace_back(0);
    tsmResults.emplace_back(1);
    tsmResults.emplace_back(2);

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    EXPECT_EQ(qr.timestamps.size(), 0u);
    EXPECT_EQ(qr.values.size(), 0u);
}

// Test with an empty tsmResults vector
TEST_F(QueryResultMergeTest, EmptyVector) {
    std::vector<TSMResult<double>> tsmResults;

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    EXPECT_EQ(qr.timestamps.size(), 0u);
    EXPECT_EQ(qr.values.size(), 0u);
}

// Test with a single result that has data (no null blocks at all)
TEST_F(QueryResultMergeTest, SingleValidResult) {
    std::vector<TSMResult<double>> tsmResults;
    tsmResults.emplace_back(0);
    auto block = makeBlock({10, 20, 30}, {1.5, 2.5, 3.5});
    tsmResults[0].appendBlock(block);

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    ASSERT_EQ(qr.timestamps.size(), 3u);
    EXPECT_EQ(qr.timestamps[0], 10u);
    EXPECT_EQ(qr.timestamps[1], 20u);
    EXPECT_EQ(qr.timestamps[2], 30u);
    EXPECT_DOUBLE_EQ(qr.values[0], 1.5);
    EXPECT_DOUBLE_EQ(qr.values[1], 2.5);
    EXPECT_DOUBLE_EQ(qr.values[2], 3.5);
}
