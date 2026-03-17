#include "query_result.hpp"
#include "tsm_result.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

// Helper to create a TSMBlock with given timestamps and values
static std::unique_ptr<TSMBlock<double>> makeReserveTestBlock(const std::vector<uint64_t>& timestamps,
                                                              const std::vector<double>& values) {
    auto block = std::make_unique<TSMBlock<double>>(timestamps.size());
    for (size_t i = 0; i < timestamps.size(); i++) {
        block->timestamps.push_back(timestamps[i]);
        block->values.push_back(values[i]);
    }
    return block;
}

class QueryResultReserveTest : public ::testing::Test {};

// Default-constructed QueryResult should NOT pre-allocate 10,000 entries.
TEST_F(QueryResultReserveTest, DefaultConstructorDoesNotReserve10K) {
    QueryResult<double> qr;

    // capacity() should be 0 (no pre-allocation), not 10000
    EXPECT_LT(qr.timestamps.capacity(), 10000u);
    EXPECT_LT(qr.values.capacity(), 10000u);
}

// Merging a small number of results should not over-allocate.
TEST_F(QueryResultReserveTest, SmallMergeDoesNotOverReserve) {
    std::vector<TSMResult<double>> tsmResults;

    tsmResults.emplace_back(0);
    auto block = makeReserveTestBlock({100, 200, 300}, {1.0, 2.0, 3.0});
    tsmResults[0].appendBlock(std::move(block));

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    ASSERT_EQ(qr.timestamps.size(), 3u);
    ASSERT_EQ(qr.values.size(), 3u);

    // Capacity should be close to the actual data size, not 10000.
    // After our fix, mergeTsmResults pre-calculates total points and reserves
    // exactly that amount, so capacity should be exactly 3.
    EXPECT_LE(qr.timestamps.capacity(), 100u);
    EXPECT_LE(qr.values.capacity(), 100u);
}

// Merging large results should still produce correct data (no data loss).
TEST_F(QueryResultReserveTest, LargeMergeDataCorrectness) {
    const size_t numPoints = 15000;

    std::vector<TSMResult<double>> tsmResults;
    tsmResults.emplace_back(0);

    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    timestamps.reserve(numPoints);
    values.reserve(numPoints);

    for (size_t i = 0; i < numPoints; i++) {
        timestamps.push_back((i + 1) * 1000);
        values.push_back(static_cast<double>(i));
    }

    auto block = makeReserveTestBlock(timestamps, values);
    tsmResults[0].appendBlock(std::move(block));

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    ASSERT_EQ(qr.timestamps.size(), numPoints);
    ASSERT_EQ(qr.values.size(), numPoints);

    // Verify first, middle, and last values
    EXPECT_EQ(qr.timestamps[0], 1000u);
    EXPECT_DOUBLE_EQ(qr.values[0], 0.0);
    EXPECT_EQ(qr.timestamps[numPoints / 2], (numPoints / 2 + 1) * 1000);
    EXPECT_DOUBLE_EQ(qr.values[numPoints / 2], static_cast<double>(numPoints / 2));
    EXPECT_EQ(qr.timestamps[numPoints - 1], numPoints * 1000);
    EXPECT_DOUBLE_EQ(qr.values[numPoints - 1], static_cast<double>(numPoints - 1));
}

// fromTsmResults path should work correctly and reserve based on actual data.
TEST_F(QueryResultReserveTest, FromTsmResultsReservesCorrectly) {
    std::vector<TSMResult<double>> tsmResults;

    // Two results with different block sizes
    tsmResults.emplace_back(0);
    auto block1 = makeReserveTestBlock({100, 200}, {1.0, 2.0});
    tsmResults[0].appendBlock(std::move(block1));

    tsmResults.emplace_back(1);
    auto block2 = makeReserveTestBlock({150, 250, 350}, {3.0, 4.0, 5.0});
    tsmResults[1].appendBlock(std::move(block2));

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    // Should have merged 5 unique timestamps
    ASSERT_EQ(qr.timestamps.size(), 5u);
    ASSERT_EQ(qr.values.size(), 5u);

    // Capacity should be at most the total points (2 + 3 = 5) plus
    // whatever std::vector overhead there is, but NOT 10000.
    EXPECT_LE(qr.timestamps.capacity(), 100u);
    EXPECT_LE(qr.values.capacity(), 100u);
}

// Capacity grows as needed when merging multiple blocks without 10K reserve.
TEST_F(QueryResultReserveTest, MultiBlockCapacityGrowsAsNeeded) {
    std::vector<TSMResult<double>> tsmResults;
    tsmResults.emplace_back(0);

    // Add multiple blocks
    auto block1 = makeReserveTestBlock({100, 200}, {1.0, 2.0});
    auto block2 = makeReserveTestBlock({300, 400}, {3.0, 4.0});
    auto block3 = makeReserveTestBlock({500, 600}, {5.0, 6.0});
    tsmResults[0].appendBlock(std::move(block1));
    tsmResults[0].appendBlock(std::move(block2));
    tsmResults[0].appendBlock(std::move(block3));

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    // All 6 points should be present
    ASSERT_EQ(qr.timestamps.size(), 6u);
    ASSERT_EQ(qr.values.size(), 6u);

    for (size_t i = 0; i < 6; i++) {
        EXPECT_EQ(qr.timestamps[i], (i + 1) * 100u);
        EXPECT_DOUBLE_EQ(qr.values[i], static_cast<double>(i + 1));
    }

    // Capacity should be reasonable (pre-reserved to total block size = 6)
    EXPECT_LE(qr.timestamps.capacity(), 100u);
    EXPECT_LE(qr.values.capacity(), 100u);
}

// Empty merge should result in no allocation waste.
TEST_F(QueryResultReserveTest, EmptyMergeNoAllocation) {
    std::vector<TSMResult<double>> tsmResults;

    auto qr = QueryResult<double>::fromTsmResults(tsmResults);

    EXPECT_EQ(qr.timestamps.size(), 0u);
    EXPECT_EQ(qr.values.size(), 0u);
    // No data, so no reservation needed beyond default
    EXPECT_LT(qr.timestamps.capacity(), 10000u);
    EXPECT_LT(qr.values.capacity(), 10000u);
}

// String type should also work without the 10K reserve.
TEST_F(QueryResultReserveTest, StringTypeNoOverReserve) {
    QueryResult<std::string> qr;
    EXPECT_LT(qr.timestamps.capacity(), 10000u);
}

// Bool type should also work without the 10K reserve.
TEST_F(QueryResultReserveTest, BoolTypeNoOverReserve) {
    QueryResult<bool> qr;
    EXPECT_LT(qr.timestamps.capacity(), 10000u);
}
