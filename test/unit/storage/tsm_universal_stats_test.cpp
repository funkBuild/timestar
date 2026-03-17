// Tests for Phase 0 (tombstone per-block overlap check) and Phase 1 (universal block stats)

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/encoding/string_encoder.hpp"
#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/query/block_aggregator.hpp"
#include "../../../lib/query/query_parser.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_writer.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>

namespace fs = std::filesystem;
using timestar::AggregationMethod;
using timestar::BlockAggregator;

class TSMUniversalStatsTest : public ::testing::Test {
protected:
    std::string testDir = "./test_tsm_universal_stats";

    void SetUp() override { fs::create_directories(testDir); }
    void TearDown() override { fs::remove_all(testDir); }
    std::string getTestFilePath(const std::string& filename) { return testDir + "/" + filename; }
};

// ==================== Phase 1: Integer Block Stats ====================

seastar::future<> testIntegerBlockStatsRoundTrip(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.int_series");

    // Write TSM file with integer series
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000};
        std::vector<int64_t> values = {10, 20, 30, 40, 50};
        writer.writeSeries(TSMValueType::Integer, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // Read back and verify block stats
    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    if (!entry) co_return;
    EXPECT_EQ(entry->indexBlocks.size(), 1u);
    if (entry->indexBlocks.empty()) co_return;

    const auto& block = entry->indexBlocks[0];
    EXPECT_EQ(block.blockCount, 5u);
    EXPECT_DOUBLE_EQ(block.blockSum, 150.0);  // 10+20+30+40+50
    EXPECT_DOUBLE_EQ(block.blockMin, 10.0);
    EXPECT_DOUBLE_EQ(block.blockMax, 50.0);
    EXPECT_DOUBLE_EQ(block.blockFirstValue, 10.0);   // at minTime=1000
    EXPECT_DOUBLE_EQ(block.blockLatestValue, 50.0);  // at maxTime=5000
    EXPECT_TRUE(block.hasExtendedStats);

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, IntegerBlockStatsRoundTrip) {
    seastar::async([&] { testIntegerBlockStatsRoundTrip(getTestFilePath("0_1.tsm")).get(); });
}

// Test Integer COUNT pushdown via aggregateSeries
seastar::future<> testIntegerCountPushdown(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.int_count");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
        std::vector<int64_t> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        writer.writeSeries(TSMValueType::Integer, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    // COUNT pushdown: should use block stats without decoding
    timestar::BlockAggregator aggregator(0, AggregationMethod::COUNT);
    aggregator.setFoldToSingleState(false);
    size_t pts = co_await tsm.aggregateSeries(seriesId, 0, UINT64_MAX, aggregator);
    EXPECT_EQ(pts, 10u);

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, IntegerCountPushdown) {
    seastar::async([&] { testIntegerCountPushdown(getTestFilePath("0_2.tsm")).get(); });
}

// Test Integer SUM/MIN/MAX pushdown
seastar::future<> testIntegerSumMinMaxPushdown(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.int_agg");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000};
        std::vector<int64_t> values = {10, -5, 30, 20, -10};
        writer.writeSeries(TSMValueType::Integer, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    // SUM pushdown
    {
        timestar::BlockAggregator aggregator(0, AggregationMethod::SUM);
        aggregator.setFoldToSingleState(false);
        size_t pts = co_await tsm.aggregateSeries(seriesId, 0, UINT64_MAX, aggregator);
        EXPECT_EQ(pts, 5u);
        auto state = aggregator.takeSingleState();
        EXPECT_DOUBLE_EQ(state.sum, 45.0);  // 10+(-5)+30+20+(-10)
    }

    // MIN pushdown
    {
        timestar::BlockAggregator aggregator(0, AggregationMethod::MIN);
        aggregator.setFoldToSingleState(false);
        size_t pts = co_await tsm.aggregateSeries(seriesId, 0, UINT64_MAX, aggregator);
        EXPECT_EQ(pts, 5u);
        auto state = aggregator.takeSingleState();
        EXPECT_DOUBLE_EQ(state.min, -10.0);
    }

    // MAX pushdown
    {
        timestar::BlockAggregator aggregator(0, AggregationMethod::MAX);
        aggregator.setFoldToSingleState(false);
        size_t pts = co_await tsm.aggregateSeries(seriesId, 0, UINT64_MAX, aggregator);
        EXPECT_EQ(pts, 5u);
        auto state = aggregator.takeSingleState();
        EXPECT_DOUBLE_EQ(state.max, 30.0);
    }

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, IntegerSumMinMaxPushdown) {
    seastar::async([&] { testIntegerSumMinMaxPushdown(getTestFilePath("0_3.tsm")).get(); });
}

// ==================== Phase 1: Boolean Block Stats ====================

seastar::future<> testBoolBlockStatsRoundTrip(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.bool_series");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000};
        std::vector<bool> values = {true, false, true, true, false};
        writer.writeSeries(TSMValueType::Boolean, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    if (!entry) co_return;
    EXPECT_EQ(entry->indexBlocks.size(), 1u);
    if (entry->indexBlocks.empty()) co_return;

    const auto& block = entry->indexBlocks[0];
    EXPECT_EQ(block.blockCount, 5u);
    EXPECT_EQ(block.boolTrueCount, 3u);
    EXPECT_EQ(block.boolFirstValue, true);    // at minTime=1000
    EXPECT_EQ(block.boolLatestValue, false);  // at maxTime=5000
    EXPECT_TRUE(block.hasExtendedStats);
    // Aggregator-compatible double fields
    EXPECT_DOUBLE_EQ(block.blockSum, 3.0);    // trueCount
    EXPECT_DOUBLE_EQ(block.blockMax, 1.0);    // has at least one true
    EXPECT_DOUBLE_EQ(block.blockMin, 0.0);    // has at least one false
    EXPECT_DOUBLE_EQ(block.blockFirstValue, 1.0);   // true
    EXPECT_DOUBLE_EQ(block.blockLatestValue, 0.0);  // false

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, BoolBlockStatsRoundTrip) {
    seastar::async([&] { testBoolBlockStatsRoundTrip(getTestFilePath("0_4.tsm")).get(); });
}

// Test Boolean COUNT pushdown
seastar::future<> testBoolCountPushdown(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.bool_count");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps;
        std::vector<bool> values;
        for (int i = 0; i < 100; ++i) {
            timestamps.push_back(1000 + i * 100);
            values.push_back(i % 3 == 0);
        }
        writer.writeSeries(TSMValueType::Boolean, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    timestar::BlockAggregator aggregator(0, AggregationMethod::COUNT);
    aggregator.setFoldToSingleState(false);
    size_t pts = co_await tsm.aggregateSeries(seriesId, 0, UINT64_MAX, aggregator);
    EXPECT_EQ(pts, 100u);

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, BoolCountPushdown) {
    seastar::async([&] { testBoolCountPushdown(getTestFilePath("0_5.tsm")).get(); });
}

// ==================== Phase 1: String Block Stats (COUNT only) ====================

seastar::future<> testStringBlockCountRoundTrip(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.str_series");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000};
        std::vector<std::string> values = {"hello", "world", "test"};
        writer.writeSeries(TSMValueType::String, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    if (!entry) co_return;
    EXPECT_EQ(entry->indexBlocks.size(), 1u);
    if (entry->indexBlocks.empty()) co_return;

    // String blocks should have blockCount set but no value stats
    const auto& block = entry->indexBlocks[0];
    EXPECT_EQ(block.blockCount, 3u);
    EXPECT_FALSE(block.hasExtendedStats);

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, StringBlockCountRoundTrip) {
    seastar::async([&] { testStringBlockCountRoundTrip(getTestFilePath("0_6.tsm")).get(); });
}

// String aggregation should return 0 (not supported)
seastar::future<> testStringAggregationReturnsZero(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.str_agg");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000};
        std::vector<std::string> values = {"a", "b", "c"};
        writer.writeSeries(TSMValueType::String, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    timestar::BlockAggregator aggregator(0, AggregationMethod::COUNT);
    aggregator.setFoldToSingleState(false);
    size_t pts = co_await tsm.aggregateSeries(seriesId, 0, UINT64_MAX, aggregator);
    EXPECT_EQ(pts, 0u);  // String aggregation not supported

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, StringAggregationReturnsZero) {
    seastar::async([&] { testStringAggregationReturnsZero(getTestFilePath("0_7.tsm")).get(); });
}

// ==================== Phase 1: Version V2 ====================

TEST_F(TSMUniversalStatsTest, TSMVersionIsV2) {
    EXPECT_EQ(TSM_VERSION, 2u);
    EXPECT_EQ(TSM_VERSION_MIN, 1u);
}

TEST_F(TSMUniversalStatsTest, IndexBlockByteSizes) {
    // V2 sizes
    EXPECT_EQ(indexBlockBytesV2(TSMValueType::Float), 80u);
    EXPECT_EQ(indexBlockBytesV2(TSMValueType::Integer), 72u);
    EXPECT_EQ(indexBlockBytesV2(TSMValueType::Boolean), 40u);
    EXPECT_EQ(indexBlockBytesV2(TSMValueType::String), 32u);

    // V1 backward compat: non-Float is 28 bytes
    EXPECT_EQ(indexBlockBytes(TSMValueType::Float, 1), 80u);
    EXPECT_EQ(indexBlockBytes(TSMValueType::Integer, 1), 28u);
    EXPECT_EQ(indexBlockBytes(TSMValueType::Boolean, 1), 28u);
    EXPECT_EQ(indexBlockBytes(TSMValueType::String, 1), 28u);
}

// ==================== Phase 1: Integer LATEST/FIRST from sparse index ====================

seastar::future<> testIntegerLatestFromSparse(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.int_latest");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000};
        std::vector<int64_t> values = {100, 200, 300, 400, 500};
        writer.writeSeries(TSMValueType::Integer, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    // Zero-I/O LATEST from sparse index
    auto latest = tsm.getLatestFromSparse(seriesId);
    EXPECT_TRUE(latest.has_value());
    if (!latest.has_value()) co_return;
    EXPECT_EQ(latest->timestamp, 5000u);
    EXPECT_DOUBLE_EQ(latest->value, 500.0);

    // Zero-I/O FIRST from sparse index
    auto first = tsm.getFirstFromSparse(seriesId);
    EXPECT_TRUE(first.has_value());
    if (!first.has_value()) co_return;
    EXPECT_EQ(first->timestamp, 1000u);
    EXPECT_DOUBLE_EQ(first->value, 100.0);

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, IntegerLatestFirstFromSparse) {
    seastar::async([&] { testIntegerLatestFromSparse(getTestFilePath("0_8.tsm")).get(); });
}

// ==================== Phase 0: Tombstone Per-Block Overlap ====================

seastar::future<> testTombstonePerBlockOverlap(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.tombstone_overlap");

    // Write 5 blocks worth of data (set max_points_per_block low enough to create multi-block)
    {
        TSMWriter writer(filename);
        // Create enough data to span multiple blocks
        // MaxPointsPerBlock() is typically 3000 — write 15000 points = ~5 blocks
        std::vector<uint64_t> timestamps;
        std::vector<double> values;
        for (int i = 0; i < 15000; ++i) {
            timestamps.push_back(1000 + i * 1000);
            values.push_back(static_cast<double>(i));
        }
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    // Verify we have multiple blocks
    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    if (!entry) co_return;
    EXPECT_GE(entry->indexBlocks.size(), 2u) << "Need multiple blocks for per-block tombstone test";

    // All blocks should have stats
    for (const auto& block : entry->indexBlocks) {
        EXPECT_GT(block.blockCount, 0u);
    }

    // Delete a range that covers only the FIRST block's time range
    const auto& firstBlock = entry->indexBlocks[0];
    co_await tsm.deleteRange(seriesId, firstBlock.minTime, firstBlock.maxTime);

    // Run aggregation — blocks NOT covered by tombstones should still use block stats
    timestar::BlockAggregator aggregator(0, AggregationMethod::SUM);
    aggregator.setFoldToSingleState(false);
    size_t pts = co_await tsm.aggregateSeries(seriesId, 0, UINT64_MAX, aggregator);

    // Should have aggregated all non-tombstoned blocks
    EXPECT_GT(pts, 0u);
    // Should NOT have aggregated the tombstoned first block's points
    EXPECT_LT(pts, 15000u);

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, TombstonePerBlockOverlap) {
    seastar::async([&] { testTombstonePerBlockOverlap(getTestFilePath("0_9.tsm")).get(); });
}

// ==================== Phase 1: Multi-block Integer stats ====================

seastar::future<> testMultiBlockIntegerStats(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.multi_block_int");

    // Write enough integer data to create multiple blocks
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps;
        std::vector<int64_t> values;
        for (int i = 0; i < 10000; ++i) {
            timestamps.push_back(1000 + i * 100);
            values.push_back(i * 10);
        }
        writer.writeSeries(TSMValueType::Integer, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    if (!entry) co_return;

    // All blocks should have integer stats
    for (const auto& block : entry->indexBlocks) {
        EXPECT_GT(block.blockCount, 0u);
        EXPECT_TRUE(block.hasExtendedStats);
    }

    // Aggregate via pushdown — should work for Integer now
    timestar::BlockAggregator aggregator(0, AggregationMethod::COUNT);
    aggregator.setFoldToSingleState(false);
    size_t pts = co_await tsm.aggregateSeries(seriesId, 0, UINT64_MAX, aggregator);
    EXPECT_EQ(pts, 10000u);

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, MultiBlockIntegerStats) {
    seastar::async([&] { testMultiBlockIntegerStats(getTestFilePath("0_10.tsm")).get(); });
}

// ==================== Phase 1: Boolean SUM (trueCount) pushdown ====================

seastar::future<> testBoolSumPushdown(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.bool_sum");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
        std::vector<bool> values = {true, true, false, true, false, true, true, false, true, false};
        writer.writeSeries(TSMValueType::Boolean, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    // SUM of booleans = trueCount
    timestar::BlockAggregator aggregator(0, AggregationMethod::SUM);
    aggregator.setFoldToSingleState(false);
    size_t pts = co_await tsm.aggregateSeries(seriesId, 0, UINT64_MAX, aggregator);
    EXPECT_EQ(pts, 10u);
    auto state = aggregator.takeSingleState();
    EXPECT_DOUBLE_EQ(state.sum, 6.0);  // 6 true values

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, BoolSumPushdown) {
    seastar::async([&] { testBoolSumPushdown(getTestFilePath("0_11.tsm")).get(); });
}

// ==================== Phase 1: Integer aggregateSeriesSelective (LATEST/FIRST) ====================

seastar::future<> testIntegerLatestSelective(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.int_selective");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000};
        std::vector<int64_t> values = {100, 200, 300, 400, 500};
        writer.writeSeries(TSMValueType::Integer, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    // LATEST with maxPoints=1 should use block stats shortcut
    {
        timestar::BlockAggregator aggregator(0, AggregationMethod::LATEST);
        aggregator.setFoldToSingleState(false);
        size_t pts = co_await tsm.aggregateSeriesSelective(seriesId, 0, UINT64_MAX, aggregator, true, 1);
        EXPECT_EQ(pts, 1u);
    }

    // FIRST with maxPoints=1
    {
        timestar::BlockAggregator aggregator(0, AggregationMethod::FIRST);
        aggregator.setFoldToSingleState(false);
        size_t pts = co_await tsm.aggregateSeriesSelective(seriesId, 0, UINT64_MAX, aggregator, false, 1);
        EXPECT_EQ(pts, 1u);
    }

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, IntegerLatestFirstSelective) {
    seastar::async([&] { testIntegerLatestSelective(getTestFilePath("0_12.tsm")).get(); });
}

// ==================== Phase 3: String Dictionary Encoding ====================

// Test low-cardinality strings use dictionary encoding
seastar::future<> testStringDictionaryRoundTrip(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.str_dict");

    // Low-cardinality: 3 unique values repeated many times
    std::vector<uint64_t> timestamps;
    std::vector<std::string> values;
    for (int i = 0; i < 300; ++i) {
        timestamps.push_back(1000 + i * 100);
        if (i % 3 == 0)
            values.push_back("status_ok");
        else if (i % 3 == 1)
            values.push_back("status_warning");
        else
            values.push_back("status_error");
    }

    {
        TSMWriter writer(filename);
        writer.writeSeries(TSMValueType::String, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // Read back and verify all values round-trip correctly
    TSM tsm(filename);
    co_await tsm.open();

    // Check dictionary was stored in the index
    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    if (!entry) co_return;
    EXPECT_FALSE(entry->stringDictionary.empty()) << "Low-cardinality strings should use dictionary";
    EXPECT_EQ(entry->stringDictionary.size(), 3u);

    // Read all data back
    TSMResult<std::string> results(0);
    co_await tsm.readSeries(seriesId, 0, UINT64_MAX, results);
    auto [ts, vals] = results.getAllData();
    EXPECT_EQ(ts.size(), 300u);
    EXPECT_EQ(vals.size(), 300u);

    // Verify values match
    for (size_t i = 0; i < vals.size(); ++i) {
        if (i % 3 == 0)
            EXPECT_EQ(vals[i], "status_ok") << "Mismatch at index " << i;
        else if (i % 3 == 1)
            EXPECT_EQ(vals[i], "status_warning") << "Mismatch at index " << i;
        else
            EXPECT_EQ(vals[i], "status_error") << "Mismatch at index " << i;
    }

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, StringDictionaryRoundTrip) {
    seastar::async([&] { testStringDictionaryRoundTrip(getTestFilePath("0_14.tsm")).get(); });
}

// Test high-cardinality strings fall back to raw encoding
seastar::future<> testStringDictionaryFallback(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.str_high_card");

    // High cardinality: every value is unique
    std::vector<uint64_t> timestamps;
    std::vector<std::string> values;
    for (int i = 0; i < 100; ++i) {
        timestamps.push_back(1000 + i * 100);
        values.push_back("unique_value_" + std::to_string(i) + "_" + std::string(100, 'x'));
    }

    {
        TSMWriter writer(filename);
        writer.writeSeries(TSMValueType::String, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    // Values should still round-trip correctly even without dictionary
    TSMResult<std::string> results(0);
    co_await tsm.readSeries(seriesId, 0, UINT64_MAX, results);
    auto [ts, vals] = results.getAllData();
    EXPECT_EQ(ts.size(), 100u);
    EXPECT_EQ(vals.size(), 100u);

    // Verify first and last values
    if (!vals.empty()) {
        EXPECT_EQ(vals[0], "unique_value_0_" + std::string(100, 'x'));
        EXPECT_EQ(vals[99], "unique_value_99_" + std::string(100, 'x'));
    }

    co_await tsm.close();
}

TEST_F(TSMUniversalStatsTest, StringDictionaryFallback) {
    seastar::async([&] { testStringDictionaryFallback(getTestFilePath("0_15.tsm")).get(); });
}

// Test dictionary encoder standalone
TEST_F(TSMUniversalStatsTest, StringDictionaryBuildValid) {
    std::vector<std::string> values = {"a", "b", "c", "a", "b", "c", "a", "a"};
    auto dict = StringEncoder::buildDictionary(values);
    EXPECT_TRUE(dict.valid);
    EXPECT_EQ(dict.entries.size(), 3u);
}

TEST_F(TSMUniversalStatsTest, StringDictionaryBuildExceedsLimit) {
    // Exceed MAX_DICT_ENTRIES (50)
    std::vector<std::string> values;
    for (int i = 0; i < 51; ++i) {
        values.push_back("str_" + std::to_string(i));
    }
    auto dict = StringEncoder::buildDictionary(values);
    EXPECT_FALSE(dict.valid);
}

TEST_F(TSMUniversalStatsTest, StringDictionarySerializeDeserialize) {
    StringEncoder::Dictionary dict;
    dict.entries = {"hello", "world", "test"};
    dict.valid = true;
    auto serialized = StringEncoder::serializeDictionary(dict);

    Slice slice(serialized.data.data(), serialized.size());
    auto deserialized = StringEncoder::deserializeDictionary(slice, serialized.size());
    EXPECT_TRUE(deserialized.valid);
    EXPECT_EQ(deserialized.entries.size(), 3u);
    EXPECT_EQ(deserialized.entries[0], "hello");
    EXPECT_EQ(deserialized.entries[1], "world");
    EXPECT_EQ(deserialized.entries[2], "test");
}
