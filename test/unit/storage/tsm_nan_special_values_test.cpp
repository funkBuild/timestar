// Regression tests: TSM block stats and decode-fold paths must implement the
// canonical special-value semantics (docs/nan_policy.md, CLAUDE.md "Special
// Float Values") IDENTICALLY to the memory-store / scalar folds, so that
// aggregation results are placement-independent:
//
//   - NaN = missing data. blockCount is the non-NaN count; COUNT/AVG via
//     block-stats pushdown must equal the scalar NaN-skipping fold. Blocks
//     containing NaN carry no extended stats (LATEST/FIRST/STDDEV decode).
//   - The COUNT-only timestamp decode falls back to a full value decode for
//     NaN-carrying Float blocks (detected via blockCount != header count).
//   - ±Inf is valid data: round-trips raw reads exactly, participates
//     arithmetically in aggregates.
//   - -0.0 round-trips raw reads bit-exactly.
//
// Memory-store-side equivalents: test/unit/query/special_values_fold_test.cpp.

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/query/block_aggregator.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_writer.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <filesystem>
#include <iterator>
#include <limits>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>

namespace fs = std::filesystem;
using timestar::AggregationMethod;
using timestar::AggregationState;
using timestar::BlockAggregator;

namespace {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
constexpr double kInf = std::numeric_limits<double>::infinity();

std::vector<uint64_t> makeTimestamps(size_t n) {
    std::vector<uint64_t> ts(n);
    for (size_t i = 0; i < n; ++i) {
        ts[i] = 1000 + i * 1000;
    }
    return ts;
}

// Reference: the memory-store-equivalent scalar fold over the same data.
double scalarFold(AggregationMethod method, const std::vector<double>& values, const std::vector<uint64_t>& timestamps,
                  uint64_t startTime, uint64_t endTime) {
    AggregationState state;
    for (size_t i = 0; i < values.size(); ++i) {
        if (timestamps[i] < startTime || timestamps[i] > endTime)
            continue;
        state.addValueForMethod(values[i], timestamps[i], method);
    }
    return state.getValue(method);
}

// TSM-side fold: write the data to a TSM file, aggregate via the pushdown
// path (fold-to-single-state, exercising block stats where eligible).
seastar::future<double> tsmFold(std::string filename, AggregationMethod method, std::vector<double> values,
                                std::vector<uint64_t> timestamps, uint64_t startTime, uint64_t endTime) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.special_values");
    {
        TSMWriter writer(filename);
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    BlockAggregator aggregator(0, method);
    aggregator.enableFoldToSingleState();
    co_await tsm.aggregateSeries(seriesId, startTime, endTime, aggregator);
    double result = aggregator.takeSingleState().getValue(method);
    co_await tsm.close();
    co_return result;
}

// EXPECT_EQ with NaN == NaN treated as equal (bitwise identity is not
// required — canonical results only distinguish NaN vs non-NaN).
void expectSameAggregate(double tsmValue, double memValue, const char* what) {
    if (std::isnan(memValue)) {
        EXPECT_TRUE(std::isnan(tsmValue)) << what << ": TSM=" << tsmValue << " mem=NaN";
    } else {
        EXPECT_DOUBLE_EQ(tsmValue, memValue) << what;
    }
}

}  // namespace

class TSMNanSpecialValuesTest : public ::testing::Test {
protected:
    std::string testDir = "./test_tsm_nan_special_values";

    void SetUp() override { fs::create_directories(testDir); }
    void TearDown() override { fs::remove_all(testDir); }
    std::string getTestFilePath(const std::string& filename) { return testDir + "/" + filename; }
};

// ==================== Writer block stats with NaN ====================

seastar::future<> testNanBlockStats(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.nan_stats");
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000};
        std::vector<double> values = {1.0, kNaN, 3.0};
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    if (!entry) {
        co_await tsm.close();
        co_return;
    }
    EXPECT_EQ(entry->indexBlocks.size(), 1u);
    if (entry->indexBlocks.empty()) {
        co_await tsm.close();
        co_return;
    }

    const auto& block = entry->indexBlocks[0];
    // Canonical: blockCount is the NON-NaN count (NaN = missing data), so
    // COUNT/AVG stats pushdown matches the scalar NaN-skipping fold.
    EXPECT_EQ(block.blockCount, 2u);
    EXPECT_DOUBLE_EQ(block.blockSum, 4.0);
    EXPECT_DOUBLE_EQ(block.blockMin, 1.0);
    EXPECT_DOUBLE_EQ(block.blockMax, 3.0);
    // NaN-carrying blocks withhold extended stats: endpoint values may be
    // NaN, so LATEST/FIRST/STDDEV must decode and skip per value.
    EXPECT_FALSE(block.hasExtendedStats);

    co_await tsm.close();
}

TEST_F(TSMNanSpecialValuesTest, NanBlockStatsSkipNaN) {
    seastar::async([&] { testNanBlockStats(getTestFilePath("0_1.tsm")).get(); }).get();
}

seastar::future<> testAllNanBlockStats(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.all_nan");
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000};
        std::vector<double> values = {kNaN, kNaN, kNaN, kNaN};
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    if (!entry || entry->indexBlocks.size() != 1u) {
        EXPECT_EQ(entry ? entry->indexBlocks.size() : 0u, 1u);
        co_await tsm.close();
        co_return;
    }
    // All-NaN block: no valid values, stats pushdown disabled.
    EXPECT_EQ(entry->indexBlocks[0].blockCount, 0u);
    EXPECT_FALSE(entry->indexBlocks[0].hasExtendedStats);

    // COUNT over the all-NaN block is 0 (state stays empty).
    BlockAggregator aggregator(0, AggregationMethod::COUNT);
    aggregator.enableFoldToSingleState();
    co_await tsm.aggregateSeries(seriesId, 0, UINT64_MAX, aggregator);
    EXPECT_EQ(aggregator.takeSingleState().count, 0u);

    co_await tsm.close();
}

TEST_F(TSMNanSpecialValuesTest, AllNanBlockHasNoStats) {
    seastar::async([&] { testAllNanBlockStats(getTestFilePath("0_2.tsm")).get(); }).get();
}

// ==================== Placement independence: NaN ====================

// [1, NaN, 3] — the exact repro from the nodejs correctness suite: count and
// avg must be identical whether answered from memory-store folds or from TSM
// block stats.
seastar::future<> testNanPlacementIndependence(std::string dir) {
    const std::vector<double> values = {1.0, kNaN, 3.0};
    const auto timestamps = makeTimestamps(values.size());

    const AggregationMethod methods[] = {AggregationMethod::COUNT, AggregationMethod::AVG, AggregationMethod::SUM,
                                         AggregationMethod::MIN, AggregationMethod::MAX};
    const char* names[] = {"COUNT", "AVG", "SUM", "MIN", "MAX"};
    for (size_t m = 0; m < std::size(methods); ++m) {
        double mem = scalarFold(methods[m], values, timestamps, 0, UINT64_MAX);
        double tsm = co_await tsmFold(dir + "/0_" + std::to_string(m + 10) + ".tsm", methods[m], values, timestamps, 0,
                                      UINT64_MAX);
        expectSameAggregate(tsm, mem, names[m]);
    }

    // Pin the canonical values themselves (NaN skipped everywhere).
    EXPECT_DOUBLE_EQ(scalarFold(AggregationMethod::COUNT, values, timestamps, 0, UINT64_MAX), 2.0);
    EXPECT_DOUBLE_EQ(scalarFold(AggregationMethod::AVG, values, timestamps, 0, UINT64_MAX), 2.0);
    EXPECT_DOUBLE_EQ(scalarFold(AggregationMethod::SUM, values, timestamps, 0, UINT64_MAX), 4.0);
}

TEST_F(TSMNanSpecialValuesTest, NanPlacementIndependence) {
    seastar::async([&] { testNanPlacementIndependence(testDir).get(); }).get();
}

// Larger series (SIMD decode-fold width) with interior NaN, all five methods.
seastar::future<> testNanPlacementIndependenceSimdWidth(std::string dir) {
    const std::vector<double> values = {1.0, kNaN, 3.0, 4.0, kNaN, 6.0, 7.0, 8.0, kNaN, 10.0};
    const auto timestamps = makeTimestamps(values.size());

    const AggregationMethod methods[] = {AggregationMethod::COUNT,  AggregationMethod::AVG,
                                         AggregationMethod::SUM,    AggregationMethod::MIN,
                                         AggregationMethod::MAX,    AggregationMethod::LATEST,
                                         AggregationMethod::FIRST,  AggregationMethod::STDDEV};
    const char* names[] = {"COUNT", "AVG", "SUM", "MIN", "MAX", "LATEST", "FIRST", "STDDEV"};
    for (size_t m = 0; m < std::size(methods); ++m) {
        double mem = scalarFold(methods[m], values, timestamps, 0, UINT64_MAX);
        double tsm = co_await tsmFold(dir + "/0_" + std::to_string(m + 20) + ".tsm", methods[m], values, timestamps, 0,
                                      UINT64_MAX);
        expectSameAggregate(tsm, mem, names[m]);
    }
    EXPECT_DOUBLE_EQ(scalarFold(AggregationMethod::COUNT, values, timestamps, 0, UINT64_MAX), 7.0);
}

TEST_F(TSMNanSpecialValuesTest, NanPlacementIndependenceSimdWidth) {
    seastar::async([&] { testNanPlacementIndependenceSimdWidth(testDir).get(); }).get();
}

// Partial time range: the block is not fully contained in the query, so the
// COUNT-only timestamp decode runs — it must detect the NaN-carrying block
// (blockCount != header count) and decode values to skip NaN.
seastar::future<> testNanCountPartialRange(std::string dir) {
    const std::vector<double> values = {1.0, kNaN, 3.0};
    const auto timestamps = makeTimestamps(values.size());  // 1000, 2000, 3000

    // Range [1001, UINT64_MAX] covers the NaN (2000) and 3.0 (3000).
    double mem = scalarFold(AggregationMethod::COUNT, values, timestamps, 1001, UINT64_MAX);
    double tsm =
        co_await tsmFold(dir + "/0_30.tsm", AggregationMethod::COUNT, values, timestamps, 1001, UINT64_MAX);
    EXPECT_DOUBLE_EQ(mem, 1.0);
    expectSameAggregate(tsm, mem, "COUNT partial range");
}

TEST_F(TSMNanSpecialValuesTest, NanCountPartialRangeSkipsNaN) {
    seastar::async([&] { testNanCountPartialRange(testDir).get(); }).get();
}

// ==================== Placement independence: ±Inf ====================

seastar::future<> testInfinityAggregates(std::string dir) {
    // +Inf participates arithmetically: SUM/AVG propagate, MIN/MAX order,
    // COUNT counts.
    {
        const std::vector<double> values = {kInf, 5.0, 7.0, 9.0, 11.0, 13.0, 15.0, 17.0};
        const auto timestamps = makeTimestamps(values.size());
        const AggregationMethod methods[] = {AggregationMethod::COUNT, AggregationMethod::AVG, AggregationMethod::SUM,
                                             AggregationMethod::MIN, AggregationMethod::MAX};
        const char* names[] = {"COUNT", "AVG", "SUM", "MIN", "MAX"};
        for (size_t m = 0; m < std::size(methods); ++m) {
            double mem = scalarFold(methods[m], values, timestamps, 0, UINT64_MAX);
            double tsm = co_await tsmFold(dir + "/0_" + std::to_string(m + 40) + ".tsm", methods[m], values,
                                          timestamps, 0, UINT64_MAX);
            expectSameAggregate(tsm, mem, names[m]);
        }
        EXPECT_EQ(scalarFold(AggregationMethod::SUM, values, timestamps, 0, UINT64_MAX), kInf);
        EXPECT_EQ(scalarFold(AggregationMethod::MAX, values, timestamps, 0, UINT64_MAX), kInf);
        EXPECT_DOUBLE_EQ(scalarFold(AggregationMethod::COUNT, values, timestamps, 0, UINT64_MAX), 8.0);
    }

    // +Inf + -Inf: the arithmetically canonical SUM is NaN; MIN/MAX still
    // order the infinities; COUNT counts both.
    {
        const std::vector<double> values = {kInf, -kInf, 5.0, 7.0};
        const auto timestamps = makeTimestamps(values.size());
        const AggregationMethod methods[] = {AggregationMethod::COUNT, AggregationMethod::SUM, AggregationMethod::MIN,
                                             AggregationMethod::MAX};
        const char* names[] = {"COUNT", "SUM", "MIN", "MAX"};
        for (size_t m = 0; m < std::size(methods); ++m) {
            double mem = scalarFold(methods[m], values, timestamps, 0, UINT64_MAX);
            double tsm = co_await tsmFold(dir + "/0_" + std::to_string(m + 50) + ".tsm", methods[m], values,
                                          timestamps, 0, UINT64_MAX);
            expectSameAggregate(tsm, mem, names[m]);
        }
        EXPECT_TRUE(std::isnan(scalarFold(AggregationMethod::SUM, values, timestamps, 0, UINT64_MAX)));
        EXPECT_EQ(scalarFold(AggregationMethod::MIN, values, timestamps, 0, UINT64_MAX), -kInf);
        EXPECT_EQ(scalarFold(AggregationMethod::MAX, values, timestamps, 0, UINT64_MAX), kInf);
        EXPECT_DOUBLE_EQ(scalarFold(AggregationMethod::COUNT, values, timestamps, 0, UINT64_MAX), 4.0);
    }
}

TEST_F(TSMNanSpecialValuesTest, InfinityAggregatesPlacementIndependent) {
    seastar::async([&] { testInfinityAggregates(testDir).get(); }).get();
}

// ==================== Raw round-trip: NaN / ±Inf / -0.0 ====================

seastar::future<> testRawRoundTrip(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.raw_roundtrip");
    const std::vector<double> values = {kNaN, kInf, -kInf, -0.0, 7.5};
    const auto timestamps = makeTimestamps(values.size());
    {
        TSMWriter writer(filename);
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    TSMResult<double> results(0);
    co_await tsm.readSeries(seriesId, 0, UINT64_MAX, results);
    auto [ts, vals] = results.getAllData();
    EXPECT_EQ(vals.size(), values.size());
    if (vals.size() != values.size()) {
        co_await tsm.close();
        co_return;
    }
    // NaN in, NaN out for raw reads (never dropped, never turned finite).
    EXPECT_TRUE(std::isnan(vals[0]));
    // Inf in, Inf out — bit-exact.
    EXPECT_EQ(vals[1], kInf);
    EXPECT_EQ(vals[2], -kInf);
    // -0.0 preserves its sign bit through the ALP exception path.
    EXPECT_EQ(vals[3], 0.0);
    EXPECT_TRUE(std::signbit(vals[3]));
    EXPECT_DOUBLE_EQ(vals[4], 7.5);

    co_await tsm.close();
}

TEST_F(TSMNanSpecialValuesTest, RawRoundTripPreservesSpecialValues) {
    seastar::async([&] { testRawRoundTrip(getTestFilePath("0_60.tsm")).get(); }).get();
}

// ==================== Bucketed (interval) placement independence ====================

// The nodejs repro uses aggregationInterval "1h" over [1, NaN, 3]: a single
// epoch bucket. Bucketed stats pushdown must match the scalar fold.
seastar::future<> testNanBucketedPlacementIndependence(std::string dir) {
    const std::vector<double> values = {1.0, kNaN, 3.0};
    const auto timestamps = makeTimestamps(values.size());
    const uint64_t interval = 3'600'000'000'000ULL;  // 1h in ns; all points in bucket 0

    const AggregationMethod methods[] = {AggregationMethod::COUNT, AggregationMethod::AVG};
    const double expected[] = {2.0, 2.0};
    const char* names[] = {"COUNT", "AVG"};
    for (size_t m = 0; m < std::size(methods); ++m) {
        // TSM path
        std::string filename = dir + "/0_" + std::to_string(m + 70) + ".tsm";
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.bucketed_nan");
        {
            TSMWriter writer(filename);
            writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
            writer.writeIndex();
            writer.close();
        }
        TSM tsm(filename);
        co_await tsm.open();
        BlockAggregator tsmAgg(interval, 0, interval - 1, methods[m], true);
        co_await tsm.aggregateSeries(seriesId, 0, interval - 1, tsmAgg);
        auto tsmBuckets = tsmAgg.takeBucketStates();
        co_await tsm.close();

        // Memory-store-equivalent path (per-point fold into the same shape).
        BlockAggregator memAgg(interval, 0, interval - 1, methods[m], true);
        for (size_t i = 0; i < values.size(); ++i) {
            memAgg.addPoint(timestamps[i], values[i]);
        }
        auto memBuckets = memAgg.takeBucketStates();

        EXPECT_EQ(tsmBuckets.size(), 1u) << names[m];
        EXPECT_EQ(memBuckets.size(), 1u) << names[m];
        if (tsmBuckets.size() != 1u || memBuckets.size() != 1u) {
            continue;
        }
        const double tsmVal = tsmBuckets.begin()->second.getValue(methods[m]);
        const double memVal = memBuckets.begin()->second.getValue(methods[m]);
        EXPECT_DOUBLE_EQ(tsmVal, memVal) << names[m];
        EXPECT_DOUBLE_EQ(tsmVal, expected[m]) << names[m];
    }
}

TEST_F(TSMNanSpecialValuesTest, NanBucketedPlacementIndependence) {
    seastar::async([&] { testNanBucketedPlacementIndependence(testDir).get(); }).get();
}
