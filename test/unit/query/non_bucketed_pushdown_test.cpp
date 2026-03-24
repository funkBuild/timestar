// Tests for non-bucketed streaming pushdown aggregation.
//
// When aggregationInterval == 0 and the method is "streamable" (AVG, MIN, MAX,
// SUM, COUNT, SPREAD, STDDEV, STDVAR), queryTsmAggregated folds all TSM data
// into a single AggregationState instead of materialising raw (timestamp, value)
// vectors.  This avoids O(N) memory for large queries.
//
// These tests verify:
//   1. Correct aggregated values for each streamable method
//   2. MEDIAN correctly falls back (returns nullopt from pushdown)
//   3. Cross-shard merge of collapsed states via mergePartialAggregationsGrouped

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/query/block_aggregator.hpp"
#include "../../../lib/query/query_parser.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <string>

namespace fs = std::filesystem;

class NonBucketedPushdownTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// ---------------------------------------------------------------------------
// Helper: insert N known values into a single series, flush to TSM.
// Values are 10, 20, 30, ..., N*10 at timestamps 1s, 2s, ..., Ns.
// ---------------------------------------------------------------------------
static seastar::future<> insertKnownValues(Engine& engine, int count = 10) {
    TimeStarInsert<double> insert("pushdown_test", "value");
    insert.addTag("host", "h0");
    for (int i = 1; i <= count; i++) {
        insert.addValue(static_cast<uint64_t>(i) * 1000000000ULL, static_cast<double>(i * 10));
    }
    co_await engine.insert(std::move(insert));
    co_await engine.rolloverMemoryStore();
    // Poll for background TSM conversion to complete (timeout after 5s)
    for (int attempt = 0; attempt < 50 && engine.getTSMFileCount() == 0; ++attempt) {
        co_await seastar::sleep(std::chrono::milliseconds(100));
    }
}

// ===========================================================================
// AVG non-bucketed pushdown
// ===========================================================================
SEASTAR_TEST_F(NonBucketedPushdownTest, AVG_ReturnsCorrectAverage) {
    Engine engine;
    co_await engine.init();
    co_await insertKnownValues(engine, 10);

    std::string seriesKey = "pushdown_test,host=h0 value";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    auto result =
        co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::AVG);

    EXPECT_TRUE(result.has_value()) << "AVG pushdown should succeed";
    if (!result.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_TRUE(result->aggregatedState.has_value()) << "Should produce collapsed state";
    if (!result->aggregatedState.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_EQ(result->aggregatedState->count, 10u);
    // AVG of 10,20,...,100 = 55.0
    double avg = result->aggregatedState->getValue(timestar::AggregationMethod::AVG);
    EXPECT_DOUBLE_EQ(avg, 55.0);

    co_await engine.stop();
}

// ===========================================================================
// MIN non-bucketed pushdown
// ===========================================================================
SEASTAR_TEST_F(NonBucketedPushdownTest, MIN_ReturnsCorrectMinimum) {
    Engine engine;
    co_await engine.init();
    co_await insertKnownValues(engine, 10);

    std::string seriesKey = "pushdown_test,host=h0 value";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    auto result =
        co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::MIN);

    EXPECT_TRUE(result.has_value()) << "MIN pushdown should succeed";
    if (!result.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_TRUE(result->aggregatedState.has_value()) << "Should produce collapsed state";
    if (!result->aggregatedState.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_EQ(result->aggregatedState->count, 10u);
    double minVal = result->aggregatedState->getValue(timestar::AggregationMethod::MIN);
    EXPECT_DOUBLE_EQ(minVal, 10.0);

    co_await engine.stop();
}

// ===========================================================================
// MAX non-bucketed pushdown
// ===========================================================================
SEASTAR_TEST_F(NonBucketedPushdownTest, MAX_ReturnsCorrectMaximum) {
    Engine engine;
    co_await engine.init();
    co_await insertKnownValues(engine, 10);

    std::string seriesKey = "pushdown_test,host=h0 value";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    auto result =
        co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::MAX);

    EXPECT_TRUE(result.has_value()) << "MAX pushdown should succeed";
    if (!result.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_TRUE(result->aggregatedState.has_value()) << "Should produce collapsed state";
    if (!result->aggregatedState.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_EQ(result->aggregatedState->count, 10u);
    double maxVal = result->aggregatedState->getValue(timestar::AggregationMethod::MAX);
    EXPECT_DOUBLE_EQ(maxVal, 100.0);

    co_await engine.stop();
}

// ===========================================================================
// COUNT non-bucketed pushdown
// ===========================================================================
SEASTAR_TEST_F(NonBucketedPushdownTest, COUNT_ReturnsCorrectCount) {
    Engine engine;
    co_await engine.init();
    co_await insertKnownValues(engine, 10);

    std::string seriesKey = "pushdown_test,host=h0 value";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    auto result =
        co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::COUNT);

    EXPECT_TRUE(result.has_value()) << "COUNT pushdown should succeed";
    if (!result.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_TRUE(result->aggregatedState.has_value()) << "Should produce collapsed state";
    if (!result->aggregatedState.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_EQ(result->aggregatedState->count, 10u);
    double count = result->aggregatedState->getValue(timestar::AggregationMethod::COUNT);
    EXPECT_DOUBLE_EQ(count, 10.0);

    co_await engine.stop();
}

// ===========================================================================
// SUM non-bucketed pushdown
// ===========================================================================
SEASTAR_TEST_F(NonBucketedPushdownTest, SUM_ReturnsCorrectSum) {
    Engine engine;
    co_await engine.init();
    co_await insertKnownValues(engine, 10);

    std::string seriesKey = "pushdown_test,host=h0 value";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    auto result =
        co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::SUM);

    EXPECT_TRUE(result.has_value()) << "SUM pushdown should succeed";
    if (!result.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_TRUE(result->aggregatedState.has_value()) << "Should produce collapsed state";
    if (!result->aggregatedState.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_EQ(result->aggregatedState->count, 10u);
    // SUM of 10+20+...+100 = 550
    double sum = result->aggregatedState->getValue(timestar::AggregationMethod::SUM);
    EXPECT_DOUBLE_EQ(sum, 550.0);

    co_await engine.stop();
}

// ===========================================================================
// SPREAD non-bucketed pushdown
// ===========================================================================
SEASTAR_TEST_F(NonBucketedPushdownTest, SPREAD_ReturnsCorrectSpread) {
    Engine engine;
    co_await engine.init();
    co_await insertKnownValues(engine, 10);

    std::string seriesKey = "pushdown_test,host=h0 value";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    auto result =
        co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::SPREAD);

    EXPECT_TRUE(result.has_value()) << "SPREAD pushdown should succeed";
    if (!result.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_TRUE(result->aggregatedState.has_value()) << "Should produce collapsed state";
    if (!result->aggregatedState.has_value()) {
        co_await engine.stop();
        co_return;
    }
    // SPREAD = max - min = 100 - 10 = 90
    double spread = result->aggregatedState->getValue(timestar::AggregationMethod::SPREAD);
    EXPECT_DOUBLE_EQ(spread, 90.0);

    co_await engine.stop();
}

// ===========================================================================
// STDDEV non-bucketed pushdown
// ===========================================================================
SEASTAR_TEST_F(NonBucketedPushdownTest, STDDEV_ReturnsCorrectStdDev) {
    Engine engine;
    co_await engine.init();
    co_await insertKnownValues(engine, 10);

    std::string seriesKey = "pushdown_test,host=h0 value";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    auto result =
        co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::STDDEV);

    EXPECT_TRUE(result.has_value()) << "STDDEV pushdown should succeed";
    if (!result.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_TRUE(result->aggregatedState.has_value()) << "Should produce collapsed state";
    if (!result->aggregatedState.has_value()) {
        co_await engine.stop();
        co_return;
    }

    double stddev = result->aggregatedState->getValue(timestar::AggregationMethod::STDDEV);
    // Population stddev of 10,20,...,100:
    // mean=55, variance = sum((x-55)^2)/10 = 825, stddev = sqrt(825)
    double expected = std::sqrt(825.0);
    EXPECT_NEAR(stddev, expected, 1e-6);

    co_await engine.stop();
}

// ===========================================================================
// STDVAR non-bucketed pushdown
// ===========================================================================
SEASTAR_TEST_F(NonBucketedPushdownTest, STDVAR_ReturnsCorrectVariance) {
    Engine engine;
    co_await engine.init();
    co_await insertKnownValues(engine, 10);

    std::string seriesKey = "pushdown_test,host=h0 value";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    auto result =
        co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::STDVAR);

    EXPECT_TRUE(result.has_value()) << "STDVAR pushdown should succeed";
    if (!result.has_value()) {
        co_await engine.stop();
        co_return;
    }
    EXPECT_TRUE(result->aggregatedState.has_value()) << "Should produce collapsed state";
    if (!result->aggregatedState.has_value()) {
        co_await engine.stop();
        co_return;
    }

    double stdvar = result->aggregatedState->getValue(timestar::AggregationMethod::STDVAR);
    // Population variance of 10,20,...,100 = 825.0
    EXPECT_NEAR(stdvar, 825.0, 1e-6);

    co_await engine.stop();
}

// ===========================================================================
// MEDIAN falls back to nullopt (not streamable)
// ===========================================================================
SEASTAR_TEST_F(NonBucketedPushdownTest, MEDIAN_FallsBackToNullopt) {
    Engine engine;
    co_await engine.init();
    co_await insertKnownValues(engine, 10);

    std::string seriesKey = "pushdown_test,host=h0 value";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    auto result =
        co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::MEDIAN);

    EXPECT_FALSE(result.has_value()) << "MEDIAN with interval=0 should return nullopt (not streamable)";

    co_await engine.stop();
}

// ===========================================================================
// BlockAggregator unit test: fold mode accumulates correctly
// ===========================================================================
TEST(BlockAggregatorFoldTest, FoldToSingleState_AccumulatesCorrectly) {
    timestar::BlockAggregator agg(0);
    agg.enableFoldToSingleState();

    // Add points individually
    agg.addPoint(1000, 10.0);
    agg.addPoint(2000, 20.0);
    agg.addPoint(3000, 30.0);

    EXPECT_EQ(agg.pointCount(), 3u);

    auto state = agg.takeSingleState();
    EXPECT_EQ(state.count, 3u);
    EXPECT_DOUBLE_EQ(state.sum, 60.0);
    EXPECT_DOUBLE_EQ(state.min, 10.0);
    EXPECT_DOUBLE_EQ(state.max, 30.0);
    EXPECT_DOUBLE_EQ(state.first, 10.0);
    EXPECT_EQ(state.firstTimestamp, 1000u);
    EXPECT_DOUBLE_EQ(state.latest, 30.0);
    EXPECT_EQ(state.latestTimestamp, 3000u);
}

TEST(BlockAggregatorFoldTest, FoldToSingleState_BatchAdd) {
    timestar::BlockAggregator agg(0);
    agg.enableFoldToSingleState();

    std::vector<uint64_t> ts = {1000, 2000, 3000, 4000, 5000};
    std::vector<double> vals = {10.0, 20.0, 30.0, 40.0, 50.0};

    agg.addPoints(ts, vals);

    EXPECT_EQ(agg.pointCount(), 5u);

    auto state = agg.takeSingleState();
    EXPECT_EQ(state.count, 5u);
    EXPECT_DOUBLE_EQ(state.sum, 150.0);
    EXPECT_DOUBLE_EQ(state.min, 10.0);
    EXPECT_DOUBLE_EQ(state.max, 50.0);
    EXPECT_DOUBLE_EQ(state.getValue(timestar::AggregationMethod::AVG), 30.0);
}

TEST(BlockAggregatorFoldTest, FoldDoesNotCollectRaw) {
    timestar::BlockAggregator agg(0);
    agg.enableFoldToSingleState();

    agg.addPoint(1000, 10.0);
    agg.addPoint(2000, 20.0);
    agg.addPoint(3000, 30.0);

    auto state = agg.takeSingleState();
    EXPECT_EQ(state.count, 3u);
    EXPECT_FALSE(state.collectRaw);
    EXPECT_TRUE(state.rawValues.empty());
}

TEST(BlockAggregatorFoldTest, NonFoldModeStillWorks) {
    // Verify that without setFoldToSingleState, the old behavior is preserved
    timestar::BlockAggregator agg(0);

    agg.addPoint(1000, 10.0);
    agg.addPoint(2000, 20.0);

    EXPECT_EQ(agg.pointCount(), 2u);

    auto ts = agg.takeTimestamps();
    auto vals = agg.takeValues();
    EXPECT_EQ(ts.size(), 2u);
    EXPECT_EQ(vals.size(), 2u);
    EXPECT_EQ(ts[0], 1000u);
    EXPECT_DOUBLE_EQ(vals[0], 10.0);
}

// ===========================================================================
// PushdownResult carries aggregatedState correctly
// ===========================================================================
TEST(PushdownResultTest, AggregatedStateCarriedCorrectly) {
    timestar::PushdownResult result;
    EXPECT_FALSE(result.aggregatedState.has_value());

    timestar::AggregationState state;
    state.addValue(42.0, 1000);
    state.addValue(58.0, 2000);
    result.aggregatedState = std::move(state);
    result.totalPoints = 2;

    EXPECT_TRUE(result.aggregatedState.has_value());
    EXPECT_EQ(result.aggregatedState->count, 2u);
    EXPECT_DOUBLE_EQ(result.aggregatedState->getValue(timestar::AggregationMethod::AVG), 50.0);
}

// ===========================================================================
// Cross-shard merge of collapsed states
// ===========================================================================

// Helper to build a group key with embedded null separator
static std::string makeGroupKey(const std::string& measurement, const std::string& field) {
    std::string key;
    key.reserve(measurement.size() + 1 + field.size());
    key.append(measurement);
    key += '\0';
    key.append(field);
    return key;
}

TEST(CollapsedStateMergeTest, MergeAllCollapsed_AVG) {
    using namespace timestar;

    std::string gk = makeGroupKey("cpu", "usage");
    size_t gkHash = std::hash<std::string>{}(gk);

    PartialAggregationResult p1;
    p1.measurement = "cpu";
    p1.fieldName = "usage";
    p1.groupKey = gk;
    p1.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(10.0, 1000);
        s.addValue(20.0, 2000);
        p1.collapsedState = std::move(s);
    }

    PartialAggregationResult p2;
    p2.measurement = "cpu";
    p2.fieldName = "usage";
    p2.groupKey = gk;
    p2.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(30.0, 3000);
        s.addValue(40.0, 4000);
        p2.collapsedState = std::move(s);
    }

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(p1));
    partials.push_back(std::move(p2));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::AVG);

    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 1u);
    // AVG of 10, 20, 30, 40 = 25.0
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 25.0);
    EXPECT_EQ(grouped[0].points[0].count, 4u);
}

TEST(CollapsedStateMergeTest, MergeAllCollapsed_SUM) {
    using namespace timestar;

    std::string gk = makeGroupKey("m", "f");
    size_t gkHash = std::hash<std::string>{}(gk);

    PartialAggregationResult p1;
    p1.measurement = "m";
    p1.fieldName = "f";
    p1.groupKey = gk;
    p1.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(100.0, 1000);
        p1.collapsedState = std::move(s);
    }

    PartialAggregationResult p2;
    p2.measurement = "m";
    p2.fieldName = "f";
    p2.groupKey = gk;
    p2.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(200.0, 2000);
        s.addValue(300.0, 3000);
        p2.collapsedState = std::move(s);
    }

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(p1));
    partials.push_back(std::move(p2));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::SUM);

    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 1u);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 600.0);
    EXPECT_EQ(grouped[0].points[0].count, 3u);
}

TEST(CollapsedStateMergeTest, MergeAllCollapsed_MIN) {
    using namespace timestar;

    std::string gk = makeGroupKey("m", "f");
    size_t gkHash = std::hash<std::string>{}(gk);

    PartialAggregationResult p1;
    p1.measurement = "m";
    p1.fieldName = "f";
    p1.groupKey = gk;
    p1.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(50.0, 1000);
        s.addValue(70.0, 2000);
        p1.collapsedState = std::move(s);
    }

    PartialAggregationResult p2;
    p2.measurement = "m";
    p2.fieldName = "f";
    p2.groupKey = gk;
    p2.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(30.0, 3000);
        s.addValue(90.0, 4000);
        p2.collapsedState = std::move(s);
    }

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(p1));
    partials.push_back(std::move(p2));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::MIN);

    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 1u);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 30.0);
}

TEST(CollapsedStateMergeTest, MergeAllCollapsed_MAX) {
    using namespace timestar;

    std::string gk = makeGroupKey("m", "f");
    size_t gkHash = std::hash<std::string>{}(gk);

    PartialAggregationResult p1;
    p1.measurement = "m";
    p1.fieldName = "f";
    p1.groupKey = gk;
    p1.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(50.0, 1000);
        s.addValue(70.0, 2000);
        p1.collapsedState = std::move(s);
    }

    PartialAggregationResult p2;
    p2.measurement = "m";
    p2.fieldName = "f";
    p2.groupKey = gk;
    p2.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(30.0, 3000);
        s.addValue(90.0, 4000);
        p2.collapsedState = std::move(s);
    }

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(p1));
    partials.push_back(std::move(p2));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::MAX);

    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 1u);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 90.0);
}

TEST(CollapsedStateMergeTest, MergeAllCollapsed_COUNT) {
    using namespace timestar;

    std::string gk = makeGroupKey("m", "f");
    size_t gkHash = std::hash<std::string>{}(gk);

    PartialAggregationResult p1;
    p1.measurement = "m";
    p1.fieldName = "f";
    p1.groupKey = gk;
    p1.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(1.0, 1000);
        s.addValue(2.0, 2000);
        s.addValue(3.0, 3000);
        p1.collapsedState = std::move(s);
    }

    PartialAggregationResult p2;
    p2.measurement = "m";
    p2.fieldName = "f";
    p2.groupKey = gk;
    p2.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(4.0, 4000);
        s.addValue(5.0, 5000);
        p2.collapsedState = std::move(s);
    }

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(p1));
    partials.push_back(std::move(p2));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::COUNT);

    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 1u);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 5.0);
}

TEST(CollapsedStateMergeTest, MergeAllCollapsed_SPREAD) {
    using namespace timestar;

    std::string gk = makeGroupKey("m", "f");
    size_t gkHash = std::hash<std::string>{}(gk);

    PartialAggregationResult p1;
    p1.measurement = "m";
    p1.fieldName = "f";
    p1.groupKey = gk;
    p1.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(20.0, 1000);
        s.addValue(40.0, 2000);
        p1.collapsedState = std::move(s);
    }

    PartialAggregationResult p2;
    p2.measurement = "m";
    p2.fieldName = "f";
    p2.groupKey = gk;
    p2.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(10.0, 3000);
        s.addValue(80.0, 4000);
        p2.collapsedState = std::move(s);
    }

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(p1));
    partials.push_back(std::move(p2));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::SPREAD);

    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 1u);
    // SPREAD = max - min = 80 - 10 = 70
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 70.0);
}

// ===========================================================================
// Mixed partials: some collapsed, some raw -- should still work
// ===========================================================================
TEST(CollapsedStateMergeTest, MixedCollapsedAndRaw_FallsBack) {
    using namespace timestar;

    std::string gk = makeGroupKey("m", "f");
    size_t gkHash = std::hash<std::string>{}(gk);

    // Partial 1: collapsed state
    PartialAggregationResult p1;
    p1.measurement = "m";
    p1.fieldName = "f";
    p1.groupKey = gk;
    p1.groupKeyHash = gkHash;
    {
        AggregationState s;
        s.addValue(10.0, 1000);
        s.addValue(20.0, 2000);
        p1.collapsedState = std::move(s);
    }

    // Partial 2: raw values (from non-streamable pushdown or fallback)
    PartialAggregationResult p2;
    p2.measurement = "m";
    p2.fieldName = "f";
    p2.groupKey = gk;
    p2.groupKeyHash = gkHash;
    p2.sortedTimestamps = {3000, 4000};
    {
        AggregationState s1;
        s1.addValue(30.0, 3000);
        AggregationState s2;
        s2.addValue(40.0, 4000);
        p2.sortedStates = {std::move(s1), std::move(s2)};
    }

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(p1));
    partials.push_back(std::move(p2));

    // Should handle mixed partials gracefully (converts collapsed to single-element state)
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::SUM);

    ASSERT_EQ(grouped.size(), 1u);
    // The merged result should have points (collapsed converted to a single-element state,
    // then merged with the 2 raw states)
    EXPECT_GE(grouped[0].points.size(), 1u);
}
