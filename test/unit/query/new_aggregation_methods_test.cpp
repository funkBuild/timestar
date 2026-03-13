// Tests for the new aggregation methods: count, first, median, stddev, stdvar, spread
// These tests cover:
//  - Parser: each method name parses to the correct AggregationMethod enum value
//  - AggregationState::getValue: correct computation with known data
//  - Edge cases: empty state (NaN), single point, all-same values (spread=0, stddev=0)

#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/query/query_parser.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <numeric>
#include <vector>

using namespace timestar;

// ============================================================================
// Parser tests — each new keyword must parse without throwing
// ============================================================================

TEST(NewAggregationMethodParserTest, ParseCount) {
    QueryRequest req = QueryParser::parseQueryString("count:temperature()");
    EXPECT_EQ(req.aggregation, AggregationMethod::COUNT);
    EXPECT_EQ(req.measurement, "temperature");
}

TEST(NewAggregationMethodParserTest, ParseCountUppercase) {
    QueryRequest req = QueryParser::parseQueryString("COUNT:cpu(usage)");
    EXPECT_EQ(req.aggregation, AggregationMethod::COUNT);
}

TEST(NewAggregationMethodParserTest, ParseFirst) {
    QueryRequest req = QueryParser::parseQueryString("first:sensor(value)");
    EXPECT_EQ(req.aggregation, AggregationMethod::FIRST);
}

TEST(NewAggregationMethodParserTest, ParseFirstMixedCase) {
    QueryRequest req = QueryParser::parseQueryString("First:sensor(value)");
    EXPECT_EQ(req.aggregation, AggregationMethod::FIRST);
}

TEST(NewAggregationMethodParserTest, ParseMedian) {
    QueryRequest req = QueryParser::parseQueryString("median:temperature()");
    EXPECT_EQ(req.aggregation, AggregationMethod::MEDIAN);
}

TEST(NewAggregationMethodParserTest, ParseMedianUppercase) {
    QueryRequest req = QueryParser::parseQueryString("MEDIAN:temperature()");
    EXPECT_EQ(req.aggregation, AggregationMethod::MEDIAN);
}

TEST(NewAggregationMethodParserTest, ParseStddev) {
    QueryRequest req = QueryParser::parseQueryString("stddev:temperature()");
    EXPECT_EQ(req.aggregation, AggregationMethod::STDDEV);
}

TEST(NewAggregationMethodParserTest, ParseStddevUppercase) {
    QueryRequest req = QueryParser::parseQueryString("STDDEV:temperature()");
    EXPECT_EQ(req.aggregation, AggregationMethod::STDDEV);
}

TEST(NewAggregationMethodParserTest, ParseStdvar) {
    QueryRequest req = QueryParser::parseQueryString("stdvar:temperature()");
    EXPECT_EQ(req.aggregation, AggregationMethod::STDVAR);
}

TEST(NewAggregationMethodParserTest, ParseSpread) {
    QueryRequest req = QueryParser::parseQueryString("spread:temperature()");
    EXPECT_EQ(req.aggregation, AggregationMethod::SPREAD);
}

TEST(NewAggregationMethodParserTest, ParseSpreadUppercase) {
    QueryRequest req = QueryParser::parseQueryString("SPREAD:temperature()");
    EXPECT_EQ(req.aggregation, AggregationMethod::SPREAD);
}

// Full query string with scopes and group-by still parses correctly
TEST(NewAggregationMethodParserTest, ParseCountWithScopesAndGroupBy) {
    QueryRequest req = QueryParser::parseQueryString("count:cpu(usage){host:server-01} by {datacenter}");
    EXPECT_EQ(req.aggregation, AggregationMethod::COUNT);
    EXPECT_EQ(req.measurement, "cpu");
    EXPECT_EQ(req.fields.size(), 1u);
    EXPECT_EQ(req.fields[0], "usage");
    EXPECT_EQ(req.scopes.at("host"), "server-01");
    EXPECT_EQ(req.groupByTags.size(), 1u);
    EXPECT_EQ(req.groupByTags[0], "datacenter");
}

TEST(NewAggregationMethodParserTest, InvalidMethodStillThrows) {
    EXPECT_THROW(QueryParser::parseQueryString("percentile:temperature()"), QueryParseException);
}

// ============================================================================
// AggregationState::getValue — empty state returns NaN for all new methods
// ============================================================================

TEST(NewAggregationStateTest, EmptyCount_NaN) {
    AggregationState s;
    EXPECT_TRUE(std::isnan(s.getValue(AggregationMethod::COUNT)));
}

TEST(NewAggregationStateTest, EmptyFirst_NaN) {
    AggregationState s;
    EXPECT_TRUE(std::isnan(s.getValue(AggregationMethod::FIRST)));
}

TEST(NewAggregationStateTest, EmptyMedian_NaN) {
    AggregationState s;
    EXPECT_TRUE(std::isnan(s.getValue(AggregationMethod::MEDIAN)));
}

TEST(NewAggregationStateTest, EmptyStddev_NaN) {
    AggregationState s;
    EXPECT_TRUE(std::isnan(s.getValue(AggregationMethod::STDDEV)));
}

TEST(NewAggregationStateTest, EmptyStdvar_NaN) {
    AggregationState s;
    EXPECT_TRUE(std::isnan(s.getValue(AggregationMethod::STDVAR)));
}

TEST(NewAggregationStateTest, EmptySpread_NaN) {
    AggregationState s;
    EXPECT_TRUE(std::isnan(s.getValue(AggregationMethod::SPREAD)));
}

// ============================================================================
// AggregationState::getValue — single data point
// ============================================================================

TEST(NewAggregationStateTest, SinglePoint_Count) {
    AggregationState s;
    s.addValue(42.0, 100);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::COUNT), 1.0);
}

TEST(NewAggregationStateTest, SinglePoint_First) {
    AggregationState s;
    s.addValue(42.0, 100);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::FIRST), 42.0);
}

TEST(NewAggregationStateTest, SinglePoint_Median) {
    AggregationState s;
    s.collectRaw = true;
    s.addValue(7.0, 100);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::MEDIAN), 7.0);
}

TEST(NewAggregationStateTest, SinglePoint_Stddev_IsZero) {
    AggregationState s;
    s.addValue(5.0, 100);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::STDDEV), 0.0);
}

TEST(NewAggregationStateTest, SinglePoint_Stdvar_IsZero) {
    AggregationState s;
    s.addValue(5.0, 100);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::STDVAR), 0.0);
}

TEST(NewAggregationStateTest, SinglePoint_Spread_IsZero) {
    AggregationState s;
    s.addValue(99.0, 100);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::SPREAD), 0.0);
}

// ============================================================================
// AggregationState::getValue — all-same values
// ============================================================================

TEST(NewAggregationStateTest, AllSame_Count) {
    AggregationState s;
    for (int i = 0; i < 5; ++i)
        s.addValue(3.0, static_cast<uint64_t>(i * 100));
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::COUNT), 5.0);
}

TEST(NewAggregationStateTest, AllSame_Spread_IsZero) {
    AggregationState s;
    for (int i = 0; i < 5; ++i)
        s.addValue(3.0, static_cast<uint64_t>(i * 100));
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::SPREAD), 0.0);
}

TEST(NewAggregationStateTest, AllSame_Stddev_IsZero) {
    AggregationState s;
    for (int i = 0; i < 5; ++i)
        s.addValue(3.0, static_cast<uint64_t>(i * 100));
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::STDDEV), 0.0);
}

TEST(NewAggregationStateTest, AllSame_Stdvar_IsZero) {
    AggregationState s;
    for (int i = 0; i < 5; ++i)
        s.addValue(3.0, static_cast<uint64_t>(i * 100));
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::STDVAR), 0.0);
}

// ============================================================================
// AggregationState::getValue — multi-point correctness
// ============================================================================

TEST(NewAggregationStateTest, Count_MultiplePoints) {
    AggregationState s;
    // Values: 1, 3, 5, 7, 9
    for (int i = 1; i <= 5; ++i) {
        s.addValue(static_cast<double>(2 * i - 1), static_cast<uint64_t>(i * 1000));
    }
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::COUNT), 5.0);
}

TEST(NewAggregationStateTest, First_EarliestTimestamp) {
    AggregationState s;
    // Add in timestamp order: first timestamp = 1000, first value = 10.0
    s.addValue(10.0, 1000);
    s.addValue(20.0, 2000);
    s.addValue(30.0, 3000);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::FIRST), 10.0);
}

TEST(NewAggregationStateTest, First_AddedOutOfOrder) {
    AggregationState s;
    // Add in reverse timestamp order — FIRST should still return the value at earliest ts
    s.addValue(30.0, 3000);
    s.addValue(10.0, 1000);
    s.addValue(20.0, 2000);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::FIRST), 10.0);
}

TEST(NewAggregationStateTest, Median_OddCount) {
    AggregationState s;
    s.collectRaw = true;
    // Values: 1, 3, 5, 7, 9 — median is 5
    for (int i = 1; i <= 5; ++i) {
        s.addValue(static_cast<double>(2 * i - 1), static_cast<uint64_t>(i * 1000));
    }
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::MEDIAN), 5.0);
}

TEST(NewAggregationStateTest, Median_EvenCount) {
    AggregationState s;
    s.collectRaw = true;
    // Values: 1, 2, 3, 4 — median is (2+3)/2 = 2.5
    s.addValue(1.0, 1000);
    s.addValue(2.0, 2000);
    s.addValue(3.0, 3000);
    s.addValue(4.0, 4000);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::MEDIAN), 2.5);
}

TEST(NewAggregationStateTest, Median_UnsortedInput) {
    AggregationState s;
    s.collectRaw = true;
    // Added in jumbled order — sort must happen inside getValue
    s.addValue(5.0, 5000);
    s.addValue(1.0, 1000);
    s.addValue(3.0, 3000);
    // Sorted: 1, 3, 5 — median is 3
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::MEDIAN), 3.0);
}

TEST(NewAggregationStateTest, Stddev_KnownValues) {
    AggregationState s;
    // Population: 2, 4, 4, 4, 5, 5, 7, 9
    // mean = 5.0, variance = 4.0, stddev = 2.0
    for (double v : {2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0}) {
        s.addValue(v, 0);
    }
    EXPECT_NEAR(s.getValue(AggregationMethod::STDDEV), 2.0, 1e-10);
}

TEST(NewAggregationStateTest, Stdvar_KnownValues) {
    AggregationState s;
    // Same population: 2, 4, 4, 4, 5, 5, 7, 9
    // variance = 4.0
    for (double v : {2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0}) {
        s.addValue(v, 0);
    }
    EXPECT_NEAR(s.getValue(AggregationMethod::STDVAR), 4.0, 1e-10);
}

TEST(NewAggregationStateTest, Stddev_TwoValues) {
    AggregationState s;
    // Values: 0, 10 — mean = 5, variance = (25 + 25) / 2 = 25, stddev = 5
    s.addValue(0.0, 1000);
    s.addValue(10.0, 2000);
    EXPECT_NEAR(s.getValue(AggregationMethod::STDDEV), 5.0, 1e-10);
}

TEST(NewAggregationStateTest, Stdvar_TwoValues) {
    AggregationState s;
    // Values: 0, 10 — variance = 25
    s.addValue(0.0, 1000);
    s.addValue(10.0, 2000);
    EXPECT_NEAR(s.getValue(AggregationMethod::STDVAR), 25.0, 1e-10);
}

TEST(NewAggregationStateTest, Spread_KnownValues) {
    AggregationState s;
    s.addValue(3.0, 1000);
    s.addValue(10.0, 2000);
    s.addValue(7.0, 3000);
    s.addValue(-2.0, 4000);
    // spread = max(10) - min(-2) = 12
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::SPREAD), 12.0);
}

TEST(NewAggregationStateTest, Spread_AllPositive) {
    AggregationState s;
    s.addValue(1.0, 1000);
    s.addValue(100.0, 2000);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::SPREAD), 99.0);
}

// ============================================================================
// Existing methods still work after adding new fields to AggregationState
// ============================================================================

TEST(NewAggregationStateTest, ExistingMethodsUnaffected) {
    AggregationState s;
    s.addValue(10.0, 100);
    s.addValue(20.0, 200);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::AVG), 15.0);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::SUM), 30.0);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::MIN), 10.0);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::MAX), 20.0);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::LATEST), 20.0);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::FIRST), 10.0);
}

// ============================================================================
// AggregationState::merge — new fields merge correctly
// ============================================================================

TEST(NewAggregationStateTest, MergeCount) {
    AggregationState a, b;
    a.addValue(1.0, 100);
    a.addValue(2.0, 200);
    b.addValue(3.0, 300);
    a.merge(b);
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::COUNT), 3.0);
}

TEST(NewAggregationStateTest, MergeFirst_TakesEarliest) {
    AggregationState a, b;
    // 'a' has earlier timestamps
    a.addValue(5.0, 500);
    b.addValue(1.0, 100);
    b.addValue(3.0, 300);
    a.merge(b);
    // Earliest is b's value at ts=100
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::FIRST), 1.0);
}

TEST(NewAggregationStateTest, MergeSpread) {
    AggregationState a, b;
    a.addValue(5.0, 100);
    a.addValue(8.0, 200);
    b.addValue(2.0, 300);
    b.addValue(11.0, 400);
    a.merge(b);
    // After merge: min=2, max=11, spread=9
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::SPREAD), 9.0);
}

TEST(NewAggregationStateTest, MergeMedian) {
    AggregationState a, b;
    a.collectRaw = true;
    b.collectRaw = true;
    a.addValue(1.0, 100);
    a.addValue(3.0, 200);
    b.addValue(2.0, 300);
    a.merge(b);
    // Merged raw values: {1, 3, 2} -> sorted {1, 2, 3} -> median = 2
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::MEDIAN), 2.0);
}

TEST(NewAggregationStateTest, MergeStddev) {
    AggregationState a, b;
    // Build same dataset as KnownValues test across two states
    // a: 2, 4, 4, 4
    for (double v : {2.0, 4.0, 4.0, 4.0}) {
        a.addValue(v, 0);
    }
    // b: 5, 5, 7, 9
    for (double v : {5.0, 5.0, 7.0, 9.0}) {
        b.addValue(v, 0);
    }
    a.merge(b);
    // Full population: 2, 4, 4, 4, 5, 5, 7, 9 — stddev = 2.0
    EXPECT_NEAR(a.getValue(AggregationMethod::STDDEV), 2.0, 1e-10);
}

// ============================================================================
// Full-pipeline integration: Aggregator::createPartialAggregations /
// mergePartialAggregationsGrouped with new methods
// ============================================================================

class NewAggMethodPipelineTest : public ::testing::Test {
protected:
    // Create a SeriesResult with double values
    static SeriesResult makeSeries(const std::string& measurement, const std::string& field,
                                   const std::vector<uint64_t>& timestamps, const std::vector<double>& values) {
        SeriesResult sr;
        sr.measurement = measurement;
        sr.fields[field] = std::make_pair(timestamps, FieldValues(values));
        return sr;
    }

    // Run the full aggregation pipeline for a single series, no buckets
    static std::vector<AggregatedPoint> runPipeline(const std::vector<uint64_t>& timestamps,
                                                    const std::vector<double>& values, AggregationMethod method) {
        auto series = makeSeries("test", "value", timestamps, values);
        auto partials = Aggregator::createPartialAggregations({series}, method, 0, {});
        auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, method);
        if (grouped.empty())
            return {};
        return {grouped[0].points.begin(), grouped[0].points.end()};
    }
};

TEST_F(NewAggMethodPipelineTest, Count_NoBuckets) {
    // 5 timestamps, each with its own slot — per-timestamp count should be 1
    std::vector<uint64_t> ts = {1000, 2000, 3000, 4000, 5000};
    std::vector<double> vals = {10.0, 20.0, 30.0, 40.0, 50.0};
    auto result = runPipeline(ts, vals, AggregationMethod::COUNT);
    ASSERT_EQ(result.size(), 5u);
    for (const auto& pt : result) {
        EXPECT_DOUBLE_EQ(pt.value, 1.0);  // one value per unique timestamp
    }
}

TEST_F(NewAggMethodPipelineTest, Count_WithBuckets) {
    // 6 points in 3-point buckets of 3000ns each
    uint64_t interval = 3000;
    std::vector<uint64_t> ts = {0, 1000, 2000, 3000, 4000, 5000};
    std::vector<double> vals = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
    auto series = makeSeries("test", "value", ts, vals);
    auto partials = Aggregator::createPartialAggregations({series}, AggregationMethod::COUNT, interval, {});
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::COUNT);
    ASSERT_EQ(grouped.size(), 1u);
    // Two buckets: [0, 3000) with 3 points, [3000, 6000) with 3 points
    ASSERT_EQ(grouped[0].points.size(), 2u);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 3.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[1].value, 3.0);
}

TEST_F(NewAggMethodPipelineTest, First_ReturnsEarliestValue) {
    // Timestamps in sorted order — first should be the value at ts=1000
    std::vector<uint64_t> ts = {1000, 2000, 3000};
    std::vector<double> vals = {100.0, 200.0, 300.0};
    auto result = runPipeline(ts, vals, AggregationMethod::FIRST);
    // Non-bucketed: one per timestamp; each has count=1 so first == that value
    ASSERT_EQ(result.size(), 3u);
    EXPECT_DOUBLE_EQ(result[0].value, 100.0);
}

TEST_F(NewAggMethodPipelineTest, First_WithBuckets) {
    // 4 points in 2-point buckets; first of each bucket is the earlier value
    uint64_t interval = 2000;
    std::vector<uint64_t> ts = {0, 1000, 2000, 3000};
    std::vector<double> vals = {5.0, 9.0, 3.0, 7.0};
    auto series = makeSeries("test", "value", ts, vals);
    auto partials = Aggregator::createPartialAggregations({series}, AggregationMethod::FIRST, interval, {});
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::FIRST);
    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 2u);
    // Bucket [0, 2000): values 5.0 @0, 9.0 @1000 — first is 5.0
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 5.0);
    // Bucket [2000, 4000): values 3.0 @2000, 7.0 @3000 — first is 3.0
    EXPECT_DOUBLE_EQ(grouped[0].points[1].value, 3.0);
}

TEST_F(NewAggMethodPipelineTest, Median_WithBuckets) {
    // 6 values in two 3-value buckets
    uint64_t interval = 3000;
    std::vector<uint64_t> ts = {0, 1000, 2000, 3000, 4000, 5000};
    // Bucket 1: 3, 1, 2 -> sorted: 1, 2, 3 -> median = 2
    // Bucket 2: 9, 7, 8 -> sorted: 7, 8, 9 -> median = 8
    std::vector<double> vals = {3.0, 1.0, 2.0, 9.0, 7.0, 8.0};
    auto series = makeSeries("test", "value", ts, vals);
    auto partials = Aggregator::createPartialAggregations({series}, AggregationMethod::MEDIAN, interval, {});
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::MEDIAN);
    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 2u);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 2.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[1].value, 8.0);
}

TEST_F(NewAggMethodPipelineTest, Stddev_WithBuckets) {
    // Two buckets of values with known stddevs
    uint64_t interval = 2000;
    // Bucket [0, 2000): 0.0, 10.0 -> mean=5, stddev=5
    // Bucket [2000, 4000): 2.0, 2.0 -> mean=2, stddev=0
    std::vector<uint64_t> ts = {0, 1000, 2000, 3000};
    std::vector<double> vals = {0.0, 10.0, 2.0, 2.0};
    auto series = makeSeries("test", "value", ts, vals);
    auto partials = Aggregator::createPartialAggregations({series}, AggregationMethod::STDDEV, interval, {});
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::STDDEV);
    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 2u);
    EXPECT_NEAR(grouped[0].points[0].value, 5.0, 1e-10);
    EXPECT_NEAR(grouped[0].points[1].value, 0.0, 1e-10);
}

TEST_F(NewAggMethodPipelineTest, Stdvar_WithBuckets) {
    // Same data as stddev test — variance = stddev^2
    uint64_t interval = 2000;
    std::vector<uint64_t> ts = {0, 1000, 2000, 3000};
    std::vector<double> vals = {0.0, 10.0, 2.0, 2.0};
    auto series = makeSeries("test", "value", ts, vals);
    auto partials = Aggregator::createPartialAggregations({series}, AggregationMethod::STDVAR, interval, {});
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::STDVAR);
    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 2u);
    EXPECT_NEAR(grouped[0].points[0].value, 25.0, 1e-10);  // variance of {0, 10}
    EXPECT_NEAR(grouped[0].points[1].value, 0.0, 1e-10);   // variance of {2, 2}
}

TEST_F(NewAggMethodPipelineTest, Spread_WithBuckets) {
    // Two 3-value buckets
    uint64_t interval = 3000;
    std::vector<uint64_t> ts = {0, 1000, 2000, 3000, 4000, 5000};
    // Bucket 1: 3, 10, 1 -> spread = 10 - 1 = 9
    // Bucket 2: 5, 5, 5  -> spread = 0
    std::vector<double> vals = {3.0, 10.0, 1.0, 5.0, 5.0, 5.0};
    auto series = makeSeries("test", "value", ts, vals);
    auto partials = Aggregator::createPartialAggregations({series}, AggregationMethod::SPREAD, interval, {});
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::SPREAD);
    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 2u);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 9.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[1].value, 0.0);
}
