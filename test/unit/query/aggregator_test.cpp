#include <gtest/gtest.h>
#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/http/http_query_handler.hpp"  // For SeriesResult
#include <vector>
#include <cmath>

using namespace tsdb;

// Helper class for testing - converts simple data to distributed format
class AggregatorTestHelper {
public:
    static SeriesResult createSeries(
        const std::string& measurement,
        const std::map<std::string, std::string>& tags,
        const std::string& fieldName,
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values) {

        SeriesResult sr;
        sr.measurement = measurement;
        sr.tags = tags;
        sr.fields[fieldName] = std::make_pair(timestamps, FieldValues(values));
        return sr;
    }

    static std::vector<AggregatedPoint> aggregateSingleSeries(
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values,
        AggregationMethod method,
        uint64_t interval = 0) {

        // Create a simple series
        auto series = createSeries("test", {}, "value", timestamps, values);

        // Create partial aggregations
        auto partials = Aggregator::createPartialAggregations({series}, method, interval, {});

        // Merge partials
        auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, method);

        // Extract results
        std::vector<AggregatedPoint> result;
        if (!grouped.empty()) {
            for (const auto& point : grouped[0].points) {
                result.push_back(point);
            }
        }

        return result;
    }
};

class AggregatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create sample data
        for (int i = 0; i < 10; ++i) {
            timestamps.push_back(1000000000 + i * 60000000000ULL); // 1 minute intervals
            values.push_back(10.0 + i * 2.0); // Values: 10, 12, 14, 16, 18, 20, 22, 24, 26, 28
        }
    }

    std::vector<uint64_t> timestamps;
    std::vector<double> values;
};

TEST_F(AggregatorTest, AverageAggregation) {
    auto result = AggregatorTestHelper::aggregateSingleSeries(timestamps, values, AggregationMethod::AVG, 0);

    // With no interval, aggregates by timestamp - should have 10 points
    ASSERT_EQ(result.size(), 10);
    // Each point should have its original value (one value per timestamp)
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result[i].value, values[i]);
    }
}

TEST_F(AggregatorTest, MinAggregation) {
    auto result = AggregatorTestHelper::aggregateSingleSeries(timestamps, values, AggregationMethod::MIN, 0);

    ASSERT_EQ(result.size(), 10);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result[i].value, values[i]);
    }
}

TEST_F(AggregatorTest, MaxAggregation) {
    auto result = AggregatorTestHelper::aggregateSingleSeries(timestamps, values, AggregationMethod::MAX, 0);

    ASSERT_EQ(result.size(), 10);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result[i].value, values[i]);
    }
}

TEST_F(AggregatorTest, SumAggregation) {
    auto result = AggregatorTestHelper::aggregateSingleSeries(timestamps, values, AggregationMethod::SUM, 0);

    ASSERT_EQ(result.size(), 10);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result[i].value, values[i]);
    }
}

TEST_F(AggregatorTest, LatestAggregation) {
    auto result = AggregatorTestHelper::aggregateSingleSeries(timestamps, values, AggregationMethod::LATEST, 0);

    ASSERT_EQ(result.size(), 10);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result[i].value, values[i]);
    }
}

TEST_F(AggregatorTest, TimeBasedBucketing) {
    // Aggregate into 5-minute buckets
    uint64_t interval = 5 * 60 * 1000000000ULL; // 5 minutes in nanoseconds
    auto result = AggregatorTestHelper::aggregateSingleSeries(timestamps, values, AggregationMethod::AVG, interval);

    ASSERT_EQ(result.size(), 2); // Should have 2 buckets (10 points, 1-min intervals, 5-min buckets)

    // First bucket: points 0-4 (values 10, 12, 14, 16, 18)
    EXPECT_DOUBLE_EQ(result[0].value, 14.0); // Average of first 5 points
    EXPECT_EQ(result[0].count, 5);

    // Second bucket: points 5-9 (values 20, 22, 24, 26, 28)
    EXPECT_DOUBLE_EQ(result[1].value, 24.0); // Average of last 5 points
    EXPECT_EQ(result[1].count, 5);
}

TEST_F(AggregatorTest, EmptyData) {
    std::vector<uint64_t> emptyTimestamps;
    std::vector<double> emptyValues;

    auto result = AggregatorTestHelper::aggregateSingleSeries(emptyTimestamps, emptyValues, AggregationMethod::AVG, 0);

    EXPECT_EQ(result.size(), 0);
}

TEST_F(AggregatorTest, SinglePoint) {
    std::vector<uint64_t> singleTimestamp = {1000000000};
    std::vector<double> singleValue = {42.0};

    auto result = AggregatorTestHelper::aggregateSingleSeries(singleTimestamp, singleValue, AggregationMethod::AVG, 0);

    ASSERT_EQ(result.size(), 1);
    EXPECT_DOUBLE_EQ(result[0].value, 42.0);
    EXPECT_EQ(result[0].timestamp, 1000000000);
    EXPECT_EQ(result[0].count, 1);
}

TEST_F(AggregatorTest, AggregateMultipleSeries) {
    // Create series with overlapping timestamps
    auto series1 = AggregatorTestHelper::createSeries("test", {}, "value",
        {1000000000, 2000000000, 3000000000},
        {10.0, 20.0, 30.0});

    auto series2 = AggregatorTestHelper::createSeries("test", {}, "value",
        {2000000000, 3000000000, 4000000000},
        {15.0, 25.0, 35.0});

    auto partials = Aggregator::createPartialAggregations({series1, series2}, AggregationMethod::AVG, 0, {});
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::AVG);

    ASSERT_EQ(grouped.size(), 1);
    auto& points = grouped[0].points;

    ASSERT_EQ(points.size(), 4);
    // timestamp 1000000000: 10.0 (only from series 1)
    EXPECT_EQ(points[0].timestamp, 1000000000);
    EXPECT_DOUBLE_EQ(points[0].value, 10.0);
    // timestamp 2000000000: (20.0+15.0)/2 = 17.5 (both series)
    EXPECT_EQ(points[1].timestamp, 2000000000);
    EXPECT_DOUBLE_EQ(points[1].value, 17.5);
    // timestamp 3000000000: (30.0+25.0)/2 = 27.5 (both series)
    EXPECT_EQ(points[2].timestamp, 3000000000);
    EXPECT_DOUBLE_EQ(points[2].value, 27.5);
    // timestamp 4000000000: 35.0 (only from series 2)
    EXPECT_EQ(points[3].timestamp, 4000000000);
    EXPECT_DOUBLE_EQ(points[3].value, 35.0);
}

TEST_F(AggregatorTest, GroupByAggregation) {
    // Create series with different tags
    auto westSeries = AggregatorTestHelper::createSeries("test", {{"region", "us-west"}}, "value", timestamps, values);

    std::vector<uint64_t> eastTimestamps;
    std::vector<double> eastValues;
    for (int i = 0; i < 10; ++i) {
        eastTimestamps.push_back(1000000000 + i * 60000000000ULL);
        eastValues.push_back(20.0 + i * 3.0); // Different values
    }
    auto eastSeries = AggregatorTestHelper::createSeries("test", {{"region", "us-east"}}, "value", eastTimestamps, eastValues);

    // Group by region
    auto partials = Aggregator::createPartialAggregations({westSeries, eastSeries}, AggregationMethod::AVG, 0, {"region"});
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::AVG);

    ASSERT_EQ(grouped.size(), 2); // Two groups

    // Check both groups exist
    bool hasWest = false, hasEast = false;
    for (const auto& g : grouped) {
        if (g.tags.at("region") == "us-west") {
            hasWest = true;
            EXPECT_EQ(g.points.size(), 10);
            EXPECT_DOUBLE_EQ(g.points[0].value, 10.0);
        } else if (g.tags.at("region") == "us-east") {
            hasEast = true;
            EXPECT_EQ(g.points.size(), 10);
            EXPECT_DOUBLE_EQ(g.points[0].value, 20.0);
        }
    }
    EXPECT_TRUE(hasWest && hasEast);
}

TEST_F(AggregatorTest, TimeIntervalWithMax) {
    // Test MAX aggregation with time intervals
    uint64_t interval = 3 * 60 * 1000000000ULL; // 3 minutes in nanoseconds
    auto result = AggregatorTestHelper::aggregateSingleSeries(timestamps, values, AggregationMethod::MAX, interval);

    ASSERT_EQ(result.size(), 4);

    EXPECT_DOUBLE_EQ(result[0].value, 14.0);
    EXPECT_EQ(result[0].count, 3);

    EXPECT_DOUBLE_EQ(result[1].value, 20.0);
    EXPECT_EQ(result[1].count, 3);

    EXPECT_DOUBLE_EQ(result[2].value, 26.0);
    EXPECT_EQ(result[2].count, 3);

    EXPECT_DOUBLE_EQ(result[3].value, 28.0);
    EXPECT_EQ(result[3].count, 1);
}

TEST_F(AggregatorTest, InvalidInput) {
    // Mismatched sizes - distributed aggregation handles this gracefully
    // Empty timestamps with values creates a group with no points
    auto series = AggregatorTestHelper::createSeries("test", {}, "value", {}, {10.0, 20.0, 30.0});
    auto partials = Aggregator::createPartialAggregations({series}, AggregationMethod::AVG, 0, {});
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::AVG);

    // Distributed aggregation creates a group even with no valid points
    ASSERT_EQ(grouped.size(), 1);
    EXPECT_EQ(grouped[0].points.size(), 0);
}

TEST_F(AggregatorTest, TimeIntervalWithSum) {
    uint64_t interval = 5 * 60 * 1000000000ULL; // 5 minutes in nanoseconds
    auto result = AggregatorTestHelper::aggregateSingleSeries(timestamps, values, AggregationMethod::SUM, interval);

    ASSERT_EQ(result.size(), 2);

    // First bucket: sum of values 10, 12, 14, 16, 18 = 70
    EXPECT_DOUBLE_EQ(result[0].value, 70.0);
    EXPECT_EQ(result[0].count, 5);

    // Second bucket: sum of values 20, 22, 24, 26, 28 = 120
    EXPECT_DOUBLE_EQ(result[1].value, 120.0);
    EXPECT_EQ(result[1].count, 5);
}

TEST_F(AggregatorTest, TimeIntervalWithLatest) {
    uint64_t interval = 4 * 60 * 1000000000ULL; // 4 minutes in nanoseconds
    auto result = AggregatorTestHelper::aggregateSingleSeries(timestamps, values, AggregationMethod::LATEST, interval);

    ASSERT_EQ(result.size(), 3);

    // First bucket: points 0-3, latest is point 3 (value 16)
    EXPECT_DOUBLE_EQ(result[0].value, 16.0);
    EXPECT_EQ(result[0].count, 4);

    // Second bucket: points 4-7, latest is point 7 (value 24)
    EXPECT_DOUBLE_EQ(result[1].value, 24.0);
    EXPECT_EQ(result[1].count, 4);

    // Third bucket: points 8-9, latest is point 9 (value 28)
    EXPECT_DOUBLE_EQ(result[2].value, 28.0);
    EXPECT_EQ(result[2].count, 2);
}

TEST_F(AggregatorTest, VerySmallInterval) {
    uint64_t interval = 30000000000ULL; // 30 seconds (data is at 1-minute intervals)
    auto result = AggregatorTestHelper::aggregateSingleSeries(timestamps, values, AggregationMethod::AVG, interval);

    // Each point should be in its own bucket since interval < data spacing
    EXPECT_EQ(result.size(), 10);

    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result[i].value, values[i]);
        EXPECT_EQ(result[i].count, 1);
    }
}

TEST_F(AggregatorTest, VeryLargeInterval) {
    uint64_t interval = 20 * 60 * 1000000000ULL; // 20 minutes (data spans 9 minutes)
    auto result = AggregatorTestHelper::aggregateSingleSeries(timestamps, values, AggregationMethod::AVG, interval);

    // Should have just one bucket containing all points
    ASSERT_EQ(result.size(), 1);
    EXPECT_DOUBLE_EQ(result[0].value, 19.0); // Average of all values
    EXPECT_EQ(result[0].count, 10);
}

TEST_F(AggregatorTest, IrregularTimestamps) {
    std::vector<uint64_t> irregularTimestamps = {
        1000000000,           // t=0 nanoseconds
        1030000000000ULL,     // t=30 seconds
        1045000000000ULL,     // t=45 seconds
        1120000000000ULL,     // t=2 minutes
        1180000000000ULL,     // t=3 minutes
        1300000000000ULL      // t=5 minutes
    };
    std::vector<double> irregularValues = {10.0, 15.0, 20.0, 25.0, 30.0, 35.0};

    uint64_t interval = 2 * 60 * 1000000000ULL; // 2 minutes in nanoseconds
    auto result = AggregatorTestHelper::aggregateSingleSeries(irregularTimestamps, irregularValues,
                                       AggregationMethod::AVG, interval);

    // Just verify all points are accounted for
    size_t totalPoints = 0;
    for (const auto& point : result) {
        totalPoints += point.count;
    }
    EXPECT_EQ(totalPoints, 6); // All 6 points should be accounted for

    // Verify we have at least 3 buckets (minimum expected)
    EXPECT_GE(result.size(), 3);
}

// ============================================================================
// AggregationState::getValue() - Empty set returns NaN for all methods
// ============================================================================

TEST(AggregationStateTest, EmptySetReturnsNaN_AVG) {
    AggregationState state;  // count = 0
    EXPECT_TRUE(std::isnan(state.getValue(AggregationMethod::AVG)));
}

TEST(AggregationStateTest, EmptySetReturnsNaN_SUM) {
    AggregationState state;
    EXPECT_TRUE(std::isnan(state.getValue(AggregationMethod::SUM)));
}

TEST(AggregationStateTest, EmptySetReturnsNaN_LATEST) {
    AggregationState state;
    EXPECT_TRUE(std::isnan(state.getValue(AggregationMethod::LATEST)));
}

TEST(AggregationStateTest, EmptySetReturnsNaN_MIN) {
    AggregationState state;
    EXPECT_TRUE(std::isnan(state.getValue(AggregationMethod::MIN)));
}

TEST(AggregationStateTest, EmptySetReturnsNaN_MAX) {
    AggregationState state;
    EXPECT_TRUE(std::isnan(state.getValue(AggregationMethod::MAX)));
}

// Non-empty sets should still return correct values
TEST(AggregationStateTest, NonEmptySetReturnsCorrectValues) {
    AggregationState state;
    state.addValue(10.0, 100);
    state.addValue(20.0, 200);
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::AVG), 15.0);
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::SUM), 30.0);
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::MIN), 10.0);
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::MAX), 20.0);
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::LATEST), 20.0);
}

// A single value of 0.0 must be distinguishable from empty (not NaN)
TEST(AggregationStateTest, SingleZeroNotNaN) {
    AggregationState state;
    state.addValue(0.0, 100);
    EXPECT_FALSE(std::isnan(state.getValue(AggregationMethod::SUM)));
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::SUM), 0.0);
    EXPECT_FALSE(std::isnan(state.getValue(AggregationMethod::LATEST)));
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::LATEST), 0.0);
    EXPECT_FALSE(std::isnan(state.getValue(AggregationMethod::AVG)));
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::AVG), 0.0);
    EXPECT_FALSE(std::isnan(state.getValue(AggregationMethod::MIN)));
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::MIN), 0.0);
    EXPECT_FALSE(std::isnan(state.getValue(AggregationMethod::MAX)));
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::MAX), 0.0);
}

// ============================================================================
// Aggregator::calculateSum/Avg - Empty vector returns NaN
// ============================================================================

TEST(AggregationStateTest, CalculateSumEmptyReturnsNaN) {
    std::vector<double> empty;
    EXPECT_TRUE(std::isnan(Aggregator::calculateSum(empty)));
}

TEST(AggregationStateTest, CalculateAvgEmptyReturnsNaN) {
    std::vector<double> empty;
    EXPECT_TRUE(std::isnan(Aggregator::calculateAvg(empty)));
}

TEST(AggregationStateTest, CalculateMinEmptyReturnsNaN) {
    std::vector<double> empty;
    EXPECT_TRUE(std::isnan(Aggregator::calculateMin(empty)));
}

TEST(AggregationStateTest, CalculateMaxEmptyReturnsNaN) {
    std::vector<double> empty;
    EXPECT_TRUE(std::isnan(Aggregator::calculateMax(empty)));
}
