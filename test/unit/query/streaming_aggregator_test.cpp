#include "streaming_aggregator.hpp"

#include "expression_evaluator.hpp"
#include "expression_parser.hpp"
#include "http_stream_handler.hpp"

#include <gtest/gtest.h>

using namespace timestar;

static StreamingDataPoint makePoint(const std::string& measurement, const std::string& field,
                                    const std::map<std::string, std::string>& tags, uint64_t timestamp, double value) {
    StreamingDataPoint pt;
    pt.measurement = measurement;
    pt.field = field;
    pt.tags = makeTags(tags);
    pt.timestamp = timestamp;
    pt.value = value;
    return pt;
}

TEST(StreamingAggregatorTest, AvgAggregation) {
    // 10-second buckets (10 billion nanoseconds)
    StreamingAggregator agg(10'000'000'000ULL, AggregationMethod::AVG);

    // All in same bucket [0, 10s)
    agg.addPoint(makePoint("temp", "value", {}, 1'000'000'000, 10.0));
    agg.addPoint(makePoint("temp", "value", {}, 2'000'000'000, 20.0));
    agg.addPoint(makePoint("temp", "value", {}, 3'000'000'000, 30.0));

    auto batch = agg.closeBuckets();
    ASSERT_EQ(batch.points.size(), 1u);
    EXPECT_EQ(batch.points[0].timestamp, 0u);                         // bucket start
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[0].value), 20.0);  // avg
    EXPECT_EQ(batch.points[0].measurement, "temp");
    EXPECT_EQ(batch.points[0].field, "value");
}

TEST(StreamingAggregatorTest, SumAggregation) {
    StreamingAggregator agg(10'000'000'000ULL, AggregationMethod::SUM);

    agg.addPoint(makePoint("temp", "value", {}, 1'000'000'000, 10.0));
    agg.addPoint(makePoint("temp", "value", {}, 2'000'000'000, 20.0));
    agg.addPoint(makePoint("temp", "value", {}, 3'000'000'000, 30.0));

    auto batch = agg.closeBuckets();
    ASSERT_EQ(batch.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[0].value), 60.0);
}

TEST(StreamingAggregatorTest, MinMaxAggregation) {
    StreamingAggregator aggMin(10'000'000'000ULL, AggregationMethod::MIN);
    StreamingAggregator aggMax(10'000'000'000ULL, AggregationMethod::MAX);

    for (auto* agg : {&aggMin, &aggMax}) {
        agg->addPoint(makePoint("temp", "value", {}, 1'000'000'000, 10.0));
        agg->addPoint(makePoint("temp", "value", {}, 2'000'000'000, 5.0));
        agg->addPoint(makePoint("temp", "value", {}, 3'000'000'000, 30.0));
    }

    auto minBatch = aggMin.closeBuckets();
    ASSERT_EQ(minBatch.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(minBatch.points[0].value), 5.0);

    auto maxBatch = aggMax.closeBuckets();
    ASSERT_EQ(maxBatch.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(maxBatch.points[0].value), 30.0);
}

TEST(StreamingAggregatorTest, LatestAggregation) {
    StreamingAggregator agg(10'000'000'000ULL, AggregationMethod::LATEST);

    agg.addPoint(makePoint("temp", "value", {}, 1'000'000'000, 10.0));
    agg.addPoint(makePoint("temp", "value", {}, 5'000'000'000, 50.0));
    agg.addPoint(makePoint("temp", "value", {}, 3'000'000'000, 30.0));

    auto batch = agg.closeBuckets();
    ASSERT_EQ(batch.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[0].value), 50.0);  // latest by timestamp
}

TEST(StreamingAggregatorTest, MultipleBuckets) {
    // 5-second buckets
    StreamingAggregator agg(5'000'000'000ULL, AggregationMethod::AVG);

    // Bucket [0, 5s): timestamps 1s and 3s
    agg.addPoint(makePoint("temp", "value", {}, 1'000'000'000, 10.0));
    agg.addPoint(makePoint("temp", "value", {}, 3'000'000'000, 20.0));

    // Bucket [5s, 10s): timestamp 7s
    agg.addPoint(makePoint("temp", "value", {}, 7'000'000'000, 30.0));

    auto batch = agg.closeBuckets();
    ASSERT_EQ(batch.points.size(), 2u);

    // Points sorted by bucket start
    EXPECT_EQ(batch.points[0].timestamp, 0u);
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[0].value), 15.0);  // avg(10, 20)

    EXPECT_EQ(batch.points[1].timestamp, 5'000'000'000ULL);
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[1].value), 30.0);  // avg(30)
}

TEST(StreamingAggregatorTest, MultipleSeriesIndependent) {
    StreamingAggregator agg(10'000'000'000ULL, AggregationMethod::AVG);

    agg.addPoint(makePoint("temp", "value", {{"loc", "west"}}, 1'000'000'000, 10.0));
    agg.addPoint(makePoint("temp", "value", {{"loc", "west"}}, 2'000'000'000, 20.0));
    agg.addPoint(makePoint("temp", "value", {{"loc", "east"}}, 1'000'000'000, 100.0));

    auto batch = agg.closeBuckets();
    ASSERT_EQ(batch.points.size(), 2u);

    // Find each series
    bool foundWest = false, foundEast = false;
    for (const auto& pt : batch.points) {
        if (pt.tags->at("loc") == "west") {
            EXPECT_DOUBLE_EQ(std::get<double>(pt.value), 15.0);
            foundWest = true;
        } else if (pt.tags->at("loc") == "east") {
            EXPECT_DOUBLE_EQ(std::get<double>(pt.value), 100.0);
            foundEast = true;
        }
    }
    EXPECT_TRUE(foundWest);
    EXPECT_TRUE(foundEast);
}

TEST(StreamingAggregatorTest, MultipleFieldsIndependent) {
    StreamingAggregator agg(10'000'000'000ULL, AggregationMethod::AVG);

    agg.addPoint(makePoint("cpu", "usage", {}, 1'000'000'000, 80.0));
    agg.addPoint(makePoint("cpu", "usage", {}, 2'000'000'000, 90.0));
    agg.addPoint(makePoint("cpu", "idle", {}, 1'000'000'000, 20.0));
    agg.addPoint(makePoint("cpu", "idle", {}, 2'000'000'000, 10.0));

    auto batch = agg.closeBuckets();
    ASSERT_EQ(batch.points.size(), 2u);

    bool foundUsage = false, foundIdle = false;
    for (const auto& pt : batch.points) {
        if (pt.field == "usage") {
            EXPECT_DOUBLE_EQ(std::get<double>(pt.value), 85.0);
            foundUsage = true;
        } else if (pt.field == "idle") {
            EXPECT_DOUBLE_EQ(std::get<double>(pt.value), 15.0);
            foundIdle = true;
        }
    }
    EXPECT_TRUE(foundUsage);
    EXPECT_TRUE(foundIdle);
}

TEST(StreamingAggregatorTest, Int64Values) {
    StreamingAggregator agg(10'000'000'000ULL, AggregationMethod::SUM);

    StreamingDataPoint pt;
    pt.measurement = "counters";
    pt.field = "requests";
    pt.timestamp = 1'000'000'000;
    pt.value = int64_t(100);
    agg.addPoint(pt);

    pt.timestamp = 2'000'000'000;
    pt.value = int64_t(200);
    agg.addPoint(pt);

    auto batch = agg.closeBuckets();
    ASSERT_EQ(batch.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[0].value), 300.0);
}

TEST(StreamingAggregatorTest, EmptyBucketsReturnEmpty) {
    StreamingAggregator agg(10'000'000'000ULL, AggregationMethod::AVG);

    EXPECT_FALSE(agg.hasData());
    auto batch = agg.closeBuckets();
    EXPECT_TRUE(batch.points.empty());
}

// Verify that computeResult() returns NaN (not 0.0) when count == 0,
// so callers can distinguish "no data" from a genuine zero measurement.
TEST(StreamingAggregatorTest, EmptyBucketComputeResultIsNaN) {
    BucketState state;
    // count == 0: no data points were ever added
    ASSERT_EQ(state.count, 0u);

    for (AggregationMethod method : {AggregationMethod::AVG, AggregationMethod::SUM, AggregationMethod::MIN,
                                     AggregationMethod::MAX, AggregationMethod::LATEST}) {
        double result = state.computeResult(method);
        EXPECT_TRUE(std::isnan(result)) << "Expected NaN for empty bucket with method " << static_cast<int>(method)
                                        << " but got " << result;
    }
}

TEST(StreamingAggregatorTest, CloseBucketsClearsState) {
    StreamingAggregator agg(10'000'000'000ULL, AggregationMethod::AVG);

    agg.addPoint(makePoint("temp", "value", {}, 1'000'000'000, 10.0));
    EXPECT_TRUE(agg.hasData());

    auto batch1 = agg.closeBuckets();
    EXPECT_EQ(batch1.points.size(), 1u);
    EXPECT_FALSE(agg.hasData());

    // Second close returns empty
    auto batch2 = agg.closeBuckets();
    EXPECT_TRUE(batch2.points.empty());

    // Add new data
    agg.addPoint(makePoint("temp", "value", {}, 11'000'000'000, 20.0));
    auto batch3 = agg.closeBuckets();
    ASSERT_EQ(batch3.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(batch3.points[0].value), 20.0);
}

// --- SSE Event Formatting Tests ---

TEST(SSEFormatTest, EventIncludesLabelWhenSet) {
    StreamingBatch batch;
    batch.label = "cpu";
    batch.sequenceId = 5;

    StreamingDataPoint pt;
    pt.measurement = "cpu";
    pt.field = "usage";
    pt.timestamp = 1000;
    pt.value = 85.0;
    batch.points.push_back(std::move(pt));

    auto event = HttpStreamHandler::formatSSEEvent(batch);
    EXPECT_NE(event.find("\"label\":\"cpu\""), std::string::npos);
    EXPECT_NE(event.find("id: 5"), std::string::npos);
    EXPECT_NE(event.find("event: data"), std::string::npos);
}

TEST(SSEFormatTest, EventOmitsLabelWhenEmpty) {
    StreamingBatch batch;
    batch.sequenceId = 0;

    StreamingDataPoint pt;
    pt.measurement = "temp";
    pt.field = "value";
    pt.timestamp = 2000;
    pt.value = 23.5;
    batch.points.push_back(std::move(pt));

    auto event = HttpStreamHandler::formatSSEEvent(batch);
    EXPECT_EQ(event.find("\"label\""), std::string::npos);
}

TEST(SSEFormatTest, BackfillEventIncludesLabel) {
    StreamingBatch batch;
    batch.label = "mem";
    batch.sequenceId = 0;

    StreamingDataPoint pt;
    pt.measurement = "memory";
    pt.field = "used";
    pt.timestamp = 3000;
    pt.value = 70.0;
    batch.points.push_back(std::move(pt));

    auto event = HttpStreamHandler::formatSSEBackfillEvent(batch);
    EXPECT_NE(event.find("event: backfill"), std::string::npos);
    EXPECT_NE(event.find("\"label\":\"mem\""), std::string::npos);
}

// --- Formula Evaluation Tests ---

TEST(StreamingFormulaTest, ScalarMultiply) {
    // Formula: a * 100
    ExpressionParser parser("a * 100");
    auto ast = parser.parse();

    StreamingBatch batch;
    batch.label = "cpu";
    batch.sequenceId = 1;
    batch.points.push_back(makePoint("cpu", "usage", {}, 1000, 0.85));
    batch.points.push_back(makePoint("cpu", "usage", {}, 2000, 0.92));

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast);
    ASSERT_EQ(result.points.size(), 2u);
    EXPECT_EQ(result.label, "cpu");
    EXPECT_EQ(result.sequenceId, 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[0].value), 85.0);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[1].value), 92.0);
    EXPECT_EQ(result.points[0].measurement, "cpu");
    EXPECT_EQ(result.points[0].field, "usage");
}

TEST(StreamingFormulaTest, ArithmeticExpression) {
    // Formula: (a - 32) * 5 / 9   (Fahrenheit to Celsius)
    ExpressionParser parser("(a - 32) * 5 / 9");
    auto ast = parser.parse();

    StreamingBatch batch;
    batch.points.push_back(makePoint("temp", "value", {}, 1000, 212.0));  // boiling
    batch.points.push_back(makePoint("temp", "value", {}, 2000, 32.0));   // freezing

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast);
    ASSERT_EQ(result.points.size(), 2u);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[0].value), 100.0);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[1].value), 0.0);
}

TEST(StreamingFormulaTest, UnaryFunction) {
    // Formula: abs(a)
    ExpressionParser parser("abs(a)");
    auto ast = parser.parse();

    StreamingBatch batch;
    batch.points.push_back(makePoint("sensor", "delta", {}, 1000, -5.0));
    batch.points.push_back(makePoint("sensor", "delta", {}, 2000, 3.0));
    batch.points.push_back(makePoint("sensor", "delta", {}, 3000, -10.0));

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast);
    ASSERT_EQ(result.points.size(), 3u);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[0].value), 5.0);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[1].value), 3.0);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[2].value), 10.0);
}

TEST(StreamingFormulaTest, DiffFunction) {
    // Formula: diff(a) — computes consecutive differences
    ExpressionParser parser("diff(a)");
    auto ast = parser.parse();

    StreamingBatch batch;
    batch.points.push_back(makePoint("counter", "value", {}, 1000, 100.0));
    batch.points.push_back(makePoint("counter", "value", {}, 2000, 130.0));
    batch.points.push_back(makePoint("counter", "value", {}, 3000, 145.0));

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast);
    // diff() returns n points (first is NaN typically, or n-1 points)
    ASSERT_GE(result.points.size(), 2u);
    // Check the non-NaN diffs
    bool found30 = false, found15 = false;
    for (const auto& pt : result.points) {
        double val = std::get<double>(pt.value);
        if (!std::isnan(val)) {
            if (std::abs(val - 30.0) < 0.001)
                found30 = true;
            if (std::abs(val - 15.0) < 0.001)
                found15 = true;
        }
    }
    EXPECT_TRUE(found30);
    EXPECT_TRUE(found15);
}

TEST(StreamingFormulaTest, MultipleSeriesAppliedIndependently) {
    // Formula should be applied to each series independently
    ExpressionParser parser("a * 2");
    auto ast = parser.parse();

    StreamingBatch batch;
    batch.points.push_back(makePoint("cpu", "usage", {{"host", "a"}}, 1000, 50.0));
    batch.points.push_back(makePoint("cpu", "usage", {{"host", "a"}}, 2000, 60.0));
    batch.points.push_back(makePoint("cpu", "usage", {{"host", "b"}}, 1000, 70.0));

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast);
    ASSERT_EQ(result.points.size(), 3u);

    // Find each series
    for (const auto& pt : result.points) {
        double val = std::get<double>(pt.value);
        if (pt.tags->at("host") == "a") {
            EXPECT_TRUE(val == 100.0 || val == 120.0);
        } else if (pt.tags->at("host") == "b") {
            EXPECT_DOUBLE_EQ(val, 140.0);
        }
    }
}

TEST(StreamingFormulaTest, PreservesMetadata) {
    // Label and sequenceId should be preserved through formula application
    ExpressionParser parser("a + 1");
    auto ast = parser.parse();

    StreamingBatch batch;
    batch.label = "myquery";
    batch.sequenceId = 42;
    batch.points.push_back(makePoint("temp", "celsius", {{"loc", "west"}}, 5000, 25.0));

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast);
    ASSERT_EQ(result.points.size(), 1u);
    EXPECT_EQ(result.label, "myquery");
    EXPECT_EQ(result.sequenceId, 42u);
    EXPECT_EQ(result.points[0].measurement, "temp");
    EXPECT_EQ(result.points[0].field, "celsius");
    EXPECT_EQ(result.points[0].tags->at("loc"), "west");
    EXPECT_EQ(result.points[0].timestamp, 5000u);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[0].value), 26.0);
}

TEST(StreamingFormulaTest, EmptyBatchReturnsEmpty) {
    ExpressionParser parser("a * 100");
    auto ast = parser.parse();

    StreamingBatch batch;
    batch.label = "test";

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast);
    EXPECT_TRUE(result.points.empty());
    EXPECT_EQ(result.label, "test");
}

TEST(StreamingFormulaTest, ClampMinFunction) {
    // Formula: clamp_min(a, 0) — clamp negative values to 0
    ExpressionParser parser("clamp_min(a, 0)");
    auto ast = parser.parse();

    StreamingBatch batch;
    batch.points.push_back(makePoint("sensor", "delta", {}, 1000, -5.0));
    batch.points.push_back(makePoint("sensor", "delta", {}, 2000, 3.0));
    batch.points.push_back(makePoint("sensor", "delta", {}, 3000, -1.0));

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast);
    ASSERT_EQ(result.points.size(), 3u);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[0].value), 0.0);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[1].value), 3.0);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[2].value), 0.0);
}

TEST(StreamingFormulaTest, AggregatorThenFormula) {
    // End-to-end: aggregate data, then apply formula to the result
    StreamingAggregator agg(10'000'000'000ULL, AggregationMethod::AVG);

    agg.addPoint(makePoint("cpu", "usage", {}, 1'000'000'000, 0.80));
    agg.addPoint(makePoint("cpu", "usage", {}, 2'000'000'000, 0.90));
    agg.addPoint(makePoint("cpu", "usage", {}, 3'000'000'000, 0.85));

    auto batch = agg.closeBuckets();
    ASSERT_EQ(batch.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[0].value), 0.85);  // avg

    // Apply "a * 100" formula
    ExpressionParser parser("a * 100");
    auto ast = parser.parse();
    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast);

    ASSERT_EQ(result.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[0].value), 85.0);
}
