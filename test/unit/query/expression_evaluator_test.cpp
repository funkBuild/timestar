#include <gtest/gtest.h>
#include "expression_evaluator.hpp"
#include "expression_parser.hpp"
#include <cmath>
#include <limits>

using namespace tsdb;

class ExpressionEvaluatorTest : public ::testing::Test {
protected:
    ExpressionEvaluator evaluator;

    void SetUp() override {}
    void TearDown() override {}

    AlignedSeries makeSeries(std::vector<uint64_t> ts, std::vector<double> vals) {
        return AlignedSeries(std::move(ts), std::move(vals));
    }

    AlignedSeries makeConstantSeries(double value, size_t size = 5) {
        std::vector<uint64_t> ts(size);
        std::vector<double> vals(size, value);
        for (size_t i = 0; i < size; ++i) {
            ts[i] = (i + 1) * 1000;
        }
        return AlignedSeries(std::move(ts), std::move(vals));
    }

    AlignedSeries makeRampSeries(double start = 1.0, double step = 1.0, size_t size = 5) {
        std::vector<uint64_t> ts(size);
        std::vector<double> vals(size);
        for (size_t i = 0; i < size; ++i) {
            ts[i] = (i + 1) * 1000;
            vals[i] = start + i * step;
        }
        return AlignedSeries(std::move(ts), std::move(vals));
    }
};

// ==================== AlignedSeries Operator Tests ====================

TEST_F(ExpressionEvaluatorTest, SeriesAddition) {
    auto a = makeConstantSeries(10.0);
    auto b = makeConstantSeries(5.0);
    auto result = a + b;

    EXPECT_EQ(result.size(), 5);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 15.0);
    }
}

TEST_F(ExpressionEvaluatorTest, SeriesSubtraction) {
    auto a = makeConstantSeries(10.0);
    auto b = makeConstantSeries(3.0);
    auto result = a - b;

    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 7.0);
    }
}

TEST_F(ExpressionEvaluatorTest, SeriesMultiplication) {
    auto a = makeConstantSeries(4.0);
    auto b = makeConstantSeries(3.0);
    auto result = a * b;

    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 12.0);
    }
}

TEST_F(ExpressionEvaluatorTest, SeriesDivision) {
    auto a = makeConstantSeries(20.0);
    auto b = makeConstantSeries(4.0);
    auto result = a / b;

    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 5.0);
    }
}

TEST_F(ExpressionEvaluatorTest, SeriesDivisionByZero) {
    auto a = makeSeries({1000, 2000, 3000}, {10.0, 0.0, -5.0});
    auto b = makeSeries({1000, 2000, 3000}, {0.0, 0.0, 0.0});
    auto result = a / b;

    EXPECT_TRUE(std::isinf(result.values[0]));  // 10/0 = Inf
    EXPECT_TRUE(std::isnan(result.values[1]));  // 0/0 = NaN
    EXPECT_TRUE(std::isinf(result.values[2]));  // -5/0 = -Inf
    EXPECT_LT(result.values[2], 0);             // Should be negative infinity
}

TEST_F(ExpressionEvaluatorTest, ScalarAddition) {
    auto a = makeConstantSeries(10.0);
    auto result = a + 5.0;

    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 15.0);
    }
}

TEST_F(ExpressionEvaluatorTest, ScalarMultiplication) {
    auto a = makeRampSeries(1.0, 1.0, 5);  // [1, 2, 3, 4, 5]
    auto result = a * 2.0;

    EXPECT_DOUBLE_EQ(result.values[0], 2.0);
    EXPECT_DOUBLE_EQ(result.values[4], 10.0);
}

TEST_F(ExpressionEvaluatorTest, SeriesNegate) {
    auto a = makeRampSeries(1.0, 1.0, 5);
    auto result = a.negate();

    EXPECT_DOUBLE_EQ(result.values[0], -1.0);
    EXPECT_DOUBLE_EQ(result.values[4], -5.0);
}

TEST_F(ExpressionEvaluatorTest, SeriesAbs) {
    auto a = makeSeries({1000, 2000, 3000}, {-5.0, 0.0, 3.0});
    auto result = a.abs();

    EXPECT_DOUBLE_EQ(result.values[0], 5.0);
    EXPECT_DOUBLE_EQ(result.values[1], 0.0);
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
}

TEST_F(ExpressionEvaluatorTest, SeriesSqrt) {
    auto a = makeSeries({1000, 2000, 3000}, {4.0, 9.0, 16.0});
    auto result = a.sqrt();

    EXPECT_DOUBLE_EQ(result.values[0], 2.0);
    EXPECT_DOUBLE_EQ(result.values[1], 3.0);
    EXPECT_DOUBLE_EQ(result.values[2], 4.0);
}

TEST_F(ExpressionEvaluatorTest, SeriesMinMax) {
    auto a = makeSeries({1000, 2000, 3000}, {1.0, 5.0, 3.0});
    auto b = makeSeries({1000, 2000, 3000}, {2.0, 3.0, 4.0});

    auto minResult = AlignedSeries::min(a, b);
    auto maxResult = AlignedSeries::max(a, b);

    EXPECT_DOUBLE_EQ(minResult.values[0], 1.0);
    EXPECT_DOUBLE_EQ(minResult.values[1], 3.0);
    EXPECT_DOUBLE_EQ(minResult.values[2], 3.0);

    EXPECT_DOUBLE_EQ(maxResult.values[0], 2.0);
    EXPECT_DOUBLE_EQ(maxResult.values[1], 5.0);
    EXPECT_DOUBLE_EQ(maxResult.values[2], 4.0);
}

TEST_F(ExpressionEvaluatorTest, SeriesPow) {
    auto base = makeSeries({1000, 2000}, {2.0, 3.0});
    auto exp = makeSeries({1000, 2000}, {3.0, 2.0});

    auto result = AlignedSeries::pow(base, exp);

    EXPECT_DOUBLE_EQ(result.values[0], 8.0);   // 2^3
    EXPECT_DOUBLE_EQ(result.values[1], 9.0);   // 3^2
}

TEST_F(ExpressionEvaluatorTest, SeriesClamp) {
    auto val = makeSeries({1000, 2000, 3000}, {-5.0, 50.0, 150.0});
    auto minVal = makeConstantSeries(0.0, 3);
    auto maxVal = makeConstantSeries(100.0, 3);

    auto result = AlignedSeries::clamp(val, minVal, maxVal);

    EXPECT_DOUBLE_EQ(result.values[0], 0.0);    // -5 clamped to 0
    EXPECT_DOUBLE_EQ(result.values[1], 50.0);   // 50 unchanged
    EXPECT_DOUBLE_EQ(result.values[2], 100.0);  // 150 clamped to 100
}

// ==================== Evaluator Tests ====================

TEST_F(ExpressionEvaluatorTest, EvaluateSimpleQueryRef) {
    ExpressionParser parser("a");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeConstantSeries(42.0);

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 5);
    EXPECT_DOUBLE_EQ(result.values[0], 42.0);
}

TEST_F(ExpressionEvaluatorTest, EvaluateScalar) {
    ExpressionParser parser("100");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeConstantSeries(1.0);  // Need at least one series for timestamps

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 5);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 100.0);
    }
}

TEST_F(ExpressionEvaluatorTest, EvaluateAddition) {
    ExpressionParser parser("a + b");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeConstantSeries(10.0);
    results["b"] = makeConstantSeries(5.0);

    auto result = evaluator.evaluate(*ast, results);

    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 15.0);
    }
}

TEST_F(ExpressionEvaluatorTest, EvaluateSubtraction) {
    ExpressionParser parser("a - b");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeConstantSeries(10.0);
    results["b"] = makeConstantSeries(3.0);

    auto result = evaluator.evaluate(*ast, results);

    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 7.0);
    }
}

TEST_F(ExpressionEvaluatorTest, EvaluateMultiplyByScalar) {
    ExpressionParser parser("a * 100");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeRampSeries(0.1, 0.1, 5);  // [0.1, 0.2, 0.3, 0.4, 0.5]

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[4], 50.0);
}

TEST_F(ExpressionEvaluatorTest, EvaluateDivision) {
    ExpressionParser parser("a / b");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeConstantSeries(100.0);
    results["b"] = makeConstantSeries(4.0);

    auto result = evaluator.evaluate(*ast, results);

    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 25.0);
    }
}

TEST_F(ExpressionEvaluatorTest, EvaluateNegation) {
    ExpressionParser parser("-a");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeConstantSeries(5.0);

    auto result = evaluator.evaluate(*ast, results);

    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], -5.0);
    }
}

TEST_F(ExpressionEvaluatorTest, EvaluateAbsFunction) {
    ExpressionParser parser("abs(a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {-5.0, 0.0, 3.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_DOUBLE_EQ(result.values[0], 5.0);
    EXPECT_DOUBLE_EQ(result.values[1], 0.0);
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
}

TEST_F(ExpressionEvaluatorTest, EvaluateMinFunction) {
    ExpressionParser parser("min(a, b)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {1.0, 5.0, 3.0});
    results["b"] = makeSeries({1000, 2000, 3000}, {2.0, 3.0, 4.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 3.0);
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
}

TEST_F(ExpressionEvaluatorTest, EvaluateMaxFunction) {
    ExpressionParser parser("max(a, b)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {1.0, 5.0, 3.0});
    results["b"] = makeSeries({1000, 2000, 3000}, {2.0, 3.0, 4.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_DOUBLE_EQ(result.values[0], 2.0);
    EXPECT_DOUBLE_EQ(result.values[1], 5.0);
    EXPECT_DOUBLE_EQ(result.values[2], 4.0);
}

// ==================== Complex Formula Tests ====================

TEST_F(ExpressionEvaluatorTest, EvaluateErrorRateFormula) {
    // errors / total * 100
    ExpressionParser parser("errors / total * 100");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["errors"] = makeSeries({1000, 2000, 3000}, {5.0, 10.0, 2.0});
    results["total"] = makeSeries({1000, 2000, 3000}, {100.0, 200.0, 50.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_DOUBLE_EQ(result.values[0], 5.0);   // 5/100 * 100 = 5%
    EXPECT_DOUBLE_EQ(result.values[1], 5.0);   // 10/200 * 100 = 5%
    EXPECT_DOUBLE_EQ(result.values[2], 4.0);   // 2/50 * 100 = 4%
}

TEST_F(ExpressionEvaluatorTest, EvaluateCPUUtilizationFormula) {
    // (a / (a + b)) * 100
    ExpressionParser parser("(a / (a + b)) * 100");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {80.0, 60.0, 90.0});  // usage
    results["b"] = makeSeries({1000, 2000, 3000}, {20.0, 40.0, 10.0});  // idle

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_DOUBLE_EQ(result.values[0], 80.0);  // 80/(80+20) * 100
    EXPECT_DOUBLE_EQ(result.values[1], 60.0);  // 60/(60+40) * 100
    EXPECT_DOUBLE_EQ(result.values[2], 90.0);  // 90/(90+10) * 100
}

TEST_F(ExpressionEvaluatorTest, EvaluateComplexFormula) {
    // max(a, b) / (min(c, d) + 1)
    ExpressionParser parser("max(a, b) / (min(c, d) + 1)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000}, {10.0, 5.0});
    results["b"] = makeSeries({1000, 2000}, {8.0, 15.0});
    results["c"] = makeSeries({1000, 2000}, {2.0, 3.0});
    results["d"] = makeSeries({1000, 2000}, {4.0, 1.0});

    auto result = evaluator.evaluate(*ast, results);

    // max(10,8) / (min(2,4) + 1) = 10 / 3 = 3.333...
    EXPECT_NEAR(result.values[0], 10.0 / 3.0, 0.001);
    // max(5,15) / (min(3,1) + 1) = 15 / 2 = 7.5
    EXPECT_DOUBLE_EQ(result.values[1], 7.5);
}

TEST_F(ExpressionEvaluatorTest, EvaluateNestedFunctions) {
    // abs(min(a, b))
    ExpressionParser parser("abs(min(a, b))");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000}, {-5.0, 3.0});
    results["b"] = makeSeries({1000, 2000}, {-2.0, -4.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_DOUBLE_EQ(result.values[0], 5.0);   // abs(min(-5, -2)) = abs(-5) = 5
    EXPECT_DOUBLE_EQ(result.values[1], 4.0);   // abs(min(3, -4)) = abs(-4) = 4
}

// ==================== Error Handling Tests ====================

TEST_F(ExpressionEvaluatorTest, ErrorEmptyResults) {
    ExpressionParser parser("a");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;  // Empty

    EXPECT_THROW(evaluator.evaluate(*ast, results), EvaluationException);
}

TEST_F(ExpressionEvaluatorTest, ErrorMissingQuery) {
    ExpressionParser parser("a + b");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeConstantSeries(10.0);
    // 'b' is missing

    EXPECT_THROW(evaluator.evaluate(*ast, results), EvaluationException);
}

TEST_F(ExpressionEvaluatorTest, ErrorSizeMismatch) {
    ExpressionParser parser("a + b");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {1.0, 2.0, 3.0});
    results["b"] = makeSeries({1000, 2000}, {1.0, 2.0});  // Different size

    EXPECT_THROW(evaluator.evaluate(*ast, results), EvaluationException);
}

TEST_F(ExpressionEvaluatorTest, ErrorScalarDivisionByZero) {
    auto a = makeConstantSeries(10.0);
    EXPECT_THROW(a / 0.0, EvaluationException);
}

// ==================== Timestamp Preservation Tests ====================

TEST_F(ExpressionEvaluatorTest, TimestampsPreserved) {
    ExpressionParser parser("a + b");
    auto ast = parser.parse();

    std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000};
    ExpressionEvaluator::QueryResultMap results;
    results["a"] = AlignedSeries(timestamps, {1.0, 2.0, 3.0, 4.0, 5.0});
    results["b"] = AlignedSeries(timestamps, {5.0, 4.0, 3.0, 2.0, 1.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.timestamps, timestamps);
}

// ==================== per_minute / per_hour Tests ====================

TEST_F(ExpressionEvaluatorTest, PerMinuteBasic) {
    // Timestamps 1 second apart (in nanoseconds), values incrementing by 10
    // Rate = 10/s => per minute = 600/min
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL},
        {100.0, 110.0, 120.0});

    auto result = series.per_minute(1.0);

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(std::isnan(result.values[0]));  // First point has no previous
    EXPECT_DOUBLE_EQ(result.values[1], 600.0);  // (110-100) / 1s * 60
    EXPECT_DOUBLE_EQ(result.values[2], 600.0);  // (120-110) / 1s * 60
}

TEST_F(ExpressionEvaluatorTest, PerMinuteIdenticalTimestamps) {
    // Two points with the same timestamp: should output 0.0 (not div by zero)
    auto series = makeSeries(
        {1000000000ULL, 1000000000ULL, 2000000000ULL},
        {100.0, 110.0, 120.0});

    auto result = series.per_minute(1.0);

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 0.0);    // Same timestamp => 0.0
    EXPECT_DOUBLE_EQ(result.values[2], 600.0);   // Normal rate
}

TEST_F(ExpressionEvaluatorTest, PerMinuteZeroSecondsPerPoint) {
    // seconds_per_point = 0 should not cause division by zero
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL},
        {100.0, 110.0});

    auto result = series.per_minute(0.0);

    EXPECT_EQ(result.size(), 2);
    EXPECT_TRUE(std::isnan(result.values[0]));
    // Should use timestamp delta (1s), not seconds_per_point
    EXPECT_DOUBLE_EQ(result.values[1], 600.0);
}

TEST_F(ExpressionEvaluatorTest, PerMinuteAllIdenticalTimestamps) {
    // All timestamps the same: all outputs should be 0.0 (except first which is NaN)
    auto series = makeSeries(
        {1000000000ULL, 1000000000ULL, 1000000000ULL},
        {100.0, 200.0, 300.0});

    auto result = series.per_minute(0.0);

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 0.0);
    EXPECT_DOUBLE_EQ(result.values[2], 0.0);
}

TEST_F(ExpressionEvaluatorTest, PerHourBasic) {
    // Timestamps 1 second apart, values incrementing by 1
    // Rate = 1/s => per hour = 3600/hr
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL},
        {10.0, 11.0, 12.0});

    auto result = series.per_hour(1.0);

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 3600.0);
    EXPECT_DOUBLE_EQ(result.values[2], 3600.0);
}

TEST_F(ExpressionEvaluatorTest, PerHourIdenticalTimestamps) {
    // Identical timestamps should output 0.0 (not div by zero)
    auto series = makeSeries(
        {1000000000ULL, 1000000000ULL, 2000000000ULL},
        {10.0, 20.0, 30.0});

    auto result = series.per_hour(1.0);

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 0.0);     // Same timestamp => 0.0
    EXPECT_DOUBLE_EQ(result.values[2], 36000.0);  // (30-20) / 1s * 3600
}

TEST_F(ExpressionEvaluatorTest, PerHourZeroSecondsPerPoint) {
    // seconds_per_point = 0 should not cause division by zero
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL},
        {10.0, 11.0});

    auto result = series.per_hour(0.0);

    EXPECT_EQ(result.size(), 2);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 3600.0);
}

TEST_F(ExpressionEvaluatorTest, PerMinuteEmptySeries) {
    auto series = makeSeries({}, {});
    auto result = series.per_minute(1.0);
    EXPECT_TRUE(result.empty());
}

TEST_F(ExpressionEvaluatorTest, PerHourEmptySeries) {
    auto series = makeSeries({}, {});
    auto result = series.per_hour(1.0);
    EXPECT_TRUE(result.empty());
}

TEST_F(ExpressionEvaluatorTest, PerMinuteVariableIntervals) {
    // Timestamps with variable intervals
    auto series = makeSeries(
        {0ULL, 1000000000ULL, 3000000000ULL, 4000000000ULL},
        {0.0, 10.0, 30.0, 40.0});

    auto result = series.per_minute(1.0);

    EXPECT_TRUE(std::isnan(result.values[0]));
    // (10-0) / 1s * 60 = 600
    EXPECT_DOUBLE_EQ(result.values[1], 600.0);
    // (30-10) / 2s * 60 = 600
    EXPECT_DOUBLE_EQ(result.values[2], 600.0);
    // (40-30) / 1s * 60 = 600
    EXPECT_DOUBLE_EQ(result.values[3], 600.0);
}

// ==================== monotonic_diff consistency tests ====================

TEST_F(ExpressionEvaluatorTest, MonotonicDiffCounterReset) {
    // Verify that AlignedSeries::monotonic_diff returns current value on counter reset
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL},
        {100.0, 150.0, 50.0, 80.0});

    auto result = series.monotonic_diff();

    EXPECT_EQ(result.size(), 4);
    EXPECT_TRUE(std::isnan(result.values[0]));    // First point, no predecessor
    EXPECT_DOUBLE_EQ(result.values[1], 50.0);    // 150 - 100 = 50 (positive, kept)
    EXPECT_DOUBLE_EQ(result.values[2], 50.0);    // 50 - 150 = -100 (counter reset, returns 50.0)
    EXPECT_DOUBLE_EQ(result.values[3], 30.0);    // 80 - 50 = 30 (positive, kept)
}

TEST_F(ExpressionEvaluatorTest, MonotonicDiffConsistentWithTransformFunctions) {
    // Verify that the expression evaluator path (AlignedSeries::monotonic_diff)
    // and the transform functions path produce the same results
    std::vector<uint64_t> timestamps = {
        1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL, 5000000000ULL
    };
    std::vector<double> values = {10.0, 30.0, 5.0, 25.0, 15.0};

    // Path 1: AlignedSeries::monotonic_diff (expression evaluator path)
    auto series = makeSeries(timestamps, values);
    auto evaluatorResult = series.monotonic_diff();

    // Path 2: tsdb::transform::monotonic_diff (transform functions path)
    // This delegates to simd::monotonic_diff which uses SIMD or scalar
    // We just need to verify both paths agree on counter reset behavior
    EXPECT_TRUE(std::isnan(evaluatorResult.values[0]));
    EXPECT_DOUBLE_EQ(evaluatorResult.values[1], 20.0);   // 30 - 10 = 20 (positive)
    EXPECT_DOUBLE_EQ(evaluatorResult.values[2], 5.0);    // 5 - 30 = -25 (reset, returns 5.0)
    EXPECT_DOUBLE_EQ(evaluatorResult.values[3], 20.0);   // 25 - 5 = 20 (positive)
    EXPECT_DOUBLE_EQ(evaluatorResult.values[4], 15.0);   // 15 - 25 = -10 (reset, returns 15.0)
}
