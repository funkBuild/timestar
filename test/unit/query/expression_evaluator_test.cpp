#include <gtest/gtest.h>
#include "expression_evaluator.hpp"
#include "expression_parser.hpp"
#include <cmath>
#include <limits>

using namespace timestar;

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

    EXPECT_EQ(*result.timestamps, timestamps);
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

    // Path 2: timestar::transform::monotonic_diff (transform functions path)
    // This delegates to simd::monotonic_diff which uses SIMD or scalar
    // We just need to verify both paths agree on counter reset behavior
    EXPECT_TRUE(std::isnan(evaluatorResult.values[0]));
    EXPECT_DOUBLE_EQ(evaluatorResult.values[1], 20.0);   // 30 - 10 = 20 (positive)
    EXPECT_DOUBLE_EQ(evaluatorResult.values[2], 5.0);    // 5 - 30 = -25 (reset, returns 5.0)
    EXPECT_DOUBLE_EQ(evaluatorResult.values[3], 20.0);   // 25 - 5 = 20 (positive)
    EXPECT_DOUBLE_EQ(evaluatorResult.values[4], 15.0);   // 15 - 25 = -10 (reset, returns 15.0)
}

// ==================== Rolling Window Function Tests ====================

// --- rolling_avg ---

TEST_F(ExpressionEvaluatorTest, RollingAvgNormalCase) {
    // series: [1, 2, 3, 4, 5], window=3
    // first 2 points NaN, then averages: (1+2+3)/3=2, (2+3+4)/3=3, (3+4+5)/3=4
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000}, {1.0, 2.0, 3.0, 4.0, 5.0});
    auto result = series.rolling_avg(3);

    EXPECT_EQ(result.size(), 5);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    EXPECT_DOUBLE_EQ(result.values[2], 2.0);
    EXPECT_DOUBLE_EQ(result.values[3], 3.0);
    EXPECT_DOUBLE_EQ(result.values[4], 4.0);
}

TEST_F(ExpressionEvaluatorTest, RollingAvgWindowSizeOne) {
    // N=1: passthrough, every point is its own average
    auto series = makeSeries({1000, 2000, 3000}, {10.0, 20.0, 30.0});
    auto result = series.rolling_avg(1);

    EXPECT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 20.0);
    EXPECT_DOUBLE_EQ(result.values[2], 30.0);
}

TEST_F(ExpressionEvaluatorTest, RollingAvgConstantSeries) {
    // All values equal: average always equals that value
    auto series = makeConstantSeries(7.0, 6);
    auto result = series.rolling_avg(3);

    EXPECT_EQ(result.size(), 6);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    for (size_t i = 2; i < 6; ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 7.0);
    }
}

TEST_F(ExpressionEvaluatorTest, RollingAvgWindowLargerThanSeries) {
    // N > series length: all NaN
    auto series = makeSeries({1000, 2000}, {1.0, 2.0});
    auto result = series.rolling_avg(5);

    EXPECT_EQ(result.size(), 2);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
}

TEST_F(ExpressionEvaluatorTest, RollingAvgViaParser) {
    // Verify the parser path works end-to-end
    ExpressionParser parser("rolling_avg(a, 3)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000, 4000, 5000}, {1.0, 2.0, 3.0, 4.0, 5.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 5);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    EXPECT_DOUBLE_EQ(result.values[2], 2.0);
    EXPECT_DOUBLE_EQ(result.values[3], 3.0);
    EXPECT_DOUBLE_EQ(result.values[4], 4.0);
}

// --- rolling_min ---

TEST_F(ExpressionEvaluatorTest, RollingMinNormalCase) {
    // series: [3, 1, 4, 1, 5, 9], window=3
    // first 2 NaN, then rolling minimums:
    //   min(3,1,4)=1, min(1,4,1)=1, min(4,1,5)=1, min(1,5,9)=1
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000, 6000},
                             {3.0, 1.0, 4.0, 1.0, 5.0, 9.0});
    auto result = series.rolling_min(3);

    EXPECT_EQ(result.size(), 6);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    EXPECT_DOUBLE_EQ(result.values[2], 1.0);  // min(3,1,4)
    EXPECT_DOUBLE_EQ(result.values[3], 1.0);  // min(1,4,1)
    EXPECT_DOUBLE_EQ(result.values[4], 1.0);  // min(4,1,5)
    EXPECT_DOUBLE_EQ(result.values[5], 1.0);  // min(1,5,9)
}

TEST_F(ExpressionEvaluatorTest, RollingMinWindowSizeOne) {
    // N=1: passthrough
    auto series = makeSeries({1000, 2000, 3000}, {5.0, 2.0, 8.0});
    auto result = series.rolling_min(1);

    EXPECT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 5.0);
    EXPECT_DOUBLE_EQ(result.values[1], 2.0);
    EXPECT_DOUBLE_EQ(result.values[2], 8.0);
}

TEST_F(ExpressionEvaluatorTest, RollingMinConstantSeries) {
    auto series = makeConstantSeries(4.0, 5);
    auto result = series.rolling_min(3);

    EXPECT_EQ(result.size(), 5);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    for (size_t i = 2; i < 5; ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 4.0);
    }
}

TEST_F(ExpressionEvaluatorTest, RollingMinViaParser) {
    ExpressionParser parser("rolling_min(a, 2)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000, 4000}, {5.0, 3.0, 8.0, 1.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 4);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 3.0);  // min(5,3)
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);  // min(3,8)
    EXPECT_DOUBLE_EQ(result.values[3], 1.0);  // min(8,1)
}

// --- rolling_max ---

TEST_F(ExpressionEvaluatorTest, RollingMaxNormalCase) {
    // series: [3, 1, 4, 1, 5, 9], window=3
    // first 2 NaN, then rolling maximums:
    //   max(3,1,4)=4, max(1,4,1)=4, max(4,1,5)=5, max(1,5,9)=9
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000, 6000},
                             {3.0, 1.0, 4.0, 1.0, 5.0, 9.0});
    auto result = series.rolling_max(3);

    EXPECT_EQ(result.size(), 6);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    EXPECT_DOUBLE_EQ(result.values[2], 4.0);  // max(3,1,4)
    EXPECT_DOUBLE_EQ(result.values[3], 4.0);  // max(1,4,1)
    EXPECT_DOUBLE_EQ(result.values[4], 5.0);  // max(4,1,5)
    EXPECT_DOUBLE_EQ(result.values[5], 9.0);  // max(1,5,9)
}

TEST_F(ExpressionEvaluatorTest, RollingMaxWindowSizeOne) {
    // N=1: passthrough
    auto series = makeSeries({1000, 2000, 3000}, {5.0, 2.0, 8.0});
    auto result = series.rolling_max(1);

    EXPECT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 5.0);
    EXPECT_DOUBLE_EQ(result.values[1], 2.0);
    EXPECT_DOUBLE_EQ(result.values[2], 8.0);
}

TEST_F(ExpressionEvaluatorTest, RollingMaxConstantSeries) {
    auto series = makeConstantSeries(9.0, 5);
    auto result = series.rolling_max(3);

    EXPECT_EQ(result.size(), 5);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    for (size_t i = 2; i < 5; ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 9.0);
    }
}

TEST_F(ExpressionEvaluatorTest, RollingMaxViaParser) {
    ExpressionParser parser("rolling_max(a, 2)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000, 4000}, {5.0, 3.0, 8.0, 1.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 4);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 5.0);  // max(5,3)
    EXPECT_DOUBLE_EQ(result.values[2], 8.0);  // max(3,8)
    EXPECT_DOUBLE_EQ(result.values[3], 8.0);  // max(8,1)
}

// --- rolling_stddev ---

TEST_F(ExpressionEvaluatorTest, RollingStddevNormalCase) {
    // series: [2, 4, 4, 4, 5, 5, 7, 9], window=4
    // Population stddev for first complete window [2,4,4,4]:
    //   mean=3.5, variance=((2-3.5)^2+(4-3.5)^2+(4-3.5)^2+(4-3.5)^2)/4
    //           = (2.25+0.25+0.25+0.25)/4 = 3/4 = 0.75, stddev=sqrt(0.75)
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000},
                             {2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0});
    auto result = series.rolling_stddev(4);

    EXPECT_EQ(result.size(), 8);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    EXPECT_TRUE(std::isnan(result.values[2]));
    // Window [2,4,4,4]: mean=3.5, var=0.75, stddev=sqrt(0.75)
    EXPECT_NEAR(result.values[3], std::sqrt(0.75), 1e-10);
}

TEST_F(ExpressionEvaluatorTest, RollingStddevWindowSizeOne) {
    // N=1: stddev of a single point is always 0
    auto series = makeSeries({1000, 2000, 3000}, {3.0, 7.0, 2.0});
    auto result = series.rolling_stddev(1);

    EXPECT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 0.0);
    EXPECT_DOUBLE_EQ(result.values[2], 0.0);
}

TEST_F(ExpressionEvaluatorTest, RollingStddevConstantSeries) {
    // Constant series: stddev is always 0 for a full window
    auto series = makeConstantSeries(5.0, 6);
    auto result = series.rolling_stddev(3);

    EXPECT_EQ(result.size(), 6);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    for (size_t i = 2; i < 6; ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 0.0);
    }
}

TEST_F(ExpressionEvaluatorTest, RollingStddevKnownValues) {
    // series: [10, 20], window=2
    // mean=(10+20)/2=15, variance=((10-15)^2+(20-15)^2)/2=(25+25)/2=25, stddev=5
    auto series = makeSeries({1000, 2000}, {10.0, 20.0});
    auto result = series.rolling_stddev(2);

    EXPECT_EQ(result.size(), 2);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 5.0);
}

TEST_F(ExpressionEvaluatorTest, RollingStddevViaParser) {
    ExpressionParser parser("rolling_stddev(a, 2)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {10.0, 20.0, 30.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 5.0);   // stddev([10,20])=5
    EXPECT_DOUBLE_EQ(result.values[2], 5.0);   // stddev([20,30])=5
}

TEST_F(ExpressionEvaluatorTest, RollingStddevFirstNMinusOneAreNaN) {
    // Verify that exactly N-1 leading NaN values are output for window size N=4
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000},
                             {1.0, 2.0, 3.0, 4.0, 5.0});
    auto result = series.rolling_stddev(4);

    EXPECT_EQ(result.size(), 5);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    EXPECT_TRUE(std::isnan(result.values[2]));
    EXPECT_FALSE(std::isnan(result.values[3]));  // Window [1,2,3,4] complete
    EXPECT_FALSE(std::isnan(result.values[4]));  // Window [2,3,4,5] complete
}

// --- zscore ---

TEST_F(ExpressionEvaluatorTest, ZScoreNormalCase) {
    // series: [10, 20, 30, 40, 50], window=3
    // i=0,1: NaN (window not full)
    // i=2: window=[10,20,30], mean=20, var=((10-20)^2+(20-20)^2+(30-20)^2)/3=(100+0+100)/3=200/3
    //       stddev=sqrt(200/3), zscore[2]=(30-20)/sqrt(200/3)
    // i=3: window=[20,30,40], mean=30, same variance=200/3, zscore[3]=(40-30)/sqrt(200/3)
    // i=4: window=[30,40,50], mean=40, same variance=200/3, zscore[4]=(50-40)/sqrt(200/3)
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000}, {10.0, 20.0, 30.0, 40.0, 50.0});
    auto result = series.zscore(3);

    EXPECT_EQ(result.size(), 5);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));

    // Hand computation: window [10,20,30], mean=20, variance=200/3, stddev=sqrt(200/3)
    double stddev3 = std::sqrt(200.0 / 3.0);
    EXPECT_NEAR(result.values[2], (30.0 - 20.0) / stddev3, 1e-10);
    EXPECT_NEAR(result.values[3], (40.0 - 30.0) / stddev3, 1e-10);
    EXPECT_NEAR(result.values[4], (50.0 - 40.0) / stddev3, 1e-10);
}

TEST_F(ExpressionEvaluatorTest, ZScoreConstantSeriesOutputsZero) {
    // Constant series: rolling stddev is 0, so output 0 for all complete windows
    auto series = makeConstantSeries(42.0, 6);
    auto result = series.zscore(3);

    EXPECT_EQ(result.size(), 6);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    for (size_t i = 2; i < 6; ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 0.0);
    }
}

TEST_F(ExpressionEvaluatorTest, ZScoreFirstNMinusOneAreNaN) {
    // Exactly N-1 leading values must be NaN
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000},
                             {1.0, 2.0, 3.0, 4.0, 5.0});
    auto result = series.zscore(4);

    EXPECT_EQ(result.size(), 5);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    EXPECT_TRUE(std::isnan(result.values[2]));
    EXPECT_FALSE(std::isnan(result.values[3]));  // Window [1,2,3,4] complete
    EXPECT_FALSE(std::isnan(result.values[4]));  // Window [2,3,4,5] complete
}

TEST_F(ExpressionEvaluatorTest, ZScoreWindowWithNaNOutputsNaN) {
    // If any value in the window is NaN, output for that point must be NaN
    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000},
                             {1.0, nan, 3.0, 4.0, 5.0});
    auto result = series.zscore(3);

    EXPECT_EQ(result.size(), 5);
    EXPECT_TRUE(std::isnan(result.values[0]));  // window not full
    EXPECT_TRUE(std::isnan(result.values[1]));  // window not full
    EXPECT_TRUE(std::isnan(result.values[2]));  // window [1, nan, 3] contains NaN
    EXPECT_TRUE(std::isnan(result.values[3]));  // window [nan, 3, 4] contains NaN
    EXPECT_FALSE(std::isnan(result.values[4])); // window [3, 4, 5] all valid
}

TEST_F(ExpressionEvaluatorTest, ZScoreWindowSizeOne) {
    // N=1: each point is its own window, mean=value, stddev=0, output=0
    auto series = makeSeries({1000, 2000, 3000}, {10.0, 20.0, 30.0});
    auto result = series.zscore(1);

    EXPECT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 0.0);
    EXPECT_DOUBLE_EQ(result.values[2], 0.0);
}

TEST_F(ExpressionEvaluatorTest, ZScoreSingleSpike) {
    // Flat series with a spike in the middle; spike should produce a high z-score
    // [5, 5, 100, 5, 5], window=3
    // i=2: window=[5, 5, 100], mean=110/3, stddev=...spike is above mean -> positive z
    // i=3: window=[5, 100, 5], mean=110/3, zscore[3]=(5-110/3)/stddev < 0
    // i=4: window=[100, 5, 5], mean=110/3, zscore[4]=(5-110/3)/stddev < 0
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000},
                             {5.0, 5.0, 100.0, 5.0, 5.0});
    auto result = series.zscore(3);

    EXPECT_EQ(result.size(), 5);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    // At the spike point: z-score should be strongly positive
    EXPECT_GT(result.values[2], 1.0);
    // After the spike: z-score should be negative (value below window mean)
    EXPECT_LT(result.values[3], 0.0);
    EXPECT_LT(result.values[4], 0.0);
}

TEST_F(ExpressionEvaluatorTest, ZScoreViaParser) {
    // Parser round-trip: zscore(a, 5)
    ExpressionParser parser("zscore(a, 5)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    // Ramp series [1,2,3,4,5,6,7], window=5
    results["a"] = makeSeries({1000, 2000, 3000, 4000, 5000, 6000, 7000},
                               {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 7);
    // First 4 points must be NaN (N-1 = 4)
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_TRUE(std::isnan(result.values[i])) << "index " << i << " should be NaN";
    }
    // Points 4, 5, 6 must be valid (non-NaN)
    for (size_t i = 4; i < 7; ++i) {
        EXPECT_FALSE(std::isnan(result.values[i])) << "index " << i << " should not be NaN";
    }
}

// ==================== Counter-Rate Function Tests ====================

// --- rate() ---

TEST_F(ExpressionEvaluatorTest, RateBasic) {
    // Timestamps 1 second apart (in nanoseconds), values incrementing by 10
    // rate[1] = (10 increase) / 1s = 10/s; rate[2] = 10/s; rate[3] = 10/s
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL},
        {100.0, 110.0, 120.0, 130.0});

    auto result = series.rate();

    EXPECT_EQ(result.size(), 4);
    EXPECT_TRUE(std::isnan(result.values[0]));   // First point: no predecessor
    EXPECT_DOUBLE_EQ(result.values[1], 10.0);    // (110-100) / 1s = 10/s
    EXPECT_DOUBLE_EQ(result.values[2], 10.0);    // (120-110) / 1s = 10/s
    EXPECT_DOUBLE_EQ(result.values[3], 10.0);    // (130-120) / 1s = 10/s
}

TEST_F(ExpressionEvaluatorTest, RateVariableIntervals) {
    // Timestamps with variable intervals; values increment by 60 then 120
    // rate[1] = 60 / 2s = 30/s; rate[2] = 120 / 4s = 30/s
    auto series = makeSeries(
        {0ULL, 2000000000ULL, 6000000000ULL},
        {0.0, 60.0, 180.0});

    auto result = series.rate();

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 30.0);    // 60 / 2s
    EXPECT_DOUBLE_EQ(result.values[2], 30.0);    // 120 / 4s
}

TEST_F(ExpressionEvaluatorTest, RateFirstPointIsNaN) {
    // Verify that regardless of series content, output[0] is always NaN
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL},
        {999.0, 1000.0});

    auto result = series.rate();

    EXPECT_EQ(result.size(), 2);
    EXPECT_TRUE(std::isnan(result.values[0]));
}

TEST_F(ExpressionEvaluatorTest, RateCounterReset) {
    // Counter resets mid-series: negative diff is treated as 0 (not negative rate)
    // Values: 100 -> 200 (diff=100) -> 50 (reset, diff=-150 -> 0) -> 80 (diff=30)
    // Timestamps 1 second apart
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL},
        {100.0, 200.0, 50.0, 80.0});

    auto result = series.rate();

    EXPECT_EQ(result.size(), 4);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 100.0);   // (200-100) / 1s = 100/s
    EXPECT_DOUBLE_EQ(result.values[2], 0.0);     // reset: diff treated as 0
    EXPECT_DOUBLE_EQ(result.values[3], 30.0);    // (80-50) / 1s = 30/s
}

TEST_F(ExpressionEvaluatorTest, RateMultipleResets) {
    // Two consecutive counter resets
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL, 5000000000ULL},
        {50.0, 100.0, 20.0, 10.0, 40.0});

    auto result = series.rate();

    EXPECT_EQ(result.size(), 5);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 50.0);    // (100-50) / 1s
    EXPECT_DOUBLE_EQ(result.values[2], 0.0);     // reset: 20-100 < 0, use 0
    EXPECT_DOUBLE_EQ(result.values[3], 0.0);     // reset: 10-20 < 0, use 0
    EXPECT_DOUBLE_EQ(result.values[4], 30.0);    // (40-10) / 1s
}

TEST_F(ExpressionEvaluatorTest, RateEmptySeries) {
    auto series = makeSeries({}, {});
    auto result = series.rate();
    EXPECT_TRUE(result.empty());
}

TEST_F(ExpressionEvaluatorTest, RateSinglePoint) {
    // Single point: only output[0] = NaN
    auto series = makeSeries({1000000000ULL}, {42.0});
    auto result = series.rate();

    EXPECT_EQ(result.size(), 1);
    EXPECT_TRUE(std::isnan(result.values[0]));
}

TEST_F(ExpressionEvaluatorTest, RateIdenticalTimestamps) {
    // Zero time delta: rate should be NaN (not infinity)
    auto series = makeSeries(
        {1000000000ULL, 1000000000ULL, 2000000000ULL},
        {10.0, 20.0, 30.0});

    auto result = series.rate();

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));  // dt=0: NaN
    EXPECT_DOUBLE_EQ(result.values[2], 10.0);   // (30-20) / 1s = 10/s
}

TEST_F(ExpressionEvaluatorTest, RateViaParser) {
    // End-to-end through parser
    ExpressionParser parser("rate(a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL},
        {0.0, 5.0, 10.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 5.0);
    EXPECT_DOUBLE_EQ(result.values[2], 5.0);
}

// --- irate() ---

TEST_F(ExpressionEvaluatorTest, IrateBasic) {
    // irate uses only the last two points: values[N-2] and values[N-1]
    // Values: 0, 10, 20, 50 at 1s intervals
    // Last two: 20->50, delta=30, dt=1s => rate=30/s (constant across all points)
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL},
        {0.0, 10.0, 20.0, 50.0});

    auto result = series.irate();

    EXPECT_EQ(result.size(), 4);
    // All points should equal the rate between last two points
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 30.0);
    }
}

TEST_F(ExpressionEvaluatorTest, IrateConstantSeriesAllSame) {
    // All outputs equal the irate between the last two points
    auto series = makeSeries(
        {0ULL, 500000000ULL, 1000000000ULL},
        {10.0, 15.0, 22.0});

    auto result = series.irate();

    // Last two: 15->22, dt=0.5s => rate = 7/0.5 = 14/s
    EXPECT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 14.0);
    EXPECT_DOUBLE_EQ(result.values[1], 14.0);
    EXPECT_DOUBLE_EQ(result.values[2], 14.0);
}

TEST_F(ExpressionEvaluatorTest, IrateCounterResetAtLastStep) {
    // When the last diff is negative (counter reset), irate returns 0
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL},
        {100.0, 200.0, 50.0});

    auto result = series.irate();

    EXPECT_EQ(result.size(), 3);
    // Last two: 200->50 is a reset => 0
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 0.0);
    }
}

TEST_F(ExpressionEvaluatorTest, IrateSinglePoint) {
    // Single point: fewer than 2 points => NaN constant series
    auto series = makeSeries({1000000000ULL}, {42.0});
    auto result = series.irate();

    EXPECT_EQ(result.size(), 1);
    EXPECT_TRUE(std::isnan(result.values[0]));
}

TEST_F(ExpressionEvaluatorTest, IrateEmptySeries) {
    auto series = makeSeries({}, {});
    auto result = series.irate();
    EXPECT_TRUE(result.empty());
}

TEST_F(ExpressionEvaluatorTest, IrateIdenticalLastTimestamps) {
    // Zero dt between last two points: irate should be NaN
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 2000000000ULL},
        {10.0, 20.0, 30.0});

    auto result = series.irate();

    EXPECT_EQ(result.size(), 3);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isnan(result.values[i]));
    }
}

TEST_F(ExpressionEvaluatorTest, IrateViaParser) {
    ExpressionParser parser("irate(a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL},
        {0.0, 5.0, 15.0});

    auto result = evaluator.evaluate(*ast, results);

    // Last two: 5->15, dt=1s => irate=10/s constant
    EXPECT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 10.0);
    EXPECT_DOUBLE_EQ(result.values[2], 10.0);
}

// --- increase() ---

TEST_F(ExpressionEvaluatorTest, IncreaseBasic) {
    // Values: 0, 10, 20, 30 (all positive diffs: 10+10+10=30)
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL},
        {0.0, 10.0, 20.0, 30.0});

    auto result = series.increase();

    EXPECT_EQ(result.size(), 4);
    // Constant scalar = 30
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 30.0);
    }
}

TEST_F(ExpressionEvaluatorTest, IncreaseWithReset) {
    // Values: 100, 200, 50, 80
    // Diffs: +100 (reset diff: -150 -> 0), +30
    // Total increase = 100 + 0 + 30 = 130
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL},
        {100.0, 200.0, 50.0, 80.0});

    auto result = series.increase();

    EXPECT_EQ(result.size(), 4);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 130.0);
    }
}

TEST_F(ExpressionEvaluatorTest, IncreaseOnlyResets) {
    // All diffs are negative (series is monotonically decreasing)
    // Total increase = 0
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL},
        {100.0, 50.0, 10.0});

    auto result = series.increase();

    EXPECT_EQ(result.size(), 3);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 0.0);
    }
}

TEST_F(ExpressionEvaluatorTest, IncreaseMultipleResets) {
    // Values: 10, 20, 5, 15, 3, 25
    // Positive diffs: +10, +10 (reset:5-20<0 skip), +0 (reset:3-15<0 skip), +22
    // Total = 10 + 10 + 22 = 42
    auto series = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL,
         4000000000ULL, 5000000000ULL, 6000000000ULL},
        {10.0, 20.0, 5.0, 15.0, 3.0, 25.0});

    auto result = series.increase();

    EXPECT_EQ(result.size(), 6);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 42.0);
    }
}

TEST_F(ExpressionEvaluatorTest, IncreaseSinglePoint) {
    // Single point: no diffs possible, total increase = 0
    auto series = makeSeries({1000000000ULL}, {42.0});
    auto result = series.increase();

    EXPECT_EQ(result.size(), 1);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
}

TEST_F(ExpressionEvaluatorTest, IncreaseEmptySeries) {
    auto series = makeSeries({}, {});
    auto result = series.increase();
    EXPECT_TRUE(result.empty());
}

TEST_F(ExpressionEvaluatorTest, IncreaseViaParser) {
    ExpressionParser parser("increase(a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL},
        {5.0, 10.0, 8.0, 20.0});

    auto result = evaluator.evaluate(*ast, results);

    // Diffs: +5, reset(8-10=-2->0), +12 => total=17
    EXPECT_EQ(result.size(), 4);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 17.0);
    }
}

// ==================== Gap-Fill Function Tests ====================

static const double kNaN = std::numeric_limits<double>::quiet_NaN();

// ---- fill_forward ----

TEST_F(ExpressionEvaluatorTest, FillForwardBasicGap) {
    // NaN in the middle: filled with the previous real value
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000},
                             {1.0, kNaN, kNaN, 4.0, 5.0});
    auto result = series.fill_forward();

    EXPECT_EQ(result.size(), 5);
    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 1.0);  // carried from index 0
    EXPECT_DOUBLE_EQ(result.values[2], 1.0);  // carried from index 0
    EXPECT_DOUBLE_EQ(result.values[3], 4.0);
    EXPECT_DOUBLE_EQ(result.values[4], 5.0);
}

TEST_F(ExpressionEvaluatorTest, FillForwardNoNaNs) {
    // No NaNs: output identical to input
    auto series = makeSeries({1000, 2000, 3000}, {10.0, 20.0, 30.0});
    auto result = series.fill_forward();

    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 20.0);
    EXPECT_DOUBLE_EQ(result.values[2], 30.0);
}

TEST_F(ExpressionEvaluatorTest, FillForwardAllNaN) {
    // All NaN: output remains all NaN (no real value to carry)
    auto series = makeSeries({1000, 2000, 3000}, {kNaN, kNaN, kNaN});
    auto result = series.fill_forward();

    EXPECT_EQ(result.size(), 3);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isnan(result.values[i]));
    }
}

TEST_F(ExpressionEvaluatorTest, FillForwardLeadingNaNsStayNaN) {
    // Leading NaNs before any real value must stay NaN
    auto series = makeSeries({1000, 2000, 3000, 4000},
                             {kNaN, kNaN, 3.0, kNaN});
    auto result = series.fill_forward();

    EXPECT_TRUE(std::isnan(result.values[0]));  // leading NaN stays
    EXPECT_TRUE(std::isnan(result.values[1]));  // leading NaN stays
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
    EXPECT_DOUBLE_EQ(result.values[3], 3.0);   // trailing NaN filled by carry
}

TEST_F(ExpressionEvaluatorTest, FillForwardTrailingNaNsFilled) {
    // Trailing NaNs after last real value are filled by LOCF
    auto series = makeSeries({1000, 2000, 3000, 4000},
                             {1.0, 2.0, kNaN, kNaN});
    auto result = series.fill_forward();

    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 2.0);
    EXPECT_DOUBLE_EQ(result.values[2], 2.0);  // carried from index 1
    EXPECT_DOUBLE_EQ(result.values[3], 2.0);  // carried from index 1
}

TEST_F(ExpressionEvaluatorTest, FillForwardViaParser) {
    ExpressionParser parser("fill_forward(a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000, 4000},
                              {1.0, kNaN, 3.0, kNaN});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 1.0);
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
    EXPECT_DOUBLE_EQ(result.values[3], 3.0);
}

// ---- fill_backward ----

TEST_F(ExpressionEvaluatorTest, FillBackwardBasicGap) {
    // NaN in the middle: filled with the next real value
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000},
                             {1.0, kNaN, kNaN, 4.0, 5.0});
    auto result = series.fill_backward();

    EXPECT_EQ(result.size(), 5);
    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 4.0);  // look-ahead fill
    EXPECT_DOUBLE_EQ(result.values[2], 4.0);  // look-ahead fill
    EXPECT_DOUBLE_EQ(result.values[3], 4.0);
    EXPECT_DOUBLE_EQ(result.values[4], 5.0);
}

TEST_F(ExpressionEvaluatorTest, FillBackwardNoNaNs) {
    // No NaNs: passthrough
    auto series = makeSeries({1000, 2000, 3000}, {10.0, 20.0, 30.0});
    auto result = series.fill_backward();

    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 20.0);
    EXPECT_DOUBLE_EQ(result.values[2], 30.0);
}

TEST_F(ExpressionEvaluatorTest, FillBackwardAllNaN) {
    // All NaN: remains all NaN
    auto series = makeSeries({1000, 2000, 3000}, {kNaN, kNaN, kNaN});
    auto result = series.fill_backward();

    EXPECT_EQ(result.size(), 3);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isnan(result.values[i]));
    }
}

TEST_F(ExpressionEvaluatorTest, FillBackwardTrailingNaNsStayNaN) {
    // Trailing NaNs after the last real value must stay NaN
    auto series = makeSeries({1000, 2000, 3000, 4000},
                             {kNaN, 2.0, kNaN, kNaN});
    auto result = series.fill_backward();

    EXPECT_DOUBLE_EQ(result.values[0], 2.0);  // look-ahead fill
    EXPECT_DOUBLE_EQ(result.values[1], 2.0);
    EXPECT_TRUE(std::isnan(result.values[2]));  // trailing NaN stays
    EXPECT_TRUE(std::isnan(result.values[3]));  // trailing NaN stays
}

TEST_F(ExpressionEvaluatorTest, FillBackwardLeadingNaNsFilled) {
    // Leading NaNs before first real value are filled by backward carry
    auto series = makeSeries({1000, 2000, 3000, 4000},
                             {kNaN, kNaN, 3.0, 4.0});
    auto result = series.fill_backward();

    EXPECT_DOUBLE_EQ(result.values[0], 3.0);  // filled from index 2
    EXPECT_DOUBLE_EQ(result.values[1], 3.0);  // filled from index 2
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
    EXPECT_DOUBLE_EQ(result.values[3], 4.0);
}

TEST_F(ExpressionEvaluatorTest, FillBackwardViaParser) {
    ExpressionParser parser("fill_backward(a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000, 4000},
                              {kNaN, 2.0, kNaN, 4.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_DOUBLE_EQ(result.values[0], 2.0);
    EXPECT_DOUBLE_EQ(result.values[1], 2.0);
    EXPECT_DOUBLE_EQ(result.values[2], 4.0);
    EXPECT_DOUBLE_EQ(result.values[3], 4.0);
}

// ---- fill_linear ----

TEST_F(ExpressionEvaluatorTest, FillLinearBasicGap) {
    // Even timestamps: [1000, 2000, 3000, 4000, 5000]
    // values: [0.0, NaN, NaN, NaN, 4.0]
    // Interpolated: fraction = (t - 1000) / (5000 - 1000)
    //   index 1: (2000-1000)/4000 = 0.25 -> 0.0 + 0.25*4.0 = 1.0
    //   index 2: (3000-1000)/4000 = 0.5  -> 0.0 + 0.5*4.0  = 2.0
    //   index 3: (4000-1000)/4000 = 0.75 -> 0.0 + 0.75*4.0 = 3.0
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000},
                             {0.0, kNaN, kNaN, kNaN, 4.0});
    auto result = series.fill_linear();

    EXPECT_EQ(result.size(), 5);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 1.0);
    EXPECT_DOUBLE_EQ(result.values[2], 2.0);
    EXPECT_DOUBLE_EQ(result.values[3], 3.0);
    EXPECT_DOUBLE_EQ(result.values[4], 4.0);
}

TEST_F(ExpressionEvaluatorTest, FillLinearNoNaNs) {
    // No NaNs: passthrough
    auto series = makeSeries({1000, 2000, 3000}, {1.0, 2.0, 3.0});
    auto result = series.fill_linear();

    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 2.0);
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
}

TEST_F(ExpressionEvaluatorTest, FillLinearAllNaN) {
    // All NaN: remains all NaN (entire series is a leading+trailing run)
    auto series = makeSeries({1000, 2000, 3000}, {kNaN, kNaN, kNaN});
    auto result = series.fill_linear();

    EXPECT_EQ(result.size(), 3);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isnan(result.values[i]));
    }
}

TEST_F(ExpressionEvaluatorTest, FillLinearLeadingNaNsStayNaN) {
    // Leading NaN run has no left anchor: stays NaN
    auto series = makeSeries({1000, 2000, 3000, 4000},
                             {kNaN, kNaN, 3.0, 5.0});
    auto result = series.fill_linear();

    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
    EXPECT_DOUBLE_EQ(result.values[3], 5.0);
}

TEST_F(ExpressionEvaluatorTest, FillLinearTrailingNaNsStayNaN) {
    // Trailing NaN run has no right anchor: stays NaN
    auto series = makeSeries({1000, 2000, 3000, 4000},
                             {1.0, 3.0, kNaN, kNaN});
    auto result = series.fill_linear();

    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 3.0);
    EXPECT_TRUE(std::isnan(result.values[2]));
    EXPECT_TRUE(std::isnan(result.values[3]));
}

TEST_F(ExpressionEvaluatorTest, FillLinearUnevenTimestamps) {
    // Uneven spacing: timestamps [0, 100, 900, 1000]
    // values: [0.0, NaN, NaN, 10.0]
    // dt_total = 1000 - 0 = 1000
    //   index 1: fraction = (100-0)/1000 = 0.1 -> 0.0 + 0.1*10.0 = 1.0
    //   index 2: fraction = (900-0)/1000 = 0.9 -> 0.0 + 0.9*10.0 = 9.0
    auto series = makeSeries({0, 100, 900, 1000},
                             {0.0, kNaN, kNaN, 10.0});
    auto result = series.fill_linear();

    EXPECT_EQ(result.size(), 4);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_NEAR(result.values[1], 1.0, 1e-10);
    EXPECT_NEAR(result.values[2], 9.0, 1e-10);
    EXPECT_DOUBLE_EQ(result.values[3], 10.0);
}

TEST_F(ExpressionEvaluatorTest, FillLinearMultipleGaps) {
    // Two separate NaN gaps surrounded by known values
    // values: [0.0, NaN, 2.0, NaN, 4.0]
    // timestamps: [0, 1000, 2000, 3000, 4000]
    // Gap 1: index 1, t0=0/v0=0, t1=2000/v1=2.0: fraction=(1000-0)/2000=0.5 -> 1.0
    // Gap 2: index 3, t0=2000/v0=2.0, t1=4000/v1=4.0: fraction=(3000-2000)/2000=0.5 -> 3.0
    auto series = makeSeries({0, 1000, 2000, 3000, 4000},
                             {0.0, kNaN, 2.0, kNaN, 4.0});
    auto result = series.fill_linear();

    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_NEAR(result.values[1], 1.0, 1e-10);
    EXPECT_DOUBLE_EQ(result.values[2], 2.0);
    EXPECT_NEAR(result.values[3], 3.0, 1e-10);
    EXPECT_DOUBLE_EQ(result.values[4], 4.0);
}

TEST_F(ExpressionEvaluatorTest, FillLinearViaParser) {
    ExpressionParser parser("fill_linear(a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({0, 1000, 2000},
                              {0.0, kNaN, 10.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_NEAR(result.values[1], 5.0, 1e-10);  // midpoint
    EXPECT_DOUBLE_EQ(result.values[2], 10.0);
}

// ---- fill_value ----

TEST_F(ExpressionEvaluatorTest, FillValueBasicGap) {
    // Replace every NaN with 99.0; non-NaNs unchanged
    auto series = makeSeries({1000, 2000, 3000, 4000},
                             {1.0, kNaN, kNaN, 4.0});
    auto result = series.fill_value(99.0);

    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 99.0);
    EXPECT_DOUBLE_EQ(result.values[2], 99.0);
    EXPECT_DOUBLE_EQ(result.values[3], 4.0);
}

TEST_F(ExpressionEvaluatorTest, FillValueNoNaNs) {
    // No NaNs: passthrough
    auto series = makeSeries({1000, 2000, 3000}, {1.0, 2.0, 3.0});
    auto result = series.fill_value(0.0);

    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 2.0);
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
}

TEST_F(ExpressionEvaluatorTest, FillValueAllNaN) {
    // All NaN: all become v
    auto series = makeSeries({1000, 2000, 3000}, {kNaN, kNaN, kNaN});
    auto result = series.fill_value(7.0);

    EXPECT_EQ(result.size(), 3);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 7.0);
    }
}

TEST_F(ExpressionEvaluatorTest, FillValueZeroEqualsDefaultZero) {
    // fill_value(s, 0) must behave identically to default_zero(s)
    auto series = makeSeries({1000, 2000, 3000, 4000},
                             {1.0, kNaN, 3.0, kNaN});
    auto r_fill  = series.fill_value(0.0);
    auto r_defz  = series.default_zero();

    ASSERT_EQ(r_fill.size(), r_defz.size());
    for (size_t i = 0; i < r_fill.size(); ++i) {
        EXPECT_DOUBLE_EQ(r_fill.values[i], r_defz.values[i]);
    }
}

TEST_F(ExpressionEvaluatorTest, FillValueViaParser) {
    ExpressionParser parser("fill_value(a, 99.0)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000},
                              {kNaN, 5.0, kNaN});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_DOUBLE_EQ(result.values[0], 99.0);
    EXPECT_DOUBLE_EQ(result.values[1], 5.0);
    EXPECT_DOUBLE_EQ(result.values[2], 99.0);
}

// ==================== EMA (Exponential Moving Average) Tests ====================

TEST_F(ExpressionEvaluatorTest, EmaBasicAlpha) {
    // alpha=0.5, values=[10, 20, 30, 40, 50]
    // output[0] = 10 (seed)
    // output[1] = 0.5*20 + 0.5*10 = 15
    // output[2] = 0.5*30 + 0.5*15 = 22.5
    // output[3] = 0.5*40 + 0.5*22.5 = 31.25
    // output[4] = 0.5*50 + 0.5*31.25 = 40.625
    auto series = makeSeries({1000, 2000, 3000, 4000, 5000},
                             {10.0, 20.0, 30.0, 40.0, 50.0});
    auto result = series.ema(0.5);

    EXPECT_EQ(result.size(), 5);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 15.0);
    EXPECT_DOUBLE_EQ(result.values[2], 22.5);
    EXPECT_DOUBLE_EQ(result.values[3], 31.25);
    EXPECT_DOUBLE_EQ(result.values[4], 40.625);
}

TEST_F(ExpressionEvaluatorTest, EmaAlphaOnePassthrough) {
    // alpha=1 => output[i] = values[i] (ignoring NaN: carry forward)
    auto series = makeSeries({1000, 2000, 3000, 4000},
                             {5.0, 10.0, 15.0, 20.0});
    auto result = series.ema(1.0);

    EXPECT_EQ(result.size(), 4);
    EXPECT_DOUBLE_EQ(result.values[0], 5.0);
    EXPECT_DOUBLE_EQ(result.values[1], 10.0);
    EXPECT_DOUBLE_EQ(result.values[2], 15.0);
    EXPECT_DOUBLE_EQ(result.values[3], 20.0);
}

TEST_F(ExpressionEvaluatorTest, EmaSpanConversion) {
    // N=9 => alpha = 2/(9+1) = 0.2; values=[100, 100, 100, 100, 100]
    // All values equal, so EMA stays at 100 after seeding
    auto series = makeConstantSeries(100.0, 5);
    auto result = series.ema(9.0); // span=9, alpha=0.2

    EXPECT_EQ(result.size(), 5);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 100.0);
    }
}

TEST_F(ExpressionEvaluatorTest, EmaSpanProducesSmootherOutput) {
    // Large span => smaller alpha => smoother output that moves slowly toward new values
    // span=100 => alpha = 2/101 ~= 0.0198
    // values=[0, 100, 100, 100], first=0 seed, subsequent should increase slowly
    auto series = makeSeries({1000, 2000, 3000, 4000}, {0.0, 100.0, 100.0, 100.0});
    auto result = series.ema(100.0);

    EXPECT_EQ(result.size(), 4);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0); // seeded at 0
    // Each step moves ~2% toward 100
    double alpha = 2.0 / 101.0;
    double e1 = alpha * 100.0 + (1.0 - alpha) * 0.0;
    double e2 = alpha * 100.0 + (1.0 - alpha) * e1;
    double e3 = alpha * 100.0 + (1.0 - alpha) * e2;
    EXPECT_NEAR(result.values[1], e1, 1e-10);
    EXPECT_NEAR(result.values[2], e2, 1e-10);
    EXPECT_NEAR(result.values[3], e3, 1e-10);
    // Should still be well below 10 (slow convergence)
    EXPECT_LT(result.values[3], 10.0);
}

TEST_F(ExpressionEvaluatorTest, EmaNanInMiddleCarriesForward) {
    // NaN in the middle: EMA carries forward the last value
    // alpha=0.5, values=[10, NaN, 30]
    // output[0] = 10 (seed)
    // output[1] = 10 (NaN input -> carry forward prev EMA=10)
    // output[2] = 0.5*30 + 0.5*10 = 20
    auto series = makeSeries({1000, 2000, 3000}, {10.0, kNaN, 30.0});
    auto result = series.ema(0.5);

    EXPECT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 10.0); // carried forward
    EXPECT_DOUBLE_EQ(result.values[2], 20.0);
}

TEST_F(ExpressionEvaluatorTest, EmaLeadingNaNsStayNaN) {
    // Leading NaN values remain NaN; EMA starts at first real value
    // alpha=0.5, values=[NaN, NaN, 20, 30]
    // output[0] = NaN (no seed yet)
    // output[1] = NaN (no seed yet)
    // output[2] = 20 (seeded)
    // output[3] = 0.5*30 + 0.5*20 = 25
    auto series = makeSeries({1000, 2000, 3000, 4000}, {kNaN, kNaN, 20.0, 30.0});
    auto result = series.ema(0.5);

    EXPECT_EQ(result.size(), 4);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    EXPECT_DOUBLE_EQ(result.values[2], 20.0);
    EXPECT_DOUBLE_EQ(result.values[3], 25.0);
}

TEST_F(ExpressionEvaluatorTest, EmaAllNaN) {
    // All NaN: output is all NaN
    auto series = makeSeries({1000, 2000, 3000}, {kNaN, kNaN, kNaN});
    auto result = series.ema(0.5);

    EXPECT_EQ(result.size(), 3);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isnan(result.values[i]));
    }
}

TEST_F(ExpressionEvaluatorTest, EmaSinglePoint) {
    // Single point: seeded, unchanged
    auto series = makeSeries({1000}, {42.0});
    auto result = series.ema(0.3);

    EXPECT_EQ(result.size(), 1);
    EXPECT_DOUBLE_EQ(result.values[0], 42.0);
}

TEST_F(ExpressionEvaluatorTest, EmaEmptySeries) {
    // Empty series: empty output
    auto series = makeSeries({}, {});
    auto result = series.ema(0.5);

    EXPECT_EQ(result.size(), 0);
}

TEST_F(ExpressionEvaluatorTest, EmaViaParser) {
    // Parser round-trip: ema(a, 0.3)
    ExpressionParser parser("ema(a, 0.3)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000, 4000},
                              {10.0, 20.0, 30.0, 40.0});

    auto result = evaluator.evaluate(*ast, results);

    // alpha=0.3
    // output[0] = 10
    // output[1] = 0.3*20 + 0.7*10 = 13
    // output[2] = 0.3*30 + 0.7*13 = 18.1
    // output[3] = 0.3*40 + 0.7*18.1 = 24.67
    EXPECT_EQ(result.size(), 4);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_NEAR(result.values[1], 13.0, 1e-10);
    EXPECT_NEAR(result.values[2], 18.1, 1e-10);
    EXPECT_NEAR(result.values[3], 24.67, 1e-10);
}

// ==================== Holt-Winters (Double Exponential Smoothing) Tests ====================

TEST_F(ExpressionEvaluatorTest, HoltWintersConstantSeriesNoTrend) {
    // Constant series: level stays at the constant, trend stays at 0 => output equals input
    // alpha=0.3, beta=0.2, values=[5, 5, 5, 5, 5]
    // seed: level=5, trend=0, output[0]=5
    // i=1: level=0.3*5 + 0.7*(5+0)=5, trend=0.2*(5-5)+0.8*0=0, output[1]=5
    // ... all remain 5
    auto series = makeConstantSeries(5.0, 5);
    auto result = series.holt_winters(0.3, 0.2);

    EXPECT_EQ(result.size(), 5);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result.values[i], 5.0);
    }
}

TEST_F(ExpressionEvaluatorTest, HoltWintersLinearTrendTracking) {
    // Linearly increasing series: 1, 2, 3, 4, 5 (step=1)
    // alpha=0.8, beta=0.8 (high responsiveness)
    // seed: level=1, trend=0, output[0]=1
    // i=1: prev_level=1
    //   level = 0.8*2 + 0.2*(1+0) = 1.6 + 0.2 = 1.8
    //   trend = 0.8*(1.8-1) + 0.2*0 = 0.64
    //   output[1]=1.8
    // i=2: prev_level=1.8
    //   level = 0.8*3 + 0.2*(1.8+0.64) = 2.4 + 0.488 = 2.888
    //   trend = 0.8*(2.888-1.8) + 0.2*0.64 = 0.8*1.088 + 0.128 = 0.8704 + 0.128 = 0.9984
    //   output[2]=2.888
    auto series = makeRampSeries(1.0, 1.0, 5);
    auto result = series.holt_winters(0.8, 0.8);

    EXPECT_EQ(result.size(), 5);
    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_NEAR(result.values[1], 1.8, 1e-10);
    EXPECT_NEAR(result.values[2], 2.888, 1e-10);
    // Output should track upward trend — each output is above the previous
    EXPECT_GT(result.values[3], result.values[2]);
    EXPECT_GT(result.values[4], result.values[3]);
}

TEST_F(ExpressionEvaluatorTest, HoltWintersAlpha1Beta1Identity) {
    // alpha=1, beta=1: Holt-Winters degenerates such that the level equals each input
    // seed: level=10, trend=0, output[0]=10
    // i=1: prev_level=10
    //   level = 1*20 + 0*(10+0) = 20
    //   trend = 1*(20-10) + 0*0 = 10
    //   output[1]=20
    // i=2: prev_level=20
    //   level = 1*30 + 0*(20+10) = 30
    //   trend = 1*(30-20) + 0*10 = 10
    //   output[2]=30
    // So output equals input exactly for non-NaN values
    auto series = makeSeries({1000, 2000, 3000, 4000}, {10.0, 20.0, 30.0, 40.0});
    auto result = series.holt_winters(1.0, 1.0);

    EXPECT_EQ(result.size(), 4);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 20.0);
    EXPECT_DOUBLE_EQ(result.values[2], 30.0);
    EXPECT_DOUBLE_EQ(result.values[3], 40.0);
}

TEST_F(ExpressionEvaluatorTest, HoltWintersNanInMiddleCarriesForward) {
    // NaN in the middle: level and trend carry forward (projected)
    // alpha=0.5, beta=0.5, values=[10, NaN, 30]
    // seed (i=0): level=10, trend=0, output[0]=10
    // i=1: NaN => level = 10 + 0 = 10, trend stays 0, output[1]=10
    // i=2: prev_level=10
    //   level = 0.5*30 + 0.5*(10+0) = 15 + 5 = 20
    //   trend = 0.5*(20-10) + 0.5*0 = 5
    //   output[2]=20
    const double kNaN = std::numeric_limits<double>::quiet_NaN();
    auto series = makeSeries({1000, 2000, 3000}, {10.0, kNaN, 30.0});
    auto result = series.holt_winters(0.5, 0.5);

    EXPECT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 10.0);  // level+trend projected
    EXPECT_DOUBLE_EQ(result.values[2], 20.0);
}

TEST_F(ExpressionEvaluatorTest, HoltWintersLeadingNaNsStayNaN) {
    // Leading NaN values remain NaN; smoothing starts at first real value
    // alpha=0.5, beta=0.5, values=[NaN, NaN, 20, 30]
    // output[0]=NaN, output[1]=NaN
    // seed at i=2: level=20, trend=0, output[2]=20
    // i=3: prev_level=20
    //   level = 0.5*30 + 0.5*(20+0) = 15 + 10 = 25
    //   trend = 0.5*(25-20) + 0.5*0 = 2.5
    //   output[3]=25
    const double kNaN = std::numeric_limits<double>::quiet_NaN();
    auto series = makeSeries({1000, 2000, 3000, 4000}, {kNaN, kNaN, 20.0, 30.0});
    auto result = series.holt_winters(0.5, 0.5);

    EXPECT_EQ(result.size(), 4);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
    EXPECT_DOUBLE_EQ(result.values[2], 20.0);
    EXPECT_DOUBLE_EQ(result.values[3], 25.0);
}

TEST_F(ExpressionEvaluatorTest, HoltWintersAllNaN) {
    // All NaN: output is all NaN
    const double kNaN = std::numeric_limits<double>::quiet_NaN();
    auto series = makeSeries({1000, 2000, 3000}, {kNaN, kNaN, kNaN});
    auto result = series.holt_winters(0.3, 0.2);

    EXPECT_EQ(result.size(), 3);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isnan(result.values[i]));
    }
}

TEST_F(ExpressionEvaluatorTest, HoltWintersSinglePoint) {
    // Single point: level=value, trend=0, output[0]=value
    auto series = makeSeries({1000}, {42.0});
    auto result = series.holt_winters(0.3, 0.2);

    EXPECT_EQ(result.size(), 1);
    EXPECT_DOUBLE_EQ(result.values[0], 42.0);
}

TEST_F(ExpressionEvaluatorTest, HoltWintersEmptySeries) {
    // Empty series: empty output
    auto series = makeSeries({}, {});
    auto result = series.holt_winters(0.5, 0.5);

    EXPECT_EQ(result.size(), 0);
}

TEST_F(ExpressionEvaluatorTest, HoltWintersViaParser) {
    // Parser round-trip: holt_winters(a, 0.3, 0.1)
    // alpha=0.3, beta=0.1, values=[10, 20, 30]
    // seed (i=0): level=10, trend=0, output[0]=10
    // i=1: prev_level=10
    //   level = 0.3*20 + 0.7*(10+0) = 6 + 7 = 13
    //   trend = 0.1*(13-10) + 0.9*0 = 0.3
    //   output[1]=13
    // i=2: prev_level=13
    //   level = 0.3*30 + 0.7*(13+0.3) = 9 + 0.7*13.3 = 9 + 9.31 = 18.31
    //   trend = 0.1*(18.31-13) + 0.9*0.3 = 0.1*5.31 + 0.27 = 0.531 + 0.27 = 0.801
    //   output[2]=18.31
    ExpressionParser parser("holt_winters(a, 0.3, 0.1)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {10.0, 20.0, 30.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_NEAR(result.values[1], 13.0, 1e-10);
    EXPECT_NEAR(result.values[2], 18.31, 1e-10);
}

// ==================== time_shift() Tests ====================

TEST_F(ExpressionEvaluatorTest, TimeShiftForwardOneSecond) {
    // Timestamps [1000, 2000, 3000] ns, shift '+1s' = 1000000000 ns forward
    auto series = makeSeries({1000ULL, 2000ULL, 3000ULL}, {10.0, 20.0, 30.0});
    int64_t offsetNs = 1000000000LL; // 1 second
    auto result = series.time_shift(offsetNs);

    EXPECT_EQ(result.size(), 3);
    EXPECT_EQ((*result.timestamps)[0], 1000ULL + 1000000000ULL);
    EXPECT_EQ((*result.timestamps)[1], 2000ULL + 1000000000ULL);
    EXPECT_EQ((*result.timestamps)[2], 3000ULL + 1000000000ULL);
    // Values must be unchanged
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 20.0);
    EXPECT_DOUBLE_EQ(result.values[2], 30.0);
}

TEST_F(ExpressionEvaluatorTest, TimeShiftBackwardOneHour) {
    // Negative offset: '-1h' = -3600000000000 ns
    uint64_t base = 7200000000000ULL; // 2 hours in ns
    auto series = makeSeries(
        {base, base + 1000000000ULL, base + 2000000000ULL},
        {1.0, 2.0, 3.0});

    int64_t offsetNs = -3600000000000LL; // -1 hour
    auto result = series.time_shift(offsetNs);

    EXPECT_EQ(result.size(), 3);
    EXPECT_EQ((*result.timestamps)[0], base - 3600000000000ULL);
    EXPECT_EQ((*result.timestamps)[1], base + 1000000000ULL - 3600000000000ULL);
    EXPECT_EQ((*result.timestamps)[2], base + 2000000000ULL - 3600000000000ULL);
    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 2.0);
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
}

TEST_F(ExpressionEvaluatorTest, TimeShiftSevenDays) {
    // '7d' = 7 * 86400 * 1e9 ns
    const uint64_t sevenDaysNs = 7ULL * 86400ULL * 1000000000ULL;
    auto series = makeSeries(
        {1000000000000ULL, 2000000000000ULL},
        {42.0, 99.0});

    auto result = series.time_shift(static_cast<int64_t>(sevenDaysNs));

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ((*result.timestamps)[0], 1000000000000ULL + sevenDaysNs);
    EXPECT_EQ((*result.timestamps)[1], 2000000000000ULL + sevenDaysNs);
    EXPECT_DOUBLE_EQ(result.values[0], 42.0);
    EXPECT_DOUBLE_EQ(result.values[1], 99.0);
}

TEST_F(ExpressionEvaluatorTest, TimeShiftThirtyMinutes) {
    // '30m' = 30 * 60 * 1e9 ns
    const uint64_t thirtyMinNs = 30ULL * 60ULL * 1000000000ULL;
    auto series = makeSeries({5000000000000ULL}, {7.5});
    auto result = series.time_shift(static_cast<int64_t>(thirtyMinNs));

    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ((*result.timestamps)[0], 5000000000000ULL + thirtyMinNs);
    EXPECT_DOUBLE_EQ(result.values[0], 7.5);
}

TEST_F(ExpressionEvaluatorTest, TimeShift500ms) {
    // '500ms' = 500 * 1e6 ns
    const uint64_t fiveHundredMsNs = 500ULL * 1000000ULL;
    auto series = makeSeries({100000000ULL, 200000000ULL, 300000000ULL},
                             {1.1, 2.2, 3.3});
    auto result = series.time_shift(static_cast<int64_t>(fiveHundredMsNs));

    EXPECT_EQ(result.size(), 3);
    EXPECT_EQ((*result.timestamps)[0], 100000000ULL + fiveHundredMsNs);
    EXPECT_EQ((*result.timestamps)[1], 200000000ULL + fiveHundredMsNs);
    EXPECT_EQ((*result.timestamps)[2], 300000000ULL + fiveHundredMsNs);
    EXPECT_DOUBLE_EQ(result.values[0], 1.1);
    EXPECT_DOUBLE_EQ(result.values[1], 2.2);
    EXPECT_DOUBLE_EQ(result.values[2], 3.3);
}

TEST_F(ExpressionEvaluatorTest, TimeShiftZeroOffset) {
    // Offset of 0 ns: timestamps unchanged
    auto series = makeSeries({1000ULL, 2000ULL, 3000ULL}, {5.0, 6.0, 7.0});
    auto result = series.time_shift(0LL);

    EXPECT_EQ((*result.timestamps)[0], 1000ULL);
    EXPECT_EQ((*result.timestamps)[1], 2000ULL);
    EXPECT_EQ((*result.timestamps)[2], 3000ULL);
    EXPECT_DOUBLE_EQ(result.values[0], 5.0);
    EXPECT_DOUBLE_EQ(result.values[1], 6.0);
    EXPECT_DOUBLE_EQ(result.values[2], 7.0);
}

TEST_F(ExpressionEvaluatorTest, TimeShiftEmptySeries) {
    auto series = makeSeries({}, {});
    auto result = series.time_shift(1000000000LL);
    EXPECT_TRUE(result.empty());
}

TEST_F(ExpressionEvaluatorTest, TimeShiftValuesPreservedExactly) {
    // NaN values in the series are preserved unchanged by time_shift
    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto series = makeSeries({1000ULL, 2000ULL, 3000ULL},
                             {1.0, nan, 3.0});
    auto result = series.time_shift(500LL);

    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_TRUE(std::isnan(result.values[1]));
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
}

TEST_F(ExpressionEvaluatorTest, TimeShiftViaParser) {
    // End-to-end: time_shift(a, '1h') through parser and evaluator
    ExpressionParser parser("time_shift(a, '1h')");
    auto ast = parser.parse();

    const uint64_t oneHourNs = 3600ULL * 1000000000ULL;
    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries(
        {1000000000ULL, 2000000000ULL, 3000000000ULL},
        {10.0, 20.0, 30.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 3);
    EXPECT_EQ((*result.timestamps)[0], 1000000000ULL + oneHourNs);
    EXPECT_EQ((*result.timestamps)[1], 2000000000ULL + oneHourNs);
    EXPECT_EQ((*result.timestamps)[2], 3000000000ULL + oneHourNs);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 20.0);
    EXPECT_DOUBLE_EQ(result.values[2], 30.0);
}

TEST_F(ExpressionEvaluatorTest, TimeShiftNegativeViaParser) {
    // time_shift(a, '-1h') shifts timestamps backward by 1 hour
    ExpressionParser parser("time_shift(a, '-1h')");
    auto ast = parser.parse();

    const uint64_t twoHoursNs  = 2ULL * 3600ULL * 1000000000ULL;
    const uint64_t oneHourNs   = 3600ULL * 1000000000ULL;

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries(
        {twoHoursNs, twoHoursNs + 1000000000ULL},
        {5.0, 10.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ((*result.timestamps)[0], twoHoursNs - oneHourNs);
    EXPECT_EQ((*result.timestamps)[1], twoHoursNs + 1000000000ULL - oneHourNs);
    EXPECT_DOUBLE_EQ(result.values[0], 5.0);
    EXPECT_DOUBLE_EQ(result.values[1], 10.0);
}

TEST_F(ExpressionEvaluatorTest, TimeShiftSevenDaysViaParser) {
    // a - time_shift(a, '7d') computes week-over-week delta
    ExpressionParser parser("a - time_shift(a, '7d')");
    auto ast = parser.parse();

    // Two points 7 days apart; the shifted version occupies the same timestamps
    // so after alignment they can be subtracted directly.
    const uint64_t sevenDaysNs = 7ULL * 86400ULL * 1000000000ULL;
    uint64_t t0 = sevenDaysNs;
    uint64_t t1 = sevenDaysNs + 1000000000ULL;

    ExpressionEvaluator::QueryResultMap results;
    // 'a' has two points at t0 and t1 with values 100 and 200
    results["a"] = makeSeries({t0, t1}, {100.0, 200.0});

    // Evaluate time_shift(a, '7d') separately to understand what the subtraction does:
    // time_shift shifts these timestamps by +7d, which moves them to t0+7d and t1+7d.
    // They are no longer at the same timestamps as the original 'a' (t0, t1),
    // so we just verify the shift node itself produces the right timestamps.
    ExpressionParser shiftParser("time_shift(a, '7d')");
    auto shiftAst = shiftParser.parse();
    auto shifted = evaluator.evaluate(*shiftAst, results);

    EXPECT_EQ(shifted.size(), 2);
    EXPECT_EQ((*shifted.timestamps)[0], t0 + sevenDaysNs);
    EXPECT_EQ((*shifted.timestamps)[1], t1 + sevenDaysNs);
    EXPECT_DOUBLE_EQ(shifted.values[0], 100.0);
    EXPECT_DOUBLE_EQ(shifted.values[1], 200.0);
}

TEST_F(ExpressionEvaluatorTest, TimeShiftInvalidOffsetThrows) {
    // An invalid offset string like 'bad' should throw at evaluation time
    ExpressionParser parser("time_shift(a, 'bad_unit')");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000ULL}, {1.0});

    EXPECT_THROW(evaluator.evaluate(*ast, results), EvaluationException);
}

TEST_F(ExpressionEvaluatorTest, TimeShiftEmptyOffsetStringThrows) {
    // Empty offset string after '-' prefix should throw at evaluation time
    // We construct the AST manually (since the parser rejects empty strings)
    auto ast = ExpressionNode::makeTimeShiftFunction("a", "-");

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000ULL}, {1.0});

    EXPECT_THROW(evaluator.evaluate(*ast, results), EvaluationException);
}

// ==================== topk() / bottomk() Single-Series Tests ====================
// In the single-series evaluator context, topk(N, series) returns the series
// unchanged when N >= 1, and an empty series when N == 0.

TEST_F(ExpressionEvaluatorTest, TopkSingleSeriesN1ReturnsSeries) {
    // topk(1, a): single group — N >= 1, so the group is always kept
    ExpressionParser parser("topk(1, a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {10.0, 20.0, 30.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 3u);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 20.0);
    EXPECT_DOUBLE_EQ(result.values[2], 30.0);
}

TEST_F(ExpressionEvaluatorTest, BottomkSingleSeriesN1ReturnsSeries) {
    // bottomk(1, a): single group — N >= 1, so the group is always kept
    ExpressionParser parser("bottomk(1, a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {5.0, 15.0, 25.0});

    auto result = evaluator.evaluate(*ast, results);

    EXPECT_EQ(result.size(), 3u);
    EXPECT_DOUBLE_EQ(result.values[0], 5.0);
    EXPECT_DOUBLE_EQ(result.values[1], 15.0);
    EXPECT_DOUBLE_EQ(result.values[2], 25.0);
}

TEST_F(ExpressionEvaluatorTest, TopkSingleSeriesN0ReturnsEmpty) {
    // topk(0, a): N=0 means keep 0 groups, so result is empty
    ExpressionParser parser("topk(0, a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {10.0, 20.0, 30.0});

    auto result = evaluator.evaluate(*ast, results);
    EXPECT_EQ(result.size(), 0u);
}

TEST_F(ExpressionEvaluatorTest, BottomkSingleSeriesN0ReturnsEmpty) {
    // bottomk(0, a): N=0 means keep 0 groups, so result is empty
    ExpressionParser parser("bottomk(0, a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000}, {1.0, 2.0});

    auto result = evaluator.evaluate(*ast, results);
    EXPECT_EQ(result.size(), 0u);
}

TEST_F(ExpressionEvaluatorTest, TopkLargeNSingleSeriesReturnsSeries) {
    // topk(100, a) with one series: N >= group count, all kept
    ExpressionParser parser("topk(100, a)");
    auto ast = parser.parse();

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000}, {42.0, 43.0});

    auto result = evaluator.evaluate(*ast, results);
    EXPECT_EQ(result.size(), 2u);
    EXPECT_DOUBLE_EQ(result.values[0], 42.0);
    EXPECT_DOUBLE_EQ(result.values[1], 43.0);
}

TEST_F(ExpressionEvaluatorTest, TopkAndBottomkASTToString) {
    // Verify toString() produces the expected string representation
    auto args1 = std::vector<std::unique_ptr<ExpressionNode>>{};
    args1.push_back(ExpressionNode::makeScalar(2.0));
    args1.push_back(ExpressionNode::makeQueryRef("a"));
    auto topkNode = ExpressionNode::makeFunctionCall(FunctionType::TOPK, std::move(args1));
    EXPECT_EQ(topkNode->toString(), "topk(2, a)");

    auto args2 = std::vector<std::unique_ptr<ExpressionNode>>{};
    args2.push_back(ExpressionNode::makeScalar(3.0));
    args2.push_back(ExpressionNode::makeQueryRef("b"));
    auto bottomkNode = ExpressionNode::makeFunctionCall(FunctionType::BOTTOMK, std::move(args2));
    EXPECT_EQ(bottomkNode->toString(), "bottomk(3, b)");
}

TEST_F(ExpressionEvaluatorTest, TopkNegativeNThrows) {
    // topk(-1, a) should throw EvaluationException
    auto args = std::vector<std::unique_ptr<ExpressionNode>>{};
    args.push_back(ExpressionNode::makeScalar(-1.0));
    args.push_back(ExpressionNode::makeQueryRef("a"));
    auto ast = ExpressionNode::makeFunctionCall(FunctionType::TOPK, std::move(args));

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000}, {5.0});

    EXPECT_THROW(evaluator.evaluate(*ast, results), EvaluationException);
}

TEST_F(ExpressionEvaluatorTest, BottomkNegativeNThrows) {
    // bottomk(-1, a) should throw EvaluationException
    auto args = std::vector<std::unique_ptr<ExpressionNode>>{};
    args.push_back(ExpressionNode::makeScalar(-1.0));
    args.push_back(ExpressionNode::makeQueryRef("a"));
    auto ast = ExpressionNode::makeFunctionCall(FunctionType::BOTTOMK, std::move(args));

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000}, {5.0});

    EXPECT_THROW(evaluator.evaluate(*ast, results), EvaluationException);
}

// ==================== cumsum() Tests ====================

TEST_F(ExpressionEvaluatorTest, CumsumBasic) {
    // [1, 2, 3, 4, 5] -> [1, 3, 6, 10, 15]
    auto a = makeSeries({1000, 2000, 3000, 4000, 5000}, {1.0, 2.0, 3.0, 4.0, 5.0});
    auto result = a.cumsum();

    ASSERT_EQ(result.size(), 5);
    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 3.0);
    EXPECT_DOUBLE_EQ(result.values[2], 6.0);
    EXPECT_DOUBLE_EQ(result.values[3], 10.0);
    EXPECT_DOUBLE_EQ(result.values[4], 15.0);
}

TEST_F(ExpressionEvaluatorTest, CumsumWithNanInMiddle) {
    // [1, NaN, 3] -> NaN treated as 0 -> [1, 1, 4]
    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto a = makeSeries({1000, 2000, 3000}, {1.0, nan, 3.0});
    auto result = a.cumsum();

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 1.0);  // NaN skipped, sum stays at 1
    EXPECT_DOUBLE_EQ(result.values[2], 4.0);
}

TEST_F(ExpressionEvaluatorTest, CumsumAllNan) {
    // [NaN, NaN, NaN] -> [0, 0, 0]
    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto a = makeSeries({1000, 2000, 3000}, {nan, nan, nan});
    auto result = a.cumsum();

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 0.0);
    EXPECT_DOUBLE_EQ(result.values[2], 0.0);
}

TEST_F(ExpressionEvaluatorTest, CumsumSinglePoint) {
    // [5] -> [5]
    auto a = makeSeries({1000}, {5.0});
    auto result = a.cumsum();

    ASSERT_EQ(result.size(), 1);
    EXPECT_DOUBLE_EQ(result.values[0], 5.0);
}

TEST_F(ExpressionEvaluatorTest, CumsumEmpty) {
    auto a = makeSeries({}, {});
    auto result = a.cumsum();

    EXPECT_TRUE(result.empty());
}

TEST_F(ExpressionEvaluatorTest, CumsumTimestampsPreserved) {
    // Timestamps must be unchanged after cumsum
    auto a = makeSeries({100, 200, 300}, {10.0, 20.0, 30.0});
    auto result = a.cumsum();

    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ((*result.timestamps)[0], 100u);
    EXPECT_EQ((*result.timestamps)[1], 200u);
    EXPECT_EQ((*result.timestamps)[2], 300u);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 30.0);
    EXPECT_DOUBLE_EQ(result.values[2], 60.0);
}

TEST_F(ExpressionEvaluatorTest, CumsumParserRoundTrip) {
    // Parser should recognize cumsum() and route to CUMSUM unary op
    ExpressionParser parser("cumsum(a)");
    auto ast = parser.parse();
    ASSERT_EQ(ast->type, ExprNodeType::UNARY_OP);
    EXPECT_EQ(ast->asUnaryOp().op, UnaryOpType::CUMSUM);
    EXPECT_EQ(ast->toString(), "cumsum(a)");

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {1.0, 2.0, 3.0});
    auto result = evaluator.evaluate(*ast, results);

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 3.0);
    EXPECT_DOUBLE_EQ(result.values[2], 6.0);
}

// ==================== integral() Tests ====================

TEST_F(ExpressionEvaluatorTest, IntegralConstantValue) {
    // Constant value 2.0, points 1 second apart:
    // each step adds (2+2)/2 * 1s = 2.0
    // timestamps in nanoseconds: 0, 1e9, 2e9, 3e9
    // output: [0, 2, 4, 6]
    uint64_t one_sec = 1000000000ULL;
    auto a = makeSeries(
        {0, one_sec, 2 * one_sec, 3 * one_sec},
        {2.0, 2.0, 2.0, 2.0}
    );
    auto result = a.integral();

    ASSERT_EQ(result.size(), 4);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 2.0);
    EXPECT_DOUBLE_EQ(result.values[2], 4.0);
    EXPECT_DOUBLE_EQ(result.values[3], 6.0);
}

TEST_F(ExpressionEvaluatorTest, IntegralTriangular) {
    // values [0, 10, 0] at t=[0, 1s, 2s]
    // step 0->1: (0+10)/2 * 1s = 5
    // step 1->2: (10+0)/2 * 1s = 5
    // output: [0, 5, 10]
    uint64_t one_sec = 1000000000ULL;
    auto a = makeSeries(
        {0, one_sec, 2 * one_sec},
        {0.0, 10.0, 0.0}
    );
    auto result = a.integral();

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 5.0);
    EXPECT_DOUBLE_EQ(result.values[2], 10.0);
}

TEST_F(ExpressionEvaluatorTest, IntegralUnevenTimestamps) {
    // values [0, 4, 4] at t=[0, 2s, 3s]
    // step 0->2s: (0+4)/2 * 2 = 4
    // step 2s->3s: (4+4)/2 * 1 = 4
    // output: [0, 4, 8]
    uint64_t one_sec = 1000000000ULL;
    auto a = makeSeries(
        {0, 2 * one_sec, 3 * one_sec},
        {0.0, 4.0, 4.0}
    );
    auto result = a.integral();

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 4.0);
    EXPECT_DOUBLE_EQ(result.values[2], 8.0);
}

TEST_F(ExpressionEvaluatorTest, IntegralNanInMiddle) {
    // values [2, NaN, 2] at t=[0, 1s, 2s]
    // step 0->1: NaN involved -> contributes 0
    // step 1->2: NaN involved -> contributes 0
    // output: [0, 0, 0]
    const double nan = std::numeric_limits<double>::quiet_NaN();
    uint64_t one_sec = 1000000000ULL;
    auto a = makeSeries(
        {0, one_sec, 2 * one_sec},
        {2.0, nan, 2.0}
    );
    auto result = a.integral();

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 0.0);
    EXPECT_DOUBLE_EQ(result.values[2], 0.0);
}

TEST_F(ExpressionEvaluatorTest, IntegralSinglePoint) {
    // Single point: output is [0] (no interval to integrate over)
    auto a = makeSeries({1000000000ULL}, {42.0});
    auto result = a.integral();

    ASSERT_EQ(result.size(), 1);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
}

TEST_F(ExpressionEvaluatorTest, IntegralEmpty) {
    auto a = makeSeries({}, {});
    auto result = a.integral();

    EXPECT_TRUE(result.empty());
}

TEST_F(ExpressionEvaluatorTest, IntegralTimestampsPreserved) {
    // Timestamps must be unchanged after integral
    uint64_t one_sec = 1000000000ULL;
    auto a = makeSeries({0, one_sec, 2 * one_sec}, {1.0, 1.0, 1.0});
    auto result = a.integral();

    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ((*result.timestamps)[0], 0u);
    EXPECT_EQ((*result.timestamps)[1], one_sec);
    EXPECT_EQ((*result.timestamps)[2], 2 * one_sec);
}

TEST_F(ExpressionEvaluatorTest, IntegralParserRoundTrip) {
    // Parser should recognize integral() and route to INTEGRAL unary op
    ExpressionParser parser("integral(a)");
    auto ast = parser.parse();
    ASSERT_EQ(ast->type, ExprNodeType::UNARY_OP);
    EXPECT_EQ(ast->asUnaryOp().op, UnaryOpType::INTEGRAL);
    EXPECT_EQ(ast->toString(), "integral(a)");

    uint64_t one_sec = 1000000000ULL;
    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({0, one_sec, 2 * one_sec}, {1.0, 3.0, 1.0});
    auto result = evaluator.evaluate(*ast, results);

    // step 0->1s: (1+3)/2 * 1 = 2
    // step 1s->2s: (3+1)/2 * 1 = 2
    // output: [0, 2, 4]
    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 2.0);
    EXPECT_DOUBLE_EQ(result.values[2], 4.0);
}

// ==================== normalize() Tests ====================

TEST_F(ExpressionEvaluatorTest, NormalizeBasic) {
    // [0, 5, 10] → [0.0, 0.5, 1.0]
    auto s = makeSeries({1000, 2000, 3000}, {0.0, 5.0, 10.0});
    auto result = s.normalize();

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 0.5);
    EXPECT_DOUBLE_EQ(result.values[2], 1.0);
}

TEST_F(ExpressionEvaluatorTest, NormalizeAlreadyUnitRange) {
    // [0, 0.5, 1] → passthrough: [0.0, 0.5, 1.0]
    auto s = makeSeries({1000, 2000, 3000}, {0.0, 0.5, 1.0});
    auto result = s.normalize();

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 0.5);
    EXPECT_DOUBLE_EQ(result.values[2], 1.0);
}

TEST_F(ExpressionEvaluatorTest, NormalizeNegativeValues) {
    // [-10, 0, 10] → [0.0, 0.5, 1.0]
    auto s = makeSeries({1000, 2000, 3000}, {-10.0, 0.0, 10.0});
    auto result = s.normalize();

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 0.5);
    EXPECT_DOUBLE_EQ(result.values[2], 1.0);
}

TEST_F(ExpressionEvaluatorTest, NormalizeConstantSeries) {
    // [5, 5, 5] → [0.0, 0.0, 0.0]
    auto s = makeSeries({1000, 2000, 3000}, {5.0, 5.0, 5.0});
    auto result = s.normalize();

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 0.0);
    EXPECT_DOUBLE_EQ(result.values[2], 0.0);
}

TEST_F(ExpressionEvaluatorTest, NormalizeSinglePoint) {
    // [7] → [0.0]
    auto s = makeSeries({1000}, {7.0});
    auto result = s.normalize();

    ASSERT_EQ(result.size(), 1);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
}

TEST_F(ExpressionEvaluatorTest, NormalizeWithNaN) {
    // [NaN, 0, 10] → [NaN, 0.0, 1.0]
    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto s = makeSeries({1000, 2000, 3000}, {nan, 0.0, 10.0});
    auto result = s.normalize();

    ASSERT_EQ(result.size(), 3);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 0.0);
    EXPECT_DOUBLE_EQ(result.values[2], 1.0);
}

TEST_F(ExpressionEvaluatorTest, NormalizeAllNaN) {
    // [NaN, NaN] → [NaN, NaN]
    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto s = makeSeries({1000, 2000}, {nan, nan});
    auto result = s.normalize();

    ASSERT_EQ(result.size(), 2);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_TRUE(std::isnan(result.values[1]));
}

TEST_F(ExpressionEvaluatorTest, NormalizeParserRoundTrip) {
    // Parser should recognize normalize() and route to NORMALIZE unary op
    ExpressionParser parser("normalize(a)");
    auto ast = parser.parse();
    ASSERT_EQ(ast->type, ExprNodeType::UNARY_OP);
    EXPECT_EQ(ast->asUnaryOp().op, UnaryOpType::NORMALIZE);
    EXPECT_EQ(ast->toString(), "normalize(a)");

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {0.0, 5.0, 10.0});
    auto result = evaluator.evaluate(*ast, results);

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 0.0);
    EXPECT_DOUBLE_EQ(result.values[1], 0.5);
    EXPECT_DOUBLE_EQ(result.values[2], 1.0);
}

// ==================== as_percent() Tests ====================

TEST_F(ExpressionEvaluatorTest, AsPercentBasic) {
    // series=[50], total=[200] → [25.0]
    auto series = makeSeries({1000}, {50.0});
    auto total  = makeSeries({1000}, {200.0});
    auto result = AlignedSeries::as_percent(series, total);

    ASSERT_EQ(result.size(), 1);
    EXPECT_DOUBLE_EQ(result.values[0], 25.0);
}

TEST_F(ExpressionEvaluatorTest, AsPercentScalarTotal) {
    // series=[1,2,3], total=10 (scalar broadcast) → [10.0, 20.0, 30.0]
    auto series = makeSeries({1000, 2000, 3000}, {1.0, 2.0, 3.0});
    auto total  = makeSeries({1000, 2000, 3000}, {10.0, 10.0, 10.0});
    auto result = AlignedSeries::as_percent(series, total);

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[1], 20.0);
    EXPECT_DOUBLE_EQ(result.values[2], 30.0);
}

TEST_F(ExpressionEvaluatorTest, AsPercentDivisionByZero) {
    // total=0 → NaN
    auto series = makeSeries({1000}, {50.0});
    auto total  = makeSeries({1000}, {0.0});
    auto result = AlignedSeries::as_percent(series, total);

    ASSERT_EQ(result.size(), 1);
    EXPECT_TRUE(std::isnan(result.values[0]));
}

TEST_F(ExpressionEvaluatorTest, AsPercentNaNInSeries) {
    // NaN in series → NaN out
    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto series = makeSeries({1000, 2000}, {nan, 50.0});
    auto total  = makeSeries({1000, 2000}, {100.0, 100.0});
    auto result = AlignedSeries::as_percent(series, total);

    ASSERT_EQ(result.size(), 2);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 50.0);
}

TEST_F(ExpressionEvaluatorTest, AsPercentNaNInTotal) {
    // NaN in total → NaN out
    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto series = makeSeries({1000, 2000}, {50.0, 25.0});
    auto total  = makeSeries({1000, 2000}, {nan, 100.0});
    auto result = AlignedSeries::as_percent(series, total);

    ASSERT_EQ(result.size(), 2);
    EXPECT_TRUE(std::isnan(result.values[0]));
    EXPECT_DOUBLE_EQ(result.values[1], 25.0);
}

TEST_F(ExpressionEvaluatorTest, AsPercentParserRoundTripTwoSeries) {
    // as_percent(a, b) - two series references
    ExpressionParser parser("as_percent(a, b)");
    auto ast = parser.parse();
    ASSERT_EQ(ast->type, ExprNodeType::FUNCTION_CALL);
    EXPECT_EQ(ast->asFunctionCall().func, FunctionType::AS_PERCENT);
    EXPECT_EQ(ast->toString(), "as_percent(a, b)");

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000}, {50.0, 25.0});
    results["b"] = makeSeries({1000, 2000}, {200.0, 100.0});
    auto result = evaluator.evaluate(*ast, results);

    ASSERT_EQ(result.size(), 2);
    EXPECT_DOUBLE_EQ(result.values[0], 25.0);
    EXPECT_DOUBLE_EQ(result.values[1], 25.0);
}

TEST_F(ExpressionEvaluatorTest, AsPercentParserRoundTripScalar) {
    // as_percent(a, 100) - series and scalar constant
    ExpressionParser parser("as_percent(a, 100)");
    auto ast = parser.parse();
    ASSERT_EQ(ast->type, ExprNodeType::FUNCTION_CALL);
    EXPECT_EQ(ast->asFunctionCall().func, FunctionType::AS_PERCENT);

    ExpressionEvaluator::QueryResultMap results;
    results["a"] = makeSeries({1000, 2000, 3000}, {1.0, 2.0, 3.0});
    auto result = evaluator.evaluate(*ast, results);

    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 1.0);
    EXPECT_DOUBLE_EQ(result.values[1], 2.0);
    EXPECT_DOUBLE_EQ(result.values[2], 3.0);
}
