#include "streaming_derived_evaluator.hpp"

#include "expression_parser.hpp"

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

TEST(StreamingDerivedEvalTest, SimpleSubtraction) {
    // Formula: a - b
    ExpressionParser parser("a - b");
    auto ast = parser.parse();

    std::map<std::string, AggregationMethod> methods = {{"a", AggregationMethod::AVG}, {"b", AggregationMethod::AVG}};

    StreamingDerivedEvaluator eval(10'000'000'000ULL, methods, std::shared_ptr<ExpressionNode>(std::move(ast)));

    // Add points for query "a" and "b" in the same bucket
    eval.addPoint("a", makePoint("cpu", "usage", {}, 1'000'000'000, 90.0));
    eval.addPoint("a", makePoint("cpu", "usage", {}, 2'000'000'000, 80.0));
    eval.addPoint("b", makePoint("mem", "used", {}, 1'000'000'000, 40.0));
    eval.addPoint("b", makePoint("mem", "used", {}, 2'000'000'000, 60.0));

    auto batch = eval.closeBuckets();
    ASSERT_EQ(batch.points.size(), 1u);
    // avg(a) = 85, avg(b) = 50, result = 85 - 50 = 35
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[0].value), 35.0);
    EXPECT_EQ(batch.points[0].measurement, "derived");
    EXPECT_EQ(batch.points[0].field, "value");
}

TEST(StreamingDerivedEvalTest, DivisionFormula) {
    // Formula: a / b * 100 (percentage)
    ExpressionParser parser("a / b * 100");
    auto ast = parser.parse();

    std::map<std::string, AggregationMethod> methods = {{"a", AggregationMethod::SUM}, {"b", AggregationMethod::SUM}};

    StreamingDerivedEvaluator eval(10'000'000'000ULL, methods, std::shared_ptr<ExpressionNode>(std::move(ast)));

    eval.addPoint("a", makePoint("cpu", "usage", {}, 1'000'000'000, 30.0));
    eval.addPoint("a", makePoint("cpu", "usage", {}, 2'000'000'000, 20.0));
    eval.addPoint("b", makePoint("total", "cap", {}, 1'000'000'000, 100.0));

    auto batch = eval.closeBuckets();
    ASSERT_EQ(batch.points.size(), 1u);
    // sum(a) = 50, sum(b) = 100, result = 50 / 100 * 100 = 50
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[0].value), 50.0);
}

TEST(StreamingDerivedEvalTest, MultipleBuckets) {
    // Formula: a + b
    ExpressionParser parser("a + b");
    auto ast = parser.parse();

    std::map<std::string, AggregationMethod> methods = {{"a", AggregationMethod::AVG}, {"b", AggregationMethod::AVG}};

    StreamingDerivedEvaluator eval(5'000'000'000ULL, methods, std::shared_ptr<ExpressionNode>(std::move(ast)));

    // Bucket [0, 5s)
    eval.addPoint("a", makePoint("cpu", "usage", {}, 1'000'000'000, 10.0));
    eval.addPoint("b", makePoint("mem", "used", {}, 1'000'000'000, 20.0));

    // Bucket [5s, 10s)
    eval.addPoint("a", makePoint("cpu", "usage", {}, 7'000'000'000, 30.0));
    eval.addPoint("b", makePoint("mem", "used", {}, 7'000'000'000, 40.0));

    auto batch = eval.closeBuckets();
    ASSERT_EQ(batch.points.size(), 2u);

    // Sort by timestamp to verify
    std::sort(batch.points.begin(), batch.points.end(),
              [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[0].value), 30.0);  // 10 + 20
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[1].value), 70.0);  // 30 + 40
}

TEST(StreamingDerivedEvalTest, CarryForwardMissingData) {
    // Formula: a - b, where "b" has no data in second bucket
    ExpressionParser parser("a - b");
    auto ast = parser.parse();

    std::map<std::string, AggregationMethod> methods = {{"a", AggregationMethod::AVG}, {"b", AggregationMethod::AVG}};

    StreamingDerivedEvaluator eval(5'000'000'000ULL, methods, std::shared_ptr<ExpressionNode>(std::move(ast)));

    // First emission: both queries have data
    eval.addPoint("a", makePoint("cpu", "usage", {}, 1'000'000'000, 100.0));
    eval.addPoint("b", makePoint("mem", "used", {}, 1'000'000'000, 40.0));

    auto batch1 = eval.closeBuckets();
    ASSERT_EQ(batch1.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(batch1.points[0].value), 60.0);  // 100 - 40

    // Second emission: only "a" has data, "b" should carry forward 40
    eval.addPoint("a", makePoint("cpu", "usage", {}, 6'000'000'000, 80.0));

    auto batch2 = eval.closeBuckets();
    ASSERT_EQ(batch2.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(batch2.points[0].value), 40.0);  // 80 - 40 (carry-forward)
}

TEST(StreamingDerivedEvalTest, EmptyWhenNoData) {
    ExpressionParser parser("a + b");
    auto ast = parser.parse();

    std::map<std::string, AggregationMethod> methods = {{"a", AggregationMethod::AVG}, {"b", AggregationMethod::AVG}};

    StreamingDerivedEvaluator eval(10'000'000'000ULL, methods, std::shared_ptr<ExpressionNode>(std::move(ast)));

    EXPECT_FALSE(eval.hasData());
    auto batch = eval.closeBuckets();
    EXPECT_TRUE(batch.points.empty());
}

TEST(StreamingDerivedEvalTest, HasDataFlag) {
    ExpressionParser parser("a + b");
    auto ast = parser.parse();

    std::map<std::string, AggregationMethod> methods = {{"a", AggregationMethod::AVG}, {"b", AggregationMethod::AVG}};

    StreamingDerivedEvaluator eval(10'000'000'000ULL, methods, std::shared_ptr<ExpressionNode>(std::move(ast)));

    EXPECT_FALSE(eval.hasData());

    eval.addPoint("a", makePoint("cpu", "usage", {}, 1'000'000'000, 50.0));
    EXPECT_TRUE(eval.hasData());

    eval.closeBuckets();
    EXPECT_FALSE(eval.hasData());
}

TEST(StreamingDerivedEvalTest, ThreeQueryFormula) {
    // Formula: (a + b) / c
    ExpressionParser parser("(a + b) / c");
    auto ast = parser.parse();

    std::map<std::string, AggregationMethod> methods = {
        {"a", AggregationMethod::AVG}, {"b", AggregationMethod::AVG}, {"c", AggregationMethod::AVG}};

    StreamingDerivedEvaluator eval(10'000'000'000ULL, methods, std::shared_ptr<ExpressionNode>(std::move(ast)));

    eval.addPoint("a", makePoint("cpu", "user", {}, 1'000'000'000, 30.0));
    eval.addPoint("b", makePoint("cpu", "sys", {}, 1'000'000'000, 20.0));
    eval.addPoint("c", makePoint("cpu", "total", {}, 1'000'000'000, 100.0));

    auto batch = eval.closeBuckets();
    ASSERT_EQ(batch.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[0].value), 0.5);  // (30+20)/100
}

TEST(StreamingDerivedEvalTest, UnaryFunctionOnCrossQuery) {
    // Formula: abs(a - b)
    ExpressionParser parser("abs(a - b)");
    auto ast = parser.parse();

    std::map<std::string, AggregationMethod> methods = {{"a", AggregationMethod::AVG}, {"b", AggregationMethod::AVG}};

    StreamingDerivedEvaluator eval(10'000'000'000ULL, methods, std::shared_ptr<ExpressionNode>(std::move(ast)));

    eval.addPoint("a", makePoint("temp", "indoor", {}, 1'000'000'000, 20.0));
    eval.addPoint("b", makePoint("temp", "outdoor", {}, 1'000'000'000, 35.0));

    auto batch = eval.closeBuckets();
    ASSERT_EQ(batch.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(batch.points[0].value), 15.0);  // abs(20 - 35)
}

// Test: string values fed into a derived formula should produce NaN, not 0.0.
// A string series cannot be meaningfully used in a numeric formula; the
// evaluator must propagate NaN rather than silently substituting 0.0.
TEST(StreamingDerivedEvalTest, StringValueProducesNaNNotZero) {
    // Formula: a + b, where "b" receives a string value
    ExpressionParser parser("a + b");
    auto ast = parser.parse();

    std::map<std::string, AggregationMethod> methods = {{"a", AggregationMethod::AVG}, {"b", AggregationMethod::AVG}};

    StreamingDerivedEvaluator eval(10'000'000'000ULL, methods, std::shared_ptr<ExpressionNode>(std::move(ast)));

    eval.addPoint("a", makePoint("cpu", "usage", {}, 1'000'000'000, 42.0));

    // Inject a string value for "b" directly via StreamingDataPoint
    StreamingDataPoint stringPt;
    stringPt.measurement = "sensor";
    stringPt.field = "label";
    stringPt.timestamp = 1'000'000'000;
    stringPt.value = std::string("active");  // string variant — non-numeric
    eval.addPoint("b", stringPt);

    auto batch = eval.closeBuckets();
    // The string "active" cannot be numerically evaluated; the result must be NaN.
    // Before the fix it was silently 0.0 (42 + 0 = 42), which is wrong.
    ASSERT_EQ(batch.points.size(), 1u);
    double result = std::get<double>(batch.points[0].value);
    EXPECT_TRUE(std::isnan(result)) << "Expected NaN for formula involving string value, got: " << result;
}

// Test: when EvaluationException is thrown during formula evaluation (e.g. formula
// references a variable not present in queryMethods), the result should contain NaN
// points — one per timestamp — rather than a silently empty batch.
//
// Before the fix, the catch block discarded the entire batch and returned empty,
// making it impossible for callers to distinguish "no data" from "formula error".
// After the fix, each timestamp that was computed produces a NaN data point.
TEST(StreamingDerivedEvalTest, EvaluationExceptionProducesNaNNotEmptyBatch) {
    // Formula references "c", but queryMethods only provides "a" and "b".
    // The evaluator will throw EvaluationException("Query 'c' not found in results").
    ExpressionParser parser("a + c");
    auto ast = parser.parse();

    std::map<std::string, AggregationMethod> methods = {{"a", AggregationMethod::AVG}, {"b", AggregationMethod::AVG}};

    StreamingDerivedEvaluator eval(10'000'000'000ULL, methods, std::shared_ptr<ExpressionNode>(std::move(ast)));

    eval.addPoint("a", makePoint("cpu", "usage", {}, 1'000'000'000, 42.0));
    eval.addPoint("b", makePoint("mem", "used", {}, 1'000'000'000, 10.0));

    auto batch = eval.closeBuckets();

    // The exception must NOT silently swallow the batch.
    // Each computed timestamp should yield a NaN result point.
    ASSERT_EQ(batch.points.size(), 1u)
        << "EvaluationException silently swallowed: expected 1 NaN point, got empty batch";
    double result = std::get<double>(batch.points[0].value);
    EXPECT_TRUE(std::isnan(result)) << "Expected NaN for unresolvable formula variable, got: " << result;
}

// Regression test: when _intervalNs is very large (> UINT64_MAX / kCarryForwardMaxIntervals),
// the multiplication kCarryForwardMaxIntervals * _intervalNs overflows uint64_t, wrapping to a
// small value. Without the saturating-multiply fix the staleness cutoff becomes nonsensical,
// treating a perfectly fresh carry-forward value as stale (NaN).
//
// Arithmetic of the bug with intervalNs = UINT64_MAX/10 + 1 = 1844674407370955162:
//   10 * intervalNs mod 2^64 = 4  (wraps to near-zero)
//   latestTs (bucket-1 start = intervalNs) > 4 → true
//   staleCutoff (buggy) = intervalNs - 4 = 1844674407370955158
//   lastRealTs for "b" = 0 (bucket-0 start)
//   0 < 1844674407370955158 → "b" treated as STALE → NaN
// With the fix (saturating multiply, product = UINT64_MAX):
//   latestTs (intervalNs) > UINT64_MAX → false → staleCutoff = 0
//   0 >= 0 → "b" treated as FRESH → carry-forward 40.0 is used correctly
TEST(StreamingDerivedEvalTest, CarryForwardStalenessOverflowSafety) {
    // UINT64_MAX/10 + 1 = 1844674407370955162.
    // 10 * this value wraps mod 2^64 to just 4, triggering the overflow bug.
    const uint64_t overflowInterval = UINT64_MAX / 10 + 1;

    ExpressionParser parser("a - b");
    auto ast = parser.parse();

    std::map<std::string, AggregationMethod> methods = {{"a", AggregationMethod::AVG}, {"b", AggregationMethod::AVG}};

    StreamingDerivedEvaluator eval(overflowInterval, methods, std::shared_ptr<ExpressionNode>(std::move(ast)));

    // First emission: both "a" and "b" have real data in bucket 0 (timestamp 0).
    // bucketStart(0) = 0 for any intervalNs; lastRealTs for "b" will be recorded as 0.
    eval.addPoint("a", makePoint("cpu", "usage", {}, 0ULL, 100.0));
    eval.addPoint("b", makePoint("mem", "used", {}, 0ULL, 40.0));
    auto batch1 = eval.closeBuckets();
    ASSERT_EQ(batch1.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(batch1.points[0].value), 60.0);  // 100 - 40

    // Second emission: only "a" has data, placed in bucket 1 (timestamp = overflowInterval,
    // so bucketStart = overflowInterval).  "b" has no new data — must carry forward 40.
    //
    // In the second closeBuckets():
    //   latestTs = overflowInterval (= 1844674407370955162)
    //   lastRealTs["b"] = 0  (recorded from batch 1)
    //
    // Buggy path: 10 * overflowInterval wraps to 4.
    //   latestTs (1844674407370955162) > 4 → true
    //   staleCutoff = 1844674407370955162 - 4 = 1844674407370955158
    //   0 (lastRealTs) < 1844674407370955158 → "b" is stale → NaN
    //   Result: 80 - NaN = NaN  (test fails on isnan check)
    //
    // Fixed path: product saturates to UINT64_MAX.
    //   latestTs (1844674407370955162) > UINT64_MAX → false → staleCutoff = 0
    //   0 (lastRealTs) >= 0 → "b" is fresh → carry-forward 40.0
    //   Result: 80 - 40 = 40.0
    eval.addPoint("a", makePoint("cpu", "usage", {}, overflowInterval, 80.0));
    auto batch2 = eval.closeBuckets();
    ASSERT_EQ(batch2.points.size(), 1u);
    double val = std::get<double>(batch2.points[0].value);
    EXPECT_FALSE(std::isnan(val)) << "carry-forward should not expire with huge interval: "
                                     "10 * intervalNs overflows uint64_t to 4, making staleCutoff nearly equal to "
                                     "latestTs and wrongly expiring a fresh carry-forward value";
    EXPECT_DOUBLE_EQ(val, 40.0);  // 80 - 40 (carry-forward of "b")
}
