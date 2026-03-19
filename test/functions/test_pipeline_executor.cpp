#include "functions/function_pipeline_executor.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cmath>
#include <map>
#include <string>
#include <variant>
#include <vector>

using namespace timestar::functions;
using ParamMap = std::map<std::string, std::variant<int64_t, double, std::string>>;

class PipelineExecutorTest : public ::testing::Test {
protected:
    // Engine can be nullptr for the inline SMA/EMA/add/multiply paths.
    FunctionPipelineExecutor executor_{nullptr};

    void expectVectorNear(const std::vector<double>& actual, const std::vector<double>& expected,
                          double tolerance = 1e-10) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            EXPECT_NEAR(actual[i], expected[i], tolerance) << "at index " << i;
        }
    }
};

// ---------------------------------------------------------------------------
// SMA pipeline
// ---------------------------------------------------------------------------

TEST_F(PipelineExecutorTest, SMAWindow3) {
    std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};
    ParamMap params = {{"window", int64_t(3)}};

    executor_.executeFunction("sma", params, data).get();

    // window=3 on [1,2,3,4,5]:
    //   result[2] = (1+2+3)/3 = 2.0
    //   result[3] = (2+3+4)/3 = 3.0
    //   result[4] = (3+4+5)/3 = 4.0
    //   leading positions [0],[1] are filled with result[2] = 2.0
    ASSERT_EQ(data.size(), 5u);
    EXPECT_NEAR(data[0], 2.0, 1e-10);
    EXPECT_NEAR(data[1], 2.0, 1e-10);
    EXPECT_NEAR(data[2], 2.0, 1e-10);
    EXPECT_NEAR(data[3], 3.0, 1e-10);
    EXPECT_NEAR(data[4], 4.0, 1e-10);
}

TEST_F(PipelineExecutorTest, SMADefaultWindow) {
    // When no window parameter is given, the default window is 3.
    std::vector<double> data = {10.0, 20.0, 30.0, 40.0, 50.0};
    ParamMap params;

    executor_.executeFunction("sma", params, data).get();

    // Same as window=3: first valid = (10+20+30)/3 = 20.0
    ASSERT_EQ(data.size(), 5u);
    EXPECT_NEAR(data[0], 20.0, 1e-10);
    EXPECT_NEAR(data[1], 20.0, 1e-10);
    EXPECT_NEAR(data[2], 20.0, 1e-10);
    EXPECT_NEAR(data[3], 30.0, 1e-10);
    EXPECT_NEAR(data[4], 40.0, 1e-10);
}

TEST_F(PipelineExecutorTest, SMAWindowClampedTo1000) {
    // Windows larger than 1000 are clamped to 1000.
    // With only 5 data points and window=1001 (clamped to 1000), data.size() < window,
    // so data should be returned unchanged.
    std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};
    std::vector<double> original = data;
    ParamMap params = {{"window", int64_t(1500)}};

    executor_.executeFunction("sma", params, data).get();

    expectVectorNear(data, original);
}

// ---------------------------------------------------------------------------
// EMA pipeline
// ---------------------------------------------------------------------------

TEST_F(PipelineExecutorTest, EMAWithAlpha) {
    std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};
    ParamMap params = {{"alpha", 0.5}};

    executor_.executeFunction("ema", params, data).get();

    // EMA with alpha=0.5:
    //   ema[0] = 1.0
    //   ema[1] = 0.5*2 + 0.5*1.0 = 1.5
    //   ema[2] = 0.5*3 + 0.5*1.5 = 2.25
    //   ema[3] = 0.5*4 + 0.5*2.25 = 3.125
    //   ema[4] = 0.5*5 + 0.5*3.125 = 4.0625
    ASSERT_EQ(data.size(), 5u);
    EXPECT_NEAR(data[0], 1.0, 1e-10);
    EXPECT_NEAR(data[1], 1.5, 1e-10);
    EXPECT_NEAR(data[2], 2.25, 1e-10);
    EXPECT_NEAR(data[3], 3.125, 1e-10);
    EXPECT_NEAR(data[4], 4.0625, 1e-10);
}

TEST_F(PipelineExecutorTest, EMAWithWindow) {
    // When alpha is not given (or invalid), EMA derives alpha from window:
    //   alpha = 2 / (window + 1)
    // window=3 => alpha = 2/4 = 0.5
    std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};
    ParamMap params = {{"window", int64_t(3)}};

    executor_.executeFunction("ema", params, data).get();

    ASSERT_EQ(data.size(), 5u);
    EXPECT_NEAR(data[0], 1.0, 1e-10);
    EXPECT_NEAR(data[1], 1.5, 1e-10);
    EXPECT_NEAR(data[2], 2.25, 1e-10);
    EXPECT_NEAR(data[3], 3.125, 1e-10);
    EXPECT_NEAR(data[4], 4.0625, 1e-10);
}

TEST_F(PipelineExecutorTest, EMASinglePoint) {
    std::vector<double> data = {42.0};
    ParamMap params = {{"alpha", 0.3}};

    executor_.executeFunction("ema", params, data).get();

    ASSERT_EQ(data.size(), 1u);
    EXPECT_NEAR(data[0], 42.0, 1e-10);
}

// ---------------------------------------------------------------------------
// Add pipeline
// ---------------------------------------------------------------------------

TEST_F(PipelineExecutorTest, AddConstant) {
    std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};
    ParamMap params = {{"value", 10.0}};

    executor_.executeFunction("add", params, data).get();

    std::vector<double> expected = {11.0, 12.0, 13.0, 14.0, 15.0};
    expectVectorNear(data, expected);
}

TEST_F(PipelineExecutorTest, AddNegativeValue) {
    std::vector<double> data = {10.0, 20.0, 30.0};
    ParamMap params = {{"value", -5.0}};

    executor_.executeFunction("add", params, data).get();

    std::vector<double> expected = {5.0, 15.0, 25.0};
    expectVectorNear(data, expected);
}

TEST_F(PipelineExecutorTest, AddNoValueParam) {
    // If "value" parameter is missing, data should be unchanged.
    std::vector<double> data = {1.0, 2.0, 3.0};
    std::vector<double> original = data;
    ParamMap params;

    executor_.executeFunction("add", params, data).get();

    expectVectorNear(data, original);
}

// ---------------------------------------------------------------------------
// Multiply pipeline
// ---------------------------------------------------------------------------

TEST_F(PipelineExecutorTest, MultiplyByFactor) {
    std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};
    ParamMap params = {{"factor", 2.0}};

    executor_.executeFunction("multiply", params, data).get();

    std::vector<double> expected = {2.0, 4.0, 6.0, 8.0, 10.0};
    expectVectorNear(data, expected);
}

TEST_F(PipelineExecutorTest, MultiplyByZero) {
    std::vector<double> data = {1.0, 2.0, 3.0};
    ParamMap params = {{"factor", 0.0}};

    executor_.executeFunction("multiply", params, data).get();

    std::vector<double> expected = {0.0, 0.0, 0.0};
    expectVectorNear(data, expected);
}

TEST_F(PipelineExecutorTest, MultiplyByNegative) {
    std::vector<double> data = {1.0, -2.0, 3.0};
    ParamMap params = {{"factor", -0.5}};

    executor_.executeFunction("multiply", params, data).get();

    std::vector<double> expected = {-0.5, 1.0, -1.5};
    expectVectorNear(data, expected);
}

TEST_F(PipelineExecutorTest, MultiplyNoFactorParam) {
    // If "factor" parameter is missing, data should be unchanged.
    std::vector<double> data = {1.0, 2.0, 3.0};
    std::vector<double> original = data;
    ParamMap params;

    executor_.executeFunction("multiply", params, data).get();

    expectVectorNear(data, original);
}

// ---------------------------------------------------------------------------
// Multi-step pipeline (chaining)
// ---------------------------------------------------------------------------

TEST_F(PipelineExecutorTest, SMAThenAdd) {
    std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};

    // Step 1: SMA with window=3
    ParamMap smaParams = {{"window", int64_t(3)}};
    executor_.executeFunction("sma", smaParams, data).get();

    // After SMA(3): [2.0, 2.0, 2.0, 3.0, 4.0]

    // Step 2: Add 10
    ParamMap addParams = {{"value", 10.0}};
    executor_.executeFunction("add", addParams, data).get();

    // After add(10): [12.0, 12.0, 12.0, 13.0, 14.0]
    ASSERT_EQ(data.size(), 5u);
    EXPECT_NEAR(data[0], 12.0, 1e-10);
    EXPECT_NEAR(data[1], 12.0, 1e-10);
    EXPECT_NEAR(data[2], 12.0, 1e-10);
    EXPECT_NEAR(data[3], 13.0, 1e-10);
    EXPECT_NEAR(data[4], 14.0, 1e-10);
}

TEST_F(PipelineExecutorTest, MultiplyThenEMA) {
    std::vector<double> data = {2.0, 4.0, 6.0, 8.0, 10.0};

    // Step 1: multiply by 0.5 => [1,2,3,4,5]
    ParamMap mulParams = {{"factor", 0.5}};
    executor_.executeFunction("multiply", mulParams, data).get();

    std::vector<double> afterMul = {1.0, 2.0, 3.0, 4.0, 5.0};
    expectVectorNear(data, afterMul);

    // Step 2: EMA with alpha=0.5
    ParamMap emaParams = {{"alpha", 0.5}};
    executor_.executeFunction("ema", emaParams, data).get();

    ASSERT_EQ(data.size(), 5u);
    EXPECT_NEAR(data[0], 1.0, 1e-10);
    EXPECT_NEAR(data[1], 1.5, 1e-10);
    EXPECT_NEAR(data[2], 2.25, 1e-10);
    EXPECT_NEAR(data[3], 3.125, 1e-10);
    EXPECT_NEAR(data[4], 4.0625, 1e-10);
}

TEST_F(PipelineExecutorTest, ThreeStepChain) {
    std::vector<double> data = {10.0, 20.0, 30.0, 40.0, 50.0};

    // Step 1: add -10 => [0, 10, 20, 30, 40]
    executor_.executeFunction("add", ParamMap{{"value", -10.0}}, data).get();

    // Step 2: multiply by 0.1 => [0, 1, 2, 3, 4]
    executor_.executeFunction("multiply", ParamMap{{"factor", 0.1}}, data).get();

    // Step 3: add 100 => [100, 101, 102, 103, 104]
    executor_.executeFunction("add", ParamMap{{"value", 100.0}}, data).get();

    std::vector<double> expected = {100.0, 101.0, 102.0, 103.0, 104.0};
    expectVectorNear(data, expected);
}

// ---------------------------------------------------------------------------
// Empty data
// ---------------------------------------------------------------------------

TEST_F(PipelineExecutorTest, EmptyDataSMA) {
    std::vector<double> data;
    ParamMap params = {{"window", int64_t(3)}};

    // Must not crash on empty input.
    executor_.executeFunction("sma", params, data).get();

    EXPECT_TRUE(data.empty());
}

TEST_F(PipelineExecutorTest, EmptyDataEMA) {
    std::vector<double> data;
    ParamMap params = {{"alpha", 0.5}};

    executor_.executeFunction("ema", params, data).get();

    EXPECT_TRUE(data.empty());
}

TEST_F(PipelineExecutorTest, EmptyDataAdd) {
    std::vector<double> data;
    ParamMap params = {{"value", 10.0}};

    executor_.executeFunction("add", params, data).get();

    EXPECT_TRUE(data.empty());
}

TEST_F(PipelineExecutorTest, EmptyDataMultiply) {
    std::vector<double> data;
    ParamMap params = {{"factor", 2.0}};

    executor_.executeFunction("multiply", params, data).get();

    EXPECT_TRUE(data.empty());
}

// ---------------------------------------------------------------------------
// Unknown function
// ---------------------------------------------------------------------------

TEST_F(PipelineExecutorTest, UnknownFunctionReturnsDataUnchanged) {
    std::vector<double> data = {1.0, 2.0, 3.0};
    std::vector<double> original = data;
    ParamMap params;

    // "nonexistent_function_xyz" is not a known inline function nor in the registry.
    // The executor should log a warning and return data unchanged.
    executor_.executeFunction("nonexistent_function_xyz", params, data).get();

    expectVectorNear(data, original);
}

TEST_F(PipelineExecutorTest, UnknownFunctionWithParams) {
    std::vector<double> data = {10.0, 20.0, 30.0, 40.0};
    std::vector<double> original = data;
    ParamMap params = {{"window", int64_t(5)}, {"alpha", 0.3}};

    executor_.executeFunction("totally_bogus", params, data).get();

    expectVectorNear(data, original);
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

TEST_F(PipelineExecutorTest, SMAWindowLargerThanData) {
    // window=10 but only 5 data points => data returned unchanged
    std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};
    std::vector<double> original = data;
    ParamMap params = {{"window", int64_t(10)}};

    executor_.executeFunction("sma", params, data).get();

    expectVectorNear(data, original);
}

TEST_F(PipelineExecutorTest, SMAWindowEqualsDataSize) {
    // window=5 with 5 data points => exactly one valid average, leading filled
    std::vector<double> data = {2.0, 4.0, 6.0, 8.0, 10.0};
    ParamMap params = {{"window", int64_t(5)}};

    executor_.executeFunction("sma", params, data).get();

    // avg = (2+4+6+8+10)/5 = 6.0 for all positions (leading fill + one valid)
    ASSERT_EQ(data.size(), 5u);
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_NEAR(data[i], 6.0, 1e-10) << "at index " << i;
    }
}

TEST_F(PipelineExecutorTest, SMAWindowZero) {
    // window=0 should leave data unchanged (guard: window > 0)
    std::vector<double> data = {1.0, 2.0, 3.0};
    std::vector<double> original = data;
    ParamMap params = {{"window", int64_t(0)}};

    executor_.executeFunction("sma", params, data).get();

    expectVectorNear(data, original);
}

TEST_F(PipelineExecutorTest, EMAInvalidAlphaFallsBackToWindow) {
    // alpha=0.0 is invalid (<= 0), so EMA falls back to default window=3 => alpha = 2/(3+1) = 0.5
    std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};
    ParamMap params = {{"alpha", 0.0}};

    executor_.executeFunction("ema", params, data).get();

    // Should behave like alpha=0.5 (from default window=3)
    ASSERT_EQ(data.size(), 5u);
    EXPECT_NEAR(data[0], 1.0, 1e-10);
    EXPECT_NEAR(data[1], 1.5, 1e-10);
    EXPECT_NEAR(data[2], 2.25, 1e-10);
}

TEST_F(PipelineExecutorTest, EMAAlphaExceedsOneFallsBackToWindow) {
    // alpha=1.5 is invalid (> 1.0), so EMA falls back to default window=3 => alpha = 0.5
    std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};
    ParamMap params = {{"alpha", 1.5}};

    executor_.executeFunction("ema", params, data).get();

    ASSERT_EQ(data.size(), 5u);
    EXPECT_NEAR(data[0], 1.0, 1e-10);
    EXPECT_NEAR(data[1], 1.5, 1e-10);
}
