#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "functions/smoothing_functions.hpp"
#include "functions/function_registry.hpp"
#include <vector>
#include <cmath>
#include <random>

using namespace tsdb::functions;
using ::testing::_;
using ::testing::DoubleNear;
using ::testing::Each;
using ::testing::Gt;
using ::testing::Le;

class SmoothingFunctionsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clear registry for clean tests
        FunctionRegistry::getInstance().clear();
        
        // Register smoothing functions
        FunctionRegistry::getInstance().registerFunction<SMAFunction>(SMAFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<EMAFunction>(EMAFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<GaussianSmoothFunction>(GaussianSmoothFunction::metadata_);
        
        setupTestData();
    }
    
    void TearDown() override {
        FunctionRegistry::getInstance().clear();
    }
    
    void setupTestData() {
        // Linear increasing data
        for (int i = 0; i < 20; ++i) {
            linearTimestamps.push_back(i * 1000ULL);
            linearValues.push_back(static_cast<double>(i));
        }
        
        // Noisy sine wave data
        std::mt19937 gen(12345);  // Fixed seed for reproducibility
        std::normal_distribution<double> noise(0.0, 0.1);
        
        for (int i = 0; i < 100; ++i) {
            noisySineTimestamps.push_back(i * 100ULL);
            double cleanValue = std::sin(i * 0.1);
            noisySineValues.push_back(cleanValue + noise(gen));
        }
        
        // Step function data
        for (int i = 0; i < 30; ++i) {
            stepTimestamps.push_back(i * 1000ULL);
            stepValues.push_back(i < 10 ? 1.0 : (i < 20 ? 5.0 : 2.0));
        }
        
        // Data with outliers
        for (int i = 0; i < 15; ++i) {
            outlierTimestamps.push_back(i * 1000ULL);
            double baseValue = static_cast<double>(i);
            // Add outliers at specific positions
            if (i == 5 || i == 10) {
                outlierValues.push_back(baseValue + 50.0);  // Large outlier
            } else {
                outlierValues.push_back(baseValue);
            }
        }
        
        // Constant data
        constantTimestamps = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
        constantValues = {5.0, 5.0, 5.0, 5.0, 5.0};
        
        // Single data point
        singleTimestamps = {1000ULL};
        singleValues = {42.0};
    }
    
    FunctionContext createContext(const std::map<std::string, ParameterValue>& params = {}) {
        FunctionContext context;
        for (const auto& [key, value] : params) {
            context.setParameter(key, value);
        }
        return context;
    }
    
    void expectVectorNear(const std::vector<double>& actual, 
                         const std::vector<double>& expected, 
                         double tolerance = 1e-10) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            if (std::isnan(expected[i])) {
                EXPECT_TRUE(std::isnan(actual[i])) << "at index " << i;
            } else {
                EXPECT_NEAR(actual[i], expected[i], tolerance) << "at index " << i;
            }
        }
    }
    
    // Test data
    std::vector<uint64_t> linearTimestamps, noisySineTimestamps, stepTimestamps;
    std::vector<uint64_t> outlierTimestamps, constantTimestamps, singleTimestamps;
    std::vector<double> linearValues, noisySineValues, stepValues;
    std::vector<double> outlierValues, constantValues, singleValues;
};

// Test SMA Function
TEST_F(SmoothingFunctionsTest, SMAFunctionBasic) {
    SMAFunction smaFunc;
    DoubleSeriesView series(&linearTimestamps, &linearValues);
    FunctionContext context = createContext({{"window", int64_t(5)}});
    
    auto result = smaFunc.execute(series, context).get();
    
    // With window=5 and truncate edge handling, we expect 20-4=16 output points
    EXPECT_EQ(result.size(), 16);
    
    // First SMA value (index 4): avg of [0,1,2,3,4] = 2.0
    EXPECT_NEAR(result.values[0], 2.0, 1e-10);
    
    // Last SMA value: avg of [15,16,17,18,19] = 17.0  
    EXPECT_NEAR(result.values[15], 17.0, 1e-10);
    
    // Middle value (index 5): avg of [1,2,3,4,5] = 3.0
    EXPECT_NEAR(result.values[1], 3.0, 1e-10);
}

TEST_F(SmoothingFunctionsTest, SMAFunctionWindow1) {
    SMAFunction smaFunc;
    DoubleSeriesView series(&linearTimestamps, &linearValues);
    FunctionContext context = createContext({{"window", int64_t(1)}});
    
    auto result = smaFunc.execute(series, context).get();
    
    // Window=1 should return the original series
    EXPECT_EQ(result.size(), linearValues.size());
    expectVectorNear(result.values, linearValues);
}

TEST_F(SmoothingFunctionsTest, SMAFunctionConstantData) {
    SMAFunction smaFunc;
    DoubleSeriesView series(&constantTimestamps, &constantValues);
    FunctionContext context = createContext({{"window", int64_t(3)}});
    
    auto result = smaFunc.execute(series, context).get();
    
    // SMA of constant data should be constant
    EXPECT_EQ(result.size(), 3);  // 5-2=3 with truncate
    EXPECT_THAT(result.values, Each(DoubleNear(5.0, 1e-10)));
}

TEST_F(SmoothingFunctionsTest, SMAFunctionWindowTooLarge) {
    SMAFunction smaFunc;
    DoubleSeriesView series(&constantTimestamps, &constantValues);  // 5 points
    FunctionContext context = createContext({{"window", int64_t(10)}});  // Window > data size
    
    EXPECT_THROW(smaFunc.execute(series, context).get(), InsufficientDataException);
}

TEST_F(SmoothingFunctionsTest, SMAFunctionEdgeHandling) {
    SMAFunction smaFunc;
    DoubleSeriesView series(&constantTimestamps, &constantValues);
    
    // Test pad_nearest edge handling
    FunctionContext context = createContext({
        {"window", int64_t(3)},
        {"edge_handling", std::string("pad_nearest")}
    });
    
    auto result = smaFunc.execute(series, context).get();
    
    // With pad_nearest, we should get the full original length
    EXPECT_EQ(result.size(), constantValues.size());
    EXPECT_THAT(result.values, Each(DoubleNear(5.0, 1e-10)));
}

// Test EMA Function
TEST_F(SmoothingFunctionsTest, EMAFunctionBasic) {
    EMAFunction emaFunc;
    DoubleSeriesView series(&linearTimestamps, &linearValues);
    FunctionContext context = createContext({{"alpha", 0.5}});
    
    auto result = emaFunc.execute(series, context).get();
    
    EXPECT_EQ(result.size(), linearValues.size());
    
    // First value should be the same as input
    EXPECT_NEAR(result.values[0], 0.0, 1e-10);
    
    // Second value: 0.5 * 1.0 + 0.5 * 0.0 = 0.5
    EXPECT_NEAR(result.values[1], 0.5, 1e-10);
    
    // Third value: 0.5 * 2.0 + 0.5 * 0.5 = 1.25
    EXPECT_NEAR(result.values[2], 1.25, 1e-10);
}

TEST_F(SmoothingFunctionsTest, EMAFunctionFromWindow) {
    EMAFunction emaFunc;
    DoubleSeriesView series(&linearTimestamps, &linearValues);
    FunctionContext context = createContext({{"window", int64_t(9)}});
    
    auto result = emaFunc.execute(series, context).get();
    
    EXPECT_EQ(result.size(), linearValues.size());
    
    // Alpha should be 2/(9+1) = 0.2
    double expectedAlpha = 0.2;
    
    // First value unchanged
    EXPECT_NEAR(result.values[0], 0.0, 1e-10);
    
    // Second value: 0.2 * 1.0 + 0.8 * 0.0 = 0.2
    EXPECT_NEAR(result.values[1], 0.2, 1e-10);
}

TEST_F(SmoothingFunctionsTest, EMAFunctionFromHalfLife) {
    EMAFunction emaFunc;
    DoubleSeriesView series(&linearTimestamps, &linearValues);
    FunctionContext context = createContext({{"half_life", 5.0}});
    
    auto result = emaFunc.execute(series, context).get();
    
    EXPECT_EQ(result.size(), linearValues.size());
    // Alpha = 1 - exp(-ln(2)/5) ≈ 0.1295
    // Just verify it runs and produces reasonable values
    EXPECT_GE(result.values.back(), 0.0);
    EXPECT_LE(result.values.back(), 20.0);
}

TEST_F(SmoothingFunctionsTest, EMAFunctionSinglePoint) {
    EMAFunction emaFunc;
    DoubleSeriesView series(&singleTimestamps, &singleValues);
    FunctionContext context = createContext({{"alpha", 0.3}});
    
    auto result = emaFunc.execute(series, context).get();
    
    EXPECT_EQ(result.size(), 1);
    EXPECT_NEAR(result.values[0], 42.0, 1e-10);
}

// Test GaussianSmoothFunction
TEST_F(SmoothingFunctionsTest, GaussianSmoothFunctionBasic) {
    GaussianSmoothFunction gaussianFunc;
    DoubleSeriesView series(&stepTimestamps, &stepValues);
    FunctionContext context = createContext({
        {"window", int64_t(7)},
        {"sigma", 1.0}
    });
    
    auto result = gaussianFunc.execute(series, context).get();
    
    // Gaussian smoothing should produce smoothed transitions
    EXPECT_GT(result.size(), 0);
    
    // Values should be smoothed - no sharp edges
    if (result.size() > 10) {
        // The step function should be smoothed at the transition from 1.0 to 5.0 (around index 9-10)
        // stepValues[9] = 1.0, stepValues[10] = 5.0, so |1.0 - 5.0| = 4.0
        // The smoothed values should have a smaller difference due to Gaussian blur
        EXPECT_LT(std::abs(result.values[9] - result.values[10]), 
                 std::abs(stepValues[9] - stepValues[10]));
    }
}

TEST_F(SmoothingFunctionsTest, GaussianSmoothFunctionSymmetric) {
    GaussianSmoothFunction gaussianFunc;
    
    // Create symmetric data around center
    std::vector<uint64_t> symTimestamps = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
    std::vector<double> symValues = {1.0, 2.0, 5.0, 2.0, 1.0};  // Symmetric peak
    
    DoubleSeriesView series(&symTimestamps, &symValues);
    FunctionContext context = createContext({
        {"window", int64_t(5)},
        {"sigma", 1.0}
    });
    
    auto result = gaussianFunc.execute(series, context).get();
    
    EXPECT_GT(result.size(), 0);
    // Peak should still be at center but smoothed
    if (result.size() >= 3) {
        size_t center = result.size() / 2;
        EXPECT_GE(result.values[center], result.values[0]);
        EXPECT_GE(result.values[center], result.values.back());
    }
}

// Removed SmoothFunction and MedianFilterFunction tests as these classes are not implemented

// Removed smoothing utilities tests as these utility functions are not implemented

// Test parameter validation
TEST_F(SmoothingFunctionsTest, ParameterValidation) {
    // Test SMA Function parameter validation
    SMAFunction smaFunc;
    
    // Valid SMA parameters
    FunctionContext validSMAContext = createContext({
        {"window", int64_t(5)},
        {"edge_handling", std::string("truncate")}
    });
    EXPECT_TRUE(smaFunc.validateParameters(validSMAContext).get());
    
    // Invalid SMA window size
    FunctionContext invalidSMAWindow = createContext({{"window", int64_t(0)}});
    EXPECT_FALSE(smaFunc.validateParameters(invalidSMAWindow).get());
    
    FunctionContext tooLargeSMAWindow = createContext({{"window", int64_t(1001)}});
    EXPECT_FALSE(smaFunc.validateParameters(tooLargeSMAWindow).get());
    
    // Invalid SMA edge handling
    FunctionContext invalidSMAEdge = createContext({
        {"window", int64_t(5)},
        {"edge_handling", std::string("invalid")}
    });
    EXPECT_FALSE(smaFunc.validateParameters(invalidSMAEdge).get());
    
    // Test EMA Function parameter validation
    EMAFunction emaFunc;
    
    // Valid EMA parameters - alpha
    FunctionContext validEMAAlpha = createContext({{"alpha", 0.5}});
    EXPECT_TRUE(emaFunc.validateParameters(validEMAAlpha).get());
    
    // Valid EMA parameters - window
    FunctionContext validEMAWindow = createContext({{"window", int64_t(10)}});
    EXPECT_TRUE(emaFunc.validateParameters(validEMAWindow).get());
    
    // Valid EMA parameters - half_life
    FunctionContext validEMAHalfLife = createContext({{"half_life", 5.0}});
    EXPECT_TRUE(emaFunc.validateParameters(validEMAHalfLife).get());
    
    // Invalid EMA alpha - too small
    FunctionContext invalidEMAAlphaSmall = createContext({{"alpha", 0.0}});
    EXPECT_FALSE(emaFunc.validateParameters(invalidEMAAlphaSmall).get());
    
    // Invalid EMA alpha - too large
    FunctionContext invalidEMAAlphaLarge = createContext({{"alpha", 1.5}});
    EXPECT_FALSE(emaFunc.validateParameters(invalidEMAAlphaLarge).get());
    
    // Invalid EMA window - zero
    FunctionContext invalidEMAWindow = createContext({{"window", int64_t(0)}});
    EXPECT_FALSE(emaFunc.validateParameters(invalidEMAWindow).get());
    
    // Invalid EMA half_life - negative
    FunctionContext invalidEMAHalfLife = createContext({{"half_life", -1.0}});
    EXPECT_FALSE(emaFunc.validateParameters(invalidEMAHalfLife).get());
    
    // Multiple parameters - should fail
    FunctionContext multipleEMAParams = createContext({
        {"alpha", 0.5},
        {"window", int64_t(10)}
    });
    EXPECT_FALSE(emaFunc.validateParameters(multipleEMAParams).get());
    
    // Test Gaussian Function parameter validation
    GaussianSmoothFunction gaussianFunc;
    
    // Valid Gaussian parameters - sigma
    FunctionContext validGaussianSigma = createContext({{"sigma", 1.5}});
    EXPECT_TRUE(gaussianFunc.validateParameters(validGaussianSigma).get());
    
    // Valid Gaussian parameters - window (odd)
    FunctionContext validGaussianWindow = createContext({{"window", int64_t(7)}});
    EXPECT_TRUE(gaussianFunc.validateParameters(validGaussianWindow).get());
    
    // Invalid Gaussian sigma - zero
    FunctionContext invalidGaussianSigma = createContext({{"sigma", 0.0}});
    EXPECT_FALSE(gaussianFunc.validateParameters(invalidGaussianSigma).get());
    
    // Invalid Gaussian sigma - negative
    FunctionContext negativeGaussianSigma = createContext({{"sigma", -1.0}});
    EXPECT_FALSE(gaussianFunc.validateParameters(negativeGaussianSigma).get());
    
    // Invalid Gaussian window - zero
    FunctionContext invalidGaussianWindow = createContext({{"window", int64_t(0)}});
    EXPECT_FALSE(gaussianFunc.validateParameters(invalidGaussianWindow).get());
    
    // Invalid Gaussian window - even
    FunctionContext evenGaussianWindow = createContext({{"window", int64_t(6)}});
    EXPECT_FALSE(gaussianFunc.validateParameters(evenGaussianWindow).get());
    
    // Multiple Gaussian parameters - should fail
    FunctionContext multipleGaussianParams = createContext({
        {"sigma", 1.0},
        {"window", int64_t(7)}
    });
    EXPECT_FALSE(gaussianFunc.validateParameters(multipleGaussianParams).get());
}

// Test function metadata
TEST_F(SmoothingFunctionsTest, FunctionMetadata) {
    SMAFunction smaFunc;
    const auto& metadata = smaFunc.getMetadata();
    
    EXPECT_EQ(metadata.name, "sma");
    EXPECT_EQ(metadata.category, FunctionCategory::SMOOTHING);
    EXPECT_EQ(metadata.outputType, "double");
    EXPECT_TRUE(metadata.supportsVectorization);
    EXPECT_TRUE(metadata.supportsStreaming);
    EXPECT_GE(metadata.minDataPoints, 1);
    EXPECT_FALSE(metadata.examples.empty());
    
    // Check for window parameter
    bool hasWindowParam = false;
    for (const auto& param : metadata.parameters) {
        if (param.name == "window") {
            hasWindowParam = true;
            EXPECT_EQ(param.type, "int");
            EXPECT_FALSE(param.required);  // Should have default
        }
    }
    EXPECT_TRUE(hasWindowParam);
}

// Test edge cases and error conditions
TEST_F(SmoothingFunctionsTest, EdgeCasesEmptyData) {
    SMAFunction smaFunc;
    std::vector<uint64_t> emptyTimestamps;
    std::vector<double> emptyValues;
    DoubleSeriesView emptySeries(&emptyTimestamps, &emptyValues);
    FunctionContext context = createContext({{"window", int64_t(3)}});
    
    EXPECT_THROW(smaFunc.execute(emptySeries, context).get(), InsufficientDataException);
}

TEST_F(SmoothingFunctionsTest, EdgeCasesNaNHandling) {
    SMAFunction smaFunc;
    std::vector<uint64_t> nanTimestamps = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
    std::vector<double> nanValues = {1.0, std::numeric_limits<double>::quiet_NaN(), 3.0, 4.0, 5.0};
    DoubleSeriesView series(&nanTimestamps, &nanValues);
    FunctionContext context = createContext({{"window", int64_t(3)}});
    
    auto result = smaFunc.execute(series, context).get();
    
    // Should handle NaN values appropriately
    EXPECT_GT(result.size(), 0);
    // Implementation should either skip NaN or handle them gracefully
}

// Performance test
TEST_F(SmoothingFunctionsTest, PerformanceLargeDataset) {
    // Create large noisy dataset
    const size_t dataSize = 10000;
    std::vector<uint64_t> largeTimestamps;
    std::vector<double> largeValues;
    
    std::mt19937 gen(12345);
    std::normal_distribution<double> noise(0.0, 1.0);
    
    for (size_t i = 0; i < dataSize; ++i) {
        largeTimestamps.push_back(i * 1000ULL);
        largeValues.push_back(std::sin(i * 0.01) + noise(gen));
    }
    
    SMAFunction smaFunc;
    DoubleSeriesView series(&largeTimestamps, &largeValues);
    FunctionContext context = createContext({{"window", int64_t(50)}});
    
    auto start = std::chrono::high_resolution_clock::now();
    auto result = smaFunc.execute(series, context).get();
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    EXPECT_GT(result.size(), 0);
    // Performance should be reasonable (less than 1 second for 10k points)
    EXPECT_LT(duration.count(), 1000);
    
    // Verify smoothing effectiveness
    if (result.size() > 100) {
        // Calculate variance of original vs smoothed in middle section
        double originalVar = 0.0, smoothedVar = 0.0;
        size_t startIdx = result.size() / 4;
        size_t endIdx = 3 * result.size() / 4;
        
        // This is a simplified variance calculation for demonstration
        for (size_t i = startIdx; i < endIdx - 1; ++i) {
            originalVar += std::pow(largeValues[i+25] - largeValues[i+24], 2);  // Offset for window
            smoothedVar += std::pow(result.values[i+1] - result.values[i], 2);
        }
        
        // Smoothed data should have lower variance (less noisy)
        EXPECT_LT(smoothedVar, originalVar);
    }
}

// Test that pad_zeros mode always divides by the full window size (treating missing
// boundary values as 0), rather than dividing by the actual number of available values.
// If the divisor were validCount (actual count) instead of windowSize, edge positions
// would return the average of available data (same as shrink mode) rather than the
// zero-padded average.
TEST_F(SmoothingFunctionsTest, SMAFunctionPadZerosDivisorIsWindowSize) {
    SMAFunction smaFunc;

    // Use all-10s input so the difference between sum/window and sum/validCount is clear.
    std::vector<uint64_t> ts = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
    std::vector<double>   vs = {10.0,    10.0,    10.0,    10.0,    10.0};
    DoubleSeriesView series(&ts, &vs);

    FunctionContext context = createContext({
        {"window", int64_t(4)},
        {"edge_handling", std::string("pad_zeros")}
    });

    auto result = smaFunc.execute(series, context).get();

    // pad_zeros always produces the same number of output points as input.
    ASSERT_EQ(result.size(), vs.size());

    // With window=4 and pad_zeros, missing positions are treated as 0:
    //   i=0: (10+0+0+0)/4 = 2.5
    //   i=1: (10+10+0+0)/4 = 5.0
    //   i=2: (10+10+10+0)/4 = 7.5
    //   i=3: (10+10+10+10)/4 = 10.0
    //   i=4: (10+10+10+10)/4 = 10.0
    //
    // If the code mistakenly divided by validCount instead of window:
    //   i=0: 10/1=10.0, i=1: 20/2=10.0, i=2: 30/3=10.0  (all equal to input)
    // That would be the "shrink" / "truncate" behaviour, NOT pad_zeros.
    EXPECT_NEAR(result.values[0],  2.5, 1e-10) << "i=0: sum=10, window=4, expect 10/4=2.5";
    EXPECT_NEAR(result.values[1],  5.0, 1e-10) << "i=1: sum=20, window=4, expect 20/4=5.0";
    EXPECT_NEAR(result.values[2],  7.5, 1e-10) << "i=2: sum=30, window=4, expect 30/4=7.5";
    EXPECT_NEAR(result.values[3], 10.0, 1e-10) << "i=3: sum=40, window=4, expect 40/4=10.0";
    EXPECT_NEAR(result.values[4], 10.0, 1e-10) << "i=4: sum=40, window=4, expect 40/4=10.0";
}

// Contrast: pad_nearest must give constant output for constant input because the
// padded boundary values equal the nearest real value, so sum/validCount==sum/window.
TEST_F(SmoothingFunctionsTest, SMAFunctionPadNearestDivisorEquivalent) {
    SMAFunction smaFunc;

    std::vector<uint64_t> ts = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
    std::vector<double>   vs = {10.0,    10.0,    10.0,    10.0,    10.0};
    DoubleSeriesView series(&ts, &vs);

    FunctionContext context = createContext({
        {"window", int64_t(4)},
        {"edge_handling", std::string("pad_nearest")}
    });

    auto result = smaFunc.execute(series, context).get();
    ASSERT_EQ(result.size(), vs.size());

    // pad_nearest pads with the nearest real value (10.0), so every window of 4
    // contains four 10s regardless of position; the average is always 10.0.
    for (size_t i = 0; i < result.values.size(); ++i) {
        EXPECT_NEAR(result.values[i], 10.0, 1e-10) << "at index " << i;
    }
}

// Integration test with registry
TEST_F(SmoothingFunctionsTest, RegistryIntegration) {
    auto& registry = FunctionRegistry::getInstance();
    
    // Test that smoothing functions are registered
    EXPECT_TRUE(registry.hasFunction("sma"));
    EXPECT_TRUE(registry.hasFunction("ema"));
    
    // Test function creation through registry
    auto smaFunc = registry.createFunction("sma");
    EXPECT_NE(smaFunc, nullptr);
    EXPECT_EQ(smaFunc->getName(), "sma");
    
    // Test category filtering
    auto smoothingFuncs = registry.getFunctionsByCategory(FunctionCategory::SMOOTHING);
    EXPECT_GE(smoothingFuncs.size(), 2);
    
    // Test that we can execute through registry-created function
    DoubleSeriesView series(&linearTimestamps, &linearValues);
    FunctionContext context = createContext({{"window", int64_t(5)}});
    
    // Cast to unary function for execution
    auto unaryFunc = dynamic_cast<IUnaryFunction*>(smaFunc.get());
    ASSERT_NE(unaryFunc, nullptr);
    
    auto result = unaryFunc->execute(series, context).get();
    EXPECT_GT(result.size(), 0);
}

// main() function removed to avoid multiple definitions when linking with other test files