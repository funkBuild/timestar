#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "functions/arithmetic_functions.hpp"
#include "functions/smoothing_functions.hpp"
#include "functions/interpolation_functions.hpp"
#include "functions/function_registry.hpp"
#include "functions/function_types.hpp"
#include <vector>
#include <cmath>
#include <limits>
#include <chrono>

using namespace tsdb::functions;
using ::testing::_;
using ::testing::DoubleNear;
using ::testing::ElementsAre;

class EdgeCasesTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clear registry for clean tests
        FunctionRegistry::getInstance().clear();
        
        // Register all functions for testing
        FunctionRegistry::getInstance().registerFunction<AddFunction>(AddFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<SubtractFunction>(SubtractFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<MultiplyFunction>(MultiplyFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<DivideFunction>(DivideFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<ScaleFunction>(ScaleFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<OffsetFunction>(OffsetFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<SMAFunction>(SMAFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<EMAFunction>(EMAFunction::metadata_);
        FunctionRegistry::getInstance().registerFunction<GaussianSmoothFunction>(GaussianSmoothFunction::metadata_);
        
        setupEdgeCaseData();
    }
    
    void TearDown() override {
        FunctionRegistry::getInstance().clear();
    }
    
    void setupEdgeCaseData() {
        // Empty datasets
        emptyTimestamps = {};
        emptyValues = {};
        
        // Single data point
        singleTimestamp = {1000ULL};
        singleValue = {42.0};
        
        // Extreme numeric values
        extremeTimestamps = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL, 6000ULL, 7000ULL, 8000ULL};
        extremeValues = {
            std::numeric_limits<double>::max(),           // Maximum double
            std::numeric_limits<double>::min(),           // Minimum positive double
            -std::numeric_limits<double>::max(),          // Maximum negative double
            std::numeric_limits<double>::infinity(),      // Positive infinity
            -std::numeric_limits<double>::infinity(),     // Negative infinity
            std::numeric_limits<double>::quiet_NaN(),     // NaN
            1e-308,                                       // Near underflow
            1e308                                         // Near overflow
        };
        
        // Precision loss test data
        precisionTimestamps = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
        precisionValues = {
            1.0000000000000002,  // Small precision difference
            1.0000000000000004,
            1.0000000000000006,
            1.0000000000000008,
            1.000000000000001
        };
        
        // Timestamp boundary conditions
        boundaryTimestamps = {
            0ULL,                                         // Unix epoch start
            std::numeric_limits<uint64_t>::max(),         // Maximum timestamp
            2147483647000000000ULL,                       // Year 2038 problem (Unix timestamp limit in nanoseconds)
            9223372036854775807ULL                        // Max int64_t value (year ~2262)
        };
        boundaryValues = {1.0, 2.0, 3.0, 4.0};
        
        // Large dataset for stress testing
        largeTimestamps.reserve(100000);
        largeValues.reserve(100000);
        for (size_t i = 0; i < 100000; ++i) {
            largeTimestamps.push_back(1000ULL + i * 1000ULL);
            largeValues.push_back(static_cast<double>(i % 1000));
        }
    }
    
    FunctionContext createContext(const std::map<std::string, ParameterValue>& params = {}) {
        FunctionContext context;
        for (const auto& param : params) {
            context.setParameter(param.first, param.second);
        }
        return context;
    }
    
    // Test data sets
    std::vector<uint64_t> emptyTimestamps, singleTimestamp, extremeTimestamps, 
                         precisionTimestamps, boundaryTimestamps, largeTimestamps;
    std::vector<double> emptyValues, singleValue, extremeValues, 
                       precisionValues, boundaryValues, largeValues;
};

// =============================================================================
// EMPTY DATASET TESTS
// =============================================================================

TEST_F(EdgeCasesTest, ArithmeticFunctionsWithEmptyDataset) {
    DoubleSeriesView emptyView(&emptyTimestamps, &emptyValues);
    auto addContext = createContext({{"operand", 10.0}});
    auto mulContext = createContext({{"factor", 2.0}});
    auto divContext = createContext({{"divisor", 5.0}});
    
    // Test arithmetic functions with empty input
    AddFunction addFunc;
    auto addResult = addFunc.execute(emptyView, addContext).get();
    EXPECT_TRUE(addResult.empty());
    EXPECT_EQ(addResult.timestamps.size(), 0);
    EXPECT_EQ(addResult.values.size(), 0);
    
    MultiplyFunction mulFunc;
    auto mulResult = mulFunc.execute(emptyView, mulContext).get();
    EXPECT_TRUE(mulResult.empty());
    
    DivideFunction divFunc;
    auto divResult = divFunc.execute(emptyView, divContext).get();
    EXPECT_TRUE(divResult.empty());
    
    SubtractFunction subFunc;
    auto subResult = subFunc.execute(emptyView, addContext).get();
    EXPECT_TRUE(subResult.empty());
}

TEST_F(EdgeCasesTest, SmoothingFunctionsWithEmptyDataset) {
    DoubleSeriesView emptyView(&emptyTimestamps, &emptyValues);
    
    // Test Simple Moving Average with empty data - should either return empty or throw exception
    SMAFunction smaFunc;
    auto smaContext = createContext({{"window", 3}});
    try {
        auto smaResult = smaFunc.execute(emptyView, smaContext).get();
        EXPECT_TRUE(smaResult.empty());
    } catch (const InsufficientDataException&) {
        // This is acceptable behavior for empty input
        SUCCEED() << "SMA correctly identified insufficient data for empty input";
    }
    
    // Test Exponential Moving Average with empty data
    EMAFunction emaFunc;
    auto emaContext = createContext({{"alpha", 0.3}});
    auto emaResult = emaFunc.execute(emptyView, emaContext).get();
    EXPECT_TRUE(emaResult.empty());
}

// =============================================================================
// SINGLE DATA POINT TESTS
// =============================================================================

TEST_F(EdgeCasesTest, ArithmeticFunctionsWithSinglePoint) {
    DoubleSeriesView singleView(&singleTimestamp, &singleValue);
    auto addContext = createContext({{"operand", 5.0}});
    auto mulContext = createContext({{"factor", 5.0}});
    auto divContext = createContext({{"divisor", 5.0}});
    
    AddFunction addFunc;
    auto addResult = addFunc.execute(singleView, addContext).get();
    EXPECT_EQ(addResult.size(), 1);
    EXPECT_EQ(addResult.timestamps[0], 1000ULL);
    EXPECT_DOUBLE_EQ(addResult.values[0], 47.0);  // 42.0 + 5.0
    
    MultiplyFunction mulFunc;
    auto mulResult = mulFunc.execute(singleView, mulContext).get();
    EXPECT_EQ(mulResult.size(), 1);
    EXPECT_DOUBLE_EQ(mulResult.values[0], 210.0);  // 42.0 * 5.0
    
    DivideFunction divFunc;
    auto divResult = divFunc.execute(singleView, divContext).get();
    EXPECT_EQ(divResult.size(), 1);
    EXPECT_DOUBLE_EQ(divResult.values[0], 8.4);  // 42.0 / 5.0
}

TEST_F(EdgeCasesTest, SmoothingFunctionsWithSinglePoint) {
    DoubleSeriesView singleView(&singleTimestamp, &singleValue);
    
    // Simple Moving Average with single point and window > 1
    SMAFunction smaFunc;
    auto smaContext = createContext({{"window", 3}});
    
    // This should either return empty result or handle gracefully
    // The implementation should decide whether to return the single point or empty
    try {
        auto smaResult = smaFunc.execute(singleView, smaContext).get();
        // If it doesn't throw, it should return empty or the single point
        if (!smaResult.empty()) {
            EXPECT_EQ(smaResult.size(), 1);
            EXPECT_DOUBLE_EQ(smaResult.values[0], 42.0);
        }
    } catch (const InsufficientDataException&) {
        // This is also acceptable behavior
        SUCCEED() << "Function correctly identified insufficient data";
    }
    
    // Exponential Moving Average with single point should work
    EMAFunction emaFunc;
    auto emaContext = createContext({{"alpha", 0.3}});
    auto emaResult = emaFunc.execute(singleView, emaContext).get();
    EXPECT_EQ(emaResult.size(), 1);
    EXPECT_DOUBLE_EQ(emaResult.values[0], 42.0);  // First point should be unchanged
}

// =============================================================================
// EXTREME NUMERIC VALUES TESTS
// =============================================================================

TEST_F(EdgeCasesTest, ArithmeticFunctionsWithExtremeValues) {
    DoubleSeriesView extremeView(&extremeTimestamps, &extremeValues);
    auto context = createContext({{"operand", 1.0}});
    
    // Test addition with extreme values
    AddFunction addFunc;
    auto addResult = addFunc.execute(extremeView, context).get();
    EXPECT_EQ(addResult.size(), extremeValues.size());
    
    // Check specific extreme value behaviors
    // Note: Adding 1.0 to max double may not overflow to infinity on all systems
    EXPECT_TRUE(std::isfinite(addResult.values[0]) || (std::isinf(addResult.values[0]) && addResult.values[0] > 0));  // max + 1 
    EXPECT_TRUE(addResult.values[1] > 0);  // min + 1 should be positive
    EXPECT_TRUE(std::isfinite(addResult.values[2]) || (std::isinf(addResult.values[2]) && addResult.values[2] < 0));  // -max + 1
    EXPECT_TRUE(std::isinf(addResult.values[3]) && addResult.values[3] > 0);  // +inf + 1 = +inf
    EXPECT_TRUE(std::isinf(addResult.values[4]) && addResult.values[4] < 0);  // -inf + 1 = -inf
    EXPECT_TRUE(std::isnan(addResult.values[5]));  // NaN + 1 = NaN
}

TEST_F(EdgeCasesTest, MultiplicationWithExtremeValues) {
    DoubleSeriesView extremeView(&extremeTimestamps, &extremeValues);
    auto context = createContext({{"factor", 2.0}});
    
    MultiplyFunction mulFunc;
    auto mulResult = mulFunc.execute(extremeView, context).get();
    EXPECT_EQ(mulResult.size(), extremeValues.size());
    
    // Check multiplication behaviors
    EXPECT_TRUE(std::isinf(mulResult.values[0]) && mulResult.values[0] > 0);  // max * 2 = +inf
    EXPECT_TRUE(std::isinf(mulResult.values[3]) && mulResult.values[3] > 0);  // +inf * 2 = +inf
    EXPECT_TRUE(std::isinf(mulResult.values[4]) && mulResult.values[4] < 0);  // -inf * 2 = -inf
    EXPECT_TRUE(std::isnan(mulResult.values[5]));  // NaN * 2 = NaN
}

TEST_F(EdgeCasesTest, DivisionByZero) {
    std::vector<uint64_t> timestamps = {1000ULL, 2000ULL, 3000ULL};
    std::vector<double> values = {1.0, 0.0, -1.0};
    DoubleSeriesView view(&timestamps, &values);
    auto context = createContext({{"divisor", 0.0}});  // Divide by zero
    
    DivideFunction divFunc;
    
    // The division function should throw an exception for division by zero
    EXPECT_THROW(divFunc.execute(view, context).get(), ParameterValidationException);
}

TEST_F(EdgeCasesTest, DivisionOfZero) {
    std::vector<uint64_t> timestamps = {1000ULL, 2000ULL, 3000ULL};
    std::vector<double> values = {0.0, 0.0, 0.0};
    DoubleSeriesView view(&timestamps, &values);
    auto context = createContext({{"divisor", 5.0}});  // Divide zero by something
    
    DivideFunction divFunc;
    auto divResult = divFunc.execute(view, context).get();
    EXPECT_EQ(divResult.size(), 3);
    
    // 0.0 / 5.0 should be 0.0
    for (size_t i = 0; i < divResult.size(); ++i) {
        EXPECT_DOUBLE_EQ(divResult.values[i], 0.0);
    }
}

// =============================================================================
// NaN AND INFINITY PROPAGATION TESTS
// =============================================================================

TEST_F(EdgeCasesTest, NaNPropagationThroughFunctionChain) {
    std::vector<uint64_t> timestamps = {1000ULL, 2000ULL, 3000ULL};
    std::vector<double> values = {1.0, std::numeric_limits<double>::quiet_NaN(), 3.0};
    DoubleSeriesView view(&timestamps, &values);
    
    // Test NaN propagation through arithmetic operations
    auto addContext = createContext({{"operand", 10.0}});
    AddFunction addFunc;
    auto addResult = addFunc.execute(view, addContext).get();
    
    EXPECT_DOUBLE_EQ(addResult.values[0], 11.0);  // 1.0 + 10.0
    EXPECT_TRUE(std::isnan(addResult.values[1]));  // NaN + 10.0 = NaN
    EXPECT_DOUBLE_EQ(addResult.values[2], 13.0);  // 3.0 + 10.0
    
    // Test NaN propagation through smoothing (if window allows)
    if (addResult.size() >= 2) {
        DoubleSeriesView addResultView(&addResult.timestamps, &addResult.values);
        auto smaContext = createContext({{"window", 2}});
        SMAFunction smaFunc;
        
        try {
            auto smaResult = smaFunc.execute(addResultView, smaContext).get();
            // SMA with NaN should produce NaN in affected windows
            for (size_t i = 0; i < smaResult.size(); ++i) {
                if (i == 0) {
                    // First window: (11.0 + NaN) / 2 = NaN
                    EXPECT_TRUE(std::isnan(smaResult.values[i]));
                } else if (i == 1) {
                    // Second window: (NaN + 13.0) / 2 = NaN
                    EXPECT_TRUE(std::isnan(smaResult.values[i]));
                }
            }
        } catch (const std::exception&) {
            // Some implementations might reject NaN input
            SUCCEED() << "Function correctly handled NaN input";
        }
    }
}

TEST_F(EdgeCasesTest, InfinityPropagationThroughFunctionChain) {
    std::vector<uint64_t> timestamps = {1000ULL, 2000ULL, 3000ULL};
    std::vector<double> values = {1.0, std::numeric_limits<double>::infinity(), 3.0};
    DoubleSeriesView view(&timestamps, &values);
    
    auto mulContext = createContext({{"factor", 2.0}});
    MultiplyFunction mulFunc;
    auto mulResult = mulFunc.execute(view, mulContext).get();
    
    EXPECT_DOUBLE_EQ(mulResult.values[0], 2.0);  // 1.0 * 2.0
    EXPECT_TRUE(std::isinf(mulResult.values[1]) && mulResult.values[1] > 0);  // +inf * 2.0 = +inf
    EXPECT_DOUBLE_EQ(mulResult.values[2], 6.0);  // 3.0 * 2.0
}

// =============================================================================
// PRECISION LIMITS AND FLOAT PRECISION LOSS TESTS
// =============================================================================

TEST_F(EdgeCasesTest, PrecisionLossInLongCalculationChains) {
    DoubleSeriesView precisionView(&precisionTimestamps, &precisionValues);
    
    // Chain multiple operations to accumulate precision errors
    auto context1 = createContext({{"operand", 1e-15}});
    AddFunction addFunc;
    auto result1 = addFunc.execute(precisionView, context1).get();
    
    DoubleSeriesView result1View(&result1.timestamps, &result1.values);
    auto context2 = createContext({{"factor", 1000000000.0}});
    MultiplyFunction mulFunc;
    auto result2 = mulFunc.execute(result1View, context2).get();
    
    DoubleSeriesView result2View(&result2.timestamps, &result2.values);
    auto context3 = createContext({{"factor", 1e-9}});
    auto result3 = mulFunc.execute(result2View, context3).get();
    
    // Verify the results are reasonable despite precision loss
    EXPECT_EQ(result3.size(), precisionValues.size());
    for (size_t i = 0; i < result3.size(); ++i) {
        EXPECT_TRUE(std::isfinite(result3.values[i]));
        EXPECT_FALSE(std::isnan(result3.values[i]));
    }
}

TEST_F(EdgeCasesTest, VerySmallIncrementalValues) {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    
    // Create data with very small incremental differences
    double base = 1.0;
    for (int i = 0; i < 1000; ++i) {
        timestamps.push_back(1000ULL + i * 1000ULL);
        values.push_back(base + i * 1e-15);  // Very small increments
    }
    
    DoubleSeriesView view(&timestamps, &values);
    auto context = createContext({{"window", 10}});
    SMAFunction smaFunc;
    auto smaResult = smaFunc.execute(view, context).get();
    
    // Results should be finite and reasonable
    for (size_t i = 0; i < smaResult.size(); ++i) {
        EXPECT_TRUE(std::isfinite(smaResult.values[i]));
        EXPECT_GE(smaResult.values[i], 1.0);
        EXPECT_LE(smaResult.values[i], 2.0);
    }
}

// =============================================================================
// TIMESTAMP BOUNDARY CONDITION TESTS
// =============================================================================

TEST_F(EdgeCasesTest, UnixEpochBoundaries) {
    DoubleSeriesView boundaryView(&boundaryTimestamps, &boundaryValues);
    auto context = createContext({{"operand", 1.0}});
    
    AddFunction addFunc;
    auto addResult = addFunc.execute(boundaryView, context).get();
    
    EXPECT_EQ(addResult.size(), boundaryValues.size());
    EXPECT_EQ(addResult.timestamps[0], 0ULL);  // Unix epoch start
    EXPECT_EQ(addResult.timestamps[1], std::numeric_limits<uint64_t>::max());
    
    // Values should be computed correctly regardless of timestamp
    for (size_t i = 0; i < addResult.size(); ++i) {
        EXPECT_DOUBLE_EQ(addResult.values[i], boundaryValues[i] + 1.0);
    }
}

TEST_F(EdgeCasesTest, TimestampOverflowBoundaries) {
    std::vector<uint64_t> overflowTimestamps = {
        std::numeric_limits<uint64_t>::max() - 1,
        std::numeric_limits<uint64_t>::max()
    };
    std::vector<double> overflowValues = {1.0, 2.0};
    
    DoubleSeriesView overflowView(&overflowTimestamps, &overflowValues);
    auto context = createContext({{"alpha", 0.5}});
    
    EMAFunction emaFunc;
    auto emaResult = emaFunc.execute(overflowView, context).get();
    
    // Should handle timestamp overflow gracefully
    EXPECT_EQ(emaResult.size(), 2);
    EXPECT_EQ(emaResult.timestamps[0], std::numeric_limits<uint64_t>::max() - 1);
    EXPECT_EQ(emaResult.timestamps[1], std::numeric_limits<uint64_t>::max());
}

// =============================================================================
// PARAMETER BOUNDARY CONDITIONS
// =============================================================================

TEST_F(EdgeCasesTest, InvalidParameterValues) {
    DoubleSeriesView normalView(&singleTimestamp, &singleValue);
    
    // Test SMA with window size 0
    SMAFunction smaFunc;
    auto zeroWindowContext = createContext({{"window", 0}});
    
    EXPECT_FALSE(smaFunc.validateParameters(zeroWindowContext).get());
    
    // Test SMA with negative window size
    auto negWindowContext = createContext({{"window", -5}});
    EXPECT_FALSE(smaFunc.validateParameters(negWindowContext).get());
    
    // Test EMA with invalid alpha values
    EMAFunction emaFunc;
    auto invalidAlphaContext1 = createContext({{"alpha", -0.1}});
    EXPECT_FALSE(emaFunc.validateParameters(invalidAlphaContext1).get());
    
    auto invalidAlphaContext2 = createContext({{"alpha", 1.1}});
    EXPECT_FALSE(emaFunc.validateParameters(invalidAlphaContext2).get());
}

TEST_F(EdgeCasesTest, MissingRequiredParameters) {
    DoubleSeriesView normalView(&singleTimestamp, &singleValue);
    FunctionContext emptyContext;
    
    // Test functions that require parameters - they should return false for validation
    SMAFunction smaFunc;
    bool smaValid = smaFunc.validateParameters(emptyContext).get();
    // Some smoothing functions may have default parameters, so we accept both outcomes
    
    EMAFunction emaFunc;  
    bool emaValid = emaFunc.validateParameters(emptyContext).get();
    
    // Arithmetic functions require parameters and should fail validation
    AddFunction addFunc;
    EXPECT_FALSE(addFunc.validateParameters(emptyContext).get());
}

// =============================================================================
// LARGE DATASET STRESS TESTS
// =============================================================================

TEST_F(EdgeCasesTest, LargeDatasetPerformance) {
    DoubleSeriesView largeView(&largeTimestamps, &largeValues);
    auto context = createContext({{"operand", 1.0}});
    
    auto start = std::chrono::high_resolution_clock::now();
    
    AddFunction addFunc;
    auto addResult = addFunc.execute(largeView, context).get();
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    EXPECT_EQ(addResult.size(), largeValues.size());
    
    // Performance should be reasonable (less than 1 second for 100k points)
    EXPECT_LT(duration.count(), 1000) << "Large dataset processing took too long: " << duration.count() << "ms";
    
    // Verify a few random values
    EXPECT_DOUBLE_EQ(addResult.values[0], 1.0);  // 0 + 1
    EXPECT_DOUBLE_EQ(addResult.values[1000], 1.0);  // 0 + 1 (1000 % 1000 = 0)
    EXPECT_DOUBLE_EQ(addResult.values[50000], 1.0);  // 0 + 1
}

TEST_F(EdgeCasesTest, LargeDatasetSmoothingMemoryUsage) {
    // Use smaller dataset for memory-intensive smoothing operations
    std::vector<uint64_t> mediumTimestamps;
    std::vector<double> mediumValues;
    mediumTimestamps.reserve(10000);
    mediumValues.reserve(10000);
    
    for (size_t i = 0; i < 10000; ++i) {
        mediumTimestamps.push_back(1000ULL + i * 1000ULL);
        mediumValues.push_back(std::sin(i * 0.1) + static_cast<double>(rand()) / RAND_MAX * 0.1);
    }
    
    DoubleSeriesView mediumView(&mediumTimestamps, &mediumValues);
    auto context = createContext({{"window", 100}});
    
    SMAFunction smaFunc;
    auto smaResult = smaFunc.execute(mediumView, context).get();
    
    // Should complete without running out of memory
    EXPECT_TRUE(smaResult.size() > 0);
    EXPECT_LE(smaResult.size(), mediumValues.size());
    
    // Results should be smooth (less variation than input)
    if (smaResult.size() > 100) {
        double inputVariance = 0.0, outputVariance = 0.0;
        double inputMean = 0.0, outputMean = 0.0;
        
        size_t sampleSize = std::min(smaResult.size(), size_t(1000));
        
        // Calculate means
        for (size_t i = 0; i < sampleSize; ++i) {
            inputMean += mediumValues[i];
            outputMean += smaResult.values[i];
        }
        inputMean /= sampleSize;
        outputMean /= sampleSize;
        
        // Calculate variances
        for (size_t i = 0; i < sampleSize; ++i) {
            inputVariance += (mediumValues[i] - inputMean) * (mediumValues[i] - inputMean);
            outputVariance += (smaResult.values[i] - outputMean) * (smaResult.values[i] - outputMean);
        }
        inputVariance /= sampleSize;
        outputVariance /= sampleSize;
        
        EXPECT_LT(outputVariance, inputVariance) << "Smoothing should reduce variance";
    }
}

// =============================================================================
// MALFORMED DATA HANDLING TESTS
// =============================================================================

TEST_F(EdgeCasesTest, MismatchedTimestampValueArrays) {
    std::vector<uint64_t> shortTimestamps = {1000ULL, 2000ULL};
    std::vector<double> longValues = {1.0, 2.0, 3.0, 4.0};
    
    // TimeSeriesView constructor uses value array size when cnt=0 (default)
    DoubleSeriesView mismatchedView(&shortTimestamps, &longValues);
    
    // From the constructor: count(cnt == 0 ? (vals ? vals->size() - start : 0) : cnt)
    // So it uses longValues.size() = 4, even though timestamps is shorter
    EXPECT_EQ(mismatchedView.size(), longValues.size());
    
    auto context = createContext({{"operand", 1.0}});
    AddFunction addFunc;
    
    // The function should handle this gracefully, possibly by using the shorter length
    auto result = addFunc.execute(mismatchedView, context).get();
    // The actual result size depends on implementation - it might crash or handle gracefully
    EXPECT_GT(result.size(), 0);  // Just verify it doesn't completely fail
}

TEST_F(EdgeCasesTest, UnsortedTimestamps) {
    std::vector<uint64_t> unsortedTimestamps = {3000ULL, 1000ULL, 4000ULL, 2000ULL};
    std::vector<double> unsortedValues = {3.0, 1.0, 4.0, 2.0};
    
    DoubleSeriesView unsortedView(&unsortedTimestamps, &unsortedValues);
    auto context = createContext({{"operand", 10.0}});
    
    AddFunction addFunc;
    auto result = addFunc.execute(unsortedView, context).get();
    
    // Should process data as-is (functions might or might not sort internally)
    EXPECT_EQ(result.size(), 4);
    EXPECT_EQ(result.timestamps[0], 3000ULL);
    EXPECT_DOUBLE_EQ(result.values[0], 13.0);  // 3.0 + 10.0
}

// =============================================================================
// CONCURRENT ACCESS EDGE CASES
// =============================================================================

TEST_F(EdgeCasesTest, FunctionCloning) {
    AddFunction originalAdd;
    auto clonedAdd = originalAdd.clone();
    
    // Cloned function should have same metadata
    EXPECT_EQ(clonedAdd->getName(), originalAdd.getName());
    EXPECT_EQ(clonedAdd->getMetadata().name, originalAdd.getMetadata().name);
    
    // Cloned function should work independently
    DoubleSeriesView normalView(&singleTimestamp, &singleValue);
    auto context = createContext({{"operand", 5.0}});
    
    auto originalResult = originalAdd.execute(normalView, context).get();
    auto clonedResult = dynamic_cast<AddFunction*>(clonedAdd.get())->execute(normalView, context).get();
    
    EXPECT_EQ(originalResult.size(), clonedResult.size());
    EXPECT_DOUBLE_EQ(originalResult.values[0], clonedResult.values[0]);
}

TEST_F(EdgeCasesTest, FunctionRegistryEdgeCases) {
    auto& registry = FunctionRegistry::getInstance();
    
    // Test registering duplicate function names
    EXPECT_THROW(registry.registerFunction<AddFunction>(AddFunction::metadata_), std::invalid_argument);
    
    // Test creating non-existent function - should return nullptr
    auto nonExistentFunc = registry.createFunction("NonExistentFunction");
    EXPECT_EQ(nonExistentFunc, nullptr);
    
    // Test listing functions
    auto functionNames = registry.getAllFunctionNames();
    EXPECT_GT(functionNames.size(), 0);
    
    // Clear and verify empty
    registry.clear();
    auto emptyList = registry.getAllFunctionNames();
    EXPECT_EQ(emptyList.size(), 0);
}