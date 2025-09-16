#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "functions/function_types.hpp"
#include "functions/function_interface.hpp"
#include "functions/function_registry.hpp"
#include "functions/vectorized_series.hpp"
#include "functions/series_alignment.hpp"
#include <vector>
#include <cmath>

using namespace tsdb::functions;
using ::testing::_;
using ::testing::Return;
using ::testing::HasSubstr;

class FunctionFrameworkTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clear registry for each test
        FunctionRegistry::getInstance().clear();
        
        // Create sample test data
        setupTestData();
    }
    
    void TearDown() override {
        FunctionRegistry::getInstance().clear();
    }
    
    void setupTestData() {
        // Linear increasing series
        linearTimestamps = {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL, 5000000000ULL};
        linearValues = {1.0, 2.0, 3.0, 4.0, 5.0};
        
        // Sine wave series
        sineTimestamps.clear();
        sineValues.clear();
        for (int i = 0; i < 100; ++i) {
            sineTimestamps.push_back(i * 1000000ULL);  // 1ms intervals
            sineValues.push_back(std::sin(i * 0.1) + 0.1 * (rand() % 100 - 50)); // with noise
        }
        
        // Series with gaps
        gappyTimestamps = {1000ULL, 3000ULL, 7000ULL, 8000ULL, 15000ULL};
        gappyValues = {10.0, 30.0, 70.0, 80.0, 150.0};
        
        // Boolean series
        boolTimestamps = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
        boolValues = {true, false, true, true, false};
    }
    
    // Test data sets
    std::vector<uint64_t> linearTimestamps, sineTimestamps, gappyTimestamps, boolTimestamps;
    std::vector<double> linearValues, sineValues, gappyValues;
    std::vector<bool> boolValues;
};

// Test basic type system functionality
TEST_F(FunctionFrameworkTest, TypeSystemBasics) {
    // Test TimeSeriesView creation and access
    DoubleSeriesView linearView(&linearTimestamps, &linearValues);
    
    EXPECT_EQ(linearView.size(), 5);
    EXPECT_FALSE(linearView.empty());
    EXPECT_EQ(linearView.timestampAt(0), 1000000000ULL);
    EXPECT_EQ(linearView.valueAt(0), 1.0);
    EXPECT_EQ(linearView.timestampAt(4), 5000000000ULL);
    EXPECT_EQ(linearView.valueAt(4), 5.0);
}

TEST_F(FunctionFrameworkTest, TimeSeriesViewSlicing) {
    DoubleSeriesView view(&sineTimestamps, &sineValues, 10, 20);
    
    EXPECT_EQ(view.size(), 20);
    EXPECT_EQ(view.timestampAt(0), sineTimestamps[10]);
    EXPECT_EQ(view.valueAt(0), sineValues[10]);
    EXPECT_EQ(view.timestampAt(19), sineTimestamps[29]);
    EXPECT_EQ(view.valueAt(19), sineValues[29]);
}

TEST_F(FunctionFrameworkTest, TimeSeriesViewIteration) {
    DoubleSeriesView view(&linearTimestamps, &linearValues);
    
    size_t count = 0;
    for (auto [timestamp, value] : view) {
        EXPECT_EQ(timestamp, linearTimestamps[count]);
        EXPECT_EQ(value, linearValues[count]);
        count++;
    }
    EXPECT_EQ(count, 5);
}

TEST_F(FunctionFrameworkTest, FunctionResultBasics) {
    FunctionResult<double> result;
    result.timestamps = {1000ULL, 2000ULL, 3000ULL};
    result.values = {1.5, 2.5, 3.5};
    result.executionTime = std::chrono::microseconds(100);
    
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 3);
    
    auto view = result.asView();
    EXPECT_EQ(view.size(), 3);
    EXPECT_EQ(view.timestampAt(1), 2000ULL);
    EXPECT_EQ(view.valueAt(1), 2.5);
}

// Test Function Context
TEST_F(FunctionFrameworkTest, FunctionContextBasics) {
    FunctionContext context;
    
    // Test parameter setting and retrieval
    context.setParameter("window", int64_t(10));
    context.setParameter("alpha", 0.3);
    context.setParameter("method", std::string("sma"));
    context.setParameter("enabled", true);
    
    EXPECT_TRUE(context.hasParameter("window"));
    EXPECT_TRUE(context.hasParameter("alpha"));
    EXPECT_FALSE(context.hasParameter("nonexistent"));
    
    EXPECT_EQ(context.getParameter<int64_t>("window"), 10);
    EXPECT_DOUBLE_EQ(context.getParameter<double>("alpha"), 0.3);
    EXPECT_EQ(context.getParameter<std::string>("method"), "sma");
    EXPECT_TRUE(context.getParameter<bool>("enabled"));
    
    // Test default values
    EXPECT_EQ(context.getParameter<int64_t>("missing", int64_t(42)), 42);
    EXPECT_DOUBLE_EQ(context.getParameter<double>("missing", 1.5), 1.5);
}

TEST_F(FunctionFrameworkTest, FunctionContextTypeValidation) {
    FunctionContext context;
    context.setParameter("window", int64_t(10));
    
    // Test type mismatch throws exception
    EXPECT_THROW(context.getParameter<double>("window"), std::bad_variant_access);
    EXPECT_THROW(context.getParameter<std::string>("window"), std::bad_variant_access);
    
    // Test missing parameter throws exception
    EXPECT_THROW(context.getParameter<int64_t>("missing"), std::invalid_argument);
}

// Test Exception Classes
TEST_F(FunctionFrameworkTest, ExceptionHierarchy) {
    // Test base exception
    FunctionException baseEx("Base error");
    EXPECT_THAT(baseEx.what(), HasSubstr("Function error"));
    EXPECT_THAT(baseEx.what(), HasSubstr("Base error"));
    
    // Test derived exceptions
    ParameterValidationException paramEx("Invalid param");
    EXPECT_THAT(paramEx.what(), HasSubstr("Parameter validation failed"));
    
    InsufficientDataException dataEx("Not enough data");
    EXPECT_THAT(dataEx.what(), HasSubstr("Insufficient data"));
    
    // Test inheritance
    EXPECT_NO_THROW(static_cast<FunctionException&>(paramEx));
    EXPECT_NO_THROW(static_cast<std::runtime_error&>(baseEx));
}

// Test Vectorized Series
TEST_F(FunctionFrameworkTest, VectorizedSeriesBasics) {
    VectorizedDoubleSeries series(linearValues);
    
    EXPECT_EQ(series.size(), 5);
    EXPECT_FALSE(series.empty());
    EXPECT_TRUE(series.isSimdAligned());
    
    // Test element access
    EXPECT_EQ(series[0], 1.0);
    EXPECT_EQ(series[4], 5.0);
    
    // Test aggregations
    EXPECT_DOUBLE_EQ(series.sum(), 15.0);  // 1+2+3+4+5
    EXPECT_DOUBLE_EQ(series.average(), 3.0);
    EXPECT_DOUBLE_EQ(series.minimum(), 1.0);
    EXPECT_DOUBLE_EQ(series.maximum(), 5.0);
}

TEST_F(FunctionFrameworkTest, VectorizedSeriesArithmetic) {
    VectorizedDoubleSeries series1(std::vector<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    VectorizedDoubleSeries series2(std::vector<double>{2.0, 2.0, 2.0, 2.0, 2.0});
    
    // Test scalar operations
    series1 += 10.0;
    EXPECT_EQ(series1[0], 11.0);
    EXPECT_EQ(series1[4], 15.0);
    
    series1 *= 2.0;
    EXPECT_EQ(series1[0], 22.0);
    EXPECT_EQ(series1[4], 30.0);
    
    // Reset for element-wise operations
    series1 = VectorizedDoubleSeries(std::vector<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    
    // Test element-wise operations
    series1 += series2;
    EXPECT_EQ(series1[0], 3.0);
    EXPECT_EQ(series1[4], 7.0);
    
    series1 *= series2;
    EXPECT_EQ(series1[0], 6.0);
    EXPECT_EQ(series1[4], 14.0);
}

TEST_F(FunctionFrameworkTest, VectorizedSeriesSizeValidation) {
    VectorizedDoubleSeries series1(std::vector<double>{1.0, 2.0, 3.0});
    VectorizedDoubleSeries series2(std::vector<double>{1.0, 2.0});  // Different size
    
    EXPECT_THROW(series1 += series2, std::invalid_argument);
    EXPECT_THROW(series1 -= series2, std::invalid_argument);
    EXPECT_THROW(series1 *= series2, std::invalid_argument);
    EXPECT_THROW(series1 /= series2, std::invalid_argument);
}

TEST_F(FunctionFrameworkTest, AlignedVectorizedSeries) {
    AlignedVectorizedSeries aligned(linearTimestamps, linearValues);
    
    EXPECT_EQ(aligned.size(), 5);
    EXPECT_FALSE(aligned.empty());
    EXPECT_EQ(aligned.timestampAt(0), 1000000000ULL);
    EXPECT_EQ(aligned.valueAt(0), 1.0);
    
    // Test conversion to function result
    auto result = aligned.toFunctionResult();
    EXPECT_EQ(result.size(), 5);
    EXPECT_EQ(result.timestamps, linearTimestamps);
    EXPECT_EQ(result.values, linearValues);
}

TEST_F(FunctionFrameworkTest, AlignedVectorizedSeriesSlicing) {
    AlignedVectorizedSeries aligned(sineTimestamps, sineValues);
    
    // Test time-based slicing
    uint64_t startTime = sineTimestamps[10];
    uint64_t endTime = sineTimestamps[20];
    auto sliced = aligned.slice(startTime, endTime);
    
    EXPECT_GT(sliced.size(), 0);
    EXPECT_LE(sliced.size(), 11);  // Should be 11 or fewer points
    EXPECT_GE(sliced.timestampAt(0), startTime);
    if (!sliced.empty()) {
        EXPECT_LE(sliced.timestampAt(sliced.size() - 1), endTime);
    }
}

// Test Function Utils
TEST_F(FunctionFrameworkTest, FunctionUtilsValidation) {
    EXPECT_NO_THROW(FunctionUtils::validateMinDataPoints(10, 5, "test"));
    EXPECT_THROW(FunctionUtils::validateMinDataPoints(3, 5, "test"), InsufficientDataException);
    
    // Test parameter validation
    FunctionContext context;
    context.setParameter("valid_param", 42.0);
    
    EXPECT_EQ(FunctionUtils::validateAndGetParameter<double>(context, "valid_param"), 42.0);
    EXPECT_EQ(FunctionUtils::validateAndGetParameter<double>(context, "missing", 10.0), 10.0);
    EXPECT_THROW(FunctionUtils::validateAndGetParameter<double>(context, "missing"), ParameterValidationException);
}

TEST_F(FunctionFrameworkTest, FunctionUtilsTimeSeriesAlignment) {
    std::vector<uint64_t> timestamps1 = {1000ULL, 2000ULL, 3000ULL, 5000ULL};
    std::vector<uint64_t> timestamps2 = {1500ULL, 2000ULL, 4000ULL, 5000ULL};
    
    auto [alignedTimestamps, indices] = FunctionUtils::alignTimeSeries(timestamps1, timestamps2);
    auto [indices1, indices2] = indices;
    
    // Should find common timestamps: 2000, 5000
    EXPECT_EQ(alignedTimestamps.size(), 2);
    EXPECT_EQ(alignedTimestamps[0], 2000ULL);
    EXPECT_EQ(alignedTimestamps[1], 5000ULL);
    
    EXPECT_EQ(indices1.size(), 2);
    EXPECT_EQ(indices2.size(), 2);
    EXPECT_EQ(indices1[0], 1);  // timestamps1[1] = 2000
    EXPECT_EQ(indices2[0], 1);  // timestamps2[1] = 2000
    EXPECT_EQ(indices1[1], 3);  // timestamps1[3] = 5000
    EXPECT_EQ(indices2[1], 3);  // timestamps2[3] = 5000
}

TEST_F(FunctionFrameworkTest, FunctionUtilsInterpolation) {
    DoubleSeriesView view(&linearTimestamps, &linearValues);
    std::vector<uint64_t> targetTimestamps = {1500000000ULL, 2500000000ULL, 4500000000ULL};
    
    auto result = FunctionUtils::interpolateLinear(view, targetTimestamps);
    
    EXPECT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 1.5);  // Linear interpolation between 1.0 and 2.0
    EXPECT_DOUBLE_EQ(result.values[1], 2.5);  // Linear interpolation between 2.0 and 3.0
    EXPECT_DOUBLE_EQ(result.values[2], 4.5);  // Linear interpolation between 4.0 and 5.0
}

// Mock Function for Registry Testing
class MockFunction : public IUnaryFunction {
public:
    MockFunction() = default;
    
    const FunctionMetadata& getMetadata() const override {
        static FunctionMetadata metadata;
        metadata.name = "mock_function";
        metadata.description = "Mock function for testing";
        metadata.category = FunctionCategory::ARITHMETIC;
        metadata.supportedInputTypes = {"double"};
        metadata.outputType = "double";
        return metadata;
    }
    
    std::unique_ptr<IFunction> clone() const override {
        return std::make_unique<MockFunction>();
    }
    
    seastar::future<bool> validateParameters(const FunctionContext& context) const override {
        return seastar::make_ready_future<bool>(true);
    }
    
    seastar::future<FunctionResult<double>> execute(
        const DoubleSeriesView& input,
        const FunctionContext& context
    ) const override {
        FunctionResult<double> result;
        result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                                input.timestamps->begin() + input.startIndex + input.count);
        result.values.reserve(input.count);
        
        // Simple identity function
        for (size_t i = 0; i < input.count; ++i) {
            result.values.push_back(input.valueAt(i));
        }
        
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }
};

TEST_F(FunctionFrameworkTest, FunctionRegistryBasics) {
    auto& registry = FunctionRegistry::getInstance();
    
    // Test empty registry
    EXPECT_FALSE(registry.hasFunction("nonexistent"));
    EXPECT_TRUE(registry.getAllFunctionNames().empty());
    
    // Register a mock function
    FunctionMetadata metadata;
    metadata.name = "test_function";
    metadata.description = "Test function";
    metadata.category = FunctionCategory::ARITHMETIC;
    
    registry.registerFunction<MockFunction>(metadata);
    
    // Test function is registered
    EXPECT_TRUE(registry.hasFunction("test_function"));
    EXPECT_EQ(registry.getAllFunctionNames().size(), 1);
    EXPECT_EQ(registry.getAllFunctionNames()[0], "test_function");
    
    // Test function creation
    auto func = registry.createFunction("test_function");
    EXPECT_NE(func, nullptr);
    EXPECT_EQ(func->getName(), "test_function");
}

TEST_F(FunctionFrameworkTest, FunctionRegistryDuplicateRegistration) {
    auto& registry = FunctionRegistry::getInstance();
    
    FunctionMetadata metadata;
    metadata.name = "duplicate";
    metadata.category = FunctionCategory::ARITHMETIC;
    
    // First registration should succeed
    EXPECT_NO_THROW(registry.registerFunction<MockFunction>(metadata));
    
    // Second registration should fail
    EXPECT_THROW(registry.registerFunction<MockFunction>(metadata), std::invalid_argument);
}

TEST_F(FunctionFrameworkTest, FunctionRegistrySearch) {
    auto& registry = FunctionRegistry::getInstance();
    
    // Register multiple functions
    FunctionMetadata metadata1;
    metadata1.name = "smooth_function";
    metadata1.description = "Smoothing operation";
    metadata1.category = FunctionCategory::SMOOTHING;
    registry.registerFunction<MockFunction>(metadata1);
    
    FunctionMetadata metadata2;
    metadata2.name = "arithmetic_add";
    metadata2.description = "Addition operation";  
    metadata2.category = FunctionCategory::ARITHMETIC;
    registry.registerFunction<MockFunction>(metadata2);
    
    // Test category filtering
    auto smoothingFuncs = registry.getFunctionsByCategory(FunctionCategory::SMOOTHING);
    EXPECT_EQ(smoothingFuncs.size(), 1);
    EXPECT_EQ(smoothingFuncs[0], "smooth_function");
    
    auto arithmeticFuncs = registry.getFunctionsByCategory(FunctionCategory::ARITHMETIC);
    EXPECT_EQ(arithmeticFuncs.size(), 1);
    EXPECT_EQ(arithmeticFuncs[0], "arithmetic_add");
    
    // Test search by name pattern
    auto results = registry.searchFunctions("smooth");
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], "smooth_function");
    
    // Test search by description
    results = registry.searchFunctions("Addition");
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], "arithmetic_add");
}

TEST_F(FunctionFrameworkTest, FunctionRegistryStats) {
    auto& registry = FunctionRegistry::getInstance();
    
    FunctionMetadata metadata1;
    metadata1.name = "func1";
    metadata1.category = FunctionCategory::SMOOTHING;
    metadata1.outputType = "double";
    registry.registerFunction<MockFunction>(metadata1);
    
    FunctionMetadata metadata2;
    metadata2.name = "func2";
    metadata2.category = FunctionCategory::ARITHMETIC;
    metadata2.outputType = "double";
    registry.registerFunction<MockFunction>(metadata2);
    
    auto stats = registry.getStats();
    EXPECT_EQ(stats.totalFunctions, 2);
    EXPECT_EQ(stats.functionsByCategory[FunctionCategory::SMOOTHING], 1);
    EXPECT_EQ(stats.functionsByCategory[FunctionCategory::ARITHMETIC], 1);
    EXPECT_EQ(stats.functionsByOutputType["double"], 2);
}

// Run the tests
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}