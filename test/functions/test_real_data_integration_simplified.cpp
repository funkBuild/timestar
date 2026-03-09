#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <filesystem>
#include <chrono>
#include <random>

// TimeStar Core
#include "core/engine.hpp"
#include "core/timestar_value.hpp"
#include "query/query_result.hpp"

// Function system
#include "functions/function_types.hpp"
#include "functions/function_registry.hpp"
#include "functions/arithmetic_functions.hpp"
#include "functions/smoothing_functions.hpp"
#include "functions/interpolation_functions.hpp"

using namespace timestar::functions;
using ::testing::_;
using ::testing::DoubleNear;
using ::testing::ElementsAre;
using ::testing::DoubleEq;

class SimplifiedRealDataIntegrationTest : public ::testing::Test {
protected:
    std::unique_ptr<Engine> engine;
    std::string testDataDir;
    
    void SetUp() override {
        // Create unique test directory for this test run
        auto now = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
        testDataDir = std::string("/tmp/timestar_test_simple_") + std::to_string(millis);
        
        // Clean up any existing directory
        std::filesystem::remove_all(testDataDir);
        std::filesystem::create_directories(testDataDir);
        
        // Initialize engine
        engine = std::make_unique<Engine>();
        engine->init().get();
        
        // Set up function registry
        setupFunctionRegistry();
        
        // Generate simple test data
        generateSimpleTestData();
    }
    
    void TearDown() override {
        if (engine) {
            engine->stop().get();
            engine.reset();
        }
        
        // Clean up test directory
        std::filesystem::remove_all(testDataDir);
        
        // Clear function registry
        FunctionRegistry::getInstance().clear();
    }
    
    void setupFunctionRegistry() {
        auto& registry = FunctionRegistry::getInstance();
        registry.clear();
        
        // Register basic arithmetic functions
        registry.registerFunction<AddFunction>(AddFunction::metadata_);
        registry.registerFunction<MultiplyFunction>(MultiplyFunction::metadata_);
        registry.registerFunction<ScaleFunction>(ScaleFunction::metadata_);
        
        // Register smoothing functions  
        registry.registerFunction<SMAFunction>(SMAFunction::metadata_);
        
        // Register interpolation functions
        registry.registerFunction<LinearInterpolationFunction>(LinearInterpolationFunction::metadata_);
    }
    
    void generateSimpleTestData() {
        // Create simple temperature data for testing
        TimeStarInsert<double> tempData("weather", "temperature");
        tempData.addTag("location", "test_datacenter");
        tempData.addTag("host", "test_server");
        
        uint64_t baseTime = 1609459200000000000ULL; // Jan 1, 2021 00:00:00 UTC
        
        // Generate 100 data points with simple pattern
        for (int i = 0; i < 100; i++) {
            uint64_t timestamp = baseTime + (i * 60 * 1000000000ULL); // 1-minute intervals
            double temp = 20.0 + 5.0 * std::sin(i * 0.1) + (i % 10 - 5) * 0.1; // Simple pattern
            tempData.addValue(timestamp, temp);
        }
        
        engine->insert(tempData).get();
        
        // Create CPU usage data
        TimeStarInsert<double> cpuData("system", "cpu_usage");
        cpuData.addTag("location", "test_datacenter");
        cpuData.addTag("host", "test_server");
        
        for (int i = 0; i < 50; i++) {
            uint64_t timestamp = baseTime + (i * 120 * 1000000000ULL); // 2-minute intervals
            double cpu = 30.0 + 40.0 * std::cos(i * 0.2) + (i % 6 - 3) * 0.5;
            cpuData.addValue(timestamp, cpu);
        }
        
        engine->insert(cpuData).get();
    }
    
    // Simplified helper methods
    FunctionResult<double> queryRealData(const std::string& measurement, 
                                        const std::string& field,
                                        const std::map<std::string, std::string>& tags,
                                        uint64_t startTime, uint64_t endTime) {
        try {
            auto result = engine->queryBySeries(measurement, tags, field, startTime, endTime).get();
            
            FunctionResult<double> functionResult;
            if (std::holds_alternative<QueryResult<double>>(result)) {
                auto doubleResult = std::get<QueryResult<double>>(result);
                functionResult.timestamps = doubleResult.timestamps;
                functionResult.values = doubleResult.values;
            }
            
            return functionResult;
        } catch (const std::exception& e) {
            // Return empty result on error
            return FunctionResult<double>();
        }
    }
    
    void expectVectorNear(const std::vector<double>& actual, 
                         const std::vector<double>& expected, 
                         double tolerance = 1e-10) {
        ASSERT_EQ(actual.size(), expected.size()) 
            << "Vector sizes don't match: actual=" << actual.size() 
            << ", expected=" << expected.size();
        
        for (size_t i = 0; i < actual.size(); ++i) {
            EXPECT_NEAR(actual[i], expected[i], tolerance) 
                << "Mismatch at index " << i << ": actual=" << actual[i] 
                << ", expected=" << expected[i];
        }
    }
};

// Test 1: Basic arithmetic operations on real data
TEST_F(SimplifiedRealDataIntegrationTest, BasicArithmeticWithRealData) {
    // Query real temperature data
    std::map<std::string, std::string> tags = {{"location", "test_datacenter"}, {"host", "test_server"}};
    uint64_t startTime = 1609459200000000000ULL;
    uint64_t endTime = startTime + (6000 * 1000000000ULL); // 100 minutes
    
    auto realData = queryRealData("weather", "temperature", tags, startTime, endTime);
    ASSERT_GT(realData.size(), 0) << "No real data retrieved from TimeStar";
    
    // Apply Add function to real data
    AddFunction addFunc;
    FunctionContext addContext;
    addContext.setParameter("operand", 5.0);
    
    auto addResult = addFunc.execute(realData.asView(), addContext).get();
    EXPECT_EQ(addResult.size(), realData.size());
    EXPECT_EQ(addResult.timestamps, realData.timestamps);
    
    // Verify that each value was incremented by 5
    for (size_t i = 0; i < std::min(addResult.values.size(), realData.values.size()); ++i) {
        EXPECT_NEAR(addResult.values[i], realData.values[i] + 5.0, 1e-10);
    }
    
    // Test Scale function
    ScaleFunction scaleFunc;
    FunctionContext scaleContext;
    scaleContext.setParameter("factor", 2.0);
    
    auto scaleResult = scaleFunc.execute(realData.asView(), scaleContext).get();
    EXPECT_EQ(scaleResult.size(), realData.size());
    
    for (size_t i = 0; i < std::min(scaleResult.values.size(), realData.values.size()); ++i) {
        EXPECT_NEAR(scaleResult.values[i], realData.values[i] * 2.0, 1e-10);
    }
}

// Test 2: Data integrity verification
TEST_F(SimplifiedRealDataIntegrationTest, DataIntegrityAfterStorage) {
    // Query the stored temperature data
    std::map<std::string, std::string> tags = {{"location", "test_datacenter"}, {"host", "test_server"}};
    uint64_t startTime = 1609459200000000000ULL;
    uint64_t endTime = startTime + (6000 * 1000000000ULL);
    
    auto retrievedData = queryRealData("weather", "temperature", tags, startTime, endTime);
    
    // Verify we got some data back
    ASSERT_GT(retrievedData.size(), 0) << "No data retrieved after storage";
    
    // Verify data is in chronological order
    for (size_t i = 1; i < retrievedData.timestamps.size(); ++i) {
        EXPECT_GE(retrievedData.timestamps[i], retrievedData.timestamps[i-1]) 
            << "Timestamps not in chronological order";
    }
    
    // Apply function to verify values are still correct
    AddFunction addFunc;
    FunctionContext context;
    context.setParameter("operand", 1.0);
    
    auto functionResult = addFunc.execute(retrievedData.asView(), context).get();
    
    // Verify function results
    EXPECT_EQ(functionResult.size(), retrievedData.size());
    for (size_t i = 0; i < std::min(functionResult.values.size(), retrievedData.values.size()); ++i) {
        EXPECT_NEAR(functionResult.values[i], retrievedData.values[i] + 1.0, 1e-10)
            << "Function result incorrect after storage at index " << i;
    }
}

// Test 3: Performance comparison between real and mock data
TEST_F(SimplifiedRealDataIntegrationTest, PerformanceComparisonRealVsMock) {
    // Test with real data
    std::map<std::string, std::string> tags = {{"location", "test_datacenter"}, {"host", "test_server"}};
    uint64_t startTime = 1609459200000000000ULL;
    uint64_t endTime = startTime + (6000 * 1000000000ULL);
    
    auto realData = queryRealData("weather", "temperature", tags, startTime, endTime);
    ASSERT_GT(realData.size(), 10) << "Need substantial data for performance test";
    
    // Measure real data performance
    auto startReal = std::chrono::high_resolution_clock::now();
    
    ScaleFunction scaleFunc;
    FunctionContext context;
    context.setParameter("factor", 2.0);
    
    auto realResult = scaleFunc.execute(realData.asView(), context).get();
    
    auto endReal = std::chrono::high_resolution_clock::now();
    auto realDuration = std::chrono::duration_cast<std::chrono::microseconds>(endReal - startReal);
    
    // Create equivalent mock data
    std::vector<uint64_t> mockTimestamps;
    std::vector<double> mockValues;
    
    for (size_t i = 0; i < realData.size(); ++i) {
        mockTimestamps.push_back(startTime + i * 1000000000ULL);
        mockValues.push_back(20.0 + std::sin(i * 0.1));
    }
    
    DoubleSeriesView mockView(&mockTimestamps, &mockValues);
    
    // Measure mock data performance
    auto startMock = std::chrono::high_resolution_clock::now();
    
    auto mockResult = scaleFunc.execute(mockView, context).get();
    
    auto endMock = std::chrono::high_resolution_clock::now();
    auto mockDuration = std::chrono::duration_cast<std::chrono::microseconds>(endMock - startMock);
    
    // Results should be equivalent in size
    EXPECT_EQ(realResult.size(), mockResult.size());
    EXPECT_EQ(realResult.size(), realData.size());
    
    // Performance should be comparable (real data shouldn't be significantly slower)
    // Allow real data to be up to 5x slower due to potential query overhead
    EXPECT_LT(realDuration.count(), mockDuration.count() * 5) 
        << "Real data processing significantly slower: real=" << realDuration.count() 
        << "μs, mock=" << mockDuration.count() << "μs";
        
    std::cout << "Performance comparison - Real: " << realDuration.count() 
              << "μs, Mock: " << mockDuration.count() << "μs" << std::endl;
}

// Test 4: Multiple series operations
TEST_F(SimplifiedRealDataIntegrationTest, MultipleSeriesOperations) {
    // Query both temperature and CPU data
    std::map<std::string, std::string> tags = {{"location", "test_datacenter"}, {"host", "test_server"}};
    uint64_t startTime = 1609459200000000000ULL;
    uint64_t endTime = startTime + (6000 * 1000000000ULL);
    
    auto tempData = queryRealData("weather", "temperature", tags, startTime, endTime);
    auto cpuData = queryRealData("system", "cpu_usage", tags, startTime, endTime);
    
    ASSERT_GT(tempData.size(), 0) << "No temperature data retrieved";
    ASSERT_GT(cpuData.size(), 0) << "No CPU data retrieved";
    
    // Apply scaling to both series
    ScaleFunction scaleFunc;
    FunctionContext tempContext;
    tempContext.setParameter("factor", 1.8); // Celsius to Fahrenheit factor
    
    FunctionContext cpuContext;
    cpuContext.setParameter("factor", 0.01); // Convert to percentage
    
    auto scaledTemp = scaleFunc.execute(tempData.asView(), tempContext).get();
    auto scaledCpu = scaleFunc.execute(cpuData.asView(), cpuContext).get();
    
    // Verify both operations succeeded
    EXPECT_GT(scaledTemp.size(), 0);
    EXPECT_GT(scaledCpu.size(), 0);
    
    // Verify scaling was applied correctly (sample first few values)
    for (size_t i = 0; i < std::min(size_t(3), scaledTemp.values.size()); ++i) {
        EXPECT_NEAR(scaledTemp.values[i], tempData.values[i] * 1.8, 0.01);
    }
    
    for (size_t i = 0; i < std::min(size_t(3), scaledCpu.values.size()); ++i) {
        EXPECT_NEAR(scaledCpu.values[i], cpuData.values[i] * 0.01, 0.001);
    }
}

// Test 5: Function chaining with real data
TEST_F(SimplifiedRealDataIntegrationTest, FunctionChainingWithRealData) {
    // Query real data
    std::map<std::string, std::string> tags = {{"location", "test_datacenter"}, {"host", "test_server"}};
    uint64_t startTime = 1609459200000000000ULL;
    uint64_t endTime = startTime + (3000 * 1000000000ULL); // 50 minutes
    
    auto realData = queryRealData("weather", "temperature", tags, startTime, endTime);
    ASSERT_GT(realData.size(), 5) << "Need data for function chaining test";
    
    // First, apply scaling (Celsius to Kelvin)
    ScaleFunction scaleFunc;
    FunctionContext scaleContext;
    scaleContext.setParameter("factor", 1.0);
    
    AddFunction addFunc;
    FunctionContext addContext;
    addContext.setParameter("operand", 273.15); // Add Kelvin offset
    
    // Chain operations: first add offset, then verify
    auto kelvinData = addFunc.execute(realData.asView(), addContext).get();
    EXPECT_EQ(kelvinData.size(), realData.size());
    
    // Then apply another operation
    ScaleFunction finalScaleFunc;
    FunctionContext finalContext;
    finalContext.setParameter("factor", 0.5); // Half the Kelvin values
    
    DoubleSeriesView kelvinView = kelvinData.asView();
    auto finalResult = finalScaleFunc.execute(kelvinView, finalContext).get();
    
    // Verify chained operations
    EXPECT_EQ(finalResult.size(), realData.size());
    
    for (size_t i = 0; i < std::min(finalResult.values.size(), realData.values.size()); ++i) {
        double expected = (realData.values[i] + 273.15) * 0.5;
        EXPECT_NEAR(finalResult.values[i], expected, 1e-10)
            << "Chained function result incorrect at index " << i;
    }
}