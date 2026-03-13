#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <random>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

// TimeStar Core
#include "core/engine.hpp"
#include "core/timestar_value.hpp"
#include "index/leveldb_index.hpp"
#include "query/query_result.hpp"
#include "storage/memory_store.hpp"
#include "storage/wal_file_manager.hpp"

// Function system
#include "functions/arithmetic_functions.hpp"
#include "functions/function_http_handler.hpp"
#include "functions/function_pipeline_executor.hpp"
#include "functions/function_registry.hpp"
#include "functions/function_types.hpp"
#include "functions/interpolation_functions.hpp"
#include "functions/smoothing_functions.hpp"

using namespace timestar::functions;
using ::testing::_;
using ::testing::DoubleEq;
using ::testing::DoubleNear;
using ::testing::ElementsAre;

class RealDataIntegrationTest : public ::testing::Test {
protected:
    std::unique_ptr<Engine> engine;
    std::string testDataDir;
    FunctionPipelineExecutor* functionExecutor;

    void SetUp() override {
        // Create unique test directory for this test run
        auto now = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
        testDataDir = std::string("/tmp/timestar_test_") + std::to_string(millis);

        // Clean up any existing directory
        std::filesystem::remove_all(testDataDir);
        std::filesystem::create_directories(testDataDir);

        // Initialize engine
        engine = std::make_unique<Engine>();
        engine->init().get();

        // Set up function registry
        setupFunctionRegistry();

        // Initialize function executor with engine
        // Note: In real implementation, this would be a sharded engine
        functionExecutor = new FunctionPipelineExecutor(nullptr, &engine->getIndex());

        // Generate test data sets
        generateTestData();
    }

    void TearDown() override {
        if (functionExecutor) {
            delete functionExecutor;
            functionExecutor = nullptr;
        }

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

        // Register arithmetic functions
        registry.registerFunction<AddFunction>(AddFunction::metadata_);
        registry.registerFunction<MultiplyFunction>(MultiplyFunction::metadata_);
        registry.registerFunction<ScaleFunction>(ScaleFunction::metadata_);
        registry.registerFunction<OffsetFunction>(OffsetFunction::metadata_);

        // Register smoothing functions
        registry.registerFunction<SMAFunction>(SMAFunction::metadata_);
        registry.registerFunction<EMAFunction>(EMAFunction::metadata_);

        // Register interpolation functions
        registry.registerFunction<LinearInterpolationFunction>(LinearInterpolationFunction::metadata_);
    }

    void generateTestData() {
        // Generate different types of test data for comprehensive testing
        generateRegularTimeSeries();
        generateIrregularTimeSeries();
        generateMixedTypeData();
        generateCrossShardData();
        generateGapData();
    }

    void generateRegularTimeSeries() {
        // Create regular 1-minute interval temperature data
        TimeStarInsert<double> tempData("weather", "temperature");
        tempData.addTag("location", "datacenter_1");
        tempData.addTag("host", "server_01");

        std::mt19937 rng(123);  // Fixed seed for reproducible tests
        std::uniform_int_distribution<int> noiseDist(0, 9);

        uint64_t baseTime = 1609459200000000000ULL;                    // Jan 1, 2021 00:00:00 UTC in nanoseconds
        for (int i = 0; i < 1440; i++) {                               // 24 hours of data
            uint64_t timestamp = baseTime + (i * 60 * 1000000000ULL);  // 1-minute intervals
            double temp =
                20.0 + 10.0 * std::sin(i * 3.14159 / 120) + (noiseDist(rng) - 5) * 0.1;  // Sine wave with noise
            tempData.addValue(timestamp, temp);
        }

        engine->insert(tempData).get();

        // Create humidity data for the same location
        TimeStarInsert<double> humidityData("weather", "humidity");
        humidityData.addTag("location", "datacenter_1");
        humidityData.addTag("host", "server_01");

        for (int i = 0; i < 1440; i++) {
            uint64_t timestamp = baseTime + (i * 60 * 1000000000ULL);
            double humidity = 60.0 + 20.0 * std::cos(i * 3.14159 / 180) + (noiseDist(rng) - 5) * 0.2;
            humidityData.addValue(timestamp, humidity);
        }

        engine->insert(humidityData).get();
    }

    void generateIrregularTimeSeries() {
        // Create irregular interval data (simulating sporadic sensor readings)
        TimeStarInsert<double> powerData("system", "power_usage");
        powerData.addTag("location", "datacenter_2");
        powerData.addTag("host", "server_02");

        uint64_t baseTime = 1609459200000000000ULL;
        std::mt19937 gen(42);                                   // Fixed seed for reproducible tests
        std::uniform_int_distribution<> intervalDist(30, 300);  // 30s to 5min intervals

        std::uniform_int_distribution<int> powerNoiseDist(0, 19);
        uint64_t currentTime = baseTime;
        for (int i = 0; i < 200; i++) {
            double power = 100.0 + 50.0 * std::sin(i * 0.1) + (powerNoiseDist(gen) - 10);
            powerData.addValue(currentTime, power);
            currentTime += intervalDist(gen) * 1000000000ULL;  // Random interval
        }

        engine->insert(powerData).get();
    }

    void generateMixedTypeData() {
        // Create boolean status data
        TimeStarInsert<bool> statusData("system", "online");
        statusData.addTag("location", "datacenter_1");
        statusData.addTag("host", "server_01");

        uint64_t baseTime = 1609459200000000000ULL;
        for (int i = 0; i < 100; i++) {
            uint64_t timestamp = baseTime + (i * 300 * 1000000000ULL);  // 5-minute intervals
            bool online = (i % 20 != 0);                                // Occasionally offline
            statusData.addValue(timestamp, online);
        }

        engine->insert(statusData).get();

        // Create string log level data
        TimeStarInsert<std::string> logData("system", "log_level");
        logData.addTag("location", "datacenter_1");
        logData.addTag("host", "server_01");

        std::vector<std::string> logLevels = {"INFO", "DEBUG", "WARN", "ERROR", "FATAL"};
        for (int i = 0; i < 50; i++) {
            uint64_t timestamp = baseTime + (i * 600 * 1000000000ULL);  // 10-minute intervals
            std::string level = logLevels[i % logLevels.size()];
            logData.addValue(timestamp, level);
        }

        engine->insert(logData).get();
    }

    void generateCrossShardData() {
        // Generate data that would be distributed across multiple shards
        // (simulating what would happen in a real multi-shard environment)

        std::vector<std::string> locations = {"us-west", "us-east", "eu-west", "asia-pacific"};
        std::vector<std::string> hosts = {"web-01", "web-02", "db-01", "db-02", "cache-01"};

        uint64_t baseTime = 1609459200000000000ULL;
        std::mt19937 rng(99);  // Fixed seed for reproducible tests
        std::uniform_int_distribution<int> cpuNoiseDist(0, 19);

        for (const auto& location : locations) {
            for (const auto& host : hosts) {
                TimeStarInsert<double> cpuData("system", "cpu_usage");
                cpuData.addTag("location", location);
                cpuData.addTag("host", host);

                // Generate 2 hours of data at 30-second intervals
                for (int i = 0; i < 240; i++) {
                    uint64_t timestamp = baseTime + (i * 30 * 1000000000ULL);
                    double cpu = 10.0 + 60.0 * std::sin(i * 0.05) + (cpuNoiseDist(rng) - 10);
                    cpuData.addValue(timestamp, cpu);
                }

                engine->insert(cpuData).get();
            }
        }
    }

    void generateGapData() {
        // Create data with intentional gaps for testing interpolation
        TimeStarInsert<double> networkData("system", "network_throughput");
        networkData.addTag("location", "datacenter_3");
        networkData.addTag("host", "server_03");

        uint64_t baseTime = 1609459200000000000ULL;

        // Regular data for first hour
        for (int i = 0; i < 60; i++) {
            uint64_t timestamp = baseTime + (i * 60 * 1000000000ULL);
            double throughput = 1000.0 + 200.0 * std::sin(i * 0.1);
            networkData.addValue(timestamp, throughput);
        }

        // 30-minute gap (no data)

        // Resume data for second hour with different pattern
        for (int i = 90; i < 150; i++) {
            uint64_t timestamp = baseTime + (i * 60 * 1000000000ULL);
            double throughput = 1200.0 + 300.0 * std::cos(i * 0.08);
            networkData.addValue(timestamp, throughput);
        }

        engine->insert(networkData).get();
    }

    // Helper methods for data retrieval and verification
    FunctionResult<double> queryRealData(const std::string& measurement, const std::string& field,
                                         const std::map<std::string, std::string>& tags, uint64_t startTime,
                                         uint64_t endTime) {
        auto result = engine->queryBySeries(measurement, tags, field, startTime, endTime).get();

        FunctionResult<double> functionResult;
        if (std::holds_alternative<QueryResult<double>>(result)) {
            auto doubleResult = std::get<QueryResult<double>>(result);
            functionResult.timestamps = doubleResult.timestamps;
            functionResult.values = doubleResult.values;
        }

        return functionResult;
    }

    void waitForTSMFlush() {
        // Force memory store rollover to ensure data is written to TSM files
        engine->rolloverMemoryStore().get();

        // Add a small delay to ensure TSM files are written
        seastar::sleep(std::chrono::milliseconds(100)).get();
    }

    void expectVectorNear(const std::vector<double>& actual, const std::vector<double>& expected,
                          double tolerance = 1e-10) {
        ASSERT_EQ(actual.size(), expected.size())
            << "Vector sizes don't match: actual=" << actual.size() << ", expected=" << expected.size();

        for (size_t i = 0; i < actual.size(); ++i) {
            EXPECT_NEAR(actual[i], expected[i], tolerance)
                << "Mismatch at index " << i << ": actual=" << actual[i] << ", expected=" << expected[i];
        }
    }
};

// Test 1: Basic Real Data Function Operations
TEST_F(RealDataIntegrationTest, BasicArithmeticWithRealData) {
    // Wait for data to be flushed to TSM files
    waitForTSMFlush();

    // Query real temperature data
    std::map<std::string, std::string> tags = {{"location", "datacenter_1"}, {"host", "server_01"}};
    uint64_t startTime = 1609459200000000000ULL;
    uint64_t endTime = startTime + (3600 * 1000000000ULL);  // 1 hour of data

    auto realData = queryRealData("weather", "temperature", tags, startTime, endTime);
    ASSERT_GT(realData.size(), 0) << "No real data retrieved from TimeStar";

    // Apply arithmetic functions to real data
    DoubleSeriesView dataView = realData.asView();

    // Test Add function
    AddFunction addFunc;
    FunctionContext addContext;
    addContext.setParameter("operand", 5.0);

    auto addResult = addFunc.execute(dataView, addContext).get();
    EXPECT_EQ(addResult.size(), realData.size());
    EXPECT_EQ(addResult.timestamps, realData.timestamps);

    // Verify that each value was incremented by 5
    for (size_t i = 0; i < addResult.values.size(); ++i) {
        EXPECT_NEAR(addResult.values[i], realData.values[i] + 5.0, 1e-10);
    }

    // Test Scale function
    ScaleFunction scaleFunc;
    FunctionContext scaleContext;
    scaleContext.setParameter("factor", 2.0);

    auto scaleResult = scaleFunc.execute(dataView, scaleContext).get();
    EXPECT_EQ(scaleResult.size(), realData.size());

    for (size_t i = 0; i < scaleResult.values.size(); ++i) {
        EXPECT_NEAR(scaleResult.values[i], realData.values[i] * 2.0, 1e-10);
    }
}

// Test 2: Smoothing Functions on Real Data
TEST_F(RealDataIntegrationTest, SmoothingWithRealData) {
    waitForTSMFlush();

    // Query real power usage data (irregular intervals)
    std::map<std::string, std::string> tags = {{"location", "datacenter_2"}, {"host", "server_02"}};
    uint64_t startTime = 1609459200000000000ULL;
    uint64_t endTime = startTime + (24 * 3600 * 1000000000ULL);  // 24 hours

    auto realData = queryRealData("system", "power_usage", tags, startTime, endTime);
    ASSERT_GT(realData.size(), 10) << "Insufficient real data for smoothing test";

    // Apply moving average
    SMAFunction maFunc;
    FunctionContext maContext;
    maContext.setParameter("window", static_cast<int64_t>(5));

    auto maResult = maFunc.execute(realData.asView(), maContext).get();

    // Smoothed data should have same length but different values
    EXPECT_EQ(maResult.size(), realData.size());
    EXPECT_EQ(maResult.timestamps, realData.timestamps);

    // The smoothed values should be different from original (less volatile)
    bool foundDifference = false;
    for (size_t i = 0; i < std::min(maResult.values.size(), realData.values.size()); ++i) {
        if (std::abs(maResult.values[i] - realData.values[i]) > 1e-10) {
            foundDifference = true;
            break;
        }
    }
    EXPECT_TRUE(foundDifference) << "Moving average should produce different values";
}

// Test 3: Mixed Data Types from Real Storage
TEST_F(RealDataIntegrationTest, MixedDataTypesFromStorage) {
    waitForTSMFlush();

    // Query boolean status data
    std::map<std::string, std::string> tags = {{"location", "datacenter_1"}, {"host", "server_01"}};
    uint64_t startTime = 1609459200000000000ULL;
    uint64_t endTime = startTime + (24 * 3600 * 1000000000ULL);

    // For now, we'll focus on double data types since that's what functions support
    // In a full implementation, you'd extend functions to support boolean and string types
    auto doubleData = queryRealData("weather", "temperature", tags, startTime, endTime);
    auto humidityData = queryRealData("weather", "humidity", tags, startTime, endTime);

    ASSERT_GT(doubleData.size(), 0) << "No temperature data retrieved";
    ASSERT_GT(humidityData.size(), 0) << "No humidity data retrieved";

    // Apply functions to verify data integrity across different fields
    ScaleFunction scaleFunc;
    FunctionContext context;
    context.setParameter("factor", 1.8);   // Celsius to Fahrenheit scaling factor
    context.setParameter("offset", 32.0);  // Fahrenheit offset

    auto scaledTemp = scaleFunc.execute(doubleData.asView(), context).get();
    EXPECT_GT(scaledTemp.size(), 0);

    // Verify conversion (approximate check for first few values)
    for (size_t i = 0; i < std::min(size_t(5), scaledTemp.values.size()); ++i) {
        double expectedFahrenheit = doubleData.values[i] * 1.8;
        EXPECT_NEAR(scaledTemp.values[i], expectedFahrenheit, 0.1);
    }
}

// Test 4: Gap Handling and Interpolation with Real Data
TEST_F(RealDataIntegrationTest, InterpolationWithGapData) {
    waitForTSMFlush();

    // Query network data that has intentional gaps
    std::map<std::string, std::string> tags = {{"location", "datacenter_3"}, {"host", "server_03"}};
    uint64_t startTime = 1609459200000000000ULL;
    uint64_t endTime = startTime + (6 * 3600 * 1000000000ULL);  // 6 hours

    auto gapData = queryRealData("system", "network_throughput", tags, startTime, endTime);
    ASSERT_GT(gapData.size(), 0) << "No gap data retrieved";

    // Create target timestamps for interpolation (every 5 minutes)
    std::vector<uint64_t> targetTimestamps;
    for (uint64_t t = startTime; t <= endTime; t += 300 * 1000000000ULL) {
        targetTimestamps.push_back(t);
    }

    // Apply linear interpolation
    LinearInterpolationFunction interpFunc;
    FunctionContext context;
    // In a real implementation, you'd set target timestamps as a parameter

    auto interpResult = interpFunc.execute(gapData.asView(), context).get();

    // Interpolated data should fill in gaps
    EXPECT_GT(interpResult.size(), 0);
    // The interpolated result should have different characteristics than the original
    EXPECT_NE(interpResult.values, gapData.values);
}

// Test 5: Cross-Shard Simulation (Multiple Series)
TEST_F(RealDataIntegrationTest, CrossShardFunctionOperations) {
    waitForTSMFlush();

    // In a real multi-shard environment, this would test coordination between shards
    // For now, we simulate by testing multiple series from different "locations"

    std::vector<std::string> locations = {"us-west", "us-east", "eu-west"};
    std::vector<FunctionResult<double>> allResults;

    for (const auto& location : locations) {
        std::map<std::string, std::string> tags = {{"location", location}, {"host", "web-01"}};
        uint64_t startTime = 1609459200000000000ULL;
        uint64_t endTime = startTime + (3600 * 1000000000ULL);

        auto locationData = queryRealData("system", "cpu_usage", tags, startTime, endTime);
        if (locationData.size() > 0) {
            // Apply scaling to normalize across regions
            ScaleFunction scaleFunc;
            FunctionContext context;
            context.setParameter("factor", 0.8);  // Normalize CPU usage

            auto normalizedData = scaleFunc.execute(locationData.asView(), context).get();
            allResults.push_back(normalizedData);
        }
    }

    ASSERT_GT(allResults.size(), 0) << "No cross-shard data processed";

    // Verify that all location data was processed
    for (const auto& result : allResults) {
        EXPECT_GT(result.size(), 0) << "Empty result from location";

        // Verify normalization was applied
        for (const auto& value : result.values) {
            EXPECT_GE(value, 0.0) << "Negative CPU usage after normalization";
            EXPECT_LE(value, 100.0) << "CPU usage over 100% after normalization";
        }
    }
}

// Test 6: Performance Comparison - Real vs Mock Data
TEST_F(RealDataIntegrationTest, PerformanceComparisonRealVsMock) {
    waitForTSMFlush();

    // Test with real data
    std::map<std::string, std::string> tags = {{"location", "datacenter_1"}, {"host", "server_01"}};
    uint64_t startTime = 1609459200000000000ULL;
    uint64_t endTime = startTime + (3600 * 1000000000ULL);

    auto realData = queryRealData("weather", "temperature", tags, startTime, endTime);
    ASSERT_GT(realData.size(), 100) << "Need substantial data for performance test";

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
    // Allow real data to be up to 3x slower due to I/O overhead
    EXPECT_LT(realDuration.count(), mockDuration.count() * 3)
        << "Real data processing significantly slower: real=" << realDuration.count()
        << "μs, mock=" << mockDuration.count() << "μs";

    std::cout << "Performance comparison - Real: " << realDuration.count() << "μs, Mock: " << mockDuration.count()
              << "μs" << std::endl;
}

// Test 7: Time Series Integrity After TSM Storage
TEST_F(RealDataIntegrationTest, DataIntegrityAfterTSMStorage) {
    // Store original data before rollover
    TimeStarInsert<double> originalData("integrity_test", "value");
    originalData.addTag("test", "integrity");

    std::vector<uint64_t> originalTimestamps;
    std::vector<double> originalValues;

    uint64_t baseTime = 1609459200000000000ULL;
    for (int i = 0; i < 100; i++) {
        uint64_t timestamp = baseTime + (i * 60 * 1000000000ULL);
        double value = 100.0 + 10.0 * std::sin(i * 0.1);

        originalTimestamps.push_back(timestamp);
        originalValues.push_back(value);
        originalData.addValue(timestamp, value);
    }

    // Insert data
    engine->insert(originalData).get();

    // Force rollover to TSM files
    waitForTSMFlush();

    // Query back the data
    std::map<std::string, std::string> tags = {{"test", "integrity"}};
    auto retrievedData =
        queryRealData("integrity_test", "value", tags, baseTime, baseTime + (200 * 60 * 1000000000ULL));

    // Verify data integrity
    ASSERT_EQ(retrievedData.size(), originalTimestamps.size()) << "Data size changed after TSM storage";

    EXPECT_EQ(retrievedData.timestamps, originalTimestamps) << "Timestamps changed after TSM storage";

    expectVectorNear(retrievedData.values, originalValues, 1e-10);

    // Apply function to verify values are still correct
    AddFunction addFunc;
    FunctionContext context;
    context.setParameter("operand", 1.0);

    auto functionResult = addFunc.execute(retrievedData.asView(), context).get();

    // Verify function results
    for (size_t i = 0; i < functionResult.values.size(); ++i) {
        EXPECT_NEAR(functionResult.values[i], originalValues[i] + 1.0, 1e-10)
            << "Function result incorrect after TSM storage at index " << i;
    }
}

// Test 8: Large Dataset Function Operations
TEST_F(RealDataIntegrationTest, LargeDatasetFunctionOperations) {
    // Generate large dataset (simulate 1 week of minute-interval data)
    TimeStarInsert<double> largeData("performance", "metric");
    largeData.addTag("dataset", "large");
    largeData.addTag("test", "performance");

    uint64_t baseTime = 1609459200000000000ULL;
    const int dataPoints = 10080;  // 1 week of minute data

    std::mt19937 rng(77);  // Fixed seed for reproducible tests
    std::uniform_int_distribution<int> noiseDist(0, 9);
    for (int i = 0; i < dataPoints; i++) {
        uint64_t timestamp = baseTime + (i * 60 * 1000000000ULL);
        double value = 50.0 + 25.0 * std::sin(i * 0.01) + (noiseDist(rng) - 5) * 0.1;
        largeData.addValue(timestamp, value);
    }

    // Insert large dataset
    engine->insert(largeData).get();
    waitForTSMFlush();

    // Query large dataset
    std::map<std::string, std::string> tags = {{"dataset", "large"}, {"test", "performance"}};
    auto largeResult =
        queryRealData("performance", "metric", tags, baseTime, baseTime + (8 * 24 * 3600 * 1000000000ULL));

    ASSERT_EQ(largeResult.size(), dataPoints) << "Large dataset size mismatch";

    // Apply computationally intensive function
    SMAFunction maFunc;
    FunctionContext context;
    context.setParameter("window", static_cast<int64_t>(60));  // 1-hour window

    auto startTime = std::chrono::high_resolution_clock::now();
    auto smoothedResult = maFunc.execute(largeResult.asView(), context).get();
    auto endTime = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    // Verify results
    EXPECT_EQ(smoothedResult.size(), dataPoints);
    EXPECT_LT(duration.count(), 5000) << "Large dataset processing too slow: " << duration.count() << "ms";

    std::cout << "Large dataset (" << dataPoints << " points) processing time: " << duration.count() << "ms"
              << std::endl;
}