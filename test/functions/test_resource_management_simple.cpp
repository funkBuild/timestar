#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "functions/function_types.hpp"
#include "functions/function_registry.hpp"
#include "functions/arithmetic_functions.hpp"
#include "functions/smoothing_functions.hpp"
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <future>
#include <chrono>
#include <random>
#include <memory>
#include <algorithm>
#include <unistd.h>
#include <fstream>
#include <sys/resource.h>

using namespace timestar::functions;
using namespace std::chrono_literals;

// Simple resource monitoring utilities
class SimpleResourceMonitor {
private:
    std::atomic<size_t> initialMemoryUsage_{0};
    
    static size_t getCurrentRSS() {
        std::ifstream statFile("/proc/self/status");
        std::string line;
        while (std::getline(statFile, line)) {
            if (line.substr(0, 6) == "VmRSS:") {
                size_t pos = line.find_first_of("0123456789");
                if (pos != std::string::npos) {
                    return std::stoull(line.substr(pos)) * 1024; // Convert KB to bytes
                }
            }
        }
        return 0;
    }

public:
    void recordInitialMemory() {
        initialMemoryUsage_ = getCurrentRSS();
    }

    size_t getCurrentMemoryUsage() const {
        return getCurrentRSS();
    }

    size_t getMemoryGrowth() const {
        size_t current = getCurrentRSS();
        size_t initial = initialMemoryUsage_;
        return current > initial ? current - initial : 0;
    }

    double getMemoryGrowthPercent() const {
        size_t initial = initialMemoryUsage_;
        if (initial == 0) return 0.0;
        
        size_t current = getCurrentRSS();
        return ((double)(current - initial) / initial) * 100.0;
    }
};

// Memory-intensive test function
class TestMemoryIntensiveFunction : public IUnaryFunction {
private:
    mutable std::vector<std::vector<double>> memoryLeakStorage_;
    
public:
    seastar::future<FunctionResult<double>> execute(
        const DoubleSeriesView& input,
        const FunctionContext& context
    ) const override {
        
        // Simulate memory-intensive operation
        size_t memorySize = context.getParameter<int64_t>("memory_size", 100000);
        bool causeMemoryLeak = context.getParameter<bool>("cause_leak", false);
        
        // Create large memory allocation
        std::vector<double> largeBuffer(memorySize, 1.0);
        
        if (causeMemoryLeak) {
            // Intentionally leak memory for testing
            memoryLeakStorage_.emplace_back(largeBuffer);
        }
        
        // Perform computation on input data
        FunctionResult<double> result;
        result.timestamps.reserve(input.size());
        result.values.reserve(input.size());
        
        for (size_t i = 0; i < input.size(); ++i) {
            result.timestamps.push_back(input.timestampAt(i));
            // Use the large buffer in computation
            double computedValue = input.valueAt(i) + (largeBuffer.empty() ? 0.0 : largeBuffer[i % largeBuffer.size()]);
            result.values.push_back(computedValue);
        }
        
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    seastar::future<bool> validateParameters(const FunctionContext&) const override {
        return seastar::make_ready_future<bool>(true);
    }

    const FunctionMetadata& getMetadata() const override {
        static FunctionMetadata metadata{
            "test_memory_intensive",
            "Memory-intensive function for resource testing",
            FunctionCategory::OTHER,
            {"double"},
            "double",
            {
                {"memory_size", "int64_t", false, "Size of memory buffer", "100000"},
                {"cause_leak", "bool", false, "Whether to cause memory leak", "false"}
            },
            true, // supportsVectorization
            true, // supportsStreaming
            1,    // minDataPoints
            {}    // examples
        };
        return metadata;
    }

    std::unique_ptr<IFunction> clone() const override {
        return std::make_unique<TestMemoryIntensiveFunction>();
    }
};

// CPU-intensive test function
class TestCPUIntensiveFunction : public IUnaryFunction {
public:
    seastar::future<FunctionResult<double>> execute(
        const DoubleSeriesView& input,
        const FunctionContext& context
    ) const override {
        
        int64_t iterations = context.getParameter<int64_t>("iterations", 10000);
        
        FunctionResult<double> result;
        result.timestamps.reserve(input.size());
        result.values.reserve(input.size());
        
        for (size_t i = 0; i < input.size(); ++i) {
            result.timestamps.push_back(input.timestampAt(i));
            
            // CPU-intensive computation
            double value = input.valueAt(i);
            for (int64_t iter = 0; iter < iterations; ++iter) {
                value = std::sin(value) * std::cos(value) + std::sqrt(std::abs(value));
                // Prevent optimization
                if (iter % 1000000 == 0) {
                    volatile double temp = value;
                    (void)temp;
                }
            }
            
            result.values.push_back(value);
        }
        
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    seastar::future<bool> validateParameters(const FunctionContext&) const override {
        return seastar::make_ready_future<bool>(true);
    }

    const FunctionMetadata& getMetadata() const override {
        static FunctionMetadata metadata{
            "test_cpu_intensive",
            "CPU-intensive function for resource testing",
            FunctionCategory::OTHER,
            {"double"},
            "double",
            {
                {"iterations", "int64_t", false, "Number of computation iterations", "10000"}
            },
            true, // supportsVectorization
            true, // supportsStreaming
            1,    // minDataPoints
            {}    // examples
        };
        return metadata;
    }

    std::unique_ptr<IFunction> clone() const override {
        return std::make_unique<TestCPUIntensiveFunction>();
    }
};

// Main test fixture
class ResourceManagementTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clear registry
        FunctionRegistry::getInstance().clear();
        
        // Register test functions
        FunctionRegistry::getInstance().registerFunction<TestMemoryIntensiveFunction>(
            TestMemoryIntensiveFunction().getMetadata());
        FunctionRegistry::getInstance().registerFunction<TestCPUIntensiveFunction>(
            TestCPUIntensiveFunction().getMetadata());
        
        // Setup test data
        setupTestData();
    }
    
    void TearDown() override {
        FunctionRegistry::getInstance().clear();
    }

private:
    void setupTestData() {
        // Create test time series data
        testTimestamps_.clear();
        testValues_.clear();
        
        auto currentTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        
        for (size_t i = 0; i < 1000; ++i) {
            testTimestamps_.push_back(currentTime + i * 1000000000ULL); // 1 second intervals
            testValues_.push_back(static_cast<double>(i) + std::sin(i * 0.1));
        }
        
        testSeriesView_ = DoubleSeriesView(&testTimestamps_, &testValues_);
    }

protected:
    std::vector<uint64_t> testTimestamps_;
    std::vector<double> testValues_;
    DoubleSeriesView testSeriesView_;
};

// Test 1: Basic Memory Usage Monitoring
TEST_F(ResourceManagementTest, BasicMemoryUsageMonitoring) {
    SimpleResourceMonitor monitor;
    monitor.recordInitialMemory();
    
    FunctionContext context;
    context.setParameter("memory_size", static_cast<int64_t>(100000));
    context.setParameter("cause_leak", false);
    
    auto function = FunctionRegistry::getInstance().createFunction("test_memory_intensive");
    ASSERT_NE(function, nullptr);
    
    auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
    ASSERT_NE(unaryFunction, nullptr);
    
    // Execute function
    auto result = unaryFunction->execute(testSeriesView_, context).get();
    EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
    
    // Allow some time for memory cleanup
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Memory growth should be reasonable after execution
    double memoryGrowth = monitor.getMemoryGrowthPercent();
    EXPECT_LT(memoryGrowth, 50.0) << "Memory growth should be reasonable after function execution";
}

// Test 2: Memory Leak Detection
TEST_F(ResourceManagementTest, BasicMemoryLeakDetection) {
    SimpleResourceMonitor monitor;
    monitor.recordInitialMemory();
    
    // Test function that doesn't leak memory
    {
        FunctionContext context;
        context.setParameter("memory_size", static_cast<int64_t>(50000));
        context.setParameter("cause_leak", false);
        
        auto function = FunctionRegistry::getInstance().createFunction("test_memory_intensive");
        ASSERT_NE(function, nullptr);
        
        auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
        ASSERT_NE(unaryFunction, nullptr);
        
        // Execute function multiple times
        for (int i = 0; i < 5; ++i) {
            auto result = unaryFunction->execute(testSeriesView_, context).get();
            EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
        }
    }
    
    // Small delay for memory cleanup
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Should not detect significant memory growth
    double memoryGrowth = monitor.getMemoryGrowthPercent();
    EXPECT_LT(memoryGrowth, 10.0) << "Should not have significant memory growth in non-leaking function";
    
    // Test function that leaks memory
    monitor.recordInitialMemory(); // Reset baseline
    {
        FunctionContext context;
        context.setParameter("memory_size", static_cast<int64_t>(200000));
        context.setParameter("cause_leak", true);
        
        auto function = FunctionRegistry::getInstance().createFunction("test_memory_intensive");
        ASSERT_NE(function, nullptr);
        
        auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
        ASSERT_NE(unaryFunction, nullptr);
        
        // Execute function multiple times to accumulate leaks
        for (int i = 0; i < 3; ++i) {
            auto result = unaryFunction->execute(testSeriesView_, context).get();
            EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
        }
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Should detect memory leak
    double memoryGrowthAfterLeak = monitor.getMemoryGrowthPercent();
    EXPECT_GT(memoryGrowthAfterLeak, 5.0) << "Should detect intentional memory leak";
}

// Test 3: CPU Usage and Performance
TEST_F(ResourceManagementTest, CPUUsageAndPerformance) {
    FunctionContext context;
    context.setParameter("iterations", static_cast<int64_t>(5000)); // Moderate CPU usage
    
    auto function = FunctionRegistry::getInstance().createFunction("test_cpu_intensive");
    ASSERT_NE(function, nullptr);
    
    auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
    ASSERT_NE(unaryFunction, nullptr);
    
    // Measure execution time
    auto start = std::chrono::steady_clock::now();
    auto result = unaryFunction->execute(testSeriesView_, context).get();
    auto executionTime = std::chrono::steady_clock::now() - start;
    
    EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
    
    // Should complete within reasonable time
    auto executionMs = std::chrono::duration_cast<std::chrono::milliseconds>(executionTime).count();
    EXPECT_LT(executionMs, 10000) << "CPU-intensive function should complete within 10 seconds";
    EXPECT_GT(executionMs, 10) << "CPU-intensive function should take some measurable time";
}

// Test 4: Resource Cleanup After Function Execution
TEST_F(ResourceManagementTest, ResourceCleanupAfterExecution) {
    SimpleResourceMonitor monitor;
    monitor.recordInitialMemory();
    
    // Execute multiple functions and verify cleanup
    std::vector<std::string> functionNames = {"test_memory_intensive", "test_cpu_intensive"};
    
    for (const auto& funcName : functionNames) {
        auto function = FunctionRegistry::getInstance().createFunction(funcName);
        ASSERT_NE(function, nullptr);
        
        auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
        ASSERT_NE(unaryFunction, nullptr);
        
        FunctionContext context;
        if (funcName == "test_memory_intensive") {
            context.setParameter("memory_size", static_cast<int64_t>(50000));
            context.setParameter("cause_leak", false);
        } else if (funcName == "test_cpu_intensive") {
            context.setParameter("iterations", static_cast<int64_t>(1000));
        }
        
        // Execute function
        auto result = unaryFunction->execute(testSeriesView_, context).get();
        EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
        
        // Force cleanup by destroying function
        function.reset();
    }
    
    // Allow time for cleanup
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    
    // Should not have significant memory growth after cleanup
    double memoryGrowth = monitor.getMemoryGrowthPercent();
    EXPECT_LT(memoryGrowth, 15.0) << "Resource cleanup should prevent significant memory growth";
}

// Test 5: Concurrent Function Execution
TEST_F(ResourceManagementTest, ConcurrentFunctionExecution) {
    std::atomic<int> successfulExecutions(0);
    std::atomic<int> failedExecutions(0);
    std::vector<std::thread> executionThreads;
    
    // Execute functions concurrently
    for (int i = 0; i < 5; ++i) {
        executionThreads.emplace_back([&, i]() {
            try {
                auto function = FunctionRegistry::getInstance().createFunction("test_memory_intensive");
                if (function) {
                    auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
                    if (unaryFunction) {
                        FunctionContext context;
                        context.setParameter("memory_size", static_cast<int64_t>(10000));
                        context.setParameter("cause_leak", false);
                        
                        auto result = unaryFunction->execute(testSeriesView_, context).get();
                        if (result.timestamps.size() == testSeriesView_.size()) {
                            successfulExecutions++;
                        } else {
                            failedExecutions++;
                        }
                    } else {
                        failedExecutions++;
                    }
                } else {
                    failedExecutions++;
                }
            } catch (const std::exception& e) {
                failedExecutions++;
            }
        });
    }
    
    // Wait for all executions to complete
    for (auto& thread : executionThreads) {
        thread.join();
    }
    
    int totalExecutions = successfulExecutions + failedExecutions;
    EXPECT_EQ(totalExecutions, 5) << "All execution attempts should be accounted for";
    
    // Most executions should succeed
    EXPECT_GE(successfulExecutions.load(), 3) << "Most concurrent executions should succeed";
}

// Test 6: Large Dataset Handling
TEST_F(ResourceManagementTest, LargeDatasetHandling) {
    // Create large dataset
    std::vector<uint64_t> largeTimestamps;
    std::vector<double> largeValues;
    
    auto currentTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    for (size_t i = 0; i < 10000; ++i) { // 10K points
        largeTimestamps.push_back(currentTime + i * 1000000ULL); // 1ms intervals
        largeValues.push_back(static_cast<double>(i) * 0.001 + std::sin(i * 0.001));
    }
    
    DoubleSeriesView largeSeriesView(&largeTimestamps, &largeValues);
    
    SimpleResourceMonitor monitor;
    monitor.recordInitialMemory();
    
    FunctionContext context;
    context.setParameter("memory_size", static_cast<int64_t>(5000));
    context.setParameter("cause_leak", false);
    
    auto function = FunctionRegistry::getInstance().createFunction("test_memory_intensive");
    ASSERT_NE(function, nullptr);
    
    auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
    ASSERT_NE(unaryFunction, nullptr);
    
    // Execute with large dataset
    auto start = std::chrono::steady_clock::now();
    auto result = unaryFunction->execute(largeSeriesView, context).get();
    auto executionTime = std::chrono::steady_clock::now() - start;
    
    EXPECT_EQ(result.timestamps.size(), largeSeriesView.size());
    
    // Check performance characteristics
    auto executionMs = std::chrono::duration_cast<std::chrono::milliseconds>(executionTime).count();
    EXPECT_LT(executionMs, 5000) << "Execution should complete within reasonable time for large dataset";
    
    // Check memory usage after processing
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    double memoryGrowth = monitor.getMemoryGrowthPercent();
    EXPECT_LT(memoryGrowth, 30.0) << "Memory growth should be reasonable after large dataset processing";
}

// Test 7: Repeated Function Calls Memory Stability
TEST_F(ResourceManagementTest, RepeatedCallsMemoryStability) {
    SimpleResourceMonitor monitor;
    
    FunctionContext context;
    context.setParameter("memory_size", static_cast<int64_t>(20000));
    context.setParameter("cause_leak", false);
    
    auto function = FunctionRegistry::getInstance().createFunction("test_memory_intensive");
    ASSERT_NE(function, nullptr);
    
    auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
    ASSERT_NE(unaryFunction, nullptr);
    
    std::vector<double> memoryGrowthSamples;
    
    // Execute function multiple times and monitor memory
    for (int i = 0; i < 10; ++i) {
        monitor.recordInitialMemory(); // Reset baseline
        
        auto result = unaryFunction->execute(testSeriesView_, context).get();
        EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        memoryGrowthSamples.push_back(monitor.getMemoryGrowthPercent());
    }
    
    // Calculate average memory growth
    double avgMemoryGrowth = 0.0;
    for (double growth : memoryGrowthSamples) {
        avgMemoryGrowth += growth;
    }
    avgMemoryGrowth /= memoryGrowthSamples.size();
    
    // Average memory growth should be stable and reasonable
    EXPECT_LT(avgMemoryGrowth, 20.0) << "Average memory growth should be reasonable across repeated calls";
    
    // Check for stability (variance shouldn't be too high)
    double variance = 0.0;
    for (double growth : memoryGrowthSamples) {
        variance += (growth - avgMemoryGrowth) * (growth - avgMemoryGrowth);
    }
    variance /= memoryGrowthSamples.size();
    double stddev = std::sqrt(variance);
    
    EXPECT_LT(stddev, 10.0) << "Memory growth should be stable across repeated calls";
}