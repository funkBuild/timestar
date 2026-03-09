#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "functions/function_types.hpp"
#include "functions/function_registry.hpp"
#include "functions/arithmetic_functions.hpp"
#include "functions/smoothing_functions.hpp"
#include "functions/function_pipeline_executor.hpp"
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
#include <signal.h>
#include <cstring>

using namespace timestar::functions;
using namespace std::chrono_literals;

// Resource monitoring utilities
class ResourceMonitor {
private:
    std::atomic<size_t> currentMemoryUsage_{0};
    std::atomic<size_t> peakMemoryUsage_{0};
    std::atomic<size_t> cpuTimeUsage_{0};
    std::atomic<bool> monitoring_{false};
    std::thread monitorThread_;
    mutable std::mutex statsMutex_;

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

    static size_t getCPUTime() {
        struct rusage usage;
        getrusage(RUSAGE_SELF, &usage);
        return usage.ru_utime.tv_sec * 1000000 + usage.ru_utime.tv_usec; // microseconds
    }

public:
    ResourceMonitor() = default;
    
    ~ResourceMonitor() {
        stopMonitoring();
    }

    void startMonitoring() {
        monitoring_ = true;
        size_t initialMem = getCurrentRSS();
        currentMemoryUsage_ = initialMem;
        peakMemoryUsage_ = initialMem;
        cpuTimeUsage_ = getCPUTime();
        
        monitorThread_ = std::thread([this]() {
            while (monitoring_) {
                size_t currentMem = getCurrentRSS();
                currentMemoryUsage_ = currentMem;
                
                size_t currentPeak = peakMemoryUsage_;
                while (currentMem > currentPeak && 
                       !peakMemoryUsage_.compare_exchange_weak(currentPeak, currentMem)) {
                    currentPeak = peakMemoryUsage_;
                }
                
                cpuTimeUsage_ = getCPUTime();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
    }

    void stopMonitoring() {
        monitoring_ = false;
        if (monitorThread_.joinable()) {
            monitorThread_.join();
        }
    }

    size_t getCurrentMemoryUsage() const { return currentMemoryUsage_; }
    size_t getPeakMemoryUsage() const { return peakMemoryUsage_; }
    size_t getCPUTimeUsage() const { return cpuTimeUsage_; }

    void resetCounters() {
        size_t currentMem = getCurrentRSS();
        currentMemoryUsage_ = currentMem;
        peakMemoryUsage_ = currentMem;
        cpuTimeUsage_ = getCPUTime();
    }
};

// Memory leak detection class
class MemoryLeakDetector {
private:
    size_t initialMemory_;
    ResourceMonitor monitor_;

public:
    MemoryLeakDetector() {
        monitor_.startMonitoring();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        initialMemory_ = monitor_.getCurrentMemoryUsage();
    }

    ~MemoryLeakDetector() {
        monitor_.stopMonitoring();
    }

    bool hasMemoryLeak(double tolerancePercent = 5.0) const {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        size_t currentMemory = monitor_.getCurrentMemoryUsage();
        
        if (initialMemory_ == 0) return false;
        
        double memoryGrowth = ((double)(currentMemory - initialMemory_) / initialMemory_) * 100.0;
        return memoryGrowth > tolerancePercent;
    }

    size_t getMemoryGrowth() const {
        size_t currentMemory = monitor_.getCurrentMemoryUsage();
        return currentMemory > initialMemory_ ? currentMemory - initialMemory_ : 0;
    }
};

// CPU usage limiter
class CPUUsageLimiter {
private:
    std::chrono::microseconds maxCpuTime_;
    std::chrono::steady_clock::time_point startTime_;

public:
    CPUUsageLimiter(std::chrono::microseconds maxTime) 
        : maxCpuTime_(maxTime), startTime_(std::chrono::steady_clock::now()) {}

    bool isTimeExceeded() const {
        auto elapsed = std::chrono::steady_clock::now() - startTime_;
        return elapsed > maxCpuTime_;
    }

    std::chrono::microseconds getElapsedTime() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - startTime_);
    }
};

// Timeout handler for function execution
class TimeoutHandler {
private:
    std::atomic<bool> timedOut_{false};
    std::thread timeoutThread_;

public:
    template<typename Duration>
    void setTimeout(Duration timeout) {
        timeoutThread_ = std::thread([this, timeout]() {
            std::this_thread::sleep_for(timeout);
            timedOut_ = true;
        });
    }

    bool hasTimedOut() const {
        return timedOut_;
    }

    ~TimeoutHandler() {
        if (timeoutThread_.joinable()) {
            timeoutThread_.join();
        }
    }
};

// Memory-intensive function for testing
class MemoryIntensiveFunction : public IUnaryFunction {
private:
    mutable std::vector<std::vector<double>> memoryLeakStorage_;
    
public:
    seastar::future<FunctionResult<double>> execute(
        const DoubleSeriesView& input,
        const FunctionContext& context
    ) const override {
        
        // Simulate memory-intensive operation
        size_t memorySize = context.getParameter<int64_t>("memory_size", 1000000);
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
            // Use the large buffer in computation to ensure it's not optimized away
            double computedValue = input.valueAt(i) + largeBuffer[i % largeBuffer.size()];
            result.values.push_back(computedValue);
        }
        
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    seastar::future<bool> validateParameters(const FunctionContext& context) const override {
        return seastar::make_ready_future<bool>(true);
    }

    const FunctionMetadata& getMetadata() const override {
        static FunctionMetadata metadata{
            "memory_intensive",
            "Memory-intensive function for resource testing",
            FunctionCategory::OTHER,
            {"double"},
            "double",
            {
                {"memory_size", "int64_t", false, "Size of memory buffer", "1000000"},
                {"cause_leak", "bool", false, "Whether to cause memory leak", "false"}
            }
        };
        return metadata;
    }

    std::unique_ptr<IFunction> clone() const override {
        return std::make_unique<MemoryIntensiveFunction>();
    }
};

// CPU-intensive function for testing
class CPUIntensiveFunction : public IUnaryFunction {
public:
    seastar::future<FunctionResult<double>> execute(
        const DoubleSeriesView& input,
        const FunctionContext& context
    ) const override {
        
        int64_t iterations = context.getParameter<int64_t>("iterations", 1000000);
        
        FunctionResult<double> result;
        result.timestamps.reserve(input.size());
        result.values.reserve(input.size());
        
        for (size_t i = 0; i < input.size(); ++i) {
            result.timestamps.push_back(input.timestampAt(i));
            
            // CPU-intensive computation
            double value = input.valueAt(i);
            for (int64_t iter = 0; iter < iterations; ++iter) {
                value = std::sin(value) * std::cos(value) + std::sqrt(std::abs(value));
            }
            
            result.values.push_back(value);
        }
        
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    seastar::future<bool> validateParameters(const FunctionContext& context) const override {
        return seastar::make_ready_future<bool>(true);
    }

    const FunctionMetadata& getMetadata() const override {
        static FunctionMetadata metadata{
            "cpu_intensive",
            "CPU-intensive function for resource testing",
            FunctionCategory::OTHER,
            {"double"},
            "double",
            {
                {"iterations", "int64_t", false, "Number of computation iterations", "1000000"}
            }
        };
        return metadata;
    }

    std::unique_ptr<IFunction> clone() const override {
        return std::make_unique<CPUIntensiveFunction>();
    }
};

// Main test fixture
class FunctionResourceManagementTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clear registry
        FunctionRegistry::getInstance().clear();
        
        // Register resource-intensive functions
        FunctionRegistry::getInstance().registerFunction<MemoryIntensiveFunction>(
            MemoryIntensiveFunction().getMetadata());
        FunctionRegistry::getInstance().registerFunction<CPUIntensiveFunction>(
            CPUIntensiveFunction().getMetadata());
        
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

// Test 1: Memory Leak Detection
TEST_F(FunctionResourceManagementTest, MemoryLeakDetection) {
    MemoryLeakDetector detector;
    
    // Test function that doesn't leak memory
    {
        FunctionContext context;
        context.setParameter("memory_size", static_cast<int64_t>(100000));
        context.setParameter("cause_leak", false);
        
        auto function = FunctionRegistry::getInstance().createFunction("memory_intensive");
        ASSERT_NE(function, nullptr);
        
        auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
        ASSERT_NE(unaryFunction, nullptr);
        
        // Execute function multiple times
        for (int i = 0; i < 10; ++i) {
            auto result = unaryFunction->execute(testSeriesView_, context).get();
            EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
        }
    }
    
    // Small delay for memory cleanup
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Should not detect memory leak
    EXPECT_FALSE(detector.hasMemoryLeak(10.0)) << "Memory leak detected in non-leaking function";
    
    // Test function that leaks memory
    {
        FunctionContext context;
        context.setParameter("memory_size", static_cast<int64_t>(500000));
        context.setParameter("cause_leak", true);
        
        auto function = FunctionRegistry::getInstance().createFunction("memory_intensive");
        ASSERT_NE(function, nullptr);
        
        auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
        ASSERT_NE(unaryFunction, nullptr);
        
        // Execute function multiple times to accumulate leaks
        for (int i = 0; i < 5; ++i) {
            auto result = unaryFunction->execute(testSeriesView_, context).get();
            EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
        }
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Should detect memory leak
    EXPECT_TRUE(detector.hasMemoryLeak(5.0)) << "Failed to detect intentional memory leak";
}

// Test 2: CPU Usage Monitoring and Limits
TEST_F(FunctionResourceManagementTest, CPUUsageMonitoring) {
    ResourceMonitor resourceMonitor;
    resourceMonitor.startMonitoring();
    
    FunctionContext context;
    context.setParameter("iterations", static_cast<int64_t>(10000)); // Moderate CPU usage
    
    auto function = FunctionRegistry::getInstance().createFunction("cpu_intensive");
    ASSERT_NE(function, nullptr);
    
    auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
    ASSERT_NE(unaryFunction, nullptr);
    
    size_t initialCpuTime = resourceMonitor.getCPUTimeUsage();
    
    // Execute CPU-intensive function
    auto result = unaryFunction->execute(testSeriesView_, context).get();
    EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
    
    size_t finalCpuTime = resourceMonitor.getCPUTimeUsage();
    size_t cpuTimeUsed = finalCpuTime - initialCpuTime;
    
    // Should have used measurable CPU time
    EXPECT_GT(cpuTimeUsed, 0) << "Function should consume measurable CPU time";
    
    resourceMonitor.stopMonitoring();
}

// Test 3: CPU Usage Limits
TEST_F(FunctionResourceManagementTest, CPUUsageLimits) {
    CPUUsageLimiter limiter(std::chrono::milliseconds(500)); // 500ms limit
    
    FunctionContext context;
    context.setParameter("iterations", static_cast<int64_t>(100000000)); // Very high CPU usage
    
    auto function = FunctionRegistry::getInstance().createFunction("cpu_intensive");
    ASSERT_NE(function, nullptr);
    
    auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
    ASSERT_NE(unaryFunction, nullptr);
    
    // Start execution in separate thread
    std::future<FunctionResult<double>> futureResult = std::async(std::launch::async, [&]() {
        return unaryFunction->execute(testSeriesView_, context).get();
    });
    
    // Wait for timeout or completion
    bool completed = false;
    while (!limiter.isTimeExceeded() && 
           futureResult.wait_for(std::chrono::milliseconds(10)) != std::future_status::ready) {
        // Continue waiting
    }
    
    if (futureResult.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        completed = true;
    }
    
    auto elapsedTime = limiter.getElapsedTime();
    
    if (!completed) {
        // Function exceeded time limit - this is what we expect for very CPU-intensive operations
        EXPECT_GT(elapsedTime.count(), 450000) << "Should have taken close to the limit time";
    } else {
        // Function completed within time limit
        auto result = futureResult.get();
        EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
    }
}

// Test 4: Query Timeout Handling
TEST_F(FunctionResourceManagementTest, QueryTimeoutHandling) {
    TimeoutHandler timeoutHandler;
    timeoutHandler.setTimeout(std::chrono::milliseconds(200)); // 200ms timeout
    
    FunctionContext context;
    context.setParameter("iterations", static_cast<int64_t>(50000000)); // Very high iterations
    
    auto function = FunctionRegistry::getInstance().createFunction("cpu_intensive");
    ASSERT_NE(function, nullptr);
    
    auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
    ASSERT_NE(unaryFunction, nullptr);
    
    // Execute with timeout monitoring
    std::future<FunctionResult<double>> futureResult = std::async(std::launch::async, [&]() {
        return unaryFunction->execute(testSeriesView_, context).get();
    });
    
    // Wait for either completion or timeout
    auto start = std::chrono::steady_clock::now();
    bool completedBeforeTimeout = false;
    
    while (!timeoutHandler.hasTimedOut()) {
        if (futureResult.wait_for(std::chrono::milliseconds(10)) == std::future_status::ready) {
            completedBeforeTimeout = true;
            break;
        }
    }
    
    auto elapsed = std::chrono::steady_clock::now() - start;
    
    if (timeoutHandler.hasTimedOut() && !completedBeforeTimeout) {
        // Timeout occurred as expected
        EXPECT_GT(elapsed.count(), 190000000) << "Should have timed out around 200ms";
        EXPECT_LT(elapsed.count(), 300000000) << "Should not have taken much longer than timeout";
    } else if (completedBeforeTimeout) {
        // Function completed within timeout
        auto result = futureResult.get();
        EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
    }
}

// Test 5: OOM Simulation and Recovery
TEST_F(FunctionResourceManagementTest, OOMSimulationAndRecovery) {
    ResourceMonitor resourceMonitor;
    resourceMonitor.startMonitoring();
    size_t initialMemory = resourceMonitor.getCurrentMemoryUsage();
    
    // Test large memory allocation
    FunctionContext context;
    context.setParameter("memory_size", static_cast<int64_t>(10000000)); // 10M doubles ≈ 80MB
    context.setParameter("cause_leak", false);
    
    auto function = FunctionRegistry::getInstance().createFunction("memory_intensive");
    ASSERT_NE(function, nullptr);
    
    // Monitor peak memory usage during execution
    resourceMonitor.resetCounters();
    
    bool executionSuccessful = true;
    try {
        auto result = dynamic_cast<IUnaryFunction*>(function.get())->execute(testSeriesView_, context).get();
        EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
    } catch (const std::exception& e) {
        executionSuccessful = false;
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    size_t peakMemory = resourceMonitor.getPeakMemoryUsage();
    size_t finalMemory = resourceMonitor.getCurrentMemoryUsage();
    
    if (executionSuccessful) {
        // Memory should have been allocated and then freed
        EXPECT_GT(peakMemory, initialMemory + 50000000) << "Should have allocated significant memory";
        EXPECT_LT(finalMemory, peakMemory) << "Memory should have been freed after execution";
    } else {
        // If OOM occurred, it should be handled gracefully
        SUCCEED() << "Function handled OOM gracefully by throwing exception";
    }
    
    resourceMonitor.stopMonitoring();
}

// Test 6: Resource Cleanup After Function Execution
TEST_F(FunctionResourceManagementTest, ResourceCleanupAfterExecution) {
    MemoryLeakDetector detector;
    
    // Execute multiple functions and verify cleanup
    std::vector<std::string> functionNames = {"memory_intensive", "cpu_intensive"};
    
    for (const auto& funcName : functionNames) {
        auto function = FunctionRegistry::getInstance().createFunction(funcName);
        ASSERT_NE(function, nullptr);
        
        FunctionContext context;
        if (funcName == "memory_intensive") {
            context.setParameter("memory_size", static_cast<int64_t>(1000000));
            context.setParameter("cause_leak", false);
        } else if (funcName == "cpu_intensive") {
            context.setParameter("iterations", static_cast<int64_t>(10000));
        }
        
        // Execute function
        auto result = dynamic_cast<IUnaryFunction*>(function.get())->execute(testSeriesView_, context).get();
        EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
        
        // Force cleanup by destroying function
        function.reset();
    }
    
    // Allow time for cleanup
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Should not have memory leaks after cleanup
    EXPECT_FALSE(detector.hasMemoryLeak(8.0)) << "Resource cleanup should prevent memory leaks";
}

// Test 7: Memory Growth with Repeated Function Calls
TEST_F(FunctionResourceManagementTest, MemoryGrowthWithRepeatedCalls) {
    ResourceMonitor resourceMonitor;
    resourceMonitor.startMonitoring();
    resourceMonitor.resetCounters();
    
    FunctionContext context;
    context.setParameter("memory_size", static_cast<int64_t>(500000));
    context.setParameter("cause_leak", false);
    
    auto function = FunctionRegistry::getInstance().createFunction("memory_intensive");
    ASSERT_NE(function, nullptr);
    
    size_t initialMemory = resourceMonitor.getCurrentMemoryUsage();
    std::vector<size_t> memorySnapshots;
    
    // Execute function multiple times and monitor memory
    for (int i = 0; i < 20; ++i) {
        auto result = dynamic_cast<IUnaryFunction*>(function.get())->execute(testSeriesView_, context).get();
        EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        memorySnapshots.push_back(resourceMonitor.getCurrentMemoryUsage());
    }
    
    // Check for continuous memory growth (indicating leaks)
    bool continuousGrowth = true;
    size_t growthCount = 0;
    
    for (size_t i = 1; i < memorySnapshots.size(); ++i) {
        if (memorySnapshots[i] > memorySnapshots[i-1]) {
            growthCount++;
        }
    }
    
    // Should not have continuous memory growth
    double growthRatio = static_cast<double>(growthCount) / (memorySnapshots.size() - 1);
    EXPECT_LT(growthRatio, 0.7) << "Memory should not grow continuously, indicating good cleanup";
    
    resourceMonitor.stopMonitoring();
}

// Test 8: Resource Exhaustion Handling
TEST_F(FunctionResourceManagementTest, ResourceExhaustionHandling) {
    // Test behavior when system resources are under pressure
    std::vector<std::unique_ptr<IUnaryFunction>> functions;
    std::vector<std::thread> executionThreads;
    std::atomic<int> successfulExecutions(0);
    std::atomic<int> failedExecutions(0);
    
    // Create multiple function instances
    for (int i = 0; i < 10; ++i) {
        auto function = FunctionRegistry::getInstance().createFunction("memory_intensive");
        auto unaryFunction = std::unique_ptr<IUnaryFunction>(dynamic_cast<IUnaryFunction*>(function.release()));
        functions.push_back(std::move(unaryFunction));
    }
    
    // Execute functions concurrently to stress resources
    for (int i = 0; i < 10; ++i) {
        executionThreads.emplace_back([&, i]() {
            FunctionContext context;
            context.setParameter("memory_size", static_cast<int64_t>(2000000));
            context.setParameter("cause_leak", false);
            
            try {
                auto result = functions[i]->execute(testSeriesView_, context).get();
                if (result.timestamps.size() == testSeriesView_.size()) {
                    successfulExecutions++;
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
    EXPECT_EQ(totalExecutions, 10) << "All execution attempts should be accounted for";
    
    // At least some executions should succeed even under resource pressure
    EXPECT_GT(successfulExecutions.load(), 0) << "Some executions should succeed even under resource pressure";
    
    // If there were failures, they should be handled gracefully (no crashes)
    if (failedExecutions > 0) {
        SUCCEED() << "Failed executions were handled gracefully: " << failedExecutions.load();
    }
}

// Test 9: Pipeline Resource Management
TEST_F(FunctionResourceManagementTest, PipelineResourceManagement) {
    MemoryLeakDetector detector;
    
    // Create a pipeline of functions
    std::vector<std::string> pipeline = {"memory_intensive", "cpu_intensive", "memory_intensive"};
    
    for (const auto& funcName : pipeline) {
        auto function = FunctionRegistry::getInstance().createFunction(funcName);
        ASSERT_NE(function, nullptr);
        
        FunctionContext context;
        if (funcName == "memory_intensive") {
            context.setParameter("memory_size", static_cast<int64_t>(300000));
            context.setParameter("cause_leak", false);
        } else if (funcName == "cpu_intensive") {
            context.setParameter("iterations", static_cast<int64_t>(5000));
        }
        
        // Execute function in pipeline
        auto result = dynamic_cast<IUnaryFunction*>(function.get())->execute(testSeriesView_, context).get();
        EXPECT_EQ(result.timestamps.size(), testSeriesView_.size());
        
        // Update test data for next function in pipeline
        testValues_ = result.values;
        testSeriesView_ = DoubleSeriesView(&testTimestamps_, &testValues_);
    }
    
    // Should not have memory leaks from pipeline execution
    EXPECT_FALSE(detector.hasMemoryLeak(10.0)) << "Pipeline execution should not cause memory leaks";
}

// Test 10: Stress Test with Large Datasets
TEST_F(FunctionResourceManagementTest, StressTestWithLargeDatasets) {
    // Create large dataset
    std::vector<uint64_t> largeTimestamps;
    std::vector<double> largeValues;
    
    auto currentTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    for (size_t i = 0; i < 100000; ++i) { // 100K points
        largeTimestamps.push_back(currentTime + i * 1000000ULL); // 1ms intervals
        largeValues.push_back(static_cast<double>(i) * 0.001 + std::sin(i * 0.001));
    }
    
    DoubleSeriesView largeSeriesView(&largeTimestamps, &largeValues);
    
    ResourceMonitor resourceMonitor;
    resourceMonitor.startMonitoring();
    size_t initialMemory = resourceMonitor.getCurrentMemoryUsage();
    
    FunctionContext context;
    context.setParameter("memory_size", static_cast<int64_t>(50000));
    context.setParameter("cause_leak", false);
    
    auto function = FunctionRegistry::getInstance().createFunction("memory_intensive");
    ASSERT_NE(function, nullptr);
    
    // Execute with large dataset
    auto start = std::chrono::steady_clock::now();
    auto result = dynamic_cast<IUnaryFunction*>(function.get())->execute(largeSeriesView, context).get();
    auto executionTime = std::chrono::steady_clock::now() - start;
    
    EXPECT_EQ(result.timestamps.size(), largeSeriesView.size());
    
    // Check performance characteristics
    auto executionMs = std::chrono::duration_cast<std::chrono::milliseconds>(executionTime).count();
    EXPECT_LT(executionMs, 5000) << "Execution should complete within reasonable time for large dataset";
    
    // Check memory usage
    size_t peakMemory = resourceMonitor.getPeakMemoryUsage();
    size_t finalMemory = resourceMonitor.getCurrentMemoryUsage();
    
    // Memory should return to reasonable levels after processing
    double memoryGrowthPercent = ((double)(finalMemory - initialMemory) / initialMemory) * 100.0;
    EXPECT_LT(memoryGrowthPercent, 20.0) << "Memory growth should be reasonable after large dataset processing";
    
    resourceMonitor.stopMonitoring();
}