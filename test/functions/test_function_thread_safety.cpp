#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "functions/function_types.hpp"
#include "functions/function_registry.hpp"
#include "functions/arithmetic_functions.hpp"
#include "functions/smoothing_functions.hpp"
#include "functions/vectorized_series.hpp"
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

using namespace timestar::functions;
using namespace std::chrono_literals;

class FunctionThreadSafetyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clear registry for each test
        FunctionRegistry::getInstance().clear();
        
        // Register test functions
        registerTestFunctions();
        
        // Setup test data
        setupTestData();
        
        // Reset shared counters
        globalCounter.store(0);
        functionCallCounter.store(0);
        registryAccessCounter.store(0);
        errorCounter.store(0);
    }
    
    void TearDown() override {
        FunctionRegistry::getInstance().clear();
    }
    
private:
    void registerTestFunctions() {
        try {
            FunctionRegistry::getInstance().registerFunction<AddFunction>(AddFunction::metadata_);
            FunctionRegistry::getInstance().registerFunction<MultiplyFunction>(MultiplyFunction::metadata_);
            FunctionRegistry::getInstance().registerFunction<ScaleFunction>(ScaleFunction::metadata_);
        } catch (const std::exception& e) {
            // Functions might already be registered, ignore
        }
    }
    
    void setupTestData() {
        // Create test data sets for concurrent access
        testTimestamps1 = {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL, 5000000000ULL};
        testValues1 = {1.0, 2.0, 3.0, 4.0, 5.0};
        
        testTimestamps2 = {1500000000ULL, 2500000000ULL, 3500000000ULL, 4500000000ULL, 5500000000ULL};
        testValues2 = {10.0, 20.0, 30.0, 40.0, 50.0};
        
        // Large dataset for stress testing
        largeTimestamps.clear();
        largeValues.clear();
        for (int i = 0; i < 1000; ++i) {
            largeTimestamps.push_back(i * 1000000ULL);
            largeValues.push_back(std::sin(i * 0.1) * 100.0);
        }
    }

protected:
    // Test data
    std::vector<uint64_t> testTimestamps1, testTimestamps2, largeTimestamps;
    std::vector<double> testValues1, testValues2, largeValues;
    
    // Shared atomic counters for testing
    std::atomic<int> globalCounter{0};
    std::atomic<int> functionCallCounter{0};
    std::atomic<int> registryAccessCounter{0};
    std::atomic<int> errorCounter{0};
    
    // Synchronization primitives
    std::mutex testMutex;
    std::condition_variable cv;
    bool ready = false;
    
    // Initializes the thread_local FunctionRegistry on the calling thread.
    // Must be called at the start of each worker thread since the registry
    // is thread_local (one instance per Seastar shard / OS thread).
    static void initRegistryOnCurrentThread() {
        try {
            FunctionRegistry::getInstance().registerFunction<AddFunction>(AddFunction::metadata_);
        } catch (...) {}
        try {
            FunctionRegistry::getInstance().registerFunction<MultiplyFunction>(MultiplyFunction::metadata_);
        } catch (...) {}
        try {
            FunctionRegistry::getInstance().registerFunction<ScaleFunction>(ScaleFunction::metadata_);
        } catch (...) {}
    }

    // Helper methods
    void waitForAll(int threadCount) {
        (void)threadCount;
        std::unique_lock<std::mutex> lock(testMutex);
        cv.wait(lock, [this] { return ready; });
    }
    
    void signalAll() {
        std::lock_guard<std::mutex> lock(testMutex);
        ready = true;
        cv.notify_all();
    }
    
    void resetSync() {
        std::lock_guard<std::mutex> lock(testMutex);
        ready = false;
    }
    
    // Helper function to get correct parameter name for each function
    std::string getParameterName(const std::string& functionName) {
        if (functionName == "add") return "operand";
        if (functionName == "multiply") return "factor";
        if (functionName == "scale") return "factor";
        return "operand"; // Default fallback
    }
    
    // Helper function to create context with correct parameter name
    FunctionContext createContextForFunction(const std::string& functionName, double value) {
        FunctionContext context;
        context.setParameter(getParameterName(functionName), value);
        return context;
    }
};

// Test concurrent function execution - multiple threads executing the same function
TEST_F(FunctionThreadSafetyTest, ConcurrentFunctionExecution) {
    const int numThreads = 8;
    const int executionsPerThread = 50;
    std::vector<std::future<void>> futures;
    std::vector<int> threadResults(numThreads, 0);
    
    auto worker = [this, executionsPerThread](int threadId, int* result) {
        (void)threadId;
        initRegistryOnCurrentThread();
        try {
            auto function = FunctionRegistry::getInstance().createFunction("add");
            if (!function) {
                errorCounter++;
                return;
            }
            
            FunctionContext context;
            context.setParameter("operand", 10.0);
            
            // Cast to IUnaryFunction to access execute method
            auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
            if (!unaryFunction) {
                errorCounter++;
                return;
            }
            
            DoubleSeriesView inputView(&testTimestamps1, &testValues1);
            
            for (int i = 0; i < executionsPerThread; ++i) {
                auto future = unaryFunction->execute(inputView, context);
                auto result_data = future.get();
                
                if (result_data.size() != testValues1.size()) {
                    errorCounter++;
                    continue;
                }
                
                // Verify results are consistent
                for (size_t j = 0; j < result_data.size(); ++j) {
                    double expected = testValues1[j] + 10.0;
                    if (std::abs(result_data.values[j] - expected) > 1e-9) {
                        errorCounter++;
                        break;
                    }
                }
                
                functionCallCounter++;
                (*result)++;
            }
        } catch (const std::exception& e) {
            errorCounter++;
        }
    };
    
    // Launch threads
    for (int i = 0; i < numThreads; ++i) {
        futures.push_back(std::async(std::launch::async, worker, i, &threadResults[i]));
    }
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
    
    // Verify results
    EXPECT_EQ(errorCounter.load(), 0) << "Errors occurred during concurrent execution";
    
    int totalExecutions = 0;
    for (int result : threadResults) {
        totalExecutions += result;
    }
    
    EXPECT_EQ(totalExecutions, numThreads * executionsPerThread) 
        << "Not all executions completed successfully";
    EXPECT_EQ(functionCallCounter.load(), numThreads * executionsPerThread)
        << "Function call counter mismatch";
}

// Test registry thread safety - concurrent registration and lookup
TEST_F(FunctionThreadSafetyTest, ConcurrentRegistryAccess) {
    const int numThreads = 6;
    std::vector<std::future<void>> futures;
    std::atomic<bool> stopFlag{false};
    
    // Thread that continuously looks up functions
    auto lookupWorker = [this, &stopFlag]() {
        initRegistryOnCurrentThread();
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 2);
        
        std::vector<std::string> functionNames = {"add", "multiply", "scale"};
        
        while (!stopFlag.load()) {
            try {
                std::string functionName = functionNames[dis(gen)];
                auto function = FunctionRegistry::getInstance().createFunction(functionName);
                
                if (function) {
                    registryAccessCounter++;
                    
                    // Try to clone the function
                    auto cloned = function->clone();
                    if (cloned) {
                        registryAccessCounter++;
                    }
                    
                    // Check function metadata
                    const auto& metadata = function->getMetadata();
                    if (!metadata.name.empty()) {
                        registryAccessCounter++;
                    }
                }
                
                std::this_thread::sleep_for(1ms);
            } catch (const std::exception& e) {
                errorCounter++;
            }
        }
    };
    
    // Thread that checks registry stats
    auto statsWorker = [this, &stopFlag]() {
        initRegistryOnCurrentThread();
        while (!stopFlag.load()) {
            try {
                auto stats = FunctionRegistry::getInstance().getStats();
                if (stats.totalFunctions > 0) {
                    registryAccessCounter++;
                }
                
                auto allFunctions = FunctionRegistry::getInstance().getAllFunctionNames();
                if (!allFunctions.empty()) {
                    registryAccessCounter++;
                }
                
                std::this_thread::sleep_for(5ms);
            } catch (const std::exception& e) {
                errorCounter++;
            }
        }
    };
    
    // Thread that searches for functions
    auto searchWorker = [this, &stopFlag]() {
        initRegistryOnCurrentThread();
        std::vector<std::string> patterns = {"add", "mul", "scale", "nonexistent"};
        size_t patternIndex = 0;
        
        while (!stopFlag.load()) {
            try {
                std::string pattern = patterns[patternIndex % patterns.size()];
                auto results = FunctionRegistry::getInstance().searchFunctions(pattern);
                registryAccessCounter++;
                
                patternIndex++;
                std::this_thread::sleep_for(3ms);
            } catch (const std::exception& e) {
                errorCounter++;
            }
        }
    };
    
    // Launch worker threads
    for (int i = 0; i < numThreads; ++i) {
        if (i % 3 == 0) {
            futures.push_back(std::async(std::launch::async, lookupWorker));
        } else if (i % 3 == 1) {
            futures.push_back(std::async(std::launch::async, statsWorker));
        } else {
            futures.push_back(std::async(std::launch::async, searchWorker));
        }
    }
    
    // Let threads run for a while
    std::this_thread::sleep_for(500ms);
    stopFlag = true;
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
    
    // Verify results
    EXPECT_EQ(errorCounter.load(), 0) << "Errors occurred during concurrent registry access";
    EXPECT_GT(registryAccessCounter.load(), 0) << "Registry access counter should be positive";
}

// Test function cloning thread safety
TEST_F(FunctionThreadSafetyTest, ConcurrentFunctionCloning) {
    const int numThreads = 8;
    const int clonesPerThread = 100;
    std::vector<std::future<void>> futures;
    std::atomic<int> cloneCounter{0};
    
    auto cloneWorker = [this, clonesPerThread, &cloneCounter](int threadId) {
        (void)threadId;
        initRegistryOnCurrentThread();
        std::vector<std::string> functionNames = {"add", "multiply", "scale"};
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, functionNames.size() - 1);
        
        for (int i = 0; i < clonesPerThread; ++i) {
            try {
                std::string functionName = functionNames[dis(gen)];
                auto original = FunctionRegistry::getInstance().createFunction(functionName);
                
                if (original) {
                    // Clone the function multiple times
                    std::vector<std::unique_ptr<IFunction>> clones;
                    for (int j = 0; j < 5; ++j) {
                        auto cloned = original->clone();
                        if (cloned) {
                            clones.push_back(std::move(cloned));
                            cloneCounter++;
                        }
                    }
                    
                    // Verify clones have same metadata
                    const auto& originalMetadata = original->getMetadata();
                    for (const auto& clone : clones) {
                        if (clone) {
                            const auto& cloneMetadata = clone->getMetadata();
                            if (originalMetadata.name != cloneMetadata.name ||
                                originalMetadata.category != cloneMetadata.category) {
                                errorCounter++;
                            }
                        }
                    }
                }
            } catch (const std::exception& e) {
                errorCounter++;
            }
        }
    };
    
    // Launch threads
    for (int i = 0; i < numThreads; ++i) {
        futures.push_back(std::async(std::launch::async, cloneWorker, i));
    }
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
    
    // Verify results
    EXPECT_EQ(errorCounter.load(), 0) << "Errors occurred during concurrent cloning";
    EXPECT_GT(cloneCounter.load(), 0) << "No successful clones created";
    EXPECT_EQ(cloneCounter.load(), numThreads * clonesPerThread * 5)
        << "Not all clones created successfully";
}

// Test shared context modifications during function execution
TEST_F(FunctionThreadSafetyTest, SharedContextModifications) {
    const int numThreads = 6;
    const int executionsPerThread = 50;
    std::vector<std::future<void>> futures;
    
    // Shared context that will be modified concurrently
    std::shared_ptr<FunctionContext> sharedContext = std::make_shared<FunctionContext>();
    sharedContext->setParameter("factor", 1.0);
    
    std::mutex contextMutex;
    std::atomic<int> successfulExecutions{0};
    
    auto worker = [this, executionsPerThread, sharedContext, &contextMutex, &successfulExecutions](int threadId) {
        initRegistryOnCurrentThread();
        try {
            auto function = FunctionRegistry::getInstance().createFunction("multiply");
            if (!function) {
                errorCounter++;
                return;
            }
            
            DoubleSeriesView inputView(&testTimestamps1, &testValues1);
            
            for (int i = 0; i < executionsPerThread; ++i) {
                // Modify shared context (thread-safe approach)
                double multiplier = threadId + i * 0.1;
                {
                    std::lock_guard<std::mutex> lock(contextMutex);
                    sharedContext->setParameter("factor", multiplier);
                }
                
                // Execute function with potentially modified context
                FunctionContext localContext = *sharedContext;  // Copy for safety
                
                // Cast to IUnaryFunction to access execute method
                auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
                if (!unaryFunction) {
                    errorCounter++;
                    continue;
                }
                
                auto future = unaryFunction->execute(inputView, localContext);
                auto result = future.get();
                
                if (result.size() == testValues1.size()) {
                    successfulExecutions++;
                }
                
                std::this_thread::sleep_for(1ms);
            }
        } catch (const std::exception& e) {
            errorCounter++;
        }
    };
    
    // Launch threads
    for (int i = 0; i < numThreads; ++i) {
        futures.push_back(std::async(std::launch::async, worker, i));
    }
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
    
    // Verify results
    EXPECT_EQ(errorCounter.load(), 0) << "Errors occurred with shared context modifications";
    EXPECT_GT(successfulExecutions.load(), 0) << "No successful executions with shared context";
}

// Test atomic operations and memory coherency
TEST_F(FunctionThreadSafetyTest, AtomicOperationsAndMemoryCoherency) {
    const int numThreads = 8;
    const int operationsPerThread = 1000;
    std::vector<std::future<void>> futures;
    
    std::atomic<long long> atomicSum{0};
    std::atomic<int> atomicCounter{0};
    volatile long long volatileSum = 0;  // For testing memory visibility
    std::mutex volatileMutex;
    
    auto worker = [this, operationsPerThread, &atomicSum, &atomicCounter, &volatileSum, &volatileMutex](int threadId) {
        (void)threadId;
        initRegistryOnCurrentThread();
        try {
            auto function = FunctionRegistry::getInstance().createFunction("add");
            if (!function) {
                errorCounter++;
                return;
            }
            
            FunctionContext context;
            context.setParameter("operand", 1.0);
            DoubleSeriesView inputView(&testTimestamps1, &testValues1);
            
            // Cast to IUnaryFunction to access execute method
            auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
            if (!unaryFunction) {
                errorCounter++;
                return;
            }
            
            for (int i = 0; i < operationsPerThread; ++i) {
                // Execute function
                auto future = unaryFunction->execute(inputView, context);
                auto result = future.get();
                
                if (result.size() == testValues1.size()) {
                    // Atomic operations
                    atomicCounter.fetch_add(1, std::memory_order_relaxed);
                    long long sum = 0;
                    for (double val : result.values) {
                        sum += static_cast<long long>(val * 1000);  // Scale for integer arithmetic
                    }
                    atomicSum.fetch_add(sum, std::memory_order_relaxed);
                    
                    // Volatile variable with mutex protection
                    {
                        std::lock_guard<std::mutex> lock(volatileMutex);
                        volatileSum += sum;
                    }
                }
            }
        } catch (const std::exception& e) {
            errorCounter++;
        }
    };
    
    // Launch threads
    for (int i = 0; i < numThreads; ++i) {
        futures.push_back(std::async(std::launch::async, worker, i));
    }
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
    
    // Verify atomic operations worked correctly
    EXPECT_EQ(errorCounter.load(), 0) << "Errors occurred during atomic operations";
    EXPECT_EQ(atomicCounter.load(), numThreads * operationsPerThread) 
        << "Atomic counter mismatch";
    
    // Check memory coherency
    long long expectedSum = atomicSum.load();
    EXPECT_EQ(volatileSum, expectedSum) 
        << "Memory coherency issue detected between atomic and volatile variables";
}

// Test race conditions in performance tracking
TEST_F(FunctionThreadSafetyTest, PerformanceTrackingRaceConditions) {
    const int numThreads = 10;
    const int executionsPerThread = 100;
    std::vector<std::future<void>> futures;
    
    struct PerformanceTracker {
        std::atomic<long long> totalExecutionTime{0};
        std::atomic<int> executionCount{0};
        std::atomic<long long> minExecutionTime{LLONG_MAX};
        std::atomic<long long> maxExecutionTime{0};
        
        void recordExecution(long long executionTime) {
            totalExecutionTime.fetch_add(executionTime);
            executionCount.fetch_add(1);
            
            // Update min/max with compare-and-swap
            long long current_min = minExecutionTime.load();
            while (executionTime < current_min && 
                   !minExecutionTime.compare_exchange_weak(current_min, executionTime)) {
                // Retry if compare_exchange failed
            }
            
            long long current_max = maxExecutionTime.load();
            while (executionTime > current_max && 
                   !maxExecutionTime.compare_exchange_weak(current_max, executionTime)) {
                // Retry if compare_exchange failed
            }
        }
        
        double getAverageExecutionTime() const {
            int count = executionCount.load();
            return count > 0 ? static_cast<double>(totalExecutionTime.load()) / count : 0.0;
        }
    };
    
    PerformanceTracker tracker;
    
    auto worker = [this, executionsPerThread, &tracker](int threadId) {
        initRegistryOnCurrentThread();
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> functionDis(0, 2);
        std::vector<std::string> functionNames = {"add", "multiply", "scale"};
        
        try {
            for (int i = 0; i < executionsPerThread; ++i) {
                auto startTime = std::chrono::high_resolution_clock::now();
                
                std::string functionName = functionNames[functionDis(gen)];
                auto function = FunctionRegistry::getInstance().createFunction(functionName);
                
                if (function) {
                    FunctionContext context = createContextForFunction(functionName, static_cast<double>(threadId + 1.0));
                    
                    // Choose input data based on thread ID
                    const std::vector<uint64_t>* timestamps = largeTimestamps.empty() ? &testTimestamps1 : &largeTimestamps;
                    const std::vector<double>* values = largeValues.empty() ? &testValues1 : &largeValues;
                    DoubleSeriesView inputView(timestamps, values);
                    
                    // Cast to IUnaryFunction to access execute method
                    auto unaryFunction = dynamic_cast<IUnaryFunction*>(function.get());
                    if (!unaryFunction) {
                        errorCounter++;
                        continue;
                    }
                    
                    auto future = unaryFunction->execute(inputView, context);
                    auto result = future.get();
                    
                    auto endTime = std::chrono::high_resolution_clock::now();
                    auto executionTime = std::chrono::duration_cast<std::chrono::microseconds>(
                        endTime - startTime).count();
                    
                    tracker.recordExecution(executionTime);
                    functionCallCounter++;
                }
                
                // Small delay to vary timing
                if (i % 10 == 0) {
                    std::this_thread::sleep_for(1ms);
                }
            }
        } catch (const std::exception& e) {
            errorCounter++;
        }
    };
    
    // Launch threads
    for (int i = 0; i < numThreads; ++i) {
        futures.push_back(std::async(std::launch::async, worker, i));
    }
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
    
    // Verify performance tracking results
    EXPECT_EQ(errorCounter.load(), 0) << "Errors occurred during performance tracking";
    EXPECT_EQ(tracker.executionCount.load(), functionCallCounter.load())
        << "Performance tracking count mismatch with function call counter";
    EXPECT_GT(tracker.totalExecutionTime.load(), 0)
        << "Total execution time should be positive";
    EXPECT_LT(tracker.minExecutionTime.load(), tracker.maxExecutionTime.load())
        << "Min execution time should be less than max execution time";
    EXPECT_GT(tracker.getAverageExecutionTime(), 0)
        << "Average execution time should be positive";
}

// Test complex concurrent scenario simulating production load
TEST_F(FunctionThreadSafetyTest, ProductionLikeScenario) {
    const int numWorkerThreads = 12;
    const int executionDuration = 2; // seconds
    std::atomic<bool> stopFlag{false};
    std::vector<std::future<void>> futures;
    
    struct Statistics {
        std::atomic<int> totalRequests{0};
        std::atomic<int> successfulRequests{0};
        std::atomic<int> failedRequests{0};
        std::atomic<int> registryLookups{0};
        std::atomic<int> functionClones{0};
    } stats;
    
    // Worker threads simulating HTTP request handlers
    auto httpWorker = [this, &stopFlag, &stats](int workerId) {
        initRegistryOnCurrentThread();
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> functionDis(0, 2);
        std::uniform_real_distribution<> paramDis(0.1, 100.0);
        std::vector<std::string> functionNames = {"add", "multiply", "scale"};
        
        while (!stopFlag.load()) {
            try {
                stats.totalRequests++;
                
                // Simulate HTTP request processing
                std::string functionName = functionNames[functionDis(gen)];
                auto function = FunctionRegistry::getInstance().createFunction(functionName);
                stats.registryLookups++;
                
                if (!function) {
                    stats.failedRequests++;
                    continue;
                }
                
                // Clone function for this request (simulating isolation)
                auto clonedFunction = function->clone();
                stats.functionClones++;
                
                if (!clonedFunction) {
                    stats.failedRequests++;
                    continue;
                }
                
                // Prepare request-specific context
                FunctionContext context = createContextForFunction(functionName, paramDis(gen));
                
                // Execute with random data
                bool useLargeData = (workerId % 3 == 0);
                const std::vector<uint64_t>* timestamps = (useLargeData && !largeTimestamps.empty()) ? &largeTimestamps : &testTimestamps1;
                const std::vector<double>* values = (useLargeData && !largeValues.empty()) ? &largeValues : &testValues1;
                DoubleSeriesView inputView(timestamps, values);
                
                // Cast to IUnaryFunction to access execute method
                auto unaryFunction = dynamic_cast<IUnaryFunction*>(clonedFunction.get());
                if (!unaryFunction) {
                    stats.failedRequests++;
                    continue;
                }
                
                auto future = unaryFunction->execute(inputView, context);
                auto result = future.get();
                
                if (!result.empty()) {
                    stats.successfulRequests++;
                } else {
                    stats.failedRequests++;
                }
                
                // Simulate processing time
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                
            } catch (const std::exception& e) {
                stats.failedRequests++;
                errorCounter++;
            }
        }
    };
    
    // Registry maintenance thread
    auto maintenanceWorker = [this, &stopFlag, &stats]() {
        initRegistryOnCurrentThread();
        while (!stopFlag.load()) {
            try {
                // Periodic registry stats collection
                auto registryStats = FunctionRegistry::getInstance().getStats();
                globalCounter += registryStats.totalFunctions;
                
                // Function discovery
                auto allFunctions = FunctionRegistry::getInstance().getAllFunctionNames();
                registryAccessCounter += allFunctions.size();
                
                std::this_thread::sleep_for(100ms);
            } catch (const std::exception& e) {
                errorCounter++;
            }
        }
    };
    
    // Launch worker threads
    for (int i = 0; i < numWorkerThreads; ++i) {
        futures.push_back(std::async(std::launch::async, httpWorker, i));
    }
    
    // Launch maintenance thread
    futures.push_back(std::async(std::launch::async, maintenanceWorker));
    
    // Let the system run under load
    std::this_thread::sleep_for(std::chrono::seconds(executionDuration));
    stopFlag = true;
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
    
    // Verify system behaved correctly under concurrent load
    EXPECT_EQ(errorCounter.load(), 0) << "Errors occurred in production-like scenario";
    EXPECT_GT(stats.totalRequests.load(), 0) << "No requests processed";
    EXPECT_GT(stats.successfulRequests.load(), 0) << "No successful requests";
    
    // Check that success rate is reasonable (at least 90%)
    double successRate = static_cast<double>(stats.successfulRequests.load()) / 
                        static_cast<double>(stats.totalRequests.load());
    EXPECT_GE(successRate, 0.9) << "Success rate too low: " << successRate;
    
    // Verify registry operations completed
    EXPECT_GT(stats.registryLookups.load(), 0) << "No registry lookups performed";
    EXPECT_GT(stats.functionClones.load(), 0) << "No function clones created";
    
    std::cout << "Production scenario statistics:\n"
              << "  Total requests: " << stats.totalRequests.load() << "\n"
              << "  Successful requests: " << stats.successfulRequests.load() << "\n"
              << "  Failed requests: " << stats.failedRequests.load() << "\n"
              << "  Success rate: " << (successRate * 100.0) << "%\n"
              << "  Registry lookups: " << stats.registryLookups.load() << "\n"
              << "  Function clones: " << stats.functionClones.load() << "\n";
}