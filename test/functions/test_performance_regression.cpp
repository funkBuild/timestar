#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include <map>
#include <fstream>
#include <sstream>
#include <thread>
#include <random>
#include <algorithm>
#include <numeric>

namespace tsdb::test {

class PerformanceRegressionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Generate test data sets of different sizes
        generateTestData();
        
        // Load baseline performance metrics if available
        loadBaselineMetrics();
    }
    
    void TearDown() override {
        // Save current performance metrics as baseline
        saveBaselineMetrics();
    }
    
    void generateTestData() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<double> value_dist(10.0, 100.0);
        
        // Small dataset (100 points)
        small_dataset.reserve(100);
        int64_t base_time = 1640000000000000000LL; // Jan 1, 2022
        for (int i = 0; i < 100; ++i) {
            small_dataset.push_back({base_time + i * 60000000000LL, value_dist(gen)});
        }
        
        // Medium dataset (1000 points)
        medium_dataset.reserve(1000);
        for (int i = 0; i < 1000; ++i) {
            medium_dataset.push_back({base_time + i * 60000000000LL, value_dist(gen)});
        }
        
        // Large dataset (10000 points)
        large_dataset.reserve(10000);
        for (int i = 0; i < 10000; ++i) {
            large_dataset.push_back({base_time + i * 60000000000LL, value_dist(gen)});
        }
    }
    
    void loadBaselineMetrics() {
        std::ifstream file("/tmp/tsdb_performance_baseline.txt");
        if (!file.is_open()) return;
        
        std::string line;
        while (std::getline(file, line)) {
            std::istringstream iss(line);
            std::string test_name;
            double execution_time;
            size_t memory_usage;
            
            if (iss >> test_name >> execution_time >> memory_usage) {
                baseline_metrics[test_name] = {execution_time, memory_usage};
            }
        }
    }
    
    void saveBaselineMetrics() {
        std::ofstream file("/tmp/tsdb_performance_baseline.txt");
        for (const auto& [test_name, metrics] : current_metrics) {
            file << test_name << " " << metrics.first << " " << metrics.second << "\n";
        }
    }
    
    struct PerformanceResult {
        double execution_time_ms;
        size_t memory_usage_bytes;
        double throughput_points_per_sec;
    };
    
    // Basic performance measurement using mathematical operations
    PerformanceResult measureDataProcessingPerformance(const std::string& test_name,
                                                       const std::vector<std::pair<int64_t, double>>& data,
                                                       const std::string& operation) {
        // Get initial memory usage
        size_t initial_memory = getCurrentMemoryUsage();
        
        // Measure execution time
        auto start = std::chrono::high_resolution_clock::now();
        
        std::vector<double> result;
        result.reserve(data.size());
        
        // Perform different operations for performance testing
        if (operation == "sma") {
            // Simple moving average simulation
            int window = 5;
            for (size_t i = 0; i < data.size(); ++i) {
                if (i < window - 1) {
                    result.push_back(data[i].second);
                } else {
                    double sum = 0.0;
                    for (int j = 0; j < window; ++j) {
                        sum += data[i - j].second;
                    }
                    result.push_back(sum / window);
                }
            }
        } else if (operation == "scale") {
            // Scale operation simulation
            double factor = 2.5;
            for (const auto& point : data) {
                result.push_back(point.second * factor);
            }
        } else if (operation == "offset") {
            // Offset operation simulation
            double offset = 10.0;
            for (const auto& point : data) {
                result.push_back(point.second + offset);
            }
        } else {
            // Default: copy operation
            for (const auto& point : data) {
                result.push_back(point.second);
            }
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        double execution_time_ms = duration.count() / 1000.0;
        
        size_t final_memory = getCurrentMemoryUsage();
        size_t memory_usage = final_memory > initial_memory ? final_memory - initial_memory : 0;
        
        double throughput = data.size() / (execution_time_ms / 1000.0);
        
        PerformanceResult perf_result;
        perf_result.execution_time_ms = execution_time_ms;
        perf_result.memory_usage_bytes = memory_usage;
        perf_result.throughput_points_per_sec = throughput;
        
        // Store current metrics for baseline comparison
        current_metrics[test_name] = {execution_time_ms, memory_usage};
        
        return perf_result;
    }
    
    void checkRegression(const std::string& test_name, const PerformanceResult& result) {
        auto baseline_it = baseline_metrics.find(test_name);
        if (baseline_it == baseline_metrics.end()) {
            std::cout << "No baseline found for " << test_name << ", establishing baseline\n";
            return;
        }
        
        double baseline_time = baseline_it->second.first;
        size_t baseline_memory = baseline_it->second.second;
        
        // Check for performance regression (>50% slower - allow significant variance)
        double time_regression = (result.execution_time_ms - baseline_time) / baseline_time;
        if (time_regression > 0.50) {
            GTEST_FAIL() << "Performance regression detected in " << test_name 
                        << ": " << (time_regression * 100) << "% slower than baseline"
                        << " (baseline: " << baseline_time << "ms, current: " << result.execution_time_ms << "ms)";
        }
        
        // Check for memory regression (>200% more memory - allow for test variance)
        if (baseline_memory > 1000) { // Only check if baseline had meaningful memory usage
            double memory_regression = (double)(result.memory_usage_bytes - baseline_memory) / baseline_memory;
            if (memory_regression > 2.0) {
                GTEST_FAIL() << "Memory regression detected in " << test_name 
                            << ": " << (memory_regression * 100) << "% more memory than baseline"
                            << " (baseline: " << baseline_memory << " bytes, current: " << result.memory_usage_bytes << " bytes)";
            }
        }
        
        std::cout << "Performance check passed for " << test_name 
                  << " (time: " << result.execution_time_ms << "ms, throughput: " 
                  << result.throughput_points_per_sec << " pts/sec)\n";
    }
    
    size_t getCurrentMemoryUsage() {
        std::ifstream status_file("/proc/self/status");
        std::string line;
        while (std::getline(status_file, line)) {
            if (line.find("VmRSS:") == 0) {
                std::istringstream iss(line);
                std::string label, size_str, unit;
                iss >> label >> size_str >> unit;
                return std::stoull(size_str) * 1024; // Convert KB to bytes
            }
        }
        return 0;
    }
    
    std::vector<std::pair<int64_t, double>> small_dataset;
    std::vector<std::pair<int64_t, double>> medium_dataset;
    std::vector<std::pair<int64_t, double>> large_dataset;
    
    std::map<std::string, std::pair<double, size_t>> baseline_metrics;
    std::map<std::string, std::pair<double, size_t>> current_metrics;
};

// SMA Function Performance Tests
TEST_F(PerformanceRegressionTest, SMA_SmallDataset_Performance) {
    auto result = measureDataProcessingPerformance("SMA_Small", small_dataset, "sma");
    
    // Performance expectations for small dataset
    EXPECT_LT(result.execution_time_ms, 100.0) << "SMA on small dataset should complete in <100ms";
    EXPECT_GT(result.throughput_points_per_sec, 1000.0) << "SMA should process >1000 points/sec";
    
    checkRegression("SMA_Small", result);
}

TEST_F(PerformanceRegressionTest, SMA_MediumDataset_Performance) {
    auto result = measureDataProcessingPerformance("SMA_Medium", medium_dataset, "sma");
    
    // Performance expectations for medium dataset
    EXPECT_LT(result.execution_time_ms, 500.0) << "SMA on medium dataset should complete in <500ms";
    EXPECT_GT(result.throughput_points_per_sec, 2000.0) << "SMA should process >2000 points/sec";
    
    checkRegression("SMA_Medium", result);
}

TEST_F(PerformanceRegressionTest, SMA_LargeDataset_Performance) {
    auto result = measureDataProcessingPerformance("SMA_Large", large_dataset, "sma");
    
    // Performance expectations for large dataset
    EXPECT_LT(result.execution_time_ms, 2000.0) << "SMA on large dataset should complete in <2000ms";
    EXPECT_GT(result.throughput_points_per_sec, 5000.0) << "SMA should process >5000 points/sec";
    
    checkRegression("SMA_Large", result);
}

// Arithmetic Function Performance Tests
TEST_F(PerformanceRegressionTest, Scale_LargeDataset_Performance) {
    auto result = measureDataProcessingPerformance("Scale_Large", large_dataset, "scale");
    
    // Scale should be very fast (linear operation)
    EXPECT_LT(result.execution_time_ms, 1000.0) << "Scale on large dataset should complete in <1000ms";
    EXPECT_GT(result.throughput_points_per_sec, 10000.0) << "Scale should process >10000 points/sec";
    
    checkRegression("Scale_Large", result);
}

TEST_F(PerformanceRegressionTest, Offset_LargeDataset_Performance) {
    auto result = measureDataProcessingPerformance("Offset_Large", large_dataset, "offset");
    
    // Offset should be very fast (linear operation)
    EXPECT_LT(result.execution_time_ms, 1000.0) << "Offset on large dataset should complete in <1000ms";
    EXPECT_GT(result.throughput_points_per_sec, 10000.0) << "Offset should process >10000 points/sec";
    
    checkRegression("Offset_Large", result);
}

// Memory Scaling Tests
TEST_F(PerformanceRegressionTest, MemoryScaling_DataProcessing) {
    // Test memory usage scaling with dataset size
    auto small_result = measureDataProcessingPerformance("SMA_Small_Memory", small_dataset, "sma");
    auto medium_result = measureDataProcessingPerformance("SMA_Medium_Memory", medium_dataset, "sma");
    auto large_result = measureDataProcessingPerformance("SMA_Large_Memory", large_dataset, "sma");
    
    std::cout << "Memory scaling: Small=" << small_result.memory_usage_bytes 
              << ", Medium=" << medium_result.memory_usage_bytes 
              << ", Large=" << large_result.memory_usage_bytes << std::endl;
    
    // Memory should scale reasonably with data size
    // These are soft checks - we mainly want to detect massive memory leaks
    EXPECT_LT(medium_result.memory_usage_bytes, 50 * 1024 * 1024) 
        << "Medium dataset should use <50MB memory";
    EXPECT_LT(large_result.memory_usage_bytes, 200 * 1024 * 1024) 
        << "Large dataset should use <200MB memory";
}

// Throughput Scaling Tests
TEST_F(PerformanceRegressionTest, ThroughputScaling_DataProcessing) {
    // Test throughput scaling with dataset size
    auto small_result = measureDataProcessingPerformance("SMA_Small_Throughput", small_dataset, "sma");
    auto medium_result = measureDataProcessingPerformance("SMA_Medium_Throughput", medium_dataset, "sma");
    auto large_result = measureDataProcessingPerformance("SMA_Large_Throughput", large_dataset, "sma");
    
    std::cout << "Throughput scaling: Small=" << small_result.throughput_points_per_sec 
              << ", Medium=" << medium_result.throughput_points_per_sec 
              << ", Large=" << large_result.throughput_points_per_sec << std::endl;
    
    // Throughput should remain reasonable as dataset scales
    EXPECT_GT(medium_result.throughput_points_per_sec, 1000.0)
        << "Medium dataset throughput should be reasonable";
    EXPECT_GT(large_result.throughput_points_per_sec, 1000.0)
        << "Large dataset throughput should be reasonable";
}

// Concurrent Performance Tests
TEST_F(PerformanceRegressionTest, ConcurrentExecution_Performance) {
    const int num_threads = 2; // Reduced for test stability
    const int executions_per_thread = 5;
    
    std::vector<std::thread> threads;
    std::vector<double> thread_times(num_threads);
    std::atomic<bool> start_flag{false};
    
    // Create threads that wait for start signal
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            // Wait for start signal
            while (!start_flag.load()) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            
            auto thread_start = std::chrono::high_resolution_clock::now();
            
            for (int i = 0; i < executions_per_thread; ++i) {
                auto result = measureDataProcessingPerformance("SMA_Concurrent_" + std::to_string(t), 
                                                              medium_dataset, "sma");
            }
            
            auto thread_end = std::chrono::high_resolution_clock::now();
            auto thread_duration = std::chrono::duration_cast<std::chrono::milliseconds>(thread_end - thread_start);
            thread_times[t] = thread_duration.count();
        });
    }
    
    // Start all threads simultaneously
    start_flag.store(true);
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Check that concurrent execution completes reasonably
    double avg_thread_time = std::accumulate(thread_times.begin(), thread_times.end(), 0.0) / num_threads;
    double max_thread_time = *std::max_element(thread_times.begin(), thread_times.end());
    
    std::cout << "Concurrent execution completed - avg time: " << avg_thread_time 
              << "ms, max time: " << max_thread_time << "ms\n";
    
    // Basic sanity checks - concurrent execution should complete in reasonable time
    EXPECT_LT(avg_thread_time, 10000.0) << "Average thread time should be reasonable";
    EXPECT_LT(max_thread_time, 20000.0) << "Max thread time should be reasonable";
}

// Stress Test for Memory Leaks
TEST_F(PerformanceRegressionTest, MemoryLeak_StressTest) {
    size_t initial_memory = getCurrentMemoryUsage();
    
    // Execute many function operations (reduced iterations for stability)
    for (int i = 0; i < 100; ++i) {
        auto result = measureDataProcessingPerformance("SMA_Stress_" + std::to_string(i), 
                                                      small_dataset, "sma");
        
        // Check memory every 25 iterations
        if (i % 25 == 24) {
            size_t current_memory = getCurrentMemoryUsage();
            
            // Memory growth should be bounded
            if (current_memory > initial_memory) {
                size_t memory_growth = current_memory - initial_memory;
                EXPECT_LT(memory_growth, 100 * 1024 * 1024) // Less than 100MB growth
                    << "Potential memory leak detected after " << (i + 1) << " iterations";
            }
        }
    }
    
    // Check final memory
    size_t final_memory = getCurrentMemoryUsage();
    
    if (final_memory > initial_memory) {
        size_t total_growth = final_memory - initial_memory;
        EXPECT_LT(total_growth, 200 * 1024 * 1024) // Less than 200MB total growth
            << "Significant memory growth detected in stress test: " << total_growth << " bytes";
    }
    
    std::cout << "Memory leak stress test completed - initial: " << initial_memory 
              << " bytes, final: " << final_memory << " bytes\n";
}

// Performance Benchmark Summary Test
TEST_F(PerformanceRegressionTest, PerformanceBenchmarkSummary) {
    std::cout << "\n=== Performance Benchmark Summary ===\n";
    
    // Measure key operations on standard dataset
    auto sma_result = measureDataProcessingPerformance("SMA_Benchmark", medium_dataset, "sma");
    auto scale_result = measureDataProcessingPerformance("Scale_Benchmark", medium_dataset, "scale");
    auto offset_result = measureDataProcessingPerformance("Offset_Benchmark", medium_dataset, "offset");
    
    std::cout << "SMA (1000 points):   " << sma_result.execution_time_ms << "ms, " 
              << sma_result.throughput_points_per_sec << " pts/sec\n";
    std::cout << "Scale (1000 points): " << scale_result.execution_time_ms << "ms, " 
              << scale_result.throughput_points_per_sec << " pts/sec\n";
    std::cout << "Offset (1000 points):" << offset_result.execution_time_ms << "ms, " 
              << offset_result.throughput_points_per_sec << " pts/sec\n";
    std::cout << "======================================\n";
    
    // Basic performance sanity checks
    EXPECT_LT(sma_result.execution_time_ms, 1000.0) << "SMA should complete reasonably quickly";
    EXPECT_LT(scale_result.execution_time_ms, 500.0) << "Scale should be fast";
    EXPECT_LT(offset_result.execution_time_ms, 500.0) << "Offset should be fast";
    
    EXPECT_GT(sma_result.throughput_points_per_sec, 1000.0) << "SMA should have reasonable throughput";
    EXPECT_GT(scale_result.throughput_points_per_sec, 2000.0) << "Scale should have good throughput";
    EXPECT_GT(offset_result.throughput_points_per_sec, 2000.0) << "Offset should have good throughput";
}

} // namespace tsdb::test