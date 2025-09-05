#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <random>
#include <atomic>

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/tsdb_value.hpp"

#include <seastar/core/app_template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>

class WritePerformanceTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test data
        for (int i = 0; i < seastar::smp::count; i++) {
            std::filesystem::remove_all("shard_" + std::to_string(i));
        }
    }
    
    void TearDown() override {
        // Clean up test data
        for (int i = 0; i < seastar::smp::count; i++) {
            std::filesystem::remove_all("shard_" + std::to_string(i));
        }
    }
};

// Performance test: Measure write throughput
seastar::future<> testWriteThroughput(int numSeries, int pointsPerSeries) {
    seastar::sharded<Engine> engineSharded;
    
    co_await engineSharded.start();
    co_await engineSharded.invoke_on_all([](Engine& engine) {
        return engine.init();
    });
    co_await engineSharded.invoke_on_all([](Engine& engine) {
        return engine.startBackgroundTasks();
    });
    
    auto startTime = std::chrono::high_resolution_clock::now();
    std::atomic<int> totalPointsWritten{0};
    
    // Generate random data
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> valueDist(0.0, 100.0);
    
    uint64_t baseTime = 1638202821000000000;
    
    // Write data for multiple series
    std::vector<seastar::future<>> writes;
    
    for (int s = 0; s < numSeries; s++) {
        TSDBInsert<double> insert("metrics", "value");
        insert.addTag("host", "server-" + std::to_string(s % 10));
        insert.addTag("metric", "metric-" + std::to_string(s));
        
        for (int p = 0; p < pointsPerSeries; p++) {
            insert.addValue(baseTime + p * 1000000000, valueDist(gen));
        }
        
        // Determine shard based on measurement
        size_t shard = std::hash<std::string>{}(insert.measurement) % seastar::smp::count;
        
        writes.push_back(engineSharded.invoke_on(shard, 
            [insert = std::move(insert), &totalPointsWritten, pointsPerSeries](Engine& engine) mutable {
                return engine.insert(std::move(insert)).then([&totalPointsWritten, pointsPerSeries] {
                    totalPointsWritten += pointsPerSeries;
                });
            }));
    }
    
    co_await seastar::when_all(writes.begin(), writes.end());
    
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    
    int totalPoints = numSeries * pointsPerSeries;
    double throughput = (totalPoints * 1000.0) / duration.count();
    
    std::cout << "\nWrite Performance Results:" << std::endl;
    std::cout << "  Series: " << numSeries << std::endl;
    std::cout << "  Points per series: " << pointsPerSeries << std::endl;
    std::cout << "  Total points: " << totalPoints << std::endl;
    std::cout << "  Time: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(0) 
              << throughput << " points/second" << std::endl;
    
    // Verify we can read some data back
    auto result = co_await engineSharded.invoke_on(0, [baseTime](Engine& engine) {
        return engine.query("metrics,host=server-0,metric=metric-0 value", 
                          baseTime, baseTime + 10 * 1000000000);
    });
    
    if (std::holds_alternative<QueryResult<double>>(result)) {
        auto& floatResult = std::get<QueryResult<double>>(result);
        EXPECT_GT(floatResult.values.size(), 0);
        std::cout << "  Verification: Successfully read back " 
                  << floatResult.values.size() << " points" << std::endl;
    }
    
    co_await engineSharded.invoke_on_all([](Engine& engine) {
        return engine.stop();
    });
    co_await engineSharded.stop();
    
    // Basic performance assertions
    EXPECT_GT(throughput, 10000);  // Should write at least 10K points/second
}

TEST_F(WritePerformanceTest, BasicThroughput) {
    seastar::app_template app;
    
    auto exitCode = app.run(0, nullptr, [&] {
        return testWriteThroughput(100, 1000).then([&] {  // 100 series, 1000 points each
            seastar::engine().exit(0);
        }).handle_exception([&](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                std::cerr << "Test failed with exception: " << e.what() << std::endl;
            }
            seastar::engine().exit(1);
        });
    });
    
    EXPECT_EQ(exitCode, 0);
}

// Stress test: High concurrency writes
seastar::future<> testConcurrentWrites() {
    seastar::sharded<Engine> engineSharded;
    
    co_await engineSharded.start();
    co_await engineSharded.invoke_on_all([](Engine& engine) {
        return engine.init();
    });
    co_await engineSharded.invoke_on_all([](Engine& engine) {
        return engine.startBackgroundTasks();
    });
    
    auto startTime = std::chrono::high_resolution_clock::now();
    
    // Launch many concurrent writes
    const int numConcurrentWrites = 1000;
    const int pointsPerWrite = 100;
    uint64_t baseTime = 1638202821000000000;
    
    std::vector<seastar::future<>> writes;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> valueDist(0.0, 100.0);
    
    for (int i = 0; i < numConcurrentWrites; i++) {
        writes.push_back(seastar::async([&engineSharded, i, baseTime, &gen, &valueDist] {
            TSDBInsert<double> insert("stress", "value");
            insert.addTag("worker", std::to_string(i));
            
            for (int p = 0; p < pointsPerWrite; p++) {
                insert.addValue(baseTime + p * 1000000, valueDist(gen));
            }
            
            size_t shard = std::hash<std::string>{}(insert.measurement) % seastar::smp::count;
            engineSharded.invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            }).get();
        }));
    }
    
    co_await seastar::when_all(writes.begin(), writes.end());
    
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    
    int totalPoints = numConcurrentWrites * pointsPerWrite;
    double throughput = (totalPoints * 1000.0) / duration.count();
    
    std::cout << "\nConcurrent Write Stress Test Results:" << std::endl;
    std::cout << "  Concurrent writes: " << numConcurrentWrites << std::endl;
    std::cout << "  Points per write: " << pointsPerWrite << std::endl;
    std::cout << "  Total points: " << totalPoints << std::endl;
    std::cout << "  Time: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(0) 
              << throughput << " points/second" << std::endl;
    
    co_await engineSharded.invoke_on_all([](Engine& engine) {
        return engine.stop();
    });
    co_await engineSharded.stop();
    
    EXPECT_GT(throughput, 5000);  // Should handle at least 5K points/second under stress
}

TEST_F(WritePerformanceTest, ConcurrentWrites) {
    seastar::app_template app;
    
    auto exitCode = app.run(0, nullptr, [&] {
        return testConcurrentWrites().then([&] {
            seastar::engine().exit(0);
        }).handle_exception([&](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                std::cerr << "Test failed with exception: " << e.what() << std::endl;
            }
            seastar::engine().exit(1);
        });
    });
    
    EXPECT_EQ(exitCode, 0);
}

// Batch write performance test
seastar::future<> testBatchWritePerformance() {
    seastar::sharded<Engine> engineSharded;
    
    co_await engineSharded.start();
    co_await engineSharded.invoke_on_all([](Engine& engine) {
        return engine.init();
    });
    co_await engineSharded.invoke_on_all([](Engine& engine) {
        return engine.startBackgroundTasks();
    });
    
    auto startTime = std::chrono::high_resolution_clock::now();
    
    const int numBatches = 100;
    const int seriesPerBatch = 10;
    const int pointsPerSeries = 100;
    uint64_t baseTime = 1638202821000000000;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> valueDist(0.0, 100.0);
    
    for (int batch = 0; batch < numBatches; batch++) {
        std::vector<seastar::future<>> batchWrites;
        
        for (int s = 0; s < seriesPerBatch; s++) {
            TSDBInsert<double> insert("batch_metrics", "value");
            insert.addTag("batch", std::to_string(batch));
            insert.addTag("series", std::to_string(s));
            
            // Add all points at once (simulating batch write)
            for (int p = 0; p < pointsPerSeries; p++) {
                insert.addValue(baseTime + p * 1000000, valueDist(gen));
            }
            
            size_t shard = std::hash<std::string>{}(insert.measurement) % seastar::smp::count;
            batchWrites.push_back(engineSharded.invoke_on(shard, 
                [insert = std::move(insert)](Engine& engine) mutable {
                    return engine.insert(std::move(insert));
                }));
        }
        
        co_await seastar::when_all(batchWrites.begin(), batchWrites.end());
    }
    
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    
    int totalPoints = numBatches * seriesPerBatch * pointsPerSeries;
    double throughput = (totalPoints * 1000.0) / duration.count();
    double batchesPerSecond = (numBatches * 1000.0) / duration.count();
    
    std::cout << "\nBatch Write Performance Results:" << std::endl;
    std::cout << "  Batches: " << numBatches << std::endl;
    std::cout << "  Series per batch: " << seriesPerBatch << std::endl;
    std::cout << "  Points per series: " << pointsPerSeries << std::endl;
    std::cout << "  Total points: " << totalPoints << std::endl;
    std::cout << "  Time: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(0) 
              << throughput << " points/second" << std::endl;
    std::cout << "  Batch rate: " << std::fixed << std::setprecision(1) 
              << batchesPerSecond << " batches/second" << std::endl;
    
    co_await engineSharded.invoke_on_all([](Engine& engine) {
        return engine.stop();
    });
    co_await engineSharded.stop();
    
    EXPECT_GT(throughput, 10000);  // Should handle at least 10K points/second in batches
}

TEST_F(WritePerformanceTest, BatchWritePerformance) {
    seastar::app_template app;
    
    auto exitCode = app.run(0, nullptr, [&] {
        return testBatchWritePerformance().then([&] {
            seastar::engine().exit(0);
        }).handle_exception([&](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                std::cerr << "Test failed with exception: " << e.what() << std::endl;
            }
            seastar::engine().exit(1);
        });
    });
    
    EXPECT_EQ(exitCode, 0);
}