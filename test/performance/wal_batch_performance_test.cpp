#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include "../../lib/storage/wal.hpp"
#include "../../lib/storage/memory_store.hpp"
#include "../../lib/core/tsdb_value.hpp"
#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

class WALBatchPerformanceTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test WAL files
        for (int i = 1000; i <= 1010; i++) {
            try {
                WAL::remove(i);
            } catch (...) {}
        }
    }
    
    void TearDown() override {
        // Clean up test WAL files
        for (int i = 1000; i <= 1010; i++) {
            try {
                WAL::remove(i);
            } catch (...) {}
        }
    }
};

TEST_F(WALBatchPerformanceTest, CompareSingleVsBatchInserts) {
    seastar::app_template app;
    
    app.run(0, nullptr, [this]() {
        return seastar::async([this]() {
            const int NUM_SERIES = 100;
            const int POINTS_PER_SERIES = 1000;
            
            // Test 1: Single inserts with immediate flush (old behavior)
            {
                WAL wal1(1000);
                MemoryStore store1(1000);
                wal1.init(&store1).get();
                wal1.setImmediateFlush(true);  // Old behavior
                
                auto start = std::chrono::high_resolution_clock::now();
                
                for (int s = 0; s < NUM_SERIES; s++) {
                    TSDBInsert<double> insert("metric", "value" + std::to_string(s));
                    insert.tags = {{"host", "server" + std::to_string(s % 10)}};
                    
                    for (int i = 0; i < POINTS_PER_SERIES; i++) {
                        insert.timestamps.push_back(1000000 + i * 1000);
                        insert.values.push_back(100.0 + i);
                    }
                    
                    wal1.insert(insert).get();
                }
                
                wal1.finalFlush().get();
                wal1.close().get();
                
                auto end = std::chrono::high_resolution_clock::now();
                auto duration1 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                
                std::cout << "Single inserts with immediate flush: " << duration1.count() << " ms" << std::endl;
                
                // Verify file size
                auto size1 = wal1.getCurrentSize();
                std::cout << "  WAL size: " << size1 << " bytes" << std::endl;
            }
            
            // Test 2: Single inserts with batched flush (new behavior)
            {
                WAL wal2(1001);
                MemoryStore store2(1001);
                wal2.init(&store2).get();
                wal2.setImmediateFlush(false);  // New behavior - batch flushes
                
                auto start = std::chrono::high_resolution_clock::now();
                
                for (int s = 0; s < NUM_SERIES; s++) {
                    TSDBInsert<double> insert("metric", "value" + std::to_string(s));
                    insert.tags = {{"host", "server" + std::to_string(s % 10)}};
                    
                    for (int i = 0; i < POINTS_PER_SERIES; i++) {
                        insert.timestamps.push_back(1000000 + i * 1000);
                        insert.values.push_back(100.0 + i);
                    }
                    
                    wal2.insert(insert).get();
                }
                
                wal2.finalFlush().get();
                wal2.close().get();
                
                auto end = std::chrono::high_resolution_clock::now();
                auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                
                std::cout << "Single inserts with batched flush: " << duration2.count() << " ms" << std::endl;
                
                // Verify file size
                auto size2 = wal2.getCurrentSize();
                std::cout << "  WAL size: " << size2 << " bytes" << std::endl;
                
                // Should be significantly faster
                EXPECT_LT(duration2.count(), duration1.count());
            }
            
            // Test 3: Batch inserts (new batch API)
            {
                WAL wal3(1002);
                MemoryStore store3(1002);
                wal3.init(&store3).get();
                
                auto start = std::chrono::high_resolution_clock::now();
                
                // Batch 10 series at a time
                for (int batch = 0; batch < NUM_SERIES / 10; batch++) {
                    std::vector<TSDBInsert<double>> batchInserts;
                    
                    for (int s = 0; s < 10; s++) {
                        int seriesIdx = batch * 10 + s;
                        TSDBInsert<double> insert("metric", "value" + std::to_string(seriesIdx));
                        insert.tags = {{"host", "server" + std::to_string(seriesIdx % 10)}};
                        
                        for (int i = 0; i < POINTS_PER_SERIES; i++) {
                            insert.timestamps.push_back(1000000 + i * 1000);
                            insert.values.push_back(100.0 + i);
                        }
                        
                        batchInserts.push_back(std::move(insert));
                    }
                    
                    wal3.insertBatch(batchInserts).get();
                }
                
                wal3.finalFlush().get();
                wal3.close().get();
                
                auto end = std::chrono::high_resolution_clock::now();
                auto duration3 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                
                std::cout << "Batch inserts (10 series per batch): " << duration3.count() << " ms" << std::endl;
                
                // Verify file size
                auto size3 = wal3.getCurrentSize();
                std::cout << "  WAL size: " << size3 << " bytes" << std::endl;
                
                // Should be fastest
                EXPECT_LE(duration3.count(), duration2.count());
            }
            
            std::cout << "\nOptimization Summary:" << std::endl;
            std::cout << "- Batched flush reduces I/O operations" << std::endl;
            std::cout << "- Larger block size (64KB) improves SSD performance" << std::endl;
            std::cout << "- Batch insert API reduces overhead for multiple series" << std::endl;
        });
    });
}

TEST_F(WALBatchPerformanceTest, VerifyDataIntegrity) {
    seastar::app_template app;
    
    app.run(0, nullptr, [this]() {
        return seastar::async([this]() {
            // Write data with batch API
            {
                WAL wal(1003);
                MemoryStore store(1003);
                wal.init(&store).get();
                
                std::vector<TSDBInsert<double>> batchInserts;
                
                for (int s = 0; s < 5; s++) {
                    TSDBInsert<double> insert("test_metric", "field" + std::to_string(s));
                    insert.tags = {{"tag1", "value" + std::to_string(s)}};
                    
                    for (int i = 0; i < 10; i++) {
                        insert.timestamps.push_back(1000 + i);
                        insert.values.push_back(s * 10.0 + i);
                    }
                    
                    batchInserts.push_back(insert);
                }
                
                wal.insertBatch(batchInserts).get();
                wal.close().get();
            }
            
            // Read back and verify
            {
                MemoryStore recoveredStore(1003);
                WALReader reader(WAL::sequenceNumberToFilename(1003));
                reader.readAll(&recoveredStore).get();
                
                // Verify all series were recovered
                for (int s = 0; s < 5; s++) {
                    TSDBInsert<double> expected("test_metric", "field" + std::to_string(s));
                    expected.tags = {{"tag1", "value" + std::to_string(s)}};
                    std::string seriesKey = expected.seriesKey();
                    
                    auto result = recoveredStore.querySeries<double>(seriesKey);
                    ASSERT_TRUE(result.has_value());
                    
                    const auto& series = result.value();
                    EXPECT_EQ(series.timestamps.size(), 10);
                    EXPECT_EQ(series.values.size(), 10);
                    
                    for (int i = 0; i < 10; i++) {
                        EXPECT_EQ(series.timestamps[i], 1000 + i);
                        EXPECT_DOUBLE_EQ(series.values[i], s * 10.0 + i);
                    }
                }
            }
            
            std::cout << "Data integrity verified - batch inserts correctly persisted and recovered" << std::endl;
        });
    });
}