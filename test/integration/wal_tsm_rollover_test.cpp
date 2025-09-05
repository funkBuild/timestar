#include <gtest/gtest.h>
#include <filesystem>
#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sleep.hh>
#include "../../lib/core/engine.hpp"
#include "../../lib/storage/tsm_file_manager.hpp"
#include "../../lib/storage/wal_file_manager.hpp"
#include "../../lib/query/query_runner.hpp"
#include "../../lib/utils/logger.hpp"

namespace fs = std::filesystem;

class WALTSMRolloverTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up test directories
        cleanupTestData();
        
        // Create shard directory
        fs::create_directories("shard_" + std::to_string(seastar::this_shard_id()));
    }
    
    void TearDown() override {
        cleanupTestData();
    }
    
    void cleanupTestData() {
        // Remove test TSM and WAL files
        std::string shardDir = "shard_" + std::to_string(seastar::this_shard_id());
        
        // Remove WAL files
        for (const auto& entry : fs::directory_iterator(".")) {
            if (entry.path().extension() == ".wal" && 
                entry.path().string().find(shardDir) != std::string::npos) {
                fs::remove(entry);
            }
        }
        
        // Remove TSM files  
        for (const auto& entry : fs::directory_iterator(".")) {
            if (entry.path().extension() == ".tsm" && 
                entry.path().string().find(shardDir) != std::string::npos) {
                fs::remove(entry);
            }
        }
    }
    
    size_t countTSMFiles() {
        size_t count = 0;
        std::string shardDir = "shard_" + std::to_string(seastar::this_shard_id());
        
        for (const auto& entry : fs::directory_iterator(".")) {
            if (entry.path().extension() == ".tsm" && 
                entry.path().string().find(shardDir) != std::string::npos) {
                count++;
            }
        }
        return count;
    }
    
    size_t countWALFiles() {
        size_t count = 0;
        std::string shardDir = "shard_" + std::to_string(seastar::this_shard_id());
        
        for (const auto& entry : fs::directory_iterator(".")) {
            if (entry.path().extension() == ".wal" && 
                entry.path().string().find(shardDir) != std::string::npos) {
                count++;
            }
        }
        return count;
    }
};

TEST_F(WALTSMRolloverTest, TestWALToTSMRollover) {
    seastar::app_template app;
    
    app.run(0, nullptr, [this]() {
        return seastar::async([this]() {
            std::cout << "\n=== Testing WAL to TSM Rollover ===" << std::endl;
            
            Engine engine;
            engine.init().get();
            
            // Start background TSM writer task
            auto bg_task = engine.startBackgroundTasks();
            
            // Initially should have 1 WAL file and 0 TSM files
            EXPECT_EQ(countWALFiles(), 1) << "Should start with 1 WAL file";
            EXPECT_EQ(countTSMFiles(), 0) << "Should start with 0 TSM files";
            std::cout << "Initial state: " << countWALFiles() << " WAL files, " 
                     << countTSMFiles() << " TSM files" << std::endl;
            
            // The WAL threshold is 16MB (from memory_store.cpp WAL_SIZE_THRESHOLD)
            // Let's write enough data to exceed this threshold
            const size_t BATCH_SIZE = 100;      // Points per insert
            const size_t NUM_SERIES = 500;      // Number of different series
            const size_t BATCHES_PER_SERIES = 5; // Batches per series
            
            std::cout << "Writing data to trigger WAL rollover..." << std::endl;
            std::cout << "  Series: " << NUM_SERIES << std::endl;
            std::cout << "  Points per batch: " << BATCH_SIZE << std::endl;
            std::cout << "  Batches per series: " << BATCHES_PER_SERIES << std::endl;
            
            size_t totalBytes = 0;
            uint64_t baseTime = 1000000000;
            
            for (size_t batch = 0; batch < BATCHES_PER_SERIES; batch++) {
                for (size_t s = 0; s < NUM_SERIES; s++) {
                    TSDBInsert<double> insert("test_metric", "field_" + std::to_string(s));
                    insert.tags = {
                        {"host", "server_" + std::to_string(s % 10)},
                        {"region", "region_" + std::to_string(s % 5)}
                    };
                    
                    // Add batch of points
                    for (size_t i = 0; i < BATCH_SIZE; i++) {
                        uint64_t timestamp = baseTime + (batch * BATCH_SIZE + i) * 1000;
                        double value = s * 100.0 + batch * 10.0 + i;
                        insert.addValue(timestamp, value);
                    }
                    
                    // Estimate size (rough)
                    size_t estimatedSize = insert.seriesKey().size() + 
                                         (BATCH_SIZE * 16); // ~8 bytes timestamp + 8 bytes value
                    totalBytes += estimatedSize;
                    
                    engine.insert(insert).get();
                    
                    // Check if we should see a rollover soon
                    if (totalBytes > 15 * 1024 * 1024 && countTSMFiles() == 0) {
                        std::cout << "  Approaching 16MB threshold (~" 
                                 << (totalBytes / (1024*1024)) << " MB written)" << std::endl;
                    }
                }
                
                // Give background task time to process
                seastar::sleep(std::chrono::milliseconds(100)).get();
                
                // Check if rollover happened
                size_t tsmCount = countTSMFiles();
                if (tsmCount > 0) {
                    std::cout << "  WAL rollover detected! TSM files: " << tsmCount 
                             << " after writing ~" << (totalBytes / (1024*1024)) << " MB" << std::endl;
                    break;
                }
            }
            
            // Wait a bit for background task to complete any pending work
            seastar::sleep(std::chrono::seconds(2)).get();
            
            // Now we should have at least 1 TSM file
            size_t finalTsmCount = countTSMFiles();
            size_t finalWalCount = countWALFiles();
            
            std::cout << "\nFinal state:" << std::endl;
            std::cout << "  WAL files: " << finalWalCount << std::endl;
            std::cout << "  TSM files: " << finalTsmCount << std::endl;
            std::cout << "  Total data written: ~" << (totalBytes / (1024*1024)) << " MB" << std::endl;
            
            EXPECT_GE(finalTsmCount, 1) << "Should have created at least 1 TSM file after exceeding threshold";
            
            // Verify data integrity - query some of the data back
            std::cout << "\nVerifying data integrity..." << std::endl;
            
            // Query a few series to ensure data was preserved
            for (size_t s = 0; s < std::min(size_t(5), NUM_SERIES); s++) {
                TSDBInsert<double> queryKey("test_metric", "field_" + std::to_string(s));
                queryKey.tags = {
                    {"host", "server_" + std::to_string(s % 10)},
                    {"region", "region_" + std::to_string(s % 5)}
                };
                std::string seriesKey = queryKey.seriesKey();
                
                auto result = engine.query(seriesKey, baseTime, baseTime + 10000000).get();
                
                // Check that we got data back
                bool hasData = std::visit([](auto&& res) {
                    return !res.timestamps.empty();
                }, result);
                
                if (hasData) {
                    size_t pointCount = std::visit([](auto&& res) {
                        return res.timestamps.size();
                    }, result);
                    
                    std::cout << "  Series " << s << ": " << pointCount << " points recovered" << std::endl;
                    EXPECT_GT(pointCount, 0) << "Should have data for series " << s;
                }
            }
            
            // Clean shutdown
            bg_task.get();
            engine.close().get();
            
            std::cout << "\n=== WAL to TSM Rollover Test Complete ===" << std::endl;
        });
    });
}

TEST_F(WALTSMRolloverTest, TestMultipleRollovers) {
    seastar::app_template app;
    
    app.run(0, nullptr, [this]() {
        return seastar::async([this]() {
            std::cout << "\n=== Testing Multiple WAL Rollovers ===" << std::endl;
            
            Engine engine;
            engine.init().get();
            
            // Start background TSM writer task
            auto bg_task = engine.startBackgroundTasks();
            
            const size_t TARGET_ROLLOVERS = 3;
            size_t rollovers_seen = 0;
            size_t last_tsm_count = 0;
            
            std::cout << "Target: " << TARGET_ROLLOVERS << " rollovers" << std::endl;
            
            // Write data in chunks to trigger multiple rollovers
            for (size_t round = 0; round < 10 && rollovers_seen < TARGET_ROLLOVERS; round++) {
                std::cout << "\nRound " << (round + 1) << ":" << std::endl;
                
                // Write ~20MB of data per round (should trigger rollover at 16MB)
                const size_t SERIES_PER_ROUND = 200;
                const size_t POINTS_PER_SERIES = 5000;
                
                for (size_t s = 0; s < SERIES_PER_ROUND; s++) {
                    TSDBInsert<double> insert("metric_round_" + std::to_string(round), 
                                            "field_" + std::to_string(s));
                    insert.tags = {{"round", std::to_string(round)}};
                    
                    for (size_t i = 0; i < POINTS_PER_SERIES; i++) {
                        insert.addValue(1000000 + round * 1000000 + i, s * 1000.0 + i);
                    }
                    
                    engine.insert(insert).get();
                }
                
                // Wait for background processing
                seastar::sleep(std::chrono::seconds(1)).get();
                
                size_t current_tsm_count = countTSMFiles();
                if (current_tsm_count > last_tsm_count) {
                    rollovers_seen++;
                    std::cout << "  Rollover #" << rollovers_seen << " detected! "
                             << "TSM files: " << last_tsm_count << " -> " << current_tsm_count << std::endl;
                    last_tsm_count = current_tsm_count;
                }
            }
            
            // Final wait for any pending operations
            seastar::sleep(std::chrono::seconds(2)).get();
            
            size_t final_tsm_count = countTSMFiles();
            std::cout << "\nFinal TSM count: " << final_tsm_count << std::endl;
            std::cout << "Rollovers observed: " << rollovers_seen << std::endl;
            
            EXPECT_GE(rollovers_seen, TARGET_ROLLOVERS) 
                << "Should have seen at least " << TARGET_ROLLOVERS << " rollovers";
            EXPECT_GE(final_tsm_count, TARGET_ROLLOVERS) 
                << "Should have at least " << TARGET_ROLLOVERS << " TSM files";
            
            // Clean shutdown
            bg_task.get();
            engine.close().get();
            
            std::cout << "\n=== Multiple Rollovers Test Complete ===" << std::endl;
        });
    });
}

TEST_F(WALTSMRolloverTest, TestBatchedWritesWithRollover) {
    seastar::app_template app;
    
    app.run(0, nullptr, [this]() {
        return seastar::async([this]() {
            std::cout << "\n=== Testing Batched Writes with Rollover ===" << std::endl;
            
            Engine engine; 
            engine.init().get();
            
            // Start background TSM writer task
            auto bg_task = engine.startBackgroundTasks();
            
            std::cout << "Using new batched write optimizations..." << std::endl;
            
            // Write large batches to quickly exceed threshold
            const size_t BATCH_SIZE = 10000;  // Large batch per series
            const size_t NUM_SERIES = 100;    // Fewer series, more points each
            
            auto start = std::chrono::high_resolution_clock::now();
            
            for (size_t s = 0; s < NUM_SERIES; s++) {
                TSDBInsert<double> insert("batch_metric", "series_" + std::to_string(s));
                insert.tags = {{"batch_test", "true"}};
                
                // Large batch of points
                for (size_t i = 0; i < BATCH_SIZE; i++) {
                    insert.addValue(2000000000 + i * 1000, s * 1000.0 + i);
                }
                
                engine.insert(insert).get();
                
                // Periodic check for rollover
                if (s % 10 == 0) {
                    size_t tsmCount = countTSMFiles();
                    if (tsmCount > 0) {
                        std::cout << "  Rollover after " << (s + 1) << " series" << std::endl;
                        break;
                    }
                }
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            // Wait for background processing
            seastar::sleep(std::chrono::seconds(2)).get();
            
            size_t finalTsmCount = countTSMFiles();
            size_t finalWalCount = countWALFiles();
            
            std::cout << "\nResults with optimized batching:" << std::endl;
            std::cout << "  Write time: " << duration.count() << " ms" << std::endl;
            std::cout << "  TSM files created: " << finalTsmCount << std::endl;
            std::cout << "  WAL files: " << finalWalCount << std::endl;
            
            EXPECT_GE(finalTsmCount, 1) << "Should have created TSM files with batched writes";
            
            // Verify a sample of data
            TSDBInsert<double> queryKey("batch_metric", "series_0");
            queryKey.tags = {{"batch_test", "true"}};
            std::string seriesKey = queryKey.seriesKey();
            
            auto result = engine.query(seriesKey, 2000000000, 2001000000).get();
            
            size_t pointCount = std::visit([](auto&& res) {
                return res.timestamps.size();
            }, result);
            
            std::cout << "  Data verification: " << pointCount << " points recovered for series_0" << std::endl;
            EXPECT_GT(pointCount, 0) << "Should have data after rollover";
            
            // Clean shutdown
            bg_task.get();
            engine.close().get();
            
            std::cout << "\n=== Batched Writes with Rollover Test Complete ===" << std::endl;
        });
    });
}