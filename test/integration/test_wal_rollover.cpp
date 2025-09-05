// Test for WAL rollover and TSM creation
#include <iostream>
#include <filesystem>
#include <chrono>
#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sleep.hh>
#include "../lib/core/engine.hpp"

namespace fs = std::filesystem;

void cleanup_test_files() {
    std::string shardDir = "shard_0";
    
    // Remove WAL and TSM files
    for (const auto& entry : fs::directory_iterator(".")) {
        auto path = entry.path();
        if ((path.extension() == ".wal" || path.extension() == ".tsm") && 
            path.string().find(shardDir) != std::string::npos) {
            try {
                fs::remove(path);
                std::cout << "Removed: " << path.filename() << std::endl;
            } catch (...) {}
        }
    }
}

size_t count_tsm_files() {
    size_t count = 0;
    std::string shardDir = "shard_0";
    
    for (const auto& entry : fs::directory_iterator(".")) {
        if (entry.path().extension() == ".tsm" && 
            entry.path().string().find(shardDir) != std::string::npos) {
            count++;
            std::cout << "Found TSM: " << entry.path().filename() << std::endl;
        }
    }
    return count;
}

int main(int argc, char** argv) {
    seastar::app_template app;
    
    return app.run(argc, argv, []() {
        return seastar::async([]() {
            std::cout << "\n========================================" << std::endl;
            std::cout << "WAL Rollover and TSM Creation Test" << std::endl;
            std::cout << "========================================\n" << std::endl;
            
            // Clean up any existing test files
            cleanup_test_files();
            
            // Create shard directory
            fs::create_directories("shard_0");
            
            Engine engine;
            std::cout << "Initializing engine..." << std::endl;
            engine.init().get();
            
            // Start background TSM writer task
            std::cout << "Starting background TSM writer..." << std::endl;
            auto bg_task = engine.startBackgroundTasks();
            
            std::cout << "\nInitial TSM files: " << count_tsm_files() << std::endl;
            
            // The WAL threshold is 16MB - write data to exceed it
            std::cout << "\nWriting data to trigger WAL rollover (16MB threshold)..." << std::endl;
            std::cout << "Note: With new optimizations, writes are batched for better performance\n" << std::endl;
            
            const size_t SERIES_COUNT = 50;
            const size_t POINTS_PER_BATCH = 2000;
            const size_t BATCHES = 15;
            
            size_t totalPoints = 0;
            size_t estimatedMB = 0;
            auto startTime = std::chrono::high_resolution_clock::now();
            
            for (size_t batch = 0; batch < BATCHES; batch++) {
                for (size_t s = 0; s < SERIES_COUNT; s++) {
                    TSDBInsert<double> insert("metric", "field_" + std::to_string(s));
                    insert.tags = {{"host", "h" + std::to_string(s % 5)}};
                    
                    uint64_t baseTime = 1000000000 + batch * POINTS_PER_BATCH * 1000;
                    for (size_t i = 0; i < POINTS_PER_BATCH; i++) {
                        insert.addValue(baseTime + i * 1000, s * 100.0 + i);
                    }
                    
                    totalPoints += POINTS_PER_BATCH;
                    engine.insert(insert).get();
                }
                
                // Estimate: ~32 bytes per point (compressed)
                estimatedMB = (totalPoints * 32) / (1024 * 1024);
                std::cout << "Batch " << (batch + 1) << "/" << BATCHES 
                         << " - Data: ~" << estimatedMB << " MB" << std::endl;
                
                // Give time for background processing
                seastar::sleep(std::chrono::milliseconds(200)).get();
                
                size_t tsmCount = count_tsm_files();
                if (tsmCount > 0) {
                    std::cout << "\n*** WAL ROLLOVER DETECTED! ***" << std::endl;
                    break;
                }
            }
            
            // Wait for background tasks
            std::cout << "\nWaiting for background tasks..." << std::endl;
            seastar::sleep(std::chrono::seconds(2)).get();
            
            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
            
            size_t finalTsmCount = count_tsm_files();
            
            std::cout << "\n========================================" << std::endl;
            std::cout << "Results:" << std::endl;
            std::cout << "========================================" << std::endl;
            std::cout << "Points written: " << totalPoints << std::endl;
            std::cout << "Estimated size: ~" << estimatedMB << " MB" << std::endl;
            std::cout << "Time: " << duration.count() << " ms" << std::endl;
            std::cout << "TSM files: " << finalTsmCount << std::endl;
            
            if (finalTsmCount > 0) {
                std::cout << "\n✓ SUCCESS: WAL rollover working!" << std::endl;
                
                // Verify data
                TSDBInsert<double> q("metric", "field_0");
                q.tags = {{"host", "h0"}};
                auto result = engine.query(q.seriesKey(), 1000000000, 2000000000).get();
                
                size_t points = std::visit([](auto&& r) { return r.timestamps.size(); }, result);
                std::cout << "Data verified: " << points << " points recovered" << std::endl;
            } else {
                std::cout << "\n✗ No TSM files created yet" << std::endl;
            }
            
            // Shutdown
            bg_task.get();
            cleanup_test_files();
            
            std::cout << "\nTest complete!" << std::endl;
        });
    });
}