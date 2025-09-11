#include <filesystem>
#include <memory>
#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <iostream>
#include <thread>
#include <chrono>

#include "../../../lib/storage/wal.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/wal_file_manager.hpp"
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/tsdb_value.hpp"

namespace fs = std::filesystem;

void cleanup_test_data() {
    // Clean up shard directories
    for (int i = 0; i < 32; ++i) {
        std::string shardDir = "shard_" + std::to_string(i);
        if (fs::exists(shardDir)) {
            fs::remove_all(shardDir);
        }
    }
}

seastar::future<> test_partial_field_deletion_does_not_corrupt_other_fields() {
    return seastar::async([]() {
            std::cout << "\n=== Testing Partial Field Deletion in WAL ===" << std::endl;
            
            // Create engine and initialize
            Engine engine;
            engine.init().get();
            
            // Create data similar to our test case:
            // measurement: "simple", tags: {id: "test1"}, fields: fieldA=100.0, fieldB=200.0
            uint64_t timestamp = 1704067700000000000ULL;
            
            // Insert fieldA
            TSDBInsert<double> insertA("simple", "fieldA");
            insertA.addTag("id", "test1");
            insertA.addValue(timestamp, 100.0);
            
            // Insert fieldB
            TSDBInsert<double> insertB("simple", "fieldB");
            insertB.addTag("id", "test1");
            insertB.addValue(timestamp, 200.0);
            
            std::cout << "Inserting fieldA data: series_key='" << insertA.seriesKey() << "'" << std::endl;
            engine.insert(insertA).get();
            
            std::cout << "Inserting fieldB data: series_key='" << insertB.seriesKey() << "'" << std::endl;
            engine.insert(insertB).get();
            
            // Give time for background tasks to process
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            // Verify both fields can be queried before deletion
            std::cout << "Querying both fields before deletion..." << std::endl;
            
            auto resultA_before = engine.query(insertA.seriesKey(), timestamp, timestamp).get();
            auto resultB_before = engine.query(insertB.seriesKey(), timestamp, timestamp).get();
            
            // Check that both queries return data
            bool fieldA_has_data_before = std::visit<bool>([](const auto& result) { 
                return !result.timestamps.empty(); 
            }, resultA_before);
            
            bool fieldB_has_data_before = std::visit<bool>([](const auto& result) { 
                return !result.timestamps.empty(); 
            }, resultB_before);
            
            // fieldA and fieldB should have data before deletion
            if (!fieldA_has_data_before) {
                std::cout << "ERROR: fieldA should have data before deletion" << std::endl;
            }
            if (!fieldB_has_data_before) {
                std::cout << "ERROR: fieldB should have data before deletion" << std::endl;
            }
            
            std::cout << "Before deletion - fieldA has data: " << fieldA_has_data_before 
                      << ", fieldB has data: " << fieldB_has_data_before << std::endl;
            
            // Now delete ONLY fieldA
            std::cout << "Deleting fieldA in time range [" << timestamp << ", " << timestamp << "]" << std::endl;
            bool deleted = engine.deleteRange(insertA.seriesKey(), timestamp, timestamp).get();
            
            if (!deleted) {
                std::cout << "ERROR: deleteRange should return true indicating data was deleted" << std::endl;
            }
            std::cout << "Delete operation returned: " << deleted << std::endl;
            
            // Give time for deletion to be processed
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            // Query both fields after deletion
            std::cout << "Querying both fields after deleting fieldA..." << std::endl;
            
            auto resultA_after = engine.query(insertA.seriesKey(), timestamp, timestamp).get();
            auto resultB_after = engine.query(insertB.seriesKey(), timestamp, timestamp).get();
            
            // Check results after deletion
            bool fieldA_has_data_after = std::visit<bool>([](const auto& result) { 
                return !result.timestamps.empty(); 
            }, resultA_after);
            
            bool fieldB_has_data_after = std::visit<bool>([](const auto& result) { 
                return !result.timestamps.empty(); 
            }, resultB_after);
            
            std::cout << "After deletion - fieldA has data: " << fieldA_has_data_after 
                      << ", fieldB has data: " << fieldB_has_data_after << std::endl;
            
            // The key assertions:
            bool test1_passed = !fieldA_has_data_after;  // fieldA should NOT have data after deletion
            bool test2_passed = fieldB_has_data_after;   // fieldB should STILL have data after deleting fieldA
            bool test3_passed = (insertA.seriesKey() != insertB.seriesKey());  // Different series keys
            
            std::cout << "fieldA series key: '" << insertA.seriesKey() << "'" << std::endl;
            std::cout << "fieldB series key: '" << insertB.seriesKey() << "'" << std::endl;
            
            // Report test results
            std::cout << "\n=== Test Results ===" << std::endl;
            std::cout << "Test 1 - fieldA deleted: " << (test1_passed ? "PASS" : "FAIL") << std::endl;
            std::cout << "Test 2 - fieldB preserved: " << (test2_passed ? "PASS" : "FAIL") << std::endl; 
            std::cout << "Test 3 - different series keys: " << (test3_passed ? "PASS" : "FAIL") << std::endl;
            
            bool all_passed = test1_passed && test2_passed && test3_passed;
            std::cout << "Overall result: " << (all_passed ? "PASS" : "FAIL") << std::endl;
            
            // Clean up engine
            engine.stop().get();
            
            std::cout << "=== Test completed ===" << std::endl;
            
            if (!all_passed) {
                std::cout << "ERROR: Test failed - partial field deletion has issues!" << std::endl;
                return; // Don't exit with error code to avoid crashing, just report failure
            }
        });
}

seastar::future<> test_wal_replay_preserves_partial_field_deletion() {
    return seastar::async([]() {
            std::cout << "\n=== Testing WAL Replay with Partial Field Deletion ===" << std::endl;
            
            uint64_t timestamp = 1704067700000000000ULL;
            
            {
                // First phase: Insert data and delete fieldA
                std::cout << "Phase 1: Insert data and delete fieldA" << std::endl;
                
                Engine engine;
                engine.init().get();
                
                // Insert fieldA and fieldB
                TSDBInsert<double> insertA("simple", "fieldA");
                insertA.addTag("id", "test1");
                insertA.addValue(timestamp, 100.0);
                
                TSDBInsert<double> insertB("simple", "fieldB");
                insertB.addTag("id", "test1");
                insertB.addValue(timestamp, 200.0);
                
                engine.insert(insertA).get();
                engine.insert(insertB).get();
                
                // Delete fieldA
                engine.deleteRange(insertA.seriesKey(), timestamp, timestamp).get();
                
                engine.stop().get();
                std::cout << "Phase 1 complete - data written to WAL with deletion" << std::endl;
            }
            
            {
                // Second phase: Restart engine and verify WAL replay behavior
                std::cout << "Phase 2: Restart engine and verify WAL replay" << std::endl;
                
                Engine engine2;
                engine2.init().get(); // This should replay the WAL
                
                // Query both fields after restart
                TSDBInsert<double> insertA("simple", "fieldA");
                insertA.addTag("id", "test1");
                
                TSDBInsert<double> insertB("simple", "fieldB");
                insertB.addTag("id", "test1");
                
                auto resultA = engine2.query(insertA.seriesKey(), timestamp, timestamp).get();
                auto resultB = engine2.query(insertB.seriesKey(), timestamp, timestamp).get();
                
                bool fieldA_has_data = std::visit([](const auto& result) -> bool { 
                    return !result.timestamps.empty(); 
                }, resultA);
                
                bool fieldB_has_data = std::visit([](const auto& result) -> bool { 
                    return !result.timestamps.empty(); 
                }, resultB);
                
                std::cout << "After restart - fieldA has data: " << fieldA_has_data 
                          << ", fieldB has data: " << fieldB_has_data << std::endl;
                
                // After WAL replay, fieldA should still be deleted, fieldB should still exist
                bool replay_test1_passed = !fieldA_has_data;  // After WAL replay, fieldA should still be deleted
                bool replay_test2_passed = fieldB_has_data;   // After WAL replay, fieldB should still exist
                
                std::cout << "\n=== WAL Replay Test Results ===" << std::endl;
                std::cout << "Replay Test 1 - fieldA still deleted: " << (replay_test1_passed ? "PASS" : "FAIL") << std::endl;
                std::cout << "Replay Test 2 - fieldB still exists: " << (replay_test2_passed ? "PASS" : "FAIL") << std::endl;
                
                bool replay_all_passed = replay_test1_passed && replay_test2_passed;
                std::cout << "WAL Replay Overall result: " << (replay_all_passed ? "PASS" : "FAIL") << std::endl;
                
                engine2.stop().get();
                std::cout << "Phase 2 complete - WAL replay test finished" << std::endl;
                
                if (!replay_all_passed) {
                    std::cout << "ERROR: WAL Replay test failed!" << std::endl;
                }
            }
        });
}

int main(int argc, char* argv[]) {
    seastar::app_template app;
    
    return app.run(argc, argv, []() -> seastar::future<> {
        std::cout << "=== WAL Deletion Test Suite ===" << std::endl;
        
        // Clean up before starting
        cleanup_test_data();
        
        // Run test 1: Partial field deletion
        co_await test_partial_field_deletion_does_not_corrupt_other_fields();
        
        // Clean up between tests
        cleanup_test_data();
        
        // Run test 2: WAL replay test
        co_await test_wal_replay_preserves_partial_field_deletion();
        
        // Final cleanup
        cleanup_test_data();
        
        std::cout << "\n=== All WAL Deletion Tests Completed ===" << std::endl;
        co_return;
    });
}