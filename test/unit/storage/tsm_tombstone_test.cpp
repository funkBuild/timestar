#include <gtest/gtest.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <filesystem>
#include "../../../lib/storage/tsm_tombstone.hpp"

using namespace tsdb;
namespace fs = std::filesystem;

class TSMTombstoneTest : public ::testing::Test {
protected:
    std::string testDir = "./test_tombstones";
    std::string tombstonePath;
    
    void SetUp() override {
        // Create test directory
        fs::create_directories(testDir);
        tombstonePath = testDir + "/test.tombstone";
        
        // Remove any existing test file
        if (fs::exists(tombstonePath)) {
            fs::remove(tombstonePath);
        }
    }
    
    void TearDown() override {
        // Clean up test directory
        if (fs::exists(testDir)) {
            fs::remove_all(testDir);
        }
    }
};

// Test basic tombstone creation and loading
TEST_F(TSMTombstoneTest, CreateAndLoad) {
    seastar::thread_local_test_case_guard guard;
    
    seastar::async([this] {
        auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
        
        // Initially should not exist
        EXPECT_FALSE(tombstone->exists().get());
        
        // Add some tombstones
        EXPECT_TRUE(tombstone->addTombstone(1, 1000, 2000).get());
        EXPECT_TRUE(tombstone->addTombstone(2, 3000, 4000).get());
        EXPECT_TRUE(tombstone->addTombstone(1, 5000, 6000).get());
        
        // Flush to disk
        tombstone->flush().get();
        EXPECT_TRUE(tombstone->exists().get());
        
        // Load from disk
        auto tombstone2 = std::make_unique<TSMTombstone>(tombstonePath);
        tombstone2->load().get();
        
        // Verify entries were loaded
        EXPECT_EQ(tombstone2->getEntryCount(), 3);
        EXPECT_EQ(tombstone2->getSeriesCount(), 2);
    });
}

// Test point deletion checking
TEST_F(TSMTombstoneTest, IsDeleted) {
    seastar::thread_local_test_case_guard guard;
    
    seastar::async([this] {
        auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
        
        // Add tombstone ranges
        tombstone->addTombstone(1, 1000, 2000).get();
        tombstone->addTombstone(1, 3000, 4000).get();
        tombstone->addTombstone(2, 5000, 6000).get();
        
        // Test series 1
        EXPECT_FALSE(tombstone->isDeleted(1, 999));   // Before range
        EXPECT_TRUE(tombstone->isDeleted(1, 1000));   // Start of range
        EXPECT_TRUE(tombstone->isDeleted(1, 1500));   // Middle of range
        EXPECT_TRUE(tombstone->isDeleted(1, 2000));   // End of range
        EXPECT_FALSE(tombstone->isDeleted(1, 2001));  // After range
        
        EXPECT_FALSE(tombstone->isDeleted(1, 2500));  // Between ranges
        EXPECT_TRUE(tombstone->isDeleted(1, 3500));   // In second range
        
        // Test series 2
        EXPECT_FALSE(tombstone->isDeleted(2, 4999));  // Before range
        EXPECT_TRUE(tombstone->isDeleted(2, 5500));   // In range
        EXPECT_FALSE(tombstone->isDeleted(2, 6001));  // After range
        
        // Test non-existent series
        EXPECT_FALSE(tombstone->isDeleted(3, 1500));
    });
}

// Test range deletion checking
TEST_F(TSMTombstoneTest, HasDeletedRange) {
    seastar::thread_local_test_case_guard guard;
    
    seastar::async([this] {
        auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
        
        tombstone->addTombstone(1, 1000, 2000).get();
        tombstone->addTombstone(1, 3000, 4000).get();
        
        // Completely outside tombstone
        EXPECT_FALSE(tombstone->hasDeletedRange(1, 0, 999));
        EXPECT_FALSE(tombstone->hasDeletedRange(1, 2001, 2999));
        
        // Overlapping with tombstone
        EXPECT_TRUE(tombstone->hasDeletedRange(1, 500, 1500));   // Partial overlap
        EXPECT_TRUE(tombstone->hasDeletedRange(1, 1500, 2500));  // Partial overlap
        EXPECT_TRUE(tombstone->hasDeletedRange(1, 1200, 1800));  // Completely inside
        EXPECT_TRUE(tombstone->hasDeletedRange(1, 500, 5000));   // Spans multiple
        
        // Exact match
        EXPECT_TRUE(tombstone->hasDeletedRange(1, 1000, 2000));
    });
}

// Test getting tombstone ranges
TEST_F(TSMTombstoneTest, GetTombstoneRanges) {
    seastar::thread_local_test_case_guard guard;
    
    seastar::async([this] {
        auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
        
        tombstone->addTombstone(1, 3000, 4000).get();
        tombstone->addTombstone(1, 1000, 2000).get();  // Add out of order
        tombstone->addTombstone(1, 5000, 6000).get();
        tombstone->addTombstone(2, 7000, 8000).get();
        
        // Get ranges for series 1
        auto ranges1 = tombstone->getTombstoneRanges(1);
        EXPECT_EQ(ranges1.size(), 3);
        
        // Should be sorted
        EXPECT_EQ(ranges1[0].first, 1000);
        EXPECT_EQ(ranges1[0].second, 2000);
        EXPECT_EQ(ranges1[1].first, 3000);
        EXPECT_EQ(ranges1[1].second, 4000);
        EXPECT_EQ(ranges1[2].first, 5000);
        EXPECT_EQ(ranges1[2].second, 6000);
        
        // Get ranges for series 2
        auto ranges2 = tombstone->getTombstoneRanges(2);
        EXPECT_EQ(ranges2.size(), 1);
        EXPECT_EQ(ranges2[0].first, 7000);
        EXPECT_EQ(ranges2[0].second, 8000);
        
        // Non-existent series
        auto ranges3 = tombstone->getTombstoneRanges(3);
        EXPECT_EQ(ranges3.size(), 0);
    });
}

// Test filtering data based on tombstones
TEST_F(TSMTombstoneTest, FilterTombstoned) {
    seastar::thread_local_test_case_guard guard;
    
    seastar::async([this] {
        auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
        
        // Add tombstone ranges
        tombstone->addTombstone(1, 2000, 3000).get();
        tombstone->addTombstone(1, 5000, 6000).get();
        
        // Test data
        std::vector<uint64_t> timestamps = {1000, 2000, 2500, 3000, 4000, 5500, 7000};
        std::vector<double> values = {1.0, 2.0, 2.5, 3.0, 4.0, 5.5, 7.0};
        
        // Filter
        auto [filteredTs, filteredVals] = tombstone->filterTombstoned(1, timestamps, values);
        
        // Expected: 1000, 4000, 7000 (indices 0, 4, 6)
        EXPECT_EQ(filteredTs.size(), 3);
        EXPECT_EQ(filteredVals.size(), 3);
        
        EXPECT_EQ(filteredTs[0], 1000);
        EXPECT_EQ(filteredVals[0], 1.0);
        EXPECT_EQ(filteredTs[1], 4000);
        EXPECT_EQ(filteredVals[1], 4.0);
        EXPECT_EQ(filteredTs[2], 7000);
        EXPECT_EQ(filteredVals[2], 7.0);
        
        // Test with no tombstones for series
        auto [filteredTs2, filteredVals2] = tombstone->filterTombstoned(2, timestamps, values);
        EXPECT_EQ(filteredTs2.size(), timestamps.size());
        EXPECT_EQ(filteredVals2, values);
    });
}

// Test merging tombstone files
TEST_F(TSMTombstoneTest, MergeTombstones) {
    seastar::thread_local_test_case_guard guard;
    
    seastar::async([this] {
        auto tombstone1 = std::make_unique<TSMTombstone>(tombstonePath);
        auto tombstone2 = std::make_unique<TSMTombstone>(testDir + "/other.tombstone");
        
        // Add to first tombstone
        tombstone1->addTombstone(1, 1000, 2000).get();
        tombstone1->addTombstone(2, 3000, 4000).get();
        
        // Add to second tombstone
        tombstone2->addTombstone(1, 5000, 6000).get();
        tombstone2->addTombstone(3, 7000, 8000).get();
        
        // Merge
        tombstone1->merge(*tombstone2);
        
        // Check merged results
        EXPECT_EQ(tombstone1->getEntryCount(), 4);
        EXPECT_EQ(tombstone1->getSeriesCount(), 3);
        
        // Verify all ranges present
        auto ranges1 = tombstone1->getTombstoneRanges(1);
        EXPECT_EQ(ranges1.size(), 2);
        
        auto ranges3 = tombstone1->getTombstoneRanges(3);
        EXPECT_EQ(ranges3.size(), 1);
        EXPECT_EQ(ranges3[0].first, 7000);
    });
}

// Test overlapping and adjacent range merging
TEST_F(TSMTombstoneTest, MergeOverlappingRanges) {
    seastar::thread_local_test_case_guard guard;
    
    seastar::async([this] {
        auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
        
        // Add overlapping ranges
        tombstone->addTombstone(1, 1000, 3000).get();
        tombstone->addTombstone(1, 2000, 4000).get();  // Overlaps
        tombstone->addTombstone(1, 4000, 5000).get();  // Adjacent
        tombstone->addTombstone(1, 6000, 7000).get();  // Separate
        
        // After sorting and merging, should have optimized ranges
        auto ranges = tombstone->getTombstoneRanges(1);
        
        // Should merge overlapping and adjacent ranges
        // Expected: [1000-5000], [6000-7000]
        EXPECT_EQ(ranges.size(), 2);
        EXPECT_EQ(ranges[0].first, 1000);
        EXPECT_EQ(ranges[0].second, 5000);
        EXPECT_EQ(ranges[1].first, 6000);
        EXPECT_EQ(ranges[1].second, 7000);
    });
}

// Test compaction (removing old tombstones)
TEST_F(TSMTombstoneTest, CompactTombstones) {
    seastar::thread_local_test_case_guard guard;
    
    seastar::async([this] {
        auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
        
        // Add tombstones across time range
        tombstone->addTombstone(1, 1000, 2000).get();
        tombstone->addTombstone(1, 3000, 4000).get();
        tombstone->addTombstone(1, 5000, 6000).get();
        tombstone->addTombstone(2, 2500, 3500).get();
        
        // Compact tombstones in range [2000, 4000]
        // Should remove tombstones that are completely within this range
        tombstone->compact(2000, 4000);
        
        // Should keep [1000-2000] (partially outside) and [5000-6000] (completely outside)
        // Should remove [3000-4000] and [2500-3500]
        auto ranges1 = tombstone->getTombstoneRanges(1);
        EXPECT_EQ(ranges1.size(), 2);
        EXPECT_EQ(ranges1[0].first, 1000);
        EXPECT_EQ(ranges1[0].second, 2000);
        EXPECT_EQ(ranges1[1].first, 5000);
        EXPECT_EQ(ranges1[1].second, 6000);
        
        auto ranges2 = tombstone->getTombstoneRanges(2);
        EXPECT_EQ(ranges2.size(), 0);  // Completely removed
    });
}

// Test persistence and checksums
TEST_F(TSMTombstoneTest, PersistenceAndChecksums) {
    seastar::thread_local_test_case_guard guard;
    
    seastar::async([this] {
        // Write tombstones
        {
            auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
            tombstone->addTombstone(1, 1000, 2000).get();
            tombstone->addTombstone(2, 3000, 4000).get();
            tombstone->addTombstone(3, 5000, 6000).get();
            tombstone->flush().get();
        }
        
        // Read back and verify
        {
            auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
            tombstone->load().get();
            
            EXPECT_EQ(tombstone->getEntryCount(), 3);
            
            // Verify specific entries
            EXPECT_TRUE(tombstone->isDeleted(1, 1500));
            EXPECT_TRUE(tombstone->isDeleted(2, 3500));
            EXPECT_TRUE(tombstone->isDeleted(3, 5500));
        }
        
        // Corrupt file and attempt to load (should handle gracefully)
        {
            // Append garbage to file
            std::ofstream file(tombstonePath, std::ios::app | std::ios::binary);
            file << "garbage";
            file.close();
            
            auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
            
            // Should either throw or handle corruption gracefully
            try {
                tombstone->load().get();
                // If it loads, it should have detected corruption
                EXPECT_EQ(tombstone->getEntryCount(), 0);
            } catch (const std::exception& e) {
                // Expected behavior - corruption detected
                EXPECT_TRUE(std::string(e.what()).find("checksum") != std::string::npos ||
                           std::string(e.what()).find("corrupt") != std::string::npos);
            }
        }
    });
}

// Test empty tombstone file
TEST_F(TSMTombstoneTest, EmptyTombstone) {
    seastar::thread_local_test_case_guard guard;
    
    seastar::async([this] {
        auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
        
        // Empty tombstone should work fine
        EXPECT_EQ(tombstone->getEntryCount(), 0);
        EXPECT_EQ(tombstone->getSeriesCount(), 0);
        EXPECT_FALSE(tombstone->isDeleted(1, 1000));
        
        auto ranges = tombstone->getTombstoneRanges(1);
        EXPECT_EQ(ranges.size(), 0);
        
        // Should be able to flush empty tombstone
        tombstone->flush().get();
        
        // And load it back
        auto tombstone2 = std::make_unique<TSMTombstone>(tombstonePath);
        tombstone2->load().get();
        EXPECT_EQ(tombstone2->getEntryCount(), 0);
    });
}

// Test large number of tombstones
TEST_F(TSMTombstoneTest, LargeTombstoneSet) {
    seastar::thread_local_test_case_guard guard;
    
    seastar::async([this] {
        auto tombstone = std::make_unique<TSMTombstone>(tombstonePath);
        
        // Add many tombstones
        const int numSeries = 100;
        const int rangesPerSeries = 10;
        
        for (int series = 1; series <= numSeries; ++series) {
            for (int range = 0; range < rangesPerSeries; ++range) {
                uint64_t start = range * 10000;
                uint64_t end = start + 5000;
                tombstone->addTombstone(series, start, end).get();
            }
        }
        
        EXPECT_EQ(tombstone->getEntryCount(), numSeries * rangesPerSeries);
        EXPECT_EQ(tombstone->getSeriesCount(), numSeries);
        
        // Test query performance with many tombstones
        auto start = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < 10000; ++i) {
            tombstone->isDeleted(50, 25000);  // Middle series, middle time
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        // Should be fast even with many tombstones (< 1ms per lookup)
        EXPECT_LT(duration.count() / 10000.0, 1.0);
        
        // Test persistence
        tombstone->flush().get();
        
        auto tombstone2 = std::make_unique<TSMTombstone>(tombstonePath);
        tombstone2->load().get();
        EXPECT_EQ(tombstone2->getEntryCount(), numSeries * rangesPerSeries);
    });
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    
    // Initialize Seastar for async operations
    seastar::app_template app;
    
    return app.run(argc, argv, [] {
        return seastar::async([] {
            auto result = RUN_ALL_TESTS();
            return result;
        });
    });
}