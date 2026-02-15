// Seastar-based tests for WALFileManager component
// Tests init/recovery, WAL-to-TSM conversion, memory store rollover,
// concurrent batch inserts, error handling during WAL replay, and graceful shutdown.

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <memory>
#include <vector>
#include <string>

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/wal_file_manager.hpp"
#include "../../../lib/storage/tsm_file_manager.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/wal.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/shared_ptr.hh>

#include "../../test_helpers.hpp"

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class WALFileManagerSeastarTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanTestShardDirectories();
    }

    void TearDown() override {
        cleanTestShardDirectories();
    }
};

// ===========================================================================
// 1. Init / Recovery - basic init creates a fresh memory store
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, InitCreatesMemoryStore) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // After engine init, the WALFileManager should be operational.
        // Verify by inserting data (which requires an active memory store).
        TSDBInsert<double> insert("temperature", "value");
        insert.addValue(1000, 20.5);
        eng->insert(std::move(insert)).get();

        auto resultOpt = eng->query("temperature value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 1u);
        EXPECT_DOUBLE_EQ(result.values[0], 20.5);
    }).join().get();
}

// ===========================================================================
// 2. Init / Recovery - recovering data from existing WAL files
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, RecoverFromExistingWALFiles) {
    // Phase 1: Insert data, then stop the engine abruptly (without rollover)
    // so that WAL files remain on disk.
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            TSDBInsert<double> insert("cpu", "usage");
            insert.addValue(1000, 50.0);
            insert.addValue(2000, 60.0);
            insert.addValue(3000, 70.0);
            eng->insert(std::move(insert)).get();

            // Engine stop flushes the current memory store WAL to disk
        }).join().get();
    }

    // Phase 2: Re-init the engine. WALFileManager should discover the WAL
    // files and convert them to TSM during init.
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            // Data from the recovered WAL should now be queryable (from TSM files)
            auto resultOpt = eng->query("cpu usage", 0, UINT64_MAX).get();
            ASSERT_TRUE(resultOpt.has_value());
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 3u);
            EXPECT_DOUBLE_EQ(result.values[0], 50.0);
            EXPECT_DOUBLE_EQ(result.values[1], 60.0);
            EXPECT_DOUBLE_EQ(result.values[2], 70.0);
        }).join().get();
    }
}

// ===========================================================================
// 3. Init / Recovery - recovering multiple WAL files
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, RecoverMultipleWALFiles) {
    // Phase 1: Insert data, rollover, insert more, then stop.
    // This should produce two WAL files (one rolled over, one active).
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            // Insert first batch
            TSDBInsert<double> insert1("sensor", "temp");
            insert1.addValue(1000, 25.0);
            insert1.addValue(2000, 26.0);
            eng->insert(std::move(insert1)).get();

            // Rollover converts current memory store to TSM and creates new one
            eng->rolloverMemoryStore().get();

            // Insert second batch into new memory store
            TSDBInsert<double> insert2("sensor", "humidity");
            insert2.addValue(3000, 60.0);
            insert2.addValue(4000, 65.0);
            eng->insert(std::move(insert2)).get();

            // Stop leaves the second WAL on disk
        }).join().get();
    }

    // Phase 2: Re-init should recover the remaining WAL file
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            // First batch was already converted to TSM during rollover
            auto r1 = eng->query("sensor temp", 0, UINT64_MAX).get();
            ASSERT_TRUE(r1.has_value());
            auto& result1 = std::get<QueryResult<double>>(r1.value());
            EXPECT_EQ(result1.timestamps.size(), 2u);

            // Second batch should also be queryable (recovered from WAL)
            auto r2 = eng->query("sensor humidity", 0, UINT64_MAX).get();
            ASSERT_TRUE(r2.has_value());
            auto& result2 = std::get<QueryResult<double>>(r2.value());
            EXPECT_EQ(result2.timestamps.size(), 2u);
        }).join().get();
    }
}

// ===========================================================================
// 4. WAL-to-TSM conversion pipeline via rollover
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, ConvertWalToTsmViaRollover) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Insert data into the memory store
        TSDBInsert<double> insert("metric", "value");
        for (uint64_t t = 1000; t <= 10000; t += 1000) {
            insert.addValue(t, static_cast<double>(t));
        }
        eng->insert(std::move(insert)).get();

        // Force rollover - this triggers convertWalToTsm internally
        eng->rolloverMemoryStore().get();

        // Data should now be in TSM files and still queryable
        auto resultOpt = eng->query("metric value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 10u);
        EXPECT_DOUBLE_EQ(result.values[0], 1000.0);
        EXPECT_DOUBLE_EQ(result.values[9], 10000.0);
    }).join().get();
}

// ===========================================================================
// 5. WAL-to-TSM conversion with mixed data types
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, ConvertWalToTsmMixedTypes) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Insert float data
        {
            TSDBInsert<double> insert("weather", "temperature");
            insert.addValue(1000, 72.5);
            insert.addValue(2000, 73.0);
            eng->insert(std::move(insert)).get();
        }

        // Insert boolean data
        {
            TSDBInsert<bool> insert("system", "healthy");
            insert.addValue(1000, true);
            insert.addValue(2000, false);
            eng->insert(std::move(insert)).get();
        }

        // Insert string data
        {
            TSDBInsert<std::string> insert("app", "status");
            insert.addValue(1000, std::string("running"));
            insert.addValue(2000, std::string("stopped"));
            eng->insert(std::move(insert)).get();
        }

        // Rollover all data to TSM
        eng->rolloverMemoryStore().get();

        // Verify all types are queryable from TSM
        {
            auto r = eng->query("weather temperature", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            auto& qr = std::get<QueryResult<double>>(r.value());
            EXPECT_EQ(qr.values.size(), 2u);
            EXPECT_DOUBLE_EQ(qr.values[0], 72.5);
        }
        {
            auto r = eng->query("system healthy", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            auto& qr = std::get<QueryResult<bool>>(r.value());
            EXPECT_EQ(qr.values.size(), 2u);
            EXPECT_EQ(qr.values[0], true);
            EXPECT_EQ(qr.values[1], false);
        }
        {
            auto r = eng->query("app status", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            auto& qr = std::get<QueryResult<std::string>>(r.value());
            EXPECT_EQ(qr.values.size(), 2u);
            EXPECT_EQ(qr.values[0], "running");
            EXPECT_EQ(qr.values[1], "stopped");
        }
    }).join().get();
}

// ===========================================================================
// 6. Memory store rollover under load - multiple sequential rollovers
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, MultipleSequentialRollovers) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        const int numRollovers = 5;
        const int pointsPerBatch = 100;

        for (int batch = 0; batch < numRollovers; ++batch) {
            TSDBInsert<double> insert("load_test", "value");
            for (int i = 0; i < pointsPerBatch; ++i) {
                uint64_t t = static_cast<uint64_t>(batch * pointsPerBatch + i + 1);
                insert.addValue(t, static_cast<double>(t));
            }
            eng->insert(std::move(insert)).get();

            // Rollover after each batch
            eng->rolloverMemoryStore().get();
        }

        // All data should be queryable from TSM files
        auto resultOpt = eng->query("load_test value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(),
                  static_cast<size_t>(numRollovers * pointsPerBatch));
    }).join().get();
}

// ===========================================================================
// 7. Memory store rollover preserves data integrity across insert + rollover cycles
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, RolloverPreservesDataIntegrity) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Phase 1: Insert and rollover
        {
            TSDBInsert<double> insert("metric", "cpu");
            insert.addValue(1000, 10.0);
            insert.addValue(2000, 20.0);
            insert.addValue(3000, 30.0);
            eng->insert(std::move(insert)).get();
        }
        eng->rolloverMemoryStore().get();

        // Phase 2: Insert more data (to new memory store)
        {
            TSDBInsert<double> insert("metric", "cpu");
            insert.addValue(4000, 40.0);
            insert.addValue(5000, 50.0);
            eng->insert(std::move(insert)).get();
        }
        eng->rolloverMemoryStore().get();

        // Phase 3: Insert even more
        {
            TSDBInsert<double> insert("metric", "cpu");
            insert.addValue(6000, 60.0);
            eng->insert(std::move(insert)).get();
        }

        // Query should merge data from multiple TSM files + current memory store
        auto resultOpt = eng->query("metric cpu", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 6u);
        EXPECT_DOUBLE_EQ(result.values[0], 10.0);
        EXPECT_DOUBLE_EQ(result.values[5], 60.0);
    }).join().get();
}

// ===========================================================================
// 8. Concurrent batch inserts - multiple series in a single batch
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, BatchInsertMultipleSeries) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        const int batchSize = 20;
        std::vector<TSDBInsert<double>> batch;

        for (int i = 0; i < batchSize; ++i) {
            TSDBInsert<double> insert("batch_test", "field_" + std::to_string(i));
            for (uint64_t t = 1; t <= 50; ++t) {
                insert.addValue(t, static_cast<double>(i * 50 + t));
            }
            batch.push_back(std::move(insert));
        }

        auto timing = eng->insertBatch(std::move(batch)).get();
        EXPECT_EQ(timing.walWriteCount, batchSize);

        // Verify all series are queryable
        for (int i = 0; i < batchSize; ++i) {
            std::string seriesKey = "batch_test field_" + std::to_string(i);
            auto resultOpt = eng->query(seriesKey, 0, UINT64_MAX).get();
            ASSERT_TRUE(resultOpt.has_value())
                << "Series " << seriesKey << " not found";
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 50u)
                << "Series " << seriesKey << " has wrong number of points";
        }
    }).join().get();
}

// ===========================================================================
// 9. Concurrent batch inserts - sequential batches to same series
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, SequentialBatchInsertsSameSeries) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        const int numBatches = 5;
        const int pointsPerBatch = 100;

        for (int batch = 0; batch < numBatches; ++batch) {
            std::vector<TSDBInsert<double>> batchInserts;
            TSDBInsert<double> insert("sequential", "value");
            for (int i = 0; i < pointsPerBatch; ++i) {
                uint64_t t = static_cast<uint64_t>(batch * pointsPerBatch + i + 1);
                insert.addValue(t, static_cast<double>(t));
            }
            batchInserts.push_back(std::move(insert));
            eng->insertBatch(std::move(batchInserts)).get();
        }

        auto resultOpt = eng->query("sequential value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(),
                  static_cast<size_t>(numBatches * pointsPerBatch));
    }).join().get();
}

// ===========================================================================
// 10. Batch insert with empty batch is a no-op
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, EmptyBatchInsertIsNoop) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::vector<TSDBInsert<double>> empty;
        auto timing = eng->insertBatch(std::move(empty)).get();
        EXPECT_EQ(timing.walWriteCount, 0);
    }).join().get();
}

// ===========================================================================
// 11. WAL replay preserves delete operations
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, WALReplayPreservesDeletes) {
    // Phase 1: Insert data, delete some, then stop
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            TSDBInsert<double> insert("metric", "value");
            insert.addValue(1000, 10.0);
            insert.addValue(2000, 20.0);
            insert.addValue(3000, 30.0);
            insert.addValue(4000, 40.0);
            insert.addValue(5000, 50.0);
            eng->insert(std::move(insert)).get();

            // Delete middle range
            eng->deleteRange("metric value", 2000, 4000).get();
        }).join().get();
    }

    // Phase 2: Re-init - WAL replay should apply the delete
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            auto resultOpt = eng->query("metric value", 0, UINT64_MAX).get();
            ASSERT_TRUE(resultOpt.has_value());
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            // After deleting [2000, 4000], only timestamps 1000 and 5000 remain
            EXPECT_EQ(result.timestamps.size(), 2u);
            EXPECT_DOUBLE_EQ(result.values[0], 10.0);
            EXPECT_DOUBLE_EQ(result.values[1], 50.0);
        }).join().get();
    }
}

// ===========================================================================
// 12. Graceful shutdown - close flushes current memory store
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, GracefulShutdownPreservesData) {
    // Phase 1: Insert data and shut down gracefully
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            TSDBInsert<double> insert("shutdown_test", "value");
            insert.addValue(1000, 100.0);
            insert.addValue(2000, 200.0);
            eng->insert(std::move(insert)).get();

            // ScopedEngine destructor calls engine->stop() which triggers close()
        }).join().get();
    }

    // Phase 2: Re-init should recover the data
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            auto resultOpt = eng->query("shutdown_test value", 0, UINT64_MAX).get();
            ASSERT_TRUE(resultOpt.has_value());
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 2u);
            EXPECT_DOUBLE_EQ(result.values[0], 100.0);
            EXPECT_DOUBLE_EQ(result.values[1], 200.0);
        }).join().get();
    }
}

// ===========================================================================
// 13. Insert after rollover works correctly
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, InsertAfterRolloverWorks) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Insert and rollover
        {
            TSDBInsert<double> insert("series", "a");
            insert.addValue(1000, 1.0);
            insert.addValue(2000, 2.0);
            eng->insert(std::move(insert)).get();
        }
        eng->rolloverMemoryStore().get();

        // Insert into new memory store after rollover
        {
            TSDBInsert<double> insert("series", "a");
            insert.addValue(3000, 3.0);
            insert.addValue(4000, 4.0);
            eng->insert(std::move(insert)).get();
        }

        // Should be able to query merged results
        auto resultOpt = eng->query("series a", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 4u);
    }).join().get();
}

// ===========================================================================
// 14. getSeriesType returns correct type from memory store
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, GetSeriesTypeFloat) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TSDBInsert<double> insert("temperature", "value");
        insert.addValue(1000, 25.5);
        eng->insert(std::move(insert)).get();

        // Query proves the series type was correctly tracked
        auto resultOpt = eng->query("temperature value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        EXPECT_TRUE(std::holds_alternative<QueryResult<double>>(resultOpt.value()));
    }).join().get();
}

TEST_F(WALFileManagerSeastarTest, GetSeriesTypeBool) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TSDBInsert<bool> insert("door", "open");
        insert.addValue(1000, true);
        eng->insert(std::move(insert)).get();

        auto resultOpt = eng->query("door open", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        EXPECT_TRUE(std::holds_alternative<QueryResult<bool>>(resultOpt.value()));
    }).join().get();
}

TEST_F(WALFileManagerSeastarTest, GetSeriesTypeString) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TSDBInsert<std::string> insert("app", "log");
        insert.addValue(1000, std::string("hello"));
        eng->insert(std::move(insert)).get();

        auto resultOpt = eng->query("app log", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        EXPECT_TRUE(std::holds_alternative<QueryResult<std::string>>(resultOpt.value()));
    }).join().get();
}

// ===========================================================================
// 15. queryMemoryStores returns data from current memory store
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, QueryMemoryStoreReturnsData) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TSDBInsert<double> insert("cpu", "usage");
        insert.addValue(1000, 50.0);
        insert.addValue(2000, 60.0);
        insert.addValue(3000, 70.0);
        eng->insert(std::move(insert)).get();

        // Data is in memory store - query should return it
        auto resultOpt = eng->query("cpu usage", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 3u);
    }).join().get();
}

// ===========================================================================
// 16. deleteFromMemoryStores removes data correctly
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, DeleteFromMemoryStores) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TSDBInsert<double> insert("metric", "val");
        insert.addValue(1000, 10.0);
        insert.addValue(2000, 20.0);
        insert.addValue(3000, 30.0);
        insert.addValue(4000, 40.0);
        eng->insert(std::move(insert)).get();

        // Delete range [2000, 3000]
        bool deleted = eng->deleteRange("metric val", 2000, 3000).get();
        EXPECT_TRUE(deleted);

        auto resultOpt = eng->query("metric val", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 2u);
        EXPECT_DOUBLE_EQ(result.values[0], 10.0);
        EXPECT_DOUBLE_EQ(result.values[1], 40.0);
    }).join().get();
}

// ===========================================================================
// 17. Delete from memory store - non-existent series
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, DeleteNonExistentSeriesFromMemoryStore) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Delete from a series that was never inserted
        bool deleted = eng->deleteRange("nonexistent value", 0, UINT64_MAX).get();
        EXPECT_FALSE(deleted);
    }).join().get();
}

// ===========================================================================
// 18. Large batch insert followed by rollover
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, LargeBatchInsertAndRollover) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        const int numSeries = 30;
        const int pointsPerSeries = 50;

        std::vector<TSDBInsert<double>> batch;
        for (int i = 0; i < numSeries; ++i) {
            TSDBInsert<double> insert("large_batch", "field_" + std::to_string(i));
            for (int j = 0; j < pointsPerSeries; ++j) {
                uint64_t t = static_cast<uint64_t>(j + 1);
                insert.addValue(t, static_cast<double>(i * 100 + j));
            }
            batch.push_back(std::move(insert));
        }

        eng->insertBatch(std::move(batch)).get();

        // Rollover to TSM
        eng->rolloverMemoryStore().get();

        // Verify all series survived the rollover
        for (int i = 0; i < numSeries; ++i) {
            std::string key = "large_batch field_" + std::to_string(i);
            auto resultOpt = eng->query(key, 0, UINT64_MAX).get();
            ASSERT_TRUE(resultOpt.has_value()) << "Series " << key << " not found after rollover";
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), static_cast<size_t>(pointsPerSeries))
                << "Series " << key << " has wrong point count after rollover";
        }
    }).join().get();
}

// ===========================================================================
// 19. Multiple insert types followed by shutdown and recovery
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, MixedTypeRecovery) {
    // Phase 1: Insert mixed types
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            {
                TSDBInsert<double> insert("sensor", "temperature");
                insert.addValue(1000, 25.5);
                insert.addValue(2000, 26.0);
                eng->insert(std::move(insert)).get();
            }
            {
                TSDBInsert<bool> insert("sensor", "active");
                insert.addValue(1000, true);
                insert.addValue(2000, false);
                eng->insert(std::move(insert)).get();
            }
            {
                TSDBInsert<std::string> insert("sensor", "label");
                insert.addValue(1000, std::string("zone-a"));
                insert.addValue(2000, std::string("zone-b"));
                eng->insert(std::move(insert)).get();
            }
        }).join().get();
    }

    // Phase 2: Recover and verify all types
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            {
                auto r = eng->query("sensor temperature", 0, UINT64_MAX).get();
                ASSERT_TRUE(r.has_value());
                auto& qr = std::get<QueryResult<double>>(r.value());
                EXPECT_EQ(qr.values.size(), 2u);
                EXPECT_DOUBLE_EQ(qr.values[0], 25.5);
            }
            {
                auto r = eng->query("sensor active", 0, UINT64_MAX).get();
                ASSERT_TRUE(r.has_value());
                auto& qr = std::get<QueryResult<bool>>(r.value());
                EXPECT_EQ(qr.values.size(), 2u);
                EXPECT_EQ(qr.values[0], true);
            }
            {
                auto r = eng->query("sensor label", 0, UINT64_MAX).get();
                ASSERT_TRUE(r.has_value());
                auto& qr = std::get<QueryResult<std::string>>(r.value());
                EXPECT_EQ(qr.values.size(), 2u);
                EXPECT_EQ(qr.values[0], "zone-a");
            }
        }).join().get();
    }
}

// ===========================================================================
// 20. Insert with tags - WAL file manager routes correctly
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, InsertWithTagsPreservedThroughRollover) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TSDBInsert<double> insert("weather", "temperature");
        insert.addTag("location", "us-west");
        insert.addTag("host", "server-01");
        insert.addValue(1000, 72.5);
        insert.addValue(2000, 73.0);

        std::string seriesKey = insert.seriesKey();
        eng->insert(std::move(insert)).get();

        // Rollover to TSM
        eng->rolloverMemoryStore().get();

        // Query using the same series key with tags
        auto resultOpt = eng->query(seriesKey, 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 2u);
        EXPECT_DOUBLE_EQ(result.values[0], 72.5);
        EXPECT_DOUBLE_EQ(result.values[1], 73.0);
    }).join().get();
}

// ===========================================================================
// 21. Rollover with empty memory store is safe
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, RolloverEmptyMemoryStoreIsSafe) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Rollover without inserting any data should not crash
        EXPECT_NO_THROW(eng->rolloverMemoryStore().get());

        // Engine should still be operational after empty rollover
        TSDBInsert<double> insert("post_empty_rollover", "value");
        insert.addValue(1000, 42.0);
        eng->insert(std::move(insert)).get();

        auto resultOpt = eng->query("post_empty_rollover value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 1u);
    }).join().get();
}

// ===========================================================================
// 22. Batch insert with boolean data type
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, BatchInsertBooleanType) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::vector<TSDBInsert<bool>> batch;
        {
            TSDBInsert<bool> insert("device", "power_on");
            insert.addValue(1000, true);
            insert.addValue(2000, false);
            insert.addValue(3000, true);
            batch.push_back(std::move(insert));
        }
        {
            TSDBInsert<bool> insert("device", "connected");
            insert.addValue(1000, false);
            insert.addValue(2000, true);
            batch.push_back(std::move(insert));
        }

        eng->insertBatch(std::move(batch)).get();

        {
            auto r = eng->query("device power_on", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            auto& qr = std::get<QueryResult<bool>>(r.value());
            EXPECT_EQ(qr.values.size(), 3u);
            EXPECT_EQ(qr.values[0], true);
            EXPECT_EQ(qr.values[1], false);
            EXPECT_EQ(qr.values[2], true);
        }
        {
            auto r = eng->query("device connected", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            auto& qr = std::get<QueryResult<bool>>(r.value());
            EXPECT_EQ(qr.values.size(), 2u);
        }
    }).join().get();
}

// ===========================================================================
// 23. Batch insert with string data type
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, BatchInsertStringType) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::vector<TSDBInsert<std::string>> batch;
        {
            TSDBInsert<std::string> insert("logs", "message");
            insert.addValue(1000, std::string("startup complete"));
            insert.addValue(2000, std::string("request received"));
            insert.addValue(3000, std::string("processing done"));
            batch.push_back(std::move(insert));
        }
        {
            TSDBInsert<std::string> insert("logs", "level");
            insert.addValue(1000, std::string("INFO"));
            insert.addValue(2000, std::string("DEBUG"));
            batch.push_back(std::move(insert));
        }

        eng->insertBatch(std::move(batch)).get();

        {
            auto r = eng->query("logs message", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            auto& qr = std::get<QueryResult<std::string>>(r.value());
            EXPECT_EQ(qr.values.size(), 3u);
            EXPECT_EQ(qr.values[0], "startup complete");
        }
        {
            auto r = eng->query("logs level", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            auto& qr = std::get<QueryResult<std::string>>(r.value());
            EXPECT_EQ(qr.values.size(), 2u);
            EXPECT_EQ(qr.values[0], "INFO");
        }
    }).join().get();
}

// ===========================================================================
// 24. Recovery after insert + rollover + insert + crash cycle
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, RecoveryAfterInsertRolloverInsertCycle) {
    // Phase 1: Insert -> rollover -> insert more -> shutdown
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            // First batch goes to WAL, then to TSM via rollover
            {
                TSDBInsert<double> insert("cycle_test", "series_a");
                insert.addValue(1000, 1.0);
                insert.addValue(2000, 2.0);
                eng->insert(std::move(insert)).get();
            }
            eng->rolloverMemoryStore().get();

            // Second batch stays in WAL (not rolled over before shutdown)
            {
                TSDBInsert<double> insert("cycle_test", "series_b");
                insert.addValue(3000, 3.0);
                insert.addValue(4000, 4.0);
                eng->insert(std::move(insert)).get();
            }

            // Shutdown - second batch WAL remains on disk
        }).join().get();
    }

    // Phase 2: Recovery should bring back both batches
    {
        seastar::thread([] {
            ScopedEngine eng;
            eng.init();

            // First batch was in TSM before shutdown
            {
                auto r = eng->query("cycle_test series_a", 0, UINT64_MAX).get();
                ASSERT_TRUE(r.has_value());
                auto& qr = std::get<QueryResult<double>>(r.value());
                EXPECT_EQ(qr.timestamps.size(), 2u);
                EXPECT_DOUBLE_EQ(qr.values[0], 1.0);
                EXPECT_DOUBLE_EQ(qr.values[1], 2.0);
            }

            // Second batch should be recovered from WAL
            {
                auto r = eng->query("cycle_test series_b", 0, UINT64_MAX).get();
                ASSERT_TRUE(r.has_value());
                auto& qr = std::get<QueryResult<double>>(r.value());
                EXPECT_EQ(qr.timestamps.size(), 2u);
                EXPECT_DOUBLE_EQ(qr.values[0], 3.0);
                EXPECT_DOUBLE_EQ(qr.values[1], 4.0);
            }
        }).join().get();
    }
}

// ===========================================================================
// 25. Sharded engine - WAL file manager per shard operates independently
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, ShardedWALFileManagerOperatesIndependently) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert series that may land on different shards
        for (int i = 0; i < 8; ++i) {
            TSDBInsert<double> insert("sharded_test", "field_" + std::to_string(i));
            insert.addTag("shard_hint", std::to_string(i));
            insert.addValue(1000, static_cast<double>(i * 10));
            insert.addValue(2000, static_cast<double>(i * 10 + 1));
            shardedInsert(eng.eng, std::move(insert));
        }

        // Verify each series is queryable on its correct shard
        int foundCount = 0;
        for (int i = 0; i < 8; ++i) {
            TSDBInsert<double> tmp("sharded_test", "field_" + std::to_string(i));
            tmp.addTag("shard_hint", std::to_string(i));
            std::string seriesKey = tmp.seriesKey();
            SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
            unsigned shard = SeriesId128::Hash{}(sid) % seastar::smp::count;

            auto resultOpt = eng.eng.invoke_on(shard, [seriesKey](Engine& engine) {
                return engine.query(seriesKey, 0, UINT64_MAX);
            }).get();

            if (resultOpt.has_value()) {
                auto& result = std::get<QueryResult<double>>(resultOpt.value());
                EXPECT_EQ(result.timestamps.size(), 2u);
                foundCount++;
            }
        }
        EXPECT_EQ(foundCount, 8);
    }).join().get();
}

// ===========================================================================
// 26. Double rollover without intervening insert
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, DoubleRolloverWithoutInsert) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TSDBInsert<double> insert("metric", "value");
        insert.addValue(1000, 42.0);
        eng->insert(std::move(insert)).get();

        // First rollover
        eng->rolloverMemoryStore().get();

        // Second rollover with no new data (empty memory store)
        EXPECT_NO_THROW(eng->rolloverMemoryStore().get());

        // Data should still be queryable
        auto resultOpt = eng->query("metric value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 1u);
    }).join().get();
}

// ===========================================================================
// 27. Delete then rollover preserves deletion
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, DeleteThenRolloverPreservesDeletion) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TSDBInsert<double> insert("metric", "value");
        insert.addValue(1000, 10.0);
        insert.addValue(2000, 20.0);
        insert.addValue(3000, 30.0);
        eng->insert(std::move(insert)).get();

        // Delete the middle point
        eng->deleteRange("metric value", 2000, 2000).get();

        // Rollover - the memory store (with deletion applied) becomes TSM
        eng->rolloverMemoryStore().get();

        // Query from TSM - deletion should be preserved
        auto resultOpt = eng->query("metric value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 2u);
        EXPECT_DOUBLE_EQ(result.values[0], 10.0);
        EXPECT_DOUBLE_EQ(result.values[1], 30.0);
    }).join().get();
}

// ===========================================================================
// 28. Stress test - many small inserts
// ===========================================================================

TEST_F(WALFileManagerSeastarTest, ManySmallInserts) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        const int numInserts = 200;
        for (int i = 0; i < numInserts; ++i) {
            TSDBInsert<double> insert("stress", "value");
            insert.addValue(static_cast<uint64_t>(i + 1), static_cast<double>(i));
            eng->insert(std::move(insert)).get();
        }

        auto resultOpt = eng->query("stress value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), static_cast<size_t>(numInserts));
    }).join().get();
}
