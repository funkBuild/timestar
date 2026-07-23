#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/wal.hpp"
#include "../../../lib/storage/wal_file_manager.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

namespace fs = std::filesystem;

static void cleanup_test_data() {
    // Clean up shard directories
    for (int i = 0; i < 32; ++i) {
        std::string shardDir = "shard_" + std::to_string(i);
        if (fs::exists(shardDir)) {
            fs::remove_all(shardDir);
        }
    }
}

// Helper to check if a VariantQueryResult optional has data
static bool variantHasData(const std::optional<VariantQueryResult>& result) {
    if (!result.has_value())
        return false;
    return std::visit([](const auto& r) -> bool { return !r.timestamps.empty(); }, *result);
}

// Safe query wrapper that handles "Series not found" exceptions
// by returning std::nullopt instead of propagating the exception.
// This is needed because QueryRunner::runQuery throws on missing series
// while Engine::query's signature promises std::optional<VariantQueryResult>.
static seastar::future<std::optional<VariantQueryResult>> safeQuery(Engine& engine, const std::string& seriesKey,
                                                                    uint64_t startTime, uint64_t endTime) {
    try {
        co_return co_await engine.query(seriesKey, startTime, endTime);
    } catch (const std::runtime_error& e) {
        if (std::string(e.what()).find("Series not found") != std::string::npos) {
            co_return std::nullopt;
        }
        throw;
    }
}

seastar::future<> test_partial_field_deletion_does_not_corrupt_other_fields() {
    Engine engine;
    std::exception_ptr ex;

    try {
        co_await engine.init();

        uint64_t timestamp = 1704067700000000000ULL;

        // Insert fieldA
        TimeStarInsert<double> insertA("simple", "fieldA");
        insertA.addTag("id", "test1");
        insertA.addValue(timestamp, 100.0);

        // Insert fieldB
        TimeStarInsert<double> insertB("simple", "fieldB");
        insertB.addTag("id", "test1");
        insertB.addValue(timestamp, 200.0);

        co_await engine.insert(insertA);
        co_await engine.insert(insertB);

        // Give time for background tasks to process
        co_await seastar::sleep(std::chrono::milliseconds(100));

        // Verify both fields can be queried before deletion
        auto resultA_before = co_await safeQuery(engine, insertA.seriesKey(), timestamp, timestamp);
        auto resultB_before = co_await safeQuery(engine, insertB.seriesKey(), timestamp, timestamp);

        EXPECT_TRUE(variantHasData(resultA_before)) << "fieldA should have data before deletion";
        EXPECT_TRUE(variantHasData(resultB_before)) << "fieldB should have data before deletion";

        // Now delete ONLY fieldA
        bool deleted = co_await engine.deleteRange(insertA.seriesKey(), timestamp, timestamp);
        EXPECT_TRUE(deleted) << "deleteRange should return true indicating data was deleted";

        // Give time for deletion to be processed
        co_await seastar::sleep(std::chrono::milliseconds(100));

        // Query both fields after deletion
        auto resultA_after = co_await safeQuery(engine, insertA.seriesKey(), timestamp, timestamp);
        auto resultB_after = co_await safeQuery(engine, insertB.seriesKey(), timestamp, timestamp);

        // The key assertions:
        EXPECT_FALSE(variantHasData(resultA_after)) << "fieldA should NOT have data after deletion";
        EXPECT_TRUE(variantHasData(resultB_after)) << "fieldB should STILL have data after deleting fieldA";
        EXPECT_NE(insertA.seriesKey(), insertB.seriesKey()) << "fieldA and fieldB should have different series keys";
    } catch (...) {
        ex = std::current_exception();
    }

    // Always stop the engine to properly close WAL output streams
    co_await engine.stop();

    if (ex) {
        std::rethrow_exception(ex);
    }
}

seastar::future<> test_wal_replay_preserves_partial_field_deletion() {
    uint64_t timestamp = 1704067700000000000ULL;

    // First phase: Insert data and delete fieldA
    {
        Engine engine;
        std::exception_ptr ex;

        try {
            co_await engine.init();

            TimeStarInsert<double> insertA("simple", "fieldA");
            insertA.addTag("id", "test1");
            insertA.addValue(timestamp, 100.0);

            TimeStarInsert<double> insertB("simple", "fieldB");
            insertB.addTag("id", "test1");
            insertB.addValue(timestamp, 200.0);

            co_await engine.insert(insertA);
            co_await engine.insert(insertB);

            // Delete fieldA
            co_await engine.deleteRange(insertA.seriesKey(), timestamp, timestamp);
        } catch (...) {
            ex = std::current_exception();
        }

        co_await engine.stop();

        if (ex) {
            std::rethrow_exception(ex);
        }
    }

    // Second phase: Restart engine and verify WAL replay behavior
    {
        Engine engine2;
        std::exception_ptr ex;

        try {
            co_await engine2.init();  // This should replay the WAL

            TimeStarInsert<double> insertA("simple", "fieldA");
            insertA.addTag("id", "test1");

            TimeStarInsert<double> insertB("simple", "fieldB");
            insertB.addTag("id", "test1");

            auto resultA = co_await safeQuery(engine2, insertA.seriesKey(), timestamp, timestamp + 1);
            auto resultB = co_await safeQuery(engine2, insertB.seriesKey(), timestamp, timestamp + 1);

            // After WAL replay, fieldA should still be deleted, fieldB should still exist
            EXPECT_FALSE(variantHasData(resultA)) << "After WAL replay, fieldA should still be deleted";
            EXPECT_TRUE(variantHasData(resultB)) << "After WAL replay, fieldB should still exist";
        } catch (...) {
            ex = std::current_exception();
        }

        co_await engine2.stop();

        if (ex) {
            std::rethrow_exception(ex);
        }
    }
}

class WALDeletionTest : public ::testing::Test {
protected:
    void SetUp() override { cleanup_test_data(); }
    void TearDown() override { cleanup_test_data(); }
};

SEASTAR_TEST_F(WALDeletionTest, PartialFieldDeletion) {
    co_await test_partial_field_deletion_does_not_corrupt_other_fields();
}

SEASTAR_TEST_F(WALDeletionTest, WALReplayPreservesPartialFieldDeletion) {
    co_await test_wal_replay_preserves_partial_field_deletion();
}
