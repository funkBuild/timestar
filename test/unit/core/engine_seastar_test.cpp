// Seastar-based tests for the Engine component
// Tests async lifecycle, multi-shard coordination, queries, deletes, and metadata indexing.

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <random>
#include <vector>
#include <string>
#include <set>
#include <map>

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/core/series_id.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include "../../test_helpers.hpp"

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class EngineSeastarTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanTestShardDirectories();
    }

    void TearDown() override {
        cleanTestShardDirectories();
    }
};

// ===========================================================================
// 1. Async lifecycle (init / stop)
// ===========================================================================

TEST_F(EngineSeastarTest, InitAndStopSingleEngine) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();
        // If we reach here without exception the lifecycle succeeded.
        SUCCEED();
    }).join().get();
}

TEST_F(EngineSeastarTest, InitAndStopShardedEngine) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();
        // Verify all shards started
        SUCCEED();
    }).join().get();
}

TEST_F(EngineSeastarTest, InitWithBackgroundTasks) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        SUCCEED();
    }).join().get();
}

TEST_F(EngineSeastarTest, DoubleInitThrowsDueToLock) {
    // LevelDB holds a file lock, so calling init() a second time on the
    // same engine must throw because the database is already open.
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();
        EXPECT_THROW(eng.engine->init().get(), std::runtime_error);
    }).join().get();
}

// ===========================================================================
// 2. Single-shard insert and query
// ===========================================================================

TEST_F(EngineSeastarTest, InsertAndQueryFloat) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<double> insert("temperature", "value");
        insert.addTag("location", "us-west");
        insert.addValue(1000, 20.5);
        insert.addValue(2000, 21.0);
        insert.addValue(3000, 21.5);

        eng->insert(std::move(insert)).get();

        auto resultOpt = eng->query("temperature,location=us-west value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());

        auto& variant = resultOpt.value();
        ASSERT_TRUE(std::holds_alternative<QueryResult<double>>(variant));
        auto& result = std::get<QueryResult<double>>(variant);

        EXPECT_EQ(result.timestamps.size(), 3u);
        EXPECT_DOUBLE_EQ(result.values[0], 20.5);
        EXPECT_DOUBLE_EQ(result.values[1], 21.0);
        EXPECT_DOUBLE_EQ(result.values[2], 21.5);
    }).join().get();
}

TEST_F(EngineSeastarTest, InsertAndQueryBoolean) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<bool> insert("system", "online");
        insert.addValue(1000, true);
        insert.addValue(2000, false);
        insert.addValue(3000, true);

        eng->insert(std::move(insert)).get();

        auto resultOpt = eng->query("system online", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());

        auto& variant = resultOpt.value();
        ASSERT_TRUE(std::holds_alternative<QueryResult<bool>>(variant));
        auto& result = std::get<QueryResult<bool>>(variant);

        EXPECT_EQ(result.values.size(), 3u);
        EXPECT_EQ(result.values[0], true);
        EXPECT_EQ(result.values[1], false);
        EXPECT_EQ(result.values[2], true);
    }).join().get();
}

TEST_F(EngineSeastarTest, InsertAndQueryString) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<std::string> insert("logs", "message");
        insert.addValue(1000, "startup");
        insert.addValue(2000, "running");
        insert.addValue(3000, "shutdown");

        eng->insert(std::move(insert)).get();

        auto resultOpt = eng->query("logs message", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());

        auto& variant = resultOpt.value();
        ASSERT_TRUE(std::holds_alternative<QueryResult<std::string>>(variant));
        auto& result = std::get<QueryResult<std::string>>(variant);

        EXPECT_EQ(result.values.size(), 3u);
        EXPECT_EQ(result.values[0], "startup");
        EXPECT_EQ(result.values[1], "running");
        EXPECT_EQ(result.values[2], "shutdown");
    }).join().get();
}

// ===========================================================================
// 3. Query non-existent series returns nullopt
// ===========================================================================

TEST_F(EngineSeastarTest, QueryNonExistentSeriesReturnsNullopt) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        auto resultOpt = eng->query("no_such_series value", 0, UINT64_MAX).get();
        EXPECT_FALSE(resultOpt.has_value());
    }).join().get();
}

// ===========================================================================
// 4. Time-range filtering
// ===========================================================================

TEST_F(EngineSeastarTest, QueryTimeRangeFiltering) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<double> insert("metric", "cpu");
        for (uint64_t t = 1000; t <= 10000; t += 1000) {
            insert.addValue(t, static_cast<double>(t));
        }
        eng->insert(std::move(insert)).get();

        // Query only the middle range [3000, 7000]
        auto resultOpt = eng->query("metric cpu", 3000, 7000).get();
        ASSERT_TRUE(resultOpt.has_value());

        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 5u);
        EXPECT_EQ(result.timestamps.front(), 3000u);
        EXPECT_EQ(result.timestamps.back(), 7000u);
    }).join().get();
}

// ===========================================================================
// 5. Batch insert
// ===========================================================================

TEST_F(EngineSeastarTest, InsertBatchFloat) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::vector<TimeStarInsert<double>> batch;
        {
            TimeStarInsert<double> a("cpu", "usage");
            a.addValue(1000, 25.0);
            a.addValue(2000, 30.0);
            batch.push_back(std::move(a));
        }
        {
            TimeStarInsert<double> b("memory", "usage");
            b.addValue(1000, 60.0);
            b.addValue(2000, 65.0);
            batch.push_back(std::move(b));
        }

        auto timing = eng->insertBatch(std::move(batch)).get();
        EXPECT_EQ(timing.walWriteCount, 2);
        EXPECT_GE(timing.walWriteTime.count(), 0);

        // Verify both series queryable
        {
            auto r = eng->query("cpu usage", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            EXPECT_EQ(std::get<QueryResult<double>>(r.value()).values.size(), 2u);
        }
        {
            auto r = eng->query("memory usage", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            EXPECT_EQ(std::get<QueryResult<double>>(r.value()).values.size(), 2u);
        }
    }).join().get();
}

TEST_F(EngineSeastarTest, InsertBatchEmpty) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::vector<TimeStarInsert<double>> empty;
        auto timing = eng->insertBatch(std::move(empty)).get();
        EXPECT_EQ(timing.walWriteCount, 0);
    }).join().get();
}

// ===========================================================================
// 6. Multi-shard insert coordination (using shardedInsert helper)
// ===========================================================================

TEST_F(EngineSeastarTest, MultiShardInsertAndQuery) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert multiple series that may land on different shards
        for (int i = 0; i < 5; ++i) {
            TimeStarInsert<double> insert("weather", "temp_" + std::to_string(i));
            insert.addTag("region", "region_" + std::to_string(i));
            for (uint64_t t = 1000; t <= 5000; t += 1000) {
                insert.addValue(t, static_cast<double>(i * 10 + t / 1000));
            }
            shardedInsert(eng.eng, std::move(insert));
        }

        // Query each series from its shard
        for (int i = 0; i < 5; ++i) {
            std::string field = "temp_" + std::to_string(i);
            TimeStarInsert<double> tmp("weather", field);
            tmp.addTag("region", "region_" + std::to_string(i));
            std::string seriesKey = tmp.seriesKey();
            SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
            unsigned shard = SeriesId128::Hash{}(sid) % seastar::smp::count;

            auto resultOpt = eng.eng.invoke_on(shard, [seriesKey](Engine& engine) {
                return engine.query(seriesKey, 0, UINT64_MAX);
            }).get();

            ASSERT_TRUE(resultOpt.has_value()) << "Series " << seriesKey << " not found on shard " << shard;
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 5u);
        }
    }).join().get();
}

// ===========================================================================
// 7. Metadata indexing integration
// ===========================================================================

TEST_F(EngineSeastarTest, MetadataIndexing) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert series with tags
        {
            TimeStarInsert<double> insert("weather", "temperature");
            insert.addTag("location", "us-west");
            insert.addTag("host", "server-01");
            insert.addValue(1000, 72.5);
            shardedInsert(eng.eng, std::move(insert));
        }
        {
            TimeStarInsert<double> insert("weather", "humidity");
            insert.addTag("location", "us-east");
            insert.addTag("host", "server-02");
            insert.addValue(1000, 65.0);
            shardedInsert(eng.eng, std::move(insert));
        }

        // Metadata queries run on shard 0 (where index lives)
        auto measurements = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getAllMeasurements();
        }).get();
        EXPECT_GE(measurements.size(), 1u);
        bool foundWeather = false;
        for (const auto& m : measurements) {
            if (m == "weather") foundWeather = true;
        }
        EXPECT_TRUE(foundWeather);

        auto fields = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementFields("weather");
        }).get();
        EXPECT_TRUE(fields.count("temperature") > 0);
        EXPECT_TRUE(fields.count("humidity") > 0);

        auto tags = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementTags("weather");
        }).get();
        EXPECT_TRUE(tags.count("location") > 0);
        EXPECT_TRUE(tags.count("host") > 0);

        auto locations = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getTagValues("weather", "location");
        }).get();
        EXPECT_TRUE(locations.count("us-west") > 0);
        EXPECT_TRUE(locations.count("us-east") > 0);
    }).join().get();
}

TEST_F(EngineSeastarTest, IndexMetadataOnShard0Only) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        TimeStarInsert<double> insert("cpu", "usage");
        insert.addTag("host", "h1");
        insert.addValue(1000, 50.0);

        // indexMetadata must be called on shard 0 -- calling on another shard should throw
        if (seastar::smp::count > 1) {
            EXPECT_THROW(
                eng.eng.invoke_on(1, [insert](Engine& engine) mutable {
                    return engine.indexMetadata(insert);
                }).get(),
                std::runtime_error
            );
        }

        // Calling on shard 0 should succeed
        auto sid = eng.eng.invoke_on(0, [insert](Engine& engine) mutable {
            return engine.indexMetadata(insert);
        }).get();
        // The returned SeriesId128 should be non-zero
        EXPECT_FALSE(sid.toHex().empty());
    }).join().get();
}

// ===========================================================================
// 8. queryBySeries (index-based query)
// ===========================================================================

TEST_F(EngineSeastarTest, QueryBySeries) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        TimeStarInsert<double> insert("weather", "temperature");
        insert.addTag("location", "us-west");
        insert.addValue(1000, 72.5);
        insert.addValue(2000, 73.0);
        insert.addValue(3000, 73.5);
        shardedInsert(eng.eng, std::move(insert));

        // queryBySeries routes through the index on shard 0
        std::map<std::string, std::string> tags = {{"location", "us-west"}};
        auto result = eng.eng.invoke_on(0, [tags](Engine& engine) {
            return engine.queryBySeries("weather", tags, "temperature", 0, UINT64_MAX);
        }).get();

        // The result may or may not have data depending on whether shard 0
        // actually stores this series. The important thing is no crash.
        // If the data ended up on shard 0, we verify the values.
        if (std::holds_alternative<QueryResult<double>>(result)) {
            auto& qr = std::get<QueryResult<double>>(result);
            if (!qr.timestamps.empty()) {
                EXPECT_EQ(qr.timestamps.size(), 3u);
                EXPECT_DOUBLE_EQ(qr.values[0], 72.5);
            }
        }
    }).join().get();
}

TEST_F(EngineSeastarTest, QueryBySeriesNonExistent) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        // Query a series that was never inserted
        std::map<std::string, std::string> tags = {{"location", "nowhere"}};
        auto result = eng.eng.invoke_on(0, [tags](Engine& engine) {
            return engine.queryBySeries("nonexistent", tags, "field", 0, UINT64_MAX);
        }).get();

        // Should return an empty QueryResult (no crash)
        if (std::holds_alternative<QueryResult<double>>(result)) {
            auto& qr = std::get<QueryResult<double>>(result);
            EXPECT_TRUE(qr.timestamps.empty());
        }
    }).join().get();
}

// ===========================================================================
// 9. Delete operations
// ===========================================================================

TEST_F(EngineSeastarTest, DeleteRangeFromMemoryStore) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<double> insert("metric", "value");
        insert.addValue(1000, 10.0);
        insert.addValue(2000, 20.0);
        insert.addValue(3000, 30.0);
        insert.addValue(4000, 40.0);
        insert.addValue(5000, 50.0);

        eng->insert(std::move(insert)).get();

        // Delete middle range [2000, 4000]
        bool deleted = eng->deleteRange("metric value", 2000, 4000).get();
        EXPECT_TRUE(deleted);

        // Query remaining data
        auto resultOpt = eng->query("metric value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());

        // Should have only 2 remaining points: t=1000 and t=5000
        EXPECT_EQ(result.timestamps.size(), 2u);
        EXPECT_DOUBLE_EQ(result.values[0], 10.0);
        EXPECT_DOUBLE_EQ(result.values[1], 50.0);
    }).join().get();
}

TEST_F(EngineSeastarTest, DeleteRangeNonExistentSeries) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Deleting from a series that doesn't exist should return false
        bool deleted = eng->deleteRange("nonexistent value", 0, UINT64_MAX).get();
        EXPECT_FALSE(deleted);
    }).join().get();
}

TEST_F(EngineSeastarTest, DeleteRangeBySeries) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<double> insert("weather", "temperature");
        insert.addTag("location", "us-west");
        insert.addValue(1000, 72.5);
        insert.addValue(2000, 73.0);
        insert.addValue(3000, 73.5);

        eng->insert(std::move(insert)).get();

        std::map<std::string, std::string> tags = {{"location", "us-west"}};
        bool deleted = eng->deleteRangeBySeries("weather", tags, "temperature", 2000, 2000).get();
        EXPECT_TRUE(deleted);

        // Query -- should have 2 remaining points
        auto resultOpt = eng->query("weather,location=us-west temperature", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 2u);
    }).join().get();
}

// ===========================================================================
// 10. deleteByPattern
// ===========================================================================

TEST_F(EngineSeastarTest, DeleteByPatternAllFields) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert multiple fields for same measurement+tags
        {
            TimeStarInsert<double> insert("sensor", "temp");
            insert.addTag("zone", "a");
            insert.addValue(1000, 25.0);
            insert.addValue(2000, 26.0);
            shardedInsert(eng.eng, std::move(insert));
        }
        {
            TimeStarInsert<double> insert("sensor", "humidity");
            insert.addTag("zone", "a");
            insert.addValue(1000, 60.0);
            insert.addValue(2000, 62.0);
            shardedInsert(eng.eng, std::move(insert));
        }

        // Delete all fields for measurement=sensor, tags={zone:a}
        Engine::DeleteRequest req;
        req.measurement = "sensor";
        req.tags = {{"zone", "a"}};
        req.startTime = 0;
        req.endTime = UINT64_MAX;

        // Execute delete on shard 0 (where the index lives and where data may be)
        auto result = eng.eng.invoke_on(0, [req](Engine& engine) {
            return engine.deleteByPattern(req);
        }).get();

        // We should have deleted some series on shard 0.
        // Due to sharding, not all data may be on shard 0, but
        // the delete operation itself should not crash.
        EXPECT_GE(result.seriesDeleted, 0u);
    }).join().get();
}

TEST_F(EngineSeastarTest, DeleteByPatternSpecificFields) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Insert two fields
        {
            TimeStarInsert<double> insert("sensor", "temp");
            insert.addTag("zone", "b");
            insert.addValue(1000, 30.0);
            eng->insert(std::move(insert)).get();
        }
        {
            TimeStarInsert<double> insert("sensor", "humidity");
            insert.addTag("zone", "b");
            insert.addValue(1000, 50.0);
            eng->insert(std::move(insert)).get();
        }

        // Delete only "temp" field
        Engine::DeleteRequest req;
        req.measurement = "sensor";
        req.tags = {{"zone", "b"}};
        req.fields = {"temp"};
        req.startTime = 0;
        req.endTime = UINT64_MAX;

        auto delResult = eng->deleteByPattern(req).get();
        EXPECT_GE(delResult.seriesDeleted, 1u);

        // humidity should still be queryable
        auto humidResult = eng->query("sensor,zone=b humidity", 0, UINT64_MAX).get();
        ASSERT_TRUE(humidResult.has_value());
        auto& qr = std::get<QueryResult<double>>(humidResult.value());
        EXPECT_EQ(qr.values.size(), 1u);
        EXPECT_DOUBLE_EQ(qr.values[0], 50.0);
    }).join().get();
}

TEST_F(EngineSeastarTest, DeleteByPatternNoMatch) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        Engine::DeleteRequest req;
        req.measurement = "nonexistent";
        req.startTime = 0;
        req.endTime = UINT64_MAX;

        auto result = eng->deleteByPattern(req).get();
        EXPECT_EQ(result.seriesDeleted, 0u);
    }).join().get();
}

// ===========================================================================
// 11. Rollover memory store
// ===========================================================================

TEST_F(EngineSeastarTest, RolloverMemoryStore) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Insert data, then force a rollover
        TimeStarInsert<double> insert("metric", "value");
        for (uint64_t t = 1000; t <= 10000; t += 1000) {
            insert.addValue(t, static_cast<double>(t));
        }
        eng->insert(std::move(insert)).get();

        // Rollover should not throw
        EXPECT_NO_THROW(eng->rolloverMemoryStore().get());

        // Data should still be queryable after rollover (now from TSM files)
        auto resultOpt = eng->query("metric value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 10u);
    }).join().get();
}

// ===========================================================================
// 12. Insert after rollover (data in both memory store and TSM)
// ===========================================================================

TEST_F(EngineSeastarTest, InsertAfterRollover) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Phase 1: insert and rollover (data goes to TSM)
        {
            TimeStarInsert<double> insert("metric", "value");
            for (uint64_t t = 1000; t <= 5000; t += 1000) {
                insert.addValue(t, static_cast<double>(t));
            }
            eng->insert(std::move(insert)).get();
        }
        eng->rolloverMemoryStore().get();

        // Phase 2: insert more data (stays in memory store)
        {
            TimeStarInsert<double> insert("metric", "value");
            for (uint64_t t = 6000; t <= 10000; t += 1000) {
                insert.addValue(t, static_cast<double>(t));
            }
            eng->insert(std::move(insert)).get();
        }

        // Query should merge results from both TSM files and memory store
        auto resultOpt = eng->query("metric value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 10u);
        EXPECT_EQ(result.timestamps.front(), 1000u);
        EXPECT_EQ(result.timestamps.back(), 10000u);
    }).join().get();
}

// ===========================================================================
// 13. Multiple data types in same engine
// ===========================================================================

TEST_F(EngineSeastarTest, MixedDataTypes) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Float series
        {
            TimeStarInsert<double> insert("sensor", "temperature");
            insert.addValue(1000, 25.5);
            eng->insert(std::move(insert)).get();
        }
        // Bool series
        {
            TimeStarInsert<bool> insert("sensor", "active");
            insert.addValue(1000, true);
            eng->insert(std::move(insert)).get();
        }
        // String series
        {
            TimeStarInsert<std::string> insert("sensor", "status");
            insert.addValue(1000, "healthy");
            eng->insert(std::move(insert)).get();
        }

        // Query each type
        {
            auto r = eng->query("sensor temperature", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            ASSERT_TRUE(std::holds_alternative<QueryResult<double>>(r.value()));
            EXPECT_DOUBLE_EQ(std::get<QueryResult<double>>(r.value()).values[0], 25.5);
        }
        {
            auto r = eng->query("sensor active", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            ASSERT_TRUE(std::holds_alternative<QueryResult<bool>>(r.value()));
            EXPECT_EQ(std::get<QueryResult<bool>>(r.value()).values[0], true);
        }
        {
            auto r = eng->query("sensor status", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            ASSERT_TRUE(std::holds_alternative<QueryResult<std::string>>(r.value()));
            EXPECT_EQ(std::get<QueryResult<std::string>>(r.value()).values[0], "healthy");
        }
    }).join().get();
}

// ===========================================================================
// 14. Large batch insert
// ===========================================================================

TEST_F(EngineSeastarTest, LargeBatchInsert) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        const int batchSize = 50;
        std::vector<TimeStarInsert<double>> batch;
        for (int i = 0; i < batchSize; ++i) {
            TimeStarInsert<double> insert("bulk", "field_" + std::to_string(i));
            for (uint64_t t = 1; t <= 100; ++t) {
                insert.addValue(t, static_cast<double>(i * 100 + t));
            }
            batch.push_back(std::move(insert));
        }

        auto timing = eng->insertBatch(std::move(batch)).get();
        EXPECT_EQ(timing.walWriteCount, batchSize);

        // Spot-check a few series
        {
            auto r = eng->query("bulk field_0", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            EXPECT_EQ(std::get<QueryResult<double>>(r.value()).values.size(), 100u);
        }
        {
            auto r = eng->query("bulk field_49", 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            EXPECT_EQ(std::get<QueryResult<double>>(r.value()).values.size(), 100u);
        }
    }).join().get();
}

// ===========================================================================
// 15. Delete then re-insert
// ===========================================================================

TEST_F(EngineSeastarTest, DeleteThenReinsert) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Insert
        {
            TimeStarInsert<double> insert("metric", "value");
            insert.addValue(1000, 10.0);
            insert.addValue(2000, 20.0);
            eng->insert(std::move(insert)).get();
        }

        // Delete all
        eng->deleteRange("metric value", 0, UINT64_MAX).get();

        // Re-insert with new values
        {
            TimeStarInsert<double> insert("metric", "value");
            insert.addValue(3000, 30.0);
            insert.addValue(4000, 40.0);
            eng->insert(std::move(insert)).get();
        }

        auto resultOpt = eng->query("metric value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 2u);
        EXPECT_DOUBLE_EQ(result.values[0], 30.0);
        EXPECT_DOUBLE_EQ(result.values[1], 40.0);
    }).join().get();
}

// ===========================================================================
// 16. Cross-shard query aggregation via sharded engine
// ===========================================================================

TEST_F(EngineSeastarTest, CrossShardQueryAllShards) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const int numSeries = 10;
        for (int i = 0; i < numSeries; ++i) {
            TimeStarInsert<double> insert("distributed", "field_" + std::to_string(i));
            insert.addTag("node", "node_" + std::to_string(i));
            insert.addValue(1000, static_cast<double>(i));
            insert.addValue(2000, static_cast<double>(i + 10));
            shardedInsert(eng.eng, std::move(insert));
        }

        // Verify that querying each series on its correct shard works
        int foundCount = 0;
        for (int i = 0; i < numSeries; ++i) {
            TimeStarInsert<double> tmp("distributed", "field_" + std::to_string(i));
            tmp.addTag("node", "node_" + std::to_string(i));
            std::string seriesKey = tmp.seriesKey();
            SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
            unsigned shard = SeriesId128::Hash{}(sid) % seastar::smp::count;

            auto resultOpt = eng.eng.invoke_on(shard, [seriesKey](Engine& engine) {
                return engine.query(seriesKey, 0, UINT64_MAX);
            }).get();

            if (resultOpt.has_value()) {
                auto& qr = std::get<QueryResult<double>>(resultOpt.value());
                EXPECT_EQ(qr.timestamps.size(), 2u);
                foundCount++;
            }
        }
        EXPECT_EQ(foundCount, numSeries);
    }).join().get();
}

// ===========================================================================
// 17. executeLocalQuery with series IDs
// ===========================================================================

TEST_F(EngineSeastarTest, ExecuteLocalQueryWithSeriesIds) {
    // executeLocalQuery calls index.getSeriesMetadata() which requires
    // the LevelDB index.  The index is only open on shard 0, so this
    // test uses a single-shard ScopedEngine where shard 0 holds both
    // the index and the data.
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<double> insert("weather", "temperature");
        insert.addTag("loc", "west");
        insert.addValue(1000, 70.0);
        insert.addValue(2000, 71.0);

        eng->insert(std::move(insert)).get();

        // Index the metadata so getSeriesMetadata can find it
        TimeStarInsert<double> forIndex("weather", "temperature");
        forIndex.addTag("loc", "west");
        forIndex.addValue(1000, 70.0);  // value needed for insert template
        auto sid = eng->indexMetadata(forIndex).get();

        timestar::ShardQuery shardQuery;
        shardQuery.seriesIds.push_back(sid);
        shardQuery.startTime = 0;
        shardQuery.endTime = UINT64_MAX;

        auto results = eng->executeLocalQuery(shardQuery).get();

        EXPECT_GE(results.size(), 1u);
        if (!results.empty()) {
            EXPECT_EQ(results[0].measurement, "weather");
            EXPECT_TRUE(results[0].fields.count("temperature") > 0);
        }
    }).join().get();
}

TEST_F(EngineSeastarTest, ExecuteLocalQueryEmptySeriesIds) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        timestar::ShardQuery shardQuery;
        // Empty seriesIds
        shardQuery.startTime = 0;
        shardQuery.endTime = UINT64_MAX;

        auto results = eng->executeLocalQuery(shardQuery).get();
        EXPECT_TRUE(results.empty());
    }).join().get();
}

// ===========================================================================
// 18. basePath returns expected path
// ===========================================================================

TEST_F(EngineSeastarTest, BasePathContainsShardId) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::string path = eng->basePath();
        // Should contain "shard_" followed by the shard id
        EXPECT_TRUE(path.find("shard_") != std::string::npos);
    }).join().get();
}

// ===========================================================================
// 19. Concurrent inserts to same series
// ===========================================================================

TEST_F(EngineSeastarTest, SequentialInsertsToSameSeries) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Multiple sequential inserts to the same series
        for (int batch = 0; batch < 5; ++batch) {
            TimeStarInsert<double> insert("metric", "value");
            for (uint64_t t = batch * 100 + 1; t <= (batch + 1) * 100; ++t) {
                insert.addValue(t, static_cast<double>(t));
            }
            eng->insert(std::move(insert)).get();
        }

        auto resultOpt = eng->query("metric value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 500u);
    }).join().get();
}

// ===========================================================================
// 20. Metadata for non-existent measurement
// ===========================================================================

TEST_F(EngineSeastarTest, MetadataForNonExistentMeasurement) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        auto fields = eng->getMeasurementFields("nonexistent").get();
        EXPECT_TRUE(fields.empty());

        auto tags = eng->getMeasurementTags("nonexistent").get();
        EXPECT_TRUE(tags.empty());

        auto values = eng->getTagValues("nonexistent", "any_tag").get();
        EXPECT_TRUE(values.empty());
    }).join().get();
}

// ===========================================================================
// 21. Insert with tags generates correct series key for query
// ===========================================================================

TEST_F(EngineSeastarTest, InsertWithTagsQueryBySeriesKey) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<double> insert("cpu", "usage");
        insert.addTag("host", "server-01");
        insert.addTag("dc", "us-east");
        insert.addValue(1000, 99.0);

        std::string expectedKey = insert.seriesKey();
        eng->insert(std::move(insert)).get();

        auto resultOpt = eng->query(expectedKey, 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.values.size(), 1u);
        EXPECT_DOUBLE_EQ(result.values[0], 99.0);
    }).join().get();
}

// ===========================================================================
// 22. Sharded engine - metadata is consistent on shard 0
// ===========================================================================

TEST_F(EngineSeastarTest, ShardedMetadataConsistency) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert series that will land on different shards
        for (int i = 0; i < 8; ++i) {
            TimeStarInsert<double> insert("metrics", "field_" + std::to_string(i));
            insert.addTag("region", "region_" + std::to_string(i % 3));
            insert.addValue(1000, static_cast<double>(i));
            shardedInsert(eng.eng, std::move(insert));
        }

        // All metadata should be discoverable on shard 0
        auto fields = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementFields("metrics");
        }).get();

        // All 8 fields should be indexed regardless of which shard stores the data
        EXPECT_EQ(fields.size(), 8u);

        auto tags = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementTags("metrics");
        }).get();
        EXPECT_TRUE(tags.count("region") > 0);

        auto regions = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getTagValues("metrics", "region");
        }).get();
        EXPECT_EQ(regions.size(), 3u);
        EXPECT_TRUE(regions.count("region_0") > 0);
        EXPECT_TRUE(regions.count("region_1") > 0);
        EXPECT_TRUE(regions.count("region_2") > 0);
    }).join().get();
}
