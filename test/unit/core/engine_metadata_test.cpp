// Tests for Engine cross-shard metadata indexing fix.
// Validates that Engine::insert() correctly forwards metadata to shard 0
// even when called on non-zero shards, by using setShardedRef().

#include <gtest/gtest.h>
#include <filesystem>
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
#include <seastar/util/defer.hh>

#include "../../test_helpers.hpp"

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class EngineMetadataTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanTestShardDirectories();
    }

    void TearDown() override {
        cleanTestShardDirectories();
    }
};

// ===========================================================================
// 1. setShardedRef compiles and can be called
// ===========================================================================

TEST_F(EngineMetadataTest, SetShardedRefCompiles) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        // setShardedRef should be callable on all shards
        eng.eng.invoke_on_all([&eng](Engine& engine) {
            engine.setShardedRef(&eng.eng);
            return seastar::make_ready_future<>();
        }).get();

        SUCCEED();
    }).join().get();
}

// ===========================================================================
// 2. On shard 0, insert still indexes metadata (existing behavior preserved)
// ===========================================================================

TEST_F(EngineMetadataTest, Shard0InsertStillIndexesMetadata) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        // Set the sharded reference
        eng.eng.invoke_on_all([&eng](Engine& engine) {
            engine.setShardedRef(&eng.eng);
            return seastar::make_ready_future<>();
        }).get();

        // Insert directly on shard 0
        TimeStarInsert<double> insert("temperature", "value");
        insert.addTag("location", "us-west");
        insert.addValue(1000, 72.5);
        insert.addValue(2000, 73.0);

        eng.eng.invoke_on(0, [insert](Engine& engine) mutable {
            return engine.insert(std::move(insert));
        }).get();

        // Verify metadata is indexed on shard 0
        auto fields = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementFields("temperature");
        }).get();
        EXPECT_TRUE(fields.count("value") > 0) << "Metadata should be indexed when insert is on shard 0";

        auto tags = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementTags("temperature");
        }).get();
        EXPECT_TRUE(tags.count("location") > 0) << "Tags should be indexed when insert is on shard 0";

        auto locations = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getTagValues("temperature", "location");
        }).get();
        EXPECT_TRUE(locations.count("us-west") > 0) << "Tag values should be indexed when insert is on shard 0";
    }).join().get();
}

// ===========================================================================
// 3. Without shardedRef set, non-zero shard insert does NOT index metadata
//    (graceful degradation, no crash)
// ===========================================================================

TEST_F(EngineMetadataTest, NoShardedRefNoCrash) {
    if (seastar::smp::count < 2) {
        GTEST_SKIP() << "Need at least 2 shards for this test";
    }

    seastar::thread([] {
        // Manually start without setting shardedRef to test graceful degradation.
        // Cannot use ScopedShardedEngine::start() since it now sets the ref automatically.
        seastar::sharded<Engine> rawEng;
        rawEng.start().get();
        rawEng.invoke_on_all([](Engine& engine) {
            return engine.init();
        }).get();

        auto cleanup = seastar::defer([&rawEng] {
            rawEng.invoke_on_all([](Engine& engine) {
                return engine.stop();
            }).get();
            rawEng.stop().get();
        });

        // Do NOT set shardedRef -- simulates legacy behavior

        TimeStarInsert<double> insert("cpu", "usage");
        insert.addTag("host", "h1");
        insert.addValue(1000, 50.0);

        // Insert on shard 1 -- should not crash even without shardedRef
        rawEng.invoke_on(1, [insert](Engine& engine) mutable {
            return engine.insert(std::move(insert));
        }).get();

        // Metadata will NOT be indexed (this was the original bug)
        auto fields = rawEng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementFields("cpu");
        }).get();
        EXPECT_TRUE(fields.empty()) << "Without shardedRef, non-zero shard insert should not index metadata";
    }).join().get();
}

// ===========================================================================
// 4. With shardedRef, non-zero shard insert DOES index metadata
//    (this is the core fix being tested)
// ===========================================================================

TEST_F(EngineMetadataTest, NonZeroShardInsertIndexesMetadataWithRef) {
    if (seastar::smp::count < 2) {
        GTEST_SKIP() << "Need at least 2 shards for this test";
    }

    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        // Set the sharded reference on all shards
        eng.eng.invoke_on_all([&eng](Engine& engine) {
            engine.setShardedRef(&eng.eng);
            return seastar::make_ready_future<>();
        }).get();

        TimeStarInsert<double> insert("cpu", "usage");
        insert.addTag("host", "h1");
        insert.addValue(1000, 50.0);

        // Insert directly on shard 1 (non-zero)
        eng.eng.invoke_on(1, [insert](Engine& engine) mutable {
            return engine.insert(std::move(insert));
        }).get();

        // Metadata SHOULD now be indexed on shard 0 via cross-shard forwarding
        auto fields = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementFields("cpu");
        }).get();
        EXPECT_TRUE(fields.count("usage") > 0) << "With shardedRef, non-zero shard insert should forward metadata to shard 0";

        auto tags = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementTags("cpu");
        }).get();
        EXPECT_TRUE(tags.count("host") > 0) << "Tags should be forwarded to shard 0";

        auto hosts = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getTagValues("cpu", "host");
        }).get();
        EXPECT_TRUE(hosts.count("h1") > 0) << "Tag values should be forwarded to shard 0";
    }).join().get();
}

// ===========================================================================
// 5. Multiple types: bool and string inserts on non-zero shards index metadata
// ===========================================================================

TEST_F(EngineMetadataTest, NonZeroShardBoolAndStringInsertIndexMetadata) {
    if (seastar::smp::count < 2) {
        GTEST_SKIP() << "Need at least 2 shards for this test";
    }

    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        eng.eng.invoke_on_all([&eng](Engine& engine) {
            engine.setShardedRef(&eng.eng);
            return seastar::make_ready_future<>();
        }).get();

        // Insert bool on shard 1
        {
            TimeStarInsert<bool> insert("sensor", "active");
            insert.addTag("zone", "east");
            insert.addValue(1000, true);
            eng.eng.invoke_on(1, [insert](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            }).get();
        }

        // Insert string on shard 1
        {
            TimeStarInsert<std::string> insert("sensor", "status");
            insert.addTag("zone", "east");
            insert.addValue(2000, std::string("healthy"));
            eng.eng.invoke_on(1, [insert](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            }).get();
        }

        // Verify both fields are indexed on shard 0
        auto fields = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementFields("sensor");
        }).get();
        EXPECT_TRUE(fields.count("active") > 0) << "Bool field should be indexed via cross-shard forwarding";
        EXPECT_TRUE(fields.count("status") > 0) << "String field should be indexed via cross-shard forwarding";

        auto tags = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementTags("sensor");
        }).get();
        EXPECT_TRUE(tags.count("zone") > 0);
    }).join().get();
}

// ===========================================================================
// 6. Idempotency: inserting the same series from shard 0 and shard 1 doesn't
//    create duplicate metadata entries
// ===========================================================================

TEST_F(EngineMetadataTest, IdempotentMetadataIndexing) {
    if (seastar::smp::count < 2) {
        GTEST_SKIP() << "Need at least 2 shards for this test";
    }

    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        eng.eng.invoke_on_all([&eng](Engine& engine) {
            engine.setShardedRef(&eng.eng);
            return seastar::make_ready_future<>();
        }).get();

        // Insert same measurement/field/tags on both shard 0 and shard 1
        TimeStarInsert<double> insert("weather", "temperature");
        insert.addTag("location", "us-west");
        insert.addValue(1000, 72.5);

        eng.eng.invoke_on(0, [insert](Engine& engine) mutable {
            return engine.insert(std::move(insert));
        }).get();

        TimeStarInsert<double> insert2("weather", "temperature");
        insert2.addTag("location", "us-west");
        insert2.addValue(2000, 73.0);

        eng.eng.invoke_on(1, [insert2](Engine& engine) mutable {
            return engine.insert(std::move(insert2));
        }).get();

        // Should have exactly 1 field, 1 tag, 1 tag value -- not duplicated
        auto fields = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementFields("weather");
        }).get();
        EXPECT_EQ(fields.size(), 1u);
        EXPECT_TRUE(fields.count("temperature") > 0);

        auto tags = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getMeasurementTags("weather");
        }).get();
        EXPECT_EQ(tags.size(), 1u);

        auto locations = eng.eng.invoke_on(0, [](Engine& engine) {
            return engine.getTagValues("weather", "location");
        }).get();
        EXPECT_EQ(locations.size(), 1u);
        EXPECT_TRUE(locations.count("us-west") > 0);
    }).join().get();
}
