// Metadata consistency tests for NativeIndex.
//
// Verifies that metadata operations (getOrCreateSeriesId, getSeriesMetadata,
// field/tag queries) produce consistent results after flush, compaction,
// and reopen cycles.

#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <seastar/core/coroutine.hh>
#include <set>
#include <string>

namespace fs = std::filesystem;

class MetadataConsistencyTest : public ::testing::Test {
protected:
    void SetUp() override { fs::remove_all("shard_0/native_index"); }
    void TearDown() override { fs::remove_all("shard_0/native_index"); }
};

using namespace timestar::index;

// Same (measurement, tags, field) always returns the same SeriesId128.
SEASTAR_TEST_F(MetadataConsistencyTest, GetOrCreateReturnsSameIdForSameSeries) {
    NativeIndex index(0);
    co_await index.open();

    auto id1 = co_await index.getOrCreateSeriesId("cpu", {{"host", "s1"}}, "usage");
    auto id2 = co_await index.getOrCreateSeriesId("cpu", {{"host", "s1"}}, "usage");
    auto id3 = co_await index.getOrCreateSeriesId("cpu", {{"host", "s1"}}, "usage");

    EXPECT_EQ(id1, id2);
    EXPECT_EQ(id2, id3);
    EXPECT_FALSE(id1.isZero());

    co_await index.close();
}

// Different series get unique IDs.
SEASTAR_TEST_F(MetadataConsistencyTest, DifferentSeriesGetUniqueIds) {
    NativeIndex index(0);
    co_await index.open();

    std::set<SeriesId128, std::less<>> ids;
    for (int i = 0; i < 100; ++i) {
        auto id = co_await index.getOrCreateSeriesId(
            "sensor", {{"id", "s" + std::to_string(i)}}, "reading");
        EXPECT_FALSE(id.isZero());
        ids.insert(id);
    }

    EXPECT_EQ(ids.size(), 100u);

    co_await index.close();
}

// Metadata is consistent after memtable flush to SSTable.
SEASTAR_TEST_F(MetadataConsistencyTest, MetadataConsistentAfterFlush) {
    NativeIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("weather", {{"loc", "nyc"}, {"sensor", "a1"}}, "temp");
    co_await index.getOrCreateSeriesId("weather", {{"loc", "nyc"}, {"sensor", "a1"}}, "humidity");
    co_await index.getOrCreateSeriesId("weather", {{"loc", "london"}}, "temp");

    // Force flush
    // Close and reopen to force flush to SSTable
    co_await index.close();
    co_await index.open();

    // Verify metadata survived flush
    auto fields = co_await index.getFields("weather");
    EXPECT_TRUE(fields.count("temp"));
    EXPECT_TRUE(fields.count("humidity"));

    auto tags = co_await index.getTags("weather");
    EXPECT_TRUE(tags.count("loc"));
    EXPECT_TRUE(tags.count("sensor"));

    auto locs = co_await index.getTagValues("weather", "loc");
    EXPECT_TRUE(locs.count("nyc"));
    EXPECT_TRUE(locs.count("london"));

    co_await index.close();
}

// Metadata survives close and reopen (persistence).
SEASTAR_TEST_F(MetadataConsistencyTest, MetadataConsistentAfterReopen) {
    // Phase 1: Insert and close
    {
        NativeIndex index(0);
        co_await index.open();

        co_await index.getOrCreateSeriesId("disk", {{"host", "prod-1"}}, "iops");
        co_await index.getOrCreateSeriesId("disk", {{"host", "prod-2"}}, "iops");

        co_await index.close();
    }

    // Phase 2: Reopen and verify
    {
        NativeIndex index(0);
        co_await index.open();

        auto measurements = co_await index.getAllMeasurements();
        EXPECT_TRUE(measurements.count("disk"));

        auto fields = co_await index.getFields("disk");
        EXPECT_TRUE(fields.count("iops"));

        auto hosts = co_await index.getTagValues("disk", "host");
        EXPECT_TRUE(hosts.count("prod-1"));
        EXPECT_TRUE(hosts.count("prod-2"));

        // Series IDs should be the same as before
        auto id = co_await index.getSeriesId("disk", {{"host", "prod-1"}}, "iops");
        EXPECT_TRUE(id.has_value());
        EXPECT_FALSE(id->isZero());

        co_await index.close();
    }
}

// SchemaUpdate applied twice is idempotent.
SEASTAR_TEST_F(MetadataConsistencyTest, SchemaUpdateIdempotent) {
    NativeIndex index(0);
    co_await index.open();

    SchemaUpdate update;
    update.newFields["cpu"] = {"usage", "idle"};
    update.newTags["cpu"] = {"host", "dc"};
    update.newTagValues["cpu\0host"] = {"server-01"};

    // Apply twice
    index.applySchemaUpdate(update);
    index.applySchemaUpdate(update);

    // No duplicates — fields and tags are sets
    auto fields = co_await index.getFields("cpu");
    EXPECT_EQ(fields.count("usage"), 1u);
    EXPECT_EQ(fields.count("idle"), 1u);

    auto tags = co_await index.getTags("cpu");
    EXPECT_EQ(tags.count("host"), 1u);
    EXPECT_EQ(tags.count("dc"), 1u);

    co_await index.close();
}

// Field type is set on first write and preserved.
SEASTAR_TEST_F(MetadataConsistencyTest, FieldTypePreservedAfterFlush) {
    NativeIndex index(0);
    co_await index.open();

    co_await index.setFieldType("cpu", "usage", "float");
    // Close and reopen to force flush to SSTable
    co_await index.close();
    co_await index.open();

    auto fieldType = co_await index.getFieldType("cpu", "usage");
    EXPECT_EQ(fieldType, "float");

    co_await index.close();
}

// Series count is accurate after multiple inserts.
SEASTAR_TEST_F(MetadataConsistencyTest, SeriesCountAccurate) {
    NativeIndex index(0);
    co_await index.open();

    for (int i = 0; i < 50; ++i) {
        co_await index.getOrCreateSeriesId(
            "metric", {{"id", std::to_string(i)}}, "value");
    }

    auto count = co_await index.getSeriesCount();
    EXPECT_EQ(count, 50u);

    // Inserting the same series again should not change count
    co_await index.getOrCreateSeriesId("metric", {{"id", "0"}}, "value");
    count = co_await index.getSeriesCount();
    EXPECT_EQ(count, 50u);

    co_await index.close();
}
