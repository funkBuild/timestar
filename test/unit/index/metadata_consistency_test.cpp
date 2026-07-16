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
    // NOTE: must build the key explicitly — a "cpu\0host" literal truncates at
    // the embedded null when converted to std::string.
    std::string tvKey = "cpu";
    tvKey.push_back('\0');
    tvKey += "host";
    update.newTagValues[tvKey] = {"server-01"};

    // Apply twice
    co_await index.applySchemaUpdate(update);
    co_await index.applySchemaUpdate(update);

    // No duplicates — fields and tags are sets
    auto fields = co_await index.getFields("cpu");
    EXPECT_EQ(fields.count("usage"), 1u);
    EXPECT_EQ(fields.count("idle"), 1u);

    auto tags = co_await index.getTags("cpu");
    EXPECT_EQ(tags.count("host"), 1u);
    EXPECT_EQ(tags.count("dc"), 1u);

    co_await index.close();
}

// TASK A: broadcast schema deltas must be PERSISTED to the receiving shard's
// KV store, so schema survives cache trims and restarts even on shards that
// own none of the measurement's series.
SEASTAR_TEST_F(MetadataConsistencyTest, SchemaUpdatePersistedAcrossReopen) {
    // Session 1: apply a broadcast delta for a measurement this shard does
    // not own (no getOrCreateSeriesId calls for it).
    {
        NativeIndex index(0);
        co_await index.open();

        SchemaUpdate update;
        update.newFields["remote_meas"] = {"f1", "f2"};
        update.newTags["remote_meas"] = {"host", "region"};
        std::string tvKey = "remote_meas";
        tvKey.push_back('\0');
        tvKey += "host";
        update.newTagValues[tvKey] = {"h1", "h2"};
        co_await index.applySchemaUpdate(update);

        co_await index.close();
    }

    // Session 2: caches are empty — everything must come from the local KV.
    {
        NativeIndex index(0);
        co_await index.open();

        auto fields = co_await index.getFields("remote_meas");
        EXPECT_TRUE(fields.count("f1"));
        EXPECT_TRUE(fields.count("f2"));

        auto tags = co_await index.getTags("remote_meas");
        EXPECT_TRUE(tags.count("host"));
        EXPECT_TRUE(tags.count("region"));

        auto values = co_await index.getTagValues("remote_meas", "host");
        EXPECT_TRUE(values.count("h1"));
        EXPECT_TRUE(values.count("h2"));

        auto measurements = co_await index.getAllMeasurements();
        EXPECT_TRUE(measurements.count("remote_meas"));

        co_await index.close();
    }
}

// TASK A: incremental broadcast deltas must union with previously persisted
// schema, not replace it.
SEASTAR_TEST_F(MetadataConsistencyTest, SchemaUpdateIncrementalDeltasUnion) {
    NativeIndex index(0);
    co_await index.open();

    SchemaUpdate u1;
    u1.newFields["m"] = {"a"};
    co_await index.applySchemaUpdate(u1);

    SchemaUpdate u2;
    u2.newFields["m"] = {"b"};
    co_await index.applySchemaUpdate(u2);

    co_await index.close();
    co_await index.open();

    auto fields = co_await index.getFields("m");
    EXPECT_TRUE(fields.count("a"));
    EXPECT_TRUE(fields.count("b"));

    co_await index.close();
}

// Phantom-measurement bug: getFields() negative-caches {} for nonexistent
// measurements; getAllMeasurements() must not list those cache entries.
SEASTAR_TEST_F(MetadataConsistencyTest, GetFieldsForTypoDoesNotCreatePhantomMeasurement) {
    NativeIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("real_meas", {{"t", "v"}}, "f");

    // Query fields for a measurement that doesn't exist (typo) — negative-caches {}
    auto fields = co_await index.getFields("real_maes_typo");
    EXPECT_TRUE(fields.empty());

    auto measurements = co_await index.getAllMeasurements();
    EXPECT_TRUE(measurements.count("real_meas"));
    EXPECT_FALSE(measurements.count("real_maes_typo")) << "typo lookup must not fabricate a measurement";

    co_await index.close();
}

// TASK B: tag values persisted via per-value marker keys — values added across
// multiple sessions must union, and survive restart.
SEASTAR_TEST_F(MetadataConsistencyTest, TagValueMarkersPersistAndUnionAcrossSessions) {
    // Session 1: three values
    {
        NativeIndex index(0);
        co_await index.open();
        for (const char* host : {"h1", "h2", "h3"}) {
            co_await index.getOrCreateSeriesId("tv_meas", {{"host", host}}, "value");
        }
        auto values = co_await index.getTagValues("tv_meas", "host");
        EXPECT_EQ(values.size(), 3u);
        co_await index.close();
    }

    // Session 2: add a fourth value; read must union persisted markers with
    // the new in-session value.
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.getOrCreateSeriesId("tv_meas", {{"host", "h4"}}, "value");
        auto values = co_await index.getTagValues("tv_meas", "host");
        EXPECT_EQ(values.size(), 4u);
        EXPECT_TRUE(values.count("h1"));
        EXPECT_TRUE(values.count("h4"));
        co_await index.close();
    }

    // Session 3: all four values persisted
    {
        NativeIndex index(0);
        co_await index.open();
        auto values = co_await index.getTagValues("tv_meas", "host");
        EXPECT_EQ(values.size(), 4u);
        for (const char* host : {"h1", "h2", "h3", "h4"}) {
            EXPECT_TRUE(values.count(host)) << "missing tag value " << host;
        }
        co_await index.close();
    }
}

// TASK A+B: tag values arriving only via schema broadcast (shard owns no
// series of the measurement) must be readable immediately and after reopen.
SEASTAR_TEST_F(MetadataConsistencyTest, BroadcastTagValuesReadableImmediatelyAndAfterReopen) {
    NativeIndex index(0);
    co_await index.open();

    SchemaUpdate update;
    std::string tvKey = "bcast_meas";
    tvKey.push_back('\0');
    tvKey += "dc";
    update.newTagValues[tvKey] = {"east", "west"};
    co_await index.applySchemaUpdate(update);

    // Immediate read (loads union of blob + markers from KV)
    auto values = co_await index.getTagValues("bcast_meas", "dc");
    EXPECT_TRUE(values.count("east"));
    EXPECT_TRUE(values.count("west"));

    // A second broadcast delta unions with the first
    SchemaUpdate update2;
    update2.newTagValues[tvKey] = {"north"};
    co_await index.applySchemaUpdate(update2);

    auto values2 = co_await index.getTagValues("bcast_meas", "dc");
    EXPECT_EQ(values2.size(), 3u);

    co_await index.close();
    co_await index.open();

    auto values3 = co_await index.getTagValues("bcast_meas", "dc");
    EXPECT_EQ(values3.size(), 3u);
    EXPECT_TRUE(values3.count("north"));

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

    auto count = index.getSeriesCountSync();
    EXPECT_EQ(count, 50u);

    // Inserting the same series again should not change count
    co_await index.getOrCreateSeriesId("metric", {{"id", "0"}}, "value");
    count = index.getSeriesCountSync();
    EXPECT_EQ(count, 50u);

    co_await index.close();
}
