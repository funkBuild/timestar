#include "../../../lib/index/native/native_index.hpp"

#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <filesystem>
#include <format>
#include <string>

using namespace timestar::index;

class NativeIndexTest : public ::testing::Test {
public:
    void SetUp() override {
        // Clean up any leftover data from previous test runs
        std::filesystem::remove_all("shard_0/native_index");
    }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

SEASTAR_TEST_F(NativeIndexTest, OpenAndClose) {
    NativeIndex index(0);
    co_await index.open();
    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, CreateAndRetrieveSeries) {
    NativeIndex index(0);
    co_await index.open();

    auto id = co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "temperature");
    EXPECT_NE(id, SeriesId128{});

    // Same series should return same ID
    auto id2 = co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "temperature");
    EXPECT_EQ(id, id2);

    // Different series should return different ID
    auto id3 = co_await index.getOrCreateSeriesId("weather", {{"location", "us-east"}}, "temperature");
    EXPECT_NE(id, id3);

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, GetSeriesMetadata) {
    NativeIndex index(0);
    co_await index.open();

    auto id = co_await index.getOrCreateSeriesId("cpu", {{"host", "server01"}, {"region", "us"}}, "usage");

    auto meta = co_await index.getSeriesMetadata(id);
    EXPECT_TRUE(meta.has_value());
    EXPECT_EQ(meta->measurement, "cpu");
    EXPECT_EQ(meta->field, "usage");
    EXPECT_EQ(meta->tags.size(), 2u);
    EXPECT_EQ(meta->tags["host"], "server01");
    EXPECT_EQ(meta->tags["region"], "us");

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, FieldsAndTags) {
    NativeIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "temperature");
    co_await index.getOrCreateSeriesId("weather", {{"location", "us-east"}}, "humidity");

    auto fields = co_await index.getFields("weather");
    EXPECT_EQ(fields.size(), 2u);
    EXPECT_TRUE(fields.count("temperature"));
    EXPECT_TRUE(fields.count("humidity"));

    auto tags = co_await index.getTags("weather");
    EXPECT_EQ(tags.size(), 1u);
    EXPECT_TRUE(tags.count("location"));

    auto tagValues = co_await index.getTagValues("weather", "location");
    EXPECT_EQ(tagValues.size(), 2u);
    EXPECT_TRUE(tagValues.count("us-west"));
    EXPECT_TRUE(tagValues.count("us-east"));

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, FieldType) {
    NativeIndex index(0);
    co_await index.open();

    co_await index.setFieldType("weather", "temperature", "float");
    auto ft = co_await index.getFieldType("weather", "temperature");
    EXPECT_EQ(ft, "float");

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, FindSeriesByTag) {
    NativeIndex index(0);
    co_await index.open();

    auto id1 = co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "temp");
    auto id2 = co_await index.getOrCreateSeriesId("weather", {{"location", "us-east"}}, "temp");
    co_await index.getOrCreateSeriesId("weather", {{"location", "eu-west"}}, "temp");

    auto result = co_await index.findSeriesByTag("weather", "location", "us-west");
    EXPECT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], id1);

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, GetAllSeriesForMeasurement) {
    NativeIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("cpu", {{"host", "h1"}}, "usage");
    co_await index.getOrCreateSeriesId("cpu", {{"host", "h2"}}, "usage");
    co_await index.getOrCreateSeriesId("cpu", {{"host", "h3"}}, "idle");

    auto result = co_await index.getAllSeriesForMeasurement("cpu");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 3u);

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, FindSeriesWithMetadata) {
    NativeIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "temp");
    co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "humidity");
    co_await index.getOrCreateSeriesId("weather", {{"location", "us-east"}}, "temp");

    auto result =
        co_await index.findSeriesWithMetadata("weather", {{"location", "us-west"}}, {"temp"});
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ(result->at(0).metadata.field, "temp");

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, AllMeasurements) {
    NativeIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("cpu", {{"host", "h1"}}, "usage");
    co_await index.getOrCreateSeriesId("weather", {{"loc", "us"}}, "temp");

    auto measurements = co_await index.getAllMeasurements();
    EXPECT_EQ(measurements.size(), 2u);
    EXPECT_TRUE(measurements.count("cpu"));
    EXPECT_TRUE(measurements.count("weather"));

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, FieldStats) {
    NativeIndex index(0);
    co_await index.open();

    auto id = co_await index.getOrCreateSeriesId("weather", {{"loc", "us"}}, "temp");

    IndexFieldStats stats{"float", 1000, 2000, 100};
    co_await index.updateFieldStats(id, "temp", stats);

    auto retrieved = co_await index.getFieldStats(id, "temp");
    EXPECT_TRUE(retrieved.has_value());
    EXPECT_EQ(retrieved->dataType, "float");
    EXPECT_EQ(retrieved->minTime, 1000);
    EXPECT_EQ(retrieved->maxTime, 2000);
    EXPECT_EQ(retrieved->pointCount, 100u);

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, SeriesCount) {
    NativeIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("m1", {{"t", "v1"}}, "f1");
    co_await index.getOrCreateSeriesId("m1", {{"t", "v2"}}, "f1");
    co_await index.getOrCreateSeriesId("m2", {{"t", "v1"}}, "f1");

    auto count = co_await index.getSeriesCount();
    EXPECT_EQ(count, 3u);

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, RetentionPolicy) {
    NativeIndex index(0);
    co_await index.open();

    RetentionPolicy policy;
    policy.measurement = "metrics";
    policy.ttl = "7d";
    policy.ttlNanos = 7ULL * 24 * 3600 * 1000000000ULL;
    co_await index.setRetentionPolicy(policy);

    auto retrieved = co_await index.getRetentionPolicy("metrics");
    EXPECT_TRUE(retrieved.has_value());
    EXPECT_EQ(retrieved->measurement, "metrics");
    EXPECT_EQ(retrieved->ttl, "7d");

    auto allPolicies = co_await index.getAllRetentionPolicies();
    EXPECT_EQ(allPolicies.size(), 1u);

    auto deleted = co_await index.deleteRetentionPolicy("metrics");
    EXPECT_TRUE(deleted);

    auto afterDelete = co_await index.getRetentionPolicy("metrics");
    EXPECT_FALSE(afterDelete.has_value());

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, NonZeroShardCanIndexAndQuery) {
    // Clean up shard 1 directory
    std::filesystem::remove_all("shard_1/native_index");

    NativeIndex index(1);  // Non-zero shard — should now be fully operational
    co_await index.open();

    // Can create series
    auto id = co_await index.getOrCreateSeriesId("weather", {{"loc", "us"}}, "temp");
    EXPECT_NE(id, SeriesId128{});

    // Can look up series
    auto foundId = co_await index.getSeriesId("weather", {{"loc", "us"}}, "temp");
    EXPECT_TRUE(foundId.has_value());
    EXPECT_EQ(*foundId, id);

    // Can get metadata
    auto meta = co_await index.getSeriesMetadata(id);
    EXPECT_TRUE(meta.has_value());
    EXPECT_EQ(meta->measurement, "weather");
    EXPECT_EQ(meta->field, "temp");

    // Can query measurements
    auto measurements = co_await index.getAllMeasurements();
    EXPECT_EQ(measurements.size(), 1u);
    EXPECT_TRUE(measurements.count("weather"));

    co_await index.close();
    std::filesystem::remove_all("shard_1/native_index");
}

SEASTAR_TEST_F(NativeIndexTest, ManySeries) {
    NativeIndex index(0);
    co_await index.open();

    const int N = 100;
    for (int i = 0; i < N; ++i) {
        co_await index.getOrCreateSeriesId("sensor", {{"id", std::format("s{:03d}", i)}}, "reading");
    }

    auto count = co_await index.getSeriesCount();
    EXPECT_EQ(count, static_cast<size_t>(N));

    auto allSeries = co_await index.getAllSeriesForMeasurement("sensor");
    EXPECT_TRUE(allSeries.has_value());
    EXPECT_EQ(allSeries->size(), static_cast<size_t>(N));

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexTest, CompactThenQuery) {
    NativeIndex index(0);
    co_await index.open();

    // Insert enough data to trigger flush + compaction
    for (int i = 0; i < 50; ++i) {
        co_await index.getOrCreateSeriesId("m", {{"k", std::format("v{:03d}", i)}}, "f");
    }

    co_await index.compact();

    // Verify all data is still accessible
    auto count = co_await index.getSeriesCount();
    EXPECT_EQ(count, 50u);

    auto allSeries = co_await index.getAllSeriesForMeasurement("m");
    EXPECT_TRUE(allSeries.has_value());
    EXPECT_EQ(allSeries->size(), 50u);

    co_await index.close();
}
