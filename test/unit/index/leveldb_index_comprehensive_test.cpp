/*
 * Comprehensive LevelDB index tests covering previously untested code paths.
 *
 * Areas covered:
 *   1. rebuildMeasurementSeriesIndex()        - rebuild from SERIES_METADATA entries
 *   2. getSeriesMetadataBatch()               - batched metadata lookup
 *   3. indexMetadataBatch()                   - batch indexing of multiple ops
 *   4. findSeriesWithMetadata()               - series discovery with metadata
 *   5. getAllSeriesForMeasurement() with limit - SeriesLimitExceeded path
 *   6. findSeries() with maxSeries limit      - SeriesLimitExceeded path
 *   7. addField() standalone                  - direct field metadata writes
 *   8. addTag() standalone                    - direct tag metadata writes
 *   9. addFieldsAndTags() directly            - batched field+tag metadata
 *  10. getSeriesId() (lookup-only, no create) - present and absent cases
 *  11. Series with empty tag map              - no-tag series
 *  12. getSeriesCount()                       - count via SERIES_METADATA scan
 *  13. compact()                              - compaction is a no-op for correctness
 *  14. Non-zero shard throws on write methods - error handling
 *  15. findSeries() short-circuit on empty tag result
 *  16. indexMetadataBatch() empty ops         - no-op path
 *  17. TSDBInsert<int64_t> indexInsert        - integer field type
 *  18. getSeriesId() on non-zero shard throws
 *  19. addField()/addTag() on non-zero shard throw
 */

#include <gtest/gtest.h>
#include "../../seastar_gtest.hpp"
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <filesystem>
#include <map>
#include <set>
#include <vector>

#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm.hpp"  // for TSMValueType

class LevelDBIndexComprehensiveTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::remove_all("shard_0");
    }
    void TearDown() override {
        std::filesystem::remove_all("shard_0");
    }
};

// ---------------------------------------------------------------------------
// 1. rebuildMeasurementSeriesIndex()
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, RebuildMeasurementSeriesIndex) {
    // Phase 1: insert several series and close
    std::vector<SeriesId128> originalIds;
    {
        LevelDBIndex index(0);
        co_await index.open();

        originalIds.push_back(co_await index.getOrCreateSeriesId(
            "rebuild_test", {{"host", "h1"}}, "cpu"));
        originalIds.push_back(co_await index.getOrCreateSeriesId(
            "rebuild_test", {{"host", "h2"}}, "cpu"));
        originalIds.push_back(co_await index.getOrCreateSeriesId(
            "rebuild_test", {{"host", "h3"}}, "cpu"));
        originalIds.push_back(co_await index.getOrCreateSeriesId(
            "other_measurement", {{"tag", "v1"}}, "value"));

        co_await index.close();
    }

    // Phase 2: reopen, clear the measurement series cache, then rebuild
    {
        LevelDBIndex index(0);
        co_await index.open();

        // Rebuild the MEASUREMENT_SERIES index from SERIES_METADATA entries
        co_await index.rebuildMeasurementSeriesIndex();

        // After rebuild, getAllSeriesForMeasurement should find the same series
        auto rebuildResult = co_await index.getAllSeriesForMeasurement("rebuild_test");
        EXPECT_TRUE(rebuildResult.has_value())
            << "getAllSeriesForMeasurement should succeed after rebuild";
        if (rebuildResult.has_value()) {
            EXPECT_EQ(rebuildResult.value().size(), 3u);
        }

        auto otherResult = co_await index.getAllSeriesForMeasurement("other_measurement");
        EXPECT_TRUE(otherResult.has_value());
        if (otherResult.has_value()) {
            EXPECT_EQ(otherResult.value().size(), 1u);
        }

        co_await index.close();
    }
    co_return;
}

// Rebuild on empty index should be a no-op (no crash)
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, RebuildMeasurementSeriesIndexEmpty) {
    LevelDBIndex index(0);
    co_await index.open();

    // Nothing indexed yet — rebuild should complete without error
    co_await index.rebuildMeasurementSeriesIndex();

    auto result = co_await index.getAllSeriesForMeasurement("nonexistent");
    EXPECT_TRUE(result.has_value());
    if (result.has_value()) {
        EXPECT_TRUE(result.value().empty());
    }

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 2. getSeriesMetadataBatch()
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, GetSeriesMetadataBatch) {
    LevelDBIndex index(0);
    co_await index.open();

    // Create several series
    auto id1 = co_await index.getOrCreateSeriesId("batch_test",
        {{"region", "us-west"}, {"env", "prod"}}, "latency");
    auto id2 = co_await index.getOrCreateSeriesId("batch_test",
        {{"region", "eu-west"}, {"env", "staging"}}, "latency");
    auto id3 = co_await index.getOrCreateSeriesId("other",
        {{"host", "srv1"}}, "cpu");

    // Fetch metadata in batch for all three
    std::vector<SeriesId128> ids = {id1, id2, id3};
    auto batchResults = co_await index.getSeriesMetadataBatch(ids);

    EXPECT_EQ(batchResults.size(), 3u);

    // Verify id1 metadata
    bool foundId1 = false;
    for (const auto& [id, meta] : batchResults) {
        if (id == id1) {
            foundId1 = true;
            EXPECT_TRUE(meta.has_value());
            if (meta.has_value()) {
                EXPECT_EQ(meta->measurement, "batch_test");
                EXPECT_EQ(meta->field, "latency");
                EXPECT_EQ(meta->tags.at("region"), "us-west");
                EXPECT_EQ(meta->tags.at("env"), "prod");
            }
        }
    }
    EXPECT_TRUE(foundId1) << "id1 should appear in batch results";

    co_await index.close();
    co_return;
}

// getSeriesMetadataBatch with mixed existing and non-existing IDs
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, GetSeriesMetadataBatchMixed) {
    LevelDBIndex index(0);
    co_await index.open();

    auto existingId = co_await index.getOrCreateSeriesId("m",
        {{"k", "v"}}, "f");
    SeriesId128 nonExistentId = SeriesId128::fromSeriesKey("completely_fake_series_key");

    auto batchResults = co_await index.getSeriesMetadataBatch({existingId, nonExistentId});
    EXPECT_EQ(batchResults.size(), 2u);

    // Existing should have metadata; non-existent should have nullopt
    for (const auto& [id, meta] : batchResults) {
        if (id == existingId) {
            EXPECT_TRUE(meta.has_value()) << "Existing series should have metadata";
        } else if (id == nonExistentId) {
            EXPECT_FALSE(meta.has_value()) << "Non-existent series should return nullopt";
        }
    }

    co_await index.close();
    co_return;
}

// getSeriesMetadataBatch with empty input
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, GetSeriesMetadataBatchEmpty) {
    LevelDBIndex index(0);
    co_await index.open();

    auto results = co_await index.getSeriesMetadataBatch({});
    EXPECT_TRUE(results.empty());

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 3. indexMetadataBatch()
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, IndexMetadataBatchNewSeries) {
    LevelDBIndex index(0);
    co_await index.open();

    // Build a batch of MetadataOp entries
    std::vector<MetadataOp> ops;
    ops.push_back({TSMValueType::Float, "sensors", "temperature",
                   {{"location", "room1"}, {"floor", "1"}}});
    ops.push_back({TSMValueType::Float, "sensors", "humidity",
                   {{"location", "room1"}, {"floor", "1"}}});
    ops.push_back({TSMValueType::Boolean, "alerts", "triggered",
                   {{"severity", "high"}}});

    co_await index.indexMetadataBatch(ops);

    // All field types should be indexed
    auto tempType = co_await index.getFieldType("sensors", "temperature");
    EXPECT_EQ(tempType, "float");

    auto humType = co_await index.getFieldType("sensors", "humidity");
    EXPECT_EQ(humType, "float");

    auto alertType = co_await index.getFieldType("alerts", "triggered");
    EXPECT_EQ(alertType, "boolean");

    // Fields should be discoverable
    auto sensorFields = co_await index.getFields("sensors");
    EXPECT_EQ(sensorFields.size(), 2u);
    EXPECT_TRUE(sensorFields.count("temperature") > 0);
    EXPECT_TRUE(sensorFields.count("humidity") > 0);

    // Tags should be indexed
    auto sensorTags = co_await index.getTags("sensors");
    EXPECT_TRUE(sensorTags.count("location") > 0);
    EXPECT_TRUE(sensorTags.count("floor") > 0);

    // Tag values should be indexed
    auto locations = co_await index.getTagValues("sensors", "location");
    EXPECT_TRUE(locations.count("room1") > 0);

    co_await index.close();
    co_return;
}

// indexMetadataBatch with empty ops vector - no-op path
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, IndexMetadataBatchEmpty) {
    LevelDBIndex index(0);
    co_await index.open();

    // Should complete without error
    co_await index.indexMetadataBatch({});

    // Index should be empty
    auto count = co_await index.getSeriesCount();
    EXPECT_EQ(count, 0u);

    co_await index.close();
    co_return;
}

// indexMetadataBatch idempotency: calling twice should not duplicate entries
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, IndexMetadataBatchIdempotent) {
    LevelDBIndex index(0);
    co_await index.open();

    MetadataOp op{TSMValueType::Float, "idempotent_test", "value",
                  {{"host", "server1"}}};
    std::vector<MetadataOp> ops = {op};

    co_await index.indexMetadataBatch(ops);
    co_await index.indexMetadataBatch(ops); // Second call should be a no-op

    // Should only have 1 series
    auto count = co_await index.getSeriesCount();
    EXPECT_EQ(count, 1u);

    // Only 1 field
    auto fields = co_await index.getFields("idempotent_test");
    EXPECT_EQ(fields.size(), 1u);

    co_await index.close();
    co_return;
}

// indexMetadataBatch with all TSMValueType variants
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, IndexMetadataBatchAllTypes) {
    LevelDBIndex index(0);
    co_await index.open();

    std::vector<MetadataOp> ops = {
        {TSMValueType::Float,   "types_test", "float_field",   {{"t", "v"}}},
        {TSMValueType::Boolean, "types_test", "bool_field",    {{"t", "v"}}},
        {TSMValueType::String,  "types_test", "string_field",  {{"t", "v"}}},
        {TSMValueType::Integer, "types_test", "integer_field", {{"t", "v"}}},
    };

    co_await index.indexMetadataBatch(ops);

    EXPECT_EQ(co_await index.getFieldType("types_test", "float_field"),   "float");
    EXPECT_EQ(co_await index.getFieldType("types_test", "bool_field"),    "boolean");
    EXPECT_EQ(co_await index.getFieldType("types_test", "string_field"),  "string");
    EXPECT_EQ(co_await index.getFieldType("types_test", "integer_field"), "integer");

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 4. findSeriesWithMetadata()
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, FindSeriesWithMetadataBasic) {
    LevelDBIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("http_req",
        {{"endpoint", "/api/users"}, {"method", "GET"}}, "count");
    co_await index.getOrCreateSeriesId("http_req",
        {{"endpoint", "/api/posts"}, {"method", "POST"}}, "count");
    co_await index.getOrCreateSeriesId("http_req",
        {{"endpoint", "/api/users"}, {"method", "POST"}}, "duration");

    // Find all series for measurement (no filter, no field filter)
    auto allResult = co_await index.findSeriesWithMetadata("http_req");
    EXPECT_TRUE(allResult.has_value());
    if (allResult.has_value()) {
        EXPECT_EQ(allResult.value().size(), 3u);
        // Each entry should have measurement and field populated
        for (const auto& entry : allResult.value()) {
            EXPECT_EQ(entry.metadata.measurement, "http_req");
            EXPECT_FALSE(entry.metadata.field.empty());
        }
    }

    co_await index.close();
    co_return;
}

// findSeriesWithMetadata with tag filter
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, FindSeriesWithMetadataTagFilter) {
    LevelDBIndex index(0);
    co_await index.open();

    auto id1 = co_await index.getOrCreateSeriesId("cpu",
        {{"host", "srv1"}, {"dc", "dc1"}}, "usage");
    auto id2 = co_await index.getOrCreateSeriesId("cpu",
        {{"host", "srv2"}, {"dc", "dc1"}}, "usage");
    auto id3 = co_await index.getOrCreateSeriesId("cpu",
        {{"host", "srv3"}, {"dc", "dc2"}}, "usage");

    // Filter by dc=dc1
    auto dc1Result = co_await index.findSeriesWithMetadata(
        "cpu", {{"dc", "dc1"}});
    EXPECT_TRUE(dc1Result.has_value());
    if (dc1Result.has_value()) {
        EXPECT_EQ(dc1Result.value().size(), 2u);
        for (const auto& entry : dc1Result.value()) {
            EXPECT_EQ(entry.metadata.tags.at("dc"), "dc1");
        }
    }

    co_await index.close();
    co_return;
}

// findSeriesWithMetadata with field filter
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, FindSeriesWithMetadataFieldFilter) {
    LevelDBIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("sensor",
        {{"device", "d1"}}, "temperature");
    co_await index.getOrCreateSeriesId("sensor",
        {{"device", "d1"}}, "humidity");
    co_await index.getOrCreateSeriesId("sensor",
        {{"device", "d2"}}, "temperature");

    // Field filter to only return "temperature" series
    std::unordered_set<std::string> fieldFilter = {"temperature"};
    auto result = co_await index.findSeriesWithMetadata(
        "sensor", {}, fieldFilter);
    EXPECT_TRUE(result.has_value());
    if (result.has_value()) {
        EXPECT_EQ(result.value().size(), 2u);
        for (const auto& entry : result.value()) {
            EXPECT_EQ(entry.metadata.field, "temperature");
        }
    }

    co_await index.close();
    co_return;
}

// findSeriesWithMetadata returns empty for non-existent measurement
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, FindSeriesWithMetadataEmpty) {
    LevelDBIndex index(0);
    co_await index.open();

    auto result = co_await index.findSeriesWithMetadata("no_such_measurement");
    EXPECT_TRUE(result.has_value());
    if (result.has_value()) {
        EXPECT_TRUE(result.value().empty());
    }

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 5 & 6. SeriesLimitExceeded paths
// ---------------------------------------------------------------------------

// getAllSeriesForMeasurement with limit that is exceeded
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, GetAllSeriesForMeasurementLimitExceeded) {
    LevelDBIndex index(0);
    co_await index.open();

    // Create 5 series
    for (int i = 0; i < 5; ++i) {
        co_await index.getOrCreateSeriesId("limit_test",
            {{"id", std::to_string(i)}}, "value");
    }

    // Request with maxSeries=3: should return SeriesLimitExceeded
    auto result = co_await index.getAllSeriesForMeasurement("limit_test", 3);
    EXPECT_FALSE(result.has_value()) << "Should return SeriesLimitExceeded when 5 > limit 3";
    if (!result.has_value()) {
        EXPECT_GT(result.error().discovered, 3u);
        EXPECT_EQ(result.error().limit, 3u);
    }

    // Request with maxSeries=10 (not exceeded): should succeed
    auto okResult = co_await index.getAllSeriesForMeasurement("limit_test", 10);
    EXPECT_TRUE(okResult.has_value());
    if (okResult.has_value()) {
        EXPECT_EQ(okResult.value().size(), 5u);
    }

    // Request with maxSeries=0 (unlimited): should succeed
    auto unlimitedResult = co_await index.getAllSeriesForMeasurement("limit_test", 0);
    EXPECT_TRUE(unlimitedResult.has_value());
    if (unlimitedResult.has_value()) {
        EXPECT_EQ(unlimitedResult.value().size(), 5u);
    }

    co_await index.close();
    co_return;
}

// findSeries with maxSeries limit exceeded (no tag filter path)
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, FindSeriesLimitExceededNoFilter) {
    LevelDBIndex index(0);
    co_await index.open();

    for (int i = 0; i < 4; ++i) {
        co_await index.getOrCreateSeriesId("find_limit",
            {{"n", std::to_string(i)}}, "v");
    }

    // Limit of 2 should be exceeded with 4 series
    auto result = co_await index.findSeries("find_limit", {}, 2);
    EXPECT_FALSE(result.has_value()) << "Should return SeriesLimitExceeded for 4 series > limit 2";
    if (!result.has_value()) {
        EXPECT_GT(result.error().discovered, 2u);
    }

    // Limit of 10 should succeed
    auto okResult = co_await index.findSeries("find_limit", {}, 10);
    EXPECT_TRUE(okResult.has_value());

    co_await index.close();
    co_return;
}

// findSeries with maxSeries limit exceeded via tag filter path
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, FindSeriesLimitExceededWithFilter) {
    LevelDBIndex index(0);
    co_await index.open();

    // Create 5 series all with dc=prod
    for (int i = 0; i < 5; ++i) {
        co_await index.getOrCreateSeriesId("tagged_limit",
            {{"dc", "prod"}, {"host", "h" + std::to_string(i)}}, "v");
    }

    // Limit of 2 should be exceeded
    auto result = co_await index.findSeries("tagged_limit", {{"dc", "prod"}}, 2);
    EXPECT_FALSE(result.has_value()) << "5 series with dc=prod should exceed limit 2";

    // Limit of 10 should succeed
    auto okResult = co_await index.findSeries("tagged_limit", {{"dc", "prod"}}, 10);
    EXPECT_TRUE(okResult.has_value());
    if (okResult.has_value()) {
        EXPECT_EQ(okResult.value().size(), 5u);
    }

    co_await index.close();
    co_return;
}

// findSeries short-circuit: one tag returns empty => whole result is empty
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, FindSeriesShortCircuitEmptyTag) {
    LevelDBIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("shortcircuit",
        {{"env", "prod"}, {"region", "us-west"}}, "v");

    // Filter with a tag that has no matches: short-circuit should return empty
    auto result = co_await index.findSeries("shortcircuit",
        {{"env", "prod"}, {"region", "nonexistent"}});
    EXPECT_TRUE(result.has_value());
    if (result.has_value()) {
        EXPECT_TRUE(result.value().empty());
    }

    co_await index.close();
    co_return;
}

// findSeriesWithMetadata returns SeriesLimitExceeded when limit exceeded
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, FindSeriesWithMetadataLimitExceeded) {
    LevelDBIndex index(0);
    co_await index.open();

    for (int i = 0; i < 5; ++i) {
        co_await index.getOrCreateSeriesId("meta_limit",
            {{"id", std::to_string(i)}}, "value");
    }

    // maxSeries=2 should be exceeded
    auto result = co_await index.findSeriesWithMetadata("meta_limit", {}, {}, 2);
    EXPECT_FALSE(result.has_value()) << "5 series should exceed limit 2";

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 7 & 8. addField() and addTag() standalone methods
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, AddFieldStandalone) {
    LevelDBIndex index(0);
    co_await index.open();

    // Add fields directly (without creating series)
    co_await index.addField("direct_fields", "field_one");
    co_await index.addField("direct_fields", "field_two");
    co_await index.addField("direct_fields", "field_one"); // idempotent

    auto fields = co_await index.getFields("direct_fields");
    EXPECT_EQ(fields.size(), 2u);
    EXPECT_TRUE(fields.count("field_one") > 0);
    EXPECT_TRUE(fields.count("field_two") > 0);

    // Non-existent measurement should return empty
    auto noFields = co_await index.getFields("nonexistent_measurement");
    EXPECT_TRUE(noFields.empty());

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, AddTagStandalone) {
    LevelDBIndex index(0);
    co_await index.open();

    // Add tags directly
    co_await index.addTag("direct_tags", "region", "us-east");
    co_await index.addTag("direct_tags", "region", "eu-west");
    co_await index.addTag("direct_tags", "env", "prod");
    co_await index.addTag("direct_tags", "region", "us-east"); // idempotent

    // Check tag keys
    auto tagKeys = co_await index.getTags("direct_tags");
    EXPECT_EQ(tagKeys.size(), 2u);
    EXPECT_TRUE(tagKeys.count("region") > 0);
    EXPECT_TRUE(tagKeys.count("env") > 0);

    // Check tag values
    auto regionValues = co_await index.getTagValues("direct_tags", "region");
    EXPECT_EQ(regionValues.size(), 2u);
    EXPECT_TRUE(regionValues.count("us-east") > 0);
    EXPECT_TRUE(regionValues.count("eu-west") > 0);

    auto envValues = co_await index.getTagValues("direct_tags", "env");
    EXPECT_EQ(envValues.size(), 1u);
    EXPECT_TRUE(envValues.count("prod") > 0);

    // Non-existent tag should return empty
    auto noValues = co_await index.getTagValues("direct_tags", "nonexistent_tag");
    EXPECT_TRUE(noValues.empty());

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 9. addFieldsAndTags() directly
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, AddFieldsAndTagsDirect) {
    LevelDBIndex index(0);
    co_await index.open();

    std::map<std::string, std::string> tags = {
        {"host", "srv1"},
        {"datacenter", "dc1"}
    };

    co_await index.addFieldsAndTags("metrics", "cpu_usage", tags);
    co_await index.addFieldsAndTags("metrics", "mem_usage", tags);

    // Both fields should be present
    auto fields = co_await index.getFields("metrics");
    EXPECT_EQ(fields.size(), 2u);
    EXPECT_TRUE(fields.count("cpu_usage") > 0);
    EXPECT_TRUE(fields.count("mem_usage") > 0);

    // Tag keys should be present
    auto tagKeys = co_await index.getTags("metrics");
    EXPECT_TRUE(tagKeys.count("host") > 0);
    EXPECT_TRUE(tagKeys.count("datacenter") > 0);

    // Tag values should be present
    auto hostValues = co_await index.getTagValues("metrics", "host");
    EXPECT_TRUE(hostValues.count("srv1") > 0);

    co_await index.close();
    co_return;
}

// addFieldsAndTags with empty tags map (measurement with no tags)
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, AddFieldsAndTagsEmptyTags) {
    LevelDBIndex index(0);
    co_await index.open();

    std::map<std::string, std::string> emptyTags;
    co_await index.addFieldsAndTags("notag_measurement", "value", emptyTags);

    // Field should exist
    auto fields = co_await index.getFields("notag_measurement");
    EXPECT_EQ(fields.size(), 1u);
    EXPECT_TRUE(fields.count("value") > 0);

    // Tags should be empty (no tags were added)
    auto tagKeys = co_await index.getTags("notag_measurement");
    EXPECT_TRUE(tagKeys.empty());

    co_await index.close();
    co_return;
}

// addFieldsAndTags idempotency
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, AddFieldsAndTagsIdempotent) {
    LevelDBIndex index(0);
    co_await index.open();

    std::map<std::string, std::string> tags = {{"k", "v"}};

    // Call multiple times
    co_await index.addFieldsAndTags("idempotent_fat", "f", tags);
    co_await index.addFieldsAndTags("idempotent_fat", "f", tags);
    co_await index.addFieldsAndTags("idempotent_fat", "f", tags);

    // Should still have exactly 1 field
    auto fields = co_await index.getFields("idempotent_fat");
    EXPECT_EQ(fields.size(), 1u);

    // Should still have exactly 1 tag key
    auto tagKeys = co_await index.getTags("idempotent_fat");
    EXPECT_EQ(tagKeys.size(), 1u);

    // Should still have exactly 1 tag value
    auto tagValues = co_await index.getTagValues("idempotent_fat", "k");
    EXPECT_EQ(tagValues.size(), 1u);

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 10. getSeriesId() - lookup-only, no creation
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, GetSeriesIdExisting) {
    LevelDBIndex index(0);
    co_await index.open();

    // First create the series
    std::map<std::string, std::string> tags = {{"host", "srv1"}};
    SeriesId128 createdId = co_await index.getOrCreateSeriesId(
        "lookup_test", tags, "cpu");

    // Then look it up with getSeriesId
    auto lookedUpId = co_await index.getSeriesId("lookup_test", tags, "cpu");
    EXPECT_TRUE(lookedUpId.has_value());
    if (lookedUpId.has_value()) {
        EXPECT_EQ(lookedUpId.value(), createdId);
    }

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, GetSeriesIdNonExistent) {
    LevelDBIndex index(0);
    co_await index.open();

    // Look up a series that was never created
    auto result = co_await index.getSeriesId(
        "nonexistent_measurement",
        {{"tag", "value"}},
        "field");

    // Should return nullopt (not create a new series)
    EXPECT_FALSE(result.has_value());

    // Verify it was NOT created (series count should be 0)
    auto count = co_await index.getSeriesCount();
    EXPECT_EQ(count, 0u);

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 11. Series with empty tag map (no-tag series)
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, SeriesWithEmptyTags) {
    LevelDBIndex index(0);
    co_await index.open();

    // Create series with no tags
    std::map<std::string, std::string> emptyTags;
    SeriesId128 id1 = co_await index.getOrCreateSeriesId(
        "notag_series", emptyTags, "value");
    EXPECT_FALSE(id1.isZero());

    // Same series should return same ID (idempotency)
    SeriesId128 id1_again = co_await index.getOrCreateSeriesId(
        "notag_series", emptyTags, "value");
    EXPECT_EQ(id1, id1_again);

    // Different field with empty tags should get different ID
    SeriesId128 id2 = co_await index.getOrCreateSeriesId(
        "notag_series", emptyTags, "count");
    EXPECT_NE(id1, id2);

    // Metadata should not contain any tags
    auto meta = co_await index.getSeriesMetadata(id1);
    EXPECT_TRUE(meta.has_value());
    if (meta.has_value()) {
        EXPECT_TRUE(meta->tags.empty());
        EXPECT_EQ(meta->measurement, "notag_series");
        EXPECT_EQ(meta->field, "value");
    }

    // findSeries with no filter should return both series
    auto allSeries = (co_await index.findSeries("notag_series")).value();
    EXPECT_EQ(allSeries.size(), 2u);

    // getTags should return empty for measurement with no-tag series
    auto tagKeys = co_await index.getTags("notag_series");
    EXPECT_TRUE(tagKeys.empty());

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 12. getSeriesCount()
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, GetSeriesCount) {
    LevelDBIndex index(0);
    co_await index.open();

    // Empty index
    EXPECT_EQ(co_await index.getSeriesCount(), 0u);

    // Add series one by one and verify count increments
    co_await index.getOrCreateSeriesId("cnt", {{"i", "0"}}, "v");
    EXPECT_EQ(co_await index.getSeriesCount(), 1u);

    co_await index.getOrCreateSeriesId("cnt", {{"i", "1"}}, "v");
    EXPECT_EQ(co_await index.getSeriesCount(), 2u);

    co_await index.getOrCreateSeriesId("cnt", {{"i", "2"}}, "v");
    EXPECT_EQ(co_await index.getSeriesCount(), 3u);

    // Adding same series again should not increment count
    co_await index.getOrCreateSeriesId("cnt", {{"i", "0"}}, "v");
    EXPECT_EQ(co_await index.getSeriesCount(), 3u);

    // Add series across different measurements - all count in total
    co_await index.getOrCreateSeriesId("other_cnt", {{"k", "v"}}, "f");
    EXPECT_EQ(co_await index.getSeriesCount(), 4u);

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 13. compact() does not corrupt data
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, CompactPreservesData) {
    LevelDBIndex index(0);
    co_await index.open();

    // Insert series
    auto id1 = co_await index.getOrCreateSeriesId("compact_test",
        {{"host", "h1"}}, "value");
    auto id2 = co_await index.getOrCreateSeriesId("compact_test",
        {{"host", "h2"}}, "value");

    // Run compaction
    co_await index.compact();

    // All data should still be accessible after compaction
    auto meta1 = co_await index.getSeriesMetadata(id1);
    EXPECT_TRUE(meta1.has_value());

    auto meta2 = co_await index.getSeriesMetadata(id2);
    EXPECT_TRUE(meta2.has_value());

    auto fields = co_await index.getFields("compact_test");
    EXPECT_EQ(fields.size(), 1u);

    auto tagValues = co_await index.getTagValues("compact_test", "host");
    EXPECT_EQ(tagValues.size(), 2u);

    auto seriesByTag = co_await index.findSeriesByTag("compact_test", "host", "h1");
    EXPECT_EQ(seriesByTag.size(), 1u);

    auto count = co_await index.getSeriesCount();
    EXPECT_EQ(count, 2u);

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 14. Non-zero shard throws on write methods
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, NonZeroShardThrowsOnWriteMethods) {
    LevelDBIndex index(1); // Non-zero shard
    co_await index.open();  // This should be a no-op (non-zero shards skip LevelDB)

    // getOrCreateSeriesId should throw
    bool threwOnCreate = false;
    try {
        co_await index.getOrCreateSeriesId("m", {}, "f");
    } catch (const std::runtime_error&) {
        threwOnCreate = true;
    }
    EXPECT_TRUE(threwOnCreate) << "getOrCreateSeriesId should throw on non-zero shard";

    // addField should throw
    bool threwOnAddField = false;
    try {
        co_await index.addField("m", "f");
    } catch (const std::runtime_error&) {
        threwOnAddField = true;
    }
    EXPECT_TRUE(threwOnAddField) << "addField should throw on non-zero shard";

    // addTag should throw
    bool threwOnAddTag = false;
    try {
        co_await index.addTag("m", "k", "v");
    } catch (const std::runtime_error&) {
        threwOnAddTag = true;
    }
    EXPECT_TRUE(threwOnAddTag) << "addTag should throw on non-zero shard";

    // addFieldsAndTags should throw
    bool threwOnFAT = false;
    try {
        co_await index.addFieldsAndTags("m", "f", {});
    } catch (const std::runtime_error&) {
        threwOnFAT = true;
    }
    EXPECT_TRUE(threwOnFAT) << "addFieldsAndTags should throw on non-zero shard";

    // setFieldType should throw
    bool threwOnSetType = false;
    try {
        co_await index.setFieldType("m", "f", "float");
    } catch (const std::runtime_error&) {
        threwOnSetType = true;
    }
    EXPECT_TRUE(threwOnSetType) << "setFieldType should throw on non-zero shard";

    // getFieldType should throw
    bool threwOnGetType = false;
    try {
        co_await index.getFieldType("m", "f");
    } catch (const std::runtime_error&) {
        threwOnGetType = true;
    }
    EXPECT_TRUE(threwOnGetType) << "getFieldType should throw on non-zero shard";

    // getTags should throw
    bool threwOnGetTags = false;
    try {
        co_await index.getTags("m");
    } catch (const std::runtime_error&) {
        threwOnGetTags = true;
    }
    EXPECT_TRUE(threwOnGetTags) << "getTags should throw on non-zero shard";

    // getTagValues should throw
    bool threwOnGetTagValues = false;
    try {
        co_await index.getTagValues("m", "k");
    } catch (const std::runtime_error&) {
        threwOnGetTagValues = true;
    }
    EXPECT_TRUE(threwOnGetTagValues) << "getTagValues should throw on non-zero shard";

    // getAllMeasurements should throw
    bool threwOnGetMeasurements = false;
    try {
        co_await index.getAllMeasurements();
    } catch (const std::runtime_error&) {
        threwOnGetMeasurements = true;
    }
    EXPECT_TRUE(threwOnGetMeasurements) << "getAllMeasurements should throw on non-zero shard";

    // indexMetadataBatch should throw
    bool threwOnBatch = false;
    try {
        MetadataOp op{TSMValueType::Float, "m", "f", {}};
        co_await index.indexMetadataBatch({op});
    } catch (const std::runtime_error&) {
        threwOnBatch = true;
    }
    EXPECT_TRUE(threwOnBatch) << "indexMetadataBatch should throw on non-zero shard";

    co_await index.close();
    co_return;
}

// Non-zero shard getSeriesId throws (no db open)
SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, NonZeroShardGetSeriesIdThrows) {
    LevelDBIndex index(1);
    co_await index.open();

    bool threw = false;
    try {
        co_await index.getSeriesId("m", {}, "f");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    EXPECT_TRUE(threw) << "getSeriesId should throw on non-zero shard (no db)";

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// 17. TSDBInsert<int64_t> indexInsert stores "integer" field type
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, IndexInsertInt64StoresIntegerType) {
    LevelDBIndex index(0);
    co_await index.open();

    TSDBInsert<int64_t> insert("counters", "events");
    insert.addTag("service", "auth");
    insert.addValue(1000000000LL, int64_t(42));

    SeriesId128 id = co_await index.indexInsert(insert);
    EXPECT_FALSE(id.isZero());

    auto fieldType = co_await index.getFieldType("counters", "events");
    EXPECT_EQ(fieldType, "integer") << "indexInsert<int64_t> should store type 'integer'";

    // Series should be findable
    auto byTag = co_await index.findSeriesByTag("counters", "service", "auth");
    EXPECT_EQ(byTag.size(), 1u);
    EXPECT_EQ(byTag[0], id);

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// Edge cases: multiple measurements, cross-measurement isolation
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, CrossMeasurementIsolation) {
    LevelDBIndex index(0);
    co_await index.open();

    // Insert into multiple measurements with same tag key names
    co_await index.getOrCreateSeriesId("cpu", {{"host", "srv1"}}, "usage");
    co_await index.getOrCreateSeriesId("memory", {{"host", "srv1"}}, "usage");
    co_await index.getOrCreateSeriesId("disk", {{"host", "srv1"}}, "usage");

    // findSeries should only find series for the requested measurement
    auto cpuSeries = (co_await index.findSeries("cpu")).value();
    EXPECT_EQ(cpuSeries.size(), 1u);

    auto memorySeries = (co_await index.findSeries("memory")).value();
    EXPECT_EQ(memorySeries.size(), 1u);

    auto diskSeries = (co_await index.findSeries("disk")).value();
    EXPECT_EQ(diskSeries.size(), 1u);

    // findSeriesByTag should not cross-contaminate measurements
    auto cpuByHost = co_await index.findSeriesByTag("cpu", "host", "srv1");
    EXPECT_EQ(cpuByHost.size(), 1u);

    auto memByHost = co_await index.findSeriesByTag("memory", "host", "srv1");
    EXPECT_EQ(memByHost.size(), 1u);

    // Fields should be per-measurement
    auto cpuFields = co_await index.getFields("cpu");
    EXPECT_EQ(cpuFields.size(), 1u);

    auto memFields = co_await index.getFields("memory");
    EXPECT_EQ(memFields.size(), 1u);

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// Persistence: data survives close/reopen
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, PersistenceAfterReopenComplex) {
    SeriesId128 savedId;
    {
        LevelDBIndex index(0);
        co_await index.open();

        savedId = co_await index.getOrCreateSeriesId(
            "persist_test",
            {{"region", "us-east"}, {"tier", "web"}},
            "requests");

        co_await index.addField("persist_test", "errors");
        co_await index.addTag("persist_test", "tier", "cache");
        co_await index.setFieldType("persist_test", "requests", "float");

        co_await index.close();
    }

    // Reopen and verify everything persisted
    {
        LevelDBIndex index(0);
        co_await index.open();

        // Series ID should be the same (deterministic)
        SeriesId128 reloadedId = co_await index.getOrCreateSeriesId(
            "persist_test",
            {{"region", "us-east"}, {"tier", "web"}},
            "requests");
        EXPECT_EQ(savedId, reloadedId);

        // Fields should persist (both the one from series creation and addField)
        auto fields = co_await index.getFields("persist_test");
        EXPECT_TRUE(fields.count("requests") > 0);
        EXPECT_TRUE(fields.count("errors") > 0);

        // Tags should persist (both from series creation and addTag)
        auto tagValues = co_await index.getTagValues("persist_test", "tier");
        EXPECT_TRUE(tagValues.count("web") > 0);
        EXPECT_TRUE(tagValues.count("cache") > 0);

        // Field type should persist
        auto fieldType = co_await index.getFieldType("persist_test", "requests");
        EXPECT_EQ(fieldType, "float");

        // Metadata should be intact
        auto meta = co_await index.getSeriesMetadata(savedId);
        EXPECT_TRUE(meta.has_value());
        if (meta.has_value()) {
            EXPECT_EQ(meta->measurement, "persist_test");
            EXPECT_EQ(meta->field, "requests");
        }

        co_await index.close();
    }
    co_return;
}

// ---------------------------------------------------------------------------
// Many series for a single measurement (batch vs. single comparison)
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, IndexMetadataBatchMultipleMeasurements) {
    LevelDBIndex index(0);
    co_await index.open();

    // Mix of measurements in a single batch
    std::vector<MetadataOp> ops;
    for (int i = 0; i < 3; ++i) {
        ops.push_back({TSMValueType::Float, "m1",
                       "field_" + std::to_string(i),
                       {{"host", "h" + std::to_string(i)}}});
        ops.push_back({TSMValueType::Integer, "m2",
                       "count",
                       {{"id", std::to_string(i)}}});
    }

    co_await index.indexMetadataBatch(ops);

    // m1 should have 3 fields
    auto m1Fields = co_await index.getFields("m1");
    EXPECT_EQ(m1Fields.size(), 3u);

    // m2 should have 1 field
    auto m2Fields = co_await index.getFields("m2");
    EXPECT_EQ(m2Fields.size(), 1u);

    // Total series: 3 (m1) + 3 (m2) = 6
    auto count = co_await index.getSeriesCount();
    EXPECT_EQ(count, 6u);

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// getSeriesMetadataBatch on non-zero shard returns all nullopt (not throw)
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, GetSeriesMetadataBatchNonZeroShard) {
    LevelDBIndex index(1); // Non-zero shard — db is null
    co_await index.open();

    SeriesId128 id = SeriesId128::fromSeriesKey("fake_key");
    auto results = co_await index.getSeriesMetadataBatch({id});

    EXPECT_EQ(results.size(), 1u);
    EXPECT_FALSE(results[0].second.has_value())
        << "Non-zero shard should return nullopt for all series in batch";

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// getAllSeriesForMeasurement returns empty for non-zero shard (no throw)
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, GetAllSeriesForMeasurementNonZeroShard) {
    LevelDBIndex index(1);
    co_await index.open();

    auto result = co_await index.getAllSeriesForMeasurement("any_measurement");
    EXPECT_TRUE(result.has_value());
    if (result.has_value()) {
        EXPECT_TRUE(result.value().empty());
    }

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
// Verify that the measurement series cache is updated incrementally
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(LevelDBIndexComprehensiveTest, MeasurementSeriesCacheIncremental) {
    LevelDBIndex index(0);
    co_await index.open();

    // First call populates the cache
    auto id1 = co_await index.getOrCreateSeriesId("cache_incr", {{"k", "v1"}}, "f");
    auto firstScan = (co_await index.getAllSeriesForMeasurement("cache_incr")).value();
    EXPECT_EQ(firstScan.size(), 1u);

    // Adding a second series should update the cached list
    auto id2 = co_await index.getOrCreateSeriesId("cache_incr", {{"k", "v2"}}, "f");
    auto secondScan = (co_await index.getAllSeriesForMeasurement("cache_incr")).value();
    EXPECT_EQ(secondScan.size(), 2u);

    // Both IDs should appear
    std::set<SeriesId128> found(secondScan.begin(), secondScan.end());
    EXPECT_TRUE(found.count(id1) > 0);
    EXPECT_TRUE(found.count(id2) > 0);

    co_await index.close();
    co_return;
}
