/*
 * Tests for atomic WriteBatch in LevelDB index operations (Task #52).
 *
 * Before the fix, getOrCreateSeriesId() performed two separate write
 * operations:
 *   1. A WriteBatch for series metadata, TAG_INDEX, and GROUP_BY_INDEX entries.
 *   2. A separate write (via addFieldsAndTags) for MEASUREMENT_FIELDS,
 *      MEASUREMENT_TAGS, and TAG_VALUES entries.
 *
 * A failure or crash between these two writes would leave the index in a
 * partially-written state: series metadata would exist with no corresponding
 * field/tag metadata.  The fix consolidates both into a single WriteBatch
 * so that all index keys for a new series are committed atomically.
 *
 * These tests verify that after getOrCreateSeriesId() returns, EVERY
 * expected index key is present and consistent.  Any partial write would
 * cause one or more assertions to fail.
 *
 * NOTE: ASSERT_* macros cannot be used inside Seastar coroutines because they
 * emit a bare `return` statement, which is invalid in coroutine context.
 * Use EXPECT_* throughout.
 */

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/index/leveldb_index.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

class LevelDBIndexAtomicWriteTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::remove_all("shard_0"); }

    void TearDown() override { std::filesystem::remove_all("shard_0"); }
};

// After getOrCreateSeriesId, ALL index keys for the series must exist.
// Before the fix, MEASUREMENT_FIELDS / MEASUREMENT_TAGS / TAG_VALUES were
// written in a separate call after the series-metadata batch, so a failure
// between the two writes would leave them missing.
SEASTAR_TEST_F(LevelDBIndexAtomicWriteTest, AllIndexKeysWrittenAtomically) {
    LevelDBIndex index(0);
    co_await index.open();

    const std::string measurement = "weather";
    const std::string field = "temperature";
    std::map<std::string, std::string> tags = {{"location", "us-west"}, {"host", "server-01"}};

    SeriesId128 seriesId = co_await index.getOrCreateSeriesId(measurement, tags, field);
    EXPECT_FALSE(seriesId.isZero()) << "Series ID must be non-zero after creation";

    // 1. Series metadata must exist (SERIES_METADATA key).
    auto meta = co_await index.getSeriesMetadata(seriesId);
    EXPECT_TRUE(meta.has_value()) << "SERIES_METADATA key is missing after getOrCreateSeriesId";
    if (meta.has_value()) {
        EXPECT_EQ(meta->measurement, measurement);
        EXPECT_EQ(meta->field, field);
        EXPECT_EQ(meta->tags, tags);
    }

    // 2. MEASUREMENT_FIELDS must include the new field.
    auto fields = co_await index.getFields(measurement);
    EXPECT_GT(fields.count(field), 0u) << "MEASUREMENT_FIELDS entry missing field '" << field << "' after atomic write";

    // 3. MEASUREMENT_TAGS must include each tag key.
    auto tagKeys = co_await index.getTags(measurement);
    for (const auto& [k, v] : tags) {
        EXPECT_GT(tagKeys.count(k), 0u) << "MEASUREMENT_TAGS entry missing tag key '" << k << "' after atomic write";
    }

    // 4. TAG_VALUES must include each tag value.
    for (const auto& [k, v] : tags) {
        auto tagValues = co_await index.getTagValues(measurement, k);
        EXPECT_GT(tagValues.count(v), 0u)
            << "TAG_VALUES entry missing value '" << v << "' for key '" << k << "' after atomic write";
    }

    // 5. findSeriesByTag must discover the series via the TAG_INDEX entry.
    for (const auto& [k, v] : tags) {
        auto found = co_await index.findSeriesByTag(measurement, k, v);
        bool containsId = false;
        for (const auto& id : found) {
            if (id == seriesId) {
                containsId = true;
                break;
            }
        }
        EXPECT_TRUE(containsId) << "TAG_INDEX entry missing for " << k << "=" << v << " after atomic write";
    }

    // 6. MEASUREMENT_SERIES index must contain the series ID.
    auto allSeriesResult = co_await index.getAllSeriesForMeasurement(measurement);
    EXPECT_TRUE(allSeriesResult.has_value());
    if (allSeriesResult.has_value()) {
        bool foundInMeasurement = false;
        for (const auto& id : allSeriesResult.value()) {
            if (id == seriesId) {
                foundInMeasurement = true;
                break;
            }
        }
        EXPECT_TRUE(foundInMeasurement) << "MEASUREMENT_SERIES entry missing after atomic write";
    }

    co_await index.close();
}

// Multiple series in the same measurement: verify that fields and tag sets
// accumulate correctly after each getOrCreateSeriesId call.
SEASTAR_TEST_F(LevelDBIndexAtomicWriteTest, MultipleSeriesFieldsAndTagsConsistent) {
    LevelDBIndex index(0);
    co_await index.open();

    const std::string measurement = "cpu";

    // First series
    co_await index.getOrCreateSeriesId(measurement, {{"host", "h1"}, {"region", "us-east"}}, "usage");

    // Second series, same measurement, different tags and field
    co_await index.getOrCreateSeriesId(measurement, {{"host", "h2"}, {"region", "eu-west"}}, "idle");

    // Both fields must appear in MEASUREMENT_FIELDS
    auto fields = co_await index.getFields(measurement);
    EXPECT_GT(fields.count("usage"), 0u) << "Field 'usage' missing from MEASUREMENT_FIELDS";
    EXPECT_GT(fields.count("idle"), 0u) << "Field 'idle' missing from MEASUREMENT_FIELDS";

    // Both tag keys must appear in MEASUREMENT_TAGS
    auto tagKeys = co_await index.getTags(measurement);
    EXPECT_GT(tagKeys.count("host"), 0u) << "Tag key 'host' missing from MEASUREMENT_TAGS";
    EXPECT_GT(tagKeys.count("region"), 0u) << "Tag key 'region' missing from MEASUREMENT_TAGS";

    // TAG_VALUES for 'host' must contain both values
    auto hostValues = co_await index.getTagValues(measurement, "host");
    EXPECT_GT(hostValues.count("h1"), 0u) << "TAG_VALUES missing 'h1' for 'host'";
    EXPECT_GT(hostValues.count("h2"), 0u) << "TAG_VALUES missing 'h2' for 'host'";

    // TAG_VALUES for 'region' must contain both values
    auto regionValues = co_await index.getTagValues(measurement, "region");
    EXPECT_GT(regionValues.count("us-east"), 0u) << "TAG_VALUES missing 'us-east' for 'region'";
    EXPECT_GT(regionValues.count("eu-west"), 0u) << "TAG_VALUES missing 'eu-west' for 'region'";

    co_await index.close();
}

// Calling getOrCreateSeriesId for the same series twice must be idempotent:
// all index keys must still be correct after the second (no-op) call.
SEASTAR_TEST_F(LevelDBIndexAtomicWriteTest, IdempotentSecondCall) {
    LevelDBIndex index(0);
    co_await index.open();

    const std::string measurement = "disk";
    const std::string field = "used";
    std::map<std::string, std::string> tags = {{"device", "sda"}, {"host", "srv01"}};

    SeriesId128 id1 = co_await index.getOrCreateSeriesId(measurement, tags, field);
    SeriesId128 id2 = co_await index.getOrCreateSeriesId(measurement, tags, field);

    EXPECT_EQ(id1, id2) << "Repeated getOrCreateSeriesId must return same ID";

    // Metadata must still be intact after the second call
    auto meta = co_await index.getSeriesMetadata(id1);
    EXPECT_TRUE(meta.has_value());
    if (meta.has_value()) {
        EXPECT_EQ(meta->measurement, measurement);
        EXPECT_EQ(meta->field, field);
    }

    auto fields = co_await index.getFields(measurement);
    EXPECT_GT(fields.count(field), 0u);

    auto tagKeys = co_await index.getTags(measurement);
    EXPECT_GT(tagKeys.count("device"), 0u);
    EXPECT_GT(tagKeys.count("host"), 0u);

    co_await index.close();
}

// Verify that indexInsert also writes field type and all metadata keys
// in a fully consistent state (no separate write that could partially fail).
SEASTAR_TEST_F(LevelDBIndexAtomicWriteTest, IndexInsertConsistency) {
    LevelDBIndex index(0);
    co_await index.open();

    TimeStarInsert<double> insert("metrics", "latency");
    insert.addTag("service", "api");
    insert.addTag("env", "prod");

    SeriesId128 id = co_await index.indexInsert(insert);
    EXPECT_FALSE(id.isZero());

    // Series metadata must exist
    auto meta = co_await index.getSeriesMetadata(id);
    EXPECT_TRUE(meta.has_value()) << "SERIES_METADATA missing after indexInsert";

    // Field must exist in MEASUREMENT_FIELDS
    auto fields = co_await index.getFields("metrics");
    EXPECT_GT(fields.count("latency"), 0u) << "MEASUREMENT_FIELDS missing 'latency'";

    // Tag keys in MEASUREMENT_TAGS
    auto tagKeys = co_await index.getTags("metrics");
    EXPECT_GT(tagKeys.count("service"), 0u) << "MEASUREMENT_TAGS missing 'service'";
    EXPECT_GT(tagKeys.count("env"), 0u) << "MEASUREMENT_TAGS missing 'env'";

    // Tag values in TAG_VALUES
    auto svcValues = co_await index.getTagValues("metrics", "service");
    EXPECT_GT(svcValues.count("api"), 0u) << "TAG_VALUES missing 'api' for 'service'";

    auto envValues = co_await index.getTagValues("metrics", "env");
    EXPECT_GT(envValues.count("prod"), 0u) << "TAG_VALUES missing 'prod' for 'env'";

    // Field type must be recorded
    auto fieldType = co_await index.getFieldType("metrics", "latency");
    EXPECT_EQ(fieldType, "float") << "Field type missing or wrong after indexInsert";

    co_await index.close();
}
