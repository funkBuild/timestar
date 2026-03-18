#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/index/schema_update.hpp"

#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <filesystem>
#include <unordered_set>

using namespace timestar::index;

// =============================================================================
// Bug-fix regression tests for NativeIndex
//
// Bug 1: applySchemaUpdate field type immutability (first-write-wins)
// Bug 2: getFieldStats uncaught parse exceptions on corrupt data
// Bug 3: Non-deterministic discovery cache key from unordered_set iteration
// =============================================================================

class NativeIndexBugfixTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

// ---------------------------------------------------------------------------
// Bug 1: applySchemaUpdate must not overwrite existing field types
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(NativeIndexBugfixTest, ApplySchemaUpdateFieldTypeImmutable) {
    NativeIndex index(0);
    co_await index.open();

    // First update: set "meas\0field" to "float"
    SchemaUpdate first;
    std::string key = "meas";
    key.push_back('\0');
    key += "value";
    first.newFieldTypes[key] = "float";
    index.applySchemaUpdate(first);

    // Second update: attempt to overwrite with "boolean" (lexicographically smaller)
    SchemaUpdate second;
    second.newFieldTypes[key] = "boolean";
    index.applySchemaUpdate(second);

    // The type must remain "float" (first-write-wins)
    auto fieldType = co_await index.getFieldType("meas", "value");
    EXPECT_EQ(fieldType, "float") << "Field type must be immutable after first write";

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexBugfixTest, ApplySchemaUpdateFieldTypeAcceptsNewKey) {
    NativeIndex index(0);
    co_await index.open();

    // First key
    SchemaUpdate u1;
    std::string key1 = "m1";
    key1.push_back('\0');
    key1 += "f1";
    u1.newFieldTypes[key1] = "float";
    index.applySchemaUpdate(u1);

    // Second, different key
    SchemaUpdate u2;
    std::string key2 = "m1";
    key2.push_back('\0');
    key2 += "f2";
    u2.newFieldTypes[key2] = "string";
    index.applySchemaUpdate(u2);

    auto t1 = co_await index.getFieldType("m1", "f1");
    auto t2 = co_await index.getFieldType("m1", "f2");
    EXPECT_EQ(t1, "float");
    EXPECT_EQ(t2, "string");

    co_await index.close();
}

// Test that SchemaUpdate::merge also uses first-write-wins
TEST_F(NativeIndexBugfixTest, SchemaUpdateMergeFieldTypeImmutable) {
    SchemaUpdate base;
    std::string key = "meas";
    key.push_back('\0');
    key += "temp";
    base.newFieldTypes[key] = "float";

    SchemaUpdate other;
    other.newFieldTypes[key] = "boolean";  // "boolean" < "float" lexicographically

    base.merge(other);
    EXPECT_EQ(base.newFieldTypes[key], "float") << "merge must preserve first-write type";
}

TEST_F(NativeIndexBugfixTest, SchemaUpdateMergeAcceptsNewKey) {
    SchemaUpdate base;
    std::string key1 = "m";
    key1.push_back('\0');
    key1 += "f1";
    base.newFieldTypes[key1] = "float";

    SchemaUpdate other;
    std::string key2 = "m";
    key2.push_back('\0');
    key2 += "f2";
    other.newFieldTypes[key2] = "string";

    base.merge(other);
    EXPECT_EQ(base.newFieldTypes[key1], "float");
    EXPECT_EQ(base.newFieldTypes[key2], "string");
}

// ---------------------------------------------------------------------------
// Bug 2: getFieldStats must not throw on corrupt data
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(NativeIndexBugfixTest, GetFieldStatsValidData) {
    NativeIndex index(0);
    co_await index.open();

    // Create a series to get a valid SeriesId128
    auto sid = co_await index.getOrCreateSeriesId("stats_test", {{"tag", "v1"}}, "temperature");

    IndexFieldStats stats;
    stats.dataType = "float";
    stats.minTime = 1000000000;
    stats.maxTime = 2000000000;
    stats.pointCount = 42;
    co_await index.updateFieldStats(sid, "temperature", stats);

    auto result = co_await index.getFieldStats(sid, "temperature");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->dataType, "float");
    EXPECT_EQ(result->minTime, 1000000000);
    EXPECT_EQ(result->maxTime, 2000000000);
    EXPECT_EQ(result->pointCount, 42u);

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexBugfixTest, GetFieldStatsNonexistentReturnsNullopt) {
    NativeIndex index(0);
    co_await index.open();

    SeriesId128 fakeSid;
    auto result = co_await index.getFieldStats(fakeSid, "no_such_field");
    EXPECT_FALSE(result.has_value());

    co_await index.close();
}

// ---------------------------------------------------------------------------
// Bug 3: Discovery cache key must be deterministic for unordered_set fields
// ---------------------------------------------------------------------------

SEASTAR_TEST_F(NativeIndexBugfixTest, DiscoveryCacheKeyDeterministic) {
    NativeIndex index(0);
    co_await index.open();

    // Create some series with multiple fields to populate the index
    TimeStarInsert<double> ins1("dcache_test", "alpha");
    ins1.addTag("host", "a");
    ins1.addValue(1000000000, 1.0);
    co_await index.indexInsert(ins1);

    TimeStarInsert<double> ins2("dcache_test", "beta");
    ins2.addTag("host", "a");
    ins2.addValue(1000000000, 2.0);
    co_await index.indexInsert(ins2);

    TimeStarInsert<double> ins3("dcache_test", "gamma");
    ins3.addTag("host", "a");
    ins3.addValue(1000000000, 3.0);
    co_await index.indexInsert(ins3);

    std::map<std::string, std::string> tags{{"host", "a"}};

    // Call findSeriesWithMetadataCached twice with the same field set
    // (unordered_set iteration order may vary, but the cache key must be stable)
    std::unordered_set<std::string> fields1{"gamma", "alpha", "beta"};
    auto r1 = co_await index.findSeriesWithMetadataCached("dcache_test", tags, fields1);
    EXPECT_TRUE(r1.has_value());

    // Second call with the same logical set should hit the cache
    std::unordered_set<std::string> fields2{"beta", "gamma", "alpha"};
    auto r2 = co_await index.findSeriesWithMetadataCached("dcache_test", tags, fields2);
    EXPECT_TRUE(r2.has_value());

    // Both results must be identical (same shared_ptr if cache hit)
    EXPECT_EQ(r1.value().get(), r2.value().get())
        << "Same field set with different insertion order must produce same cache key";

    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexBugfixTest, DiscoveryCacheDifferentFieldSetsMiss) {
    NativeIndex index(0);
    co_await index.open();

    TimeStarInsert<double> ins1("dcache2", "x");
    ins1.addTag("t", "v");
    ins1.addValue(1000000000, 1.0);
    co_await index.indexInsert(ins1);

    TimeStarInsert<double> ins2("dcache2", "y");
    ins2.addTag("t", "v");
    ins2.addValue(1000000000, 2.0);
    co_await index.indexInsert(ins2);

    std::map<std::string, std::string> tags{{"t", "v"}};

    // Different field sets should NOT share a cache entry
    std::unordered_set<std::string> set1{"x"};
    auto r1 = co_await index.findSeriesWithMetadataCached("dcache2", tags, set1);
    EXPECT_TRUE(r1.has_value());

    std::unordered_set<std::string> set2{"x", "y"};
    auto r2 = co_await index.findSeriesWithMetadataCached("dcache2", tags, set2);
    EXPECT_TRUE(r2.has_value());

    // Different field sets should not alias to same pointer
    EXPECT_NE(r1.value().get(), r2.value().get())
        << "Different field sets must produce different cache keys";

    co_await index.close();
}
