#include <gtest/gtest.h>
#include <filesystem>

#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/core/timestar_value.hpp"

// =============================================================================
// LevelDB Index Field Type Integration Tests
//
// Verifies that indexInsert() automatically stores field types via setFieldType()
// and that getFieldType() returns the correct type for each template parameter.
// =============================================================================

class LevelDBIndexFieldTypeTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::remove_all("shard_0");
    }

    void TearDown() override {
        std::filesystem::remove_all("shard_0");
    }
};

// Test that indexInsert<double> stores field type as "float"
seastar::future<> testIndexInsertStoresFloatType() {
    LevelDBIndex index(0);
    co_await index.open();

    TimeStarInsert<double> insert("temperature", "value");
    insert.addTag("location", "us-west");
    insert.addValue(1000000000, 23.5);

    co_await index.indexInsert(insert);

    auto fieldType = co_await index.getFieldType("temperature", "value");
    EXPECT_EQ(fieldType, "float") << "indexInsert<double> should store field type as 'float'";

    co_await index.close();
}

TEST_F(LevelDBIndexFieldTypeTest, IndexInsertStoresFloatType) {
    testIndexInsertStoresFloatType().get();
}

// Test that indexInsert<bool> stores field type as "boolean"
seastar::future<> testIndexInsertStoresBooleanType() {
    LevelDBIndex index(0);
    co_await index.open();

    TimeStarInsert<bool> insert("sensor", "is_active");
    insert.addTag("location", "us-east");
    insert.addValue(1000000000, true);

    co_await index.indexInsert(insert);

    auto fieldType = co_await index.getFieldType("sensor", "is_active");
    EXPECT_EQ(fieldType, "boolean") << "indexInsert<bool> should store field type as 'boolean'";

    co_await index.close();
}

TEST_F(LevelDBIndexFieldTypeTest, IndexInsertStoresBooleanType) {
    testIndexInsertStoresBooleanType().get();
}

// Test that indexInsert<std::string> stores field type as "string"
seastar::future<> testIndexInsertStoresStringType() {
    LevelDBIndex index(0);
    co_await index.open();

    TimeStarInsert<std::string> insert("logs", "message");
    insert.addTag("host", "server-01");
    insert.addValue(1000000000, std::string("hello world"));

    co_await index.indexInsert(insert);

    auto fieldType = co_await index.getFieldType("logs", "message");
    EXPECT_EQ(fieldType, "string") << "indexInsert<std::string> should store field type as 'string'";

    co_await index.close();
}

TEST_F(LevelDBIndexFieldTypeTest, IndexInsertStoresStringType) {
    testIndexInsertStoresStringType().get();
}

// Test that multiple fields on the same measurement each get correct types
seastar::future<> testMultipleFieldTypesSameMeasurement() {
    LevelDBIndex index(0);
    co_await index.open();

    // Insert a float field
    TimeStarInsert<double> floatInsert("metrics", "cpu_usage");
    floatInsert.addTag("host", "server-01");
    floatInsert.addValue(1000000000, 75.5);
    co_await index.indexInsert(floatInsert);

    // Insert a boolean field on the same measurement
    TimeStarInsert<bool> boolInsert("metrics", "is_healthy");
    boolInsert.addTag("host", "server-01");
    boolInsert.addValue(1000000000, true);
    co_await index.indexInsert(boolInsert);

    // Insert a string field on the same measurement
    TimeStarInsert<std::string> stringInsert("metrics", "status_message");
    stringInsert.addTag("host", "server-01");
    stringInsert.addValue(1000000000, std::string("all systems go"));
    co_await index.indexInsert(stringInsert);

    // Verify each field type
    auto cpuType = co_await index.getFieldType("metrics", "cpu_usage");
    EXPECT_EQ(cpuType, "float");

    auto healthyType = co_await index.getFieldType("metrics", "is_healthy");
    EXPECT_EQ(healthyType, "boolean");

    auto statusType = co_await index.getFieldType("metrics", "status_message");
    EXPECT_EQ(statusType, "string");

    co_await index.close();
}

TEST_F(LevelDBIndexFieldTypeTest, MultipleFieldTypesSameMeasurement) {
    testMultipleFieldTypesSameMeasurement().get();
}

// Test that calling indexInsert multiple times for the same field doesn't change the type
seastar::future<> testIdempotentFieldType() {
    LevelDBIndex index(0);
    co_await index.open();

    // Insert same field multiple times
    for (int i = 0; i < 5; i++) {
        TimeStarInsert<double> insert("weather", "temperature");
        insert.addTag("location", "us-west");
        insert.addValue(1000000000 + i, 20.0 + i);
        co_await index.indexInsert(insert);
    }

    auto fieldType = co_await index.getFieldType("weather", "temperature");
    EXPECT_EQ(fieldType, "float") << "Repeated indexInsert should not change field type";

    co_await index.close();
}

TEST_F(LevelDBIndexFieldTypeTest, IdempotentFieldType) {
    testIdempotentFieldType().get();
}

// Test that field types persist across index close/reopen
seastar::future<> testFieldTypePersistence() {
    // First session: insert and set types
    {
        LevelDBIndex index(0);
        co_await index.open();

        TimeStarInsert<double> floatInsert("persistent_test", "value");
        floatInsert.addTag("tag", "v1");
        floatInsert.addValue(1000000000, 42.0);
        co_await index.indexInsert(floatInsert);

        TimeStarInsert<bool> boolInsert("persistent_test", "enabled");
        boolInsert.addTag("tag", "v1");
        boolInsert.addValue(1000000000, false);
        co_await index.indexInsert(boolInsert);

        co_await index.close();
    }

    // Second session: reopen and verify types persisted
    {
        LevelDBIndex index(0);
        co_await index.open();

        auto valueType = co_await index.getFieldType("persistent_test", "value");
        EXPECT_EQ(valueType, "float") << "Field type should persist after close/reopen";

        auto enabledType = co_await index.getFieldType("persistent_test", "enabled");
        EXPECT_EQ(enabledType, "boolean") << "Field type should persist after close/reopen";

        co_await index.close();
    }
}

TEST_F(LevelDBIndexFieldTypeTest, FieldTypePersistence) {
    testFieldTypePersistence().get();
}

// Test that a field with no stored type returns empty string (default behavior)
seastar::future<> testUnknownFieldTypeReturnsEmpty() {
    LevelDBIndex index(0);
    co_await index.open();

    // Manually create a series without going through indexInsert
    // (simulating legacy data that existed before field type tracking)
    co_await index.getOrCreateSeriesId("legacy_measurement",
        {{"tag", "value"}}, "some_field");

    // getFieldType should return empty since setFieldType was not called
    // (getOrCreateSeriesId doesn't call setFieldType directly)
    // Note: We don't call indexInsert here, which is what stores the type
    auto fieldType = co_await index.getFieldType("legacy_measurement", "some_field");
    EXPECT_EQ(fieldType, "") << "Field with no stored type should return empty string";

    co_await index.close();
}

TEST_F(LevelDBIndexFieldTypeTest, UnknownFieldTypeReturnsEmpty) {
    testUnknownFieldTypeReturnsEmpty().get();
}
