#include "../../lib/core/timestar_value.hpp"
#include <gtest/gtest.h>

// TimeStarInsert is in the global namespace

TEST(TimeStarInsertTest, ConstructorMoveSemantics) {
    TimeStarInsert<double> insert("cpu", "value");
    EXPECT_EQ(insert.measurement, "cpu");
    EXPECT_EQ(insert.field, "value");
    EXPECT_TRUE(insert.timestamps.empty());
    EXPECT_TRUE(insert.values.empty());
}

TEST(TimeStarInsertTest, AddValueIncrementsSizes) {
    TimeStarInsert<double> insert("cpu", "value");
    insert.addValue(1000, 42.5);
    insert.addValue(2000, 43.0);
    EXPECT_EQ(insert.timestamps.size(), 2u);
    EXPECT_EQ(insert.values.size(), 2u);
    EXPECT_EQ(insert.timestamps[0], 1000u);
    EXPECT_DOUBLE_EQ(insert.values[1], 43.0);
}

TEST(TimeStarInsertTest, SeriesKeyFormat) {
    TimeStarInsert<double> insert("cpu", "value");
    insert.addTag("host", "server01");
    insert.addTag("region", "us-west");
    std::string key = insert.seriesKey();
    // Series key should contain measurement, tags, and field
    EXPECT_NE(key.find("cpu"), std::string::npos);
    EXPECT_NE(key.find("value"), std::string::npos);
    EXPECT_NE(key.find("host"), std::string::npos);
    EXPECT_NE(key.find("server01"), std::string::npos);
}

TEST(TimeStarInsertTest, SeriesKeyCaching) {
    TimeStarInsert<double> insert("cpu", "value");
    insert.addTag("host", "server01");

    std::string key1 = insert.seriesKey();
    std::string key2 = insert.seriesKey();
    // Cached: should return identical string
    EXPECT_EQ(key1, key2);
}

TEST(TimeStarInsertTest, AddTagInvalidatesCache) {
    TimeStarInsert<double> insert("cpu", "value");
    insert.addTag("host", "server01");

    std::string key1 = insert.seriesKey();

    // Adding a tag must invalidate the cache
    insert.addTag("region", "us-west");
    std::string key2 = insert.seriesKey();

    // Keys should differ after adding a tag
    EXPECT_NE(key1, key2);
    // New key should contain the new tag
    EXPECT_NE(key2.find("region"), std::string::npos);
    EXPECT_NE(key2.find("us-west"), std::string::npos);
}

TEST(TimeStarInsertTest, AddTagInvalidatesSeriesId128Cache) {
    TimeStarInsert<double> insert("cpu", "value");
    insert.addTag("host", "server01");

    auto id1 = insert.seriesId128();

    insert.addTag("region", "us-west");
    auto id2 = insert.seriesId128();

    // IDs should differ after tag change
    EXPECT_NE(id1, id2);
}

TEST(TimeStarInsertTest, SetSharedTagsInvalidatesCache) {
    TimeStarInsert<double> insert("cpu", "value");
    insert.addTag("host", "server01");
    std::string key1 = insert.seriesKey();

    auto sharedTags = std::make_shared<const std::map<std::string, std::string>>(
        std::map<std::string, std::string>{{"host", "server02"}});
    insert.setSharedTags(sharedTags);

    std::string key2 = insert.seriesKey();
    EXPECT_NE(key1, key2);
    EXPECT_NE(key2.find("server02"), std::string::npos);
}

TEST(TimeStarInsertTest, SeriesKeyDeterministic) {
    // Two inserts with same data should produce same key
    TimeStarInsert<double> a("cpu", "value");
    a.addTag("host", "server01");

    TimeStarInsert<double> b("cpu", "value");
    b.addTag("host", "server01");

    EXPECT_EQ(a.seriesKey(), b.seriesKey());
    EXPECT_EQ(a.seriesId128(), b.seriesId128());
}

TEST(TimeStarInsertTest, EmptyMeasurementAndField) {
    TimeStarInsert<double> insert("", "");
    // Should not crash
    std::string key = insert.seriesKey();
    EXPECT_FALSE(key.empty());  // At minimum contains the space separator
}

TEST(TimeStarInsertTest, StringType) {
    TimeStarInsert<std::string> insert("logs", "message");
    insert.addValue(1000, "hello world");
    insert.addValue(2000, "goodbye");
    EXPECT_EQ(insert.values.size(), 2u);
    EXPECT_EQ(insert.values[0], "hello world");
}

TEST(TimeStarInsertTest, BoolType) {
    TimeStarInsert<bool> insert("sensor", "active");
    insert.addValue(1000, true);
    insert.addValue(2000, false);
    EXPECT_EQ(insert.values.size(), 2u);
    EXPECT_TRUE(insert.values[0]);
    EXPECT_FALSE(insert.values[1]);
}

TEST(TimeStarInsertTest, Int64Type) {
    TimeStarInsert<int64_t> insert("counter", "requests");
    insert.addValue(1000, 42);
    insert.addValue(2000, -100);
    EXPECT_EQ(insert.values.size(), 2u);
    EXPECT_EQ(insert.values[0], 42);
    EXPECT_EQ(insert.values[1], -100);
}

TEST(TimeStarInsertTest, AddValueDoesNotInvalidateSeriesKey) {
    TimeStarInsert<double> insert("cpu", "value");
    insert.addTag("host", "server01");
    std::string key1 = insert.seriesKey();

    // Adding values should NOT change the series key
    insert.addValue(1000, 42.5);
    std::string key2 = insert.seriesKey();
    EXPECT_EQ(key1, key2);
}
