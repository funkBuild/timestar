#include "../../../lib/index/leveldb_index.hpp"

#include "../../../lib/core/timestar_value.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>

class LevelDBIndexTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test index
        std::filesystem::remove_all("shard_0");
    }

    void TearDown() override {
        // Clean up test index
        std::filesystem::remove_all("shard_0");
    }
};

seastar::future<> runIndexTest() {
    LevelDBIndex index(0);  // Use shard 0 for testing

    co_await index.open();

    // Test 1: Create a series and get ID
    TimeStarInsert<double> tempInsert("weather", "temperature");
    tempInsert.addTag("location", "us-midwest");
    tempInsert.addTag("host", "server-01");

    SeriesId128 seriesId1 = co_await index.indexInsert(tempInsert);

    // Test 2: Same series should return same ID
    SeriesId128 seriesId2 = co_await index.indexInsert(tempInsert);
    EXPECT_EQ(seriesId1, seriesId2);

    // Test 3: Different field should get different ID
    TimeStarInsert<double> humidityInsert("weather", "humidity");
    humidityInsert.addTag("location", "us-midwest");
    humidityInsert.addTag("host", "server-01");

    SeriesId128 seriesId3 = co_await index.indexInsert(humidityInsert);
    EXPECT_NE(seriesId1, seriesId3);

    // Test 4: Check measurement fields
    auto fields = co_await index.getFields("weather");
    EXPECT_EQ(fields.size(), 2);
    EXPECT_TRUE(fields.count("temperature") > 0);
    EXPECT_TRUE(fields.count("humidity") > 0);

    // Test 5: Check measurement tags
    auto tags = co_await index.getTags("weather");
    EXPECT_EQ(tags.size(), 2);
    EXPECT_TRUE(tags.count("location") > 0);
    EXPECT_TRUE(tags.count("host") > 0);

    // Test 6: Check tag values
    auto locationValues = co_await index.getTagValues("weather", "location");
    EXPECT_EQ(locationValues.size(), 1);
    EXPECT_TRUE(locationValues.count("us-midwest") > 0);

    // Test 7: Add another location
    TimeStarInsert<double> tempInsert2("weather", "temperature");
    tempInsert2.addTag("location", "us-west");
    tempInsert2.addTag("host", "server-02");

    co_await index.indexInsert(tempInsert2);

    locationValues = co_await index.getTagValues("weather", "location");
    EXPECT_EQ(locationValues.size(), 2);
    EXPECT_TRUE(locationValues.count("us-midwest") > 0);
    EXPECT_TRUE(locationValues.count("us-west") > 0);

    co_await index.close();
}

TEST_F(LevelDBIndexTest, BasicIndexOperations) {
    runIndexTest().get();
}

// Additional integration tests for series ID generation
seastar::future<> testSeriesIdGeneration() {
    LevelDBIndex index(0);  // Use shard 0 for this test

    co_await index.open();

    // Test 1: Multiple series with same measurement, different tags
    std::string measurement = "cpu_usage";

    std::map<std::string, std::string> tags1 = {{"host", "server-01"}, {"cpu", "cpu0"}};
    std::map<std::string, std::string> tags2 = {{"host", "server-01"}, {"cpu", "cpu1"}};
    std::map<std::string, std::string> tags3 = {{"host", "server-02"}, {"cpu", "cpu0"}};

    SeriesId128 id1 = co_await index.getOrCreateSeriesId(measurement, tags1, "idle");
    SeriesId128 id2 = co_await index.getOrCreateSeriesId(measurement, tags2, "idle");
    SeriesId128 id3 = co_await index.getOrCreateSeriesId(measurement, tags3, "idle");

    // All should have different IDs
    EXPECT_NE(id1, id2);
    EXPECT_NE(id1, id3);
    EXPECT_NE(id2, id3);

    // Test 2: Same measurement and tags, different fields
    SeriesId128 id4 = co_await index.getOrCreateSeriesId(measurement, tags1, "system");
    SeriesId128 id5 = co_await index.getOrCreateSeriesId(measurement, tags1, "user");

    // Different fields should get different IDs
    EXPECT_NE(id1, id4);
    EXPECT_NE(id1, id5);
    EXPECT_NE(id4, id5);

    // Test 3: Verify series count
    size_t count = co_await index.getSeriesCount();
    EXPECT_EQ(count, 5);

    co_await index.close();

    // Test 4: Persistence - reopen and verify IDs are consistent
    LevelDBIndex index2(0);
    co_await index2.open();

    SeriesId128 id1_check = co_await index2.getOrCreateSeriesId(measurement, tags1, "idle");
    EXPECT_EQ(id1, id1_check);

    SeriesId128 id4_check = co_await index2.getOrCreateSeriesId(measurement, tags1, "system");
    EXPECT_EQ(id4, id4_check);

    co_await index2.close();
}

TEST_F(LevelDBIndexTest, SeriesIdGeneration) {
    testSeriesIdGeneration().get();
}

// Test metadata indexing
seastar::future<> testMetadataIndexing() {
    LevelDBIndex index(0);  // Use shard 0 for this test

    co_await index.open();

    // Create multiple series with various combinations
    co_await index.getOrCreateSeriesId("temperature", {{"location", "us-west"}, {"sensor", "temp-01"}}, "value");
    co_await index.getOrCreateSeriesId("temperature", {{"location", "us-west"}, {"sensor", "temp-02"}}, "value");
    co_await index.getOrCreateSeriesId("temperature", {{"location", "us-east"}, {"sensor", "temp-01"}}, "value");
    co_await index.getOrCreateSeriesId("temperature", {{"location", "us-east"}, {"sensor", "temp-01"}}, "humidity");

    co_await index.getOrCreateSeriesId("pressure", {{"location", "us-west"}, {"sensor", "press-01"}}, "value");
    co_await index.getOrCreateSeriesId("pressure", {{"location", "us-central"}}, "value");

    // Test field indexing
    auto tempFields = co_await index.getFields("temperature");
    EXPECT_EQ(tempFields.size(), 2);
    EXPECT_TRUE(tempFields.count("value") > 0);
    EXPECT_TRUE(tempFields.count("humidity") > 0);

    auto pressureFields = co_await index.getFields("pressure");
    EXPECT_EQ(pressureFields.size(), 1);
    EXPECT_TRUE(pressureFields.count("value") > 0);

    // Test tag indexing
    auto tempTags = co_await index.getTags("temperature");
    EXPECT_EQ(tempTags.size(), 2);
    EXPECT_TRUE(tempTags.count("location") > 0);
    EXPECT_TRUE(tempTags.count("sensor") > 0);

    auto pressureTags = co_await index.getTags("pressure");
    EXPECT_EQ(pressureTags.size(), 2);
    EXPECT_TRUE(pressureTags.count("location") > 0);
    EXPECT_TRUE(pressureTags.count("sensor") > 0);

    // Test tag value indexing
    auto tempLocations = co_await index.getTagValues("temperature", "location");
    EXPECT_EQ(tempLocations.size(), 2);
    EXPECT_TRUE(tempLocations.count("us-west") > 0);
    EXPECT_TRUE(tempLocations.count("us-east") > 0);

    auto tempSensors = co_await index.getTagValues("temperature", "sensor");
    EXPECT_EQ(tempSensors.size(), 2);
    EXPECT_TRUE(tempSensors.count("temp-01") > 0);
    EXPECT_TRUE(tempSensors.count("temp-02") > 0);

    auto pressureLocations = co_await index.getTagValues("pressure", "location");
    EXPECT_EQ(pressureLocations.size(), 2);
    EXPECT_TRUE(pressureLocations.count("us-west") > 0);
    EXPECT_TRUE(pressureLocations.count("us-central") > 0);

    co_await index.close();
}

TEST_F(LevelDBIndexTest, MetadataIndexing) {
    testMetadataIndexing().get();
}

// Test series discovery methods
seastar::future<> testFindSeries() {
    LevelDBIndex index(0);

    co_await index.open();

    // Create multiple series with different tag combinations
    auto id1 = co_await index.getOrCreateSeriesId("cpu", {{"host", "server-01"}, {"datacenter", "dc1"}}, "usage");
    auto id2 = co_await index.getOrCreateSeriesId("cpu", {{"host", "server-02"}, {"datacenter", "dc1"}}, "usage");
    auto id3 = co_await index.getOrCreateSeriesId("cpu", {{"host", "server-01"}, {"datacenter", "dc2"}}, "usage");
    auto id4 = co_await index.getOrCreateSeriesId("memory", {{"host", "server-01"}, {"datacenter", "dc1"}}, "usage");

    // Test 1: Find all series for a measurement (no filters)
    auto allCpuSeries = (co_await index.findSeries("cpu")).value();
    EXPECT_EQ(allCpuSeries.size(), 3);

    // Test 2: Find series with single tag filter
    auto dc1Series = (co_await index.findSeries("cpu", {{"datacenter", "dc1"}})).value();
    EXPECT_EQ(dc1Series.size(), 2);

    // Test 3: Find series with multiple tag filters
    auto server01Dc1 = (co_await index.findSeries("cpu", {{"host", "server-01"}, {"datacenter", "dc1"}})).value();
    EXPECT_EQ(server01Dc1.size(), 1);
    EXPECT_EQ(server01Dc1[0], id1);

    // Test 4: Find series with no matches
    auto noMatches = (co_await index.findSeries("cpu", {{"host", "server-99"}})).value();
    EXPECT_EQ(noMatches.size(), 0);

    // Test 5: Find series for different measurement
    auto memSeries = (co_await index.findSeries("memory")).value();
    EXPECT_EQ(memSeries.size(), 1);
    EXPECT_EQ(memSeries[0], id4);

    co_await index.close();
}

TEST_F(LevelDBIndexTest, FindSeries) {
    testFindSeries().get();
}

// Test optimized single-tag lookup
seastar::future<> testFindSeriesByTag() {
    LevelDBIndex index(0);

    co_await index.open();

    // Create series with various tags
    auto id1 = co_await index.getOrCreateSeriesId("requests", {{"endpoint", "/api/users"}, {"method", "GET"}}, "count");
    auto id2 = co_await index.getOrCreateSeriesId("requests", {{"endpoint", "/api/posts"}, {"method", "GET"}}, "count");
    auto id3 =
        co_await index.getOrCreateSeriesId("requests", {{"endpoint", "/api/users"}, {"method", "POST"}}, "count");

    // Test 1: Find by method=GET
    auto getSeries = co_await index.findSeriesByTag("requests", "method", "GET");
    EXPECT_EQ(getSeries.size(), 2);

    // Test 2: Find by endpoint=/api/users
    auto usersSeries = co_await index.findSeriesByTag("requests", "endpoint", "/api/users");
    EXPECT_EQ(usersSeries.size(), 2);

    // Test 3: Find by endpoint=/api/posts
    auto postsSeries = co_await index.findSeriesByTag("requests", "endpoint", "/api/posts");
    EXPECT_EQ(postsSeries.size(), 1);
    EXPECT_EQ(postsSeries[0], id2);

    // Test 4: Find with non-existent tag value
    auto noResults = co_await index.findSeriesByTag("requests", "method", "DELETE");
    EXPECT_EQ(noResults.size(), 0);

    co_await index.close();
}

TEST_F(LevelDBIndexTest, FindSeriesByTag) {
    testFindSeriesByTag().get();
}

// Test grouping series by tag values
seastar::future<> testGetSeriesGroupedByTag() {
    LevelDBIndex index(0);

    co_await index.open();

    // Create series across multiple regions and hosts
    auto id1 = co_await index.getOrCreateSeriesId("disk", {{"region", "us-west"}, {"host", "web-01"}}, "usage");
    auto id2 = co_await index.getOrCreateSeriesId("disk", {{"region", "us-west"}, {"host", "web-02"}}, "usage");
    auto id3 = co_await index.getOrCreateSeriesId("disk", {{"region", "us-east"}, {"host", "web-03"}}, "usage");
    auto id4 = co_await index.getOrCreateSeriesId("disk", {{"region", "us-east"}, {"host", "web-04"}}, "usage");
    auto id5 = co_await index.getOrCreateSeriesId("disk", {{"region", "eu-west"}, {"host", "web-05"}}, "usage");

    // Test 1: Group by region
    auto byRegion = co_await index.getSeriesGroupedByTag("disk", "region");
    EXPECT_EQ(byRegion.size(), 3);
    EXPECT_EQ(byRegion["us-west"].size(), 2);
    EXPECT_EQ(byRegion["us-east"].size(), 2);
    EXPECT_EQ(byRegion["eu-west"].size(), 1);

    // Verify correct series in each group
    EXPECT_TRUE(std::find(byRegion["us-west"].begin(), byRegion["us-west"].end(), id1) != byRegion["us-west"].end());
    EXPECT_TRUE(std::find(byRegion["us-west"].begin(), byRegion["us-west"].end(), id2) != byRegion["us-west"].end());

    // Test 2: Group by host (each host should have 1 series)
    auto byHost = co_await index.getSeriesGroupedByTag("disk", "host");
    EXPECT_EQ(byHost.size(), 5);
    EXPECT_EQ(byHost["web-01"].size(), 1);
    EXPECT_EQ(byHost["web-05"].size(), 1);

    // Test 3: Group by non-existent tag
    auto byNonExistent = co_await index.getSeriesGroupedByTag("disk", "nonexistent");
    EXPECT_EQ(byNonExistent.size(), 0);

    co_await index.close();
}

TEST_F(LevelDBIndexTest, GetSeriesGroupedByTag) {
    testGetSeriesGroupedByTag().get();
}

// Test field type management
seastar::future<> testFieldTypes() {
    LevelDBIndex index(0);

    co_await index.open();

    // Set field types for different measurements
    co_await index.setFieldType("metrics", "cpu_usage", "float");
    co_await index.setFieldType("metrics", "request_count", "integer");
    co_await index.setFieldType("status", "online", "boolean");
    co_await index.setFieldType("logs", "message", "string");

    // Test retrieving field types
    auto cpuType = co_await index.getFieldType("metrics", "cpu_usage");
    EXPECT_EQ(cpuType, "float");

    auto countType = co_await index.getFieldType("metrics", "request_count");
    EXPECT_EQ(countType, "integer");

    auto onlineType = co_await index.getFieldType("status", "online");
    EXPECT_EQ(onlineType, "boolean");

    auto messageType = co_await index.getFieldType("logs", "message");
    EXPECT_EQ(messageType, "string");

    // Test retrieving non-existent field type (should return empty string)
    auto unknownType = co_await index.getFieldType("metrics", "nonexistent");
    EXPECT_EQ(unknownType, "");

    co_await index.close();
}

TEST_F(LevelDBIndexTest, FieldTypes) {
    testFieldTypes().get();
}

// Test field statistics tracking
seastar::future<> testFieldStatistics() {
    LevelDBIndex index(0);

    co_await index.open();

    // Create series and track statistics
    auto seriesId1 = co_await index.getOrCreateSeriesId("temperature", {{"location", "us-west"}}, "value");
    auto seriesId2 = co_await index.getOrCreateSeriesId("temperature", {{"location", "us-east"}}, "value");

    // Update field stats for first series
    LevelDBIndex::FieldStats stats1{
        "float",     // dataType
        1000000000,  // minTime
        2000000000,  // maxTime
        1000         // pointCount
    };
    co_await index.updateFieldStats(seriesId1, "value", stats1);

    // Update field stats for second series
    LevelDBIndex::FieldStats stats2{
        "float",     // dataType
        1500000000,  // minTime
        2500000000,  // maxTime
        500          // pointCount
    };
    co_await index.updateFieldStats(seriesId2, "value", stats2);

    // Retrieve and verify stats
    auto retrieved1 = co_await index.getFieldStats(seriesId1, "value");
    EXPECT_TRUE(retrieved1.has_value());
    if (retrieved1.has_value()) {
        EXPECT_EQ(retrieved1->dataType, "float");
        EXPECT_EQ(retrieved1->minTime, 1000000000);
        EXPECT_EQ(retrieved1->maxTime, 2000000000);
        EXPECT_EQ(retrieved1->pointCount, 1000);
    }

    auto retrieved2 = co_await index.getFieldStats(seriesId2, "value");
    EXPECT_TRUE(retrieved2.has_value());
    if (retrieved2.has_value()) {
        EXPECT_EQ(retrieved2->pointCount, 500);
    }

    // Test retrieving non-existent stats
    SeriesId128 nonExistentId = SeriesId128::fromSeriesKey("nonexistent.series");
    auto noStats = co_await index.getFieldStats(nonExistentId, "value");
    EXPECT_FALSE(noStats.has_value());

    co_await index.close();
}

TEST_F(LevelDBIndexTest, FieldStatistics) {
    testFieldStatistics().get();
}

// Test series metadata retrieval
seastar::future<> testSeriesMetadata() {
    LevelDBIndex index(0);

    co_await index.open();

    // Create series and retrieve metadata
    std::map<std::string, std::string> tags1 = {
        {"host", "web-server-01"}, {"region", "us-west-2"}, {"environment", "production"}};

    auto seriesId1 = co_await index.getOrCreateSeriesId("http_requests", tags1, "count");

    // Retrieve metadata
    auto metadata1 = co_await index.getSeriesMetadata(seriesId1);
    EXPECT_TRUE(metadata1.has_value());
    if (metadata1.has_value()) {
        EXPECT_EQ(metadata1->measurement, "http_requests");
        EXPECT_EQ(metadata1->field, "count");
        EXPECT_EQ(metadata1->tags.size(), 3);
        EXPECT_EQ(metadata1->tags["host"], "web-server-01");
        EXPECT_EQ(metadata1->tags["region"], "us-west-2");
        EXPECT_EQ(metadata1->tags["environment"], "production");
    }

    // Create another series
    std::map<std::string, std::string> tags2 = {{"sensor", "temp-01"}};

    auto seriesId2 = co_await index.getOrCreateSeriesId("temperature", tags2, "celsius");

    auto metadata2 = co_await index.getSeriesMetadata(seriesId2);
    EXPECT_TRUE(metadata2.has_value());
    if (metadata2.has_value()) {
        EXPECT_EQ(metadata2->measurement, "temperature");
        EXPECT_EQ(metadata2->field, "celsius");
        EXPECT_EQ(metadata2->tags.size(), 1);
        EXPECT_EQ(metadata2->tags["sensor"], "temp-01");
    }

    // Test retrieving non-existent metadata
    SeriesId128 nonExistentId = SeriesId128::fromSeriesKey("nonexistent.series");
    auto noMetadata = co_await index.getSeriesMetadata(nonExistentId);
    EXPECT_FALSE(noMetadata.has_value());

    co_await index.close();
}

TEST_F(LevelDBIndexTest, SeriesMetadata) {
    testSeriesMetadata().get();
}

// Test getAllMeasurements
seastar::future<> testGetAllMeasurements() {
    LevelDBIndex index(0);

    co_await index.open();

    // Create series across different measurements
    co_await index.getOrCreateSeriesId("cpu", {{"host", "server1"}}, "usage");
    co_await index.getOrCreateSeriesId("memory", {{"host", "server1"}}, "usage");
    co_await index.getOrCreateSeriesId("disk", {{"host", "server1"}}, "usage");
    co_await index.getOrCreateSeriesId("cpu", {{"host", "server2"}}, "usage");  // Same measurement, different tags
    co_await index.getOrCreateSeriesId("network", {{"interface", "eth0"}}, "bytes_sent");

    // Get all measurements
    auto measurements = co_await index.getAllMeasurements();

    // Should have 4 unique measurements
    EXPECT_EQ(measurements.size(), 4);
    EXPECT_TRUE(measurements.count("cpu") > 0);
    EXPECT_TRUE(measurements.count("memory") > 0);
    EXPECT_TRUE(measurements.count("disk") > 0);
    EXPECT_TRUE(measurements.count("network") > 0);

    co_await index.close();
}

TEST_F(LevelDBIndexTest, GetAllMeasurements) {
    testGetAllMeasurements().get();
}

// Test open/close lifecycle to verify filter policy RAII cleanup (no leaks)
seastar::future<> testOpenCloseLifecycle() {
    // Cycle 1: open, use, close
    {
        LevelDBIndex index(0);
        co_await index.open();

        auto id = co_await index.getOrCreateSeriesId("lifecycle_test", {{"tag", "value1"}}, "field1");
        EXPECT_TRUE(id.toHex().size() > 0);

        co_await index.close();
    }

    // Cycle 2: reopen same path, use, close
    {
        LevelDBIndex index(0);
        co_await index.open();

        // Previously created series should still exist
        auto id = co_await index.getOrCreateSeriesId("lifecycle_test", {{"tag", "value1"}}, "field1");
        EXPECT_TRUE(id.toHex().size() > 0);

        auto fields = co_await index.getFields("lifecycle_test");
        EXPECT_EQ(fields.size(), 1);

        co_await index.close();
    }

    // Cycle 3: open and let destructor handle cleanup (no explicit close)
    {
        LevelDBIndex index(0);
        co_await index.open();

        co_await index.getOrCreateSeriesId("lifecycle_test", {{"tag", "value2"}}, "field2");

        // Destructor should clean up db, filter policy, etc. without leaking
    }

    // Cycle 4: reopen after destructor-based cleanup to verify integrity
    {
        LevelDBIndex index(0);
        co_await index.open();

        auto fields = co_await index.getFields("lifecycle_test");
        EXPECT_EQ(fields.size(), 2);
        EXPECT_TRUE(fields.count("field1") > 0);
        EXPECT_TRUE(fields.count("field2") > 0);

        co_await index.close();
    }
}

TEST_F(LevelDBIndexTest, OpenCloseLifecycle) {
    testOpenCloseLifecycle().get();
}
