#include <gtest/gtest.h>
#include <filesystem>

#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/core/tsdb_value.hpp"

#include <seastar/core/app_template.hh>
#include <seastar/core/coroutine.hh>

class LevelDBIndexTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test index
        std::filesystem::remove_all("shard_999");
    }
    
    void TearDown() override {
        // Clean up test index
        std::filesystem::remove_all("shard_999");
    }
};

seastar::future<> runIndexTest() {
    LevelDBIndex index(999); // Use shard 999 for testing
    
    co_await index.open();
    
    // Test 1: Create a series and get ID
    TSDBInsert<double> tempInsert("weather", "temperature");
    tempInsert.addTag("location", "us-midwest");
    tempInsert.addTag("host", "server-01");
    
    uint64_t seriesId1 = co_await index.indexInsert(tempInsert);
    EXPECT_GT(seriesId1, 0);
    std::cout << "Created series ID: " << seriesId1 << std::endl;
    
    // Test 2: Same series should return same ID
    uint64_t seriesId2 = co_await index.indexInsert(tempInsert);
    EXPECT_EQ(seriesId1, seriesId2);
    
    // Test 3: Different field should get different ID
    TSDBInsert<double> humidityInsert("weather", "humidity");
    humidityInsert.addTag("location", "us-midwest");
    humidityInsert.addTag("host", "server-01");
    
    uint64_t seriesId3 = co_await index.indexInsert(humidityInsert);
    EXPECT_GT(seriesId3, 0);
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
    TSDBInsert<double> tempInsert2("weather", "temperature");
    tempInsert2.addTag("location", "us-west");
    tempInsert2.addTag("host", "server-02");
    
    co_await index.indexInsert(tempInsert2);
    
    locationValues = co_await index.getTagValues("weather", "location");
    EXPECT_EQ(locationValues.size(), 2);
    EXPECT_TRUE(locationValues.count("us-midwest") > 0);
    EXPECT_TRUE(locationValues.count("us-west") > 0);
    
    co_await index.close();
    
    std::cout << "All LevelDB index tests passed!" << std::endl;
}

TEST_F(LevelDBIndexTest, BasicIndexOperations) {
    seastar::app_template app;
    
    auto exitCode = app.run(0, nullptr, [&] {
        return runIndexTest().then([&] {
            seastar::engine().exit(0);
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "Test failed with exception" << std::endl;
            seastar::engine().exit(1);
        });
    });
    
    EXPECT_EQ(exitCode, 0);
}

// Additional integration tests for series ID generation
seastar::future<> testSeriesIdGeneration() {
    LevelDBIndex index(998); // Use shard 998 for this test
    
    co_await index.open();
    
    // Test 1: Multiple series with same measurement, different tags
    std::string measurement = "cpu_usage";
    
    std::map<std::string, std::string> tags1 = {{"host", "server-01"}, {"cpu", "cpu0"}};
    std::map<std::string, std::string> tags2 = {{"host", "server-01"}, {"cpu", "cpu1"}};
    std::map<std::string, std::string> tags3 = {{"host", "server-02"}, {"cpu", "cpu0"}};
    
    uint64_t id1 = co_await index.getOrCreateSeriesId(measurement, tags1, "idle");
    uint64_t id2 = co_await index.getOrCreateSeriesId(measurement, tags2, "idle");
    uint64_t id3 = co_await index.getOrCreateSeriesId(measurement, tags3, "idle");
    
    // All should have different IDs
    EXPECT_NE(id1, id2);
    EXPECT_NE(id1, id3);
    EXPECT_NE(id2, id3);
    
    // Test 2: Same measurement and tags, different fields
    uint64_t id4 = co_await index.getOrCreateSeriesId(measurement, tags1, "system");
    uint64_t id5 = co_await index.getOrCreateSeriesId(measurement, tags1, "user");
    
    // Different fields should get different IDs
    EXPECT_NE(id1, id4);
    EXPECT_NE(id1, id5);
    EXPECT_NE(id4, id5);
    
    // Test 3: Verify series count
    size_t count = co_await index.getSeriesCount();
    EXPECT_EQ(count, 5);
    
    co_await index.close();
    
    // Test 4: Persistence - reopen and verify IDs are consistent
    LevelDBIndex index2(998);
    co_await index2.open();
    
    uint64_t id1_check = co_await index2.getOrCreateSeriesId(measurement, tags1, "idle");
    EXPECT_EQ(id1, id1_check);
    
    uint64_t id4_check = co_await index2.getOrCreateSeriesId(measurement, tags1, "system");
    EXPECT_EQ(id4, id4_check);
    
    co_await index2.close();
    
    std::cout << "Series ID generation tests passed!" << std::endl;
}

TEST_F(LevelDBIndexTest, SeriesIdGeneration) {
    std::filesystem::remove_all("shard_998");
    
    seastar::app_template app;
    
    auto exitCode = app.run(0, nullptr, [&] {
        return testSeriesIdGeneration().then([&] {
            seastar::engine().exit(0);
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "Test failed with exception" << std::endl;
            seastar::engine().exit(1);
        });
    });
    
    std::filesystem::remove_all("shard_998");
    EXPECT_EQ(exitCode, 0);
}

// Test metadata indexing
seastar::future<> testMetadataIndexing() {
    LevelDBIndex index(997); // Use shard 997 for this test
    
    co_await index.open();
    
    // Create multiple series with various combinations
    co_await index.getOrCreateSeriesId("temperature", 
        {{"location", "us-west"}, {"sensor", "temp-01"}}, "value");
    co_await index.getOrCreateSeriesId("temperature",
        {{"location", "us-west"}, {"sensor", "temp-02"}}, "value");
    co_await index.getOrCreateSeriesId("temperature",
        {{"location", "us-east"}, {"sensor", "temp-01"}}, "value");
    co_await index.getOrCreateSeriesId("temperature",
        {{"location", "us-east"}, {"sensor", "temp-01"}}, "humidity");
    
    co_await index.getOrCreateSeriesId("pressure",
        {{"location", "us-west"}, {"sensor", "press-01"}}, "value");
    co_await index.getOrCreateSeriesId("pressure",
        {{"location", "us-central"}}, "value");
    
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
    
    std::cout << "Metadata indexing tests passed!" << std::endl;
}

TEST_F(LevelDBIndexTest, MetadataIndexing) {
    std::filesystem::remove_all("shard_997");
    
    seastar::app_template app;
    
    auto exitCode = app.run(0, nullptr, [&] {
        return testMetadataIndexing().then([&] {
            seastar::engine().exit(0);
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "Test failed with exception" << std::endl;
            seastar::engine().exit(1);
        });
    });
    
    std::filesystem::remove_all("shard_997");
    EXPECT_EQ(exitCode, 0);
}