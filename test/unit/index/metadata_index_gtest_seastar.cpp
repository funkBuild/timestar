/*
 * Google Test + Seastar integration for MetadataIndex tests
 * 
 * This test file uses Google Test framework with a Seastar runtime wrapper
 * to properly test async code with futures and coroutines.
 */

#include <gtest/gtest.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include "../../../lib/index/metadata_index.hpp"
#include <filesystem>
#include <functional>

using namespace seastar;

static const std::string TEST_DB_PATH = "/tmp/test_metadata_index_gtest";

// Helper to clean up test database
static void cleanup_test_db() {
    std::filesystem::remove_all(TEST_DB_PATH);
}

// Wrapper to run Seastar code within Google Test
// This creates a minimal Seastar runtime for each test
static int run_in_seastar(std::function<seastar::future<>()> func) {
    // Create minimal argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;
    
    seastar::app_template app;
    try {
        return app.run(argc, argv, [func = std::move(func)]() {
            return func().handle_exception([](std::exception_ptr ep) {
                try {
                    std::rethrow_exception(ep);
                } catch (const std::exception& e) {
                    std::cerr << "Test failed with exception: " << e.what() << std::endl;
                    return make_exception_future<>(ep);
                }
            });
        });
    } catch (const std::exception& e) {
        std::cerr << "Failed to run Seastar app: " << e.what() << std::endl;
        return 1;
    }
}

// Test fixture
class MetadataIndexAsyncTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanup_test_db();
    }
    
    void TearDown() override {
        cleanup_test_db();
    }
};

TEST_F(MetadataIndexAsyncTest, SeriesMetadataSerialization) {
    SeriesMetadata original;
    original.seriesId = 12345;
    original.measurement = "temperature";
    original.tags = {{"location", "room1"}, {"sensor", "temp01"}};
    original.fields = {"value", "quality"};
    original.minTime = 1000000;
    original.maxTime = 2000000;
    original.shardId = 5;
    
    std::string serialized = original.serialize();
    ASSERT_FALSE(serialized.empty());
    
    SeriesMetadata deserialized = SeriesMetadata::deserialize(serialized);
    
    EXPECT_EQ(deserialized.seriesId, original.seriesId);
    EXPECT_EQ(deserialized.measurement, original.measurement);
    EXPECT_EQ(deserialized.tags, original.tags);
    EXPECT_EQ(deserialized.fields, original.fields);
    EXPECT_EQ(deserialized.minTime, original.minTime);
    EXPECT_EQ(deserialized.maxTime, original.maxTime);
    EXPECT_EQ(deserialized.shardId, original.shardId);
}

TEST_F(MetadataIndexAsyncTest, SeriesKeyGeneration) {
    std::string measurement = "temperature";
    std::map<std::string, std::string> tags = {
        {"location", "room1"},
        {"sensor", "temp01"}
    };
    std::string field = "value";
    
    std::string key1 = SeriesMetadata::generateSeriesKey(measurement, tags, field);
    std::string key2 = SeriesMetadata::generateSeriesKey(measurement, tags, field);
    
    // Same inputs should generate same key
    EXPECT_EQ(key1, key2);
    
    // Different field should generate different key
    std::string key3 = SeriesMetadata::generateSeriesKey(measurement, tags, "other_field");
    EXPECT_NE(key1, key3);
    
    // Different tags should generate different key
    std::map<std::string, std::string> tags2 = {{"location", "room2"}};
    std::string key4 = SeriesMetadata::generateSeriesKey(measurement, tags2, field);
    EXPECT_NE(key1, key4);
}

TEST_F(MetadataIndexAsyncTest, GetOrCreateSeriesId) {
    int result = run_in_seastar([]() -> seastar::future<> {
        auto index = std::make_unique<MetadataIndex>(TEST_DB_PATH);
        co_await index->init();
        
        std::string measurement = "temperature";
        std::map<std::string, std::string> tags = {
            {"location", "room1"},
            {"sensor", "temp01"}
        };
        std::string field = "value";
        
        // First call should create new series
        uint64_t id1 = co_await index->getOrCreateSeriesId(measurement, tags, field);
        EXPECT_GT(id1, 0);
        
        // Second call with same parameters should return same ID
        uint64_t id2 = co_await index->getOrCreateSeriesId(measurement, tags, field);
        EXPECT_EQ(id1, id2);
        
        // Different field should create new series
        uint64_t id3 = co_await index->getOrCreateSeriesId(measurement, tags, "humidity");
        EXPECT_NE(id1, id3);
        
        // Different tags should create new series
        std::map<std::string, std::string> tags2 = {{"location", "room2"}};
        uint64_t id4 = co_await index->getOrCreateSeriesId(measurement, tags2, field);
        EXPECT_NE(id1, id4);
        
        co_await index->close();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(MetadataIndexAsyncTest, FindSeriesByMeasurement) {
    int result = run_in_seastar([]() -> seastar::future<> {
        auto index = std::make_unique<MetadataIndex>(TEST_DB_PATH);
        co_await index->init();
        
        std::string measurement = "temperature";
        std::vector<uint64_t> createdIds;
        
        // Create multiple series for same measurement
        for (int i = 0; i < 5; i++) {
            std::map<std::string, std::string> tags = {
                {"location", "room" + std::to_string(i)}
            };
            uint64_t id = co_await index->getOrCreateSeriesId(measurement, tags, "value");
            createdIds.push_back(id);
        }
        
        // Create series for different measurement
        co_await index->getOrCreateSeriesId("humidity", {{"location", "room1"}}, "value");
        
        // Find all temperature series
        std::vector<uint64_t> foundIds = co_await index->findSeriesByMeasurement("temperature");
        
        EXPECT_EQ(foundIds.size(), 5);
        
        // Check all created IDs are found
        std::sort(createdIds.begin(), createdIds.end());
        std::sort(foundIds.begin(), foundIds.end());
        EXPECT_EQ(createdIds, foundIds);
        
        co_await index->close();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(MetadataIndexAsyncTest, FindSeriesByTag) {
    int result = run_in_seastar([]() -> seastar::future<> {
        auto index = std::make_unique<MetadataIndex>(TEST_DB_PATH);
        co_await index->init();
        
        std::string measurement = "temperature";
        
        // Create series with different tag values
        std::map<std::string, std::string> tags1 = {
            {"location", "room1"},
            {"sensor", "temp01"}
        };
        uint64_t id1 = co_await index->getOrCreateSeriesId(measurement, tags1, "value");
        
        std::map<std::string, std::string> tags2 = {
            {"location", "room1"},
            {"sensor", "temp02"}
        };
        uint64_t id2 = co_await index->getOrCreateSeriesId(measurement, tags2, "value");
        
        std::map<std::string, std::string> tags3 = {
            {"location", "room2"},
            {"sensor", "temp01"}
        };
        uint64_t id3 = co_await index->getOrCreateSeriesId(measurement, tags3, "value");
        
        // Find series by location=room1
        std::vector<uint64_t> foundIds = co_await index->findSeriesByTag(
            measurement, "location", "room1");
        
        EXPECT_EQ(foundIds.size(), 2);
        std::sort(foundIds.begin(), foundIds.end());
        std::vector<uint64_t> expected = {id1, id2};
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(foundIds, expected);
        
        co_await index->close();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(MetadataIndexAsyncTest, FindSeriesByTags) {
    int result = run_in_seastar([]() -> seastar::future<> {
        auto index = std::make_unique<MetadataIndex>(TEST_DB_PATH);
        co_await index->init();
        
        std::string measurement = "temperature";
        
        // Create series with various tag combinations
        std::map<std::string, std::string> tags1 = {
            {"location", "room1"},
            {"sensor", "temp01"}
        };
        uint64_t id1 = co_await index->getOrCreateSeriesId(measurement, tags1, "value");
        
        std::map<std::string, std::string> tags2 = {
            {"location", "room1"},
            {"sensor", "temp02"}
        };
        co_await index->getOrCreateSeriesId(measurement, tags2, "value");
        
        std::map<std::string, std::string> tags3 = {
            {"location", "room2"},
            {"sensor", "temp01"}
        };
        co_await index->getOrCreateSeriesId(measurement, tags3, "value");
        
        // Find series with exact tag match
        std::vector<uint64_t> foundIds = co_await index->findSeriesByTags(measurement, tags1);
        
        EXPECT_EQ(foundIds.size(), 1);
        EXPECT_EQ(foundIds[0], id1);
        
        co_await index->close();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(MetadataIndexAsyncTest, GetSeriesMetadata) {
    int result = run_in_seastar([]() -> seastar::future<> {
        auto index = std::make_unique<MetadataIndex>(TEST_DB_PATH);
        co_await index->init();
        
        std::string measurement = "temperature";
        std::map<std::string, std::string> tags = {
            {"location", "room1"},
            {"sensor", "temp01"}
        };
        std::string field = "value";
        
        // Create series
        uint64_t seriesId = co_await index->getOrCreateSeriesId(measurement, tags, field);
        
        // Get metadata
        auto metadata = co_await index->getSeriesMetadata(seriesId);
        
        EXPECT_TRUE(metadata.has_value());
        EXPECT_EQ(metadata->seriesId, seriesId);
        EXPECT_EQ(metadata->measurement, measurement);
        EXPECT_EQ(metadata->tags, tags);
        EXPECT_EQ(metadata->fields.size(), 1);
        EXPECT_EQ(metadata->fields[0], field);
        
        // Non-existent series should return empty
        auto metadata2 = co_await index->getSeriesMetadata(999999);
        EXPECT_FALSE(metadata2.has_value());
        
        co_await index->close();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(MetadataIndexAsyncTest, Persistence) {
    uint64_t originalId = 0;
    
    // Create series and close index
    int result1 = run_in_seastar([&originalId]() -> seastar::future<> {
        auto index = std::make_unique<MetadataIndex>(TEST_DB_PATH);
        co_await index->init();
        
        std::string measurement = "temperature";
        std::map<std::string, std::string> tags = {{"location", "room1"}};
        
        originalId = co_await index->getOrCreateSeriesId(measurement, tags, "value");
        co_await index->close();
        co_return;
    });
    EXPECT_EQ(result1, 0);
    EXPECT_GT(originalId, 0);
    
    // Reopen index and verify persistence
    int result2 = run_in_seastar([originalId]() -> seastar::future<> {
        auto index = std::make_unique<MetadataIndex>(TEST_DB_PATH);
        co_await index->init();
        
        std::string measurement = "temperature";
        std::map<std::string, std::string> tags = {{"location", "room1"}};
        
        // Should get same series ID
        uint64_t newId = co_await index->getOrCreateSeriesId(measurement, tags, "value");
        EXPECT_EQ(originalId, newId);
        
        // Should be able to find the series
        std::vector<uint64_t> foundIds = co_await index->findSeriesByMeasurement(measurement);
        EXPECT_EQ(foundIds.size(), 1);
        EXPECT_EQ(foundIds[0], originalId);
        
        co_await index->close();
        co_return;
    });
    EXPECT_EQ(result2, 0);
}

TEST_F(MetadataIndexAsyncTest, ConcurrentSeriesCreation) {
    int result = run_in_seastar([]() -> seastar::future<> {
        auto index = std::make_unique<MetadataIndex>(TEST_DB_PATH);
        co_await index->init();
        
        std::string measurement = "temperature";
        std::vector<seastar::future<uint64_t>> futures;
        
        // Create multiple series concurrently
        for (int i = 0; i < 100; i++) {
            std::map<std::string, std::string> tags = {
                {"location", "room" + std::to_string(i)}
            };
            futures.push_back(index->getOrCreateSeriesId(measurement, tags, "value"));
        }
        
        // Wait for all to complete
        std::vector<uint64_t> ids = co_await seastar::when_all_succeed(futures.begin(), futures.end());
        
        // All IDs should be unique
        std::set<uint64_t> uniqueIds(ids.begin(), ids.end());
        EXPECT_EQ(uniqueIds.size(), ids.size());
        
        // All series should be findable
        std::vector<uint64_t> foundIds = co_await index->findSeriesByMeasurement(measurement);
        EXPECT_EQ(foundIds.size(), 100);
        
        co_await index->close();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(MetadataIndexAsyncTest, GetStats) {
    int result = run_in_seastar([]() -> seastar::future<> {
        auto index = std::make_unique<MetadataIndex>(TEST_DB_PATH);
        co_await index->init();
        
        // Create some data
        for (int i = 0; i < 10; i++) {
            std::map<std::string, std::string> tags = {
                {"location", "room" + std::to_string(i)}
            };
            co_await index->getOrCreateSeriesId("temperature", tags, "value");
        }
        
        std::string stats = index->getStats();
        EXPECT_FALSE(stats.empty());
        // Stats should contain leveldb information
        EXPECT_NE(stats.find("Compactions"), std::string::npos);
        
        co_await index->close();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}