#include <gtest/gtest.h>
#include "../../../lib/index/metadata_index_sync.hpp"
#include <filesystem>

class MetadataIndexTest : public ::testing::Test {
protected:
    std::unique_ptr<MetadataIndexSync> index;
    std::string testDbPath = "/tmp/test_metadata_index";
    
    void SetUp() override {
        // Clean up any existing test database
        std::filesystem::remove_all(testDbPath);
        
        // Create new index
        index = std::make_unique<MetadataIndexSync>(testDbPath);
        index->init();
    }
    
    void TearDown() override {
        if (index) {
            index->close();
            index.reset();
        }
        
        // Clean up test database
        std::filesystem::remove_all(testDbPath);
    }
};

// Test SeriesMetadata serialization
TEST_F(MetadataIndexTest, SeriesMetadataSerialization) {
    SeriesMetadataSync original;
    original.seriesId = 12345;
    original.measurement = "temperature";
    original.tags = {{"location", "room1"}, {"sensor", "temp01"}};
    original.fields = {"value", "quality"};
    original.minTime = 1000000;
    original.maxTime = 2000000;
    original.shardId = 5;
    
    std::string serialized = original.serialize();
    ASSERT_FALSE(serialized.empty());
    
    SeriesMetadataSync deserialized = SeriesMetadataSync::deserialize(serialized);
    
    EXPECT_EQ(deserialized.seriesId, original.seriesId);
    EXPECT_EQ(deserialized.measurement, original.measurement);
    EXPECT_EQ(deserialized.tags, original.tags);
    EXPECT_EQ(deserialized.fields, original.fields);
    EXPECT_EQ(deserialized.minTime, original.minTime);
    EXPECT_EQ(deserialized.maxTime, original.maxTime);
    EXPECT_EQ(deserialized.shardId, original.shardId);
}

// Test series key generation
TEST_F(MetadataIndexTest, SeriesKeyGeneration) {
    std::string measurement = "temperature";
    std::map<std::string, std::string> tags = {
        {"location", "room1"},
        {"sensor", "temp01"}
    };
    std::string field = "value";
    
    std::string key1 = SeriesMetadataSync::generateSeriesKey(measurement, tags, field);
    std::string key2 = SeriesMetadataSync::generateSeriesKey(measurement, tags, field);
    
    // Same inputs should generate same key
    EXPECT_EQ(key1, key2);
    
    // Different field should generate different key
    std::string key3 = SeriesMetadataSync::generateSeriesKey(measurement, tags, "other_field");
    EXPECT_NE(key1, key3);
    
    // Different tags should generate different key
    std::map<std::string, std::string> tags2 = {{"location", "room2"}};
    std::string key4 = SeriesMetadataSync::generateSeriesKey(measurement, tags2, field);
    EXPECT_NE(key1, key4);
}

// Test getOrCreateSeriesId
TEST_F(MetadataIndexTest, GetOrCreateSeriesId) {
    std::string measurement = "temperature";
    std::map<std::string, std::string> tags = {
        {"location", "room1"},
        {"sensor", "temp01"}
    };
    std::string field = "value";
    
    // First call should create new series
    uint64_t id1 = index->getOrCreateSeriesId(measurement, tags, field);
    EXPECT_GT(id1, 0);
    
    // Second call with same parameters should return same ID
    uint64_t id2 = index->getOrCreateSeriesId(measurement, tags, field);
    EXPECT_EQ(id1, id2);
    
    // Different field should create new series
    uint64_t id3 = index->getOrCreateSeriesId(measurement, tags, "humidity");
    EXPECT_NE(id1, id3);
    
    // Different tags should create new series
    std::map<std::string, std::string> tags2 = {{"location", "room2"}};
    uint64_t id4 = index->getOrCreateSeriesId(measurement, tags2, field);
    EXPECT_NE(id1, id4);
}

// Test findSeriesByMeasurement
TEST_F(MetadataIndexTest, FindSeriesByMeasurement) {
    // Create multiple series for same measurement
    std::string measurement = "temperature";
    std::vector<uint64_t> createdIds;
    
    for (int i = 0; i < 5; i++) {
        std::map<std::string, std::string> tags = {
            {"location", "room" + std::to_string(i)}
        };
        uint64_t id = index->getOrCreateSeriesId(measurement, tags, "value");
        createdIds.push_back(id);
    }
    
    // Create series for different measurement
    index->getOrCreateSeriesId("humidity", {{"location", "room1"}}, "value");
    
    // Find all temperature series
    std::vector<uint64_t> foundIds = index->findSeriesByMeasurement("temperature");
    
    EXPECT_EQ(foundIds.size(), 5);
    
    // Check all created IDs are found
    std::sort(createdIds.begin(), createdIds.end());
    std::sort(foundIds.begin(), foundIds.end());
    EXPECT_EQ(createdIds, foundIds);
}

// Test findSeriesByTag
TEST_F(MetadataIndexTest, FindSeriesByTag) {
    std::string measurement = "temperature";
    
    // Create series with different tag values
    std::map<std::string, std::string> tags1 = {
        {"location", "room1"},
        {"sensor", "temp01"}
    };
    uint64_t id1 = index->getOrCreateSeriesId(measurement, tags1, "value");
    
    std::map<std::string, std::string> tags2 = {
        {"location", "room1"},
        {"sensor", "temp02"}
    };
    uint64_t id2 = index->getOrCreateSeriesId(measurement, tags2, "value");
    
    std::map<std::string, std::string> tags3 = {
        {"location", "room2"},
        {"sensor", "temp01"}
    };
    uint64_t id3 = index->getOrCreateSeriesId(measurement, tags3, "value");
    
    // Find series by location=room1
    std::vector<uint64_t> foundIds = index->findSeriesByTag(measurement, "location", "room1");
    
    EXPECT_EQ(foundIds.size(), 2);
    std::sort(foundIds.begin(), foundIds.end());
    std::vector<uint64_t> expected = {id1, id2};
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(foundIds, expected);
}

// Test findSeriesByTags (composite)
TEST_F(MetadataIndexTest, FindSeriesByTags) {
    std::string measurement = "temperature";
    
    // Create series with various tag combinations
    std::map<std::string, std::string> tags1 = {
        {"location", "room1"},
        {"sensor", "temp01"}
    };
    uint64_t id1 = index->getOrCreateSeriesId(measurement, tags1, "value");
    
    std::map<std::string, std::string> tags2 = {
        {"location", "room1"},
        {"sensor", "temp02"}
    };
    index->getOrCreateSeriesId(measurement, tags2, "value");
    
    std::map<std::string, std::string> tags3 = {
        {"location", "room2"},
        {"sensor", "temp01"}
    };
    index->getOrCreateSeriesId(measurement, tags3, "value");
    
    // Find series with exact tag match
    std::vector<uint64_t> foundIds = index->findSeriesByTags(measurement, tags1);
    
    EXPECT_EQ(foundIds.size(), 1);
    EXPECT_EQ(foundIds[0], id1);
}

// Test getSeriesMetadata
TEST_F(MetadataIndexTest, GetSeriesMetadata) {
    std::string measurement = "temperature";
    std::map<std::string, std::string> tags = {
        {"location", "room1"},
        {"sensor", "temp01"}
    };
    std::string field = "value";
    
    // Create series
    uint64_t seriesId = index->getOrCreateSeriesId(measurement, tags, field);
    
    // Get metadata
    auto metadata = index->getSeriesMetadata(seriesId);
    
    ASSERT_TRUE(metadata.has_value());
    EXPECT_EQ(metadata->seriesId, seriesId);
    EXPECT_EQ(metadata->measurement, measurement);
    EXPECT_EQ(metadata->tags, tags);
    EXPECT_EQ(metadata->fields.size(), 1);
    EXPECT_EQ(metadata->fields[0], field);
    
    // Non-existent series should return empty
    auto metadata2 = index->getSeriesMetadata(999999);
    EXPECT_FALSE(metadata2.has_value());
}

// Test FieldStats serialization
TEST_F(MetadataIndexTest, FieldStatsSerialization) {
    FieldStatsSync original;
    original.dataType = "float";
    original.minValue = 10.5;
    original.maxValue = 99.9;
    original.pointCount = 1000;
    
    std::string serialized = original.serialize();
    ASSERT_FALSE(serialized.empty());
    
    FieldStatsSync deserialized = FieldStatsSync::deserialize(serialized);
    
    EXPECT_EQ(deserialized.dataType, original.dataType);
    EXPECT_DOUBLE_EQ(deserialized.minValue, original.minValue);
    EXPECT_DOUBLE_EQ(deserialized.maxValue, original.maxValue);
    EXPECT_EQ(deserialized.pointCount, original.pointCount);
}

// Test persistence across restarts
TEST_F(MetadataIndexTest, Persistence) {
    std::string measurement = "temperature";
    std::map<std::string, std::string> tags = {
        {"location", "room1"}
    };
    
    // Create series
    uint64_t originalId = index->getOrCreateSeriesId(measurement, tags, "value");
    
    // Close index
    index->close();
    index.reset();
    
    // Reopen index
    index = std::make_unique<MetadataIndexSync>(testDbPath);
    index->init();
    
    // Should get same series ID
    uint64_t newId = index->getOrCreateSeriesId(measurement, tags, "value");
    
    EXPECT_EQ(originalId, newId);
    
    // Should be able to find the series
    std::vector<uint64_t> foundIds = index->findSeriesByMeasurement(measurement);
    
    EXPECT_EQ(foundIds.size(), 1);
    EXPECT_EQ(foundIds[0], originalId);
}

// Test concurrent series creation (simulated)
TEST_F(MetadataIndexTest, ConcurrentSeriesCreation) {
    std::string measurement = "temperature";
    std::vector<uint64_t> ids;
    
    // Create multiple series rapidly
    for (int i = 0; i < 100; i++) {
        std::map<std::string, std::string> tags = {
            {"location", "room" + std::to_string(i)}
        };
        uint64_t id = index->getOrCreateSeriesId(measurement, tags, "value");
        ids.push_back(id);
    }
    
    // All IDs should be unique
    std::set<uint64_t> uniqueIds(ids.begin(), ids.end());
    EXPECT_EQ(uniqueIds.size(), ids.size());
    
    // All series should be findable
    std::vector<uint64_t> foundIds = index->findSeriesByMeasurement(measurement);
    EXPECT_EQ(foundIds.size(), 100);
}

// Test index stats
TEST_F(MetadataIndexTest, GetStats) {
    // Create some data
    for (int i = 0; i < 10; i++) {
        std::map<std::string, std::string> tags = {
            {"location", "room" + std::to_string(i)}
        };
        index->getOrCreateSeriesId("temperature", tags, "value");
    }
    
    std::string stats = index->getStats();
    EXPECT_FALSE(stats.empty());
    // Stats should contain leveldb information
    EXPECT_NE(stats.find("Compactions"), std::string::npos);
}