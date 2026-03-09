#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <filesystem>

#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/wal.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/core/series_id.hpp"

namespace fs = std::filesystem;

class MemoryStoreTest : public ::testing::Test {
protected:
    std::shared_ptr<MemoryStore> store;
    std::string testDir = "./test_memory_store";

    void SetUp() override {
        store = std::make_shared<MemoryStore>(1);  // sequence number 1
        fs::create_directories(testDir);
        fs::create_directories(testDir + "/shard_0");
    }

    void TearDown() override {
        fs::remove_all(testDir);
    }
};

TEST_F(MemoryStoreTest, InsertFloatValues) {
    // Insert float values
    TimeStarInsert<double> insert("temperature", "sensor1");
    insert.addValue(1000, 20.5);
    insert.addValue(2000, 21.0);
    insert.addValue(3000, 21.5);
    insert.addValue(4000, 22.0);
    insert.addValue(5000, 22.5);
    
    auto seriesKey = insert.seriesKey();
    SeriesId128 seriesId = insert.seriesId128();
    store->insertMemory(std::move(insert));

    // Verify series exists
    auto seriesType = store->getSeriesType(seriesId);

    ASSERT_TRUE(seriesType.has_value());
    EXPECT_EQ(seriesType.value(), TSMValueType::Float);

    // Verify data was inserted
    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());

    auto& seriesData = std::get<InMemorySeries<double>>(it->second);
    EXPECT_EQ(seriesData.timestamps.size(), 5);
    EXPECT_EQ(seriesData.values.size(), 5);
    EXPECT_DOUBLE_EQ(seriesData.values[0], 20.5);
    EXPECT_DOUBLE_EQ(seriesData.values[4], 22.5);
}

TEST_F(MemoryStoreTest, InsertBooleanValues) {
    // Insert boolean values
    TimeStarInsert<bool> insert("status", "online");
    insert.addValue(1000, true);
    insert.addValue(2000, false);
    insert.addValue(3000, true);
    
    auto seriesKey = insert.seriesKey();
    SeriesId128 seriesId = insert.seriesId128();
    store->insertMemory(std::move(insert));

    // Verify series exists
    auto seriesType = store->getSeriesType(seriesId);

    ASSERT_TRUE(seriesType.has_value());
    EXPECT_EQ(seriesType.value(), TSMValueType::Boolean);

    // Verify data was inserted
    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());

    auto& seriesData = std::get<InMemorySeries<bool>>(it->second);
    EXPECT_EQ(seriesData.values.size(), 3);
    EXPECT_EQ(seriesData.values[0], true);
    EXPECT_EQ(seriesData.values[1], false);
    EXPECT_EQ(seriesData.values[2], true);
}

TEST_F(MemoryStoreTest, MultipleSeries) {
    // Insert into multiple series
    TimeStarInsert<double> temp1("temperature", "room1");
    temp1.addValue(1000, 20.0);
    temp1.addValue(2000, 21.0);
    std::string temp1Key = temp1.seriesKey();
    SeriesId128 temp1Id = temp1.seriesId128();
    store->insertMemory(std::move(temp1));

    TimeStarInsert<double> temp2("temperature", "room2");
    temp2.addValue(1000, 22.0);
    temp2.addValue(2000, 23.0);
    std::string temp2Key = temp2.seriesKey();
    SeriesId128 temp2Id = temp2.seriesId128();
    store->insertMemory(std::move(temp2));

    TimeStarInsert<bool> door("door", "open");
    door.addValue(1000, true);
    door.addValue(2000, false);
    std::string doorKey = door.seriesKey();
    SeriesId128 doorId = door.seriesId128();
    store->insertMemory(std::move(door));

    // Verify all series exist
    EXPECT_EQ(store->series.size(), 3);

    auto temp1Type = store->getSeriesType(temp1Id);
    ASSERT_TRUE(temp1Type.has_value());
    EXPECT_EQ(temp1Type.value(), TSMValueType::Float);

    auto temp2Type = store->getSeriesType(temp2Id);
    ASSERT_TRUE(temp2Type.has_value());
    EXPECT_EQ(temp2Type.value(), TSMValueType::Float);

    auto doorType = store->getSeriesType(doorId);
    ASSERT_TRUE(doorType.has_value());
    EXPECT_EQ(doorType.value(), TSMValueType::Boolean);
}

TEST_F(MemoryStoreTest, AppendToExistingSeries) {
    // Initial insert
    TimeStarInsert<double> insert1("metrics", "requests");
    insert1.addValue(1000, 100.0);
    insert1.addValue(2000, 200.0);
    auto seriesKey = insert1.seriesKey();
    SeriesId128 seriesId = insert1.seriesId128();
    store->insertMemory(std::move(insert1));

    // Append more values
    TimeStarInsert<double> insert2("metrics", "requests");
    insert2.addValue(3000, 300.0);
    insert2.addValue(4000, 400.0);
    store->insertMemory(std::move(insert2));

    // Verify all values exist
    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());
    
    auto& seriesData = std::get<InMemorySeries<double>>(it->second);
    EXPECT_EQ(seriesData.values.size(), 4);
    EXPECT_DOUBLE_EQ(seriesData.values[0], 100.0);
    EXPECT_DOUBLE_EQ(seriesData.values[3], 400.0);
}

TEST_F(MemoryStoreTest, NonExistentSeries) {
    std::string nonExistent = "non.existent series";
    SeriesId128 nonExistentId = SeriesId128::fromSeriesKey(nonExistent);
    auto seriesType = store->getSeriesType(nonExistentId);
    EXPECT_FALSE(seriesType.has_value());
}

TEST_F(MemoryStoreTest, EmptyStore) {
    EXPECT_TRUE(store->isEmpty());
    EXPECT_FALSE(store->isClosed());
    // isFull() is now async, skip in sync test
    
    // Add data
    TimeStarInsert<double> insert("test", "series");
    insert.addValue(1000, 1.0);
    store->insertMemory(std::move(insert));
    
    EXPECT_FALSE(store->isEmpty());
}

TEST_F(MemoryStoreTest, SeriesKeyFormat) {
    // Test with tags
    TimeStarInsert<double> insert("weather", "temperature");
    insert.addTag("location", "seattle");
    insert.addTag("sensor", "outdoor");
    insert.addValue(1000, 15.5);
    
    auto seriesKey = insert.seriesKey();
    
    // Series key should contain measurement, tags, and field
    EXPECT_NE(seriesKey.find("weather"), std::string::npos);
    EXPECT_NE(seriesKey.find("temperature"), std::string::npos);
    EXPECT_NE(seriesKey.find("location=seattle"), std::string::npos);
    EXPECT_NE(seriesKey.find("sensor=outdoor"), std::string::npos);
    
    SeriesId128 seriesId = insert.seriesId128();
    store->insertMemory(std::move(insert));

    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());
}

TEST_F(MemoryStoreTest, SortingTimestamps) {
    TimeStarInsert<double> insert("test", "ordering");
    
    // Insert out of order
    insert.addValue(3000, 3.0);
    insert.addValue(1000, 1.0);
    insert.addValue(2000, 2.0);
    insert.addValue(5000, 5.0);
    insert.addValue(4000, 4.0);
    
    auto seriesKey = insert.seriesKey();
    SeriesId128 seriesId = insert.seriesId128();
    store->insertMemory(std::move(insert));

    // Check that series exists
    ASSERT_NE(store->series.find(seriesId), store->series.end());

    // Access mutable reference using at()
    auto& seriesData = std::get<InMemorySeries<double>>(store->series.at(seriesId));

    // Sort the series
    seriesData.sort();
    
    // Verify timestamps are in order
    for (size_t i = 1; i < seriesData.timestamps.size(); i++) {
        EXPECT_GT(seriesData.timestamps[i], seriesData.timestamps[i-1]);
    }
    
    // Verify values correspond to correct timestamps
    for (size_t i = 0; i < seriesData.values.size(); i++) {
        EXPECT_DOUBLE_EQ(seriesData.values[i], seriesData.timestamps[i] / 1000.0);
    }
}

TEST_F(MemoryStoreTest, InsertStringValues) {
    // Insert string values
    TimeStarInsert<std::string> insert("logs", "message");
    insert.addValue(1000, "Error: Connection timeout");
    insert.addValue(2000, "Warning: High CPU usage");
    insert.addValue(3000, "Info: Request completed");
    insert.addValue(4000, "Debug: Cache hit");

    auto seriesKey = insert.seriesKey();
    SeriesId128 seriesId = insert.seriesId128();
    store->insertMemory(std::move(insert));

    // Verify series exists
    auto seriesType = store->getSeriesType(seriesId);

    ASSERT_TRUE(seriesType.has_value());
    EXPECT_EQ(seriesType.value(), TSMValueType::String);

    // Verify data was inserted
    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());

    auto& seriesData = std::get<InMemorySeries<std::string>>(it->second);
    EXPECT_EQ(seriesData.timestamps.size(), 4);
    EXPECT_EQ(seriesData.values.size(), 4);
    EXPECT_EQ(seriesData.values[0], "Error: Connection timeout");
    EXPECT_EQ(seriesData.values[3], "Debug: Cache hit");
}

TEST_F(MemoryStoreTest, InsertMixedTypes) {
    // Test that we can have float, bool, and string series in same store
    TimeStarInsert<double> floatInsert("metrics", "cpu");
    floatInsert.addValue(1000, 75.5);
    SeriesId128 floatId = floatInsert.seriesId128();
    store->insertMemory(std::move(floatInsert));

    TimeStarInsert<bool> boolInsert("status", "online");
    boolInsert.addValue(1000, true);
    SeriesId128 boolId = boolInsert.seriesId128();
    store->insertMemory(std::move(boolInsert));

    TimeStarInsert<std::string> stringInsert("app", "state");
    stringInsert.addValue(1000, "running");
    SeriesId128 stringId = stringInsert.seriesId128();
    store->insertMemory(std::move(stringInsert));

    EXPECT_EQ(store->series.size(), 3);

    // Verify each series has correct type
    EXPECT_EQ(store->getSeriesType(floatId).value(), TSMValueType::Float);
    EXPECT_EQ(store->getSeriesType(boolId).value(), TSMValueType::Boolean);
    EXPECT_EQ(store->getSeriesType(stringId).value(), TSMValueType::String);
}

TEST_F(MemoryStoreTest, QuerySeriesFloat) {
    // Insert data
    TimeStarInsert<double> insert("temperature", "sensor1");
    insert.addValue(1000, 20.5);
    insert.addValue(2000, 21.0);
    insert.addValue(3000, 21.5);
    SeriesId128 seriesId = insert.seriesId128();
    store->insertMemory(std::move(insert));

    // Query the series
    auto result = store->querySeries<double>(seriesId);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->values.size(), 3);
    EXPECT_DOUBLE_EQ(result->values[0], 20.5);
    EXPECT_DOUBLE_EQ(result->values[2], 21.5);
}

TEST_F(MemoryStoreTest, QuerySeriesString) {
    // Insert data
    TimeStarInsert<std::string> insert("app", "status");
    insert.addValue(1000, "starting");
    insert.addValue(2000, "running");
    insert.addValue(3000, "stopping");
    SeriesId128 seriesId = insert.seriesId128();
    store->insertMemory(std::move(insert));

    // Query the series
    auto result = store->querySeries<std::string>(seriesId);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->values.size(), 3);
    EXPECT_EQ(result->values[0], "starting");
    EXPECT_EQ(result->values[2], "stopping");
}

TEST_F(MemoryStoreTest, QueryNonExistentSeries) {
    SeriesId128 fakeId = SeriesId128::fromSeriesKey("nonexistent.series");
    auto result = store->querySeries<double>(fakeId);
    EXPECT_EQ(result, nullptr);
}

TEST_F(MemoryStoreTest, DeleteRangeFloat) {
    // Insert data
    TimeStarInsert<double> insert("metrics", "value");
    insert.addValue(1000, 10.0);
    insert.addValue(2000, 20.0);
    insert.addValue(3000, 30.0);
    insert.addValue(4000, 40.0);
    insert.addValue(5000, 50.0);
    SeriesId128 seriesId = insert.seriesId128();
    store->insertMemory(std::move(insert));

    // Delete middle range (2000-3000)
    store->deleteRange(seriesId, 2000, 3000);

    // Verify deletion
    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());

    auto& seriesData = std::get<InMemorySeries<double>>(it->second);
    EXPECT_EQ(seriesData.values.size(), 3);  // Should have 3 values left
    EXPECT_DOUBLE_EQ(seriesData.values[0], 10.0);  // 1000
    EXPECT_DOUBLE_EQ(seriesData.values[1], 40.0);  // 4000
    EXPECT_DOUBLE_EQ(seriesData.values[2], 50.0);  // 5000
}

TEST_F(MemoryStoreTest, DeleteRangeString) {
    // Insert data
    TimeStarInsert<std::string> insert("logs", "message");
    insert.addValue(1000, "first");
    insert.addValue(2000, "second");
    insert.addValue(3000, "third");
    insert.addValue(4000, "fourth");
    SeriesId128 seriesId = insert.seriesId128();
    store->insertMemory(std::move(insert));

    // Delete range (2000-2000) - just the second value
    store->deleteRange(seriesId, 2000, 2000);

    // Verify deletion
    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());

    auto& seriesData = std::get<InMemorySeries<std::string>>(it->second);
    EXPECT_EQ(seriesData.values.size(), 3);
    EXPECT_EQ(seriesData.values[0], "first");
    EXPECT_EQ(seriesData.values[1], "third");
    EXPECT_EQ(seriesData.values[2], "fourth");
}

TEST_F(MemoryStoreTest, EmptyStringValues) {
    // Test handling of empty strings
    TimeStarInsert<std::string> insert("test", "empty");
    insert.addValue(1000, "");
    insert.addValue(2000, "not empty");
    insert.addValue(3000, "");

    SeriesId128 seriesId = insert.seriesId128();
    store->insertMemory(std::move(insert));

    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());

    auto& seriesData = std::get<InMemorySeries<std::string>>(it->second);
    EXPECT_EQ(seriesData.values.size(), 3);
    EXPECT_EQ(seriesData.values[0], "");
    EXPECT_EQ(seriesData.values[1], "not empty");
    EXPECT_EQ(seriesData.values[2], "");
}

TEST_F(MemoryStoreTest, LongStringValues) {
    // Test handling of long strings
    std::string longString(10000, 'x');  // 10KB string

    TimeStarInsert<std::string> insert("test", "long");
    insert.addValue(1000, longString);
    insert.addValue(2000, "short");

    SeriesId128 seriesId = insert.seriesId128();
    store->insertMemory(std::move(insert));

    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());

    auto& seriesData = std::get<InMemorySeries<std::string>>(it->second);
    EXPECT_EQ(seriesData.values.size(), 2);
    EXPECT_EQ(seriesData.values[0].length(), 10000);
    EXPECT_EQ(seriesData.values[1], "short");
}

