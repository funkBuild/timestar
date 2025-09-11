#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"

class MemoryStoreTest : public ::testing::Test {
protected:
    std::shared_ptr<MemoryStore> store;
    
    void SetUp() override {
        store = std::make_shared<MemoryStore>(1);  // sequence number 1
    }
};

TEST_F(MemoryStoreTest, InsertFloatValues) {
    // Insert float values
    TSDBInsert<double> insert("temperature", "sensor1");
    insert.addValue(1000, 20.5);
    insert.addValue(2000, 21.0);
    insert.addValue(3000, 21.5);
    insert.addValue(4000, 22.0);
    insert.addValue(5000, 22.5);
    
    store->insertMemory(insert);
    
    // Verify series exists
    auto seriesKey = insert.seriesKey();
    SeriesId128 seriesId = insert.seriesId128();
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
    TSDBInsert<bool> insert("status", "online");
    insert.addValue(1000, true);
    insert.addValue(2000, false);
    insert.addValue(3000, true);
    
    store->insertMemory(insert);
    
    // Verify series exists
    auto seriesKey = insert.seriesKey();
    SeriesId128 seriesId = insert.seriesId128();
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
    TSDBInsert<double> temp1("temperature", "room1");
    temp1.addValue(1000, 20.0);
    temp1.addValue(2000, 21.0);
    store->insertMemory(temp1);
    
    TSDBInsert<double> temp2("temperature", "room2");
    temp2.addValue(1000, 22.0);
    temp2.addValue(2000, 23.0);
    store->insertMemory(temp2);
    
    TSDBInsert<bool> door("door", "open");
    door.addValue(1000, true);
    door.addValue(2000, false);
    store->insertMemory(door);
    
    // Verify all series exist
    EXPECT_EQ(store->series.size(), 3);
    
    std::string temp1Key = temp1.seriesKey();
    SeriesId128 temp1Id = temp1.seriesId128();
    auto temp1Type = store->getSeriesType(temp1Id);
    ASSERT_TRUE(temp1Type.has_value());
    EXPECT_EQ(temp1Type.value(), TSMValueType::Float);
    
    std::string temp2Key = temp2.seriesKey();
    SeriesId128 temp2Id = temp2.seriesId128();
    auto temp2Type = store->getSeriesType(temp2Id);
    ASSERT_TRUE(temp2Type.has_value());
    EXPECT_EQ(temp2Type.value(), TSMValueType::Float);
    
    std::string doorKey = door.seriesKey();
    SeriesId128 doorId = door.seriesId128();
    auto doorType = store->getSeriesType(doorId);
    ASSERT_TRUE(doorType.has_value());
    EXPECT_EQ(doorType.value(), TSMValueType::Boolean);
}

TEST_F(MemoryStoreTest, AppendToExistingSeries) {
    // Initial insert
    TSDBInsert<double> insert1("metrics", "requests");
    insert1.addValue(1000, 100.0);
    insert1.addValue(2000, 200.0);
    store->insertMemory(insert1);
    
    // Append more values
    TSDBInsert<double> insert2("metrics", "requests");
    insert2.addValue(3000, 300.0);
    insert2.addValue(4000, 400.0);
    store->insertMemory(insert2);
    
    // Verify all values exist
    auto seriesKey = insert1.seriesKey();
    SeriesId128 seriesId = insert1.seriesId128();
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
    TSDBInsert<double> insert("test", "series");
    insert.addValue(1000, 1.0);
    store->insertMemory(insert);
    
    EXPECT_FALSE(store->isEmpty());
}

TEST_F(MemoryStoreTest, SeriesKeyFormat) {
    // Test with tags
    TSDBInsert<double> insert("weather", "temperature");
    insert.addTag("location", "seattle");
    insert.addTag("sensor", "outdoor");
    insert.addValue(1000, 15.5);
    
    auto seriesKey = insert.seriesKey();
    
    // Series key should contain measurement, tags, and field
    EXPECT_NE(seriesKey.find("weather"), std::string::npos);
    EXPECT_NE(seriesKey.find("temperature"), std::string::npos);
    EXPECT_NE(seriesKey.find("location=seattle"), std::string::npos);
    EXPECT_NE(seriesKey.find("sensor=outdoor"), std::string::npos);
    
    store->insertMemory(insert);
    
    SeriesId128 seriesId = insert.seriesId128();
    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());
}

TEST_F(MemoryStoreTest, SortingTimestamps) {
    TSDBInsert<double> insert("test", "ordering");
    
    // Insert out of order
    insert.addValue(3000, 3.0);
    insert.addValue(1000, 1.0);
    insert.addValue(2000, 2.0);
    insert.addValue(5000, 5.0);
    insert.addValue(4000, 4.0);
    
    store->insertMemory(insert);
    
    auto seriesKey = insert.seriesKey();
    SeriesId128 seriesId = insert.seriesId128();
    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());
    
    auto& seriesData = std::get<InMemorySeries<double>>(it->second);
    
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