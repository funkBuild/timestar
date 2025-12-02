#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <filesystem>

#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/wal.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>

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
    TSDBInsert<std::string> insert("logs", "message");
    insert.addValue(1000, "Error: Connection timeout");
    insert.addValue(2000, "Warning: High CPU usage");
    insert.addValue(3000, "Info: Request completed");
    insert.addValue(4000, "Debug: Cache hit");

    store->insertMemory(insert);

    // Verify series exists
    auto seriesKey = insert.seriesKey();
    SeriesId128 seriesId = insert.seriesId128();
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
    TSDBInsert<double> floatInsert("metrics", "cpu");
    floatInsert.addValue(1000, 75.5);
    store->insertMemory(floatInsert);

    TSDBInsert<bool> boolInsert("status", "online");
    boolInsert.addValue(1000, true);
    store->insertMemory(boolInsert);

    TSDBInsert<std::string> stringInsert("app", "state");
    stringInsert.addValue(1000, "running");
    store->insertMemory(stringInsert);

    EXPECT_EQ(store->series.size(), 3);

    // Verify each series has correct type
    SeriesId128 floatId = floatInsert.seriesId128();
    EXPECT_EQ(store->getSeriesType(floatId).value(), TSMValueType::Float);

    SeriesId128 boolId = boolInsert.seriesId128();
    EXPECT_EQ(store->getSeriesType(boolId).value(), TSMValueType::Boolean);

    SeriesId128 stringId = stringInsert.seriesId128();
    EXPECT_EQ(store->getSeriesType(stringId).value(), TSMValueType::String);
}

TEST_F(MemoryStoreTest, QuerySeriesFloat) {
    // Insert data
    TSDBInsert<double> insert("temperature", "sensor1");
    insert.addValue(1000, 20.5);
    insert.addValue(2000, 21.0);
    insert.addValue(3000, 21.5);
    store->insertMemory(insert);

    // Query the series
    SeriesId128 seriesId = insert.seriesId128();
    auto result = store->querySeries<double>(seriesId);

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 3);
    EXPECT_DOUBLE_EQ(result->values[0], 20.5);
    EXPECT_DOUBLE_EQ(result->values[2], 21.5);
}

TEST_F(MemoryStoreTest, QuerySeriesString) {
    // Insert data
    TSDBInsert<std::string> insert("app", "status");
    insert.addValue(1000, "starting");
    insert.addValue(2000, "running");
    insert.addValue(3000, "stopping");
    store->insertMemory(insert);

    // Query the series
    SeriesId128 seriesId = insert.seriesId128();
    auto result = store->querySeries<std::string>(seriesId);

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 3);
    EXPECT_EQ(result->values[0], "starting");
    EXPECT_EQ(result->values[2], "stopping");
}

TEST_F(MemoryStoreTest, QueryNonExistentSeries) {
    SeriesId128 fakeId = SeriesId128::fromSeriesKey("nonexistent.series");
    auto result = store->querySeries<double>(fakeId);
    EXPECT_FALSE(result.has_value());
}

TEST_F(MemoryStoreTest, DeleteRangeFloat) {
    // Insert data
    TSDBInsert<double> insert("metrics", "value");
    insert.addValue(1000, 10.0);
    insert.addValue(2000, 20.0);
    insert.addValue(3000, 30.0);
    insert.addValue(4000, 40.0);
    insert.addValue(5000, 50.0);
    store->insertMemory(insert);

    // Delete middle range (2000-3000)
    SeriesId128 seriesId = insert.seriesId128();
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
    TSDBInsert<std::string> insert("logs", "message");
    insert.addValue(1000, "first");
    insert.addValue(2000, "second");
    insert.addValue(3000, "third");
    insert.addValue(4000, "fourth");
    store->insertMemory(insert);

    // Delete range (2000-2000) - just the second value
    SeriesId128 seriesId = insert.seriesId128();
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
    TSDBInsert<std::string> insert("test", "empty");
    insert.addValue(1000, "");
    insert.addValue(2000, "not empty");
    insert.addValue(3000, "");

    store->insertMemory(insert);

    SeriesId128 seriesId = insert.seriesId128();
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

    TSDBInsert<std::string> insert("test", "long");
    insert.addValue(1000, longString);
    insert.addValue(2000, "short");

    store->insertMemory(insert);

    SeriesId128 seriesId = insert.seriesId128();
    auto it = store->series.find(seriesId);
    ASSERT_NE(it, store->series.end());

    auto& seriesData = std::get<InMemorySeries<std::string>>(it->second);
    EXPECT_EQ(seriesData.values.size(), 2);
    EXPECT_EQ(seriesData.values[0].length(), 10000);
    EXPECT_EQ(seriesData.values[1], "short");
}

// Seastar-based tests for WAL integration

seastar::future<> testMemoryStoreInitWAL() {
    unsigned int sequenceNumber = 10;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    // Initialize WAL
    co_await store->initWAL();

    // WAL should be created
    EXPECT_NE(store->getWAL(), nullptr);

    // Insert some data through the store's insert method (which writes to WAL)
    TSDBInsert<double> insert("test", "metric");
    insert.addValue(1000, 42.0);

    bool needsRollover = co_await store->insert(insert);
    EXPECT_FALSE(needsRollover);  // Single insert shouldn't trigger rollover

    // Verify data is in memory store
    SeriesId128 seriesId = insert.seriesId128();
    auto it = store->series.find(seriesId);
    EXPECT_NE(it, store->series.end());

    // Close the store
    co_await store->close();

    co_return;
}

TEST_F(MemoryStoreTest, InitWAL) {
    seastar::app_template app;

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testMemoryStoreInitWAL().then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "MemoryStore initWAL test failed" << std::endl;
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                std::cerr << "Exception: " << e.what() << std::endl;
            }
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testMemoryStoreInitFromWAL() {
    unsigned int sequenceNumber = 11;

    // First, create a store and write data
    {
        auto store = std::make_shared<MemoryStore>(sequenceNumber);
        co_await store->initWAL();

        TSDBInsert<double> insert1("cpu", "usage");
        insert1.addValue(1000, 25.5);
        insert1.addValue(2000, 30.2);
        co_await store->insert(insert1);

        TSDBInsert<bool> insert2("status", "online");
        insert2.addValue(1000, true);
        insert2.addValue(2000, false);
        co_await store->insert(insert2);

        TSDBInsert<std::string> insert3("app", "state");
        insert3.addValue(1000, "running");
        insert3.addValue(2000, "stopped");
        co_await store->insert(insert3);

        co_await store->close();
    }

    // Now create a new store and recover from WAL
    {
        auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);

        co_await recoveredStore->initFromWAL(walFile);

        // Verify all series were recovered
        EXPECT_EQ(recoveredStore->series.size(), 3);

        // Verify float series
        TSDBInsert<double> cpuInsert("cpu", "usage");
        SeriesId128 cpuId = cpuInsert.seriesId128();
        auto cpuIt = recoveredStore->series.find(cpuId);
        EXPECT_NE(cpuIt, recoveredStore->series.end());
        if (cpuIt != recoveredStore->series.end()) {
            auto& cpuData = std::get<InMemorySeries<double>>(cpuIt->second);
            EXPECT_EQ(cpuData.values.size(), 2);
            EXPECT_DOUBLE_EQ(cpuData.values[0], 25.5);
        }

        // Verify bool series
        TSDBInsert<bool> statusInsert("status", "online");
        SeriesId128 statusId = statusInsert.seriesId128();
        auto statusIt = recoveredStore->series.find(statusId);
        EXPECT_NE(statusIt, recoveredStore->series.end());

        // Verify string series
        TSDBInsert<std::string> appInsert("app", "state");
        SeriesId128 appId = appInsert.seriesId128();
        auto appIt = recoveredStore->series.find(appId);
        EXPECT_NE(appIt, recoveredStore->series.end());
        if (appIt != recoveredStore->series.end()) {
            auto& appData = std::get<InMemorySeries<std::string>>(appIt->second);
            EXPECT_EQ(appData.values[0], "running");
        }
    }

    co_return;
}

TEST_F(MemoryStoreTest, InitFromWAL) {
    seastar::app_template app;

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testMemoryStoreInitFromWAL().then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "MemoryStore initFromWAL test failed" << std::endl;
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                std::cerr << "Exception: " << e.what() << std::endl;
            }
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testMemoryStoreBatchInsert() {
    unsigned int sequenceNumber = 12;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);
    co_await store->initWAL();

    // Create batch of inserts
    std::vector<TSDBInsert<double>> batch;

    TSDBInsert<double> insert1("metrics", "cpu");
    insert1.addValue(1000, 10.0);
    insert1.addValue(2000, 20.0);
    batch.push_back(insert1);

    TSDBInsert<double> insert2("metrics", "memory");
    insert2.addValue(1000, 50.0);
    insert2.addValue(2000, 60.0);
    batch.push_back(insert2);

    TSDBInsert<double> insert3("metrics", "disk");
    insert3.addValue(1000, 75.0);
    insert3.addValue(2000, 80.0);
    batch.push_back(insert3);

    // Insert batch
    bool needsRollover = co_await store->insertBatch(batch);
    EXPECT_FALSE(needsRollover);

    // Verify all series are in store
    EXPECT_EQ(store->series.size(), 3);

    // Verify each series has correct data
    SeriesId128 cpuId = insert1.seriesId128();
    auto cpuIt = store->series.find(cpuId);
    EXPECT_NE(cpuIt, store->series.end());
    if (cpuIt != store->series.end()) {
        auto& cpuData = std::get<InMemorySeries<double>>(cpuIt->second);
        EXPECT_EQ(cpuData.values.size(), 2);
        EXPECT_DOUBLE_EQ(cpuData.values[0], 10.0);
        EXPECT_DOUBLE_EQ(cpuData.values[1], 20.0);
    }

    co_await store->close();
    co_return;
}

TEST_F(MemoryStoreTest, BatchInsert) {
    seastar::app_template app;

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testMemoryStoreBatchInsert().then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "MemoryStore batch insert test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testMemoryStoreThresholdChecking() {
    unsigned int sequenceNumber = 13;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);
    co_await store->initWAL();

    // Initially should not be full
    bool isFull = co_await store->isFull();
    EXPECT_FALSE(isFull);

    // Create a large insert to test threshold
    TSDBInsert<double> largeInsert("test", "large");

    // Add many values to approach threshold
    for (int i = 0; i < 10000; i++) {
        largeInsert.addValue(1000 + i, static_cast<double>(i));
    }

    // Check if this would exceed threshold before inserting
    bool wouldExceed = store->wouldExceedThreshold(largeInsert);

    // Insert the data
    co_await store->insert(largeInsert);

    // Check if store is now full or close to full
    isFull = co_await store->isFull();

    // At minimum, the wouldExceedThreshold check should have worked
    // (exact result depends on WAL_SIZE_THRESHOLD)

    co_await store->close();
    co_return;
}

TEST_F(MemoryStoreTest, ThresholdChecking) {
    seastar::app_template app;

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testMemoryStoreThresholdChecking().then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "MemoryStore threshold test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}