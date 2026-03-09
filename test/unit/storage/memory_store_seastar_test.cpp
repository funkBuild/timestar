// Seastar-based tests for MemoryStore WAL integration

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <filesystem>

#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/wal.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/core/series_id.hpp"

#include <seastar/core/coroutine.hh>

namespace fs = std::filesystem;

class MemoryStoreSeastarTest : public ::testing::Test {
protected:
    std::string testDir = "./test_memory_store_seastar";

    void SetUp() override {
        fs::create_directories(testDir);
        fs::create_directories(testDir + "/shard_0");
    }

    void TearDown() override {
        fs::remove_all(testDir);
    }
};

seastar::future<> testMemoryStoreInitWAL() {
    unsigned int sequenceNumber = 10;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    // Initialize WAL
    co_await store->initWAL();

    // WAL should be created
    EXPECT_NE(store->getWAL(), nullptr);

    // Insert some data through the store's insert method (which writes to WAL)
    TimeStarInsert<double> insert("test", "metric");
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

TEST_F(MemoryStoreSeastarTest, InitWAL) {
    testMemoryStoreInitWAL().get();
}

seastar::future<> testMemoryStoreInitFromWAL() {
    unsigned int sequenceNumber = 11;

    // First, create a store and write data
    {
        auto store = std::make_shared<MemoryStore>(sequenceNumber);
        co_await store->initWAL();

        TimeStarInsert<double> insert1("cpu", "usage");
        insert1.addValue(1000, 25.5);
        insert1.addValue(2000, 30.2);
        co_await store->insert(insert1);

        TimeStarInsert<bool> insert2("status", "online");
        insert2.addValue(1000, true);
        insert2.addValue(2000, false);
        co_await store->insert(insert2);

        TimeStarInsert<std::string> insert3("app", "state");
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
        TimeStarInsert<double> cpuInsert("cpu", "usage");
        SeriesId128 cpuId = cpuInsert.seriesId128();
        auto cpuIt = recoveredStore->series.find(cpuId);
        EXPECT_NE(cpuIt, recoveredStore->series.end());
        if (cpuIt == recoveredStore->series.end()) co_return;
        auto& cpuData = std::get<InMemorySeries<double>>(cpuIt->second);
        EXPECT_EQ(cpuData.values.size(), 2);
        EXPECT_DOUBLE_EQ(cpuData.values[0], 25.5);

        // Verify bool series
        TimeStarInsert<bool> statusInsert("status", "online");
        SeriesId128 statusId = statusInsert.seriesId128();
        auto statusIt = recoveredStore->series.find(statusId);
        EXPECT_NE(statusIt, recoveredStore->series.end());

        // Verify string series
        TimeStarInsert<std::string> appInsert("app", "state");
        SeriesId128 appId = appInsert.seriesId128();
        auto appIt = recoveredStore->series.find(appId);
        EXPECT_NE(appIt, recoveredStore->series.end());
        if (appIt == recoveredStore->series.end()) co_return;
        auto& appData = std::get<InMemorySeries<std::string>>(appIt->second);
        EXPECT_EQ(appData.values[0], "running");
    }

    co_return;
}

TEST_F(MemoryStoreSeastarTest, InitFromWAL) {
    testMemoryStoreInitFromWAL().get();
}

seastar::future<> testMemoryStoreBatchInsert() {
    unsigned int sequenceNumber = 12;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);
    co_await store->initWAL();

    // Insert multiple entries in batch
    for (int i = 0; i < 100; i++) {
        TimeStarInsert<double> insert("batch", "test");
        insert.addValue(i * 1000, static_cast<double>(i));
        co_await store->insert(insert);
    }

    // Verify data
    TimeStarInsert<double> queryInsert("batch", "test");
    SeriesId128 seriesId = queryInsert.seriesId128();
    auto it = store->series.find(seriesId);
    EXPECT_NE(it, store->series.end());
    if (it == store->series.end()) co_return;
    auto& data = std::get<InMemorySeries<double>>(it->second);
    EXPECT_EQ(data.values.size(), 100);

    co_await store->close();
    co_return;
}

TEST_F(MemoryStoreSeastarTest, BatchInsert) {
    testMemoryStoreBatchInsert().get();
}

seastar::future<> testMemoryStoreThresholdChecking() {
    unsigned int sequenceNumber = 13;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);
    co_await store->initWAL();

    // Insert data up to threshold (16MB)
    // This would normally require a lot of data, so we just test the API
    TimeStarInsert<double> insert("threshold", "test");
    insert.addValue(1000, 1.0);

    bool needsRollover = co_await store->insert(insert);

    // Single insert shouldn't trigger rollover
    EXPECT_FALSE(needsRollover);

    co_await store->close();
    co_return;
}

TEST_F(MemoryStoreSeastarTest, ThresholdChecking) {
    testMemoryStoreThresholdChecking().get();
}
