#include <gtest/gtest.h>
#include <filesystem>
#include <memory>

#include "../../../lib/storage/wal.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/core/tsdb_value.hpp"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>

namespace fs = std::filesystem;

class WALTest : public ::testing::Test {
protected:
    std::string testDir = "./test_wal_files";
    
    void SetUp() override {
        fs::create_directories(testDir);
        // Change to test directory for WAL files
        fs::current_path(testDir);
    }
    
    void TearDown() override {
        // Change back to parent directory
        fs::current_path("..");
        fs::remove_all(testDir);
    }
};

TEST_F(WALTest, SequenceNumberToFilename) {
    EXPECT_EQ(WAL::sequenceNumberToFilename(1), "shard_0/0000000001.wal");
    EXPECT_EQ(WAL::sequenceNumberToFilename(42), "shard_0/0000000042.wal");
    EXPECT_EQ(WAL::sequenceNumberToFilename(999), "shard_0/0000000999.wal");
    EXPECT_EQ(WAL::sequenceNumberToFilename(12345678), "shard_0/0012345678.wal");
}

// Seastar-based tests for WAL write and recovery operations

seastar::future<> testWALWriteAndRecoverFloat() {
    unsigned int sequenceNumber = 1;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    // Write to WAL
    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        TSDBInsert<double> insert("temperature", "sensor1");
        insert.addValue(1000, 20.5);
        insert.addValue(2000, 21.0);
        insert.addValue(3000, 21.5);

        co_await wal.insert(insert);
        co_await wal.close();
    }

    // Create new store and recover from WAL
    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    // Verify recovered data
    TSDBInsert<double> testInsert("temperature", "sensor1");
    SeriesId128 seriesId = testInsert.seriesId128();
    auto it = recoveredStore->series.find(seriesId);
    EXPECT_NE(it, recoveredStore->series.end());

    if (it != recoveredStore->series.end()) {
        auto& seriesData = std::get<InMemorySeries<double>>(it->second);
        EXPECT_EQ(seriesData.values.size(), 3);
        EXPECT_DOUBLE_EQ(seriesData.values[0], 20.5);
        EXPECT_DOUBLE_EQ(seriesData.values[1], 21.0);
        EXPECT_DOUBLE_EQ(seriesData.values[2], 21.5);
        EXPECT_EQ(seriesData.timestamps[0], 1000);
        EXPECT_EQ(seriesData.timestamps[1], 2000);
        EXPECT_EQ(seriesData.timestamps[2], 3000);
    }

    co_return;
}

TEST_F(WALTest, WriteAndRecoverFloatData) {
    seastar::app_template app;

    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;
    auto exitCode = app.run(argc, argv, [&] {
        return testWALWriteAndRecoverFloat().then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "WAL float test failed" << std::endl;
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

seastar::future<> testWALWriteAndRecoverBoolean() {
    unsigned int sequenceNumber = 2;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    // Write to WAL
    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        TSDBInsert<bool> insert("door", "open");
        insert.addValue(1000, true);
        insert.addValue(2000, false);
        insert.addValue(3000, true);
        insert.addValue(4000, false);

        co_await wal.insert(insert);
        co_await wal.close();
    }

    // Create new store and recover from WAL
    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    // Verify recovered data
    TSDBInsert<bool> testInsert("door", "open");
    SeriesId128 seriesId = testInsert.seriesId128();
    auto it = recoveredStore->series.find(seriesId);
    EXPECT_NE(it, recoveredStore->series.end());

    if (it != recoveredStore->series.end()) {
        auto& seriesData = std::get<InMemorySeries<bool>>(it->second);
        EXPECT_EQ(seriesData.values.size(), 4);
        EXPECT_EQ(seriesData.values[0], true);
        EXPECT_EQ(seriesData.values[1], false);
        EXPECT_EQ(seriesData.values[2], true);
        EXPECT_EQ(seriesData.values[3], false);
    }

    co_return;
}

TEST_F(WALTest, WriteAndRecoverBooleanData) {
    seastar::app_template app;

    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;
    auto exitCode = app.run(argc, argv, [&] {
        return testWALWriteAndRecoverBoolean().then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "WAL boolean test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testWALWriteAndRecoverString() {
    unsigned int sequenceNumber = 3;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    // Write to WAL
    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        TSDBInsert<std::string> insert("message", "log");
        insert.addValue(1000, "Error: connection failed");
        insert.addValue(2000, "Warning: high latency");
        insert.addValue(3000, "Info: request completed");

        co_await wal.insert(insert);
        co_await wal.close();
    }

    // Create new store and recover from WAL
    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    // Verify recovered data
    TSDBInsert<std::string> testInsert("message", "log");
    SeriesId128 seriesId = testInsert.seriesId128();
    auto it = recoveredStore->series.find(seriesId);
    EXPECT_NE(it, recoveredStore->series.end());

    if (it != recoveredStore->series.end()) {
        auto& seriesData = std::get<InMemorySeries<std::string>>(it->second);
        EXPECT_EQ(seriesData.values.size(), 3);
        EXPECT_EQ(seriesData.values[0], "Error: connection failed");
        EXPECT_EQ(seriesData.values[1], "Warning: high latency");
        EXPECT_EQ(seriesData.values[2], "Info: request completed");
    }

    co_return;
}

TEST_F(WALTest, WriteAndRecoverStringData) {
    seastar::app_template app;

    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;
    auto exitCode = app.run(argc, argv, [&] {
        return testWALWriteAndRecoverString().then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "WAL string test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testWALBatchInsert() {
    unsigned int sequenceNumber = 4;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    // Write batch to WAL
    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        std::vector<TSDBInsert<double>> batch;

        TSDBInsert<double> insert1("cpu", "usage");
        insert1.addValue(1000, 25.5);
        insert1.addValue(2000, 30.2);
        batch.push_back(insert1);

        TSDBInsert<double> insert2("memory", "usage");
        insert2.addValue(1000, 65.3);
        insert2.addValue(2000, 67.8);
        batch.push_back(insert2);

        TSDBInsert<double> insert3("disk", "usage");
        insert3.addValue(1000, 80.1);
        insert3.addValue(2000, 82.4);
        batch.push_back(insert3);

        co_await wal.insertBatch(batch);
        co_await wal.close();
    }

    // Create new store and recover from WAL
    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    // Verify recovered data
    EXPECT_EQ(recoveredStore->series.size(), 3);

    TSDBInsert<double> cpuInsert("cpu", "usage");
    SeriesId128 cpuId = cpuInsert.seriesId128();
    auto cpuIt = recoveredStore->series.find(cpuId);
    EXPECT_NE(cpuIt, recoveredStore->series.end());
    if (cpuIt != recoveredStore->series.end()) {
        auto& cpuData = std::get<InMemorySeries<double>>(cpuIt->second);
        EXPECT_EQ(cpuData.values.size(), 2);
        EXPECT_DOUBLE_EQ(cpuData.values[0], 25.5);
    }

    co_return;
}

TEST_F(WALTest, BatchInsert) {
    seastar::app_template app;

    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;
    auto exitCode = app.run(argc, argv, [&] {
        return testWALBatchInsert().then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "WAL batch insert test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testWALMultipleSeries() {
    unsigned int sequenceNumber = 5;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    // Write multiple series to WAL
    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        // Float series
        TSDBInsert<double> temp("weather", "temperature");
        temp.addTag("location", "us-west");
        temp.addValue(1000, 72.5);
        temp.addValue(2000, 73.1);
        co_await wal.insert(temp);

        // Boolean series
        TSDBInsert<bool> alarm("system", "alert");
        alarm.addTag("severity", "high");
        alarm.addValue(1000, false);
        alarm.addValue(2000, true);
        co_await wal.insert(alarm);

        // String series
        TSDBInsert<std::string> msg("app", "status");
        msg.addTag("component", "api");
        msg.addValue(1000, "running");
        msg.addValue(2000, "degraded");
        co_await wal.insert(msg);

        co_await wal.close();
    }

    // Recover from WAL
    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    // Verify all series recovered
    EXPECT_EQ(recoveredStore->series.size(), 3);

    co_return;
}

TEST_F(WALTest, MultipleSeries) {
    seastar::app_template app;

    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;
    auto exitCode = app.run(argc, argv, [&] {
        return testWALMultipleSeries().then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "WAL multiple series test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testWALDeleteRange() {
    unsigned int sequenceNumber = 6;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    // Write data then delete a range
    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        TSDBInsert<double> insert("metrics", "value");
        insert.addValue(1000, 10.0);
        insert.addValue(2000, 20.0);
        insert.addValue(3000, 30.0);
        insert.addValue(4000, 40.0);
        insert.addValue(5000, 50.0);

        SeriesId128 seriesId = insert.seriesId128();

        co_await wal.insert(insert);

        // Delete middle range (2000-3000)
        co_await wal.deleteRange(seriesId, 2000, 3000);

        co_await wal.close();
    }

    // Recover and verify deletion
    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    TSDBInsert<double> testInsert("metrics", "value");
    SeriesId128 seriesId = testInsert.seriesId128();
    auto it = recoveredStore->series.find(seriesId);
    EXPECT_NE(it, recoveredStore->series.end());

    if (it != recoveredStore->series.end()) {
        auto& seriesData = std::get<InMemorySeries<double>>(it->second);
        // Should have 3 values (deleted 2000 and 3000)
        EXPECT_EQ(seriesData.values.size(), 3);
        EXPECT_DOUBLE_EQ(seriesData.values[0], 10.0);
        EXPECT_DOUBLE_EQ(seriesData.values[1], 40.0);
        EXPECT_DOUBLE_EQ(seriesData.values[2], 50.0);
    }

    co_return;
}

TEST_F(WALTest, DeleteRange) {
    seastar::app_template app;

    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;
    auto exitCode = app.run(argc, argv, [&] {
        return testWALDeleteRange().then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "WAL delete range test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}