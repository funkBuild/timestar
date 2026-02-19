// Seastar-based tests for WAL write and recovery operations

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <memory>

#include "../../../lib/storage/wal.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/utils/crc32.hpp"

#include <seastar/core/coroutine.hh>

namespace fs = std::filesystem;

class WALSeastarTest : public ::testing::Test {
protected:
    std::string testDir = "./test_wal_seastar_files";
    fs::path savedCwd;

    void SetUp() override {
        savedCwd = fs::current_path();
        // If a previous test left us inside the test directory (e.g. due to
        // a crash or exception that bypassed TearDown), step out first.
        if (fs::current_path().filename() == "test_wal_seastar_files") {
            fs::current_path(savedCwd.parent_path());
            savedCwd = fs::current_path();
        }
        // Remove stale test directory from previous runs, then recreate
        fs::remove_all(testDir);
        fs::create_directories(testDir);
        fs::current_path(testDir);
    }

    void TearDown() override {
        fs::current_path(savedCwd);
        fs::remove_all(testDir);
    }
};

seastar::future<> testWALWriteAndRecoverFloat() {
    unsigned int sequenceNumber = 1;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

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

    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

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
    }

    co_return;
}

TEST_F(WALSeastarTest, WriteAndRecoverFloatData) {
    testWALWriteAndRecoverFloat().get();
}

seastar::future<> testWALWriteAndRecoverBoolean() {
    unsigned int sequenceNumber = 2;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

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

    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

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

TEST_F(WALSeastarTest, WriteAndRecoverBooleanData) {
    testWALWriteAndRecoverBoolean().get();
}

seastar::future<> testWALWriteAndRecoverString() {
    unsigned int sequenceNumber = 3;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

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

    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

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

TEST_F(WALSeastarTest, WriteAndRecoverStringData) {
    testWALWriteAndRecoverString().get();
}

seastar::future<> testWALBatchInsert() {
    unsigned int sequenceNumber = 4;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

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

    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

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

TEST_F(WALSeastarTest, BatchInsert) {
    testWALBatchInsert().get();
}

seastar::future<> testWALMultipleSeries() {
    unsigned int sequenceNumber = 5;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        TSDBInsert<double> temp("weather", "temperature");
        temp.addTag("location", "us-west");
        temp.addValue(1000, 72.5);
        temp.addValue(2000, 73.1);
        co_await wal.insert(temp);

        TSDBInsert<bool> alarm("system", "alert");
        alarm.addTag("severity", "high");
        alarm.addValue(1000, false);
        alarm.addValue(2000, true);
        co_await wal.insert(alarm);

        TSDBInsert<std::string> msg("app", "status");
        msg.addTag("component", "api");
        msg.addValue(1000, "running");
        msg.addValue(2000, "degraded");
        co_await wal.insert(msg);

        co_await wal.close();
    }

    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    EXPECT_EQ(recoveredStore->series.size(), 3);

    co_return;
}

TEST_F(WALSeastarTest, MultipleSeries) {
    testWALMultipleSeries().get();
}

seastar::future<> testWALDeleteRange() {
    unsigned int sequenceNumber = 6;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

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
        co_await wal.deleteRange(seriesId, 2000, 3000);
        co_await wal.close();
    }

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
        EXPECT_EQ(seriesData.values.size(), 3);
        EXPECT_DOUBLE_EQ(seriesData.values[0], 10.0);
        EXPECT_DOUBLE_EQ(seriesData.values[1], 40.0);
        EXPECT_DOUBLE_EQ(seriesData.values[2], 50.0);
    }

    co_return;
}

TEST_F(WALSeastarTest, DeleteRange) {
    testWALDeleteRange().get();
}

// ---------------------------------------------------------------------------
// CRC32 WAL integration tests
// ---------------------------------------------------------------------------

seastar::future<> testCRC32RoundtripFloat() {
    unsigned int sequenceNumber = 200;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        TSDBInsert<double> insert("crc_test", "value");
        insert.addValue(1000, 42.5);
        insert.addValue(2000, 43.0);
        insert.addValue(3000, 43.5);

        co_await wal.insert(insert);
        co_await wal.close();
    }

    // Recover and verify data integrity
    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    TSDBInsert<double> testInsert("crc_test", "value");
    SeriesId128 seriesId = testInsert.seriesId128();
    auto it = recoveredStore->series.find(seriesId);
    EXPECT_NE(it, recoveredStore->series.end());

    if (it != recoveredStore->series.end()) {
        auto& seriesData = std::get<InMemorySeries<double>>(it->second);
        EXPECT_EQ(seriesData.values.size(), 3);
        EXPECT_DOUBLE_EQ(seriesData.values[0], 42.5);
        EXPECT_DOUBLE_EQ(seriesData.values[1], 43.0);
        EXPECT_DOUBLE_EQ(seriesData.values[2], 43.5);
    }

    co_return;
}

TEST_F(WALSeastarTest, CRC32RoundtripFloat) {
    testCRC32RoundtripFloat().get();
}

seastar::future<> testCRC32CorruptionDetection() {
    unsigned int sequenceNumber = 201;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        TSDBInsert<double> insert("corrupt_test", "value");
        insert.addValue(1000, 99.9);

        co_await wal.insert(insert);
        co_await wal.close();
    }

    // Corrupt a payload byte in the WAL file
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        // Read the file, flip a byte in the payload area (after length + CRC = 8 bytes),
        // then write it back
        std::ifstream fin(walFile, std::ios::binary);
        std::vector<char> contents((std::istreambuf_iterator<char>(fin)),
                                    std::istreambuf_iterator<char>());
        fin.close();

        // Corrupt a byte well into the payload (offset 12 = past length(4) + crc(4) + a few payload bytes)
        if (contents.size() > 12) {
            contents[12] ^= 0xFF;  // flip all bits in one payload byte
        }

        std::ofstream fout(walFile, std::ios::binary | std::ios::trunc);
        fout.write(contents.data(), contents.size());
        fout.close();
    }

    // Recovery should detect corruption and discard the entry
    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    // The corrupted entry should have been discarded
    EXPECT_EQ(recoveredStore->series.size(), 0);

    co_return;
}

TEST_F(WALSeastarTest, CRC32CorruptionDetection) {
    testCRC32CorruptionDetection().get();
}

seastar::future<> testCRC32PartialCorruption() {
    unsigned int sequenceNumber = 202;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        // Write two entries
        TSDBInsert<double> insert1("partial_test", "first");
        insert1.addValue(1000, 11.1);
        co_await wal.insert(insert1);

        TSDBInsert<double> insert2("partial_test", "second");
        insert2.addValue(2000, 22.2);
        co_await wal.insert(insert2);

        co_await wal.close();
    }

    // Corrupt only the second entry by finding its CRC and breaking it
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        std::ifstream fin(walFile, std::ios::binary);
        std::vector<char> contents((std::istreambuf_iterator<char>(fin)),
                                    std::istreambuf_iterator<char>());
        fin.close();

        // Read the first entry length to find where the second entry starts
        uint32_t firstEntryLen;
        std::memcpy(&firstEntryLen, contents.data(), sizeof(uint32_t));
        // Second entry starts at offset = 4 (first length) + firstEntryLen
        size_t secondEntryOffset = 4 + firstEntryLen;

        // Corrupt a byte in the second entry's payload area
        // Second entry: [length(4)][crc(4)][payload...]
        // Corrupt at offset secondEntryOffset + 12 (past length+crc+some payload)
        size_t corruptOffset = secondEntryOffset + 12;
        if (corruptOffset < contents.size()) {
            contents[corruptOffset] ^= 0xFF;
        }

        std::ofstream fout(walFile, std::ios::binary | std::ios::trunc);
        fout.write(contents.data(), contents.size());
        fout.close();
    }

    // Recovery: first entry should survive, second should be discarded
    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    // First entry should be recovered
    TSDBInsert<double> testInsert1("partial_test", "first");
    SeriesId128 seriesId1 = testInsert1.seriesId128();
    auto it1 = recoveredStore->series.find(seriesId1);
    EXPECT_NE(it1, recoveredStore->series.end());

    if (it1 != recoveredStore->series.end()) {
        auto& data = std::get<InMemorySeries<double>>(it1->second);
        EXPECT_EQ(data.values.size(), 1);
        EXPECT_DOUBLE_EQ(data.values[0], 11.1);
    }

    // Second entry should NOT be recovered (corrupted)
    TSDBInsert<double> testInsert2("partial_test", "second");
    SeriesId128 seriesId2 = testInsert2.seriesId128();
    auto it2 = recoveredStore->series.find(seriesId2);
    EXPECT_EQ(it2, recoveredStore->series.end());

    co_return;
}

TEST_F(WALSeastarTest, CRC32PartialCorruption) {
    testCRC32PartialCorruption().get();
}

seastar::future<> testCRC32BatchInsertRoundtrip() {
    unsigned int sequenceNumber = 203;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        std::vector<TSDBInsert<double>> batch;

        TSDBInsert<double> insert1("batch_crc", "alpha");
        insert1.addValue(1000, 10.0);
        insert1.addValue(2000, 20.0);
        batch.push_back(insert1);

        TSDBInsert<double> insert2("batch_crc", "beta");
        insert2.addValue(1000, 30.0);
        insert2.addValue(2000, 40.0);
        batch.push_back(insert2);

        co_await wal.insertBatch(batch);
        co_await wal.close();
    }

    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    EXPECT_EQ(recoveredStore->series.size(), 2);

    TSDBInsert<double> alphaInsert("batch_crc", "alpha");
    auto alphaIt = recoveredStore->series.find(alphaInsert.seriesId128());
    EXPECT_NE(alphaIt, recoveredStore->series.end());
    if (alphaIt != recoveredStore->series.end()) {
        auto& data = std::get<InMemorySeries<double>>(alphaIt->second);
        EXPECT_EQ(data.values.size(), 2);
        EXPECT_DOUBLE_EQ(data.values[0], 10.0);
        EXPECT_DOUBLE_EQ(data.values[1], 20.0);
    }

    TSDBInsert<double> betaInsert("batch_crc", "beta");
    auto betaIt = recoveredStore->series.find(betaInsert.seriesId128());
    EXPECT_NE(betaIt, recoveredStore->series.end());
    if (betaIt != recoveredStore->series.end()) {
        auto& data = std::get<InMemorySeries<double>>(betaIt->second);
        EXPECT_EQ(data.values.size(), 2);
        EXPECT_DOUBLE_EQ(data.values[0], 30.0);
        EXPECT_DOUBLE_EQ(data.values[1], 40.0);
    }

    co_return;
}

TEST_F(WALSeastarTest, CRC32BatchInsertRoundtrip) {
    testCRC32BatchInsertRoundtrip().get();
}

seastar::future<> testCRC32DeleteRangeRoundtrip() {
    unsigned int sequenceNumber = 204;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());

        TSDBInsert<double> insert("crc_del", "value");
        insert.addValue(1000, 10.0);
        insert.addValue(2000, 20.0);
        insert.addValue(3000, 30.0);
        insert.addValue(4000, 40.0);

        SeriesId128 seriesId = insert.seriesId128();
        co_await wal.insert(insert);
        co_await wal.deleteRange(seriesId, 2000, 3000);
        co_await wal.close();
    }

    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    TSDBInsert<double> testInsert("crc_del", "value");
    SeriesId128 seriesId = testInsert.seriesId128();
    auto it = recoveredStore->series.find(seriesId);
    EXPECT_NE(it, recoveredStore->series.end());

    if (it != recoveredStore->series.end()) {
        auto& seriesData = std::get<InMemorySeries<double>>(it->second);
        // After delete range [2000,3000], we should have values at t=1000 and t=4000
        EXPECT_EQ(seriesData.values.size(), 2);
        EXPECT_DOUBLE_EQ(seriesData.values[0], 10.0);
        EXPECT_DOUBLE_EQ(seriesData.values[1], 40.0);
    }

    co_return;
}

TEST_F(WALSeastarTest, CRC32DeleteRangeRoundtrip) {
    testCRC32DeleteRangeRoundtrip().get();
}

// ---------------------------------------------------------------------------
// Issue #11: Padding recovery tests
// ---------------------------------------------------------------------------

// Test that entries written with immediate flush (which triggers padToAlignment
// after every insert) are all recovered correctly.
seastar::future<> testPaddingRecoveryWithImmediateFlush() {
    unsigned int sequenceNumber = 100;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    const int numEntries = 5;
    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());
        wal.setImmediateFlush(true);

        for (int i = 0; i < numEntries; i++) {
            TSDBInsert<double> insert("flush_test", "value_" + std::to_string(i));
            insert.addValue(1000 * (i + 1), static_cast<double>(i) * 1.5);
            co_await wal.insert(insert);
        }
        co_await wal.close();
    }

    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    EXPECT_EQ(recoveredStore->series.size(), static_cast<size_t>(numEntries))
        << "Expected all " << numEntries
        << " entries to be recovered after immediate-flush padding";

    co_return;
}

TEST_F(WALSeastarTest, PaddingRecoveryWithImmediateFlush) {
    testPaddingRecoveryWithImmediateFlush().get();
}

// Test recovery of entries whose serialized length has 0x00 as the lowest byte
// in little-endian representation (e.g., 256 = 0x00000100 LE).  The old
// byte-scanning reader would mistake the 0x00 byte for padding and misparse
// subsequent data.
seastar::future<> testPaddingRecoveryEntryLengthWithZeroLowByte() {
    unsigned int sequenceNumber = 101;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    const int numEntries = 256;
    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());
        wal.setImmediateFlush(true);

        for (int i = 0; i < numEntries; i++) {
            // Vary measurement name length so that some entries will have
            // entryLength whose lowest byte is 0x00 in little-endian.
            std::string measurement = "meas" + std::string(i, 'x');
            TSDBInsert<double> insert(measurement, "field");
            insert.addValue(1000 * (i + 1), static_cast<double>(i));
            co_await wal.insert(insert);
        }
        co_await wal.close();
    }

    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    EXPECT_EQ(recoveredStore->series.size(), static_cast<size_t>(numEntries))
        << "Expected all " << numEntries
        << " entries recovered; entries with 0x00 low byte in length must "
           "survive padding correctly";

    co_return;
}

TEST_F(WALSeastarTest, PaddingRecoveryEntryLengthWithZeroLowByte) {
    testPaddingRecoveryEntryLengthWithZeroLowByte().get();
}

// Test that mixed-type entries with immediate flush are all recovered.
seastar::future<> testPaddingRecoveryMixedTypes() {
    unsigned int sequenceNumber = 102;
    auto store = std::make_shared<MemoryStore>(sequenceNumber);

    {
        WAL wal(sequenceNumber);
        co_await wal.init(store.get());
        wal.setImmediateFlush(true);

        TSDBInsert<double> floatInsert("mixed", "temperature");
        floatInsert.addValue(1000, 23.5);
        co_await wal.insert(floatInsert);

        TSDBInsert<bool> boolInsert("mixed", "active");
        boolInsert.addValue(2000, true);
        co_await wal.insert(boolInsert);

        TSDBInsert<std::string> stringInsert("mixed", "status");
        stringInsert.addValue(3000, "running");
        co_await wal.insert(stringInsert);

        TSDBInsert<double> floatInsert2("mixed", "humidity");
        floatInsert2.addValue(4000, 65.0);
        co_await wal.insert(floatInsert2);

        co_await wal.close();
    }

    auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
    {
        std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
        WALReader reader(walFile);
        co_await reader.readAll(recoveredStore.get());
    }

    EXPECT_EQ(recoveredStore->series.size(), 4u)
        << "Expected all 4 mixed-type entries to be recovered after "
           "immediate-flush padding";

    // Verify each entry
    {
        TSDBInsert<double> ti("mixed", "temperature");
        auto it = recoveredStore->series.find(ti.seriesId128());
        EXPECT_NE(it, recoveredStore->series.end());
        if (it != recoveredStore->series.end()) {
            auto& s = std::get<InMemorySeries<double>>(it->second);
            EXPECT_EQ(s.values.size(), 1u);
            EXPECT_DOUBLE_EQ(s.values[0], 23.5);
        }
    }
    {
        TSDBInsert<bool> ti("mixed", "active");
        auto it = recoveredStore->series.find(ti.seriesId128());
        EXPECT_NE(it, recoveredStore->series.end());
        if (it != recoveredStore->series.end()) {
            auto& s = std::get<InMemorySeries<bool>>(it->second);
            EXPECT_EQ(s.values.size(), 1u);
            EXPECT_EQ(s.values[0], true);
        }
    }
    {
        TSDBInsert<std::string> ti("mixed", "status");
        auto it = recoveredStore->series.find(ti.seriesId128());
        EXPECT_NE(it, recoveredStore->series.end());
        if (it != recoveredStore->series.end()) {
            auto& s = std::get<InMemorySeries<std::string>>(it->second);
            EXPECT_EQ(s.values.size(), 1u);
            EXPECT_EQ(s.values[0], "running");
        }
    }
    {
        TSDBInsert<double> ti("mixed", "humidity");
        auto it = recoveredStore->series.find(ti.seriesId128());
        EXPECT_NE(it, recoveredStore->series.end());
        if (it != recoveredStore->series.end()) {
            auto& s = std::get<InMemorySeries<double>>(it->second);
            EXPECT_EQ(s.values.size(), 1u);
            EXPECT_DOUBLE_EQ(s.values[0], 65.0);
        }
    }

    co_return;
}

TEST_F(WALSeastarTest, PaddingRecoveryMixedTypes) {
    testPaddingRecoveryMixedTypes().get();
}
