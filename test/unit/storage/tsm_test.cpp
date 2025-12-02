#include <gtest/gtest.h>
#include <filesystem>
#include <random>
#include <chrono>

#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/compressed_buffer.hpp"
#include "../../../lib/encoding/float_encoder.hpp"
#include "../../../lib/encoding/integer_encoder.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>

namespace fs = std::filesystem;

class TSMTest : public ::testing::Test {
protected:
    std::string testDir = "./test_tsm_files";
    
    void SetUp() override {
        fs::create_directories(testDir);
    }
    
    void TearDown() override {
        fs::remove_all(testDir);
    }
    
    std::string getTestFilePath(const std::string& filename) {
        return testDir + "/" + filename;
    }
};

TEST_F(TSMTest, BasicTSMWriterFloat) {
    std::string filename = getTestFilePath("test_float.tsm");
    
    TSMWriter writer(filename);
    
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    
    uint64_t baseTime = 1600000000000;
    for (int i = 0; i < 1000; i++) {
        timestamps.push_back(baseTime + i * 1000);
        values.push_back(100.0 + sin(i * 0.1) * 10.0);
    }
    
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("temperature.sensor1");
    writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
    writer.writeIndex();
    writer.close();
    
    // Verify file was created
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
}

TEST_F(TSMTest, BasicTSMWriterBoolean) {
    std::string filename = getTestFilePath("test_bool.tsm");
    
    TSMWriter writer(filename);
    
    std::vector<uint64_t> timestamps;
    std::vector<bool> values;
    
    uint64_t baseTime = 1600000000000;
    for (int i = 0; i < 500; i++) {
        timestamps.push_back(baseTime + i * 1000);
        values.push_back(i % 2 == 0);
    }
    
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("status.online");
    writer.writeSeries(TSMValueType::Boolean, seriesId, timestamps, values);
    writer.writeIndex();
    writer.close();
    
    // Verify file was created
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
}

TEST_F(TSMTest, WriteMultipleSeries) {
    std::string filename = getTestFilePath("test_multi.tsm");
    
    TSMWriter writer(filename);
    
    uint64_t baseTime = 1600000000000;
    
    // Series 1: Temperature
    std::vector<uint64_t> tempTimestamps;
    std::vector<double> tempValues;
    for (int i = 0; i < 100; i++) {
        tempTimestamps.push_back(baseTime + i * 1000);
        tempValues.push_back(20.0 + i * 0.1);
    }
    SeriesId128 tempSeriesId = SeriesId128::fromSeriesKey("temperature.room1");
    writer.writeSeries(TSMValueType::Float, tempSeriesId, tempTimestamps, tempValues);
    
    // Series 2: Humidity
    std::vector<uint64_t> humidTimestamps;
    std::vector<double> humidValues;
    for (int i = 0; i < 100; i++) {
        humidTimestamps.push_back(baseTime + i * 1000);
        humidValues.push_back(45.0 + i * 0.2);
    }
    SeriesId128 humidSeriesId = SeriesId128::fromSeriesKey("humidity.room1");
    writer.writeSeries(TSMValueType::Float, humidSeriesId, humidTimestamps, humidValues);
    
    // Series 3: Door status
    std::vector<uint64_t> doorTimestamps;
    std::vector<bool> doorValues;
    for (int i = 0; i < 50; i++) {
        doorTimestamps.push_back(baseTime + i * 5000);
        doorValues.push_back(i % 3 == 0);
    }
    SeriesId128 doorSeriesId = SeriesId128::fromSeriesKey("door.status");
    writer.writeSeries(TSMValueType::Boolean, doorSeriesId, doorTimestamps, doorValues);
    
    writer.writeIndex();
    writer.close();
    
    // Verify file was created
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
}

TEST_F(TSMTest, LargeDataset) {
    std::string filename = getTestFilePath("test_large.tsm");
    
    const int NUM_POINTS = 100000;
    uint64_t baseTime = 1600000000000;
    
    TSMWriter writer(filename);
    
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    
    std::default_random_engine generator;
    std::normal_distribution<double> distribution(100.0, 10.0);
    
    for (int i = 0; i < NUM_POINTS; i++) {
        timestamps.push_back(baseTime + i * 100);
        values.push_back(distribution(generator));
    }
    
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("metrics.large");
    writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
    writer.writeIndex();
    writer.close();
    
    // Verify file was created and has reasonable size
    EXPECT_TRUE(fs::exists(filename));
    auto fileSize = fs::file_size(filename);
    EXPECT_GT(fileSize, 1000); // Should be at least 1KB
    
    // Check compression ratio (rough estimate)
    // Uncompressed would be ~1.6MB (100k * 16 bytes per point)
    // Good compression should get it under 1MB
    EXPECT_LT(fileSize, 1000000);
}

TEST_F(TSMTest, BlockBoundaries) {
    std::string filename = getTestFilePath("test_blocks.tsm");
    
    // Test data that spans multiple blocks (MaxPointsPerBlock = 10000)
    const int NUM_POINTS = 25000; // Should create 3 blocks
    
    TSMWriter writer(filename);
    
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    
    uint64_t baseTime = 1600000000000;
    for (int i = 0; i < NUM_POINTS; i++) {
        timestamps.push_back(baseTime + i * 1000);
        values.push_back(i * 1.0);
    }
    
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("metrics.blocks");
    writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
    writer.writeIndex();
    writer.close();
    
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
}

// Seastar-based tests for TSM read operations

seastar::future<> testTSMReadFloat(std::string filename) {
    // Write test data first
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000};
        std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.series");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // Read back
    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    // Verify series type
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.series");
    // Load full index entry into cache first
    auto* indexEntry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(indexEntry, nullptr);

    auto seriesType = tsm.getSeriesType(seriesId);
    EXPECT_TRUE(seriesType.has_value());
    EXPECT_EQ(seriesType.value(), TSMValueType::Float);

    // Read full series
    TSMResult<double> results(0);
    co_await tsm.readSeries(seriesId, 0, UINT64_MAX, results);

    auto [timestamps, values] = results.getAllData();
    EXPECT_EQ(timestamps.size(), 5);
    EXPECT_EQ(values.size(), 5);
    EXPECT_DOUBLE_EQ(values[0], 1.0);
    EXPECT_DOUBLE_EQ(values[4], 5.0);
    EXPECT_EQ(timestamps[0], 1000);
    EXPECT_EQ(timestamps[4], 5000);

    co_await tsm.close();
    co_return;
}

TEST_F(TSMTest, ReadFloatData) {
    seastar::app_template app;
    std::string filename = getTestFilePath("0_1.tsm");  // tier=0, seq=1

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testTSMReadFloat(filename).then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "TSM read float test failed" << std::endl;
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

seastar::future<> testTSMReadBoolean(std::string filename) {
    // Write test data
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000};
        std::vector<bool> values = {true, false, true, false};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.bool");
        writer.writeSeries(TSMValueType::Boolean, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // Read back
    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.bool");
    // Load full index entry into cache first
    auto* indexEntry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(indexEntry, nullptr);

    auto seriesType = tsm.getSeriesType(seriesId);
    EXPECT_TRUE(seriesType.has_value());
    EXPECT_EQ(seriesType.value(), TSMValueType::Boolean);

    // Read full series
    TSMResult<bool> results(0);
    co_await tsm.readSeries(seriesId, 0, UINT64_MAX, results);

    auto [timestamps, values] = results.getAllData();
    EXPECT_EQ(values.size(), 4);
    EXPECT_EQ(values[0], true);
    EXPECT_EQ(values[1], false);
    EXPECT_EQ(values[2], true);
    EXPECT_EQ(values[3], false);

    co_await tsm.close();
    co_return;
}

TEST_F(TSMTest, ReadBooleanData) {
    seastar::app_template app;
    std::string filename = getTestFilePath("0_2.tsm");  // tier=0, seq=2

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testTSMReadBoolean(filename).then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "TSM read boolean test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testTSMReadString(std::string filename) {
    // Write test data
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000};
        std::vector<std::string> values = {"hello", "world", "test"};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.string");
        writer.writeSeries(TSMValueType::String, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // Read back
    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.string");
    // Load full index entry into cache first
    auto* indexEntry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(indexEntry, nullptr);

    auto seriesType = tsm.getSeriesType(seriesId);
    EXPECT_TRUE(seriesType.has_value());
    EXPECT_EQ(seriesType.value(), TSMValueType::String);

    // Read full series
    TSMResult<std::string> results(0);
    co_await tsm.readSeries(seriesId, 0, UINT64_MAX, results);

    auto [timestamps, values] = results.getAllData();
    EXPECT_EQ(values.size(), 3);
    EXPECT_EQ(values[0], "hello");
    EXPECT_EQ(values[1], "world");
    EXPECT_EQ(values[2], "test");

    co_await tsm.close();
    co_return;
}

TEST_F(TSMTest, ReadStringData) {
    seastar::app_template app;
    std::string filename = getTestFilePath("0_3.tsm");  // tier=0, seq=3

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testTSMReadString(filename).then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "TSM read string test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testTSMReadTimeRange(std::string filename) {
    // Write test data with known time range
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
        std::vector<double> values = {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.range");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // Read back with time range filter
    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.range");

    // Read middle range (3000-7000)
    TSMResult<double> results(0);
    co_await tsm.readSeries(seriesId, 3000, 7000, results);

    auto [timestamps, values] = results.getAllData();
    // Should get values at 3000, 4000, 5000, 6000, 7000
    EXPECT_EQ(values.size(), 5);
    EXPECT_DOUBLE_EQ(values[0], 30.0);
    EXPECT_DOUBLE_EQ(values[4], 70.0);
    EXPECT_EQ(timestamps[0], 3000);
    EXPECT_EQ(timestamps[4], 7000);

    co_await tsm.close();
    co_return;
}

TEST_F(TSMTest, ReadTimeRange) {
    seastar::app_template app;
    std::string filename = getTestFilePath("0_4.tsm");  // tier=0, seq=4

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testTSMReadTimeRange(filename).then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "TSM read time range test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testTSMReadMultipleSeries(std::string filename) {
    // Write multiple series
    {
        TSMWriter writer(filename);

        // Series 1
        std::vector<uint64_t> timestamps1 = {1000, 2000, 3000};
        std::vector<double> values1 = {1.1, 2.2, 3.3};
        SeriesId128 seriesId1 = SeriesId128::fromSeriesKey("metrics.cpu");
        writer.writeSeries(TSMValueType::Float, seriesId1, timestamps1, values1);

        // Series 2
        std::vector<uint64_t> timestamps2 = {1000, 2000, 3000, 4000};
        std::vector<double> values2 = {10.0, 20.0, 30.0, 40.0};
        SeriesId128 seriesId2 = SeriesId128::fromSeriesKey("metrics.memory");
        writer.writeSeries(TSMValueType::Float, seriesId2, timestamps2, values2);

        // Series 3
        std::vector<uint64_t> timestamps3 = {1000, 2000};
        std::vector<bool> values3 = {true, false};
        SeriesId128 seriesId3 = SeriesId128::fromSeriesKey("metrics.status");
        writer.writeSeries(TSMValueType::Boolean, seriesId3, timestamps3, values3);

        writer.writeIndex();
        writer.close();
    }

    // Read back all series
    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    // Read series 1
    SeriesId128 seriesId1 = SeriesId128::fromSeriesKey("metrics.cpu");
    TSMResult<double> results1(0);
    co_await tsm.readSeries(seriesId1, 0, UINT64_MAX, results1);
    auto [ts1, vals1] = results1.getAllData();
    EXPECT_EQ(vals1.size(), 3);
    EXPECT_DOUBLE_EQ(vals1[0], 1.1);

    // Read series 2
    SeriesId128 seriesId2 = SeriesId128::fromSeriesKey("metrics.memory");
    TSMResult<double> results2(0);
    co_await tsm.readSeries(seriesId2, 0, UINT64_MAX, results2);
    auto [ts2, vals2] = results2.getAllData();
    EXPECT_EQ(vals2.size(), 4);
    EXPECT_DOUBLE_EQ(vals2[3], 40.0);

    // Read series 3
    SeriesId128 seriesId3 = SeriesId128::fromSeriesKey("metrics.status");
    TSMResult<bool> results3(0);
    co_await tsm.readSeries(seriesId3, 0, UINT64_MAX, results3);
    auto [ts3, vals3] = results3.getAllData();
    EXPECT_EQ(vals3.size(), 2);
    EXPECT_EQ(vals3[0], true);

    co_await tsm.close();
    co_return;
}

TEST_F(TSMTest, ReadMultipleSeries) {
    seastar::app_template app;
    std::string filename = getTestFilePath("0_5.tsm");  // tier=0, seq=5

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testTSMReadMultipleSeries(filename).then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "TSM read multiple series test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

// TSM Tombstone and Deletion Tests

seastar::future<> testTSMDeleteRange(std::string filename) {
    // Write test data
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
        std::vector<double> values = {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.delete");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // Open TSM and delete a range
    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();
    co_await tsm.loadTombstones();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.delete");

    // Delete range 3000-7000
    bool deleted = co_await tsm.deleteRange(seriesId, 3000, 7000);
    EXPECT_TRUE(deleted);

    // Verify tombstones were created
    EXPECT_TRUE(tsm.hasTombstones());

    co_await tsm.close();
    co_return;
}

TEST_F(TSMTest, DeleteRange) {
    seastar::app_template app;
    std::string filename = getTestFilePath("0_6.tsm");  // tier=0, seq=6

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testTSMDeleteRange(filename).then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "TSM delete range test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testTSMQueryWithTombstones(std::string filename) {
    // Write test data
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000};
        std::vector<double> values = {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.tombstone");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // Open TSM, delete range, and query
    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();
    co_await tsm.loadTombstones();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.tombstone");

    // Delete middle range (3000-5000)
    bool deleted = co_await tsm.deleteRange(seriesId, 3000, 5000);
    EXPECT_TRUE(deleted);

    // Query with tombstones
    TSMResult<double> results = co_await tsm.queryWithTombstones<double>(seriesId, 0, UINT64_MAX);

    auto [timestamps, values] = results.getAllData();
    // Should have 5 values (8 original - 3 deleted: 3000, 4000, 5000)
    EXPECT_EQ(values.size(), 5);
    EXPECT_DOUBLE_EQ(values[0], 10.0);   // 1000
    EXPECT_DOUBLE_EQ(values[1], 20.0);   // 2000
    EXPECT_DOUBLE_EQ(values[2], 60.0);   // 6000
    EXPECT_DOUBLE_EQ(values[3], 70.0);   // 7000
    EXPECT_DOUBLE_EQ(values[4], 80.0);   // 8000

    co_await tsm.close();
    co_return;
}

TEST_F(TSMTest, QueryWithTombstones) {
    seastar::app_template app;
    std::string filename = getTestFilePath("0_7.tsm");  // tier=0, seq=7

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testTSMQueryWithTombstones(filename).then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "TSM query with tombstones test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

seastar::future<> testTSMLoadTombstones(std::string filename) {
    // Write test data
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000};
        std::vector<double> values = {1.0, 2.0, 3.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.load");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // First pass: create tombstones
    {
        TSM tsm(filename);
        co_await tsm.open();
        co_await tsm.readSparseIndex();
        co_await tsm.loadTombstones();

        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.load");
        co_await tsm.deleteRange(seriesId, 2000, 2000);

        EXPECT_TRUE(tsm.hasTombstones());
        co_await tsm.close();
    }

    // Second pass: reload and verify tombstones persist
    {
        TSM tsm(filename);
        co_await tsm.open();
        co_await tsm.readSparseIndex();
        co_await tsm.loadTombstones();

        // Should still have tombstones
        EXPECT_TRUE(tsm.hasTombstones());

        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.load");
        TSMResult<double> results = co_await tsm.queryWithTombstones<double>(seriesId, 0, UINT64_MAX);

        auto [timestamps, values] = results.getAllData();
        // Should have 2 values (3 original - 1 deleted)
        EXPECT_EQ(values.size(), 2);
        EXPECT_DOUBLE_EQ(values[0], 1.0);
        EXPECT_DOUBLE_EQ(values[1], 3.0);

        co_await tsm.close();
    }

    co_return;
}

TEST_F(TSMTest, LoadTombstones) {
    seastar::app_template app;
    std::string filename = getTestFilePath("0_8.tsm");  // tier=0, seq=8

    // Create proper argc/argv for Seastar
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;

    auto exitCode = app.run(argc, argv, [&] {
        return testTSMLoadTombstones(filename).then([&] {
            // Future completes successfully
        }).handle_exception([&](std::exception_ptr ep) {
            std::cerr << "TSM load tombstones test failed" << std::endl;
            throw;
        });
    });

    EXPECT_EQ(exitCode, 0);
}

// TSM Reference Counting Tests

TEST_F(TSMTest, ReferenceCountingBasic) {
    std::string filename = getTestFilePath("0_9.tsm");  // tier=0, seq=9

    // Write a minimal file
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000};
        std::vector<double> values = {1.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.ref");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // Test reference counting (synchronous operations)
    TSM tsm(filename);

    // Initial ref count should be 0
    EXPECT_EQ(tsm.getRefCount(), 0);

    // Add references
    tsm.addRef();
    EXPECT_EQ(tsm.getRefCount(), 1);

    tsm.addRef();
    EXPECT_EQ(tsm.getRefCount(), 2);

    // Release references
    tsm.releaseRef();
    EXPECT_EQ(tsm.getRefCount(), 1);

    tsm.releaseRef();
    EXPECT_EQ(tsm.getRefCount(), 0);
}

TEST_F(TSMTest, MarkForDeletion) {
    std::string filename = getTestFilePath("0_10.tsm");  // tier=0, seq=10

    // Write a minimal file
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000};
        std::vector<double> values = {1.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.mark");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);

    // Initially not scheduled for deletion
    EXPECT_FALSE(tsm.scheduleDeletionFlag);

    // Mark for deletion with no references
    tsm.markForDeletion();
    EXPECT_TRUE(tsm.scheduleDeletionFlag);
}

TEST_F(TSMTest, MarkForDeletionWithReferences) {
    std::string filename = getTestFilePath("0_11.tsm");  // tier=0, seq=11

    // Write a minimal file
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000};
        std::vector<double> values = {1.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.mark_refs");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);

    // Add a reference
    tsm.addRef();
    EXPECT_EQ(tsm.getRefCount(), 1);

    // Mark for deletion
    tsm.markForDeletion();

    // Should not be scheduled yet because there's a reference
    EXPECT_FALSE(tsm.scheduleDeletionFlag);

    // Release the reference
    tsm.releaseRef();

    // Now should be scheduled for deletion
    EXPECT_TRUE(tsm.scheduleDeletionFlag);
}