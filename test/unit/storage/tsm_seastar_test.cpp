// Seastar-based tests for TSM read and tombstone operations

#include <gtest/gtest.h>
#include <filesystem>

#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"

#include <seastar/core/coroutine.hh>

namespace fs = std::filesystem;

class TSMSeastarTest : public ::testing::Test {
protected:
    std::string testDir = "./test_tsm_seastar_files";

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

seastar::future<> testTSMReadFloat(std::string filename) {
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000};
        std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.series");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.series");
    auto indexEntry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(indexEntry, nullptr);

    auto seriesType = tsm.getSeriesType(seriesId);
    EXPECT_TRUE(seriesType.has_value());
    EXPECT_EQ(seriesType.value(), TSMValueType::Float);

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
}

TEST_F(TSMSeastarTest, ReadFloatData) {
    testTSMReadFloat(getTestFilePath("0_1.tsm")).get();
}

seastar::future<> testTSMReadBoolean(std::string filename) {
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000};
        std::vector<bool> values = {true, false, true, false};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.bool");
        writer.writeSeries(TSMValueType::Boolean, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.bool");
    auto indexEntry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(indexEntry, nullptr);

    auto seriesType = tsm.getSeriesType(seriesId);
    EXPECT_TRUE(seriesType.has_value());
    EXPECT_EQ(seriesType.value(), TSMValueType::Boolean);

    TSMResult<bool> results(0);
    co_await tsm.readSeries(seriesId, 0, UINT64_MAX, results);

    auto [timestamps, values] = results.getAllData();
    EXPECT_EQ(values.size(), 4);
    EXPECT_EQ(values[0], true);
    EXPECT_EQ(values[1], false);
    EXPECT_EQ(values[2], true);
    EXPECT_EQ(values[3], false);

    co_await tsm.close();
}

TEST_F(TSMSeastarTest, ReadBooleanData) {
    testTSMReadBoolean(getTestFilePath("0_2.tsm")).get();
}

seastar::future<> testTSMReadString(std::string filename) {
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000};
        std::vector<std::string> values = {"hello", "world", "test"};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.string");
        writer.writeSeries(TSMValueType::String, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.string");
    auto indexEntry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(indexEntry, nullptr);

    auto seriesType = tsm.getSeriesType(seriesId);
    EXPECT_TRUE(seriesType.has_value());
    EXPECT_EQ(seriesType.value(), TSMValueType::String);

    TSMResult<std::string> results(0);
    co_await tsm.readSeries(seriesId, 0, UINT64_MAX, results);

    auto [timestamps, values] = results.getAllData();
    EXPECT_EQ(values.size(), 3);
    EXPECT_EQ(values[0], "hello");
    EXPECT_EQ(values[1], "world");
    EXPECT_EQ(values[2], "test");

    co_await tsm.close();
}

TEST_F(TSMSeastarTest, ReadStringData) {
    testTSMReadString(getTestFilePath("0_3.tsm")).get();
}

seastar::future<> testTSMReadTimeRange(std::string filename) {
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
        std::vector<double> values = {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.range");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.range");

    TSMResult<double> results(0);
    co_await tsm.readSeries(seriesId, 3000, 7000, results);

    auto [timestamps, values] = results.getAllData();
    EXPECT_EQ(values.size(), 5);
    EXPECT_DOUBLE_EQ(values[0], 30.0);
    EXPECT_DOUBLE_EQ(values[4], 70.0);
    EXPECT_EQ(timestamps[0], 3000);
    EXPECT_EQ(timestamps[4], 7000);

    co_await tsm.close();
}

TEST_F(TSMSeastarTest, ReadTimeRange) {
    testTSMReadTimeRange(getTestFilePath("0_4.tsm")).get();
}

seastar::future<> testTSMReadMultipleSeries(std::string filename) {
    {
        TSMWriter writer(filename);

        std::vector<uint64_t> timestamps1 = {1000, 2000, 3000};
        std::vector<double> values1 = {1.1, 2.2, 3.3};
        SeriesId128 seriesId1 = SeriesId128::fromSeriesKey("metrics.cpu");
        writer.writeSeries(TSMValueType::Float, seriesId1, timestamps1, values1);

        std::vector<uint64_t> timestamps2 = {1000, 2000, 3000, 4000};
        std::vector<double> values2 = {10.0, 20.0, 30.0, 40.0};
        SeriesId128 seriesId2 = SeriesId128::fromSeriesKey("metrics.memory");
        writer.writeSeries(TSMValueType::Float, seriesId2, timestamps2, values2);

        std::vector<uint64_t> timestamps3 = {1000, 2000};
        std::vector<bool> values3 = {true, false};
        SeriesId128 seriesId3 = SeriesId128::fromSeriesKey("metrics.status");
        writer.writeSeries(TSMValueType::Boolean, seriesId3, timestamps3, values3);

        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    SeriesId128 seriesId1 = SeriesId128::fromSeriesKey("metrics.cpu");
    TSMResult<double> results1(0);
    co_await tsm.readSeries(seriesId1, 0, UINT64_MAX, results1);
    auto [ts1, vals1] = results1.getAllData();
    EXPECT_EQ(vals1.size(), 3);
    EXPECT_DOUBLE_EQ(vals1[0], 1.1);

    SeriesId128 seriesId2 = SeriesId128::fromSeriesKey("metrics.memory");
    TSMResult<double> results2(0);
    co_await tsm.readSeries(seriesId2, 0, UINT64_MAX, results2);
    auto [ts2, vals2] = results2.getAllData();
    EXPECT_EQ(vals2.size(), 4);
    EXPECT_DOUBLE_EQ(vals2[3], 40.0);

    SeriesId128 seriesId3 = SeriesId128::fromSeriesKey("metrics.status");
    TSMResult<bool> results3(0);
    co_await tsm.readSeries(seriesId3, 0, UINT64_MAX, results3);
    auto [ts3, vals3] = results3.getAllData();
    EXPECT_EQ(vals3.size(), 2);
    EXPECT_EQ(vals3[0], true);

    co_await tsm.close();
}

TEST_F(TSMSeastarTest, ReadMultipleSeries) {
    testTSMReadMultipleSeries(getTestFilePath("0_5.tsm")).get();
}

seastar::future<> testTSMDeleteRange(std::string filename) {
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
        std::vector<double> values = {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.delete");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();
    co_await tsm.loadTombstones();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.delete");

    bool deleted = co_await tsm.deleteRange(seriesId, 3000, 7000);
    EXPECT_TRUE(deleted);
    EXPECT_TRUE(tsm.hasTombstones());

    co_await tsm.close();
}

TEST_F(TSMSeastarTest, DeleteRange) {
    testTSMDeleteRange(getTestFilePath("0_6.tsm")).get();
}

seastar::future<> testTSMQueryWithTombstones(std::string filename) {
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000};
        std::vector<double> values = {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.tombstone");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();
    co_await tsm.loadTombstones();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.tombstone");

    bool deleted = co_await tsm.deleteRange(seriesId, 3000, 5000);
    EXPECT_TRUE(deleted);

    TSMResult<double> results = co_await tsm.queryWithTombstones<double>(seriesId, 0, UINT64_MAX);

    auto [timestamps, values] = results.getAllData();
    EXPECT_EQ(values.size(), 5);
    EXPECT_DOUBLE_EQ(values[0], 10.0);
    EXPECT_DOUBLE_EQ(values[1], 20.0);
    EXPECT_DOUBLE_EQ(values[2], 60.0);
    EXPECT_DOUBLE_EQ(values[3], 70.0);
    EXPECT_DOUBLE_EQ(values[4], 80.0);

    co_await tsm.close();
}

TEST_F(TSMSeastarTest, QueryWithTombstones) {
    testTSMQueryWithTombstones(getTestFilePath("0_7.tsm")).get();
}

seastar::future<> testTSMLoadTombstones(std::string filename) {
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000};
        std::vector<double> values = {1.0, 2.0, 3.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.load");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

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

    {
        TSM tsm(filename);
        co_await tsm.open();
        co_await tsm.readSparseIndex();
        co_await tsm.loadTombstones();

        EXPECT_TRUE(tsm.hasTombstones());

        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.load");
        TSMResult<double> results = co_await tsm.queryWithTombstones<double>(seriesId, 0, UINT64_MAX);

        auto [timestamps, values] = results.getAllData();
        EXPECT_EQ(values.size(), 2);
        EXPECT_DOUBLE_EQ(values[0], 1.0);
        EXPECT_DOUBLE_EQ(values[1], 3.0);

        co_await tsm.close();
    }
}

TEST_F(TSMSeastarTest, LoadTombstones) {
    testTSMLoadTombstones(getTestFilePath("0_8.tsm")).get();
}
