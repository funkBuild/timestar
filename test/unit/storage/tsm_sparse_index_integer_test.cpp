// Regression test for integer sparse index offset mismatch.
//
// BUG: readSparseIndex() read Integer first/latest values at byte offsets
// 52 and 60, but the writer serializes them at 56 and 64. This produced
// corrupted values for zero-I/O LATEST/FIRST on Integer series.

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_writer.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>

namespace fs = std::filesystem;

class TSMSparseIndexIntegerTest : public ::testing::Test {
protected:
    std::string testDir = "./test_tsm_sparse_int";
    void SetUp() override { fs::create_directories(testDir); }
    void TearDown() override { fs::remove_all(testDir); }
    std::string path(const std::string& f) { return testDir + "/" + f; }
};

static seastar::future<> testIntegerSparseFirstLatest(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("cpu,host=a usage_idle");

    // Write an Integer series with known first and latest values
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000, 4000, 5000};
        std::vector<int64_t> values = {100, 200, 300, 400, 500};
        writer.writeSeries(TSMValueType::Integer, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    // The sparse index should have correct first and latest values
    auto first = tsm.getFirstFromSparse(seriesId);
    auto latest = tsm.getLatestFromSparse(seriesId);

    EXPECT_TRUE(first.has_value()) << "First should be available from sparse index";
    EXPECT_TRUE(latest.has_value()) << "Latest should be available from sparse index";

    if (first.has_value()) {
        EXPECT_EQ(first->timestamp, 1000u);
        EXPECT_DOUBLE_EQ(first->value, 100.0)
            << "First value should be 100 (int64 -> double). "
               "If this is wrong, readSparseIndex is reading at the wrong offset.";
    }

    if (latest.has_value()) {
        EXPECT_EQ(latest->timestamp, 5000u);
        EXPECT_DOUBLE_EQ(latest->value, 500.0)
            << "Latest value should be 500 (int64 -> double). "
               "If this is wrong, readSparseIndex is reading at the wrong offset.";
    }

    co_await tsm.close();
}

TEST_F(TSMSparseIndexIntegerTest, FirstAndLatestValuesCorrect) {
    testIntegerSparseFirstLatest(path("0_1.tsm")).get();
}

static seastar::future<> testIntegerSparseDistinguishableValues(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test,t=1 field");

    // Use values where blockMax != blockFirstValue to expose the offset bug.
    // If the reader reads at offset 52 (wrong), it gets 4 bytes of blockMax
    // concatenated with 4 bytes of blockFirstValue — a garbage value.
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000, 2000, 3000};
        // first=42, max=999, latest=7 — all different so misaligned reads are detectable
        std::vector<int64_t> values = {42, 999, 7};
        writer.writeSeries(TSMValueType::Integer, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    auto first = tsm.getFirstFromSparse(seriesId);
    auto latest = tsm.getLatestFromSparse(seriesId);

    EXPECT_TRUE(first.has_value());
    EXPECT_TRUE(latest.has_value());

    if (first.has_value()) {
        EXPECT_DOUBLE_EQ(first->value, 42.0)
            << "First value must be 42. A wrong value indicates offset mismatch "
               "(reading blockMax bytes instead of blockFirstValue).";
    }
    if (latest.has_value()) {
        EXPECT_DOUBLE_EQ(latest->value, 7.0)
            << "Latest value must be 7. A wrong value indicates offset mismatch.";
    }

    co_await tsm.close();
}

TEST_F(TSMSparseIndexIntegerTest, DistinguishableFirstLatestValues) {
    testIntegerSparseDistinguishableValues(path("0_2.tsm")).get();
}
