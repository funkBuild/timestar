#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_writer.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

class TSMRevisionRangeTest : public ::testing::Test {
protected:
    std::string testDir = "test_tsm_revrange";
    void SetUp() override { fs::create_directories(testDir); }
    void TearDown() override { fs::remove_all(testDir); }
    std::string path(const std::string& f) { return testDir + "/" + f; }
};

// A V4 file stamps each block's [minRev, maxRev] from the per-point revisions,
// and the reader parses them back exactly (min/max in the right order).
seastar::future<> testRevisionRangeRoundTrips(std::string p) {
    SeriesId128 series = SeriesId128::fromSeriesKey("revtest.value");
    std::vector<uint64_t> ts = {1000, 2000, 3000, 4000, 5000};
    std::vector<double> vs = {1.0, 2.0, 3.0, 4.0, 5.0};
    std::vector<uint64_t> revs = {5, 10, 3, 20, 7};  // min 3, max 20 (unordered on purpose)

    {
        TSMWriter writer(p);
        writer.writeSeries(TSMValueType::Float, series, ts, vs, revs);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(p);
    co_await tsm.open();
    EXPECT_EQ(tsm.fileFormatVersion(), 4u);

    auto* entry = co_await tsm.getFullIndexEntry(series);
    EXPECT_NE(entry, nullptr);
    if (entry && entry->indexBlocks.size() == 1u) {
        EXPECT_EQ(entry->indexBlocks[0].blockMinRev, 3u);
        EXPECT_EQ(entry->indexBlocks[0].blockMaxRev, 20u);
    } else {
        ADD_FAILURE() << "expected exactly one index block";
    }
    co_return;
}
TEST_F(TSMRevisionRangeTest, RevisionRangeRoundTrips) {
    seastar::async([&] { testRevisionRangeRoundTrips(path("0_1.tsm")).get(); }).get();
}

// Without revisions the writer leaves every block at the migrated-floor [0, 0].
seastar::future<> testNoRevisionsIsMigratedFloor(std::string p) {
    SeriesId128 series = SeriesId128::fromSeriesKey("norev.value");
    std::vector<uint64_t> ts = {10, 20, 30};
    std::vector<double> vs = {1.0, 2.0, 3.0};

    {
        TSMWriter writer(p);
        writer.writeSeries(TSMValueType::Float, series, ts, vs);  // no revisions
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(p);
    co_await tsm.open();
    EXPECT_EQ(tsm.fileFormatVersion(), 4u);
    auto* entry = co_await tsm.getFullIndexEntry(series);
    EXPECT_NE(entry, nullptr);
    if (entry && entry->indexBlocks.size() == 1u) {
        EXPECT_EQ(entry->indexBlocks[0].blockMinRev, 0u);
        EXPECT_EQ(entry->indexBlocks[0].blockMaxRev, 0u);
    } else {
        ADD_FAILURE() << "expected exactly one index block";
    }
    co_return;
}
TEST_F(TSMRevisionRangeTest, NoRevisionsIsMigratedFloor) {
    seastar::async([&] { testNoRevisionsIsMigratedFloor(path("0_2.tsm")).get(); }).get();
}

// The file-level max-revision trailer (V4) is read cheaply at open() and equals
// the maximum block revision across the whole file.
seastar::future<> testMaxRevisionTrailer(std::string p) {
    {
        TSMWriter writer(p);
        std::vector<uint64_t> ts1 = {10, 20, 30};
        std::vector<double> vs1 = {1.0, 2.0, 3.0};
        std::vector<uint64_t> rev1 = {5, 6, 7};
        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("mrt.a"), ts1, vs1, rev1);
        std::vector<uint64_t> ts2 = {10, 20};
        std::vector<double> vs2 = {1.0, 2.0};
        std::vector<uint64_t> rev2 = {40, 42};
        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("mrt.b"), ts2, vs2, rev2);
        writer.writeIndex();
        writer.close();
    }
    TSM tsm(p);
    co_await tsm.open();
    EXPECT_EQ(tsm.fileFormatVersion(), 4u);
    EXPECT_EQ(tsm.maxRevision(), 42u) << "file-level max across both series";
    co_return;
}
TEST_F(TSMRevisionRangeTest, MaxRevisionTrailer) {
    seastar::async([&] { testMaxRevisionTrailer(path("0_5.tsm")).get(); }).get();
}

seastar::future<> testMaxRevisionZeroWhenUntracked(std::string p) {
    {
        TSMWriter writer(p);
        std::vector<uint64_t> ts = {10, 20};
        std::vector<double> vs = {1.0, 2.0};
        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("mrz.a"), ts, vs);  // no revisions
        writer.writeIndex();
        writer.close();
    }
    TSM tsm(p);
    co_await tsm.open();
    EXPECT_EQ(tsm.maxRevision(), 0u) << "untracked file's max revision is the migrated floor";
    co_return;
}
TEST_F(TSMRevisionRangeTest, MaxRevisionZeroWhenUntracked) {
    seastar::async([&] { testMaxRevisionZeroWhenUntracked(path("0_6.tsm")).get(); }).get();
}

// A mismatched revisions length is rejected before any file is written.
TEST_F(TSMRevisionRangeTest, MismatchedRevisionLengthThrows) {
    TSMWriter writer(path("0_3.tsm"));
    SeriesId128 series = SeriesId128::fromSeriesKey("bad.value");
    std::vector<uint64_t> ts = {1, 2, 3};
    std::vector<double> vs = {1.0, 2.0, 3.0};
    std::vector<uint64_t> revs = {1, 2};  // too short
    EXPECT_THROW(writer.writeSeries(TSMValueType::Float, series, ts, vs, revs), std::invalid_argument);
}

}  // namespace
