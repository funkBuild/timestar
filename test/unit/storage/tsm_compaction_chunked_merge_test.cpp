// Tests for chunked (incremental) emission on compaction's merge path.
//
// Background: the merge path used to accumulate the ENTIRE merged series in
// SeriesCompactionData::timestamps/values before handing it to the writer, so
// compaction memory scaled with series size — one of the allocations behind the
// original std::bad_alloc. Merged points are now spilled to the writer in
// bounded chunks (TSMCompactor::MERGE_CHUNK_POINTS) via TSMWriter::
// appendSeriesChunk().
//
// Two things about that are easy to get wrong, and both are pinned here:
//
//  1. appendSeriesChunk must APPEND to the series' index entry. writeSeries()
//     ASSIGNS indexEntries[seriesId], so emitting a series in several calls that
//     way would silently discard every chunk but the last — the series would come
//     back short, with no error anywhere.
//
//  2. Last-write-wins dedup must survive a chunk boundary. processPoint resolves
//     a duplicate timestamp by overwriting the last buffered value, so a spill
//     that emptied the buffer completely would leave the next duplicate with
//     nothing to overwrite and write the point twice. The spill deliberately
//     retains the final point for this reason.

#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_compactor.hpp"
#include "../../../lib/storage/tsm_file_manager.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <vector>

namespace fs = std::filesystem;

class TSMChunkedMergeTest : public ::testing::Test {
public:
    std::string testDir = "./test_compaction_chunked_merge";
    fs::path savedCwd;
    std::unique_ptr<TSMFileManager> fileManager;
    std::unique_ptr<TSMCompactor> compactor;

    void SetUp() override {
        savedCwd = fs::current_path();
        if (fs::current_path().filename() == "test_compaction_chunked_merge") {
            fs::current_path(savedCwd.parent_path());
            savedCwd = fs::current_path();
        }
        fs::remove_all(testDir);
        fs::create_directories(testDir + "/shard_0/tsm");
        fs::current_path(testDir);

        fileManager = std::make_unique<TSMFileManager>();
        compactor = std::make_unique<TSMCompactor>(fileManager.get());
    }

    void TearDown() override {
        compactor.reset();
        fileManager.reset();
        fs::current_path(savedCwd);
        fs::remove_all(testDir);
    }

    static void writeFloatFile(const std::string& filename, const SeriesId128& seriesId,
                               const std::vector<uint64_t>& ts, const std::vector<double>& vals) {
        TSMWriter writer(filename);
        writer.writeSeries(TSMValueType::Float, seriesId, ts, vals);
        writer.writeIndex();
        writer.close();
    }
};

// Overlapping inputs large enough to force MANY chunk spills. Every timestamp
// must survive exactly once, with the newer file's value winning the overlap.
seastar::future<> testChunkedMergePreservesAllPoints(TSMChunkedMergeTest* self) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("chunked.merge,host=a#value");

    // ~600K points total, well past MERGE_CHUNK_POINTS (256K), so the merge
    // spills several times rather than buffering the series.
    constexpr size_t kPointsPerFile = 400000;
    constexpr size_t kOverlap = 100000;  // trailing region of file A re-written by B

    std::vector<uint64_t> tsA, tsB;
    std::vector<double> valsA, valsB;
    tsA.reserve(kPointsPerFile);
    valsA.reserve(kPointsPerFile);
    for (size_t i = 0; i < kPointsPerFile; ++i) {
        tsA.push_back(1000ull + static_cast<uint64_t>(i) * 1000ull);
        valsA.push_back(1.0);  // "old" value
    }
    // File B starts inside A's range, so the two genuinely overlap in time and
    // the compaction cannot take the zero-copy carry.
    const size_t bStart = kPointsPerFile - kOverlap;
    tsB.reserve(kPointsPerFile);
    valsB.reserve(kPointsPerFile);
    for (size_t i = 0; i < kPointsPerFile; ++i) {
        tsB.push_back(1000ull + static_cast<uint64_t>(bStart + i) * 1000ull);
        valsB.push_back(2.0);  // "new" value: must win every duplicate
    }

    const uint64_t expectedDistinct = kPointsPerFile + kPointsPerFile - kOverlap;
    const uint64_t firstOverlapTs = tsB.front();

    TSMChunkedMergeTest::writeFloatFile("shard_0/tsm/0_1.tsm", seriesId, tsA, valsA);
    TSMChunkedMergeTest::writeFloatFile("shard_0/tsm/0_2.tsm", seriesId, tsB, valsB);

    auto fileA = seastar::make_shared<TSM>("shard_0/tsm/0_1.tsm");
    auto fileB = seastar::make_shared<TSM>("shard_0/tsm/0_2.tsm");
    co_await fileA->open();
    co_await fileB->open();

    std::vector<seastar::shared_ptr<TSM>> files{fileA, fileB};
    auto result = co_await self->compactor->compact(files);
    EXPECT_FALSE(result.outputPath.empty());
    if (result.outputPath.empty()) {
        co_await fileA->close();
        co_await fileB->close();
        co_return;
    }

    co_await fileA->close();
    co_await fileB->close();

    auto merged = seastar::make_shared<TSM>(result.outputPath);
    co_await merged->open();

    auto* entry = co_await merged->getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr) << "series missing from compacted file -- chunks were dropped, not appended";
    if (!entry) {
        co_await merged->close();
        co_return;
    }
    EXPECT_GT(entry->indexBlocks.size(), 1u);

    // Every point must be present exactly once, in ascending order, with the
    // newer file's value across the overlap.
    TSMResult<double> readback(0);
    co_await merged->readSeries<double>(seriesId, 0, std::numeric_limits<uint64_t>::max(), readback);

    std::vector<uint64_t> outTs;
    std::vector<double> outVals;
    for (const auto& block : readback.blocks) {
        for (size_t i = 0; i < block->timestamps.size(); ++i) {
            outTs.push_back(block->timestamps[i]);
            outVals.push_back(block->values[i]);
        }
    }

    EXPECT_EQ(outTs.size(), expectedDistinct) << "chunked merge lost or duplicated points";
    EXPECT_EQ(outTs.size(), outVals.size());

    bool ascending = true;
    bool duplicates = false;
    for (size_t i = 1; i < outTs.size(); ++i) {
        if (outTs[i] < outTs[i - 1])
            ascending = false;
        if (outTs[i] == outTs[i - 1])
            duplicates = true;
    }
    EXPECT_TRUE(ascending) << "chunked merge emitted out-of-order timestamps";
    EXPECT_FALSE(duplicates) << "chunked merge emitted duplicate timestamps across a chunk boundary";

    // Last-write-wins across the whole overlap, including wherever a chunk
    // boundary happened to land inside it.
    size_t overlapChecked = 0;
    for (size_t i = 0; i < outTs.size(); ++i) {
        if (outTs[i] >= firstOverlapTs) {
            EXPECT_DOUBLE_EQ(outVals[i], 2.0) << "stale value survived at ts=" << outTs[i];
            ++overlapChecked;
        } else {
            EXPECT_DOUBLE_EQ(outVals[i], 1.0) << "unexpected value before the overlap at ts=" << outTs[i];
        }
    }
    EXPECT_GT(overlapChecked, kOverlap) << "overlap region not actually exercised";

    co_await merged->close();
}

TEST_F(TSMChunkedMergeTest, ChunkedMergePreservesAllPointsAndLastWriteWins) {
    seastar::async([&] { testChunkedMergePreservesAllPoints(this).get(); }).get();
}

// Overlap that STRADDLES a run boundary.
//
// The merge groups blocks into maximal time-connected runs. An earlier design
// split on whether CONSECUTIVE blocks overlap, which is subtly wrong: when a run
// of non-overlapping blocks is followed by an overlapping one, the pair that
// actually overlaps sits either side of the split, so the two blocks would be
// merged in different groups and never compared — silently losing last-write-wins
// between them.
//
// Here file B covers a narrow window in the MIDDLE of file A, so the overlap is
// interior rather than at either end, and B is the newer file so its values must
// win exactly across that window and nowhere else.
seastar::future<> testInteriorOverlapWins(TSMChunkedMergeTest* self) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("interior.overlap,host=a#value");

    constexpr size_t kBasePoints = 60000;
    constexpr size_t kWindowStart = 25000;
    constexpr size_t kWindowLen = 5000;

    std::vector<uint64_t> tsA;
    std::vector<double> valsA;
    for (size_t i = 0; i < kBasePoints; ++i) {
        tsA.push_back(1000ull + static_cast<uint64_t>(i) * 1000ull);
        valsA.push_back(1.0);
    }
    std::vector<uint64_t> tsB;
    std::vector<double> valsB;
    for (size_t i = 0; i < kWindowLen; ++i) {
        tsB.push_back(1000ull + static_cast<uint64_t>(kWindowStart + i) * 1000ull);
        valsB.push_back(2.0);
    }

    const uint64_t windowLo = tsB.front();
    const uint64_t windowHi = tsB.back();

    TSMChunkedMergeTest::writeFloatFile("shard_0/tsm/0_1.tsm", seriesId, tsA, valsA);
    TSMChunkedMergeTest::writeFloatFile("shard_0/tsm/0_2.tsm", seriesId, tsB, valsB);

    auto fileA = seastar::make_shared<TSM>("shard_0/tsm/0_1.tsm");
    auto fileB = seastar::make_shared<TSM>("shard_0/tsm/0_2.tsm");
    co_await fileA->open();
    co_await fileB->open();

    std::vector<seastar::shared_ptr<TSM>> files{fileA, fileB};
    auto result = co_await self->compactor->compact(files);
    EXPECT_FALSE(result.outputPath.empty());
    co_await fileA->close();
    co_await fileB->close();
    if (result.outputPath.empty()) {
        co_return;
    }

    auto merged = seastar::make_shared<TSM>(result.outputPath);
    co_await merged->open();

    TSMResult<double> readback(0);
    co_await merged->readSeries<double>(seriesId, 0, std::numeric_limits<uint64_t>::max(), readback);

    std::vector<uint64_t> outTs;
    std::vector<double> outVals;
    for (const auto& block : readback.blocks) {
        for (size_t i = 0; i < block->timestamps.size(); ++i) {
            outTs.push_back(block->timestamps[i]);
            outVals.push_back(block->values[i]);
        }
    }

    // B adds no new timestamps, so the merged series is exactly A's length.
    EXPECT_EQ(outTs.size(), kBasePoints) << "interior overlap changed the point count";

    size_t winners = 0;
    for (size_t i = 0; i < outTs.size(); ++i) {
        const bool inWindow = outTs[i] >= windowLo && outTs[i] <= windowHi;
        if (inWindow) {
            EXPECT_DOUBLE_EQ(outVals[i], 2.0) << "stale value survived inside the interior overlap at ts=" << outTs[i];
            ++winners;
        } else {
            EXPECT_DOUBLE_EQ(outVals[i], 1.0) << "value overwritten outside the overlap at ts=" << outTs[i];
        }
    }
    EXPECT_EQ(winners, kWindowLen) << "overlap window not fully covered";

    co_await merged->close();
}

TEST_F(TSMChunkedMergeTest, InteriorOverlapStraddlingGroupBoundaryStillWins) {
    seastar::async([&] { testInteriorOverlapWins(this).get(); }).get();
}
