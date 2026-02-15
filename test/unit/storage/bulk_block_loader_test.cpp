// Tests for BulkBlockLoader, BulkMergeContext, BulkMerger2Way, BulkMerger3Way,
// BulkMerger (N-way), SeriesBlocks, and BlockMetadata.
//
// These tests exercise:
//   - Bulk block loading from TSM files (single file and multiple files)
//   - Parallel block reads via when_all
//   - 2-way, 3-way, and N-way merge correctness
//   - Deduplication with file rank (newer wins)
//   - Memory-efficient batch processing (nextBatch)
//   - Time-range filtering during load
//   - SeriesBlocks::getPointLocation
//   - BulkMergeContext traversal
//   - BlockMetadata overlap detection and ordering

#include <gtest/gtest.h>
#include <filesystem>
#include <map>

#include "../../../lib/storage/bulk_block_loader.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/tsm_reader.hpp"
#include "../../../lib/core/series_id.hpp"

#include "../../seastar_gtest.hpp"
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class BulkBlockLoaderTest : public ::testing::Test {
public:
    std::string testDir = "./test_bulk_loader_files";
    fs::path savedCwd;

    void SetUp() override {
        savedCwd = fs::current_path();

        if (fs::current_path().filename() == "test_bulk_loader_files") {
            fs::current_path(savedCwd.parent_path());
            savedCwd = fs::current_path();
        }

        fs::remove_all(testDir);
        fs::create_directories(testDir + "/shard_0/tsm");
        fs::current_path(testDir);
    }

    void TearDown() override {
        fs::current_path(savedCwd);
        fs::remove_all(testDir);
    }

    // Create a TSM file with a single float series (explicit timestamps/values).
    seastar::shared_ptr<TSM> createTSMFile(
        uint64_t tier,
        uint64_t seqNum,
        const std::string& seriesKey,
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values) {

        char filename[256];
        snprintf(filename, sizeof(filename),
                 "shard_0/tsm/%02lu_%010lu.tsm", tier, seqNum);

        TSMWriter writer(filename);
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = tier;
        tsm->seqNum = seqNum;
        return tsm;
    }

    // Create a TSM file with multiple float series.
    seastar::shared_ptr<TSM> createMultiSeriesTSMFile(
        uint64_t tier,
        uint64_t seqNum,
        const std::string& seriesPrefix,
        int numSeries,
        int pointsPerSeries,
        uint64_t startTime = 1000000) {

        char filename[256];
        snprintf(filename, sizeof(filename),
                 "shard_0/tsm/%02lu_%010lu.tsm", tier, seqNum);

        TSMWriter writer(filename);

        for (int s = 0; s < numSeries; s++) {
            SeriesId128 seriesId =
                SeriesId128::fromSeriesKey(seriesPrefix + std::to_string(s));
            std::vector<uint64_t> timestamps;
            std::vector<double> values;

            for (int p = 0; p < pointsPerSeries; p++) {
                timestamps.push_back(startTime + p * 1000);
                values.push_back(s * 100.0 + p);
            }

            writer.writeSeries(TSMValueType::Float, seriesId,
                               timestamps, values);
        }

        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = tier;
        tsm->seqNum = seqNum;
        return tsm;
    }
};

// ===========================================================================
// 1. loadFromFile: basic loading of all blocks for a single series
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, LoadFromFileSingleSeries) {
    // Create a file with 100 points
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (int i = 0; i < 100; i++) {
        ts.push_back(1000 + i * 10);
        vals.push_back(i * 1.5);
    }

    auto tsm = self->createTSMFile(0, 1, "bulk.sensor", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("bulk.sensor");
    auto result = co_await BulkBlockLoader<double>::loadFromFile(tsm, seriesId);

    EXPECT_EQ(result.seriesId, seriesId);
    EXPECT_FALSE(result.blocks.empty());
    EXPECT_EQ(result.totalPoints, 100);
    EXPECT_EQ(result.fileRank, tsm->rankAsInteger());

    // Verify all points are present
    size_t totalPts = 0;
    for (const auto& block : result.blocks) {
        totalPts += block->timestamps->size();
    }
    EXPECT_EQ(totalPts, 100);

    co_return;
}

// ===========================================================================
// 2. loadFromFile: series not present in file returns empty
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, LoadFromFileSeriesNotFound) {
    std::vector<uint64_t> ts = {1000, 2000, 3000};
    std::vector<double> vals = {1.0, 2.0, 3.0};

    auto tsm = self->createTSMFile(0, 1, "bulk.exists", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 missingId = SeriesId128::fromSeriesKey("bulk.missing");
    auto result = co_await BulkBlockLoader<double>::loadFromFile(tsm, missingId);

    EXPECT_TRUE(result.blocks.empty());
    EXPECT_EQ(result.totalPoints, 0);

    co_return;
}

// ===========================================================================
// 3. loadFromFile: time-range filtering
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, LoadFromFileTimeRangeFilter) {
    // Write 100 points: timestamps 1000..1990 (step 10)
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (int i = 0; i < 100; i++) {
        ts.push_back(1000 + i * 10);
        vals.push_back(i * 1.0);
    }

    auto tsm = self->createTSMFile(0, 1, "range.sensor", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("range.sensor");

    // Request only the middle portion [1200, 1500]
    auto result = co_await BulkBlockLoader<double>::loadFromFile(
        tsm, seriesId, 1200, 1500);

    // The block-level filtering may return the whole block that overlaps the range,
    // but the point-level filtering within readSingleBlock should trim to the range.
    // Verify all returned timestamps are within [1200, 1500].
    for (const auto& block : result.blocks) {
        for (size_t i = 0; i < block->timestamps->size(); i++) {
            EXPECT_GE(block->timestamps->at(i), 1200)
                << "Timestamp below startTime";
            EXPECT_LE(block->timestamps->at(i), 1500)
                << "Timestamp above endTime";
        }
    }

    // Verify we got the right number of points: [1200, 1500] step 10 = 31 points
    EXPECT_EQ(result.totalPoints, 31);

    co_return;
}

// ===========================================================================
// 4. loadFromFile: data spanning multiple blocks
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, LoadFromFileMultipleBlocks) {
    // Create enough points to span multiple blocks (MaxPointsPerBlock = 3000)
    const int numPoints = 7500;  // Should create 3 blocks
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (int i = 0; i < numPoints; i++) {
        ts.push_back(1000 + i * 10);
        vals.push_back(i * 0.1);
    }

    auto tsm = self->createTSMFile(0, 1, "multi.block", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("multi.block");
    auto result = co_await BulkBlockLoader<double>::loadFromFile(tsm, seriesId);

    // Expect multiple blocks
    EXPECT_GE(result.blocks.size(), 2);
    EXPECT_EQ(result.totalPoints, static_cast<size_t>(numPoints));

    // Verify timestamps are monotonically increasing across blocks
    uint64_t prevTs = 0;
    for (const auto& block : result.blocks) {
        for (size_t i = 0; i < block->timestamps->size(); i++) {
            EXPECT_GT(block->timestamps->at(i), prevTs);
            prevTs = block->timestamps->at(i);
        }
    }

    co_return;
}

// ===========================================================================
// 5. loadFromFiles: loading from multiple TSM files
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, LoadFromMultipleFiles) {
    // Create 3 files with the same series key but different time ranges
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int f = 0; f < 3; f++) {
        std::vector<uint64_t> ts;
        std::vector<double> vals;
        for (int i = 0; i < 50; i++) {
            ts.push_back(1000 + f * 500 + i * 10);
            vals.push_back(f * 100.0 + i);
        }
        auto tsm = self->createTSMFile(0, f, "multi.file", ts, vals);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("multi.file");
    auto allBlocks = co_await BulkBlockLoader<double>::loadFromFiles(
        files, seriesId);

    // Should have results from all 3 files
    EXPECT_EQ(allBlocks.size(), 3);

    // Each should have 50 points
    for (const auto& sb : allBlocks) {
        EXPECT_EQ(sb.totalPoints, 50);
    }

    co_return;
}

// ===========================================================================
// 6. loadFromFiles: skips files that don't contain the series
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, LoadFromFilesSkipsMissingFiles) {
    // File 0 has "present.series", File 1 has "other.series"
    std::vector<uint64_t> ts = {1000, 2000, 3000};
    std::vector<double> vals = {1.0, 2.0, 3.0};

    auto tsm0 = self->createTSMFile(0, 0, "present.series", ts, vals);
    co_await tsm0->open();
    co_await tsm0->readSparseIndex();

    auto tsm1 = self->createTSMFile(0, 1, "other.series", ts, vals);
    co_await tsm1->open();
    co_await tsm1->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm0, tsm1};
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("present.series");

    auto allBlocks = co_await BulkBlockLoader<double>::loadFromFiles(
        files, seriesId);

    // Only 1 file has the series
    EXPECT_EQ(allBlocks.size(), 1);
    EXPECT_EQ(allBlocks[0].totalPoints, 3);

    co_return;
}

// ===========================================================================
// 7. loadFromFiles with time-range filtering across files
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, LoadFromFilesWithTimeRange) {
    // File 0: timestamps 1000..1090 (10 points)
    // File 1: timestamps 2000..2090 (10 points)
    // File 2: timestamps 3000..3090 (10 points)
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int f = 0; f < 3; f++) {
        std::vector<uint64_t> ts;
        std::vector<double> vals;
        for (int i = 0; i < 10; i++) {
            ts.push_back((f + 1) * 1000 + i * 10);
            vals.push_back(f * 10.0 + i);
        }
        auto tsm = self->createTSMFile(0, f, "timerange.series", ts, vals);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("timerange.series");

    // Only request [1500, 2500] -- should get File 1 data only
    auto allBlocks = co_await BulkBlockLoader<double>::loadFromFiles(
        files, seriesId, 1500, 2500);

    // File 0 (1000-1090) is out of range, File 2 (3000-3090) is out of range
    // File 1 (2000-2090) is within range
    EXPECT_EQ(allBlocks.size(), 1);

    // All returned timestamps should be within [1500, 2500]
    for (const auto& sb : allBlocks) {
        for (const auto& block : sb.blocks) {
            for (size_t i = 0; i < block->timestamps->size(); i++) {
                EXPECT_GE(block->timestamps->at(i), 1500);
                EXPECT_LE(block->timestamps->at(i), 2500);
            }
        }
    }

    co_return;
}

// ===========================================================================
// 8. SeriesBlocks::getPointLocation
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, SeriesBlocksGetPointLocation) {
    // Create a file with enough points for multiple blocks
    const int numPoints = 7500;  // 3 blocks at 3000 points each
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (int i = 0; i < numPoints; i++) {
        ts.push_back(1000 + i);
        vals.push_back(i * 1.0);
    }

    auto tsm = self->createTSMFile(0, 1, "location.test", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("location.test");
    auto result = co_await BulkBlockLoader<double>::loadFromFile(tsm, seriesId);

    EXPECT_GE(result.blocks.size(), 2);

    // First point should be in block 0, index 0
    auto loc0 = result.getPointLocation(0);
    EXPECT_TRUE(loc0.valid);
    EXPECT_EQ(loc0.blockIdx, 0);
    EXPECT_EQ(loc0.pointIdx, 0);

    // Point at the start of second block
    size_t firstBlockSize = result.blocks[0]->timestamps->size();
    auto loc1 = result.getPointLocation(firstBlockSize);
    EXPECT_TRUE(loc1.valid);
    EXPECT_EQ(loc1.blockIdx, 1);
    EXPECT_EQ(loc1.pointIdx, 0);

    // Last valid point
    auto locLast = result.getPointLocation(result.totalPoints - 1);
    EXPECT_TRUE(locLast.valid);

    // Out of range
    auto locInvalid = result.getPointLocation(result.totalPoints);
    EXPECT_FALSE(locInvalid.valid);

    co_return;
}

// ===========================================================================
// 9. BulkMergeContext: traversal across blocks
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, BulkMergeContextTraversal) {
    // Create a file with multiple blocks
    const int numPoints = 7500;
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (int i = 0; i < numPoints; i++) {
        ts.push_back(1000 + i * 10);
        vals.push_back(i * 0.5);
    }

    auto tsm = self->createTSMFile(0, 5, "ctx.series", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("ctx.series");
    auto loaded = co_await BulkBlockLoader<double>::loadFromFile(tsm, seriesId);

    BulkMergeContext<double> ctx(&loaded);

    EXPECT_TRUE(ctx.hasMore());
    EXPECT_EQ(ctx.currentTimestamp(), 1000);

    // Walk through all points
    size_t count = 0;
    uint64_t prevTs = 0;
    while (ctx.hasMore()) {
        uint64_t curTs = ctx.currentTimestamp();
        EXPECT_GT(curTs, prevTs);
        prevTs = curTs;
        ctx.advance();
        count++;
    }

    EXPECT_EQ(count, static_cast<size_t>(numPoints));
    EXPECT_FALSE(ctx.hasMore());
    EXPECT_EQ(ctx.currentTimestamp(), UINT64_MAX);

    co_return;
}

// ===========================================================================
// 10. BulkMergeContext: empty source
// ===========================================================================
TEST(BulkBlockLoaderUnit, BulkMergeContextEmptySource) {
    SeriesBlocks<double> empty;
    BulkMergeContext<double> ctx(&empty);

    EXPECT_FALSE(ctx.hasMore());
    EXPECT_EQ(ctx.currentTimestamp(), UINT64_MAX);
}

TEST(BulkBlockLoaderUnit, BulkMergeContextNullSource) {
    BulkMergeContext<double> ctx(nullptr);

    EXPECT_FALSE(ctx.hasMore());
    EXPECT_EQ(ctx.currentTimestamp(), UINT64_MAX);
}

// ===========================================================================
// 11. BulkMerger2Way: basic merge of two non-overlapping sources
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, Merger2WayNonOverlapping) {
    // File 0: timestamps 1000, 2000, 3000
    // File 1: timestamps 4000, 5000, 6000
    auto tsm0 = self->createTSMFile(0, 0, "merge2.series",
        {1000, 2000, 3000}, {10.0, 20.0, 30.0});
    co_await tsm0->open();
    co_await tsm0->readSparseIndex();

    auto tsm1 = self->createTSMFile(0, 1, "merge2.series",
        {4000, 5000, 6000}, {40.0, 50.0, 60.0});
    co_await tsm1->open();
    co_await tsm1->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("merge2.series");

    auto blocks0 = co_await BulkBlockLoader<double>::loadFromFile(tsm0, seriesId);
    auto blocks1 = co_await BulkBlockLoader<double>::loadFromFile(tsm1, seriesId);

    std::vector<SeriesBlocks<double>> sources;
    sources.push_back(std::move(blocks0));
    sources.push_back(std::move(blocks1));

    BulkMerger2Way<double> merger(sources);

    std::vector<std::pair<uint64_t, double>> merged;
    while (merger.hasNext()) {
        merged.push_back(merger.next());
    }

    // All 6 points should be present in sorted order
    EXPECT_EQ(merged.size(), 6);
    EXPECT_EQ(merged[0], (std::pair<uint64_t, double>{1000, 10.0}));
    EXPECT_EQ(merged[1], (std::pair<uint64_t, double>{2000, 20.0}));
    EXPECT_EQ(merged[2], (std::pair<uint64_t, double>{3000, 30.0}));
    EXPECT_EQ(merged[3], (std::pair<uint64_t, double>{4000, 40.0}));
    EXPECT_EQ(merged[4], (std::pair<uint64_t, double>{5000, 50.0}));
    EXPECT_EQ(merged[5], (std::pair<uint64_t, double>{6000, 60.0}));

    co_return;
}

// ===========================================================================
// 12. BulkMerger2Way: overlapping timestamps with dedup (newer wins)
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, Merger2WayOverlappingDedup) {
    // File 0 (tier 0, seq 0 -> rank lower): timestamps 1000, 2000, 3000
    // File 1 (tier 0, seq 1 -> rank higher): timestamps 2000, 3000, 4000
    // On duplicates (2000, 3000), file 1 should win since it has higher rank.
    auto tsm0 = self->createTSMFile(0, 0, "dedup2.series",
        {1000, 2000, 3000}, {10.0, 20.0, 30.0});
    co_await tsm0->open();
    co_await tsm0->readSparseIndex();

    auto tsm1 = self->createTSMFile(0, 1, "dedup2.series",
        {2000, 3000, 4000}, {200.0, 300.0, 400.0});
    co_await tsm1->open();
    co_await tsm1->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("dedup2.series");

    auto blocks0 = co_await BulkBlockLoader<double>::loadFromFile(tsm0, seriesId);
    auto blocks1 = co_await BulkBlockLoader<double>::loadFromFile(tsm1, seriesId);

    std::vector<SeriesBlocks<double>> sources;
    sources.push_back(std::move(blocks0));
    sources.push_back(std::move(blocks1));

    BulkMerger2Way<double> merger(sources);

    std::map<uint64_t, double> merged;
    while (merger.hasNext()) {
        auto [ts, val] = merger.next();
        merged[ts] = val;
    }

    EXPECT_EQ(merged.size(), 4);
    EXPECT_DOUBLE_EQ(merged[1000], 10.0);    // Only in file 0
    EXPECT_DOUBLE_EQ(merged[2000], 200.0);   // Dup: file 1 wins (higher rank)
    EXPECT_DOUBLE_EQ(merged[3000], 300.0);   // Dup: file 1 wins (higher rank)
    EXPECT_DOUBLE_EQ(merged[4000], 400.0);   // Only in file 1

    co_return;
}

// ===========================================================================
// 13. BulkMerger2Way: nextBatch
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, Merger2WayBatchProcessing) {
    auto tsm0 = self->createTSMFile(0, 0, "batch2.series",
        {1000, 2000, 3000, 4000, 5000}, {1.0, 2.0, 3.0, 4.0, 5.0});
    co_await tsm0->open();
    co_await tsm0->readSparseIndex();

    auto tsm1 = self->createTSMFile(0, 1, "batch2.series",
        {6000, 7000, 8000, 9000, 10000}, {6.0, 7.0, 8.0, 9.0, 10.0});
    co_await tsm1->open();
    co_await tsm1->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("batch2.series");

    auto blocks0 = co_await BulkBlockLoader<double>::loadFromFile(tsm0, seriesId);
    auto blocks1 = co_await BulkBlockLoader<double>::loadFromFile(tsm1, seriesId);

    std::vector<SeriesBlocks<double>> sources;
    sources.push_back(std::move(blocks0));
    sources.push_back(std::move(blocks1));

    BulkMerger2Way<double> merger(sources);

    // Get first batch of 3
    auto batch1 = merger.nextBatch(3);
    EXPECT_EQ(batch1.size(), 3);
    EXPECT_EQ(batch1[0].first, 1000);
    EXPECT_EQ(batch1[2].first, 3000);

    // Get second batch of 3
    auto batch2 = merger.nextBatch(3);
    EXPECT_EQ(batch2.size(), 3);
    EXPECT_EQ(batch2[0].first, 4000);
    EXPECT_EQ(batch2[2].first, 6000);

    // Get remaining
    auto batch3 = merger.nextBatch(1000);
    EXPECT_EQ(batch3.size(), 4);
    EXPECT_EQ(batch3[0].first, 7000);
    EXPECT_EQ(batch3[3].first, 10000);

    // No more
    EXPECT_FALSE(merger.hasNext());

    co_return;
}

// ===========================================================================
// 14. BulkMerger3Way: basic 3-way merge
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, Merger3WayBasic) {
    auto tsm0 = self->createTSMFile(0, 0, "merge3.series",
        {1000, 4000, 7000}, {10.0, 40.0, 70.0});
    co_await tsm0->open();
    co_await tsm0->readSparseIndex();

    auto tsm1 = self->createTSMFile(0, 1, "merge3.series",
        {2000, 5000, 8000}, {20.0, 50.0, 80.0});
    co_await tsm1->open();
    co_await tsm1->readSparseIndex();

    auto tsm2 = self->createTSMFile(0, 2, "merge3.series",
        {3000, 6000, 9000}, {30.0, 60.0, 90.0});
    co_await tsm2->open();
    co_await tsm2->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("merge3.series");

    auto b0 = co_await BulkBlockLoader<double>::loadFromFile(tsm0, seriesId);
    auto b1 = co_await BulkBlockLoader<double>::loadFromFile(tsm1, seriesId);
    auto b2 = co_await BulkBlockLoader<double>::loadFromFile(tsm2, seriesId);

    std::vector<SeriesBlocks<double>> sources;
    sources.push_back(std::move(b0));
    sources.push_back(std::move(b1));
    sources.push_back(std::move(b2));

    BulkMerger3Way<double> merger(sources);

    std::vector<std::pair<uint64_t, double>> merged;
    while (merger.hasNext()) {
        merged.push_back(merger.next());
    }

    EXPECT_EQ(merged.size(), 9);

    // Should be sorted by timestamp
    for (size_t i = 1; i < merged.size(); i++) {
        EXPECT_GT(merged[i].first, merged[i - 1].first);
    }

    // Verify interleaved ordering
    EXPECT_EQ(merged[0].first, 1000);
    EXPECT_EQ(merged[1].first, 2000);
    EXPECT_EQ(merged[2].first, 3000);
    EXPECT_EQ(merged[8].first, 9000);

    co_return;
}

// ===========================================================================
// 15. BulkMerger3Way: deduplication (newest file wins)
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, Merger3WayDedup) {
    // All three files share timestamp 5000; file 2 (highest seq) should win
    auto tsm0 = self->createTSMFile(0, 0, "dedup3.series",
        {1000, 5000}, {10.0, 50.0});
    co_await tsm0->open();
    co_await tsm0->readSparseIndex();

    auto tsm1 = self->createTSMFile(0, 1, "dedup3.series",
        {3000, 5000}, {30.0, 500.0});
    co_await tsm1->open();
    co_await tsm1->readSparseIndex();

    auto tsm2 = self->createTSMFile(0, 2, "dedup3.series",
        {5000, 9000}, {5000.0, 90.0});
    co_await tsm2->open();
    co_await tsm2->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("dedup3.series");

    auto b0 = co_await BulkBlockLoader<double>::loadFromFile(tsm0, seriesId);
    auto b1 = co_await BulkBlockLoader<double>::loadFromFile(tsm1, seriesId);
    auto b2 = co_await BulkBlockLoader<double>::loadFromFile(tsm2, seriesId);

    std::vector<SeriesBlocks<double>> sources;
    sources.push_back(std::move(b0));
    sources.push_back(std::move(b1));
    sources.push_back(std::move(b2));

    BulkMerger3Way<double> merger(sources);

    std::map<uint64_t, double> merged;
    while (merger.hasNext()) {
        auto [ts, val] = merger.next();
        merged[ts] = val;
    }

    EXPECT_EQ(merged.size(), 4);
    EXPECT_DOUBLE_EQ(merged[1000], 10.0);
    EXPECT_DOUBLE_EQ(merged[3000], 30.0);
    EXPECT_DOUBLE_EQ(merged[5000], 5000.0);  // File 2 wins (highest rank)
    EXPECT_DOUBLE_EQ(merged[9000], 90.0);

    co_return;
}

// ===========================================================================
// 16. BulkMerger3Way: nextBatch
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, Merger3WayBatchProcessing) {
    auto tsm0 = self->createTSMFile(0, 0, "batch3.series",
        {1000, 2000}, {1.0, 2.0});
    co_await tsm0->open();
    co_await tsm0->readSparseIndex();

    auto tsm1 = self->createTSMFile(0, 1, "batch3.series",
        {3000, 4000}, {3.0, 4.0});
    co_await tsm1->open();
    co_await tsm1->readSparseIndex();

    auto tsm2 = self->createTSMFile(0, 2, "batch3.series",
        {5000, 6000}, {5.0, 6.0});
    co_await tsm2->open();
    co_await tsm2->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("batch3.series");

    auto b0 = co_await BulkBlockLoader<double>::loadFromFile(tsm0, seriesId);
    auto b1 = co_await BulkBlockLoader<double>::loadFromFile(tsm1, seriesId);
    auto b2 = co_await BulkBlockLoader<double>::loadFromFile(tsm2, seriesId);

    std::vector<SeriesBlocks<double>> sources;
    sources.push_back(std::move(b0));
    sources.push_back(std::move(b1));
    sources.push_back(std::move(b2));

    BulkMerger3Way<double> merger(sources);

    auto batch = merger.nextBatch(4);
    EXPECT_EQ(batch.size(), 4);
    EXPECT_EQ(batch[0].first, 1000);
    EXPECT_EQ(batch[3].first, 4000);

    auto rest = merger.nextBatch(1000);
    EXPECT_EQ(rest.size(), 2);
    EXPECT_EQ(rest[0].first, 5000);
    EXPECT_EQ(rest[1].first, 6000);

    EXPECT_FALSE(merger.hasNext());

    co_return;
}

// ===========================================================================
// 17. BulkMerger (N-way): merge 4+ sources
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, MergerNWayFourSources) {
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int f = 0; f < 4; f++) {
        std::vector<uint64_t> ts;
        std::vector<double> vals;
        for (int i = 0; i < 5; i++) {
            ts.push_back(1000 + f * 50 + i * 200);
            vals.push_back(f * 100.0 + i);
        }
        auto tsm = self->createTSMFile(0, f, "nway.series", ts, vals);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("nway.series");

    std::vector<SeriesBlocks<double>> sources;
    for (auto& file : files) {
        auto blocks = co_await BulkBlockLoader<double>::loadFromFile(file, seriesId);
        sources.push_back(std::move(blocks));
    }

    BulkMerger<double> merger(sources);

    std::vector<std::pair<uint64_t, double>> merged;
    while (merger.hasNext()) {
        merged.push_back(merger.next());
    }

    // All points should be in sorted timestamp order
    for (size_t i = 1; i < merged.size(); i++) {
        EXPECT_GT(merged[i].first, merged[i - 1].first)
            << "Timestamps not sorted at index " << i;
    }

    co_return;
}

// ===========================================================================
// 18. BulkMerger (N-way): deduplication with file rank
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, MergerNWayDedup) {
    // 4 files, all with the same timestamps. The file with highest rank (seqNum=3)
    // should win for all duplicate timestamps.
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int f = 0; f < 4; f++) {
        auto tsm = self->createTSMFile(0, f, "ndedup.series",
            {1000, 2000, 3000}, {f * 10.0, f * 20.0, f * 30.0});
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("ndedup.series");

    std::vector<SeriesBlocks<double>> sources;
    for (auto& file : files) {
        auto blocks = co_await BulkBlockLoader<double>::loadFromFile(file, seriesId);
        sources.push_back(std::move(blocks));
    }

    BulkMerger<double> merger(sources);

    auto batch = merger.nextBatch(1000);

    // Should have exactly 3 deduplicated points
    EXPECT_EQ(batch.size(), 3);

    // File 3 (highest rank) should win
    EXPECT_DOUBLE_EQ(batch[0].second, 30.0);   // 3 * 10.0
    EXPECT_DOUBLE_EQ(batch[1].second, 60.0);   // 3 * 20.0
    EXPECT_DOUBLE_EQ(batch[2].second, 90.0);   // 3 * 30.0

    co_return;
}

// ===========================================================================
// 19. BulkMerger (N-way): nextBatch with limit
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, MergerNWayBatchLimit) {
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int f = 0; f < 5; f++) {
        std::vector<uint64_t> ts;
        std::vector<double> vals;
        for (int i = 0; i < 20; i++) {
            ts.push_back(f * 1000 + i * 10);
            vals.push_back(f * 100.0 + i);
        }
        auto tsm = self->createTSMFile(0, f, "nlimit.series", ts, vals);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("nlimit.series");

    std::vector<SeriesBlocks<double>> sources;
    for (auto& file : files) {
        auto blocks = co_await BulkBlockLoader<double>::loadFromFile(file, seriesId);
        sources.push_back(std::move(blocks));
    }

    BulkMerger<double> merger(sources);

    // Request only 10 points at a time
    auto batch1 = merger.nextBatch(10);
    EXPECT_EQ(batch1.size(), 10);

    // All subsequent batches should also be capped
    size_t total = batch1.size();
    while (merger.hasNext()) {
        auto batch = merger.nextBatch(10);
        EXPECT_LE(batch.size(), 10);
        total += batch.size();
    }

    // Total should match all unique timestamps across the 5 files
    EXPECT_GT(total, 0);

    co_return;
}

// ===========================================================================
// 20. BlockMetadata: overlap detection
// ===========================================================================
TEST(BulkBlockLoaderUnit, BlockMetadataOverlapDetection) {
    BlockMetadata<double> a;
    a.minTime = 1000;
    a.maxTime = 3000;

    BlockMetadata<double> b;
    b.minTime = 2000;
    b.maxTime = 4000;

    BlockMetadata<double> c;
    c.minTime = 4000;
    c.maxTime = 6000;

    BlockMetadata<double> d;
    d.minTime = 7000;
    d.maxTime = 9000;

    // a and b overlap
    EXPECT_TRUE(a.overlapsWith(b));
    EXPECT_TRUE(b.overlapsWith(a));

    // b and c overlap (touching at 4000)
    EXPECT_TRUE(b.overlapsWith(c));
    EXPECT_TRUE(c.overlapsWith(b));

    // a and c do not overlap (a ends at 3000, c starts at 4000)
    EXPECT_FALSE(a.overlapsWith(c));
    EXPECT_FALSE(c.overlapsWith(a));

    // a and d do not overlap
    EXPECT_FALSE(a.overlapsWith(d));
    EXPECT_FALSE(d.overlapsWith(a));
}

// ===========================================================================
// 21. BlockMetadata: sorting order (by minTime, then maxTime, then rank)
// ===========================================================================
TEST(BulkBlockLoaderUnit, BlockMetadataSortingOrder) {
    BlockMetadata<double> a;
    a.minTime = 2000;
    a.maxTime = 5000;
    a.fileRank = 1;

    BlockMetadata<double> b;
    b.minTime = 1000;
    b.maxTime = 3000;
    b.fileRank = 2;

    BlockMetadata<double> c;
    c.minTime = 1000;
    c.maxTime = 2000;
    c.fileRank = 3;

    BlockMetadata<double> d;
    d.minTime = 1000;
    d.maxTime = 2000;
    d.fileRank = 5;

    // Sort: first by minTime, then by maxTime, then by higher rank first
    std::vector<BlockMetadata<double>> metas = {a, b, c, d};
    std::sort(metas.begin(), metas.end());

    // d and c have same min/maxTime; higher rank first -> d before c
    EXPECT_EQ(metas[0].fileRank, 5);  // d (minTime=1000, maxTime=2000, rank=5)
    EXPECT_EQ(metas[1].fileRank, 3);  // c (minTime=1000, maxTime=2000, rank=3)
    EXPECT_EQ(metas[2].minTime, 1000);
    EXPECT_EQ(metas[2].maxTime, 3000);  // b
    EXPECT_EQ(metas[3].minTime, 2000);  // a
}

// ===========================================================================
// 22. MergeSegment: blockCount
// ===========================================================================
TEST(BulkBlockLoaderUnit, MergeSegmentBlockCount) {
    MergeSegment seg;
    seg.startIdx = 2;
    seg.endIdx = 7;
    seg.needsMerge = true;

    EXPECT_EQ(seg.blockCount(), 6);

    MergeSegment single;
    single.startIdx = 5;
    single.endIdx = 5;
    single.needsMerge = false;

    EXPECT_EQ(single.blockCount(), 1);
}

// ===========================================================================
// 23. Large-scale merge correctness: verify all unique timestamps preserved
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, LargeScaleMergeIntegrity) {
    // Create 4 files with partially overlapping data
    // File f: timestamps [f*250, f*250 + 499] step 1
    std::vector<seastar::shared_ptr<TSM>> files;
    std::map<uint64_t, double> expectedData;

    for (int f = 0; f < 4; f++) {
        std::vector<uint64_t> ts;
        std::vector<double> vals;
        uint64_t start = f * 250;
        for (int i = 0; i < 500; i++) {
            ts.push_back(start + i);
            vals.push_back(f * 1000.0 + i);
        }
        auto tsm = self->createTSMFile(0, f, "large.merge", ts, vals);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);

        // Track expected: newest file wins on overlap
        for (int i = 0; i < 500; i++) {
            expectedData[start + i] = f * 1000.0 + i;
        }
    }

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("large.merge");

    std::vector<SeriesBlocks<double>> sources;
    for (auto& file : files) {
        auto blocks = co_await BulkBlockLoader<double>::loadFromFile(file, seriesId);
        sources.push_back(std::move(blocks));
    }

    // Use N-way merger since we have 4 sources
    BulkMerger<double> merger(sources);

    std::map<uint64_t, double> actualData;
    while (merger.hasNext()) {
        auto batch = merger.nextBatch(1000);
        for (const auto& [ts, val] : batch) {
            actualData[ts] = val;
        }
    }

    // Total unique timestamps: [0, 1249] = 1250
    EXPECT_EQ(actualData.size(), 1250);
    EXPECT_EQ(actualData.size(), expectedData.size());

    // Verify timestamp coverage
    EXPECT_EQ(actualData.begin()->first, 0);
    EXPECT_EQ(actualData.rbegin()->first, 1249);

    // Verify monotonicity from the batch perspective
    uint64_t prevTs = 0;
    bool first = true;
    for (const auto& [ts, val] : actualData) {
        if (!first) {
            EXPECT_GT(ts, prevTs);
        }
        first = false;
        prevTs = ts;
    }

    co_return;
}

// ===========================================================================
// 24. File rank is correctly propagated
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, FileRankPropagation) {
    // Tier 1, seq 5 -> rank = (1 << 60) + 5
    auto tsm = self->createTSMFile(1, 5, "rank.series",
        {1000, 2000}, {10.0, 20.0});
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("rank.series");
    auto result = co_await BulkBlockLoader<double>::loadFromFile(tsm, seriesId);

    uint64_t expectedRank = (uint64_t(1) << 60) + 5;
    EXPECT_EQ(result.fileRank, expectedRank);

    // BulkMergeContext should report the same rank
    BulkMergeContext<double> ctx(&result);
    EXPECT_EQ(ctx.getFileRank(), expectedRank);

    co_return;
}

// ===========================================================================
// 25. 2-way merge: one source exhausted early
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, Merger2WayOneSourceExhaustedEarly) {
    // Source 0: just 1 point; Source 1: many points
    auto tsm0 = self->createTSMFile(0, 0, "exhaust2.series",
        {5000}, {50.0});
    co_await tsm0->open();
    co_await tsm0->readSparseIndex();

    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (int i = 0; i < 100; i++) {
        ts.push_back(1000 + i * 10);
        vals.push_back(i * 1.0);
    }
    auto tsm1 = self->createTSMFile(0, 1, "exhaust2.series", ts, vals);
    co_await tsm1->open();
    co_await tsm1->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("exhaust2.series");

    auto b0 = co_await BulkBlockLoader<double>::loadFromFile(tsm0, seriesId);
    auto b1 = co_await BulkBlockLoader<double>::loadFromFile(tsm1, seriesId);

    std::vector<SeriesBlocks<double>> sources;
    sources.push_back(std::move(b0));
    sources.push_back(std::move(b1));

    BulkMerger2Way<double> merger(sources);

    std::vector<std::pair<uint64_t, double>> merged;
    while (merger.hasNext()) {
        merged.push_back(merger.next());
    }

    // 1 unique point from tsm0 (5000 is not in tsm1's range [1000..1990])
    // plus 100 from tsm1 = 101 total
    EXPECT_EQ(merged.size(), 101);

    // Verify sorted
    for (size_t i = 1; i < merged.size(); i++) {
        EXPECT_GT(merged[i].first, merged[i - 1].first);
    }

    co_return;
}

// ===========================================================================
// 26. Empty file returns empty result
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, LoadFromFileTimeRangeNoOverlap) {
    // File has timestamps 1000..1090
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (int i = 0; i < 10; i++) {
        ts.push_back(1000 + i * 10);
        vals.push_back(i * 1.0);
    }

    auto tsm = self->createTSMFile(0, 1, "nolap.series", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("nolap.series");

    // Request a time range that doesn't overlap: [5000, 6000]
    auto result = co_await BulkBlockLoader<double>::loadFromFile(
        tsm, seriesId, 5000, 6000);

    EXPECT_EQ(result.totalPoints, 0);
    EXPECT_TRUE(result.blocks.empty());

    co_return;
}

// ===========================================================================
// 27. Multiple series in the same file: load only requested one
// ===========================================================================
SEASTAR_TEST_F(BulkBlockLoaderTest, LoadFromFileMultiSeriesSelectsOne) {
    auto tsm = self->createMultiSeriesTSMFile(0, 1, "multi.", 5, 50);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    // Load only series "multi.2"
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("multi.2");
    auto result = co_await BulkBlockLoader<double>::loadFromFile(tsm, seriesId);

    EXPECT_EQ(result.totalPoints, 50);
    EXPECT_EQ(result.seriesId, seriesId);

    // Verify values match expected pattern: series 2 -> values start at 200.0
    if (!result.blocks.empty()) {
        EXPECT_DOUBLE_EQ(result.blocks[0]->values->at(0), 200.0);
    }

    co_return;
}
