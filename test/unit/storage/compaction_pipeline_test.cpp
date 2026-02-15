// Tests for SeriesPrefetchManager (lib/storage/compaction_pipeline.hpp).
//
// Covers:
//   - Construction and initialization
//   - Series prefetch queue management (hasNext, getNext, getPrefetchQueueSize)
//   - Pipelined I/O and computation overlap
//   - Memory usage tracking fields
//   - Prefetch depth variations
//   - Edge cases (empty series list, single series, large series count)
//   - Iteration through all series via getNext loop

#include <gtest/gtest.h>
#include <filesystem>
#include <set>
#include <map>

#include "../../../lib/storage/compaction_pipeline.hpp"
#include "../../../lib/storage/tsm_compactor.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/tsm_reader.hpp"
#include "../../../lib/core/series_id.hpp"

#include "../../seastar_gtest.hpp"
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class CompactionPipelineTest : public ::testing::Test {
public:
    std::string testDir = "./test_compaction_pipeline_files";
    fs::path savedCwd;

    void SetUp() override {
        savedCwd = fs::current_path();

        // Guard against leftover directory from a crashed run.
        if (fs::current_path().filename() == "test_compaction_pipeline_files") {
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

    // Create a TSM file containing `numSeries` float series, each with
    // `pointsPerSeries` data points starting at `startTime` with stride 1000.
    seastar::shared_ptr<TSM> createTestTSMFile(
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

    // Collect all unique series IDs from a set of opened TSM files.
    std::vector<SeriesId128> getAllSeriesIds(
        const std::vector<seastar::shared_ptr<TSM>>& files) {
        std::set<SeriesId128> uniqueIds;
        for (const auto& file : files) {
            auto ids = file->getSeriesIds();
            for (const auto& id : ids) {
                uniqueIds.insert(id);
            }
        }
        return std::vector<SeriesId128>(uniqueIds.begin(), uniqueIds.end());
    }

    // Pre-load full index entries into the TSM cache for all series across
    // all files.  TSMBlockIterator::init() calls getSeriesBlocks() which
    // only returns data already in fullIndexCache; callers must ensure each
    // series has been loaded via getFullIndexEntry() beforehand.
    static seastar::future<> preloadFullIndex(
        const std::vector<seastar::shared_ptr<TSM>>& files,
        const std::vector<SeriesId128>& seriesIds) {
        for (const auto& file : files) {
            for (const auto& seriesId : seriesIds) {
                co_await file->getFullIndexEntry(seriesId);
            }
        }
    }
};

// ===========================================================================
// 1. Empty series list: hasNext is false, getNext returns nullptr
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, EmptySeriesList) {
    auto tsm = self->createTestTSMFile(0, 0, "empty.", 1, 10);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    std::vector<SeriesId128> emptySeries;

    SeriesPrefetchManager<double> manager(files, emptySeries, 2);
    co_await manager.init();

    EXPECT_FALSE(manager.hasNext());
    EXPECT_EQ(manager.getPrefetchQueueSize(), 0);

    auto iter = co_await manager.getNext();
    EXPECT_EQ(iter, nullptr);

    co_return;
}

// ===========================================================================
// 2. Single series: prefetch and retrieval
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, SingleSeries) {
    auto tsm = self->createTestTSMFile(0, 0, "single.", 1, 20);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 1);
    if (allSeries.empty()) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    SeriesPrefetchManager<double> manager(files, allSeries, 2);
    co_await manager.init();

    EXPECT_TRUE(manager.hasNext());

    auto iter = co_await manager.getNext();
    EXPECT_NE(iter, nullptr);

    // The iterator should yield data
    EXPECT_TRUE(iter->hasNext());

    // Drain the iterator and count points
    size_t pointCount = 0;
    while (iter->hasNext()) {
        auto [ts, val] = co_await iter->next();
        pointCount++;
    }
    EXPECT_EQ(pointCount, 20);

    // No more series
    EXPECT_FALSE(manager.hasNext());
    auto nullIter = co_await manager.getNext();
    EXPECT_EQ(nullIter, nullptr);

    co_return;
}

// ===========================================================================
// 3. Multiple series with default prefetch depth (2)
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, MultipleSeriesDefaultDepth) {
    auto tsm = self->createTestTSMFile(0, 0, "multi.", 5, 10);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 5);
    if (allSeries.size() != 5) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    // Default prefetch depth = 2
    SeriesPrefetchManager<double> manager(files, allSeries);
    co_await manager.init();

    EXPECT_TRUE(manager.hasNext());

    // After init(), prefetchDepth (2) items should be queued
    EXPECT_EQ(manager.getPrefetchQueueSize(), 2);

    // Iterate through all series
    size_t seriesCount = 0;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);

        // Each series should have data
        EXPECT_TRUE(iter->hasNext());

        // Drain the iterator
        size_t pointCount = 0;
        while (iter->hasNext()) {
            co_await iter->next();
            pointCount++;
        }
        EXPECT_EQ(pointCount, 10);

        seriesCount++;
    }

    EXPECT_EQ(seriesCount, 5);

    co_return;
}

// ===========================================================================
// 4. Prefetch depth = 1 (minimal lookahead)
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, PrefetchDepthOne) {
    auto tsm = self->createTestTSMFile(0, 0, "depth1.", 4, 15);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 4);
    if (allSeries.size() != 4) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    SeriesPrefetchManager<double> manager(files, allSeries, 1);
    co_await manager.init();

    // With depth=1, only 1 item prefetched
    EXPECT_EQ(manager.getPrefetchQueueSize(), 1);

    size_t seriesCount = 0;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);
        seriesCount++;
    }
    EXPECT_EQ(seriesCount, 4);

    co_return;
}

// ===========================================================================
// 5. Prefetch depth larger than series count
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, PrefetchDepthLargerThanSeriesCount) {
    auto tsm = self->createTestTSMFile(0, 0, "large_depth.", 3, 10);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 3);
    if (allSeries.size() != 3) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    // Prefetch depth (10) > series count (3) -- should be clamped
    SeriesPrefetchManager<double> manager(files, allSeries, 10);
    co_await manager.init();

    // All 3 series should be prefetched (min of depth and count)
    EXPECT_EQ(manager.getPrefetchQueueSize(), 3);
    EXPECT_TRUE(manager.hasNext());

    // Iterate through all
    size_t seriesCount = 0;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);
        seriesCount++;
    }
    EXPECT_EQ(seriesCount, 3);

    co_return;
}

// ===========================================================================
// 6. Multiple files: merge iterators span several TSM files
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, MultipleFiles) {
    // Create 3 files each containing the same 2 series with different time ranges.
    // The merge iterator should merge data from all files for each series.
    std::vector<seastar::shared_ptr<TSM>> files;
    for (int i = 0; i < 3; i++) {
        auto tsm = self->createTestTSMFile(
            0, i, "mf.", 2, 10, 1000000 + i * 10000);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 2);
    if (allSeries.size() != 2) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    SeriesPrefetchManager<double> manager(files, allSeries, 2);
    co_await manager.init();

    size_t seriesCount = 0;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);

        // Each series has 10 points in each of 3 files with non-overlapping
        // timestamps, so the merged result should have 30 points total.
        size_t pointCount = 0;
        while (iter->hasNext()) {
            co_await iter->next();
            pointCount++;
        }
        EXPECT_EQ(pointCount, 30);

        seriesCount++;
    }
    EXPECT_EQ(seriesCount, 2);

    co_return;
}

// ===========================================================================
// 7. Overlapping timestamps across files: dedup via merge iterator
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, OverlappingTimestampsDedup) {
    // Create 2 files with the same series and fully overlapping timestamps.
    // File with higher seqNum (rank) wins in the merge iterator.
    std::vector<seastar::shared_ptr<TSM>> files;
    for (int i = 0; i < 2; i++) {
        auto tsm = self->createTestTSMFile(
            0, i, "overlap.", 1, 10, 1000000);  // Same start time
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 1);
    if (allSeries.size() != 1) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    SeriesPrefetchManager<double> manager(files, allSeries, 2);
    co_await manager.init();

    auto iter = co_await manager.getNext();
    EXPECT_NE(iter, nullptr);

    // With nextBatch, duplicate timestamps are resolved.
    // The merged result should have exactly 10 unique timestamps.
    auto batch = co_await iter->nextBatch(100);
    EXPECT_EQ(batch.size(), 10);

    co_return;
}

// ===========================================================================
// 8. Prefetch queue size decreases as series are consumed
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, PrefetchQueueDraining) {
    auto tsm = self->createTestTSMFile(0, 0, "drain.", 6, 5);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 6);
    if (allSeries.size() != 6) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    SeriesPrefetchManager<double> manager(files, allSeries, 3);
    co_await manager.init();

    // After init: 3 items prefetched
    EXPECT_EQ(manager.getPrefetchQueueSize(), 3);

    // Consume first series: queue should remain at 3 (one consumed, one added)
    auto iter1 = co_await manager.getNext();
    EXPECT_NE(iter1, nullptr);
    EXPECT_EQ(manager.getPrefetchQueueSize(), 3);

    // Consume second: still 3 (one consumed, one added)
    auto iter2 = co_await manager.getNext();
    EXPECT_NE(iter2, nullptr);
    EXPECT_EQ(manager.getPrefetchQueueSize(), 3);

    // Consume third: now only 3 remain total (indices 3,4,5), 3 were prefetched,
    // we consumed index 2, and no more to add (index 5 was already added).
    // Queue: was 3, consumed one, can't add more (index 5 already prefetched) = 2
    auto iter3 = co_await manager.getNext();
    EXPECT_NE(iter3, nullptr);

    // Consume fourth
    auto iter4 = co_await manager.getNext();
    EXPECT_NE(iter4, nullptr);

    // Consume fifth
    auto iter5 = co_await manager.getNext();
    EXPECT_NE(iter5, nullptr);

    // Consume sixth (last)
    auto iter6 = co_await manager.getNext();
    EXPECT_NE(iter6, nullptr);

    // Queue should be empty now
    EXPECT_EQ(manager.getPrefetchQueueSize(), 0);
    EXPECT_FALSE(manager.hasNext());

    co_return;
}

// ===========================================================================
// 9. Memory tracking fields are initialized correctly
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, MemoryTrackingDefaults) {
    auto tsm = self->createTestTSMFile(0, 0, "mem.", 1, 5);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    // Default max memory is 256MB
    SeriesPrefetchManager<double> manager(files, allSeries);
    co_await manager.init();

    // Manager should function correctly with default memory settings
    EXPECT_TRUE(manager.hasNext());
    auto iter = co_await manager.getNext();
    EXPECT_NE(iter, nullptr);

    co_return;
}

// ===========================================================================
// 10. Custom memory limit constructor parameter
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, CustomMemoryLimit) {
    auto tsm = self->createTestTSMFile(0, 0, "cmem.", 2, 10);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    // Custom memory limit: 64MB, depth 1
    size_t customMemory = 64 * 1024 * 1024;
    SeriesPrefetchManager<double> manager(files, allSeries, 1, customMemory);
    co_await manager.init();

    // Should still function correctly
    size_t seriesCount = 0;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);

        // Drain the iterator
        while (iter->hasNext()) {
            co_await iter->next();
        }
        seriesCount++;
    }
    EXPECT_EQ(seriesCount, 2);

    co_return;
}

// ===========================================================================
// 11. Series ordering is preserved through the pipeline
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, SeriesOrderPreserved) {
    auto tsm = self->createTestTSMFile(0, 0, "order.", 4, 5);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 4);
    if (allSeries.size() != 4) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    SeriesPrefetchManager<double> manager(files, allSeries, 2);
    co_await manager.init();

    // Verify that series come out in the same order they were given
    std::vector<SeriesId128> retrievedOrder;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);
        retrievedOrder.push_back(iter->getSeriesId());
    }

    EXPECT_EQ(retrievedOrder.size(), allSeries.size());
    for (size_t i = 0; i < std::min(retrievedOrder.size(), allSeries.size()); i++) {
        EXPECT_EQ(retrievedOrder[i], allSeries[i])
            << "Series at index " << i << " does not match expected order";
    }

    co_return;
}

// ===========================================================================
// 12. Large number of series with small prefetch depth
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, ManySeriesSmallDepth) {
    // 20 series, prefetch depth 2 -- tests repeated queue fill/drain cycles
    auto tsm = self->createTestTSMFile(0, 0, "many.", 20, 5);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 20);
    if (allSeries.size() != 20) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    SeriesPrefetchManager<double> manager(files, allSeries, 2);
    co_await manager.init();

    size_t seriesCount = 0;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);

        // Each series should have exactly 5 points
        size_t pointCount = 0;
        while (iter->hasNext()) {
            co_await iter->next();
            pointCount++;
        }
        EXPECT_EQ(pointCount, 5);
        seriesCount++;
    }
    EXPECT_EQ(seriesCount, 20);

    co_return;
}

// ===========================================================================
// 13. Boolean type specialization works with the pipeline
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, BooleanSeriesPrefetch) {
    // Create a TSM file with boolean series
    char filename[] = "shard_0/tsm/00_0000000000.tsm";
    {
        TSMWriter writer(filename);
        for (int s = 0; s < 3; s++) {
            SeriesId128 seriesId =
                SeriesId128::fromSeriesKey("bool." + std::to_string(s));
            std::vector<uint64_t> timestamps;
            std::vector<bool> values;
            for (int p = 0; p < 8; p++) {
                timestamps.push_back(1000 + p * 100);
                values.push_back((s + p) % 2 == 0);
            }
            writer.writeSeries(TSMValueType::Boolean, seriesId,
                               timestamps, values);
        }
        writer.writeIndex();
        writer.close();
    }

    auto tsm = seastar::make_shared<TSM>(filename);
    tsm->tierNum = 0;
    tsm->seqNum = 0;
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 3);
    if (allSeries.size() != 3) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    SeriesPrefetchManager<bool> manager(files, allSeries, 2);
    co_await manager.init();

    size_t seriesCount = 0;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);

        size_t pointCount = 0;
        while (iter->hasNext()) {
            auto [ts, val] = co_await iter->next();
            (void)ts;
            (void)val;
            pointCount++;
        }
        EXPECT_EQ(pointCount, 8);
        seriesCount++;
    }
    EXPECT_EQ(seriesCount, 3);

    co_return;
}

// ===========================================================================
// 14. String type specialization works with the pipeline
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, StringSeriesPrefetch) {
    char filename[] = "shard_0/tsm/00_0000000000.tsm";
    {
        TSMWriter writer(filename);
        for (int s = 0; s < 2; s++) {
            SeriesId128 seriesId =
                SeriesId128::fromSeriesKey("str." + std::to_string(s));
            std::vector<uint64_t> timestamps;
            std::vector<std::string> values;
            for (int p = 0; p < 6; p++) {
                timestamps.push_back(1000 + p * 100);
                values.push_back("value_s" + std::to_string(s) +
                                 "_p" + std::to_string(p));
            }
            writer.writeSeries(TSMValueType::String, seriesId,
                               timestamps, values);
        }
        writer.writeIndex();
        writer.close();
    }

    auto tsm = seastar::make_shared<TSM>(filename);
    tsm->tierNum = 0;
    tsm->seqNum = 0;
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 2);
    if (allSeries.size() != 2) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    SeriesPrefetchManager<std::string> manager(files, allSeries, 2);
    co_await manager.init();

    size_t seriesCount = 0;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);

        size_t pointCount = 0;
        while (iter->hasNext()) {
            auto [ts, val] = co_await iter->next();
            // Verify string values are non-empty
            EXPECT_FALSE(val.empty());
            pointCount++;
        }
        EXPECT_EQ(pointCount, 6);
        seriesCount++;
    }
    EXPECT_EQ(seriesCount, 2);

    co_return;
}

// ===========================================================================
// 15. Prefetch depth 0: should still work via synchronous fallback
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, PrefetchDepthZero) {
    auto tsm = self->createTestTSMFile(0, 0, "d0.", 3, 5);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 3);
    if (allSeries.size() != 3) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    // Depth 0: no prefetching, should fall back to synchronous creation
    SeriesPrefetchManager<double> manager(files, allSeries, 0);
    co_await manager.init();

    // Nothing should be prefetched
    EXPECT_EQ(manager.getPrefetchQueueSize(), 0);

    // But iteration should still work via the fallback path in getNext()
    size_t seriesCount = 0;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);

        size_t pointCount = 0;
        while (iter->hasNext()) {
            co_await iter->next();
            pointCount++;
        }
        EXPECT_EQ(pointCount, 5);
        seriesCount++;
    }
    EXPECT_EQ(seriesCount, 3);

    co_return;
}

// ===========================================================================
// 16. Multiple files with disjoint series: pipeline handles series from
//     different files
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, DisjointSeriesAcrossFiles) {
    // File 0 has series "disjoint.0" and "disjoint.1"
    // File 1 has series "disjoint_b.0" and "disjoint_b.1"
    // The pipeline should create merge iterators that may reference series
    // not present in all files (yielding empty iterators for missing series).
    auto tsm0 = self->createTestTSMFile(0, 0, "disjoint.", 2, 10, 1000000);
    co_await tsm0->open();
    co_await tsm0->readSparseIndex();

    auto tsm1 = self->createTestTSMFile(0, 1, "disjoint_b.", 2, 10, 2000000);
    co_await tsm1->open();
    co_await tsm1->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm0, tsm1};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 4);
    if (allSeries.size() != 4) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    SeriesPrefetchManager<double> manager(files, allSeries, 2);
    co_await manager.init();

    size_t seriesCount = 0;
    size_t totalPoints = 0;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);

        while (iter->hasNext()) {
            co_await iter->next();
            totalPoints++;
        }
        seriesCount++;
    }
    EXPECT_EQ(seriesCount, 4);
    // Each series has 10 points in one file only = 4 * 10 = 40 total
    EXPECT_EQ(totalPoints, 40);

    co_return;
}

// ===========================================================================
// 17. Data integrity: values from prefetched iterators match direct reads
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, DataIntegrity) {
    auto tsm = self->createTestTSMFile(0, 0, "integrity.", 3, 20);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);

    // Read data directly using TSMResult for comparison.
    // This also populates the full index cache as a side effect.
    std::map<SeriesId128, std::map<uint64_t, double>> directData;
    for (const auto& seriesId : allSeries) {
        TSMResult<double> result(0);
        co_await tsm->readSeries(seriesId, 0, UINT64_MAX, result);
        auto [timestamps, values] = result.getAllData();
        for (size_t i = 0; i < timestamps.size(); i++) {
            directData[seriesId][timestamps[i]] = values[i];
        }
    }

    // Now read via the prefetch manager (full index already cached above)
    SeriesPrefetchManager<double> manager(files, allSeries, 2);
    co_await manager.init();

    std::map<SeriesId128, std::map<uint64_t, double>> pipelineData;
    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);

        auto seriesId = iter->getSeriesId();
        while (iter->hasNext()) {
            auto [ts, val] = co_await iter->next();
            pipelineData[seriesId][ts] = val;
        }
    }

    // Compare
    EXPECT_EQ(pipelineData.size(), directData.size());
    for (const auto& [seriesId, data] : directData) {
        auto it = pipelineData.find(seriesId);
        EXPECT_NE(it, pipelineData.end())
            << "Missing series " << seriesId.toHex() << " in pipeline data";
        if (it == pipelineData.end()) continue;

        EXPECT_EQ(it->second.size(), data.size())
            << "Point count mismatch for series " << seriesId.toHex();

        for (const auto& [ts, val] : data) {
            auto pIt = it->second.find(ts);
            EXPECT_NE(pIt, it->second.end())
                << "Missing timestamp " << ts << " for series " << seriesId.toHex();
            if (pIt != it->second.end()) {
                EXPECT_DOUBLE_EQ(pIt->second, val);
            }
        }
    }

    co_return;
}

// ===========================================================================
// 18. Series with no data in any file: iterator is empty
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, SeriesNotInAnyFile) {
    auto tsm = self->createTestTSMFile(0, 0, "present.", 1, 10);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};

    // Create a series ID that does not exist in the file
    std::vector<SeriesId128> fakeSeries = {
        SeriesId128::fromSeriesKey("nonexistent.series")
    };

    // Preload will attempt to load a non-existent series; getFullIndexEntry
    // returns nullptr for unknown series but populates nothing.
    co_await CompactionPipelineTest::preloadFullIndex(files, fakeSeries);

    SeriesPrefetchManager<double> manager(files, fakeSeries, 1);
    co_await manager.init();

    EXPECT_TRUE(manager.hasNext());

    auto iter = co_await manager.getNext();
    EXPECT_NE(iter, nullptr);

    // Iterator should be empty since the series doesn't exist
    EXPECT_FALSE(iter->hasNext());

    EXPECT_FALSE(manager.hasNext());

    co_return;
}

// ===========================================================================
// 19. Batch retrieval through prefetched iterators (nextBatch)
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, BatchRetrievalThroughPipeline) {
    auto tsm = self->createTestTSMFile(0, 0, "batch.", 2, 100);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    std::vector<seastar::shared_ptr<TSM>> files = {tsm};
    auto allSeries = self->getAllSeriesIds(files);
    EXPECT_EQ(allSeries.size(), 2);
    if (allSeries.size() != 2) co_return;

    co_await CompactionPipelineTest::preloadFullIndex(files, allSeries);

    SeriesPrefetchManager<double> manager(files, allSeries, 2);
    co_await manager.init();

    while (manager.hasNext()) {
        auto iter = co_await manager.getNext();
        EXPECT_NE(iter, nullptr);

        size_t totalPoints = 0;
        while (iter->hasNext()) {
            auto batch = co_await iter->nextBatch(25);
            EXPECT_GT(batch.size(), 0);
            EXPECT_LE(batch.size(), 25);
            totalPoints += batch.size();
        }
        EXPECT_EQ(totalPoints, 100);
    }

    co_return;
}

// ===========================================================================
// 20. Pipeline with multiple overlapping files: correct merge ordering
// ===========================================================================
SEASTAR_TEST_F(CompactionPipelineTest, MergeOrderingMultipleFiles) {
    // File 0 (seqNum=0, lower rank): ts 1000..5000 step 1000, vals 10..50
    // File 1 (seqNum=1, higher rank): ts 3000..7000 step 1000, vals 110..150
    // For overlapping timestamps (3000,4000,5000), file 1 values should win.
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int f = 0; f < 2; f++) {
        char filename[256];
        snprintf(filename, sizeof(filename),
                 "shard_0/tsm/00_%010d.tsm", f);

        TSMWriter writer(filename);
        std::vector<uint64_t> ts;
        std::vector<double> vals;
        double baseVal = f * 100.0 + 10.0;
        uint64_t startTs = 1000 + f * 2000;
        for (int p = 0; p < 5; p++) {
            ts.push_back(startTs + p * 1000);
            vals.push_back(baseVal + p * 10.0);
        }
        writer.writeSeries(TSMValueType::Float,
                           SeriesId128::fromSeriesKey("merge.order"),
                           ts, vals);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = f;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    std::vector<SeriesId128> series = {
        SeriesId128::fromSeriesKey("merge.order")
    };

    co_await CompactionPipelineTest::preloadFullIndex(files, series);

    SeriesPrefetchManager<double> manager(files, series, 1);
    co_await manager.init();

    auto iter = co_await manager.getNext();
    EXPECT_NE(iter, nullptr);

    // Collect all points via nextBatch (dedup included)
    std::map<uint64_t, double> results;
    while (iter->hasNext()) {
        auto batch = co_await iter->nextBatch(100);
        for (const auto& [ts, val] : batch) {
            results[ts] = val;
        }
    }

    // Expected: 7 unique timestamps (1000..7000 step 1000)
    EXPECT_EQ(results.size(), 7);

    // For overlapping timestamps, file 1 (higher rank) values should win
    // File 1 has ts 3000=110, 4000=120, 5000=130
    EXPECT_DOUBLE_EQ(results[3000], 110.0);
    EXPECT_DOUBLE_EQ(results[4000], 120.0);
    EXPECT_DOUBLE_EQ(results[5000], 130.0);

    // File 0 only values
    EXPECT_DOUBLE_EQ(results[1000], 10.0);
    EXPECT_DOUBLE_EQ(results[2000], 20.0);

    // File 1 only values
    EXPECT_DOUBLE_EQ(results[6000], 140.0);
    EXPECT_DOUBLE_EQ(results[7000], 150.0);

    co_return;
}
