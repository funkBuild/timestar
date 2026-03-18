// Async Seastar-based tests for TSMCompactor component.
//
// These tests exercise the async compaction APIs that the existing synchronous
// tests (tsm_compactor_test.cpp) do not cover:
//   - Background compaction loop (startCompactionLoop / stopCompactionLoop)
//   - Compaction under active writes
//   - Multi-tier compaction chains
//   - Error recovery during compaction
//   - Tombstone integration during compaction
//   - Concurrent compaction requests
//   - Data integrity verification after compaction

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/tsm_compactor.hpp"
#include "../../../lib/storage/tsm_file_manager.hpp"
#include "../../../lib/storage/tsm_reader.hpp"
#include "../../../lib/storage/tsm_tombstone.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <set>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class TSMCompactorAsyncTest : public ::testing::Test {
public:
    std::string testDir = "./test_compactor_async_files";
    fs::path savedCwd;
    std::unique_ptr<TSMFileManager> fileManager;
    std::unique_ptr<TSMCompactor> compactor;

    void SetUp() override {
        savedCwd = fs::current_path();

        // If leftover from a crashed run, step out first.
        if (fs::current_path().filename() == "test_compactor_async_files") {
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

    // Create a TSM file with float data.  Each series gets `pointsPerSeries`
    // data points starting at `startTime` with a stride of 1000 ns.
    seastar::shared_ptr<TSM> createTestTSMFile(uint64_t tier, uint64_t seqNum, const std::string& seriesPrefix,
                                               int numSeries, int pointsPerSeries, uint64_t startTime = 1000000) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/%02lu_%010lu.tsm", tier, seqNum);

        TSMWriter writer(filename);

        for (int s = 0; s < numSeries; s++) {
            SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesPrefix + std::to_string(s));
            std::vector<uint64_t> timestamps;
            std::vector<double> values;

            for (int p = 0; p < pointsPerSeries; p++) {
                timestamps.push_back(startTime + p * 1000);
                values.push_back(s * 100.0 + p);
            }

            writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        }

        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = tier;
        tsm->seqNum = seqNum;

        return tsm;
    }

    // Create a TSM file with a single float series whose timestamps and values
    // are given explicitly.
    seastar::shared_ptr<TSM> createTestTSMFileExplicit(uint64_t tier, uint64_t seqNum, const std::string& seriesKey,
                                                       const std::vector<uint64_t>& timestamps,
                                                       const std::vector<double>& values) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/%02lu_%010lu.tsm", tier, seqNum);

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

    // Helper: read all float data from a TSM file for a given series key.
    // Returns a sorted map of timestamp -> value.
    static seastar::future<std::map<uint64_t, double>> readAllFloatData(seastar::shared_ptr<TSM> tsm,
                                                                        const std::string& seriesKey) {
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        TSMResult<double> result(0);
        co_await tsm->readSeries(seriesId, 0, UINT64_MAX, result);

        std::map<uint64_t, double> data;
        for (const auto& block : result.blocks) {
            for (size_t i = 0; i < block->timestamps.size(); i++) {
                data[block->timestamps.at(i)] = block->values.at(i);
            }
        }
        co_return data;
    }
};

// ===========================================================================
// 1. Background compaction loop: start, let it run briefly, stop cleanly
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, BackgroundCompactionLoopStartStop) {
    // Create enough tier-0 files to trigger compaction (4 is the threshold)
    std::vector<seastar::shared_ptr<TSM>> files;
    for (int i = 0; i < 5; i++) {
        auto tsm = self->createTestTSMFile(0, i, "bg.", 2, 50, 1000000 + i * 50000);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
        co_await self->fileManager->addTSMFile(tsm);
    }

    EXPECT_TRUE(self->compactor->shouldCompact(0));

    // Start the loop -- it will kick off a compaction for tier 0 and then
    // sleep.  We stop it quickly.
    auto loopFuture = self->compactor->runCompactionLoop();

    // Give it time to compact tier 0
    co_await seastar::sleep(std::chrono::milliseconds(500));

    // Stop the loop
    self->compactor->stopCompaction();
    co_await std::move(loopFuture);

    // After compaction the file count in tier 0 should have decreased.
    // The compacted result goes to tier 1.
    EXPECT_LT(self->fileManager->getFileCountInTier(0), 5);
    EXPECT_GE(self->fileManager->getFileCountInTier(1), 1);

    co_return;
}

// ===========================================================================
// 2. Compaction under active writes: interleave writes and compaction
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, CompactionUnderActiveWrites) {
    // Phase 1: create initial files
    std::vector<seastar::shared_ptr<TSM>> files;
    for (int i = 0; i < 4; i++) {
        auto tsm = self->createTestTSMFile(0, i, "wr.", 3, 100, 1000000 + i * 100000);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
        self->fileManager->setSequencedTsmFile(i, tsm);
    }

    // Phase 2: kick off compaction
    auto compactionFuture = self->compactor->compact(files);

    // Phase 3: while compaction is running, create a new file that simulates
    // a concurrent write (non-overlapping timestamps so the new file is
    // independent of the compaction input).
    auto newTsm = self->createTestTSMFile(0, 100, "wr_new.", 2, 50, 9000000);
    co_await newTsm->open();
    co_await newTsm->readSparseIndex();
    self->fileManager->setSequencedTsmFile(100, newTsm);

    // Phase 4: wait for compaction to finish
    auto compactedResult = co_await std::move(compactionFuture);
    auto compactedFile = compactedResult.outputPath;

    EXPECT_FALSE(compactedFile.empty());
    EXPECT_TRUE(fs::exists(compactedFile));

    // The new file (seqNum=100) should still be present and readable
    EXPECT_NE(self->fileManager->getSequencedTsmFiles().find(100), self->fileManager->getSequencedTsmFiles().end());

    // Verify the compacted file contains expected data
    auto compactedTSM = seastar::make_shared<TSM>(compactedFile);
    co_await compactedTSM->open();
    co_await compactedTSM->readSparseIndex();

    auto seriesIds = compactedTSM->getSeriesIds();
    EXPECT_GE(seriesIds.size(), 3);  // The 3 original series

    co_return;
}

// ===========================================================================
// 3. Multi-tier compaction chain: tier 0 -> 1 -> 2
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, MultiTierCompactionChain) {
    // Step 1: Create 4 tier-0 files with disjoint timestamps
    std::vector<seastar::shared_ptr<TSM>> tier0Files;
    for (int i = 0; i < 4; i++) {
        auto tsm = self->createTestTSMFileExplicit(0, i, "chain.series",
                                                   {uint64_t(1000 + i * 1000), uint64_t(1500 + i * 1000)},
                                                   {double(i) * 10.0, double(i) * 10.0 + 5.0});
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        tier0Files.push_back(tsm);
        co_await self->fileManager->addTSMFile(tsm);
    }

    // Compact tier 0 -> tier 1
    auto plan0 = self->compactor->planCompaction(0);
    EXPECT_TRUE(plan0.isValid());
    auto stats0 = co_await self->compactor->executeCompaction(plan0);
    EXPECT_GT(stats0.filesCompacted, 0);
    EXPECT_GE(self->fileManager->getFileCountInTier(1), 1);

    // Step 2: Create 3 more tier-1 files so we reach the compaction threshold
    for (int i = 10; i < 13; i++) {
        auto tsm = self->createTestTSMFileExplicit(1, i, "chain.series",
                                                   {uint64_t(50000 + i * 1000), uint64_t(50500 + i * 1000)},
                                                   {double(i) * 100.0, double(i) * 100.0 + 50.0});
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        co_await self->fileManager->addTSMFile(tsm);
    }

    // Tier 1 should now have >= 4 files
    EXPECT_GE(self->fileManager->getFileCountInTier(1), 4);

    // Compact tier 1 -> tier 2
    auto plan1 = self->compactor->planCompaction(1);
    EXPECT_TRUE(plan1.isValid());
    auto stats1 = co_await self->compactor->executeCompaction(plan1);
    EXPECT_GT(stats1.filesCompacted, 0);
    EXPECT_GE(self->fileManager->getFileCountInTier(2), 1);

    // Verify data integrity: read from the tier-2 file
    auto tier2Files = self->fileManager->getFilesInTier(2);
    EXPECT_GE(tier2Files.size(), 1);

    auto data = co_await TSMCompactorAsyncTest::readAllFloatData(tier2Files[0], "chain.series");
    // All timestamps from the tier-0 and tier-1 files should be present
    EXPECT_GT(data.size(), 0);

    co_return;
}

// ===========================================================================
// 4. Error recovery during compaction: empty plans and bad plans
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, ErrorRecoveryDuringCompaction) {
    // 4a: executing an invalid plan returns zero-value stats
    CompactionPlan emptyPlan;
    EXPECT_FALSE(emptyPlan.isValid());
    auto stats = co_await self->compactor->executeCompaction(emptyPlan);
    EXPECT_EQ(stats.filesCompacted, 0);
    EXPECT_EQ(stats.pointsRead, 0);
    EXPECT_EQ(stats.pointsWritten, 0);

    // 4b: compacting an empty vector returns an empty path
    std::vector<seastar::shared_ptr<TSM>> noFiles;
    auto result = co_await self->compactor->compact(noFiles);
    EXPECT_TRUE(result.outputPath.empty());

    // 4c: compacting a single valid file still works (no merge needed)
    auto tsm = self->createTestTSMFile(0, 1, "err.", 1, 20);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    auto singleCompactResult = co_await self->compactor->compact({tsm});
    auto singleResult = singleCompactResult.outputPath;
    EXPECT_FALSE(singleResult.empty());
    EXPECT_TRUE(fs::exists(singleResult));

    // Verify data preserved
    auto compacted = seastar::make_shared<TSM>(singleResult);
    co_await compacted->open();
    co_await compacted->readSparseIndex();
    auto data = co_await TSMCompactorAsyncTest::readAllFloatData(compacted, "err.0");
    EXPECT_EQ(data.size(), 20);

    co_return;
}

// ===========================================================================
// 5. Tombstone integration during compaction: deleted data is excluded
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, TombstoneIntegrationDuringCompaction) {
    // Create two TSM files, both with the same series and overlapping time.
    // File 0: timestamps 1000..10000 (step 1000), values 1.0..10.0
    // File 1: timestamps 1000..10000 (step 1000), values 101.0..110.0
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int f = 0; f < 2; f++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_%010d.tsm", f);

        TSMWriter writer(filename);
        std::vector<uint64_t> ts;
        std::vector<double> vals;
        for (int p = 1; p <= 10; p++) {
            ts.push_back(p * 1000);
            vals.push_back(f * 100.0 + p);
        }
        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("ts.sensor"), ts, vals);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = f;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        co_await tsm->loadTombstones();

        // Delete the range [3000, 5000] via tombstone on file 1
        if (f == 1) {
            SeriesId128 seriesId = SeriesId128::fromSeriesKey("ts.sensor");
            co_await tsm->deleteRange(seriesId, 3000, 5000);
        }

        files.push_back(tsm);
    }

    // Compact -- tombstoned points should be filtered out
    auto compactedResult = co_await self->compactor->compact(files);
    auto compactedPath = compactedResult.outputPath;
    EXPECT_FALSE(compactedPath.empty());
    EXPECT_TRUE(fs::exists(compactedPath));

    auto compactedTSM = seastar::make_shared<TSM>(compactedPath);
    co_await compactedTSM->open();
    co_await compactedTSM->readSparseIndex();

    auto data = co_await TSMCompactorAsyncTest::readAllFloatData(compactedTSM, "ts.sensor");

    // Timestamps 3000, 4000, 5000 should be absent (tombstoned).
    // Remaining: 1000, 2000, 6000, 7000, 8000, 9000, 10000 = 7 points
    EXPECT_EQ(data.count(3000), 0);
    EXPECT_EQ(data.count(4000), 0);
    EXPECT_EQ(data.count(5000), 0);
    EXPECT_EQ(data.size(), 7);

    // Values should come from the newest file (file 1)
    EXPECT_DOUBLE_EQ(data[1000], 101.0);
    EXPECT_DOUBLE_EQ(data[2000], 102.0);
    EXPECT_DOUBLE_EQ(data[6000], 106.0);

    co_return;
}

// ===========================================================================
// 6. Concurrent compaction requests: semaphore limits concurrency
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, ConcurrentCompactionRequests) {
    // Create two independent sets of files for two concurrent compactions.
    std::vector<seastar::shared_ptr<TSM>> setA;
    std::vector<seastar::shared_ptr<TSM>> setB;

    for (int i = 0; i < 4; i++) {
        auto tsmA = self->createTestTSMFile(0, i, "setA.", 2, 50, 1000000 + i * 50000);
        co_await tsmA->open();
        co_await tsmA->readSparseIndex();
        setA.push_back(tsmA);

        auto tsmB = self->createTestTSMFile(0, 100 + i, "setB.", 2, 50, 5000000 + i * 50000);
        co_await tsmB->open();
        co_await tsmB->readSparseIndex();
        setB.push_back(tsmB);
    }

    // Launch both compactions concurrently.  The compactor's internal
    // semaphore (MAX_CONCURRENT_COMPACTIONS = 2) should allow both through.
    auto futA = self->compactor->compact(setA);
    auto futB = self->compactor->compact(setB);

    auto resultA = co_await std::move(futA);
    auto resultB = co_await std::move(futB);

    EXPECT_FALSE(resultA.outputPath.empty());
    EXPECT_FALSE(resultB.outputPath.empty());
    EXPECT_TRUE(fs::exists(resultA.outputPath));
    EXPECT_TRUE(fs::exists(resultB.outputPath));

    // Verify both output files contain the correct data
    auto tsmA = seastar::make_shared<TSM>(resultA.outputPath);
    co_await tsmA->open();
    co_await tsmA->readSparseIndex();
    auto idsA = tsmA->getSeriesIds();
    EXPECT_GE(idsA.size(), 2);

    auto tsmB = seastar::make_shared<TSM>(resultB.outputPath);
    co_await tsmB->open();
    co_await tsmB->readSparseIndex();
    auto idsB = tsmB->getSeriesIds();
    EXPECT_GE(idsB.size(), 2);

    co_return;
}

// ===========================================================================
// 7. Data integrity after compaction: verify every point is preserved
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, DataIntegrityAfterCompaction) {
    // Build a deterministic data set across 4 files with partially
    // overlapping time ranges.  Keep track of expected final state.
    //
    // File 0 (seqNum=0): ts 1000..5000 step 1000, vals 10..50
    // File 1 (seqNum=1): ts 3000..7000 step 1000, vals 130..170
    // File 2 (seqNum=2): ts 5000..9000 step 1000, vals 250..290
    // File 3 (seqNum=3): ts 7000..11000 step 1000, vals 370..410
    //
    // Expected after dedup (newest wins):
    //   ts=1000  -> 10   (file 0)
    //   ts=2000  -> 20   (file 0)
    //   ts=3000  -> 130  (file 1 overrides 0)
    //   ts=4000  -> 140  (file 1 overrides 0)
    //   ts=5000  -> 250  (file 2 overrides 0,1)
    //   ts=6000  -> 260  (file 2 overrides 1)
    //   ts=7000  -> 370  (file 3 overrides 1,2)
    //   ts=8000  -> 380  (file 3 overrides 2)
    //   ts=9000  -> 390  (file 3 overrides 2)
    //   ts=10000 -> 400  (file 3)
    //   ts=11000 -> 410  (file 3)

    std::vector<seastar::shared_ptr<TSM>> files;

    struct FileSpec {
        uint64_t startTs;
        uint64_t endTs;
        double baseValue;
    };
    std::vector<FileSpec> specs = {{1000, 5000, 10.0}, {3000, 7000, 130.0}, {5000, 9000, 250.0}, {7000, 11000, 370.0}};

    for (int f = 0; f < 4; f++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_%010d.tsm", f);

        TSMWriter writer(filename);
        std::vector<uint64_t> ts;
        std::vector<double> vals;
        double v = specs[f].baseValue;
        for (uint64_t t = specs[f].startTs; t <= specs[f].endTs; t += 1000) {
            ts.push_back(t);
            vals.push_back(v);
            v += 10.0;
        }
        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("integrity.test"), ts, vals);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = f;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    auto compactedPathResult = co_await self->compactor->compact(files);
    auto compactedPath = compactedPathResult.outputPath;
    EXPECT_FALSE(compactedPath.empty());

    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    auto data = co_await TSMCompactorAsyncTest::readAllFloatData(compacted, "integrity.test");

    // Build expected map
    std::map<uint64_t, double> expected = {{1000, 10.0},  {2000, 20.0},   {3000, 130.0}, {4000, 140.0},
                                           {5000, 250.0}, {6000, 260.0},  {7000, 370.0}, {8000, 380.0},
                                           {9000, 390.0}, {10000, 400.0}, {11000, 410.0}};

    EXPECT_EQ(data.size(), expected.size());
    for (const auto& [ts, val] : expected) {
        auto it = data.find(ts);
        EXPECT_NE(it, data.end()) << "Missing timestamp " << ts;
        if (it == data.end())
            continue;
        EXPECT_DOUBLE_EQ(it->second, val) << "Wrong value at timestamp " << ts;
    }

    co_return;
}

// ===========================================================================
// 8. Force full compaction across all tiers
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, ForceFullCompactionAcrossTiers) {
    // Populate tiers 0, 1, and 2 with 3 files each
    int seq = 0;
    for (uint64_t tier = 0; tier < 3; tier++) {
        for (int i = 0; i < 3; i++) {
            auto tsm = self->createTestTSMFile(tier, seq, "full.", 2, 20, 1000000 + seq * 20000);
            co_await tsm->open();
            co_await tsm->readSparseIndex();
            self->fileManager->setSequencedTsmFile(seq, tsm);
            seq++;
        }
    }

    size_t totalBefore = self->fileManager->getSequencedTsmFiles().size();
    EXPECT_EQ(totalBefore, 9);

    co_await self->compactor->forceFullCompaction();

    size_t totalAfter = self->fileManager->getSequencedTsmFiles().size();
    EXPECT_LT(totalAfter, totalBefore);

    co_return;
}

// ===========================================================================
// 9. Compaction with mixed data types (float + boolean + string)
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, MixedDataTypeCompaction) {
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int i = 0; i < 3; i++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_%010d.tsm", i);

        TSMWriter writer(filename);

        // Float series
        {
            std::vector<uint64_t> ts;
            std::vector<double> vals;
            for (int p = 0; p < 20; p++) {
                ts.push_back(1000 + p * 100);
                vals.push_back(i * 10.0 + p);
            }
            writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("mixed.temp"), ts, vals);
        }

        // Boolean series
        {
            std::vector<uint64_t> ts;
            std::vector<bool> vals;
            for (int p = 0; p < 20; p++) {
                ts.push_back(1000 + p * 100);
                vals.push_back((i + p) % 2 == 0);
            }
            writer.writeSeries(TSMValueType::Boolean, SeriesId128::fromSeriesKey("mixed.status"), ts, vals);
        }

        // String series
        {
            std::vector<uint64_t> ts;
            std::vector<std::string> vals;
            for (int p = 0; p < 20; p++) {
                ts.push_back(1000 + p * 100);
                vals.push_back("file" + std::to_string(i) + "_point" + std::to_string(p));
            }
            writer.writeSeries(TSMValueType::String, SeriesId128::fromSeriesKey("mixed.label"), ts, vals);
        }

        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = i;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    auto compactedPathResult = co_await self->compactor->compact(files);
    auto compactedPath = compactedPathResult.outputPath;
    EXPECT_FALSE(compactedPath.empty());
    EXPECT_TRUE(fs::exists(compactedPath));

    // Verify all three types are present
    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    auto ids = compacted->getSeriesIds();
    EXPECT_EQ(ids.size(), 3);

    // Verify types
    EXPECT_EQ(compacted->getSeriesType(SeriesId128::fromSeriesKey("mixed.temp")).value(), TSMValueType::Float);
    EXPECT_EQ(compacted->getSeriesType(SeriesId128::fromSeriesKey("mixed.status")).value(), TSMValueType::Boolean);
    EXPECT_EQ(compacted->getSeriesType(SeriesId128::fromSeriesKey("mixed.label")).value(), TSMValueType::String);

    // Verify float data is deduplicated to 20 points (all files overlap)
    auto floatData = co_await TSMCompactorAsyncTest::readAllFloatData(compacted, "mixed.temp");
    EXPECT_EQ(floatData.size(), 20);

    // Verify string data is deduplicated to 20 points
    TSMResult<std::string> strResult(0);
    co_await compacted->readSeries(SeriesId128::fromSeriesKey("mixed.label"), 0, UINT64_MAX, strResult);
    auto [strTs, strVals] = strResult.getAllData();
    EXPECT_EQ(strTs.size(), 20);

    // Newest file (seqNum=2) should win -- verify string values
    for (size_t i = 0; i < strVals.size(); i++) {
        EXPECT_EQ(strVals[i].substr(0, 5), "file2") << "At index " << i << ": got " << strVals[i];
    }

    co_return;
}

// ===========================================================================
// 10. executeCompaction lifecycle: plan, execute, verify file manager updates
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, ExecuteCompactionLifecycle) {
    // Create 5 tier-0 files
    for (int i = 0; i < 5; i++) {
        auto tsm = self->createTestTSMFile(0, i, "lifecycle.", 2, 30, 1000000 + i * 30000);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        co_await self->fileManager->addTSMFile(tsm);
    }

    EXPECT_EQ(self->fileManager->getFileCountInTier(0), 5);
    EXPECT_TRUE(self->compactor->shouldCompact(0));

    // Plan and execute
    auto plan = self->compactor->planCompaction(0);
    EXPECT_TRUE(plan.isValid());
    EXPECT_GE(plan.sourceFiles.size(), 4);
    EXPECT_EQ(plan.targetTier, 1);

    auto stats = co_await self->compactor->executeCompaction(plan);

    EXPECT_GT(stats.filesCompacted, 0);
    // Note: pointsWritten may be 0 when the zero-copy path is taken
    // (non-overlapping blocks skip decompression/recompression).
    // We verify data integrity via readAllFloatData below instead.
    EXPECT_GT(stats.duration.count(), 0);

    // Old tier-0 files should be removed, new tier-1 file should exist
    EXPECT_LT(self->fileManager->getFileCountInTier(0), 5);
    EXPECT_GE(self->fileManager->getFileCountInTier(1), 1);

    // The new tier-1 file should be readable and contain all data
    auto tier1Files = self->fileManager->getFilesInTier(1);
    EXPECT_GE(tier1Files.size(), 1);

    auto data = co_await TSMCompactorAsyncTest::readAllFloatData(tier1Files[0], "lifecycle.0");
    // 5 files x 30 points each, non-overlapping, so all 150 points preserved
    EXPECT_EQ(data.size(), 150);

    co_return;
}

// ===========================================================================
// 11. Compaction with tombstones on multiple files
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, TombstoneMultipleFilesCompaction) {
    // File 0 (older): timestamps 1000..5000, file 1 (newer): timestamps 1000..5000
    // Delete range [1000,2000] on file 0 and [4000,5000] on file 1
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int f = 0; f < 2; f++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_%010d.tsm", f);

        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {1000, 2000, 3000, 4000, 5000};
        std::vector<double> vals;
        for (int p = 0; p < 5; p++) {
            vals.push_back(f * 100.0 + p + 1);
        }
        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("tomb.multi"), ts, vals);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = f;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        co_await tsm->loadTombstones();

        SeriesId128 sid = SeriesId128::fromSeriesKey("tomb.multi");
        if (f == 0) {
            co_await tsm->deleteRange(sid, 1000, 2000);
        } else {
            co_await tsm->deleteRange(sid, 4000, 5000);
        }
        files.push_back(tsm);
    }

    auto compactedPathResult = co_await self->compactor->compact(files);
    auto compactedPath = compactedPathResult.outputPath;
    EXPECT_FALSE(compactedPath.empty());

    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    auto data = co_await TSMCompactorAsyncTest::readAllFloatData(compacted, "tomb.multi");

    // Both tombstone ranges should be applied.
    // Timestamps 1000, 2000, 4000, 5000 should be gone.
    // Only timestamp 3000 should remain.
    EXPECT_EQ(data.count(1000), 0);
    EXPECT_EQ(data.count(2000), 0);
    EXPECT_EQ(data.count(4000), 0);
    EXPECT_EQ(data.count(5000), 0);
    EXPECT_EQ(data.count(3000), 1);
    EXPECT_EQ(data.size(), 1);

    co_return;
}

// ===========================================================================
// 12. Concurrent reads during compaction (reference counting safety)
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, ConcurrentReadsDuringCompaction) {
    // Create test files
    std::vector<seastar::shared_ptr<TSM>> files;
    for (int i = 0; i < 4; i++) {
        auto tsm = self->createTestTSMFile(0, i, "cread.", 3, 100);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
        self->fileManager->setSequencedTsmFile(i, tsm);
    }

    std::atomic<bool> compactionDone{false};
    std::atomic<int> readsCompleted{0};

    // Launch compaction
    auto compactionFuture = seastar::async([&]() {
        auto result = self->compactor->compact(files).get();
        compactionDone = true;
        EXPECT_FALSE(result.outputPath.empty());
    });

    // Give compaction a small head start
    co_await seastar::sleep(std::chrono::milliseconds(5));

    // Launch concurrent readers
    std::vector<seastar::future<>> readFutures;
    for (int r = 0; r < 8; r++) {
        auto readFut = seastar::async([&files, &readsCompleted, r]() {
            // Hold a reader reference (RAII ref counting)
            TSMReader reader(files[r % files.size()]);

            // Read a known series
            SeriesId128 sid = SeriesId128::fromSeriesKey("cread.0");
            auto result = reader.readSeries<double>(sid, 0, UINT64_MAX).get();

            // Should always succeed regardless of compaction state
            EXPECT_FALSE(result.empty());
            readsCompleted++;
        });
        readFutures.push_back(std::move(readFut));
    }

    co_await std::move(compactionFuture);
    co_await seastar::when_all(readFutures.begin(), readFutures.end());

    EXPECT_TRUE(compactionDone);
    EXPECT_EQ(readsCompleted, 8);

    co_return;
}

// ===========================================================================
// 13. Large-scale compaction data integrity
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, LargeScaleCompactionIntegrity) {
    // Create 4 files each with 10 series and 500 points per series
    // with partially overlapping timestamps to stress dedup
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int f = 0; f < 4; f++) {
        auto tsm = self->createTestTSMFile(0, f, "large.", 10, 500,
                                           1000000 + f * 250000);  // 50% overlap between adjacent files
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    auto compactedPathResult = co_await self->compactor->compact(files);
    auto compactedPath = compactedPathResult.outputPath;
    EXPECT_FALSE(compactedPath.empty());
    EXPECT_TRUE(fs::exists(compactedPath));

    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    // All 10 series should be present
    auto ids = compacted->getSeriesIds();
    EXPECT_EQ(ids.size(), 10);

    // Check series 0: file 0 starts at 1000000, file 3 ends at
    // 1000000 + 3*250000 + 499*1000 = 1000000 + 750000 + 499000 = 2249000
    // Total range: [1000000, 2249000], step 1000 = 1250 unique timestamps
    auto data = co_await TSMCompactorAsyncTest::readAllFloatData(compacted, "large.0");
    EXPECT_EQ(data.size(), 1250);

    // Verify monotonic timestamps
    uint64_t prevTs = 0;
    for (const auto& [ts, val] : data) {
        EXPECT_GT(ts, prevTs);
        prevTs = ts;
    }

    co_return;
}

// ===========================================================================
// 14. Compaction stops cleanly when stopCompaction is called before loop runs
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, StopCompactionBeforeLoopRuns) {
    // Stop immediately before the loop even starts
    self->compactor->stopCompaction();

    // Now run the loop -- it should exit immediately
    co_await self->compactor->runCompactionLoop();

    // If we get here without hanging, the test passes.
    EXPECT_TRUE(true);

    co_return;
}

// ===========================================================================
// 15. Compaction statistics tracking across executeCompaction
// ===========================================================================
SEASTAR_TEST_F(TSMCompactorAsyncTest, CompactionStatisticsTracking) {
    // Create files with fully overlapping data to ensure measurable dedup
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int i = 0; i < 3; i++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_%010d.tsm", i);

        TSMWriter writer(filename);
        std::vector<uint64_t> ts;
        std::vector<double> vals;
        for (int p = 0; p < 100; p++) {
            ts.push_back(1000 + p * 10);
            vals.push_back(i * 1000.0 + p);
        }
        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("stats.metric"), ts, vals);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = i;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
        co_await self->fileManager->addTSMFile(tsm);
    }

    CompactionPlan plan;
    plan.sourceFiles = files;
    plan.targetTier = 1;
    plan.targetSeqNum = 100;
    plan.targetPath = "shard_0/tsm/01_0000000100.tsm";

    auto stats = co_await self->compactor->executeCompaction(plan);

    EXPECT_EQ(stats.filesCompacted, 3);
    EXPECT_GT(stats.pointsRead, 0);
    // After dedup, exactly 100 unique points
    EXPECT_EQ(stats.pointsWritten, 100);
    EXPECT_GT(stats.duration.count(), 0);

    // Verify no active compactions remain
    auto activeStats = self->compactor->getActiveCompactionStats();
    EXPECT_TRUE(activeStats.empty());

    co_return;
}
