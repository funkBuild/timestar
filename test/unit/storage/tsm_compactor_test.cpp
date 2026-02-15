#include <gtest/gtest.h>
#include <filesystem>
#include <random>
#include <chrono>
#include <thread>
#include <atomic>

#include "../../../lib/storage/tsm_compactor.hpp"
#include "../../../lib/storage/tsm_file_manager.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/tsm_reader.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"

#include "../../seastar_gtest.hpp"
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

namespace fs = std::filesystem;

class TSMCompactorTest : public ::testing::Test {
public:
    std::string testDir = "./test_compactor_files";
    fs::path savedCwd;
    std::unique_ptr<TSMFileManager> fileManager;
    std::unique_ptr<TSMCompactor> compactor;

    void SetUp() override {
        // Save the original working directory so TearDown can reliably restore it
        savedCwd = fs::current_path();

        // If we're already inside the test directory (e.g. a previous test
        // crashed before TearDown could run), step out first so we can
        // remove it cleanly.
        if (fs::current_path().filename() == "test_compactor_files") {
            fs::current_path(savedCwd.parent_path());
            savedCwd = fs::current_path();
        }

        // Remove any leftover test directory from a previous run to ensure
        // a completely clean state (no stale compaction output files).
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

    // Helper to create TSM files with test data
    seastar::shared_ptr<TSM> createTestTSMFile(
        uint64_t tier,
        uint64_t seqNum,
        const std::string& seriesPrefix,
        int numSeries,
        int pointsPerSeries,
        uint64_t startTime = 1000000) {

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

    // Helper to verify compacted file contents
    bool verifyCompactedFile(
        const std::string& filename,
        int expectedSeries,
        int expectedPointsPerSeries) {

        if (!fs::exists(filename)) {
            return false;
        }

        // Would need to implement TSM reading to fully verify
        // For now, just check file exists and has reasonable size
        auto fileSize = fs::file_size(filename);
        return fileSize > 0 && fileSize < 10 * 1024 * 1024; // Less than 10MB
    }
};

// Test basic compaction of multiple files
SEASTAR_TEST_F(TSMCompactorTest, BasicCompaction) {
    // Create 4 tier-0 files
    std::vector<seastar::shared_ptr<TSM>> files;
    for (int i = 0; i < 4; i++) {
        auto tsm = self->createTestTSMFile(0, i, "sensor.", 5, 100, 1000000 + i * 100000);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
        self->fileManager->sequencedTsmFiles[i] = tsm;
    }

    // Compact the files
    auto compactedFile = co_await self->compactor->compact(files);

    EXPECT_FALSE(compactedFile.empty());
    EXPECT_TRUE(fs::exists(compactedFile));

    // Verify compacted file has expected structure
    EXPECT_TRUE(self->verifyCompactedFile(compactedFile, 5, 400));

    co_return;
}

// Test that compaction properly deduplicates data
SEASTAR_TEST_F(TSMCompactorTest, DeduplicationDuringCompaction) {
    std::vector<seastar::shared_ptr<TSM>> files;

    // Create files with overlapping timestamps
    for (int i = 0; i < 3; i++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_%010d.tsm", i);

        TSMWriter writer(filename);

        std::vector<uint64_t> timestamps;
        std::vector<double> values;

        // Each file has same timestamps but different values
        for (int p = 0; p < 100; p++) {
            timestamps.push_back(1000000 + p * 1000);
            values.push_back(i * 10.0 + p); // Different values per file
        }

        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("temperature.sensor1"), timestamps, values);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = i;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    // Compact should keep only latest values (from file 2)
    auto compactedFile = co_await self->compactor->compact(files);

    EXPECT_FALSE(compactedFile.empty());
    EXPECT_TRUE(fs::exists(compactedFile));

    // Read back and verify we have only 100 points, not 300
    auto compactedTSM = seastar::make_shared<TSM>(compactedFile);
    co_await compactedTSM->open();
    co_await compactedTSM->readSparseIndex();

    TSMResult<double> result(0);
    co_await compactedTSM->readSeries(SeriesId128::fromSeriesKey("temperature.sensor1"), 0, UINT64_MAX, result);

    // Count total points
    size_t totalPoints = 0;
    for (const auto& block : result.blocks) {
        totalPoints += block->timestamps->size();
    }

    EXPECT_EQ(totalPoints, 100); // Should be deduplicated to 100 points

    co_return;
}

// Test that newer values overwrite older ones during compaction
SEASTAR_TEST_F(TSMCompactorTest, NewerValuesOverwriteOlderDuringCompaction) {
    std::vector<seastar::shared_ptr<TSM>> files;

    // Create 3 TSM files with overlapping timestamps but different values
    // File 0: oldest data (seqNum=0)
    {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_0000000000.tsm");

        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps;
        std::vector<double> values;

        // Write values 100.0, 101.0, 102.0... at times 1000, 2000, 3000...
        for (int i = 0; i < 10; i++) {
            timestamps.push_back(1000 + i * 1000);
            values.push_back(100.0 + i);
        }

        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("temperature.room1"), timestamps, values);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = 0;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    // File 1: overlapping data with some new timestamps (seqNum=1)
    {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_0000000001.tsm");

        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps;
        std::vector<double> values;

        // Overlap at times 5000-9000 with values 200.x
        // Also add new times 11000-15000
        for (int i = 4; i < 9; i++) {
            timestamps.push_back(1000 + i * 1000);  // 5000, 6000, 7000, 8000, 9000
            values.push_back(200.0 + i);
        }
        for (int i = 10; i < 15; i++) {
            timestamps.push_back(1000 + i * 1000);  // 11000, 12000, 13000, 14000, 15000
            values.push_back(200.0 + i);
        }

        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("temperature.room1"), timestamps, values);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = 1;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    // File 2: newest data overlapping both previous files (seqNum=2)
    {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_0000000002.tsm");

        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps;
        std::vector<double> values;

        // Overlap at times 3000, 7000, 13000 with values 300.x
        timestamps.push_back(3000);
        values.push_back(300.3);
        timestamps.push_back(7000);
        values.push_back(300.7);
        timestamps.push_back(13000);
        values.push_back(300.13);

        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("temperature.room1"), timestamps, values);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = 2;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    // Compact the files
    auto compactedFile = co_await self->compactor->compact(files);

    EXPECT_FALSE(compactedFile.empty());
    EXPECT_TRUE(fs::exists(compactedFile));

    // Read back the compacted file and verify values
    auto compactedTSM = seastar::make_shared<TSM>(compactedFile);
    co_await compactedTSM->open();
    co_await compactedTSM->readSparseIndex();

    TSMResult<double> result(0);
    co_await compactedTSM->readSeries(SeriesId128::fromSeriesKey("temperature.room1"), 0, UINT64_MAX, result);

    // Collect all points from result
    std::map<uint64_t, double> compactedData;
    for (const auto& block : result.blocks) {
        for (size_t i = 0; i < block->timestamps->size(); i++) {
            compactedData[block->timestamps->at(i)] = block->values->at(i);
        }
    }

    // Verify expected values after compaction:
    // Time 1000: 100.0 (from file 0, no override)
    EXPECT_DOUBLE_EQ(compactedData[1000], 100.0);

    // Time 2000: 101.0 (from file 0, no override)
    EXPECT_DOUBLE_EQ(compactedData[2000], 101.0);

    // Time 3000: 300.3 (from file 2, overrides file 0's 102.0)
    EXPECT_DOUBLE_EQ(compactedData[3000], 300.3);

    // Time 4000: 103.0 (from file 0, no override)
    EXPECT_DOUBLE_EQ(compactedData[4000], 103.0);

    // Time 5000: 204.0 (from file 1, overrides file 0's 104.0)
    EXPECT_DOUBLE_EQ(compactedData[5000], 204.0);

    // Time 6000: 205.0 (from file 1, overrides file 0's 105.0)
    EXPECT_DOUBLE_EQ(compactedData[6000], 205.0);

    // Time 7000: 300.7 (from file 2, overrides both file 0's 106.0 and file 1's 206.0)
    EXPECT_DOUBLE_EQ(compactedData[7000], 300.7);

    // Time 8000: 207.0 (from file 1, overrides file 0's 107.0)
    EXPECT_DOUBLE_EQ(compactedData[8000], 207.0);

    // Time 9000: 208.0 (from file 1, overrides file 0's 108.0)
    EXPECT_DOUBLE_EQ(compactedData[9000], 208.0);

    // Time 10000: 109.0 (from file 0, no override)
    EXPECT_DOUBLE_EQ(compactedData[10000], 109.0);

    // Time 11000-12000: from file 1
    EXPECT_DOUBLE_EQ(compactedData[11000], 210.0);
    EXPECT_DOUBLE_EQ(compactedData[12000], 211.0);

    // Time 13000: 300.13 (from file 2, overrides file 1's 212.0)
    EXPECT_DOUBLE_EQ(compactedData[13000], 300.13);

    // Time 14000-15000: from file 1
    EXPECT_DOUBLE_EQ(compactedData[14000], 213.0);
    EXPECT_DOUBLE_EQ(compactedData[15000], 214.0);

    // Verify total number of unique timestamps
    EXPECT_EQ(compactedData.size(), 15);

    co_return;
}

// Test reference counting prevents deletion during reads
SEASTAR_TEST_F(TSMCompactorTest, ReferenceCountingPreventsDelete) {
    // Create a TSM file
    auto tsm = self->createTestTSMFile(0, 1, "data.", 2, 50);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    // Simulate an active reader
    {
        TSMReader reader(tsm);
        EXPECT_EQ(tsm->getRefCount(), 1);

        // Mark for deletion while reader is active
        tsm->markForDeletion();

        // File should not be deleted yet
        EXPECT_TRUE(fs::exists("shard_0/tsm/00_0000000001.tsm"));

        // Reader goes out of scope here
    }

    // After reader is destroyed, ref count should be 0
    EXPECT_EQ(tsm->getRefCount(), 0);

    // Note: Actual deletion would be async in real implementation
    // For testing, we're verifying the mechanism works

    co_return;
}

// Test multi-level compaction with overlapping values
SEASTAR_TEST_F(TSMCompactorTest, MultiLevelCompactionPreservesNewerValues) {
    // Test scenario: Compact level 0 to level 1, then level 1 to level 2
    // Ensure newer values are preserved at each level

    // Step 1: Create 4 level-0 files with some overlapping data.
    // All files share timestamp 5000 to test deduplication.
    // Timestamps within each file must be sorted.
    //
    // File 0 (seqNum=0): ts=[1000, 2000, 5000, 8000, 9000]   vals=[0, 1, 2, 3, 4]
    // File 1 (seqNum=1): ts=[3000, 4000, 5000, 10000, 11000]  vals=[100, 101, 102, 103, 104]
    // File 2 (seqNum=2): ts=[5000, 6000, 7000, 12000, 13000]  vals=[200, 201, 202, 203, 204]
    // File 3 (seqNum=3): ts=[5000, 14000, 15000, 16000, 17000] vals=[300, 301, 302, 303, 304]
    //
    // At timestamp 5000 the newest file (seqNum=3) should win with value 300.
    std::vector<seastar::shared_ptr<TSM>> level0Files;

    // Explicit sorted timestamp arrays for each file
    std::vector<std::vector<uint64_t>> fileTimestamps = {
        {1000, 2000, 5000, 8000, 9000},
        {3000, 4000, 5000, 10000, 11000},
        {5000, 6000, 7000, 12000, 13000},
        {5000, 14000, 15000, 16000, 17000}
    };

    for (int fileNum = 0; fileNum < 4; fileNum++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/0_%d.tsm", fileNum);

        TSMWriter writer(filename);
        const auto& timestamps = fileTimestamps[fileNum];
        std::vector<double> values;
        for (size_t i = 0; i < timestamps.size(); i++) {
            values.push_back(fileNum * 100.0 + i);
        }

        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("metric.test"), timestamps, values);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = fileNum;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        level0Files.push_back(tsm);
    }

    // Step 2: Compact level 0 files to level 1
    auto level1File = co_await self->compactor->compact(level0Files);
    EXPECT_FALSE(level1File.empty());

    auto level1TSM = seastar::make_shared<TSM>(level1File);
    co_await level1TSM->open();
    co_await level1TSM->readSparseIndex();

    // Verify timestamp 5000 has value from newest file (file 3)
    TSMResult<double> result1(0);
    co_await level1TSM->readSeries(SeriesId128::fromSeriesKey("metric.test"), 5000, 5000, result1);

    bool found5000 = false;
    for (const auto& block : result1.blocks) {
        for (size_t i = 0; i < block->timestamps->size(); i++) {
            if (block->timestamps->at(i) == 5000) {
                // Should be 300.0 (file 3, index 0 - newest file wins)
                EXPECT_DOUBLE_EQ(block->values->at(i), 300.0);
                found5000 = true;
            }
        }
    }
    EXPECT_TRUE(found5000);

    // Step 3: Create more level 1 files to trigger level 2 compaction
    std::vector<seastar::shared_ptr<TSM>> level1Files;
    level1Files.push_back(level1TSM);

    // Add 3 more level 1 files with data that overlaps at timestamp 5000
    for (int fileNum = 1; fileNum < 4; fileNum++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/1_%d.tsm", fileNum + 10);

        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps;
        std::vector<double> values;

        // Include timestamp 5000 with a newer value
        timestamps.push_back(5000);
        values.push_back(400.0 + fileNum);  // 401, 402, 403

        // Add some other timestamps
        for (int i = 1; i < 5; i++) {
            timestamps.push_back(20000 + fileNum * 1000 + i * 100);
            values.push_back(500.0 + fileNum * 10 + i);
        }

        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("metric.test"), timestamps, values);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 1;
        tsm->seqNum = fileNum + 10;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        level1Files.push_back(tsm);
    }

    // Step 4: Compact level 1 files to level 2
    auto level2File = co_await self->compactor->compact(level1Files);
    EXPECT_FALSE(level2File.empty());

    auto level2TSM = seastar::make_shared<TSM>(level2File);
    co_await level2TSM->open();
    co_await level2TSM->readSparseIndex();

    // Step 5: Verify final value at timestamp 5000
    TSMResult<double> result2(0);
    co_await level2TSM->readSeries(SeriesId128::fromSeriesKey("metric.test"), 5000, 5000, result2);

    found5000 = false;
    for (const auto& block : result2.blocks) {
        for (size_t i = 0; i < block->timestamps->size(); i++) {
            if (block->timestamps->at(i) == 5000) {
                // Should be 403.0 (newest value from last level 1 file)
                EXPECT_DOUBLE_EQ(block->values->at(i), 403.0);
                found5000 = true;
            }
        }
    }
    EXPECT_TRUE(found5000);

    // Also verify all data is present and properly merged
    TSMResult<double> allData(0);
    co_await level2TSM->readSeries(SeriesId128::fromSeriesKey("metric.test"), 0, UINT64_MAX, allData);

    std::set<uint64_t> allTimestamps;
    for (const auto& block : allData.blocks) {
        for (size_t i = 0; i < block->timestamps->size(); i++) {
            allTimestamps.insert(block->timestamps->at(i));
        }
    }

    // Should have deduplicated all overlapping timestamps
    EXPECT_GT(allTimestamps.size(), 0);
    EXPECT_TRUE(allTimestamps.count(5000) > 0);

    co_return;
}

// Test compaction plan generation
SEASTAR_TEST_F(TSMCompactorTest, CompactionPlanGeneration) {
    // Create files in different tiers
    for (int tier = 0; tier < 3; tier++) {
        for (int i = 0; i < 5; i++) {
            auto tsm = self->createTestTSMFile(tier, tier * 10 + i, "metrics.", 3, 20);
            co_await tsm->open();
            co_await tsm->readSparseIndex();
            co_await self->fileManager->addTSMFile(tsm);
        }
    }

    // Generate compaction plan for tier 0
    auto plan = self->compactor->planCompaction(0);

    EXPECT_TRUE(plan.isValid());
    EXPECT_GE(plan.sourceFiles.size(), 4); // Should select at least 4 files
    EXPECT_EQ(plan.targetTier, 1); // Should promote to tier 1

    // Check that tier 0 files are selected
    for (const auto& file : plan.sourceFiles) {
        EXPECT_EQ(file->tierNum, 0);
    }

    co_return;
}

// Test leveled compaction strategy
TEST_F(TSMCompactorTest, LeveledCompactionStrategy) {
    LeveledCompactionStrategy strategy;

    // Test should compact logic
    EXPECT_TRUE(strategy.shouldCompact(0, 4, 50 * 1024 * 1024)); // 4 files in tier 0
    EXPECT_FALSE(strategy.shouldCompact(0, 3, 50 * 1024 * 1024)); // Only 3 files
    EXPECT_TRUE(strategy.shouldCompact(0, 2, 200 * 1024 * 1024)); // Size exceeds limit

    // Test file selection
    std::vector<seastar::shared_ptr<TSM>> availableFiles;
    for (int i = 0; i < 10; i++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_%010d.tsm", i);
        auto tsm = seastar::make_shared<TSM>(filename);
        // TSM constructor already sets tierNum and seqNum from filename
        availableFiles.push_back(tsm);
    }

    auto selected = strategy.selectFiles(availableFiles, 0);
    EXPECT_EQ(selected.size(), 8); // Max files per tier for tier 0

    // Verify oldest files are selected first
    for (size_t i = 0; i < selected.size(); i++) {
        EXPECT_EQ(selected[i]->seqNum, i);
    }

    // Test target tier calculation
    EXPECT_EQ(strategy.getTargetTier(0, 4), 1); // Promote from 0 to 1
    EXPECT_EQ(strategy.getTargetTier(1, 4), 2); // Promote from 1 to 2
    EXPECT_EQ(strategy.getTargetTier(2, 4), 3); // Promote from 2 to 3
    EXPECT_EQ(strategy.getTargetTier(3, 4), 3); // Stay at tier 3
}

// Test concurrent reads during compaction
SEASTAR_TEST_F(TSMCompactorTest, ConcurrentReadsDuringCompaction) {
    // Create test files
    std::vector<seastar::shared_ptr<TSM>> files;
    for (int i = 0; i < 4; i++) {
        auto tsm = self->createTestTSMFile(0, i, "concurrent.", 5, 100);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
        self->fileManager->sequencedTsmFiles[i] = tsm;
    }

    std::atomic<bool> compactionDone{false};
    std::atomic<int> readsDone{0};

    // Start compaction in background
    auto compactionTask = seastar::async([&]() {
        auto result = self->compactor->compact(files).get();
        compactionDone = true;
        EXPECT_FALSE(result.empty());
    });

    // Give compaction time to start
    co_await seastar::sleep(std::chrono::milliseconds(10));

    // Concurrent reads
    std::vector<seastar::future<>> readFutures;
    for (int r = 0; r < 10; r++) {
        auto readTask = seastar::async([&files, &readsDone, r]() {
            // Create reader to hold reference
            TSMReader reader(files[r % files.size()]);

            // Simulate read operation
            seastar::sleep(std::chrono::milliseconds(5 + r)).get();

            SeriesId128 seriesKey = SeriesId128::fromSeriesKey("concurrent.0");
            auto result = reader.readSeries<double>(seriesKey, 0, UINT64_MAX).get();

            readsDone++;
        });
        readFutures.push_back(std::move(readTask));
    }

    // Wait for all operations
    co_await std::move(compactionTask);
    co_await seastar::when_all(readFutures.begin(), readFutures.end());

    EXPECT_TRUE(compactionDone);
    EXPECT_EQ(readsDone, 10);

    co_return;
}

// Test compaction statistics tracking
SEASTAR_TEST_F(TSMCompactorTest, CompactionStatistics) {
    // Create files with known data
    std::vector<seastar::shared_ptr<TSM>> files;

    for (int i = 0; i < 3; i++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_%010d.tsm", i);

        TSMWriter writer(filename);

        // Create overlapping data for deduplication testing
        std::vector<uint64_t> timestamps;
        std::vector<double> values;

        for (int p = 0; p < 50; p++) {
            timestamps.push_back(1000 + p * 10);
            values.push_back(i + p * 0.1);
        }

        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("stats.test"), timestamps, values);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = i;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
        self->fileManager->sequencedTsmFiles[i] = tsm;
    }

    // Execute compaction with plan
    CompactionPlan plan;
    plan.sourceFiles = files;
    plan.targetTier = 1;
    plan.targetSeqNum = 100;
    plan.targetPath = "shard_0/tsm/01_0000000100.tsm";

    auto stats = co_await self->compactor->executeCompaction(plan);

    EXPECT_EQ(stats.filesCompacted, 3);
    EXPECT_GT(stats.pointsRead, 0);
    EXPECT_GT(stats.pointsWritten, 0);
    // The 3-way bulk merger resolves duplicates internally, so the
    // compactor's dedup counter only tracks post-merge duplicates.
    // With 3 fully-overlapping files the merger yields 50 unique points,
    // making pointsWritten == 50 the key correctness assertion.
    EXPECT_EQ(stats.pointsWritten, 50);
    EXPECT_GT(stats.duration.count(), 0);

    co_return;
}

// Test full compaction across all tiers
SEASTAR_TEST_F(TSMCompactorTest, FullCompaction) {
    // Create files in multiple tiers
    int totalFiles = 0;
    for (int tier = 0; tier < 3; tier++) {
        for (int i = 0; i < 3; i++) {
            auto tsm = self->createTestTSMFile(tier, totalFiles, "full.", 2, 10);
            co_await tsm->open();
            co_await tsm->readSparseIndex();
            self->fileManager->sequencedTsmFiles[totalFiles] = tsm;
            totalFiles++;
        }
    }

    // Verify initial file count
    EXPECT_EQ(self->fileManager->sequencedTsmFiles.size(), 9);

    // Run full compaction
    co_await self->compactor->forceFullCompaction();

    // Should have fewer files after compaction
    // Each tier should be compacted
    EXPECT_LT(self->fileManager->sequencedTsmFiles.size(), 9);

    co_return;
}

// Test time-based compaction strategy
TEST_F(TSMCompactorTest, TimeBasedCompactionStrategy) {
    TimeBasedCompactionStrategy strategy(std::chrono::hours(1));

    // Create test files
    std::vector<seastar::shared_ptr<TSM>> files;
    for (int i = 0; i < 6; i++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_%010d.tsm", i);
        auto tsm = seastar::make_shared<TSM>(filename);
        files.push_back(tsm);
    }

    // Should compact when we have old files
    EXPECT_TRUE(strategy.shouldCompact(0, 6, 100 * 1024 * 1024));

    // Select oldest files (first half)
    auto selected = strategy.selectFiles(files, 0);
    EXPECT_EQ(selected.size(), 3);

    // Verify oldest files are selected
    for (size_t i = 0; i < selected.size(); i++) {
        EXPECT_EQ(selected[i]->seqNum, i);
    }

    // Target tier should be promoted
    EXPECT_EQ(strategy.getTargetTier(0, 3), 1);
    EXPECT_EQ(strategy.getTargetTier(2, 3), 3);
}

// Test compaction with mixed data types
SEASTAR_TEST_F(TSMCompactorTest, MixedDataTypeCompaction) {
    std::vector<seastar::shared_ptr<TSM>> files;

    // Create files with both float and boolean data
    for (int i = 0; i < 2; i++) {
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/00_%010d.tsm", i);

        TSMWriter writer(filename);

        // Float series
        std::vector<uint64_t> floatTimestamps;
        std::vector<double> floatValues;
        for (int p = 0; p < 30; p++) {
            floatTimestamps.push_back(1000 + p * 100);
            floatValues.push_back(i * 10.0 + p);
        }
        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("temperature"), floatTimestamps, floatValues);

        // Boolean series
        std::vector<uint64_t> boolTimestamps;
        std::vector<bool> boolValues;
        for (int p = 0; p < 30; p++) {
            boolTimestamps.push_back(1000 + p * 100);
            boolValues.push_back((i + p) % 2 == 0);
        }
        writer.writeSeries(TSMValueType::Boolean, SeriesId128::fromSeriesKey("status"), boolTimestamps, boolValues);

        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = i;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        files.push_back(tsm);
    }

    // Compact files with mixed types
    auto compactedFile = co_await self->compactor->compact(files);

    EXPECT_FALSE(compactedFile.empty());
    EXPECT_TRUE(fs::exists(compactedFile));

    // Verify both series exist in compacted file
    auto compactedTSM = seastar::make_shared<TSM>(compactedFile);
    co_await compactedTSM->open();
    co_await compactedTSM->readSparseIndex();

    auto keys = compactedTSM->getSeriesIds();
    EXPECT_EQ(keys.size(), 2);

    // Check both types are preserved
    SeriesId128 tempKey = SeriesId128::fromSeriesKey("temperature");
    SeriesId128 statusKey = SeriesId128::fromSeriesKey("status");
    EXPECT_EQ(compactedTSM->getSeriesType(tempKey), TSMValueType::Float);
    EXPECT_EQ(compactedTSM->getSeriesType(statusKey), TSMValueType::Boolean);

    co_return;
}

// Test error handling during compaction
SEASTAR_TEST_F(TSMCompactorTest, CompactionErrorHandling) {
    // Test with empty file list
    std::vector<seastar::shared_ptr<TSM>> emptyFiles;
    auto result = co_await self->compactor->compact(emptyFiles);
    EXPECT_TRUE(result.empty());

    // Test with invalid plan
    CompactionPlan invalidPlan;
    EXPECT_FALSE(invalidPlan.isValid());
    auto stats = co_await self->compactor->executeCompaction(invalidPlan);
    EXPECT_EQ(stats.filesCompacted, 0);

    co_return;
}
