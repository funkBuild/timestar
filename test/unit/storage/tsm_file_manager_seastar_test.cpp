// Seastar-based tests for TSMFileManager component

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <memory>
#include <vector>

#include "../../../lib/storage/tsm_file_manager.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/core/series_id.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>

namespace fs = std::filesystem;

class TSMFileManagerSeastarTest : public ::testing::Test {
public:
    // TSMFileManager uses shard_<id>/tsm/ as its base path.
    // In the test runner, this_shard_id() is 0, so it uses shard_0/tsm/.
    std::string shardDir = "./shard_0";
    std::string tsmDir = "./shard_0/tsm";

    void SetUp() override {
        // Clean up any leftover shard directories from prior test runs
        fs::remove_all(shardDir);
        fs::create_directories(tsmDir);
    }

    void TearDown() override {
        fs::remove_all(shardDir);
    }

    // Helper to create a TSM file with float data on disk
    void createTestTSMFile(const std::string& filename,
                           const std::string& seriesKey,
                           const std::vector<uint64_t>& timestamps,
                           const std::vector<double>& values) {
        std::string path = tsmDir + "/" + filename;
        TSMWriter writer(path);
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // Helper to create a TSM file with boolean data on disk
    void createTestTSMFileBool(const std::string& filename,
                               const std::string& seriesKey,
                               const std::vector<uint64_t>& timestamps,
                               const std::vector<bool>& values) {
        std::string path = tsmDir + "/" + filename;
        TSMWriter writer(path);
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        writer.writeSeries(TSMValueType::Boolean, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }
};

// ---------------------------------------------------------------------------
// Test: Init with no existing TSM files
// ---------------------------------------------------------------------------
seastar::future<> testFMInitEmpty() {
    TSMFileManager mgr;
    co_await mgr.init();

    // No files should have been loaded
    EXPECT_TRUE(mgr.getSequencedTsmFiles().empty());
    EXPECT_EQ(mgr.getFileCountInTier(0), 0);
    EXPECT_EQ(mgr.getFileCountInTier(1), 0);
}

TEST_F(TSMFileManagerSeastarTest, InitEmpty) {
    testFMInitEmpty().get();
}

// ---------------------------------------------------------------------------
// Test: Init discovers existing TSM files on disk
// ---------------------------------------------------------------------------
seastar::future<> testFMInitDiscoversExistingFiles(TSMFileManagerSeastarTest* self) {
    // Create some TSM files before init
    self->createTestTSMFile("0_1.tsm", "cpu.usage",
                            {1000, 2000, 3000}, {10.0, 20.0, 30.0});
    self->createTestTSMFile("0_2.tsm", "mem.usage",
                            {1000, 2000}, {65.0, 70.0});

    TSMFileManager mgr;
    co_await mgr.init();

    // Both files should be discovered
    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 2);
    EXPECT_EQ(mgr.getFileCountInTier(0), 2);
}

TEST_F(TSMFileManagerSeastarTest, InitDiscoversExistingFiles) {
    testFMInitDiscoversExistingFiles(this).get();
}

// ---------------------------------------------------------------------------
// Test: Sequence number tracking across init
// ---------------------------------------------------------------------------
seastar::future<> testFMSequenceNumberTracking(TSMFileManagerSeastarTest* self) {
    // Create files with seq 3 and seq 7
    self->createTestTSMFile("0_3.tsm", "cpu.usage",
                            {1000, 2000}, {10.0, 20.0});
    self->createTestTSMFile("0_7.tsm", "mem.usage",
                            {1000, 2000}, {65.0, 70.0});

    TSMFileManager mgr;
    co_await mgr.init();

    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 2);

    // After init, the next sequence ID should be max(3,7)+1 = 8.
    // We verify indirectly: writeMemstore should produce a file with seq >= 8
    auto store = seastar::make_shared<MemoryStore>(0);
    TimeStarInsert<double> insert("test", "metric");
    insert.addValue(1000, 42.0);
    store->insertMemory(std::move(insert));

    co_await mgr.writeMemstore(store, 0);

    // Now there should be 3 files
    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 3);

    // The new file should have been created with the next available sequence number
    bool foundHighSeq = false;
    for (const auto& [seqRank, tsmFile] : mgr.getSequencedTsmFiles()) {
        if (tsmFile->seqNum >= 8) {
            foundHighSeq = true;
        }
    }
    EXPECT_TRUE(foundHighSeq);
}

TEST_F(TSMFileManagerSeastarTest, SequenceNumberTracking) {
    testFMSequenceNumberTracking(this).get();
}

// ---------------------------------------------------------------------------
// Test: Tier-based file tracking
// ---------------------------------------------------------------------------
seastar::future<> testFMTierBasedTracking(TSMFileManagerSeastarTest* self) {
    // Create files in different tiers
    self->createTestTSMFile("0_1.tsm", "series.a",
                            {1000, 2000}, {1.0, 2.0});
    self->createTestTSMFile("0_2.tsm", "series.b",
                            {1000, 2000}, {3.0, 4.0});
    self->createTestTSMFile("1_3.tsm", "series.c",
                            {1000, 2000}, {5.0, 6.0});
    self->createTestTSMFile("2_4.tsm", "series.d",
                            {1000, 2000}, {7.0, 8.0});

    TSMFileManager mgr;
    co_await mgr.init();

    EXPECT_EQ(mgr.getFileCountInTier(0), 2);
    EXPECT_EQ(mgr.getFileCountInTier(1), 1);
    EXPECT_EQ(mgr.getFileCountInTier(2), 1);
    EXPECT_EQ(mgr.getFileCountInTier(3), 0);
    EXPECT_EQ(mgr.getFileCountInTier(4), 0);
}

TEST_F(TSMFileManagerSeastarTest, TierBasedTracking) {
    testFMTierBasedTracking(this).get();
}

// ---------------------------------------------------------------------------
// Test: getFilesInTier returns correct files
// ---------------------------------------------------------------------------
seastar::future<> testFMGetFilesInTier(TSMFileManagerSeastarTest* self) {
    self->createTestTSMFile("0_1.tsm", "series.a",
                            {1000, 2000}, {1.0, 2.0});
    self->createTestTSMFile("0_2.tsm", "series.b",
                            {1000, 2000}, {3.0, 4.0});
    self->createTestTSMFile("1_3.tsm", "series.c",
                            {1000, 2000}, {5.0, 6.0});

    TSMFileManager mgr;
    co_await mgr.init();

    auto tier0Files = mgr.getFilesInTier(0);
    EXPECT_EQ(tier0Files.size(), 2);

    auto tier1Files = mgr.getFilesInTier(1);
    EXPECT_EQ(tier1Files.size(), 1);
    EXPECT_EQ(tier1Files[0]->tierNum, 1);
    EXPECT_EQ(tier1Files[0]->seqNum, 3);

    // Out of range tier returns empty
    auto tier99Files = mgr.getFilesInTier(99);
    EXPECT_TRUE(tier99Files.empty());
}

TEST_F(TSMFileManagerSeastarTest, GetFilesInTier) {
    testFMGetFilesInTier(this).get();
}

// ---------------------------------------------------------------------------
// Test: shouldCompactTier threshold (FILES_PER_COMPACTION = 4)
// ---------------------------------------------------------------------------
seastar::future<> testFMShouldCompactTier(TSMFileManagerSeastarTest* self) {
    // Create 3 files in tier 0 -- should NOT trigger compaction
    self->createTestTSMFile("0_1.tsm", "s.a", {1000}, {1.0});
    self->createTestTSMFile("0_2.tsm", "s.b", {1000}, {2.0});
    self->createTestTSMFile("0_3.tsm", "s.c", {1000}, {3.0});

    TSMFileManager mgr;
    co_await mgr.init();

    EXPECT_FALSE(mgr.shouldCompactTier(0));

    // Out of range tier should also return false
    EXPECT_FALSE(mgr.shouldCompactTier(99));
}

TEST_F(TSMFileManagerSeastarTest, ShouldCompactTierBelowThreshold) {
    testFMShouldCompactTier(this).get();
}

// ---------------------------------------------------------------------------
// Test: addTSMFile adds file to all tracking structures
// ---------------------------------------------------------------------------
seastar::future<> testFMAddTSMFile(TSMFileManagerSeastarTest* self) {
    TSMFileManager mgr;
    co_await mgr.init();

    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 0);

    // Create a TSM file and add it via addTSMFile
    self->createTestTSMFile("1_10.tsm", "added.series",
                            {1000, 2000, 3000}, {10.0, 20.0, 30.0});

    std::string absPath = fs::canonical(fs::absolute(self->tsmDir + "/1_10.tsm")).string();
    auto tsmFile = seastar::make_shared<TSM>(absPath);
    co_await tsmFile->open();

    co_await mgr.addTSMFile(tsmFile);

    // Verify file is tracked
    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 1);
    EXPECT_EQ(mgr.getFileCountInTier(1), 1);

    auto tier1Files = mgr.getFilesInTier(1);
    EXPECT_EQ(tier1Files.size(), 1);
    EXPECT_EQ(tier1Files[0]->tierNum, 1);
    EXPECT_EQ(tier1Files[0]->seqNum, 10);

    co_await tsmFile->close();
}

TEST_F(TSMFileManagerSeastarTest, AddTSMFile) {
    testFMAddTSMFile(this).get();
}

// ---------------------------------------------------------------------------
// Test: removeTSMFiles removes from all tracking structures
// ---------------------------------------------------------------------------
seastar::future<> testFMRemoveTSMFiles(TSMFileManagerSeastarTest* self) {
    self->createTestTSMFile("0_1.tsm", "s.a", {1000, 2000}, {1.0, 2.0});
    self->createTestTSMFile("0_2.tsm", "s.b", {1000, 2000}, {3.0, 4.0});
    self->createTestTSMFile("0_3.tsm", "s.c", {1000, 2000}, {5.0, 6.0});

    TSMFileManager mgr;
    co_await mgr.init();

    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 3);
    EXPECT_EQ(mgr.getFileCountInTier(0), 3);

    // Remove one file
    auto tier0Files = mgr.getFilesInTier(0);
    std::vector<seastar::shared_ptr<TSM>> toRemove = { tier0Files[0] };
    co_await mgr.removeTSMFiles(toRemove);

    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 2);
    EXPECT_EQ(mgr.getFileCountInTier(0), 2);
}

TEST_F(TSMFileManagerSeastarTest, RemoveTSMFiles) {
    testFMRemoveTSMFiles(this).get();
}

// ---------------------------------------------------------------------------
// Test: removeTSMFiles actually deletes TSM files from disk
// ---------------------------------------------------------------------------
seastar::future<> testFMRemoveDeletesFiles(TSMFileManagerSeastarTest* self) {
    self->createTestTSMFile("0_1.tsm", "s.a", {1000}, {1.0});

    TSMFileManager mgr;
    co_await mgr.init();

    auto tier0Files = mgr.getFilesInTier(0);
    EXPECT_EQ(tier0Files.size(), 1);
    if (tier0Files.size() != 1) co_return;

    auto fileToRemove = tier0Files[0];

    co_await mgr.removeTSMFiles({ fileToRemove });

    // After removeTSMFiles, the physical file should be deleted from disk
    EXPECT_FALSE(fs::exists("shard_0/tsm/0_1.tsm"));
}

TEST_F(TSMFileManagerSeastarTest, RemoveDeletesFiles) {
    testFMRemoveDeletesFiles(this).get();
}

// ---------------------------------------------------------------------------
// Test: writeMemstore creates a new TSM file from a MemoryStore
// ---------------------------------------------------------------------------
seastar::future<> testFMWriteMemstore() {
    TSMFileManager mgr;
    co_await mgr.init();

    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 0);

    // Create a memory store with some data
    auto store = seastar::make_shared<MemoryStore>(0);
    TimeStarInsert<double> insert("temperature", "value");
    insert.addValue(1000, 20.5);
    insert.addValue(2000, 21.0);
    insert.addValue(3000, 21.5);
    store->insertMemory(std::move(insert));

    co_await mgr.writeMemstore(store, 0);

    // A new TSM file should have been created and tracked
    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 1);
    EXPECT_EQ(mgr.getFileCountInTier(0), 1);

    // Verify the file is readable by checking series type
    // Series key format: "measurement field" (space-separated)
    std::string seriesKey = "temperature value";
    auto seriesType = mgr.getSeriesType(seriesKey);
    EXPECT_TRUE(seriesType.has_value());
    EXPECT_EQ(seriesType.value(), TSMValueType::Float);
}

TEST_F(TSMFileManagerSeastarTest, WriteMemstore) {
    testFMWriteMemstore().get();
}

// ---------------------------------------------------------------------------
// Test: writeMemstore with specific tier
// ---------------------------------------------------------------------------
seastar::future<> testFMWriteMemstoreWithTier() {
    TSMFileManager mgr;
    co_await mgr.init();

    auto store = seastar::make_shared<MemoryStore>(0);
    TimeStarInsert<double> insert("cpu", "load");
    insert.addValue(1000, 0.75);
    store->insertMemory(std::move(insert));

    // Write to tier 2
    co_await mgr.writeMemstore(store, 2);

    EXPECT_EQ(mgr.getFileCountInTier(0), 0);
    EXPECT_EQ(mgr.getFileCountInTier(1), 0);
    EXPECT_EQ(mgr.getFileCountInTier(2), 1);

    auto tier2Files = mgr.getFilesInTier(2);
    EXPECT_EQ(tier2Files.size(), 1);
    EXPECT_EQ(tier2Files[0]->tierNum, 2);
}

TEST_F(TSMFileManagerSeastarTest, WriteMemstoreWithTier) {
    testFMWriteMemstoreWithTier().get();
}

// ---------------------------------------------------------------------------
// Test: Multiple writeMemstore calls increment sequence numbers
// ---------------------------------------------------------------------------
seastar::future<> testFMMultipleWriteMemstoreSequence() {
    TSMFileManager mgr;
    co_await mgr.init();

    for (int i = 0; i < 3; ++i) {
        auto store = seastar::make_shared<MemoryStore>(0);
        TimeStarInsert<double> insert("metric", "value");
        insert.addValue(static_cast<uint64_t>(i * 1000 + 1000), static_cast<double>(i));
        store->insertMemory(std::move(insert));
        co_await mgr.writeMemstore(store, 0);
    }

    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 3);
    EXPECT_EQ(mgr.getFileCountInTier(0), 3);

    // Verify all sequence numbers are distinct
    std::vector<uint64_t> seqNums;
    for (const auto& [rank, file] : mgr.getSequencedTsmFiles()) {
        seqNums.push_back(file->seqNum);
    }
    std::sort(seqNums.begin(), seqNums.end());
    for (size_t i = 1; i < seqNums.size(); ++i) {
        EXPECT_GT(seqNums[i], seqNums[i - 1]);
    }
}

TEST_F(TSMFileManagerSeastarTest, MultipleWriteMemstoreSequence) {
    testFMMultipleWriteMemstoreSequence().get();
}

// ---------------------------------------------------------------------------
// Test: getSeriesType looks up series across multiple TSM files
// ---------------------------------------------------------------------------
seastar::future<> testFMGetSeriesType(TSMFileManagerSeastarTest* self) {
    self->createTestTSMFile("0_1.tsm", "cpu.usage",
                            {1000, 2000}, {10.0, 20.0});
    self->createTestTSMFileBool("0_2.tsm", "door.open",
                                {1000, 2000}, {true, false});

    TSMFileManager mgr;
    co_await mgr.init();

    std::string floatKey = "cpu.usage";
    auto floatType = mgr.getSeriesType(floatKey);
    EXPECT_TRUE(floatType.has_value());
    EXPECT_EQ(floatType.value(), TSMValueType::Float);

    std::string boolKey = "door.open";
    auto boolType = mgr.getSeriesType(boolKey);
    EXPECT_TRUE(boolType.has_value());
    EXPECT_EQ(boolType.value(), TSMValueType::Boolean);

    std::string unknownKey = "nonexistent.series";
    auto unknownType = mgr.getSeriesType(unknownKey);
    EXPECT_FALSE(unknownType.has_value());
}

TEST_F(TSMFileManagerSeastarTest, GetSeriesType) {
    testFMGetSeriesType(this).get();
}

// ---------------------------------------------------------------------------
// Test: getFileCountInTier with out-of-range tier returns 0
// ---------------------------------------------------------------------------
seastar::future<> testFMGetFileCountInTierOutOfRange() {
    TSMFileManager mgr;
    co_await mgr.init();

    EXPECT_EQ(mgr.getFileCountInTier(5), 0);
    EXPECT_EQ(mgr.getFileCountInTier(100), 0);
    EXPECT_EQ(mgr.getFileCountInTier(UINT64_MAX), 0);
}

TEST_F(TSMFileManagerSeastarTest, GetFileCountInTierOutOfRange) {
    testFMGetFileCountInTierOutOfRange().get();
}

// ---------------------------------------------------------------------------
// Test: Init ignores non-TSM files in the TSM directory
// ---------------------------------------------------------------------------
seastar::future<> testFMInitIgnoresNonTSMFiles(TSMFileManagerSeastarTest* self) {
    // Create a valid TSM file
    self->createTestTSMFile("0_1.tsm", "valid.series",
                            {1000, 2000}, {1.0, 2.0});

    // Create some non-TSM files that should be ignored
    {
        std::ofstream f(self->tsmDir + "/readme.txt");
        f << "This is not a TSM file";
    }
    {
        std::ofstream f(self->tsmDir + "/data.csv");
        f << "timestamp,value\n1000,1.0\n";
    }

    TSMFileManager mgr;
    co_await mgr.init();

    // Only the .tsm file should be loaded
    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 1);
}

TEST_F(TSMFileManagerSeastarTest, InitIgnoresNonTSMFiles) {
    testFMInitIgnoresNonTSMFiles(this).get();
}

// ---------------------------------------------------------------------------
// Test: Init handles corrupted TSM file gracefully
// ---------------------------------------------------------------------------
seastar::future<> testFMInitHandlesCorruptedFile(TSMFileManagerSeastarTest* self) {
    // Create a valid TSM file
    self->createTestTSMFile("0_1.tsm", "valid.series",
                            {1000, 2000}, {1.0, 2.0});

    // Create a file with a valid .tsm extension but invalid content
    {
        std::ofstream f(self->tsmDir + "/0_99.tsm");
        f << "not valid tsm data";
    }

    TSMFileManager mgr;
    co_await mgr.init();

    // The valid file should still be loaded; the corrupted one should be skipped
    // (openTsmFile catches runtime_error and logs it)
    EXPECT_GE(mgr.getSequencedTsmFiles().size(), 1);
}

TEST_F(TSMFileManagerSeastarTest, InitHandlesCorruptedFile) {
    testFMInitHandlesCorruptedFile(this).get();
}

// ---------------------------------------------------------------------------
// Test: addTSMFile updates sequence number tracking
// ---------------------------------------------------------------------------
seastar::future<> testFMAddTSMFileUpdatesSequenceNumber(TSMFileManagerSeastarTest* self) {
    TSMFileManager mgr;
    co_await mgr.init();

    // Create and add a file with a high sequence number
    self->createTestTSMFile("0_50.tsm", "series.high",
                            {1000}, {1.0});

    std::string absPath = fs::canonical(fs::absolute(self->tsmDir + "/0_50.tsm")).string();
    auto tsmFile = seastar::make_shared<TSM>(absPath);
    co_await tsmFile->open();
    co_await mgr.addTSMFile(tsmFile);

    // Now writeMemstore should use seqNum >= 51
    auto store = seastar::make_shared<MemoryStore>(0);
    TimeStarInsert<double> insert("test", "val");
    insert.addValue(1000, 1.0);
    store->insertMemory(std::move(insert));
    co_await mgr.writeMemstore(store, 0);

    // The new file should have seqNum >= 51
    bool foundHighSeq = false;
    for (const auto& [rank, file] : mgr.getSequencedTsmFiles()) {
        if (file->seqNum >= 51) {
            foundHighSeq = true;
        }
    }
    EXPECT_TRUE(foundHighSeq);

    co_await tsmFile->close();
}

TEST_F(TSMFileManagerSeastarTest, AddTSMFileUpdatesSequenceNumber) {
    testFMAddTSMFileUpdatesSequenceNumber(this).get();
}

// ---------------------------------------------------------------------------
// Test: Remove all files leaves manager in empty state
// ---------------------------------------------------------------------------
seastar::future<> testFMRemoveAllFiles(TSMFileManagerSeastarTest* self) {
    self->createTestTSMFile("0_1.tsm", "s.a", {1000}, {1.0});
    self->createTestTSMFile("0_2.tsm", "s.b", {1000}, {2.0});

    TSMFileManager mgr;
    co_await mgr.init();

    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 2);

    auto allFiles = mgr.getFilesInTier(0);
    co_await mgr.removeTSMFiles(allFiles);

    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 0);
    EXPECT_EQ(mgr.getFileCountInTier(0), 0);
    EXPECT_TRUE(mgr.getFilesInTier(0).empty());
}

TEST_F(TSMFileManagerSeastarTest, RemoveAllFiles) {
    testFMRemoveAllFiles(this).get();
}

// ---------------------------------------------------------------------------
// Test: writeMemstore with mixed data types in memory store
// ---------------------------------------------------------------------------
seastar::future<> testFMWriteMemstoreMixedTypes() {
    TSMFileManager mgr;
    co_await mgr.init();

    auto store = seastar::make_shared<MemoryStore>(0);

    TimeStarInsert<double> floatInsert("weather", "temperature");
    floatInsert.addValue(1000, 72.5);
    floatInsert.addValue(2000, 73.1);
    store->insertMemory(std::move(floatInsert));

    TimeStarInsert<bool> boolInsert("system", "healthy");
    boolInsert.addValue(1000, true);
    boolInsert.addValue(2000, false);
    store->insertMemory(std::move(boolInsert));

    TimeStarInsert<std::string> strInsert("app", "status");
    strInsert.addValue(1000, std::string("running"));
    strInsert.addValue(2000, std::string("stopped"));
    store->insertMemory(std::move(strInsert));

    co_await mgr.writeMemstore(store, 0);

    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 1);
    EXPECT_EQ(mgr.getFileCountInTier(0), 1);

    // Verify that we can look up the float series type
    // Series key format: "measurement field" (space-separated)
    std::string floatKey = "weather temperature";
    auto floatType = mgr.getSeriesType(floatKey);
    EXPECT_TRUE(floatType.has_value());
    EXPECT_EQ(floatType.value(), TSMValueType::Float);
}

TEST_F(TSMFileManagerSeastarTest, WriteMemstoreMixedTypes) {
    testFMWriteMemstoreMixedTypes().get();
}

// ---------------------------------------------------------------------------
// Test: Interleaved add and remove operations
// ---------------------------------------------------------------------------
seastar::future<> testFMAddAndRemoveInterleaved() {
    TSMFileManager mgr;
    co_await mgr.init();

    // Add three files via writeMemstore
    for (int i = 0; i < 3; ++i) {
        auto store = seastar::make_shared<MemoryStore>(0);
        TimeStarInsert<double> insert("series", std::to_string(i));
        insert.addValue(1000, static_cast<double>(i));
        store->insertMemory(std::move(insert));
        co_await mgr.writeMemstore(store, 0);
    }

    EXPECT_EQ(mgr.getFileCountInTier(0), 3);

    // Remove the first file
    auto files = mgr.getFilesInTier(0);
    co_await mgr.removeTSMFiles({ files[0] });

    EXPECT_EQ(mgr.getFileCountInTier(0), 2);

    // Add another file
    auto store = seastar::make_shared<MemoryStore>(0);
    TimeStarInsert<double> insert("series", "new");
    insert.addValue(1000, 99.0);
    store->insertMemory(std::move(insert));
    co_await mgr.writeMemstore(store, 0);

    EXPECT_EQ(mgr.getFileCountInTier(0), 3);
    EXPECT_EQ(mgr.getSequencedTsmFiles().size(), 3);
}

TEST_F(TSMFileManagerSeastarTest, AddAndRemoveInterleaved) {
    testFMAddAndRemoveInterleaved().get();
}

// ---------------------------------------------------------------------------
// Test: removeTSMFiles deletes files directly (no ref counting needed)
// ---------------------------------------------------------------------------
seastar::future<> testFMRemoveDeletesDirectly(TSMFileManagerSeastarTest* self) {
    self->createTestTSMFile("0_1.tsm", "s.a", {1000}, {1.0});

    TSMFileManager mgr;
    co_await mgr.init();

    auto files = mgr.getFilesInTier(0);
    EXPECT_EQ(files.size(), 1);
    if (files.size() != 1) co_return;

    // Verify file exists before removal
    EXPECT_TRUE(fs::exists("shard_0/tsm/0_1.tsm"));

    co_await mgr.removeTSMFiles(files);

    // File should be deleted directly by removeTSMFiles
    EXPECT_FALSE(fs::exists("shard_0/tsm/0_1.tsm"));

    // Manager should have no files in tier 0
    EXPECT_EQ(mgr.getFilesInTier(0).size(), 0);
}

TEST_F(TSMFileManagerSeastarTest, RemoveDeletesDirectly) {
    testFMRemoveDeletesDirectly(this).get();
}

// ---------------------------------------------------------------------------
// Test: Init after writeMemstore persists files across manager lifecycle
// ---------------------------------------------------------------------------
seastar::future<> testFMPersistenceAcrossManagerLifecycle() {
    // First manager writes a memstore
    {
        TSMFileManager mgr1;
        co_await mgr1.init();

        auto store = seastar::make_shared<MemoryStore>(0);
        TimeStarInsert<double> insert("persistent", "data");
        insert.addValue(1000, 42.0);
        insert.addValue(2000, 43.0);
        store->insertMemory(std::move(insert));

        co_await mgr1.writeMemstore(store, 0);

        EXPECT_EQ(mgr1.getSequencedTsmFiles().size(), 1);
    }

    // Second manager should discover the file on init
    {
        TSMFileManager mgr2;
        co_await mgr2.init();

        EXPECT_EQ(mgr2.getSequencedTsmFiles().size(), 1);
        EXPECT_EQ(mgr2.getFileCountInTier(0), 1);

        // Verify the data is accessible
        // Series key format: "measurement field" (space-separated)
        std::string key = "persistent data";
        auto seriesType = mgr2.getSeriesType(key);
        EXPECT_TRUE(seriesType.has_value());
        EXPECT_EQ(seriesType.value(), TSMValueType::Float);
    }
}

TEST_F(TSMFileManagerSeastarTest, PersistenceAcrossManagerLifecycle) {
    testFMPersistenceAcrossManagerLifecycle().get();
}
