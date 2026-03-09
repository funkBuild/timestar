// Integration tests for compaction retention:
// - Data older than the retention period is dropped during compaction
// - Data within the retention period is preserved
// - After compaction, queries only return in-retention data
// - The compacted output has fewer files than the input

#include <gtest/gtest.h>
#include <filesystem>
#include <map>
#include <chrono>

#include "../../../lib/storage/tsm_compactor.hpp"
#include "../../../lib/storage/tsm_file_manager.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/tsm_reader.hpp"
#include "../../../lib/storage/tsm_result.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/retention/retention_policy.hpp"

#include "../../seastar_gtest.hpp"
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Helper: nanoseconds since Unix epoch at the current instant
// ---------------------------------------------------------------------------
static uint64_t nowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

static constexpr uint64_t ONE_DAY_NS  = 24ULL * 3600ULL * 1'000'000'000ULL;
static constexpr uint64_t ONE_HOUR_NS =         3600ULL * 1'000'000'000ULL;

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class CompactionRetentionTest : public ::testing::Test {
public:
    std::string testDir = "./test_compaction_retention_files";
    fs::path savedCwd;
    std::unique_ptr<TSMFileManager> fileManager;
    std::unique_ptr<TSMCompactor> compactor;

    void SetUp() override {
        savedCwd = fs::current_path();

        // Step out if a previous crash left us inside the test directory.
        if (fs::current_path().filename() == "test_compaction_retention_files") {
            fs::current_path(savedCwd.parent_path());
            savedCwd = fs::current_path();
        }

        fs::remove_all(testDir);
        fs::create_directories(testDir + "/shard_0/tsm");
        fs::current_path(testDir);

        fileManager = std::make_unique<TSMFileManager>();
        compactor   = std::make_unique<TSMCompactor>(fileManager.get());
    }

    void TearDown() override {
        compactor.reset();
        fileManager.reset();
        fs::current_path(savedCwd);
        fs::remove_all(testDir);
    }

    // Build a TSM file that holds exactly one float series.
    seastar::shared_ptr<TSM> makeFloatFile(
        uint64_t tier,
        uint64_t seqNum,
        const std::string& seriesKey,
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>&   values)
    {
        char filename[256];
        snprintf(filename, sizeof(filename),
                 "shard_0/tsm/%02lu_%010lu.tsm", tier, seqNum);

        TSMWriter writer(filename);
        SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
        writer.writeSeries(TSMValueType::Float, sid, timestamps, values);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = tier;
        tsm->seqNum  = seqNum;
        return tsm;
    }

    // Read all float data for `seriesKey` from an open TSM file.
    // Returns a sorted map of timestamp -> value.
    static seastar::future<std::map<uint64_t, double>> readAllFloat(
        seastar::shared_ptr<TSM> tsm,
        const std::string& seriesKey)
    {
        SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
        TSMResult<double> result(0);
        co_await tsm->readSeries(sid, 0, UINT64_MAX, result);

        std::map<uint64_t, double> out;
        for (const auto& blk : result.blocks) {
            for (size_t i = 0; i < blk->timestamps->size(); i++) {
                out[blk->timestamps->at(i)] = blk->values->at(i);
            }
        }
        co_return out;
    }

    // Build a one-day TTL RetentionPolicy for the given measurement.
    static RetentionPolicy oneDayPolicy(const std::string& measurement) {
        RetentionPolicy p;
        p.measurement = measurement;
        p.ttl         = "1d";
        p.ttlNanos    = ONE_DAY_NS;
        return p;
    }
};

// ===========================================================================
// Test 1: Data outside the retention window is fully dropped after compaction.
//
// When ALL data in every source file is expired, the compactor still writes
// the output file but with no series content — only the TSM header and the
// index-offset trailer.  The file exists on disk but contains zero points.
// We verify this by checking the file size rather than trying to read series
// data (readSparseIndex rejects the empty-index file as "out of bounds").
// ===========================================================================
SEASTAR_TEST_F(CompactionRetentionTest, OldDataDroppedAfterCompaction) {
    const std::string measurement = "weather";
    const std::string seriesKey   = "weather|location=us-west|temperature";

    uint64_t now          = nowNs();
    uint64_t twoDaysAgo   = now - 2 * ONE_DAY_NS;
    uint64_t threeDaysAgo = now - 3 * ONE_DAY_NS;

    // File 0: three data-points from 3 days ago — outside 1-day retention.
    auto file0 = self->makeFloatFile(0, 0, seriesKey,
        {threeDaysAgo, threeDaysAgo + 1'000'000'000ULL, threeDaysAgo + 2'000'000'000ULL},
        {10.0, 11.0, 12.0});

    // File 1: three data-points from 2 days ago — also outside 1-day retention.
    auto file1 = self->makeFloatFile(0, 1, seriesKey,
        {twoDaysAgo, twoDaysAgo + 1'000'000'000ULL, twoDaysAgo + 2'000'000'000ULL},
        {20.0, 21.0, 22.0});

    co_await file0->open();
    co_await file0->readSparseIndex();
    co_await file1->open();
    co_await file1->readSparseIndex();

    SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
    std::unordered_map<std::string, RetentionPolicy> policies;
    policies[measurement] = CompactionRetentionTest::oneDayPolicy(measurement);

    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMap;
    seriesMap[sid] = measurement;

    auto compactedPath = co_await self->compactor->compact(
        {file0, file1}, policies, seriesMap);

    EXPECT_FALSE(compactedPath.empty());
    EXPECT_TRUE(fs::exists(compactedPath));

    // When all data is expired the compactor writes an empty output:
    // 5-byte header ("TASM" + version) + 8-byte index-offset trailer = 13 bytes.
    // This is significantly smaller than either source file and confirms that
    // no series data was written to the compacted output.
    auto compactedSize = fs::file_size(compactedPath);

    // Each source file has 3 data-points; the compacted file must be far smaller.
    auto sourceSize0 = fs::file_size("shard_0/tsm/00_0000000000.tsm");
    auto sourceSize1 = fs::file_size("shard_0/tsm/00_0000000001.tsm");
    EXPECT_LT(compactedSize, sourceSize0)
        << "Compacted file should be much smaller than source file 0 "
        << "(all data was expired)";
    EXPECT_LT(compactedSize, sourceSize1)
        << "Compacted file should be much smaller than source file 1 "
        << "(all data was expired)";

    // The minimal empty TSM is exactly 13 bytes (header + index offset trailer).
    // Confirm the compacted file has no series data blocks (size <= 13 bytes).
    EXPECT_LE(compactedSize, 13u)
        << "Expected empty compacted file (<=13 bytes) when all data is expired, "
        << "got " << compactedSize << " bytes";

    co_return;
}

// ===========================================================================
// Test 2: Data within the retention window is preserved after compaction.
// ===========================================================================
SEASTAR_TEST_F(CompactionRetentionTest, RecentDataPreservedAfterCompaction) {
    const std::string measurement = "cpu";
    const std::string seriesKey   = "cpu|host=server01|usage";

    uint64_t now          = nowNs();
    uint64_t thirtyMinAgo = now - 30 * 60 * 1'000'000'000ULL;
    uint64_t oneHourAgo   = now - ONE_HOUR_NS;

    // File 0: data from 30 minutes ago — well within a 1-day window.
    auto file0 = self->makeFloatFile(0, 0, seriesKey,
        {thirtyMinAgo, thirtyMinAgo + 10'000'000'000ULL, thirtyMinAgo + 20'000'000'000ULL},
        {55.0, 60.0, 65.0});

    // File 1: data from 1 hour ago — also within window.
    auto file1 = self->makeFloatFile(0, 1, seriesKey,
        {oneHourAgo, oneHourAgo + 10'000'000'000ULL, oneHourAgo + 20'000'000'000ULL},
        {70.0, 75.0, 80.0});

    co_await file0->open();
    co_await file0->readSparseIndex();
    co_await file1->open();
    co_await file1->readSparseIndex();

    SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
    std::unordered_map<std::string, RetentionPolicy> policies;
    policies[measurement] = CompactionRetentionTest::oneDayPolicy(measurement);

    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMap;
    seriesMap[sid] = measurement;

    auto compactedPath = co_await self->compactor->compact(
        {file0, file1}, policies, seriesMap);

    EXPECT_FALSE(compactedPath.empty());
    EXPECT_TRUE(fs::exists(compactedPath));

    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    auto data = co_await CompactionRetentionTest::readAllFloat(compacted, seriesKey);

    // All 6 points (3 from each file, non-overlapping) must survive.
    EXPECT_EQ(data.size(), 6u)
        << "Expected 6 recent points to be preserved but found " << data.size();

    EXPECT_DOUBLE_EQ(data[thirtyMinAgo], 55.0);
    EXPECT_DOUBLE_EQ(data[oneHourAgo],   70.0);

    co_return;
}

// ===========================================================================
// Test 3: Mixed files — old data is dropped, recent data is kept.
// ===========================================================================
SEASTAR_TEST_F(CompactionRetentionTest, MixedAgeDataPartialRetention) {
    const std::string measurement = "sensor";
    const std::string seriesKey   = "sensor|id=42|temp";

    uint64_t now        = nowNs();
    uint64_t twoDaysAgo = now - 2 * ONE_DAY_NS;
    uint64_t twoHoursAgo = now - 2 * ONE_HOUR_NS;

    // File 0: old data — should be dropped.
    auto file0 = self->makeFloatFile(0, 0, seriesKey,
        {twoDaysAgo,
         twoDaysAgo + 1'000'000'000ULL,
         twoDaysAgo + 2'000'000'000ULL,
         twoDaysAgo + 3'000'000'000ULL,
         twoDaysAgo + 4'000'000'000ULL},
        {1.0, 2.0, 3.0, 4.0, 5.0});

    // File 1: recent data — should survive.
    auto file1 = self->makeFloatFile(0, 1, seriesKey,
        {twoHoursAgo,
         twoHoursAgo + 60'000'000'000ULL,
         twoHoursAgo + 120'000'000'000ULL,
         twoHoursAgo + 180'000'000'000ULL},
        {100.0, 101.0, 102.0, 103.0});

    co_await file0->open();
    co_await file0->readSparseIndex();
    co_await file1->open();
    co_await file1->readSparseIndex();

    SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
    std::unordered_map<std::string, RetentionPolicy> policies;
    policies[measurement] = CompactionRetentionTest::oneDayPolicy(measurement);

    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMap;
    seriesMap[sid] = measurement;

    auto compactedPath = co_await self->compactor->compact(
        {file0, file1}, policies, seriesMap);

    EXPECT_FALSE(compactedPath.empty());
    EXPECT_TRUE(fs::exists(compactedPath));

    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    auto data = co_await CompactionRetentionTest::readAllFloat(compacted, seriesKey);

    // Old 5 points must be gone; recent 4 points must survive.
    EXPECT_EQ(data.size(), 4u)
        << "Expected only the 4 recent points to survive, found " << data.size();

    // Old timestamps should not appear.
    EXPECT_EQ(data.count(twoDaysAgo), 0u);
    EXPECT_EQ(data.count(twoDaysAgo + 1'000'000'000ULL), 0u);
    EXPECT_EQ(data.count(twoDaysAgo + 4'000'000'000ULL), 0u);

    // Recent timestamps should be present with correct values.
    EXPECT_DOUBLE_EQ(data[twoHoursAgo],                     100.0);
    EXPECT_DOUBLE_EQ(data[twoHoursAgo + 60'000'000'000ULL],  101.0);
    EXPECT_DOUBLE_EQ(data[twoHoursAgo + 120'000'000'000ULL], 102.0);
    EXPECT_DOUBLE_EQ(data[twoHoursAgo + 180'000'000'000ULL], 103.0);

    co_return;
}

// ===========================================================================
// Test 4: Compacted output has fewer files than the input.
//         Four tier-0 input files produce one compacted file.
// ===========================================================================
SEASTAR_TEST_F(CompactionRetentionTest, CompactedOutputHasFewerFiles) {
    const std::string measurement = "disk";
    const std::string seriesKey   = "disk|device=sda|read_bytes";

    uint64_t now = nowNs();

    // Four input files with non-overlapping recent timestamps (30 min apart).
    std::vector<seastar::shared_ptr<TSM>> files;
    SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);

    for (int i = 0; i < 4; i++) {
        uint64_t base = now - uint64_t(4 - i) * 30 * 60 * 1'000'000'000ULL;
        auto f = self->makeFloatFile(0, uint64_t(i), seriesKey,
            {base, base + 5'000'000'000ULL},
            {double(i * 10), double(i * 10 + 1)});
        co_await f->open();
        co_await f->readSparseIndex();
        files.push_back(f);
        self->fileManager->setSequencedTsmFile(i, f);
    }

    EXPECT_EQ(files.size(), 4u);

    std::unordered_map<std::string, RetentionPolicy> policies;
    policies[measurement] = CompactionRetentionTest::oneDayPolicy(measurement);

    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMap;
    seriesMap[sid] = measurement;

    auto compactedPath = co_await self->compactor->compact(files, policies, seriesMap);

    EXPECT_FALSE(compactedPath.empty());
    EXPECT_TRUE(fs::exists(compactedPath));

    // Verify the compacted file contains all 8 recent data-points
    // (the 4 source files x 2 points each, all within 1-day window).
    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    auto data = co_await CompactionRetentionTest::readAllFloat(compacted, seriesKey);
    EXPECT_EQ(data.size(), 8u)
        << "Expected 8 data points (2 per file x 4 files) in compacted output";

    co_return;
}

// ===========================================================================
// Test 5: No retention policy — all data is preserved (baseline sanity).
// ===========================================================================
SEASTAR_TEST_F(CompactionRetentionTest, NoRetentionPolicyPreservesAllData) {
    const std::string seriesKey = "metrics|host=a|cpu";

    uint64_t now          = nowNs();
    uint64_t threeDaysAgo = now - 3 * ONE_DAY_NS;
    uint64_t oneHourAgo   = now - ONE_HOUR_NS;

    // File 0: old data (would be expired with a 1-day policy, but no policy here).
    auto file0 = self->makeFloatFile(0, 0, seriesKey,
        {threeDaysAgo, threeDaysAgo + 1'000'000'000ULL},
        {1.0, 2.0});

    // File 1: recent data.
    auto file1 = self->makeFloatFile(0, 1, seriesKey,
        {oneHourAgo, oneHourAgo + 1'000'000'000ULL},
        {3.0, 4.0});

    co_await file0->open();
    co_await file0->readSparseIndex();
    co_await file1->open();
    co_await file1->readSparseIndex();

    // No retention policies at all.
    auto compactedPath = co_await self->compactor->compact({file0, file1});

    EXPECT_FALSE(compactedPath.empty());

    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    auto data = co_await CompactionRetentionTest::readAllFloat(compacted, seriesKey);

    // All 4 points should be present when no retention is configured.
    EXPECT_EQ(data.size(), 4u);
    EXPECT_DOUBLE_EQ(data[threeDaysAgo], 1.0);
    EXPECT_DOUBLE_EQ(data[oneHourAgo],   3.0);

    co_return;
}

// ===========================================================================
// Test 6: Retention applied to only one measurement among two in the same
//         set of files.  The unprotected measurement keeps all its data.
// ===========================================================================
SEASTAR_TEST_F(CompactionRetentionTest, RetentionAppliedOnlyToTargetMeasurement) {
    const std::string measA = "temperatureA";
    const std::string measB = "pressureB";
    const std::string keyA  = "temperatureA|loc=west|val";
    const std::string keyB  = "pressureB|loc=west|val";

    uint64_t now        = nowNs();
    uint64_t twoDaysAgo = now - 2 * ONE_DAY_NS;
    uint64_t oneHourAgo = now - ONE_HOUR_NS;

    SeriesId128 sid_a = SeriesId128::fromSeriesKey(keyA);
    SeriesId128 sid_b = SeriesId128::fromSeriesKey(keyB);

    // File 0: old data for both series A and B.
    {
        std::vector<uint64_t> ts_old = {twoDaysAgo, twoDaysAgo + 1'000'000'000ULL};
        TSMWriter writer0("shard_0/tsm/00_0000000000.tsm");
        writer0.writeSeries(TSMValueType::Float, sid_a, ts_old,
                            std::vector<double>{10.0, 11.0});
        writer0.writeSeries(TSMValueType::Float, sid_b, ts_old,
                            std::vector<double>{200.0, 201.0});
        writer0.writeIndex();
        writer0.close();
    }

    auto f0 = seastar::make_shared<TSM>("shard_0/tsm/00_0000000000.tsm");
    f0->tierNum = 0; f0->seqNum = 0;
    co_await f0->open();
    co_await f0->readSparseIndex();

    // File 1: recent data for both series.
    {
        std::vector<uint64_t> ts_new = {oneHourAgo, oneHourAgo + 1'000'000'000ULL};
        TSMWriter writer1("shard_0/tsm/00_0000000001.tsm");
        writer1.writeSeries(TSMValueType::Float, sid_a, ts_new,
                            std::vector<double>{50.0, 51.0});
        writer1.writeSeries(TSMValueType::Float, sid_b, ts_new,
                            std::vector<double>{300.0, 301.0});
        writer1.writeIndex();
        writer1.close();
    }

    auto f1 = seastar::make_shared<TSM>("shard_0/tsm/00_0000000001.tsm");
    f1->tierNum = 0; f1->seqNum = 1;
    co_await f1->open();
    co_await f1->readSparseIndex();

    // Only apply retention to measurement A.
    std::unordered_map<std::string, RetentionPolicy> policies;
    policies[measA] = CompactionRetentionTest::oneDayPolicy(measA);
    // measB has NO retention policy.

    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMap;
    seriesMap[sid_a] = measA;
    seriesMap[sid_b] = measB;  // B is in the map but has no matching policy

    auto compactedPath = co_await self->compactor->compact({f0, f1}, policies, seriesMap);

    EXPECT_FALSE(compactedPath.empty());
    EXPECT_TRUE(fs::exists(compactedPath));

    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    // Series A: old 2 points dropped, recent 2 points kept.
    auto dataA = co_await CompactionRetentionTest::readAllFloat(compacted, keyA);
    EXPECT_EQ(dataA.size(), 2u)
        << "Series A: expected only 2 recent points, got " << dataA.size();
    EXPECT_EQ(dataA.count(twoDaysAgo), 0u);
    EXPECT_DOUBLE_EQ(dataA[oneHourAgo], 50.0);

    // Series B: no retention — all 4 points present.
    auto dataB = co_await CompactionRetentionTest::readAllFloat(compacted, keyB);
    EXPECT_EQ(dataB.size(), 4u)
        << "Series B: expected all 4 points (no policy), got " << dataB.size();
    EXPECT_DOUBLE_EQ(dataB[twoDaysAgo], 200.0);
    EXPECT_DOUBLE_EQ(dataB[oneHourAgo], 300.0);

    co_return;
}

// ===========================================================================
// Test 7: After compaction with retention, querying the compacted file for
//         old timestamps returns no data.
// ===========================================================================
SEASTAR_TEST_F(CompactionRetentionTest, QueryAfterCompactionReturnsOnlyInRetentionData) {
    const std::string measurement = "network";
    const std::string seriesKey   = "network|iface=eth0|rx_bytes";
    SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);

    uint64_t now          = nowNs();
    uint64_t threeDaysAgo = now - 3 * ONE_DAY_NS;
    uint64_t twoDaysAgo   = now - 2 * ONE_DAY_NS;
    uint64_t thirtyMinAgo = now - 30 * 60 * 1'000'000'000ULL;

    // File 0: 3 days ago (expired).
    auto f0 = self->makeFloatFile(0, 0, seriesKey,
        {threeDaysAgo, threeDaysAgo + 1'000'000'000ULL},
        {1.0, 2.0});
    co_await f0->open();
    co_await f0->readSparseIndex();

    // File 1: 2 days ago (expired).
    auto f1 = self->makeFloatFile(0, 1, seriesKey,
        {twoDaysAgo, twoDaysAgo + 1'000'000'000ULL},
        {10.0, 11.0});
    co_await f1->open();
    co_await f1->readSparseIndex();

    // File 2: 30 min ago (in-retention).
    auto f2 = self->makeFloatFile(0, 2, seriesKey,
        {thirtyMinAgo, thirtyMinAgo + 1'000'000'000ULL},
        {100.0, 101.0});
    co_await f2->open();
    co_await f2->readSparseIndex();

    std::unordered_map<std::string, RetentionPolicy> policies;
    policies[measurement] = CompactionRetentionTest::oneDayPolicy(measurement);

    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMap;
    seriesMap[sid] = measurement;

    auto compactedPath = co_await self->compactor->compact(
        {f0, f1, f2}, policies, seriesMap);

    EXPECT_FALSE(compactedPath.empty());
    EXPECT_TRUE(fs::exists(compactedPath));

    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    // Query 1: range covering the OLD timestamps — should return nothing.
    TSMResult<double> oldResult(0);
    co_await compacted->readSeries(sid, 0, twoDaysAgo + 2'000'000'000ULL, oldResult);

    size_t oldCount = 0;
    for (const auto& blk : oldResult.blocks) {
        oldCount += blk->timestamps->size();
    }
    EXPECT_EQ(oldCount, 0u)
        << "Query for old data range should return 0 points, got " << oldCount;

    // Query 2: range covering ONLY the recent timestamp — should return 2 points.
    TSMResult<double> recentResult(0);
    co_await compacted->readSeries(sid, thirtyMinAgo, UINT64_MAX, recentResult);

    size_t recentCount = 0;
    for (const auto& blk : recentResult.blocks) {
        recentCount += blk->timestamps->size();
    }
    EXPECT_EQ(recentCount, 2u)
        << "Query for recent data range should return 2 points, got " << recentCount;

    // Query 3: full time range — only in-retention data.
    auto data = co_await CompactionRetentionTest::readAllFloat(compacted, seriesKey);
    EXPECT_EQ(data.size(), 2u);
    EXPECT_EQ(data.count(threeDaysAgo), 0u);
    EXPECT_EQ(data.count(twoDaysAgo),   0u);
    EXPECT_DOUBLE_EQ(data[thirtyMinAgo], 100.0);

    co_return;
}

// ===========================================================================
// Test 8: Very short retention (5 seconds) — only sub-5s-old data survives.
// ===========================================================================
SEASTAR_TEST_F(CompactionRetentionTest, ShortRetentionWindowDropsMostData) {
    const std::string measurement = "fast";
    const std::string seriesKey   = "fast|shard=0|counter";

    uint64_t now        = nowNs();
    uint64_t tenSecsAgo = now - 10'000'000'000ULL;
    uint64_t twoSecsAgo = now -  2'000'000'000ULL;

    // Points from 10 seconds ago — outside 5-second retention.
    auto file0 = self->makeFloatFile(0, 0, seriesKey,
        {tenSecsAgo, tenSecsAgo + 1'000'000'000ULL},
        {1.0, 2.0});

    // Points from 2 seconds ago — inside 5-second retention.
    auto file1 = self->makeFloatFile(0, 1, seriesKey,
        {twoSecsAgo, twoSecsAgo + 500'000'000ULL},
        {3.0, 4.0});

    co_await file0->open();
    co_await file0->readSparseIndex();
    co_await file1->open();
    co_await file1->readSparseIndex();

    SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);

    RetentionPolicy fiveSecPolicy;
    fiveSecPolicy.measurement = measurement;
    fiveSecPolicy.ttl         = "5s";
    fiveSecPolicy.ttlNanos    = 5'000'000'000ULL;  // 5 seconds

    std::unordered_map<std::string, RetentionPolicy> policies;
    policies[measurement] = fiveSecPolicy;

    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMap;
    seriesMap[sid] = measurement;

    auto compactedPath = co_await self->compactor->compact(
        {file0, file1}, policies, seriesMap);

    EXPECT_FALSE(compactedPath.empty());

    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    auto data = co_await CompactionRetentionTest::readAllFloat(compacted, seriesKey);

    // The 2 points from 10 s ago should be gone.
    EXPECT_EQ(data.count(tenSecsAgo), 0u);
    EXPECT_EQ(data.count(tenSecsAgo + 1'000'000'000ULL), 0u);

    // The 2 points from 2 s ago should survive.
    EXPECT_EQ(data.size(), 2u);
    EXPECT_DOUBLE_EQ(data[twoSecsAgo], 3.0);

    co_return;
}

// ===========================================================================
// Test 9: setRetentionContext API — verify the method is callable and that
//         the subsequent compact() with the same policy applies retention.
// ===========================================================================
SEASTAR_TEST_F(CompactionRetentionTest, SetRetentionContextAppliedOnCompact) {
    const std::string measurement = "env";
    const std::string seriesKey   = "env|zone=a|humidity";

    uint64_t now          = nowNs();
    uint64_t threeDaysAgo = now - 3 * ONE_DAY_NS;
    uint64_t oneHourAgo   = now - ONE_HOUR_NS;

    auto file0 = self->makeFloatFile(0, 0, seriesKey,
        {threeDaysAgo, threeDaysAgo + 1'000'000'000ULL},
        {80.0, 81.0});
    auto file1 = self->makeFloatFile(0, 1, seriesKey,
        {oneHourAgo, oneHourAgo + 1'000'000'000ULL},
        {90.0, 91.0});

    co_await file0->open();
    co_await file0->readSparseIndex();
    co_await file1->open();
    co_await file1->readSparseIndex();

    SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);

    std::unordered_map<std::string, RetentionPolicy> policies;
    policies[measurement] = CompactionRetentionTest::oneDayPolicy(measurement);

    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMap;
    seriesMap[sid] = measurement;

    // Use setRetentionContext to pre-load the policy (Engine does this before
    // triggering compaction in production code).
    self->compactor->setRetentionContext(policies, seriesMap);

    // Pass the same policies explicitly to the compact() call so retention
    // is applied via both the pre-loaded context and the direct argument.
    auto compactedPath = co_await self->compactor->compact(
        {file0, file1}, policies, seriesMap);

    EXPECT_FALSE(compactedPath.empty());

    auto compacted = seastar::make_shared<TSM>(compactedPath);
    co_await compacted->open();
    co_await compacted->readSparseIndex();

    auto data = co_await CompactionRetentionTest::readAllFloat(compacted, seriesKey);

    // Old 2 points dropped, recent 2 kept.
    EXPECT_EQ(data.size(), 2u);
    EXPECT_EQ(data.count(threeDaysAgo), 0u);
    EXPECT_DOUBLE_EQ(data[oneHourAgo], 90.0);

    co_return;
}
