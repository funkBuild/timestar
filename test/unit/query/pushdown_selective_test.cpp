// Tests for the aggregation-method-aware TSM pushdown optimization.
//
// These tests exercise the LATEST/FIRST-specific block selection paths:
//   - aggregateSeriesSelective (non-bucketed LATEST/FIRST)
//   - aggregateSeriesBucketed  (bucketed LATEST/FIRST with early termination)
//   - File ordering by actual time ranges (not rank)
//   - Tombstone filtering in selective/bucketed paths
//   - Bucket key consistency with BlockAggregator

#include <gtest/gtest.h>
#include <filesystem>

#include "../../../lib/query/block_aggregator.hpp"
#include "../../../lib/query/query_parser.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/core/series_id.hpp"

#include "../../seastar_gtest.hpp"
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------
class PushdownSelectiveTest : public ::testing::Test {
public:
    std::string testDir = "./test_pushdown_selective";
    fs::path savedCwd;

    void SetUp() override {
        savedCwd = fs::current_path();
        if (fs::current_path().filename() == "test_pushdown_selective") {
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

    // Helper: create a TSM file with a single float series.
    seastar::shared_ptr<TSM> createTSMFile(
        uint64_t tier, uint64_t seqNum,
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

};

// Free function so SEASTAR_TEST_F-generated functions can call it.
static void generatePoints(uint64_t baseTime, int count, uint64_t step,
                           std::vector<uint64_t>& ts, std::vector<double>& vals) {
    ts.reserve(count);
    vals.reserve(count);
    for (int i = 0; i < count; i++) {
        ts.push_back(baseTime + i * step);
        vals.push_back(static_cast<double>(i) + 1.0);
    }
}

// ===========================================================================
// aggregateSeriesSelective — non-bucketed
// ===========================================================================

SEASTAR_TEST_F(PushdownSelectiveTest, LatestSelectiveReturnsSingleLatestPoint) {
    // 100 points: t=1000..1990 (step 10), values 1.0..100.0
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 100, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "sel.latest", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sel.latest");
    timestar::BlockAggregator aggregator(0);  // non-bucketed

    size_t pts = co_await tsm->aggregateSeriesSelective(
        seriesId, 1000, 1990, aggregator, /*reverse=*/true, /*maxPoints=*/1);

    EXPECT_EQ(pts, 1u);
    EXPECT_EQ(aggregator.pointCount(), 1u);

    // The single point should be the latest: t=1990, val=100.0
    auto timestamps = aggregator.takeTimestamps();
    auto values = aggregator.takeValues();
    EXPECT_EQ(timestamps.size(), 1u);
    EXPECT_EQ(timestamps[0], 1990u);
    EXPECT_DOUBLE_EQ(values[0], 100.0);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, FirstSelectiveReturnsSingleEarliestPoint) {
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 100, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "sel.first", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sel.first");
    timestar::BlockAggregator aggregator(0);

    size_t pts = co_await tsm->aggregateSeriesSelective(
        seriesId, 1000, 1990, aggregator, /*reverse=*/false, /*maxPoints=*/1);

    EXPECT_EQ(pts, 1u);
    auto timestamps = aggregator.takeTimestamps();
    auto values = aggregator.takeValues();
    EXPECT_EQ(timestamps.size(), 1u);
    EXPECT_EQ(timestamps[0], 1000u);
    EXPECT_DOUBLE_EQ(values[0], 1.0);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, LatestSelectiveMaxPointsThree) {
    // Verify maxPoints > 1 works and returns the N latest points.
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 50, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "sel.multi", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sel.multi");
    timestar::BlockAggregator aggregator(0);

    size_t pts = co_await tsm->aggregateSeriesSelective(
        seriesId, 1000, 1490, aggregator, /*reverse=*/true, /*maxPoints=*/3);

    EXPECT_EQ(pts, 3u);
    auto timestamps = aggregator.takeTimestamps();
    EXPECT_EQ(timestamps.size(), 3u);
    // Should be the 3 latest: 1490, 1480, 1470 (in reverse order as added)
    EXPECT_EQ(timestamps[0], 1490u);
    EXPECT_EQ(timestamps[1], 1480u);
    EXPECT_EQ(timestamps[2], 1470u);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, SelectiveSeriesNotFound) {
    std::vector<uint64_t> ts = {1000, 2000};
    std::vector<double> vals = {1.0, 2.0};

    auto tsm = self->createTSMFile(0, 1, "sel.exists", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 missingId = SeriesId128::fromSeriesKey("sel.missing");
    timestar::BlockAggregator aggregator(0);

    size_t pts = co_await tsm->aggregateSeriesSelective(
        missingId, 0, 99999, aggregator, true, 1);
    EXPECT_EQ(pts, 0u);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, SelectiveTimeRangeFilter) {
    // 100 points at t=1000..1990, query only [1500, 1700]
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 100, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "sel.range", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sel.range");
    timestar::BlockAggregator aggregator(0);

    size_t pts = co_await tsm->aggregateSeriesSelective(
        seriesId, 1500, 1700, aggregator, /*reverse=*/true, /*maxPoints=*/1);

    EXPECT_EQ(pts, 1u);
    auto timestamps = aggregator.takeTimestamps();
    EXPECT_EQ(timestamps.size(), 1u);
    // Latest within [1500, 1700] = 1700
    EXPECT_EQ(timestamps[0], 1700u);

    co_return;
}

// ===========================================================================
// aggregateSeriesSelective with tombstones
// ===========================================================================

SEASTAR_TEST_F(PushdownSelectiveTest, LatestSelectiveSkipsTombstonedPoints) {
    // 10 points: t=1000..1090 (step 10), values 1.0..10.0
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 10, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "sel.tomb", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sel.tomb");

    // Tombstone the last 3 points: [1070, 1090]
    co_await tsm->deleteRange(seriesId, 1070, 1090);

    timestar::BlockAggregator aggregator(0);
    size_t pts = co_await tsm->aggregateSeriesSelective(
        seriesId, 1000, 1090, aggregator, /*reverse=*/true, /*maxPoints=*/1);

    EXPECT_EQ(pts, 1u);
    auto timestamps = aggregator.takeTimestamps();
    EXPECT_EQ(timestamps.size(), 1u);
    // Latest non-tombstoned point: t=1060, val=7.0
    EXPECT_EQ(timestamps[0], 1060u);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, FirstSelectiveSkipsTombstonedPoints) {
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 10, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "sel.tomb.first", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sel.tomb.first");

    // Tombstone first 3 points: [1000, 1020]
    co_await tsm->deleteRange(seriesId, 1000, 1020);

    timestar::BlockAggregator aggregator(0);
    size_t pts = co_await tsm->aggregateSeriesSelective(
        seriesId, 1000, 1090, aggregator, /*reverse=*/false, /*maxPoints=*/1);

    EXPECT_EQ(pts, 1u);
    auto timestamps = aggregator.takeTimestamps();
    EXPECT_EQ(timestamps.size(), 1u);
    // First non-tombstoned: t=1030, val=4.0
    EXPECT_EQ(timestamps[0], 1030u);

    co_return;
}

// ===========================================================================
// aggregateSeriesBucketed
// ===========================================================================

SEASTAR_TEST_F(PushdownSelectiveTest, LatestBucketedFillsBuckets) {
    // 100 points: t=1000..1990, step=10
    // Buckets with interval=500: [1000, 1500]
    // Each bucket should get only 1 point (the latest in that bucket).
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 100, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "bkt.latest", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("bkt.latest");

    uint64_t interval = 500;
    uint64_t startTime = 1000;
    uint64_t endTime = 1990;

    // Compute expected bucket count using the same formula as BlockAggregator
    uint64_t firstBucket = (startTime / interval) * interval;
    uint64_t lastBucket = (endTime / interval) * interval;
    size_t totalBuckets = static_cast<size_t>((lastBucket - firstBucket) / interval + 1);

    std::unordered_set<uint64_t> filledBuckets;
    timestar::BlockAggregator aggregator(interval, startTime, endTime);

    size_t pts = co_await tsm->aggregateSeriesBucketed(
        seriesId, startTime, endTime, aggregator, /*reverse=*/true,
        interval, filledBuckets, totalBuckets);

    // Should have filled all buckets, one point each
    EXPECT_EQ(filledBuckets.size(), totalBuckets);
    EXPECT_EQ(pts, totalBuckets);

    // Verify bucket keys match BlockAggregator's formula
    auto bucketStates = aggregator.takeBucketStates();
    for (const auto& [key, state] : bucketStates) {
        EXPECT_EQ(key, (key / interval) * interval)
            << "Bucket key should be interval-aligned (absolute truncation)";
        EXPECT_TRUE(filledBuckets.count(key))
            << "filledBuckets should contain same keys as aggregator bucketStates";
    }

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, FirstBucketedFillsBuckets) {
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 100, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "bkt.first", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("bkt.first");

    uint64_t interval = 500;
    uint64_t startTime = 1000;
    uint64_t endTime = 1990;
    uint64_t firstBucket = (startTime / interval) * interval;
    uint64_t lastBucket = (endTime / interval) * interval;
    size_t totalBuckets = static_cast<size_t>((lastBucket - firstBucket) / interval + 1);

    std::unordered_set<uint64_t> filledBuckets;
    timestar::BlockAggregator aggregator(interval, startTime, endTime);

    size_t pts = co_await tsm->aggregateSeriesBucketed(
        seriesId, startTime, endTime, aggregator, /*reverse=*/false,
        interval, filledBuckets, totalBuckets);

    EXPECT_EQ(filledBuckets.size(), totalBuckets);
    EXPECT_EQ(pts, totalBuckets);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, BucketedLatestGetsActualLatestPerBucket) {
    // Verify that within a single block, the latest point per bucket is selected
    // (not the first point encountered).
    // 10 points in one bucket (interval=10000 > data range):
    // t=1000,1010,1020,...,1090  val=1,2,...,10
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 10, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "bkt.within", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("bkt.within");

    uint64_t interval = 10000;  // One bucket covers all points
    uint64_t startTime = 1000;
    uint64_t endTime = 1090;
    size_t totalBuckets = 1;

    std::unordered_set<uint64_t> filledBuckets;
    timestar::BlockAggregator aggregator(interval, startTime, endTime);

    co_await tsm->aggregateSeriesBucketed(
        seriesId, startTime, endTime, aggregator, /*reverse=*/true,
        interval, filledBuckets, totalBuckets);

    EXPECT_EQ(filledBuckets.size(), 1u);

    auto bucketStates = aggregator.takeBucketStates();
    EXPECT_EQ(bucketStates.size(), 1u);

    // The single bucket should contain the latest point (t=1090, val=10.0)
    auto& state = bucketStates.begin()->second;
    EXPECT_EQ(state.latestTimestamp, 1090u);
    EXPECT_DOUBLE_EQ(state.latest, 10.0);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, BucketedFirstGetsActualFirstPerBucket) {
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 10, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "bkt.within.first", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("bkt.within.first");

    uint64_t interval = 10000;
    uint64_t startTime = 1000;
    uint64_t endTime = 1090;
    size_t totalBuckets = 1;

    std::unordered_set<uint64_t> filledBuckets;
    timestar::BlockAggregator aggregator(interval, startTime, endTime);

    co_await tsm->aggregateSeriesBucketed(
        seriesId, startTime, endTime, aggregator, /*reverse=*/false,
        interval, filledBuckets, totalBuckets);

    auto bucketStates = aggregator.takeBucketStates();
    EXPECT_EQ(bucketStates.size(), 1u);

    auto& state = bucketStates.begin()->second;
    // FIRST: earliest point in bucket (t=1000, val=1.0)
    // AggregationState tracks firstTimestamp via the earliest addValue call
    EXPECT_EQ(state.latestTimestamp, 1000u);
    EXPECT_DOUBLE_EQ(state.latest, 1.0);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, BucketedSkipsAlreadyFilledBuckets) {
    // Create a file, pre-fill one bucket in filledBuckets, verify it's skipped.
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 100, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "bkt.skip", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("bkt.skip");

    uint64_t interval = 500;
    uint64_t startTime = 1000;
    uint64_t endTime = 1990;
    uint64_t firstBucket = (startTime / interval) * interval;
    uint64_t lastBucket = (endTime / interval) * interval;
    size_t totalBuckets = static_cast<size_t>((lastBucket - firstBucket) / interval + 1);

    // Pre-fill bucket 1000
    std::unordered_set<uint64_t> filledBuckets;
    filledBuckets.insert(1000);

    timestar::BlockAggregator aggregator(interval, startTime, endTime);
    // Manually add a "pre-existing" point for bucket 1000
    aggregator.addPoint(1499, 999.0);

    co_await tsm->aggregateSeriesBucketed(
        seriesId, startTime, endTime, aggregator, /*reverse=*/true,
        interval, filledBuckets, totalBuckets);

    // The pre-filled bucket should still have only 1 point (from our manual add)
    // plus one point per remaining bucket
    EXPECT_EQ(filledBuckets.size(), totalBuckets);

    auto bucketStates = aggregator.takeBucketStates();
    // Bucket 1000 should contain our manually-added value, not overwritten
    auto it = bucketStates.find(1000);
    EXPECT_NE(it, bucketStates.end());
    // The bucket has the pre-existing point (t=1499, v=999.0) plus the new aggregated point.
    // Since we pre-filled it in filledBuckets, no new points should be added from TSM.
    // addPoint was called once manually, so count should be 1.
    EXPECT_EQ(it->second.count, 1u);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, BucketedEarlyTerminationWhenAllFilled) {
    // Pre-fill ALL buckets, verify zero TSM points are read.
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 100, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "bkt.noop", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("bkt.noop");

    uint64_t interval = 500;
    uint64_t startTime = 1000;
    uint64_t endTime = 1990;
    uint64_t firstBucket = (startTime / interval) * interval;
    uint64_t lastBucket = (endTime / interval) * interval;
    size_t totalBuckets = static_cast<size_t>((lastBucket - firstBucket) / interval + 1);

    std::unordered_set<uint64_t> filledBuckets;
    for (uint64_t b = firstBucket; b <= lastBucket; b += interval) {
        filledBuckets.insert(b);
    }

    timestar::BlockAggregator aggregator(interval, startTime, endTime);

    size_t pts = co_await tsm->aggregateSeriesBucketed(
        seriesId, startTime, endTime, aggregator, /*reverse=*/true,
        interval, filledBuckets, totalBuckets);

    EXPECT_EQ(pts, 0u);
    EXPECT_EQ(aggregator.pointCount(), 0u);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, BucketedWithTombstones) {
    // 10 points, single bucket. Tombstone the latest, verify next-latest used.
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 10, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "bkt.tomb", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("bkt.tomb");

    // Tombstone the last point: [1090, 1090]
    co_await tsm->deleteRange(seriesId, 1090, 1090);

    uint64_t interval = 10000;
    uint64_t startTime = 1000;
    uint64_t endTime = 1090;
    size_t totalBuckets = 1;

    std::unordered_set<uint64_t> filledBuckets;
    timestar::BlockAggregator aggregator(interval, startTime, endTime);

    co_await tsm->aggregateSeriesBucketed(
        seriesId, startTime, endTime, aggregator, /*reverse=*/true,
        interval, filledBuckets, totalBuckets);

    auto bucketStates = aggregator.takeBucketStates();
    EXPECT_EQ(bucketStates.size(), 1u);

    // Bucket should contain t=1080 (latest non-tombstoned)
    auto& state = bucketStates.begin()->second;
    EXPECT_EQ(state.latestTimestamp, 1080u);
    EXPECT_DOUBLE_EQ(state.latest, 9.0);

    co_return;
}

// ===========================================================================
// Bucket key consistency: verify filledBuckets keys match BlockAggregator's
// bucket formula when startTime is NOT interval-aligned.
// This is the P0 bug the review caught (now fixed).
// ===========================================================================

SEASTAR_TEST_F(PushdownSelectiveTest, BucketKeyConsistencyUnalignedStartTime) {
    // startTime=1050 (not aligned to interval=500 → bucket boundary is 1000).
    // Ensure filledBuckets uses (t/interval)*interval, NOT relative to startTime.
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1050, 50, 10, ts, vals);  // t=1050..1540

    auto tsm = self->createTSMFile(0, 1, "bkt.unalign", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("bkt.unalign");

    uint64_t interval = 500;
    uint64_t startTime = 1050;
    uint64_t endTime = 1540;

    // Compute buckets using the aggregator's formula
    uint64_t firstBucket = (startTime / interval) * interval;  // 1000
    uint64_t lastBucket = (endTime / interval) * interval;      // 1500
    size_t totalBuckets = static_cast<size_t>((lastBucket - firstBucket) / interval + 1);  // 2

    std::unordered_set<uint64_t> filledBuckets;
    timestar::BlockAggregator aggregator(interval, startTime, endTime);

    co_await tsm->aggregateSeriesBucketed(
        seriesId, startTime, endTime, aggregator, /*reverse=*/true,
        interval, filledBuckets, totalBuckets);

    // Verify filledBuckets contains keys matching aggregator's formula
    EXPECT_EQ(filledBuckets.size(), totalBuckets);
    for (uint64_t b = firstBucket; b <= lastBucket; b += interval) {
        EXPECT_TRUE(filledBuckets.count(b))
            << "Expected bucket key " << b << " (absolute truncation) in filledBuckets";
    }

    // Verify aggregator's bucketStates keys are the same
    auto bucketStates = aggregator.takeBucketStates();
    for (const auto& [key, _] : bucketStates) {
        EXPECT_TRUE(filledBuckets.count(key))
            << "Aggregator bucket key " << key << " not found in filledBuckets";
    }

    co_return;
}

// ===========================================================================
// Multi-block tests: verify correct behavior across block boundaries.
// MaxPointsPerBlock defaults to 3000, so 5000 points => 2 blocks.
// ===========================================================================

SEASTAR_TEST_F(PushdownSelectiveTest, LatestSelectiveMultiBlock) {
    // 5000 points spanning 2 blocks. LATEST should read only the last block.
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 5000, 10, ts, vals);
    // Last timestamp: 1000 + 4999 * 10 = 50990

    auto tsm = self->createTSMFile(0, 1, "sel.multiblock", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sel.multiblock");
    timestar::BlockAggregator aggregator(0);

    size_t pts = co_await tsm->aggregateSeriesSelective(
        seriesId, 1000, 50990, aggregator, /*reverse=*/true, /*maxPoints=*/1);

    EXPECT_EQ(pts, 1u);
    auto timestamps = aggregator.takeTimestamps();
    EXPECT_EQ(timestamps.size(), 1u);
    EXPECT_EQ(timestamps[0], 50990u);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, FirstSelectiveMultiBlock) {
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 5000, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "sel.multiblock.first", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sel.multiblock.first");
    timestar::BlockAggregator aggregator(0);

    size_t pts = co_await tsm->aggregateSeriesSelective(
        seriesId, 1000, 50990, aggregator, /*reverse=*/false, /*maxPoints=*/1);

    EXPECT_EQ(pts, 1u);
    auto timestamps = aggregator.takeTimestamps();
    EXPECT_EQ(timestamps.size(), 1u);
    EXPECT_EQ(timestamps[0], 1000u);

    co_return;
}

// ===========================================================================
// Edge cases
// ===========================================================================

SEASTAR_TEST_F(PushdownSelectiveTest, SinglePointFile) {
    std::vector<uint64_t> ts = {5000};
    std::vector<double> vals = {42.0};

    auto tsm = self->createTSMFile(0, 1, "sel.single", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sel.single");

    // LATEST
    {
        timestar::BlockAggregator aggregator(0);
        size_t pts = co_await tsm->aggregateSeriesSelective(
            seriesId, 0, 99999, aggregator, true, 1);
        EXPECT_EQ(pts, 1u);
        auto timestamps = aggregator.takeTimestamps();
        EXPECT_EQ(timestamps[0], 5000u);
    }

    // FIRST
    {
        timestar::BlockAggregator aggregator(0);
        size_t pts = co_await tsm->aggregateSeriesSelective(
            seriesId, 0, 99999, aggregator, false, 1);
        EXPECT_EQ(pts, 1u);
        auto timestamps = aggregator.takeTimestamps();
        EXPECT_EQ(timestamps[0], 5000u);
    }

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, EmptyTimeRange) {
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 100, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "sel.empty", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sel.empty");
    timestar::BlockAggregator aggregator(0);

    // Query a time range that doesn't overlap any data
    size_t pts = co_await tsm->aggregateSeriesSelective(
        seriesId, 9000, 9999, aggregator, true, 1);
    EXPECT_EQ(pts, 0u);

    co_return;
}

SEASTAR_TEST_F(PushdownSelectiveTest, AllPointsTombstoned) {
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    generatePoints(1000, 10, 10, ts, vals);

    auto tsm = self->createTSMFile(0, 1, "sel.alltomb", ts, vals);
    co_await tsm->open();
    co_await tsm->readSparseIndex();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sel.alltomb");
    co_await tsm->deleteRange(seriesId, 1000, 1090);

    timestar::BlockAggregator aggregator(0);
    size_t pts = co_await tsm->aggregateSeriesSelective(
        seriesId, 1000, 1090, aggregator, true, 1);
    EXPECT_EQ(pts, 0u);

    co_return;
}
