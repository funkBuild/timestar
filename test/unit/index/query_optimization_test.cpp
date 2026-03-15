/*
 * Phase 4: Query Optimization Tests
 *
 * Tests for HyperLogLog cardinality estimation, measurement bloom filters,
 * and cardinality estimation methods on NativeIndex.
 */

#include "../../../lib/index/native/hyperloglog.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <cmath>
#include <filesystem>
#include <random>
#include <string>

using namespace timestar::index;

// ============================================================================
// HyperLogLog unit tests
// ============================================================================

class HyperLogLogTest : public ::testing::Test {};

TEST(HyperLogLogTest, EmptySketch) {
    HyperLogLog hll;
    EXPECT_TRUE(hll.empty());
    EXPECT_DOUBLE_EQ(hll.estimate(), 0.0);
}

TEST(HyperLogLogTest, SingleItem) {
    HyperLogLog hll;
    hll.add(static_cast<uint32_t>(42));
    EXPECT_FALSE(hll.empty());
    EXPECT_GT(hll.estimate(), 0.0);
    EXPECT_LT(hll.estimate(), 5.0);  // Should be close to 1
}

TEST(HyperLogLogTest, Accuracy1K) {
    HyperLogLog hll;
    constexpr int N = 1000;
    for (uint32_t i = 0; i < N; ++i) {
        hll.add(i);
    }
    double est = hll.estimate();
    double error = std::abs(est - N) / N;
    EXPECT_LT(error, 0.05) << "Estimated " << est << " for " << N << " items (error: " << error * 100 << "%)";
}

TEST(HyperLogLogTest, Accuracy10K) {
    HyperLogLog hll;
    constexpr int N = 10000;
    for (uint32_t i = 0; i < N; ++i) {
        hll.add(i);
    }
    double est = hll.estimate();
    double error = std::abs(est - N) / N;
    EXPECT_LT(error, 0.05) << "Estimated " << est << " for " << N << " items (error: " << error * 100 << "%)";
}

TEST(HyperLogLogTest, Accuracy100K) {
    HyperLogLog hll;
    constexpr int N = 100000;
    for (uint32_t i = 0; i < N; ++i) {
        hll.add(i);
    }
    double est = hll.estimate();
    double error = std::abs(est - N) / N;
    EXPECT_LT(error, 0.05) << "Estimated " << est << " for " << N << " items (error: " << error * 100 << "%)";
}

TEST(HyperLogLogTest, DuplicatesDoNotInflate) {
    HyperLogLog hll;
    for (int round = 0; round < 100; ++round) {
        for (uint32_t i = 0; i < 100; ++i) {
            hll.add(i);
        }
    }
    double est = hll.estimate();
    EXPECT_LT(est, 150.0) << "Duplicates should not inflate estimate: " << est;
}

TEST(HyperLogLogTest, StringKeys) {
    HyperLogLog hll;
    constexpr int N = 5000;
    for (int i = 0; i < N; ++i) {
        hll.add(std::string_view("key-" + std::to_string(i)));
    }
    double est = hll.estimate();
    double error = std::abs(est - N) / N;
    EXPECT_LT(error, 0.05) << "Estimated " << est << " for " << N << " string keys";
}

TEST(HyperLogLogTest, SerializationRoundTrip) {
    HyperLogLog original;
    for (uint32_t i = 0; i < 10000; ++i) {
        original.add(i);
    }
    double originalEst = original.estimate();

    std::string serialized;
    original.serialize(serialized);
    EXPECT_EQ(serialized.size(), HyperLogLog::SERIALIZED_SIZE);

    auto restored = HyperLogLog::deserialize(serialized);
    EXPECT_DOUBLE_EQ(restored.estimate(), originalEst);
}

TEST(HyperLogLogTest, MergeUnion) {
    HyperLogLog a, b;

    // A has 0..4999, B has 5000..9999 → merged should estimate ~10000
    for (uint32_t i = 0; i < 5000; ++i) a.add(i);
    for (uint32_t i = 5000; i < 10000; ++i) b.add(i);

    a.merge(b);
    double est = a.estimate();
    double error = std::abs(est - 10000.0) / 10000.0;
    EXPECT_LT(error, 0.05) << "Merged estimate: " << est;
}

TEST(HyperLogLogTest, MergeOverlapping) {
    HyperLogLog a, b;

    // A has 0..7999, B has 5000..9999 → merged should estimate ~10000
    for (uint32_t i = 0; i < 8000; ++i) a.add(i);
    for (uint32_t i = 5000; i < 10000; ++i) b.add(i);

    a.merge(b);
    double est = a.estimate();
    double error = std::abs(est - 10000.0) / 10000.0;
    EXPECT_LT(error, 0.05) << "Merged overlapping estimate: " << est;
}

TEST(HyperLogLogTest, EmptySerializationRoundTrip) {
    HyperLogLog original;
    std::string serialized;
    original.serialize(serialized);
    auto restored = HyperLogLog::deserialize(serialized);
    EXPECT_TRUE(restored.empty());
    EXPECT_DOUBLE_EQ(restored.estimate(), 0.0);
}

TEST(HyperLogLogTest, MergeWithEmpty) {
    HyperLogLog a;
    for (uint32_t i = 0; i < 1000; ++i) a.add(i);
    double before = a.estimate();

    HyperLogLog empty;
    a.merge(empty);
    EXPECT_DOUBLE_EQ(a.estimate(), before);
}

// ============================================================================
// NativeIndex cardinality estimation integration tests
// ============================================================================

class QueryOptimizationTest : public ::testing::Test {
public:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

SEASTAR_TEST_F(QueryOptimizationTest, MeasurementHLLUpdatedOnInsert) {
    NativeIndex index(0);
    co_await index.open();

    // Insert 100 series into "cpu" measurement
    for (int i = 0; i < 100; ++i) {
        co_await index.getOrCreateSeriesId("cpu",
            {{"host", "server-" + std::to_string(i)}}, "usage");
    }

    double est = index.estimateMeasurementCardinality("cpu");
    EXPECT_GT(est, 80.0) << "Should estimate close to 100 series";
    EXPECT_LT(est, 120.0) << "Should estimate close to 100 series";

    co_await index.close();
}

SEASTAR_TEST_F(QueryOptimizationTest, TagHLLUpdatedOnInsert) {
    NativeIndex index(0);
    co_await index.open();

    // Insert 50 series with region=us-west
    for (int i = 0; i < 50; ++i) {
        co_await index.getOrCreateSeriesId("cpu",
            {{"host", "server-" + std::to_string(i)}, {"region", "us-west"}}, "usage");
    }
    // Insert 30 series with region=us-east
    for (int i = 50; i < 80; ++i) {
        co_await index.getOrCreateSeriesId("cpu",
            {{"host", "server-" + std::to_string(i)}, {"region", "us-east"}}, "usage");
    }

    double westEst = index.estimateTagCardinality("cpu", "region", "us-west");
    EXPECT_GT(westEst, 40.0) << "us-west should estimate ~50";
    EXPECT_LT(westEst, 65.0);

    double eastEst = index.estimateTagCardinality("cpu", "region", "us-east");
    EXPECT_GT(eastEst, 22.0) << "us-east should estimate ~30";
    EXPECT_LT(eastEst, 40.0);

    co_await index.close();
}

SEASTAR_TEST_F(QueryOptimizationTest, HLLPersistsAcrossFlushCompactReopen) {
    // Phase 1: Insert data and close (triggers flush)
    {
        NativeIndex index(0);
        co_await index.open();

        for (int i = 0; i < 200; ++i) {
            co_await index.getOrCreateSeriesId("temperature",
                {{"sensor", "sensor-" + std::to_string(i)}}, "value");
        }

        // Explicit compaction to force HLLs to SSTable
        co_await index.compact();
        co_await index.close();
    }

    // Phase 2: Reopen and verify HLLs survived
    {
        NativeIndex index(0);
        co_await index.open();

        double est = index.estimateMeasurementCardinality("temperature");
        EXPECT_GT(est, 160.0) << "HLL should persist: estimated " << est << " (expected ~200)";
        EXPECT_LT(est, 240.0);

        co_await index.close();
    }
}

SEASTAR_TEST_F(QueryOptimizationTest, NonExistentMeasurementReturnsZero) {
    NativeIndex index(0);
    co_await index.open();

    double est = index.estimateMeasurementCardinality("nonexistent");
    EXPECT_DOUBLE_EQ(est, 0.0);

    double tagEst = index.estimateTagCardinality("nonexistent", "host", "server-1");
    EXPECT_DOUBLE_EQ(tagEst, 0.0);

    co_await index.close();
}

SEASTAR_TEST_F(QueryOptimizationTest, MultipleMeasurementsIndependent) {
    NativeIndex index(0);
    co_await index.open();

    for (int i = 0; i < 100; ++i) {
        co_await index.getOrCreateSeriesId("cpu",
            {{"host", "h-" + std::to_string(i)}}, "usage");
    }
    for (int i = 0; i < 50; ++i) {
        co_await index.getOrCreateSeriesId("memory",
            {{"host", "h-" + std::to_string(i)}}, "used");
    }

    double cpuEst = index.estimateMeasurementCardinality("cpu");
    double memEst = index.estimateMeasurementCardinality("memory");

    EXPECT_GT(cpuEst, 80.0);
    EXPECT_LT(cpuEst, 120.0);
    EXPECT_GT(memEst, 38.0);
    EXPECT_LT(memEst, 65.0);

    co_await index.close();
}

SEASTAR_TEST_F(QueryOptimizationTest, BloomFilterPreventsUnnecessaryLookups) {
    NativeIndex index(0);
    co_await index.open();

    // Insert series to populate bitmaps
    for (int i = 0; i < 50; ++i) {
        co_await index.getOrCreateSeriesId("cpu",
            {{"host", "server-" + std::to_string(i)}, {"region", "us-west"}}, "usage");
    }

    // Flush to build bloom filters
    co_await index.compact();

    // Query for a tag value that doesn't exist — bloom should catch this
    auto result = co_await index.findSeries("cpu", {{"host", "nonexistent-host-xyz"}});
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 0u);

    co_await index.close();
}

SEASTAR_TEST_F(QueryOptimizationTest, BloomFilterAllowsExistingTags) {
    NativeIndex index(0);
    co_await index.open();

    for (int i = 0; i < 20; ++i) {
        co_await index.getOrCreateSeriesId("cpu",
            {{"host", "server-" + std::to_string(i)}}, "usage");
    }

    co_await index.compact();

    // Query for a tag that DOES exist — bloom must not block it
    auto result = co_await index.findSeries("cpu", {{"host", "server-5"}});
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);

    co_await index.close();
}

SEASTAR_TEST_F(QueryOptimizationTest, CardinalityAfterMixedInserts) {
    NativeIndex index(0);
    co_await index.open();

    // Insert 3 measurements with overlapping tag keys
    for (int i = 0; i < 100; ++i) {
        std::string host = "h-" + std::to_string(i);
        co_await index.getOrCreateSeriesId("cpu",
            {{"host", host}, {"region", i < 50 ? "east" : "west"}}, "usage");
        co_await index.getOrCreateSeriesId("memory",
            {{"host", host}, {"region", i < 50 ? "east" : "west"}}, "used");
    }
    for (int i = 0; i < 30; ++i) {
        co_await index.getOrCreateSeriesId("disk",
            {{"host", "h-" + std::to_string(i)}, {"mount", "/data"}}, "free");
    }

    double cpuEst = index.estimateMeasurementCardinality("cpu");
    double memEst = index.estimateMeasurementCardinality("memory");
    double diskEst = index.estimateMeasurementCardinality("disk");

    // CPU and memory should both be ~100
    EXPECT_GT(cpuEst, 80.0);
    EXPECT_LT(cpuEst, 125.0);
    EXPECT_GT(memEst, 80.0);
    EXPECT_LT(memEst, 125.0);
    // Disk should be ~30
    EXPECT_GT(diskEst, 22.0);
    EXPECT_LT(diskEst, 40.0);

    // Tag cardinality for region=east (cpu) should be ~50
    double eastEst = index.estimateTagCardinality("cpu", "region", "east");
    EXPECT_GT(eastEst, 38.0);
    EXPECT_LT(eastEst, 65.0);

    co_await index.close();
}
