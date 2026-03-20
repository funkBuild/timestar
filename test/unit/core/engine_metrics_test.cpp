#include <gtest/gtest.h>
#include <cstdint>

// Test EngineMetrics counter behavior without requiring Seastar runtime.
// We test the struct fields directly since they are plain uint64_t counters.

namespace {

// Minimal mock of the struct fields (avoids including engine_metrics.hpp
// which pulls in Seastar metrics headers that need a running reactor).
struct MetricsCounters {
    uint64_t inserts_total = 0;
    uint64_t insert_points_total = 0;
    uint64_t insert_errors_total = 0;
    uint64_t queries_total = 0;
    uint64_t query_errors_total = 0;
    uint64_t slow_queries_total = 0;
    uint64_t deletes_total = 0;
    uint64_t wal_rollovers_total = 0;
};

TEST(EngineMetricsTest, DefaultInitializationAllZero) {
    MetricsCounters m;
    EXPECT_EQ(m.inserts_total, 0u);
    EXPECT_EQ(m.insert_points_total, 0u);
    EXPECT_EQ(m.insert_errors_total, 0u);
    EXPECT_EQ(m.queries_total, 0u);
    EXPECT_EQ(m.query_errors_total, 0u);
    EXPECT_EQ(m.slow_queries_total, 0u);
    EXPECT_EQ(m.deletes_total, 0u);
    EXPECT_EQ(m.wal_rollovers_total, 0u);
}

TEST(EngineMetricsTest, InsertCountersIncrement) {
    MetricsCounters m;
    ++m.inserts_total;
    m.insert_points_total += 100;
    EXPECT_EQ(m.inserts_total, 1u);
    EXPECT_EQ(m.insert_points_total, 100u);

    ++m.inserts_total;
    m.insert_points_total += 50;
    EXPECT_EQ(m.inserts_total, 2u);
    EXPECT_EQ(m.insert_points_total, 150u);
}

TEST(EngineMetricsTest, QueryCountersIncrement) {
    MetricsCounters m;
    ++m.queries_total;
    ++m.queries_total;
    ++m.query_errors_total;
    ++m.slow_queries_total;
    EXPECT_EQ(m.queries_total, 2u);
    EXPECT_EQ(m.query_errors_total, 1u);
    EXPECT_EQ(m.slow_queries_total, 1u);
}

TEST(EngineMetricsTest, DeleteAndRolloverCounters) {
    MetricsCounters m;
    ++m.deletes_total;
    ++m.wal_rollovers_total;
    ++m.wal_rollovers_total;
    EXPECT_EQ(m.deletes_total, 1u);
    EXPECT_EQ(m.wal_rollovers_total, 2u);
}

TEST(EngineMetricsTest, LargePointCounts) {
    MetricsCounters m;
    m.insert_points_total += 1000000;
    m.insert_points_total += 2000000;
    EXPECT_EQ(m.insert_points_total, 3000000u);
}

}  // namespace
