// Seastar-based tests for QueryRunner component
// Tests TSM query execution, memory store integration, result merging,
// type-based routing, empty result handling, and large result sets.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/query/query_result.hpp"
#include "../../../lib/query/query_runner.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

class QueryRunnerSeastarTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }

    void TearDown() override { cleanTestShardDirectories(); }
};

// Helper: run a block of async test code with Engine lifecycle management.
// Ensures engine.stop() is always called, even on assertion failure.
#define WITH_ENGINE(engine_var, body)        \
    do {                                     \
        Engine engine_var;                   \
        std::exception_ptr __ex;             \
        try {                                \
            co_await engine_var.init();      \
            body                             \
        } catch (...) {                      \
            __ex = std::current_exception(); \
        }                                    \
        co_await engine_var.stop();          \
        if (__ex)                            \
            std::rethrow_exception(__ex);    \
    } while (0)

// ---------------------------------------------------------------------------
// Test: Query float data from memory store (data not yet flushed to TSM)
// ---------------------------------------------------------------------------
seastar::future<> testQueryFloatFromMemoryStore() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert("temperature", "value");
        insert.addTag("location", "us-west");
        insert.addValue(1000, 20.5);
        insert.addValue(2000, 21.0);
        insert.addValue(3000, 21.5);
        insert.addValue(4000, 22.0);
        insert.addValue(5000, 22.5);

        co_await engine.insert(std::move(insert));

        auto resultOpt = co_await engine.query("temperature,location=us-west value", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 5u);
            EXPECT_EQ(result.values.size(), 5u);
            EXPECT_DOUBLE_EQ(result.values[0], 20.5);
            EXPECT_DOUBLE_EQ(result.values[4], 22.5);
            EXPECT_EQ(result.timestamps[0], 1000u);
            EXPECT_EQ(result.timestamps[4], 5000u);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryFloatFromMemoryStore) {
    co_await testQueryFloatFromMemoryStore();
}

// ---------------------------------------------------------------------------
// Test: Query boolean data
// ---------------------------------------------------------------------------
seastar::future<> testQueryBooleanData() {
    WITH_ENGINE(engine, {
        TimeStarInsert<bool> insert("door", "open");
        insert.addTag("building", "hq");
        insert.addValue(1000, true);
        insert.addValue(2000, false);
        insert.addValue(3000, true);
        insert.addValue(4000, false);

        co_await engine.insert(std::move(insert));

        auto resultOpt = co_await engine.query("door,building=hq open", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<bool>>(resultOpt.value());
            EXPECT_EQ(result.values.size(), 4u);
            EXPECT_EQ(result.values[0], true);
            EXPECT_EQ(result.values[1], false);
            EXPECT_EQ(result.values[2], true);
            EXPECT_EQ(result.values[3], false);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryBooleanData) {
    co_await testQueryBooleanData();
}

// ---------------------------------------------------------------------------
// Test: Query string data
// ---------------------------------------------------------------------------
seastar::future<> testQueryStringData() {
    WITH_ENGINE(engine, {
        TimeStarInsert<std::string> insert("logs", "message");
        insert.addTag("level", "info");
        insert.addValue(1000, "server started");
        insert.addValue(2000, "connection established");
        insert.addValue(3000, "request processed");

        co_await engine.insert(std::move(insert));

        auto resultOpt = co_await engine.query("logs,level=info message", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<std::string>>(resultOpt.value());
            EXPECT_EQ(result.values.size(), 3u);
            EXPECT_EQ(result.values[0], "server started");
            EXPECT_EQ(result.values[1], "connection established");
            EXPECT_EQ(result.values[2], "request processed");
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryStringData) {
    co_await testQueryStringData();
}

// ---------------------------------------------------------------------------
// Test: Query non-existent series returns nullopt
// ---------------------------------------------------------------------------
seastar::future<> testQueryNonExistentSeries() {
    WITH_ENGINE(engine, {
        auto resultOpt = co_await engine.query("nonexistent series_key", 0, UINT64_MAX);
        EXPECT_FALSE(resultOpt.has_value());
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryNonExistentSeries) {
    co_await testQueryNonExistentSeries();
}

// ---------------------------------------------------------------------------
// Test: Time range filtering returns only points within range
// ---------------------------------------------------------------------------
seastar::future<> testTimeRangeFiltering() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert("cpu", "usage");
        for (uint64_t ts = 1000; ts <= 10000; ts += 1000) {
            insert.addValue(ts, static_cast<double>(ts) / 100.0);
        }

        co_await engine.insert(std::move(insert));

        // Query a sub-range: timestamps 3000-7000
        auto resultOpt = co_await engine.query("cpu usage", 3000, 7000);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 5u);
            EXPECT_EQ(result.timestamps[0], 3000u);
            EXPECT_EQ(result.timestamps[4], 7000u);
            EXPECT_DOUBLE_EQ(result.values[0], 30.0);
            EXPECT_DOUBLE_EQ(result.values[4], 70.0);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, TimeRangeFiltering) {
    co_await testTimeRangeFiltering();
}

// ---------------------------------------------------------------------------
// Test: Query after rollover to TSM (data read from TSM files)
// ---------------------------------------------------------------------------
seastar::future<> testQueryFromTSMFiles() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert("disk", "usage");
        insert.addValue(1000, 50.0);
        insert.addValue(2000, 55.0);
        insert.addValue(3000, 60.0);

        co_await engine.insert(std::move(insert));

        // Force rollover to create a TSM file
        co_await engine.rolloverMemoryStore();

        // Query should still return data (now from TSM)
        auto resultOpt = co_await engine.query("disk usage", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 3u);
            EXPECT_DOUBLE_EQ(result.values[0], 50.0);
            EXPECT_DOUBLE_EQ(result.values[1], 55.0);
            EXPECT_DOUBLE_EQ(result.values[2], 60.0);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryFromTSMFiles) {
    co_await testQueryFromTSMFiles();
}

// ---------------------------------------------------------------------------
// Test: Result merging from TSM files and memory store
// ---------------------------------------------------------------------------
seastar::future<> testResultMergingTSMAndMemory() {
    WITH_ENGINE(engine, {
        // Insert first batch of data
        TimeStarInsert<double> insert1("network", "throughput");
        insert1.addValue(1000, 100.0);
        insert1.addValue(2000, 200.0);
        insert1.addValue(3000, 300.0);

        co_await engine.insert(std::move(insert1));

        // Rollover to flush first batch to TSM
        co_await engine.rolloverMemoryStore();

        // Insert second batch (stays in memory store)
        TimeStarInsert<double> insert2("network", "throughput");
        insert2.addValue(4000, 400.0);
        insert2.addValue(5000, 500.0);
        insert2.addValue(6000, 600.0);

        co_await engine.insert(std::move(insert2));

        // Query should merge TSM and memory store results, sorted by timestamp
        auto resultOpt = co_await engine.query("network throughput", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 6u);

            // Verify sorted order
            for (size_t i = 1; i < result.timestamps.size(); ++i) {
                EXPECT_LT(result.timestamps[i - 1], result.timestamps[i]);
            }

            EXPECT_DOUBLE_EQ(result.values[0], 100.0);
            EXPECT_DOUBLE_EQ(result.values[5], 600.0);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, ResultMergingTSMAndMemory) {
    co_await testResultMergingTSMAndMemory();
}

// ---------------------------------------------------------------------------
// Test: Multiple rollovers create multiple TSM files, results merge correctly
// ---------------------------------------------------------------------------
seastar::future<> testMultipleTSMFileMerging() {
    WITH_ENGINE(engine, {
        // Create 3 TSM files via rollovers
        for (int batch = 0; batch < 3; batch++) {
            TimeStarInsert<double> insert("metrics", "latency");
            for (int i = 0; i < 5; i++) {
                uint64_t ts = (batch * 5 + i + 1) * 1000;
                double val = static_cast<double>(batch * 5 + i + 1);
                insert.addValue(ts, val);
            }
            co_await engine.insert(std::move(insert));
            co_await engine.rolloverMemoryStore();
        }

        // Query all data
        auto resultOpt = co_await engine.query("metrics latency", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 15u);

            // Verify ascending timestamp order
            for (size_t i = 1; i < result.timestamps.size(); ++i) {
                EXPECT_LT(result.timestamps[i - 1], result.timestamps[i]);
            }

            EXPECT_DOUBLE_EQ(result.values[0], 1.0);
            EXPECT_DOUBLE_EQ(result.values[14], 15.0);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, MultipleTSMFileMerging) {
    co_await testMultipleTSMFileMerging();
}

// ---------------------------------------------------------------------------
// Test: Large result set (10,000 points)
// ---------------------------------------------------------------------------
seastar::future<> testLargeResultSet() {
    WITH_ENGINE(engine, {
        const int numPoints = 10000;

        TimeStarInsert<double> insert("sensor", "reading");
        for (int i = 0; i < numPoints; i++) {
            insert.addValue(static_cast<uint64_t>(i + 1) * 1000, static_cast<double>(i) * 0.1);
        }

        co_await engine.insert(std::move(insert));

        auto resultOpt = co_await engine.query("sensor reading", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), static_cast<size_t>(numPoints));
            EXPECT_EQ(result.values.size(), static_cast<size_t>(numPoints));

            // Verify first and last values
            EXPECT_DOUBLE_EQ(result.values[0], 0.0);
            EXPECT_DOUBLE_EQ(result.values[numPoints - 1], (numPoints - 1) * 0.1);

            // Verify timestamps are sorted
            for (size_t i = 1; i < result.timestamps.size(); ++i) {
                EXPECT_LE(result.timestamps[i - 1], result.timestamps[i]);
            }
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, LargeResultSet) {
    co_await testLargeResultSet();
}

// ---------------------------------------------------------------------------
// Test: Query empty time range returns no results
// ---------------------------------------------------------------------------
seastar::future<> testQueryEmptyTimeRange() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert("temperature", "celsius");
        insert.addValue(5000, 25.0);
        insert.addValue(6000, 26.0);
        insert.addValue(7000, 27.0);

        co_await engine.insert(std::move(insert));

        // Query a time range that has no data
        auto resultOpt = co_await engine.query("temperature celsius", 1000, 2000);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 0u);
            EXPECT_EQ(result.values.size(), 0u);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryEmptyTimeRange) {
    co_await testQueryEmptyTimeRange();
}

// ---------------------------------------------------------------------------
// Test: Type routing - different types for different series
// ---------------------------------------------------------------------------
seastar::future<> testTypeBasedRouting() {
    WITH_ENGINE(engine, {
        // Insert float series
        TimeStarInsert<double> floatInsert("metrics", "cpu_usage");
        floatInsert.addValue(1000, 75.5);
        floatInsert.addValue(2000, 80.2);
        co_await engine.insert(std::move(floatInsert));

        // Insert boolean series
        TimeStarInsert<bool> boolInsert("alerts", "active");
        boolInsert.addValue(1000, true);
        boolInsert.addValue(2000, false);
        co_await engine.insert(std::move(boolInsert));

        // Insert string series
        TimeStarInsert<std::string> stringInsert("events", "description");
        stringInsert.addValue(1000, "deploy started");
        stringInsert.addValue(2000, "deploy completed");
        co_await engine.insert(std::move(stringInsert));

        // Query each and verify correct type routing
        auto floatResult = co_await engine.query("metrics cpu_usage", 0, UINT64_MAX);
        EXPECT_TRUE(floatResult.has_value());
        EXPECT_TRUE(std::holds_alternative<QueryResult<double>>(floatResult.value()));
        auto& fr = std::get<QueryResult<double>>(floatResult.value());
        EXPECT_EQ(fr.values.size(), 2u);
        EXPECT_DOUBLE_EQ(fr.values[0], 75.5);

        auto boolResult = co_await engine.query("alerts active", 0, UINT64_MAX);
        EXPECT_TRUE(boolResult.has_value());
        EXPECT_TRUE(std::holds_alternative<QueryResult<bool>>(boolResult.value()));
        auto& br = std::get<QueryResult<bool>>(boolResult.value());
        EXPECT_EQ(br.values.size(), 2u);
        EXPECT_EQ(br.values[0], true);

        auto stringResult = co_await engine.query("events description", 0, UINT64_MAX);
        EXPECT_TRUE(stringResult.has_value());
        EXPECT_TRUE(std::holds_alternative<QueryResult<std::string>>(stringResult.value()));
        auto& sr = std::get<QueryResult<std::string>>(stringResult.value());
        EXPECT_EQ(sr.values.size(), 2u);
        EXPECT_EQ(sr.values[0], "deploy started");
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, TypeBasedRouting) {
    co_await testTypeBasedRouting();
}

// ---------------------------------------------------------------------------
// Test: Query with tags produces correct series key routing
// ---------------------------------------------------------------------------
seastar::future<> testQueryWithTags() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert1("weather", "temperature");
        insert1.addTag("city", "nyc");
        insert1.addValue(1000, 32.0);
        insert1.addValue(2000, 33.0);
        co_await engine.insert(std::move(insert1));

        TimeStarInsert<double> insert2("weather", "temperature");
        insert2.addTag("city", "sf");
        insert2.addValue(1000, 65.0);
        insert2.addValue(2000, 66.0);
        co_await engine.insert(std::move(insert2));

        // Query NYC series
        auto nycResult = co_await engine.query("weather,city=nyc temperature", 0, UINT64_MAX);
        EXPECT_TRUE(nycResult.has_value());
        if (nycResult.has_value()) {
            auto& result = std::get<QueryResult<double>>(nycResult.value());
            EXPECT_EQ(result.values.size(), 2u);
            EXPECT_DOUBLE_EQ(result.values[0], 32.0);
            EXPECT_DOUBLE_EQ(result.values[1], 33.0);
        }

        // Query SF series
        auto sfResult = co_await engine.query("weather,city=sf temperature", 0, UINT64_MAX);
        EXPECT_TRUE(sfResult.has_value());
        if (sfResult.has_value()) {
            auto& result = std::get<QueryResult<double>>(sfResult.value());
            EXPECT_EQ(result.values.size(), 2u);
            EXPECT_DOUBLE_EQ(result.values[0], 65.0);
            EXPECT_DOUBLE_EQ(result.values[1], 66.0);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryWithTags) {
    co_await testQueryWithTags();
}

// ---------------------------------------------------------------------------
// Test: Query after deleting data returns filtered results
// ---------------------------------------------------------------------------
seastar::future<> testQueryAfterDeletion() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert("metrics", "value");
        insert.addValue(1000, 10.0);
        insert.addValue(2000, 20.0);
        insert.addValue(3000, 30.0);
        insert.addValue(4000, 40.0);
        insert.addValue(5000, 50.0);

        co_await engine.insert(std::move(insert));

        // Delete middle range
        co_await engine.deleteRange("metrics value", 2000, 3000);

        auto resultOpt = co_await engine.query("metrics value", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            // Should have 3 points: 1000, 4000, 5000
            EXPECT_EQ(result.timestamps.size(), 3u);
            EXPECT_EQ(result.timestamps[0], 1000u);
            EXPECT_EQ(result.timestamps[1], 4000u);
            EXPECT_EQ(result.timestamps[2], 5000u);
            EXPECT_DOUBLE_EQ(result.values[0], 10.0);
            EXPECT_DOUBLE_EQ(result.values[1], 40.0);
            EXPECT_DOUBLE_EQ(result.values[2], 50.0);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryAfterDeletion) {
    co_await testQueryAfterDeletion();
}

// ---------------------------------------------------------------------------
// Test: Query TSM files with time range filtering
// ---------------------------------------------------------------------------
seastar::future<> testQueryTSMWithTimeRange() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert("io", "bytes");
        for (uint64_t ts = 1000; ts <= 20000; ts += 1000) {
            insert.addValue(ts, static_cast<double>(ts));
        }

        co_await engine.insert(std::move(insert));

        // Force to TSM
        co_await engine.rolloverMemoryStore();

        // Query a sub-range
        auto resultOpt = co_await engine.query("io bytes", 5000, 15000);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 11u);
            EXPECT_EQ(result.timestamps[0], 5000u);
            EXPECT_EQ(result.timestamps[10], 15000u);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryTSMWithTimeRange) {
    co_await testQueryTSMWithTimeRange();
}

// ---------------------------------------------------------------------------
// Test: Boolean data from TSM files
// ---------------------------------------------------------------------------
seastar::future<> testQueryBoolFromTSM() {
    WITH_ENGINE(engine, {
        TimeStarInsert<bool> insert("status", "healthy");
        insert.addValue(1000, true);
        insert.addValue(2000, true);
        insert.addValue(3000, false);
        insert.addValue(4000, true);

        co_await engine.insert(std::move(insert));
        co_await engine.rolloverMemoryStore();

        auto resultOpt = co_await engine.query("status healthy", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<bool>>(resultOpt.value());
            EXPECT_EQ(result.values.size(), 4u);
            EXPECT_EQ(result.values[0], true);
            EXPECT_EQ(result.values[1], true);
            EXPECT_EQ(result.values[2], false);
            EXPECT_EQ(result.values[3], true);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryBoolFromTSM) {
    co_await testQueryBoolFromTSM();
}

// ---------------------------------------------------------------------------
// Test: String data from TSM files
// ---------------------------------------------------------------------------
seastar::future<> testQueryStringFromTSM() {
    WITH_ENGINE(engine, {
        TimeStarInsert<std::string> insert("audit", "action");
        insert.addValue(1000, "login");
        insert.addValue(2000, "view_dashboard");
        insert.addValue(3000, "logout");

        co_await engine.insert(std::move(insert));
        co_await engine.rolloverMemoryStore();

        auto resultOpt = co_await engine.query("audit action", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<std::string>>(resultOpt.value());
            EXPECT_EQ(result.values.size(), 3u);
            EXPECT_EQ(result.values[0], "login");
            EXPECT_EQ(result.values[1], "view_dashboard");
            EXPECT_EQ(result.values[2], "logout");
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryStringFromTSM) {
    co_await testQueryStringFromTSM();
}

// ---------------------------------------------------------------------------
// Test: Large result set spanning TSM files and memory store
// ---------------------------------------------------------------------------
seastar::future<> testLargeResultSetAcrossTSMAndMemory() {
    WITH_ENGINE(engine, {
        const int pointsPerBatch = 1000;
        const int numBatches = 3;

        // Insert data in batches, rolling over between each
        for (int batch = 0; batch < numBatches; batch++) {
            TimeStarInsert<double> insert("large_series", "data");
            for (int i = 0; i < pointsPerBatch; i++) {
                uint64_t ts = static_cast<uint64_t>(batch * pointsPerBatch + i + 1) * 1000;
                double val = static_cast<double>(batch * pointsPerBatch + i);
                insert.addValue(ts, val);
            }
            co_await engine.insert(std::move(insert));

            // Roll over all but the last batch (last stays in memory)
            if (batch < numBatches - 1) {
                co_await engine.rolloverMemoryStore();
            }
        }

        // Query all data
        auto resultOpt = co_await engine.query("large_series data", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), static_cast<size_t>(pointsPerBatch * numBatches));

            // Verify sorted order
            for (size_t i = 1; i < result.timestamps.size(); ++i) {
                EXPECT_LT(result.timestamps[i - 1], result.timestamps[i]);
            }

            // Verify boundary values
            EXPECT_DOUBLE_EQ(result.values[0], 0.0);
            EXPECT_DOUBLE_EQ(result.values[pointsPerBatch * numBatches - 1],
                             static_cast<double>(pointsPerBatch * numBatches - 1));
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, LargeResultSetAcrossTSMAndMemory) {
    co_await testLargeResultSetAcrossTSMAndMemory();
}

// ---------------------------------------------------------------------------
// Test: Query single point
// ---------------------------------------------------------------------------
seastar::future<> testQuerySinglePoint() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert("test", "one_point");
        insert.addValue(42000, 99.9);

        co_await engine.insert(std::move(insert));

        auto resultOpt = co_await engine.query("test one_point", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 1u);
            EXPECT_EQ(result.timestamps[0], 42000u);
            EXPECT_DOUBLE_EQ(result.values[0], 99.9);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QuerySinglePoint) {
    co_await testQuerySinglePoint();
}

// ---------------------------------------------------------------------------
// Test: Query boundary timestamps (exact start and end match)
// ---------------------------------------------------------------------------
seastar::future<> testQueryBoundaryTimestamps() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert("boundary", "test");
        insert.addValue(1000, 1.0);
        insert.addValue(2000, 2.0);
        insert.addValue(3000, 3.0);
        insert.addValue(4000, 4.0);
        insert.addValue(5000, 5.0);

        co_await engine.insert(std::move(insert));

        // Query with exact boundaries
        auto resultOpt = co_await engine.query("boundary test", 2000, 4000);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 3u);
            EXPECT_EQ(result.timestamps[0], 2000u);
            EXPECT_EQ(result.timestamps[2], 4000u);
            EXPECT_DOUBLE_EQ(result.values[0], 2.0);
            EXPECT_DOUBLE_EQ(result.values[2], 4.0);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryBoundaryTimestamps) {
    co_await testQueryBoundaryTimestamps();
}

// ---------------------------------------------------------------------------
// Test: Deletion from TSM then query confirms tombstone filtering
// ---------------------------------------------------------------------------
seastar::future<> testQueryTSMAfterDeletion() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert("tsm_delete", "val");
        insert.addValue(1000, 10.0);
        insert.addValue(2000, 20.0);
        insert.addValue(3000, 30.0);
        insert.addValue(4000, 40.0);
        insert.addValue(5000, 50.0);

        co_await engine.insert(std::move(insert));
        co_await engine.rolloverMemoryStore();

        // Delete from TSM via tombstones
        co_await engine.deleteRange("tsm_delete val", 2000, 4000);

        auto resultOpt = co_await engine.query("tsm_delete val", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 2u);
            EXPECT_EQ(result.timestamps[0], 1000u);
            EXPECT_EQ(result.timestamps[1], 5000u);
            EXPECT_DOUBLE_EQ(result.values[0], 10.0);
            EXPECT_DOUBLE_EQ(result.values[1], 50.0);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryTSMAfterDeletion) {
    co_await testQueryTSMAfterDeletion();
}

// ---------------------------------------------------------------------------
// Test: Two memory stores with no TSM data (rollover without TSM conversion)
//
// Regression test for C1 critical bug: after a memory store rollover but
// before the background TSM conversion completes, two memory stores exist.
// A query at this moment must return data from BOTH stores, not just the
// newer one. This scenario is the normal steady-state during heavy writes:
//   1. Insert data -> goes to memory store 0
//   2. Rollover -> old data moves to memory store 1, new empty store at 0
//   3. Insert more data -> goes to new memory store 0
//   4. Query -> must merge data from BOTH memory stores (0 and 1)
// ---------------------------------------------------------------------------
seastar::future<> testQueryWithTwoMemoryStoresNoTSM() {
    WITH_ENGINE(engine, {
        // Phase 1: Insert data into the first memory store (timestamps 1000-3000)
        TimeStarInsert<double> insert1("pressure", "value");
        insert1.addTag("sensor", "main");
        insert1.addValue(1000, 101.0);
        insert1.addValue(2000, 102.0);
        insert1.addValue(3000, 103.0);

        co_await engine.insert(std::move(insert1));

        // Phase 2: Trigger rollover. This creates a new memory store at index 0
        // and pushes the old store (with timestamps 1000-3000) to index 1.
        // The background TSM conversion is launched asynchronously but has NOT
        // completed yet — Seastar is cooperative, so no conversion work runs
        // until we yield to the reactor (which we won't do before querying).
        co_await engine.rolloverMemoryStore();

        // Phase 3: Insert more data into the NEW memory store (timestamps 4000-6000)
        TimeStarInsert<double> insert2("pressure", "value");
        insert2.addTag("sensor", "main");
        insert2.addValue(4000, 104.0);
        insert2.addValue(5000, 105.0);
        insert2.addValue(6000, 106.0);

        co_await engine.insert(std::move(insert2));

        // Verify: no TSM files should exist yet. The background conversion has
        // not had a chance to run because we haven't yielded to the reactor
        // between the rollover and this point (inserts are inline).
        EXPECT_EQ(engine.getTSMFileCount(), 0u)
            << "Expected zero TSM files — background conversion should not have completed yet";

        // Phase 4: Query the full time range. The query runner must discover and
        // merge data from BOTH memory stores (the active one at index 0 with
        // timestamps 4000-6000, and the read-only one at index 1 with 1000-3000).
        auto resultOpt = co_await engine.query("pressure,sensor=main value", 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value()) << "Query should return data from memory stores";

        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());

            // Must have ALL 6 points from both memory stores
            EXPECT_EQ(result.timestamps.size(), 6u)
                << "Expected 6 points from two memory stores, got " << result.timestamps.size();
            EXPECT_EQ(result.values.size(), 6u);

            // Verify sorted timestamp order (merged from both stores)
            for (size_t i = 1; i < result.timestamps.size(); ++i) {
                EXPECT_LT(result.timestamps[i - 1], result.timestamps[i])
                    << "Timestamps should be in ascending order at index " << i;
            }

            // Verify all timestamps are present
            EXPECT_EQ(result.timestamps[0], 1000u);
            EXPECT_EQ(result.timestamps[1], 2000u);
            EXPECT_EQ(result.timestamps[2], 3000u);
            EXPECT_EQ(result.timestamps[3], 4000u);
            EXPECT_EQ(result.timestamps[4], 5000u);
            EXPECT_EQ(result.timestamps[5], 6000u);

            // Verify all values are present and correct
            EXPECT_DOUBLE_EQ(result.values[0], 101.0);
            EXPECT_DOUBLE_EQ(result.values[1], 102.0);
            EXPECT_DOUBLE_EQ(result.values[2], 103.0);
            EXPECT_DOUBLE_EQ(result.values[3], 104.0);
            EXPECT_DOUBLE_EQ(result.values[4], 105.0);
            EXPECT_DOUBLE_EQ(result.values[5], 106.0);
        }

        // Also verify a sub-range query spanning both stores works correctly
        auto subResult = co_await engine.query("pressure,sensor=main value", 2000, 5000);
        EXPECT_TRUE(subResult.has_value());

        if (subResult.has_value()) {
            auto& result = std::get<QueryResult<double>>(subResult.value());
            EXPECT_EQ(result.timestamps.size(), 4u)
                << "Sub-range query should return 4 points spanning both stores";
            EXPECT_EQ(result.timestamps[0], 2000u);
            EXPECT_EQ(result.timestamps[1], 3000u);
            EXPECT_EQ(result.timestamps[2], 4000u);
            EXPECT_EQ(result.timestamps[3], 5000u);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryWithTwoMemoryStoresNoTSM) {
    co_await testQueryWithTwoMemoryStoresNoTSM();
}

// ---------------------------------------------------------------------------
// Test: Multiple independent series do not interfere with each other
// ---------------------------------------------------------------------------
seastar::future<> testMultipleIndependentSeries() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert1("sys", "cpu");
        insert1.addValue(1000, 10.0);
        insert1.addValue(2000, 20.0);
        co_await engine.insert(std::move(insert1));

        TimeStarInsert<double> insert2("sys", "mem");
        insert2.addValue(1000, 512.0);
        insert2.addValue(2000, 1024.0);
        co_await engine.insert(std::move(insert2));

        TimeStarInsert<double> insert3("sys", "disk");
        insert3.addValue(1000, 80.0);
        insert3.addValue(2000, 85.0);
        co_await engine.insert(std::move(insert3));

        // Verify each series returns only its own data
        auto cpuResult = co_await engine.query("sys cpu", 0, UINT64_MAX);
        EXPECT_TRUE(cpuResult.has_value());
        auto& cpu = std::get<QueryResult<double>>(cpuResult.value());
        EXPECT_EQ(cpu.values.size(), 2u);
        EXPECT_DOUBLE_EQ(cpu.values[0], 10.0);

        auto memResult = co_await engine.query("sys mem", 0, UINT64_MAX);
        EXPECT_TRUE(memResult.has_value());
        auto& mem = std::get<QueryResult<double>>(memResult.value());
        EXPECT_EQ(mem.values.size(), 2u);
        EXPECT_DOUBLE_EQ(mem.values[0], 512.0);

        auto diskResult = co_await engine.query("sys disk", 0, UINT64_MAX);
        EXPECT_TRUE(diskResult.has_value());
        auto& disk = std::get<QueryResult<double>>(diskResult.value());
        EXPECT_EQ(disk.values.size(), 2u);
        EXPECT_DOUBLE_EQ(disk.values[0], 80.0);
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, MultipleIndependentSeries) {
    co_await testMultipleIndependentSeries();
}

// ---------------------------------------------------------------------------
// Test: Query after all data is deleted returns empty or nullopt
// ---------------------------------------------------------------------------
seastar::future<> testQueryAfterFullDeletion() {
    WITH_ENGINE(engine, {
        TimeStarInsert<double> insert("ephemeral", "data");
        insert.addValue(1000, 1.0);
        insert.addValue(2000, 2.0);
        insert.addValue(3000, 3.0);

        co_await engine.insert(std::move(insert));

        // Delete all data
        co_await engine.deleteRange("ephemeral data", 0, UINT64_MAX);

        auto resultOpt = co_await engine.query("ephemeral data", 0, UINT64_MAX);
        // After full deletion, result may be empty or nullopt
        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 0u);
        }
    });
    co_return;
}

SEASTAR_TEST_F(QueryRunnerSeastarTest, QueryAfterFullDeletion) {
    co_await testQueryAfterFullDeletion();
}
