// Behavioral tests: query correctness under concurrent inserts and rollovers.
//
// Companion to query_runner_race_test.cpp (kept as a source-inspection test):
// the guarded bug there — parallel_for_each coroutines pushing to a shared
// mutable vector in QueryRunner::queryTsm, where a reallocation can invalidate
// references held by suspended coroutines — is undefined behavior and cannot
// be reproduced deterministically from the outside. These tests instead pin
// down the OBSERVABLE contract that any such race would corrupt:
//
//   - A query spanning multiple TSM files + memory stores returns the exact
//     point set, sorted, with no drops and no duplicates — even while other
//     coroutines are inserting and rolling over concurrently.
//   - Repeated concurrent queries of an immutable series are bit-for-bit
//     stable; queries of a series being appended to return a consistent,
//     sorted, duplicate-free prefix that only ever grows.
//
// Interleaving is driven purely by cooperative scheduling (when_all + yields
// and real I/O suspension points) — no sleeps.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/later.hh>
#include <string>
#include <vector>

class QueryRunnerConcurrencyTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

namespace {

constexpr uint64_t kStableStep = 1000;

// Series key helpers: all series use tag src=qc.
std::string seriesKeyFor(const std::string& measurement) { return measurement + ",src=qc value"; }

seastar::future<> insertPoints(Engine& engine, const std::string& measurement, uint64_t startTs, int count,
                               uint64_t step) {
    TimeStarInsert<double> insert(measurement, "value");
    insert.addTag("src", "qc");
    for (int i = 0; i < count; ++i) {
        uint64_t ts = startTs + static_cast<uint64_t>(i) * step;
        insert.addValue(ts, static_cast<double>(ts));
    }
    co_return co_await engine.insert(std::move(insert));
}

// Wait (cooperatively, no sleeps) for background WAL->TSM conversions to
// produce at least `want` TSM files. Bounded by wall-clock so a slow disk
// degrades to querying memory stores rather than hanging the suite.
seastar::future<bool> waitForTsmFiles(Engine& engine, size_t want) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (engine.getTSMFileCount() < want) {
        if (std::chrono::steady_clock::now() > deadline) {
            co_return false;
        }
        co_await seastar::yield();
    }
    co_return true;
}

// Verify a query result for a series whose points are ts = start + i*step,
// value == ts: sorted, duplicate-free, exact count, exact values.
void checkExactSeries(const QueryResult<double>& result, uint64_t startTs, size_t count, uint64_t step,
                      const char* what) {
    EXPECT_EQ(result.timestamps.size(), count) << what << ": dropped or duplicated points";
    EXPECT_EQ(result.values.size(), result.timestamps.size()) << what << ": timestamps/values desynced";
    for (size_t i = 1; i < result.timestamps.size(); ++i) {
        EXPECT_LT(result.timestamps[i - 1], result.timestamps[i]) << what << ": unsorted or duplicate timestamps";
    }
    size_t n = std::min(result.timestamps.size(), count);
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(result.timestamps[i], startTs + i * step) << what << ": wrong timestamp at " << i;
        EXPECT_DOUBLE_EQ(result.values[i], static_cast<double>(result.timestamps[i]))
            << what << ": value does not match its timestamp at " << i;
    }
}

}  // namespace

// ---------------------------------------------------------------------------
// 1. Concurrent queries over multiple TSM files + memory data return the
//    exact same complete result every time, while inserts/rollovers of OTHER
//    series churn the engine.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(QueryRunnerConcurrencyTest, ConcurrentQueriesOverMultipleTsmFilesAreExact) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        // Build the immutable series across 3 TSM files (3 rollovers) plus a
        // 4th batch left in the memory store: 80 points total, ts = i*1000.
        for (int batch = 0; batch < 3; ++batch) {
            co_await insertPoints(engine, "qc_stable", static_cast<uint64_t>(batch) * 20 * kStableStep + kStableStep,
                                  20, kStableStep);
            co_await engine.rolloverMemoryStore();
        }
        co_await insertPoints(engine, "qc_stable", 61 * kStableStep, 20, kStableStep);

        // Let background conversions land so queryTsm's parallel_for_each
        // actually fans out over multiple files.
        bool converted = co_await waitForTsmFiles(engine, 3);
        EXPECT_TRUE(converted) << "Background TSM conversion did not finish in time; "
                                  "test still validates memory-store merge correctness";

        // Concurrent workload: 3 query workers on the stable series + 2
        // insert workers churning a different series + 1 rollover stream.
        auto queryWorker = [&engine]() -> seastar::future<> {
            for (int iter = 0; iter < 5; ++iter) {
                auto resultOpt = co_await engine.query(seriesKeyFor("qc_stable"), 0, UINT64_MAX);
                EXPECT_TRUE(resultOpt.has_value());
                if (resultOpt.has_value()) {
                    auto& result = std::get<QueryResult<double>>(resultOpt.value());
                    checkExactSeries(result, kStableStep, 80, kStableStep, "qc_stable under concurrency");
                }
                co_await seastar::yield();
            }
        };
        auto insertWorker = [&engine](int worker) -> seastar::future<> {
            for (int i = 0; i < 15; ++i) {
                co_await insertPoints(engine, "qc_churn" + std::to_string(worker),
                                      static_cast<uint64_t>(i + 1) * 100, 1, 1);
                co_await seastar::yield();
            }
        };
        auto rolloverWorker = [&engine]() -> seastar::future<> {
            for (int i = 0; i < 2; ++i) {
                co_await engine.rolloverMemoryStore();
                co_await seastar::yield();
            }
        };

        co_await seastar::when_all_succeed(queryWorker(), queryWorker(), queryWorker(), insertWorker(0),
                                           insertWorker(1), rolloverWorker());

        // The churned series must also be complete afterwards.
        for (int worker = 0; worker < 2; ++worker) {
            auto resultOpt = co_await engine.query(seriesKeyFor("qc_churn" + std::to_string(worker)), 0, UINT64_MAX);
            EXPECT_TRUE(resultOpt.has_value());
            if (resultOpt.has_value()) {
                auto& result = std::get<QueryResult<double>>(resultOpt.value());
                checkExactSeries(result, 100, 15, 100, "qc_churn after concurrency");
            }
        }
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ---------------------------------------------------------------------------
// 2. Queries racing inserts INTO THE SAME series always observe a consistent
//    prefix: sorted, duplicate-free, values matching their timestamps, and a
//    point count that never shrinks between successive queries.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(QueryRunnerConcurrencyTest, QueriesRacingSameSeriesInsertsSeeConsistentPrefix) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        // Seed then roll over, so queries merge TSM + memory while racing.
        co_await insertPoints(engine, "qc_live", 1000, 10, 1000);
        co_await engine.rolloverMemoryStore();

        auto writer = [&engine]() -> seastar::future<> {
            for (int i = 0; i < 40; ++i) {
                co_await insertPoints(engine, "qc_live", static_cast<uint64_t>(11 + i) * 1000, 1, 1000);
                co_await seastar::yield();
            }
        };

        auto reader = [&engine]() -> seastar::future<> {
            size_t lastCount = 0;
            for (int iter = 0; iter < 20; ++iter) {
                auto resultOpt = co_await engine.query(seriesKeyFor("qc_live"), 0, UINT64_MAX);
                EXPECT_TRUE(resultOpt.has_value());
                if (resultOpt.has_value()) {
                    auto& result = std::get<QueryResult<double>>(resultOpt.value());
                    // Consistent prefix: points are ts = 1000, 2000, ... with
                    // value == ts, sorted and duplicate-free.
                    EXPECT_GE(result.timestamps.size(), 10u) << "Seed (TSM) data missing from racing query";
                    EXPECT_LE(result.timestamps.size(), 50u);
                    EXPECT_EQ(result.values.size(), result.timestamps.size());
                    checkExactSeries(result, 1000, result.timestamps.size(), 1000, "qc_live racing query");
                    // Monotonic visibility: a later query never sees fewer points.
                    EXPECT_GE(result.timestamps.size(), lastCount) << "Query result shrank between iterations";
                    lastCount = result.timestamps.size();
                }
                co_await seastar::yield();
            }
        };

        co_await seastar::when_all_succeed(writer(), reader(), reader());

        // Final state: all 50 points, exact.
        auto resultOpt = co_await engine.query(seriesKeyFor("qc_live"), 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());
        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            checkExactSeries(result, 1000, 50, 1000, "qc_live final");
        }
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}
