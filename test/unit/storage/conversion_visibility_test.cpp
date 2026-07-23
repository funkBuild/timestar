// Regression tests: WAL->TSM conversion visibility gap.
//
// BUG (intermittent, observed as empty query results for tens-to-hundreds of
// ms right after a rollover): the query paths snapshotted the TSM file list
// FIRST, suspended on TSM I/O, and only then read the LIVE memory-store list.
// A background conversion completing inside that window (registering its TSM
// file after the snapshot, erasing the retiring store before the memory read)
// made the series visible in NEITHER source.  The aggregated pushdown path
// had the same skew through its early memory-min-timestamp probe: the TSM
// scan range was clipped below the probed timestamp, and the late memory fold
// found the store already gone — silently dropping the tail of the range.
//
// INVARIANT UNDER TEST: at every instant during (and after) a rollover +
// background conversion, a query over the full time range returns EVERY
// point written so far — from the retiring memory store, the registered TSM
// file, or a deduplicated merge of both.  Enforced by pinning the memory
// stores BEFORE snapshotting the TSM file list (queryTsm, queryTsmAggregated,
// batchLatest) — see WALFileManager::pinMemoryStores().
//
// The tests below drive real rollovers and query in a tight cooperative loop
// THROUGHOUT the conversion window (deterministic version of the flake: every
// reactor interleaving the conversion can produce is crossed by hundreds of
// queries, so a reintroduced ordering bug fails within a few rounds).

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/query/block_aggregator.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/later.hh>
#include <string>
#include <vector>

class ConversionVisibilityTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

namespace {

constexpr uint64_t kStep = 1000000000ULL;  // 1s in ns
constexpr int kPointsPerRound = 100;

seastar::future<> insertFloatPoints(Engine& engine, const std::string& measurement, uint64_t startTs, int count) {
    TimeStarInsert<double> insert(measurement, "value");
    insert.addTag("src", "cv");
    for (int i = 0; i < count; ++i) {
        uint64_t ts = startTs + static_cast<uint64_t>(i) * kStep;
        insert.addValue(ts, static_cast<double>(ts));
    }
    co_return co_await engine.insert(std::move(insert));
}

seastar::future<> insertStringPoints(Engine& engine, const std::string& measurement, uint64_t startTs, int count) {
    TimeStarInsert<std::string> insert(measurement, "note");
    insert.addTag("src", "cv");
    for (int i = 0; i < count; ++i) {
        uint64_t ts = startTs + static_cast<uint64_t>(i) * kStep;
        insert.addValue(ts, "s" + std::to_string(ts));
    }
    co_return co_await engine.insert(std::move(insert));
}

// Count points visible through the RAW query path (queryTsm merge).
template <class T>
seastar::future<size_t> visibleRawCount(Engine& engine, const std::string& seriesKey) {
    auto resultOpt = co_await engine.query(seriesKey, 0, UINT64_MAX);
    if (!resultOpt.has_value()) {
        co_return 0;
    }
    co_return std::get<QueryResult<T>>(resultOpt.value()).timestamps.size();
}

// Count points visible through the AGGREGATED pushdown path (COUNT with a
// bucketing interval — the streaming/interval query plan used by the HTTP
// layer).  nullopt means the pushdown declined (e.g. overlap with an additive
// method); the caller falls back to the raw count, mirroring production.
seastar::future<size_t> visibleAggregatedCount(Engine& engine, const std::string& seriesKey,
                                               const SeriesId128& seriesId) {
    auto pr = co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX - 1, /*interval=*/3600 * kStep,
                                              timestar::AggregationMethod::COUNT, /*foldNoInterval=*/false);
    if (!pr.has_value()) {
        co_return co_await visibleRawCount<double>(engine, seriesKey);
    }
    size_t total = 0;
    for (const auto& [bucketTs, state] : pr->bucketStates) {
        total += static_cast<size_t>(state.count);
    }
    co_return total;
}

}  // namespace

// ---------------------------------------------------------------------------
// Float series: raw path + aggregated pushdown path + batchLatest must never
// return an incomplete result during rollover/conversion.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(ConversionVisibilityTest, FloatSeriesNeverInvisibleDuringConversion) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const std::string measurement = "cv_float";
        TimeStarInsert<double> keyBuilder(measurement, "value");
        keyBuilder.addTag("src", "cv");
        const std::string seriesKey = keyBuilder.seriesKey();
        const SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

        size_t expected = 0;
        uint64_t latestTs = 0;

        for (int round = 0; round < 3; ++round) {
            const uint64_t base = kStep + static_cast<uint64_t>(round) * kPointsPerRound * kStep;
            co_await insertFloatPoints(engine, measurement, base, kPointsPerRound);
            expected += kPointsPerRound;
            latestTs = base + static_cast<uint64_t>(kPointsPerRound - 1) * kStep;

            const size_t filesBefore = engine.getTSMFileCount();
            co_await engine.rolloverMemoryStore();  // launches background conversion

            // Query in a tight cooperative loop THROUGHOUT the conversion
            // window.  Every iteration yields, so the background conversion
            // interleaves with the queries at every suspension point it has.
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
            size_t iterations = 0;
            while (engine.getTSMFileCount() == filesBefore) {
                size_t raw = co_await visibleRawCount<double>(engine, seriesKey);
                EXPECT_EQ(raw, expected) << "raw query lost points mid-conversion (round " << round << ", iteration "
                                         << iterations << ")";

                size_t agg = co_await visibleAggregatedCount(engine, seriesKey, seriesId);
                EXPECT_EQ(agg, expected) << "aggregated query lost points mid-conversion (round " << round
                                         << ", iteration " << iterations << ")";

                std::vector<Engine::BatchLatestEntry> entries(1);
                entries[0].seriesId = seriesId;
                co_await engine.batchLatest(entries, 0, UINT64_MAX, /*wantFirst=*/false);
                EXPECT_TRUE(entries[0].resolved)
                    << "batchLatest lost the series mid-conversion (round " << round << ")";
                EXPECT_EQ(entries[0].timestamp, latestTs)
                    << "batchLatest returned a stale point mid-conversion (round " << round << ")";
                if (raw != expected || agg != expected || !entries[0].resolved || entries[0].timestamp != latestTs) {
                    throw std::runtime_error("visibility invariant violated mid-conversion");
                }

                ++iterations;
                if (std::chrono::steady_clock::now() > deadline) {
                    break;
                }
                co_await seastar::yield();
            }
            EXPECT_GT(engine.getTSMFileCount(), filesBefore)
                << "background TSM conversion did not complete within 30s (round " << round << ")";
            if (engine.getTSMFileCount() <= filesBefore) {
                throw std::runtime_error("conversion did not complete");
            }

            // A few post-conversion checks (TSM-only reads).
            for (int i = 0; i < 5; ++i) {
                size_t raw = co_await visibleRawCount<double>(engine, seriesKey);
                EXPECT_EQ(raw, expected) << "raw query lost points after conversion (round " << round << ")";
                if (raw != expected) {
                    throw std::runtime_error("visibility invariant violated after conversion");
                }
                co_await seastar::yield();
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
// String series: the raw string path (the only query plan for strings) must
// never return an empty or partial result during rollover/conversion.
// Guards the reported ">5s string invisibility after conversion" variant.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(ConversionVisibilityTest, StringSeriesNeverInvisibleDuringConversion) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const std::string measurement = "cv_string";
        TimeStarInsert<std::string> keyBuilder(measurement, "note");
        keyBuilder.addTag("src", "cv");
        const std::string seriesKey = keyBuilder.seriesKey();

        size_t expected = 0;

        for (int round = 0; round < 3; ++round) {
            const uint64_t base = kStep + static_cast<uint64_t>(round) * kPointsPerRound * kStep;
            co_await insertStringPoints(engine, measurement, base, kPointsPerRound);
            expected += kPointsPerRound;

            const size_t filesBefore = engine.getTSMFileCount();
            co_await engine.rolloverMemoryStore();

            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
            size_t iterations = 0;
            while (engine.getTSMFileCount() == filesBefore) {
                size_t raw = co_await visibleRawCount<std::string>(engine, seriesKey);
                EXPECT_EQ(raw, expected) << "string query lost points mid-conversion (round " << round
                                         << ", iteration " << iterations << ")";
                if (raw != expected) {
                    throw std::runtime_error("string visibility invariant violated mid-conversion");
                }
                ++iterations;
                if (std::chrono::steady_clock::now() > deadline) {
                    break;
                }
                co_await seastar::yield();
            }
            EXPECT_GT(engine.getTSMFileCount(), filesBefore)
                << "background TSM conversion did not complete within 30s (round " << round << ")";
            if (engine.getTSMFileCount() <= filesBefore) {
                throw std::runtime_error("conversion did not complete");
            }

            for (int i = 0; i < 5; ++i) {
                size_t raw = co_await visibleRawCount<std::string>(engine, seriesKey);
                EXPECT_EQ(raw, expected) << "string query lost points after conversion (round " << round << ")";
                if (raw != expected) {
                    throw std::runtime_error("string visibility invariant violated after conversion");
                }
                co_await seastar::yield();
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
