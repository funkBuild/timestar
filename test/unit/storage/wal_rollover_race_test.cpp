// Behavioral tests for the WAL rollover / concurrent-insert race.
//
// Bug history: WALFileManager::rolloverMemoryStore() suspends at several
// co_await points (WAL file creation, store close). If the sequence ever
// closes or removes the current store before a fresh, initialized store is
// installed at memoryStores[0], any insert coroutine dispatched by the
// reactor during a suspension hits a closed store: MemoryStore::insert()
// throws "MemoryStore is closed" and the point is lost.
//
// The invariant: memoryStores[0] always points to an OPEN store across every
// suspension point of the rollover, so concurrent inserts never fail and
// never lose data.
//
// These tests replace the former source-inspection variant (which asserted
// textual ordering of make_shared/initWAL/insert/close inside
// rolloverMemoryStore — and had gone stale: it matched the empty-store
// cleanup close, not the conversion path). Instead we drive a real Engine
// with interleaved coroutines: insert streams that yield between points run
// concurrently (when_all) with rollover calls. If the rollover ordering
// regressed, the inserts throw and/or the final read-back misses points.
//
// Determinism: interleaving relies only on Seastar's cooperative scheduler
// (yield + real WAL I/O suspension points) — no sleeps, no timing dependence.
// Correctness is verified by exact read-back of every inserted timestamp.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/later.hh>
#include <string>
#include <utility>
#include <vector>

class WALRolloverRaceTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

namespace {

// Insert `count` single-point writes for the given series, yielding to the
// reactor between points so rollover coroutines can interleave.
seastar::future<> insertStream(Engine& engine, std::string measurement, uint64_t baseTs, int count) {
    for (int i = 0; i < count; ++i) {
        TimeStarInsert<double> insert(measurement, "value");
        insert.addTag("src", "race");
        uint64_t ts = baseTs + static_cast<uint64_t>(i);
        insert.addValue(ts, static_cast<double>(ts));
        co_await engine.insert(std::move(insert));
        co_await seastar::yield();
    }
}

// Trigger `count` rollovers, yielding between them.
seastar::future<> rolloverStream(Engine& engine, int count) {
    for (int i = 0; i < count; ++i) {
        co_await engine.rolloverMemoryStore();
        co_await seastar::yield();
    }
}

// Read back the series and verify every expected timestamp is present exactly
// once with the expected value (value == timestamp). Coroutine context: uses
// EXPECT (never ASSERT) with explicit guards.
seastar::future<> verifyCompleteReadback(Engine& engine, std::string measurement,
                                         std::vector<std::pair<uint64_t, int>> ranges) {
    auto resultOpt = co_await engine.query(measurement + ",src=race value", 0, UINT64_MAX);
    EXPECT_TRUE(resultOpt.has_value()) << "Series vanished after rollover";
    if (!resultOpt.has_value()) {
        co_return;
    }
    auto& result = std::get<QueryResult<double>>(resultOpt.value());

    size_t expectedCount = 0;
    for (const auto& [base, count] : ranges) {
        expectedCount += static_cast<size_t>(count);
    }

    // Sorted strictly ascending => no duplicates, no reordering.
    for (size_t i = 1; i < result.timestamps.size(); ++i) {
        EXPECT_LT(result.timestamps[i - 1], result.timestamps[i]) << "Result must be sorted and duplicate-free";
    }

    EXPECT_EQ(result.timestamps.size(), expectedCount)
        << "Points were lost (or duplicated) by inserts racing a rollover";
    EXPECT_EQ(result.values.size(), expectedCount);
    if (result.timestamps.size() != expectedCount || result.values.size() != expectedCount) {
        co_return;
    }

    // Exact content check: every inserted timestamp present, value matches.
    size_t idx = 0;
    std::vector<uint64_t> expected;
    expected.reserve(expectedCount);
    for (const auto& [base, count] : ranges) {
        for (int i = 0; i < count; ++i) {
            expected.push_back(base + static_cast<uint64_t>(i));
        }
    }
    std::sort(expected.begin(), expected.end());
    for (uint64_t ts : expected) {
        EXPECT_EQ(result.timestamps[idx], ts) << "Missing or wrong timestamp at index " << idx;
        EXPECT_DOUBLE_EQ(result.values[idx], static_cast<double>(ts));
        ++idx;
    }
}

}  // namespace

// ---------------------------------------------------------------------------
// 1. Inserts racing a rollover: every point must land, none may throw.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(WALRolloverRaceTest, InsertsDuringRolloverNeverHitClosedStore) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        // Seed data so the store is non-empty when the rollover starts
        // (an empty store makes rolloverMemoryStore a no-op).
        co_await insertStream(engine, "roll_race", 0, 10);

        // Two insert streams interleaved with a stream of rollovers. Each
        // rollover suspends on WAL file I/O; the inserts that run inside
        // those suspension windows must always find an open store at
        // memoryStores[0].
        co_await seastar::when_all_succeed(insertStream(engine, "roll_race", 1000, 30),
                                           insertStream(engine, "roll_race", 2000, 30), rolloverStream(engine, 3));

        co_await verifyCompleteReadback(engine, "roll_race", {{0, 10}, {1000, 30}, {2000, 30}});
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ---------------------------------------------------------------------------
// 2. Multiple rollover calls racing each other AND concurrent inserts.
//    The rollover semaphore + isEmpty() re-check must serialize them; the
//    inserts must never observe a closed or missing store.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(WALRolloverRaceTest, ConcurrentRolloversWithInsertsAreSafe) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        co_await insertStream(engine, "roll_multi", 0, 5);

        // Fire several rollovers at once (unserialised callers) while an
        // insert stream keeps writing.
        co_await seastar::when_all_succeed(rolloverStream(engine, 1), rolloverStream(engine, 1),
                                           rolloverStream(engine, 1), insertStream(engine, "roll_multi", 3000, 20));

        co_await verifyCompleteReadback(engine, "roll_multi", {{0, 5}, {3000, 20}});
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}
