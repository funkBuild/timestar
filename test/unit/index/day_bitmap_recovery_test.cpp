/*
 * Startup recovery of day-bitmap membership — the Engine wiring.
 *
 * The unit tests in time_scoped_postings_test.cpp pin what
 * NativeIndex::rebuildDayBitmapsFromBounds() must do. This one pins that
 * Engine::init() actually CALLS it, with bounds taken from the TSM sparse
 * index, because that placement is the fragile part: day bitmaps are the one
 * index structure that cannot be rebuilt from index state alone (membership is
 * a function of insert timestamps, which the index never persists per series),
 * so the repair cannot live in NativeIndex::open() — TSM files are not loaded
 * until after it returns.
 *
 * Production shape being reproduced (1.4.2, 2026-09-01): a query over
 * [T, now] returned zero series while [T-2d, now] returned all of them,
 * because an unclean exit lost every day bitmap recorded since the last
 * memtable flush.
 */

#include "../../../lib/core/engine.hpp"
#include "../../../lib/index/key_encoding.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"
#include "../../test_helpers/native_index_test_access.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <seastar/core/coroutine.hh>
#include <string>
#include <vector>

namespace ke = timestar::index::keys;
using timestar::index::NativeIndexTestAccess;

class DayBitmapRecoveryTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

namespace {

constexpr uint32_t kFirstDay = 21500;
constexpr int kDays = 4;  // kFirstDay is durable; the next three are the crash window
constexpr const char* kMeasurement = "recovery_stats";

seastar::future<> insertDay(Engine& engine, uint32_t day) {
    TimeStarInsert<double> insert(kMeasurement, "batteryPercent");
    insert.addTag("deviceId", "dev-a");
    insert.addValue(static_cast<uint64_t>(day) * ke::NS_PER_DAY + 1'000'000'000ULL, 91.0);
    co_await engine.insert(std::move(insert));
}

seastar::future<size_t> seriesInDay(Engine& engine, uint32_t day) {
    const uint64_t start = static_cast<uint64_t>(day) * ke::NS_PER_DAY;
    auto found = co_await engine.getIndex().findSeriesWithMetadataTimeScoped(kMeasurement, {{"deviceId", "dev-a"}}, {},
                                                                             start, start + ke::NS_PER_DAY - 1);
    co_return found.has_value() ? found->size() : 0;
}

}  // namespace

SEASTAR_TEST_F(DayBitmapRecoveryTest, InitRepairsDayBitmapsLostToAnUncleanShutdown) {
    {
        Engine engine;
        co_await engine.init();

        for (int d = 0; d < kDays; ++d) {
            co_await insertDay(engine, kFirstDay + d);
        }

        // Everything through kFirstDay is durable; the rest dies with the
        // process, exactly as an unclean exit leaves it. The data itself is
        // untouched — that is the whole point, it was always readable.
        co_await NativeIndexTestAccess::dropDayBitmapsFrom(engine.getIndex(), kMeasurement, kFirstDay + 1);

        EXPECT_EQ(co_await seriesInDay(engine, kFirstDay + kDays - 1), 0u)
            << "test precondition: the lost days should be undiscoverable before the repair";

        co_await engine.stop();
    }

    Engine engine;
    co_await engine.init();  // WAL replay -> TSM, then the day-bitmap repair

    // Every day of the crash window must be discoverable on its own, not only
    // via a range wide enough to catch the last durable day.
    for (int d = 0; d < kDays; ++d) {
        EXPECT_EQ(co_await seriesInDay(engine, kFirstDay + d), 1u) << "day " << (kFirstDay + d);
    }

    // A day the series never wrote to must still prune — the repair restores
    // membership, it does not disable time scoping.
    EXPECT_EQ(co_await seriesInDay(engine, kFirstDay + kDays + 5), 0u);

    co_await engine.stop();
}
