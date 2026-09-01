/*
 * Discovery consistency: scoped lookup vs independent enumeration.
 *
 * Three production incidents share one shape — a structure derived from a cache
 * is consulted first, and an empty result is treated as "does not exist" rather
 * than "unknown, go look":
 *
 *   1.4.0  measurement blooms rebuilt from bitmapCache_ alone → scoped queries
 *          returned 0 series for ~52% of the series that existed.
 *   1.4.2  per-day bitmaps never rebuilt at open → any query whose range STARTED
 *          in the window lost to a crash returned 0 series.
 *   live   recordDaySpan's 366-day clamp → a first batch carrying older history
 *          records no membership for it, so a query starting there returns 0.
 *
 * Every one of them is invisible to a test that only asks the scoped path — it
 * answers confidently, just wrongly. The invariant that catches all three is a
 * differential: whatever a scoped lookup finds, an INDEPENDENT enumeration must
 * also find.
 *
 * getAllSeriesForMeasurement is the one true oracle here. It is a single
 * kvPrefixScan over MEASUREMENT_SERIES keys and touches none of bitmapCache_,
 * measurementBloomCache_, dayBitmapCache_, discoveryCache_, localIdMap_ or
 * seriesMetadataCache_ — so it cannot confirm the structures under test.
 *
 * Deliberately NOT used as the oracle:
 *   - findSeries with empty tagFilters — it delegates to
 *     getAllSeriesForMeasurement, so comparing them proves nothing.
 *   - getSeriesGroupedByTag — it shares the postings substrate AND warms
 *     bitmapCache_ as a side effect, which would mask the very false negative
 *     being hunted on any later scoped call.
 */

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/index/key_encoding.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers/native_index_test_access.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <seastar/core/coroutine.hh>
#include <set>
#include <string>
#include <vector>

using namespace timestar::index;
namespace ke = timestar::index::keys;

class DiscoveryConsistencyTest : public ::testing::Test {
public:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

namespace {

uint32_t todayDay() {
    const auto nowNs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
    return ke::dayBucketFromNs(nowNs);
}

// Every series the scoped/tag-filtered path can reach, unioned over the
// measurement's own tag values.
seastar::future<std::set<SeriesId128>> scopedReachable(NativeIndex& index, const std::string& measurement) {
    // The discovery cache must not answer — it would return whatever the
    // structures said last time, which is what we are trying to test.
    index.invalidateDiscoveryCache(measurement);

    std::set<SeriesId128> reachable;
    auto tagKeys = co_await index.getTags(measurement);
    for (const auto& tagKey : tagKeys) {
        auto values = co_await index.getTagValues(measurement, tagKey);
        for (const auto& value : values) {
            // maxSeries = 0: findSeriesByTag TRUNCATES rather than erroring on a
            // limit, which would look like a real difference below.
            auto found = co_await index.findSeriesByTag(measurement, tagKey, value, 0);
            reachable.insert(found.begin(), found.end());
        }
    }
    co_return reachable;
}

seastar::future<std::set<SeriesId128>> enumerated(NativeIndex& index, const std::string& measurement) {
    auto all = co_await index.getAllSeriesForMeasurement(measurement, 0);
    EXPECT_TRUE(all.has_value());
    std::set<SeriesId128> ids;
    if (all.has_value()) {
        ids.insert(all->begin(), all->end());
    }
    co_return ids;
}

// Report the difference rather than just a count: the failing ids are the whole
// diagnostic — they are exactly {bloom false negatives} ∪ {missing LocalIds} ∪
// {lost day membership}.
void expectNoneMissing(const std::set<SeriesId128>& enumeratedIds, const std::set<SeriesId128>& reachableIds,
                       const char* what) {
    std::vector<std::string> missing;
    for (const auto& id : enumeratedIds) {
        if (reachableIds.count(id) == 0) {
            missing.push_back(id.toHex());
        }
    }
    EXPECT_TRUE(missing.empty()) << missing.size() << " series exist but are unreachable by " << what << ": "
                                 << (missing.empty() ? std::string{} : missing.front())
                                 << (missing.size() > 1 ? " …" : "");
}

}  // namespace

// Baseline: on a healthy index the two agree exactly.
SEASTAR_TEST_F(DiscoveryConsistencyTest, ScopedLookupReachesEverySeriesEnumerationFinds) {
    NativeIndex index(0);
    co_await index.open();

    const uint64_t dayNs = static_cast<uint64_t>(todayDay() - 1) * ke::NS_PER_DAY;
    for (int i = 0; i < 50; ++i) {
        TimeStarInsert<double> insert("consistency", "v");
        insert.tags = {{"deviceId", "dev-" + std::to_string(i)}, {"site", i % 2 ? "north" : "south"}};
        insert.timestamps = {dayNs + static_cast<uint64_t>(i)};
        insert.values = {1.0};
        co_await index.indexInsert(insert);
    }

    auto enumeratedIds = co_await enumerated(index, "consistency");
    auto reachableIds = co_await scopedReachable(index, "consistency");
    EXPECT_EQ(enumeratedIds.size(), 50u);
    expectNoneMissing(enumeratedIds, reachableIds, "tag-scoped lookup");

    co_await index.close();
}

// The 1.4.0 shape: a persisted bloom that has lost keys must not be able to
// hide a series. This is the differential the incident needed and did not have.
SEASTAR_TEST_F(DiscoveryConsistencyTest, StaleBloomCannotHideSeriesFromScopedLookup) {
    {
        NativeIndex index(0);
        co_await index.open();
        for (int i = 0; i < 5; ++i) {
            co_await index.getOrCreateSeriesId("bloomcheck", {{"deviceId", "dev-" + std::to_string(i)}}, "v");
        }
        co_await index.close();
    }

    // Plant a bloom that only knows dev-0 — what a <= 1.4.0 server could leave
    // on disk — and shut down with it in place.
    {
        NativeIndex index(0);
        co_await index.open();
        auto onlyDev0 = ke::encodePostingsBitmapPrefix("bloomcheck", "deviceId") + "dev-0";
        co_await NativeIndexTestAccess::plantStaleBloom(index, "bloomcheck", {onlyDev0});

        // Within THIS session the stale bloom does hide the other four: nothing
        // re-derives it mid-session, and the guarantee the code actually makes
        // is repair at open(). Asserting consistency here would be asserting
        // something the implementation never promised.
        auto blinded = co_await scopedReachable(index, "bloomcheck");
        EXPECT_LT(blinded.size(), 5u) << "test precondition: the planted bloom should blind the scoped path";
        co_await index.close();
    }

    // The guarantee: the next open() rebuilds every measurement bloom from the
    // persisted postings keys BEFORE serving, so the differential holds again.
    NativeIndex index(0);
    co_await index.open();

    auto enumeratedIds = co_await enumerated(index, "bloomcheck");
    auto reachableIds = co_await scopedReachable(index, "bloomcheck");
    EXPECT_EQ(enumeratedIds.size(), 5u);
    expectNoneMissing(enumeratedIds, reachableIds, "tag-scoped lookup after a stale bloom was repaired at open");

    co_await index.close();
}

// The 1.4.2 shape, plus the still-live clamp: time-scoped discovery over the
// range the data actually spans must reach every series enumeration finds.
//
// The range is kept under MAX_DAY_SCAN (365) on purpose — beyond it the day
// filter is bypassed entirely and the check would pass without testing
// anything.
SEASTAR_TEST_F(DiscoveryConsistencyTest, TimeScopedDiscoveryReachesEverySeriesAfterLostDayMembership) {
    const uint32_t lastDay = todayDay() - 1;
    const uint32_t firstDay = lastDay - 10;

    NativeIndex index(0);
    co_await index.open();

    for (int i = 0; i < 20; ++i) {
        TimeStarInsert<double> insert("daycheck", "v");
        insert.tags = {{"deviceId", "dev-" + std::to_string(i)}};
        insert.timestamps = {static_cast<uint64_t>(firstDay + (i % 10)) * ke::NS_PER_DAY + 1};
        insert.values = {1.0};
        co_await index.indexInsert(insert);
    }

    // Lose the second half of the window, as a crash would.
    co_await NativeIndexTestAccess::dropDayBitmapsInRange(index, "daycheck", firstDay + 5, lastDay);

    std::vector<NativeIndex::SeriesTimeBounds> bounds;
    for (int i = 0; i < 20; ++i) {
        bounds.push_back({SeriesId128::fromSeriesKey(
                              timestar::buildSeriesKey("daycheck", {{"deviceId", "dev-" + std::to_string(i)}}, "v")),
                          static_cast<uint64_t>(firstDay) * ke::NS_PER_DAY,
                          static_cast<uint64_t>(lastDay) * ke::NS_PER_DAY});
    }
    co_await index.rebuildDayBitmapsFromBounds(bounds, firstDay, lastDay);

    index.invalidateDiscoveryCache("daycheck");
    auto timeScoped = co_await index.findSeriesWithMetadataTimeScoped(
        "daycheck", {}, {}, static_cast<uint64_t>(firstDay) * ke::NS_PER_DAY,
        static_cast<uint64_t>(lastDay + 1) * ke::NS_PER_DAY - 1);
    EXPECT_TRUE(timeScoped.has_value());

    std::set<SeriesId128> reachableIds;
    if (timeScoped.has_value()) {
        for (const auto& s : *timeScoped) {
            reachableIds.insert(s.seriesId);
        }
    }
    auto enumeratedIds = co_await enumerated(index, "daycheck");
    EXPECT_EQ(enumeratedIds.size(), 20u);
    expectNoneMissing(enumeratedIds, reachableIds, "time-scoped discovery after a repair");

    co_await index.close();
}

// The still-live clamp, end to end: a first batch spanning more than
// kMaxDaySpan (366) days records membership only for the trailing window, so a
// query starting at the old end has no day bitmap to match. It must fall back
// to unscoped discovery rather than reporting nothing — this is the
// "invisible until endTime >= 1e17" behaviour seen in production.
SEASTAR_TEST_F(DiscoveryConsistencyTest, QueryBelowTheOldestRecordedDayFallsBackInsteadOfReturningNothing) {
    const uint32_t recentDay = todayDay() - 1;
    const uint64_t nearEpochNs = 90'000'000'000ULL;  // ~day 1: an unset device clock

    NativeIndex index(0);
    co_await index.open();

    // The batch path: metadata carries only [minTs, maxTs], and the span is far
    // wider than the clamp allows.
    MetadataOp op;
    op.valueType = TSMValueType::Float;
    op.measurement = "clamped";
    op.fieldName = "v";
    op.tags = {{"deviceId", "dev-a"}};
    op.minTs = nearEpochNs;
    op.maxTs = static_cast<uint64_t>(recentDay) * ke::NS_PER_DAY;
    co_await index.indexMetadataBatch({op});

    // A narrow query at the near-epoch end: no day bitmap covers it.
    auto early = co_await index.findSeriesWithMetadataTimeScoped("clamped", {{"deviceId", "dev-a"}}, {}, 0,
                                                                 10ULL * ke::NS_PER_DAY);
    EXPECT_TRUE(early.has_value());
    EXPECT_EQ(early->size(), 1u) << "a query below the oldest recorded day must fall back, not report nothing";

    // A measurement that was never clamped must NOT fall back: days below its
    // oldest recorded day are days before it existed, and a fallback there
    // would turn every ordinary query on a young measurement into a full
    // discovery scan.
    {
        MetadataOp young;
        young.valueType = TSMValueType::Float;
        young.measurement = "young";
        young.fieldName = "v";
        young.tags = {{"deviceId", "dev-b"}};
        young.minTs = static_cast<uint64_t>(recentDay) * ke::NS_PER_DAY;
        young.maxTs = young.minTs;
        co_await index.indexMetadataBatch({young});

        const uint64_t beforeItExisted = static_cast<uint64_t>(recentDay - 30) * ke::NS_PER_DAY;
        auto r = co_await index.findSeriesWithMetadataTimeScoped("young", {{"deviceId", "dev-b"}}, {}, beforeItExisted,
                                                                 beforeItExisted + ke::NS_PER_DAY - 1);
        EXPECT_TRUE(r.has_value());
        EXPECT_EQ(r->size(), 0u) << "an unclamped measurement must keep pruning below its oldest recorded day";
    }

    // A quiet day INSIDE the recorded range must still prune — the fallback may
    // not become "always discoverable".
    const uint64_t quietStart = static_cast<uint64_t>(recentDay - 200) * ke::NS_PER_DAY;
    auto quiet = co_await index.findSeriesWithMetadataTimeScoped("clamped", {{"deviceId", "dev-a"}}, {}, quietStart,
                                                                 quietStart + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(quiet.has_value());
    EXPECT_EQ(quiet->size(), 0u) << "pruning inside the recorded range must survive the fallback";

    co_await index.close();
}

// One healthy series must not be able to hide a clamped one.
//
// The clamp fallback originally lived inside the "day union came back empty"
// branch. A measurement normally holds many devices: if one device's history
// was clamped away while its neighbours are healthy, the union is NON-empty —
// the neighbours match — so that branch never runs and the clamped device is
// dropped from every query reaching into its missing range. The single-series
// test passed the whole time, because with one series the union IS empty.
SEASTAR_TEST_F(DiscoveryConsistencyTest, ClampedSeriesIsFoundEvenWhenHealthySeriesMatchTheSameRange) {
    const uint32_t recentDay = todayDay() - 1;
    const uint32_t oldDay = recentDay - 900;  // far outside kMaxDaySpan (366)

    NativeIndex index(0);
    co_await index.open();

    // dev-clamped: first batch spans ~900 days, so the old end is refused.
    MetadataOp clamped;
    clamped.valueType = TSMValueType::Float;
    clamped.measurement = "mixed";
    clamped.fieldName = "v";
    clamped.tags = {{"deviceId", "dev-clamped"}};
    clamped.minTs = static_cast<uint64_t>(oldDay) * ke::NS_PER_DAY;
    clamped.maxTs = static_cast<uint64_t>(recentDay) * ke::NS_PER_DAY;
    co_await index.indexMetadataBatch({clamped});

    // dev-healthy: writes on the old day too, and IS recorded there, so the day
    // union for a query over that range is non-empty.
    MetadataOp healthy;
    healthy.valueType = TSMValueType::Float;
    healthy.measurement = "mixed";
    healthy.fieldName = "v";
    healthy.tags = {{"deviceId", "dev-healthy"}};
    healthy.minTs = static_cast<uint64_t>(oldDay) * ke::NS_PER_DAY;
    healthy.maxTs = static_cast<uint64_t>(oldDay) * ke::NS_PER_DAY;
    co_await index.indexMetadataBatch({healthy});

    const uint64_t start = static_cast<uint64_t>(oldDay) * ke::NS_PER_DAY;
    auto found = co_await index.findSeriesWithMetadataTimeScoped("mixed", {}, {}, start, start + 3 * ke::NS_PER_DAY);
    EXPECT_TRUE(found.has_value());

    std::set<std::string> devices;
    if (found.has_value()) {
        for (const auto& s : *found) {
            auto it = s.metadata.tags.find("deviceId");
            if (it != s.metadata.tags.end()) {
                devices.insert(it->second);
            }
        }
    }
    EXPECT_TRUE(devices.count("dev-healthy")) << "the healthy series must still be found";
    EXPECT_TRUE(devices.count("dev-clamped"))
        << "a healthy neighbour made the day union non-empty and hid the clamped series";

    co_await index.close();
}
