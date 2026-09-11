// Production-path tests for retention during compaction (the tripwire).
//
// THE BUG THESE EXIST FOR: TSMCompactor::setRetentionContext() -- whose own
// comment said "Called by Engine before triggering compaction" -- had zero
// callers outside a unit test. The real path was
//
//   TSMFileManager::tierCompactionLoop -> compactOneTier
//     -> TSMCompactor::executeCompaction
//     -> std::exchange(_pendingRetentionPolicies, {})   <- ALWAYS EMPTY
//     -> compact(files, targetTier, targetSeq, {}, {})
//
// so in every running server per-point TTL trimming was silently not applied
// (a file holding one in-retention point kept every expired point forever) and
// downsampling never happened at all.
//
// The suite was green throughout, because EVERY retention test called
// compactor->compact(files, policies, seriesMap) directly -- entering BELOW the
// wiring that was broken. Even the test named SetRetentionContextAppliedOnCompact
// passed the policies to compact() explicitly as well, so the consumption path
// had literally zero coverage.
//
// The governing principle for these tests: ENTER AT OR ABOVE THE LAYER THAT WAS
// BROKEN. They drive a real Engine, install a policy exactly the way
// PUT /retention does, accumulate real tier-0 files through real rollovers, and
// trigger the real merge entry point (TSMFileManager::compactOneTier).

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/retention/retention_policy.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/later.hh>
#include <string>
#include <vector>

class CompactionRetentionWiringTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

namespace {

constexpr uint64_t kSec = 1'000'000'000ULL;
constexpr uint64_t kMin = 60 * kSec;
constexpr uint64_t kHour = 60 * kMin;
constexpr uint64_t kDay = 24 * kHour;

uint64_t nowNanos() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

// Roll the memory store over and wait for the background WAL->TSM conversion
// to register its file, so the caller ends up with a known tier-0 file count.
seastar::future<bool> rolloverAndAwaitTsmFile(Engine& engine) {
    const size_t before = engine.getTSMFileCount();
    co_await engine.rolloverMemoryStore();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (engine.getTSMFileCount() == before) {
        if (std::chrono::steady_clock::now() > deadline) {
            co_return false;
        }
        co_await seastar::yield();
    }
    co_return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// THE TRIPWIRE.
//
// Aged points must fold and TTL-expired points must disappear when the merge is
// driven the way production drives it. Nothing here passes a policy to
// compact(): the ONLY route from the policy cache to the merge is the provider
// Engine::init() installs, which is precisely the wiring that did not exist.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(CompactionRetentionWiringTest, RetentionAppliedOnProductionCompactionPath) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const std::string measurement = "scada_wire";
        const uint64_t now = nowNanos();

        // Downsample buckets are epoch-aligned, so anchor the aged points to a
        // real bucket start rather than to `now`.
        const uint64_t kInterval = 10 * kMin;
        const uint64_t agedBucket = ((now - 6 * kHour) / kInterval) * kInterval;
        const uint64_t expiredBase = now - 3 * kDay;  // outside a 2-day TTL
        const uint64_t recentBase = now - 5 * kMin;   // newer than the 1h fold threshold

        const size_t filesPerMerge = timestar::config().storage.compaction.files_per_merge;
        constexpr size_t kAgedPerFile = 5;

        // Sum of the aged values, for the expected average.
        double agedSum = 0.0;
        size_t agedCount = 0;

        TimeStarInsert<double> keyBuilder(measurement, "value");
        keyBuilder.addTag("rtu", "r1");
        const std::string seriesKey = keyBuilder.seriesKey();

        for (size_t f = 0; f < filesPerMerge; ++f) {
            TimeStarInsert<double> insert(measurement, "value");
            insert.addTag("rtu", "r1");

            // 1) A point well outside the TTL.
            insert.addValue(expiredBase + f * kSec, 1.0);

            // 2) Aged points, ALL inside one downsample bucket, spread across
            //    the files so the fold has to happen after the merge.
            for (size_t k = 0; k < kAgedPerFile; ++k) {
                const size_t n = f * kAgedPerFile + k;
                const double v = static_cast<double>(n + 1);
                insert.addValue(agedBucket + n * kSec, v);
                agedSum += v;
                ++agedCount;
            }

            // 3) A recent point, newer than the fold threshold.
            insert.addValue(recentBase + f * kSec, 100.0 + static_cast<double>(f));

            co_await engine.insert(std::move(insert));

            const bool converted = co_await rolloverAndAwaitTsmFile(engine);
            EXPECT_TRUE(converted) << "WAL->TSM conversion did not complete for file " << f;
            if (!converted) {
                throw std::runtime_error("conversion did not complete");
            }
        }

        EXPECT_EQ(engine.getTSMFileManager().getFileCountInTier(0), filesPerMerge);

        // Install the policy exactly the way PUT /retention does: write it into
        // every shard's cache (invoke_on_all -> updateRetentionPolicyCache).
        RetentionPolicy policy;
        policy.measurement = measurement;
        policy.ttl = "2d";
        policy.ttlNanos = 2 * kDay;
        DownsamplePolicy ds;
        ds.after = "1h";
        ds.afterNanos = kHour;
        ds.interval = "10m";
        ds.intervalNanos = kInterval;
        ds.method = "avg";
        policy.downsample = ds;
        engine.updateRetentionPolicyCache(policy);

        // Drive the real merge entry point.
        const bool merged = co_await engine.getTSMFileManager().compactOneTier(0);
        EXPECT_TRUE(merged) << "tier 0 did not merge";
        EXPECT_EQ(engine.getTSMFileManager().getConsecutiveFailures(0), 0u) << "the merge failed";

        // Assert through an Engine query -- the shape a client would see.
        auto resultOpt = co_await engine.query(seriesKey, 0, UINT64_MAX);
        EXPECT_TRUE(resultOpt.has_value());
        if (!resultOpt.has_value()) {
            throw std::runtime_error("series vanished from the engine after compaction");
        }
        const auto& result = std::get<QueryResult<double>>(resultOpt.value());

        std::map<uint64_t, double> points;
        for (size_t i = 0; i < result.timestamps.size(); ++i) {
            points[result.timestamps[i]] = result.values[i];
        }

        // TTL: every expired point is gone. Before the fix these survived,
        // because the merge ran with an empty policy map.
        for (size_t f = 0; f < filesPerMerge; ++f) {
            EXPECT_EQ(points.count(expiredBase + f * kSec), 0u)
                << "TTL-expired point at " << (expiredBase + f * kSec)
                << " survived compaction: per-point TTL trimming is not reaching the merge.";
        }

        // Downsample: the aged points collapsed into ONE bucket-start point
        // carrying their average.
        EXPECT_EQ(points.count(agedBucket), 1u)
            << "no folded point at the bucket start: downsampling is not reaching the merge.";
        if (points.count(agedBucket)) {
            EXPECT_DOUBLE_EQ(points.at(agedBucket), agedSum / static_cast<double>(agedCount));
        }
        for (size_t n = 1; n < agedCount; ++n) {
            EXPECT_EQ(points.count(agedBucket + n * kSec), 0u)
                << "raw aged point at offset " << n << " survived the fold";
        }

        // Recent points are untouched.
        for (size_t f = 0; f < filesPerMerge; ++f) {
            EXPECT_EQ(points.count(recentBase + f * kSec), 1u) << "recent point " << f << " was lost";
        }

        EXPECT_EQ(points.size(), 1 + filesPerMerge) << "expected exactly one folded bucket plus the recent points";
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ---------------------------------------------------------------------------
// The provider itself: with a policy installed it resolves the plan's series to
// their measurement through the index and returns the policy map.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(CompactionRetentionWiringTest, ProviderResolvesSeriesMeasurementForPolicyBearingSeries) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const uint64_t now = nowNanos();

        TimeStarInsert<double> covered("prov_covered", "value");
        covered.addTag("rtu", "r1");
        covered.addValue(now - kHour, 1.0);
        const SeriesId128 coveredId = SeriesId128::fromSeriesKey(covered.seriesKey());
        co_await engine.insert(std::move(covered));

        TimeStarInsert<double> other("prov_other", "value");
        other.addTag("rtu", "r2");
        other.addValue(now - kHour, 2.0);
        const SeriesId128 otherId = SeriesId128::fromSeriesKey(other.seriesKey());
        co_await engine.insert(std::move(other));

        RetentionPolicy policy;
        policy.measurement = "prov_covered";
        policy.ttl = "1d";
        policy.ttlNanos = kDay;
        engine.updateRetentionPolicyCache(policy);

        auto ctx = co_await engine.buildCompactionRetentionContext({coveredId, otherId});

        EXPECT_EQ(ctx.policies.count("prov_covered"), 1u);
        EXPECT_EQ(ctx.seriesMeasurement.count(coveredId), 1u) << "the policy-bearing series was not resolved";
        if (ctx.seriesMeasurement.count(coveredId)) {
            EXPECT_EQ(ctx.seriesMeasurement.at(coveredId), "prov_covered");
        }
        // A series whose measurement carries no policy is filtered out here
        // rather than handed to the compactor to filter again.
        EXPECT_EQ(ctx.seriesMeasurement.count(otherId), 0u);

        // Both ids were resolved, so both are cached for the next merge.
        EXPECT_EQ(engine.seriesMeasurementCacheSize(), 2u);

        // Second call must be answerable from the cache alone (same answer).
        auto ctx2 = co_await engine.buildCompactionRetentionContext({coveredId, otherId});
        EXPECT_EQ(ctx2.seriesMeasurement.count(coveredId), 1u);
        EXPECT_EQ(engine.seriesMeasurementCacheSize(), 2u);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ---------------------------------------------------------------------------
// No-policy fast path: ZERO index resolution.
//
// Compaction is the hottest background path in the server and most deployments
// have no retention policy at all, so the provider must not cost a metadata
// lookup per merge just to discover there is nothing to do.
//
// Observability: the provider's ONLY index call is getSeriesMetadataBatch, and
// every successful resolution populates _seriesMeasurementCache. The series
// below are all indexed, so a resolution could not fail to populate it -- a
// cache that is still empty proves nothing was resolved.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(CompactionRetentionWiringTest, NoPolicyFastPathPerformsNoIndexResolution) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const uint64_t now = nowNanos();
        std::vector<SeriesId128> ids;
        for (int i = 0; i < 8; ++i) {
            TimeStarInsert<double> insert("fastpath", "value");
            insert.addTag("rtu", "r" + std::to_string(i));
            insert.addValue(now - kHour, static_cast<double>(i));
            ids.push_back(SeriesId128::fromSeriesKey(insert.seriesKey()));
            co_await engine.insert(std::move(insert));
        }

        // 1) No policies at all.
        auto ctx = co_await engine.buildCompactionRetentionContext(ids);
        EXPECT_TRUE(ctx.policies.empty());
        EXPECT_TRUE(ctx.seriesMeasurement.empty());
        EXPECT_EQ(engine.seriesMeasurementCacheSize(), 0u)
            << "the no-policy fast path resolved series metadata; it must return without touching the index.";

        // 2) A policy that exists but is INERT (no TTL, no usable downsample
        //    clause) cannot change any stored point, so it must not trigger
        //    resolution either.
        RetentionPolicy inert;
        inert.measurement = "fastpath";
        engine.updateRetentionPolicyCache(inert);

        auto ctx2 = co_await engine.buildCompactionRetentionContext(ids);
        EXPECT_TRUE(ctx2.policies.empty());
        EXPECT_TRUE(ctx2.seriesMeasurement.empty());
        EXPECT_EQ(engine.seriesMeasurementCacheSize(), 0u)
            << "an inert policy (no TTL, no downsample) triggered index resolution on the merge path.";

        // 3) Give it a TTL and the provider must now do the work -- otherwise
        //    the checks above would pass for the wrong reason.
        inert.ttl = "1d";
        inert.ttlNanos = kDay;
        engine.updateRetentionPolicyCache(inert);

        auto ctx3 = co_await engine.buildCompactionRetentionContext(ids);
        EXPECT_EQ(ctx3.policies.count("fastpath"), 1u);
        EXPECT_EQ(ctx3.seriesMeasurement.size(), ids.size());
        EXPECT_EQ(engine.seriesMeasurementCacheSize(), ids.size());
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ---------------------------------------------------------------------------
// Bulk resolution path (by MEASUREMENT, not by id).
//
// Resolving a large merge id-by-id costs ~70us per bloom-filtered point lookup
// issued strictly sequentially -- measured 6.9s for 100k series, which a merge
// running every few seconds cannot afford. Above
// MAX_UNRESOLVED_FOR_PER_ID_LOOKUP the provider instead enumerates each
// policy-bearing measurement through the 0x0A MEASUREMENT_SERIES prefix (228ms
// for the same 100k), which answers membership for every id at once.
//
// That path learns MEMBERSHIP rather than identity, so it records a negative
// sentinel for ids it did not find -- valid only relative to the policy set
// that produced it. The second half of this test pins the invalidation.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(CompactionRetentionWiringTest, BulkResolutionByMeasurementAndPolicyChangeInvalidation) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        // Comfortably above MAX_UNRESOLVED_FOR_PER_ID_LOOKUP (4096).
        constexpr size_t kCovered = 5000;
        constexpr size_t kUncovered = 500;

        std::vector<SeriesId128> ids;
        ids.reserve(kCovered + kUncovered);
        for (size_t i = 0; i < kCovered; ++i) {
            std::map<std::string, std::string> tags{{"rtu", "r" + std::to_string(i)}};
            ids.push_back(co_await engine.getIndex().getOrCreateSeriesId("bulk_covered", tags, "value"));
        }
        for (size_t i = 0; i < kUncovered; ++i) {
            std::map<std::string, std::string> tags{{"rtu", "u" + std::to_string(i)}};
            ids.push_back(co_await engine.getIndex().getOrCreateSeriesId("bulk_other", tags, "value"));
        }

        RetentionPolicy policy;
        policy.measurement = "bulk_covered";
        policy.ttl = "1d";
        policy.ttlNanos = kDay;
        engine.updateRetentionPolicyCache(policy);

        auto ctx = co_await engine.buildCompactionRetentionContext(ids);
        EXPECT_EQ(ctx.seriesMeasurement.size(), kCovered)
            << "bulk resolution did not identify every series of the policy-bearing measurement";
        for (size_t i = 0; i < kCovered; ++i) {
            EXPECT_EQ(ctx.seriesMeasurement.count(ids[i]), 1u);
        }
        for (size_t i = kCovered; i < ids.size(); ++i) {
            EXPECT_EQ(ctx.seriesMeasurement.count(ids[i]), 0u) << "a series with no policy was handed to the compactor";
        }

        // Every id got an answer -- positive for the covered measurement,
        // negative sentinel for the rest -- so the next merge resolves nothing.
        EXPECT_EQ(engine.seriesMeasurementCacheSize(), ids.size());
        auto ctxCached = co_await engine.buildCompactionRetentionContext(ids);
        EXPECT_EQ(ctxCached.seriesMeasurement.size(), kCovered);

        // POLICY CHANGE: the negative sentinels recorded above are only valid
        // for the old policy set. Adding a policy for the other measurement
        // must not leave those series silently exempt from retention.
        RetentionPolicy other;
        other.measurement = "bulk_other";
        other.ttl = "1d";
        other.ttlNanos = kDay;
        engine.updateRetentionPolicyCache(other);

        EXPECT_EQ(engine.seriesMeasurementCacheSize(), 0u) << "a policy change did not invalidate the cache";

        auto ctxAfter = co_await engine.buildCompactionRetentionContext(ids);
        EXPECT_EQ(ctxAfter.seriesMeasurement.size(), ids.size())
            << "series newly covered by a policy were still treated as uncovered -- a stale negative "
               "sentinel silently exempts them from TTL and downsampling.";
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ---------------------------------------------------------------------------
// A policy broadcast that lands WHILE a provider call is suspended.
//
// invalidateSeriesMeasurementCache() clears the cache synchronously at the
// moment of the mutation, but the bulk resolution path computes its negative
// sentinels from the policy set it snapshotted BEFORE its index scan and writes
// them AFTER it resumes -- i.e. into the cache that was just cleared. Those
// entries then say "this series belongs to no policy-bearing measurement" about
// a policy set that no longer exists, and nothing will ever invalidate them
// again (the only invalidation trigger is the next policy mutation).
//
// Consequence: every subsequent merge resolves those series from the poisoned
// cache and hands the compactor an empty retention context for them, so TTL
// trimming and downsampling silently do not happen -- the exact defect Phase 1
// exists to fix, reintroduced through the cache.
//
// The index is compacted first so the 0x0A prefix scan reads SSTables (O_DIRECT,
// so a genuine suspension) rather than running to completion inside the
// memtable-only synchronous fast path.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(CompactionRetentionWiringTest, PolicyChangeDuringProviderCallDoesNotStrandNegativeSentinels) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        constexpr size_t kCovered = 5000;  // > MAX_UNRESOLVED_FOR_PER_ID_LOOKUP
        constexpr size_t kOther = 500;

        std::vector<SeriesId128> ids;
        ids.reserve(kCovered + kOther);
        for (size_t i = 0; i < kCovered; ++i) {
            std::map<std::string, std::string> tags{{"rtu", "r" + std::to_string(i)}};
            ids.push_back(co_await engine.getIndex().getOrCreateSeriesId("race_covered", tags, "value"));
        }
        for (size_t i = 0; i < kOther; ++i) {
            std::map<std::string, std::string> tags{{"rtu", "u" + std::to_string(i)}};
            ids.push_back(co_await engine.getIndex().getOrCreateSeriesId("race_other", tags, "value"));
        }

        // Push everything into SSTables so the prefix scan really suspends.
        co_await engine.getIndex().compact();

        RetentionPolicy covered;
        covered.measurement = "race_covered";
        covered.ttl = "1d";
        covered.ttlNanos = kDay;
        engine.updateRetentionPolicyCache(covered);

        RetentionPolicy other;
        other.measurement = "race_other";
        other.ttl = "1d";
        other.ttlNanos = kDay;

        // The broadcast lands on the reactor while the provider is suspended in
        // its scan -- exactly what invoke_on_all -> updateRetentionPolicyCache
        // does when PUT /retention arrives during a merge.
        auto mutation = seastar::yield().then([&engine, other] { engine.updateRetentionPolicyCache(other); });
        auto provider = engine.buildCompactionRetentionContext(ids);

        co_await std::move(mutation);
        auto ctx = co_await std::move(provider);
        (void)ctx;

        // The next merge must see BOTH measurements. If the in-flight call wrote
        // its pre-broadcast sentinels, the race_other series are permanently
        // exempt from retention.
        auto ctxAfter = co_await engine.buildCompactionRetentionContext(ids);
        EXPECT_EQ(ctxAfter.seriesMeasurement.size(), ids.size())
            << "a policy broadcast that landed during a provider call left stale negative sentinels: "
            << (ids.size() - ctxAfter.seriesMeasurement.size())
            << " series are now silently exempt from TTL and downsampling on every future merge.";
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ===========================================================================
// PER-FIELD DOWNSAMPLE METHODS (Phase 4) — through the real Engine.
//
// The compactor-level fold is covered in
// test/unit/storage/downsample_field_methods_test.cpp. These enter above the
// provider, because the field half of the retention context is resolved there
// and nowhere else: a fold that is correct given a field map proves nothing if
// the field map is never built.
// ===========================================================================

// THE COST CLAIM, made observable. A shard whose policies name no
// `fieldMethods` must not resolve one field, ever — Phase 2 deliberately moved
// context resolution to per-measurement to avoid a per-series charge, and
// per-field methods must not quietly undo that.
SEASTAR_TEST_F(CompactionRetentionWiringTest, NoFieldResolutionWithoutFieldMethods) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const uint64_t now = nowNanos();
        std::vector<SeriesId128> ids;
        for (const char* field : {"level", "flow_total", "status"}) {
            TimeStarInsert<double> insert("fm_cost", field);
            insert.addTag("rtu", "r1");
            insert.addValue(now - kHour, 1.0);
            ids.push_back(SeriesId128::fromSeriesKey(insert.seriesKey()));
            co_await engine.insert(std::move(insert));
        }

        RetentionPolicy policy;
        policy.measurement = "fm_cost";
        policy.ttl = "30d";
        policy.ttlNanos = 30 * kDay;
        DownsamplePolicy ds;
        ds.after = "1h";
        ds.afterNanos = kHour;
        ds.interval = "10m";
        ds.intervalNanos = 10 * kMin;
        ds.method = "avg";
        policy.downsampleTiers = {ds};
        timestar::retention::normalizeRetentionTiers(policy);
        engine.updateRetentionPolicyCache(policy);

        auto ctx = co_await engine.buildCompactionRetentionContext(ids);
        EXPECT_EQ(ctx.seriesMeasurement.size(), ids.size()) << "the measurement half must still resolve";
        EXPECT_TRUE(ctx.seriesField.empty())
            << "a policy with no fieldMethods produced a field map: the default path is paying for a "
               "feature it does not use";
        EXPECT_EQ(engine.seriesFieldCacheSize(), 0u)
            << "a policy with no fieldMethods resolved fields through the index";

        // Now add overrides and re-resolve: only the NAMED fields appear.
        policy.downsampleTiers[0].fieldMethods = std::map<std::string, std::string>{{"flow_total", "max"}};
        timestar::retention::normalizeRetentionTiers(policy);
        engine.updateRetentionPolicyCache(policy);

        auto ctx2 = co_await engine.buildCompactionRetentionContext(ids);
        EXPECT_EQ(ctx2.seriesField.size(), 1u)
            << "only the OVERRIDDEN field belongs in the field map; every other series must miss it and "
               "take the measurement default";
        for (const auto& [sid, field] : ctx2.seriesField) {
            (void)sid;
            EXPECT_EQ(field, "flow_total");
        }
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// The whole feature, end to end on the production merge path: an analog, a
// totalizer and a BOOLEAN status word under one policy, folded by one real
// compaction, read back through Engine::query().
//
// The boolean assertion is the load-bearing one. It comes back through
// QueryResult<bool>, so a value coerced to 1.0/0.0 could not appear at all —
// and its two buckets disagree, so no numeric fold could reproduce the pair.
SEASTAR_TEST_F(CompactionRetentionWiringTest, PerFieldMethodsAppliedOnProductionCompactionPath) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const std::string measurement = "fm_scada";
        const uint64_t now = nowNanos();
        const uint64_t kInterval = 10 * kMin;
        // Two aged buckets, both well past the 1h fold threshold.
        const uint64_t bucket0 = ((now - 6 * kHour) / kInterval) * kInterval;
        const uint64_t bucket1 = bucket0 + kInterval;

        const size_t filesPerMerge = timestar::config().storage.compaction.files_per_merge;

        TimeStarInsert<double> levelKeyB(measurement, "level");
        levelKeyB.addTag("rtu", "r1");
        const std::string levelKey = levelKeyB.seriesKey();
        TimeStarInsert<double> flowKeyB(measurement, "flow_total");
        flowKeyB.addTag("rtu", "r1");
        const std::string flowKey = flowKeyB.seriesKey();
        TimeStarInsert<bool> statusKeyB(measurement, "status");
        statusKeyB.addTag("rtu", "r1");
        const std::string statusKey = statusKeyB.seriesKey();

        double levelSum = 0.0;
        size_t levelCount = 0;
        double flowMax = 0.0;

        for (size_t f = 0; f < filesPerMerge; ++f) {
            TimeStarInsert<double> level(measurement, "level");
            level.addTag("rtu", "r1");
            TimeStarInsert<double> flow(measurement, "flow_total");
            flow.addTag("rtu", "r1");
            TimeStarInsert<bool> status(measurement, "status");
            status.addTag("rtu", "r1");

            // All of these land in bucket0; the last file also seeds bucket1.
            const uint64_t ts = bucket0 + f * kSec;
            const double lv = 10.0 * static_cast<double>(f + 1);
            const double fv = 1000.0 + static_cast<double>(f);
            level.addValue(ts, lv);
            flow.addValue(ts, fv);
            // bucket0 must end FALSE: the last-written point of the bucket is
            // the one at the greatest timestamp, which is f == filesPerMerge-1.
            status.addValue(ts, f + 1 < filesPerMerge);
            levelSum += lv;
            ++levelCount;
            flowMax = std::max(flowMax, fv);

            if (f + 1 == filesPerMerge) {
                // bucket1 ends TRUE, so the two folded booleans disagree.
                status.addValue(bucket1, false);
                status.addValue(bucket1 + kSec, true);
            }

            co_await engine.insert(std::move(level));
            co_await engine.insert(std::move(flow));
            co_await engine.insert(std::move(status));

            const bool converted = co_await rolloverAndAwaitTsmFile(engine);
            EXPECT_TRUE(converted) << "WAL->TSM conversion did not complete for file " << f;
            if (!converted) {
                throw std::runtime_error("conversion did not complete");
            }
        }

        RetentionPolicy policy;
        policy.measurement = measurement;
        DownsamplePolicy ds;
        ds.after = "1h";
        ds.afterNanos = kHour;
        ds.interval = "10m";
        ds.intervalNanos = kInterval;
        ds.method = "avg";
        ds.fieldMethods = std::map<std::string, std::string>{{"flow_total", "max"}, {"status", "latest"}};
        policy.downsampleTiers = {ds};
        timestar::retention::normalizeRetentionTiers(policy);
        if (auto why = timestar::retention::validateRetentionPolicy(policy); why.has_value()) {
            throw std::runtime_error("fixture policy is invalid: " + *why);
        }
        engine.updateRetentionPolicyCache(policy);

        const bool merged = co_await engine.getTSMFileManager().compactOneTier(0);
        EXPECT_TRUE(merged) << "tier 0 did not merge";
        EXPECT_EQ(engine.getTSMFileManager().getConsecutiveFailures(0), 0u) << "the merge failed";

        auto levelOpt = co_await engine.query(levelKey, 0, UINT64_MAX);
        auto flowOpt = co_await engine.query(flowKey, 0, UINT64_MAX);
        auto statusOpt = co_await engine.query(statusKey, 0, UINT64_MAX);
        if (!levelOpt.has_value() || !flowOpt.has_value() || !statusOpt.has_value()) {
            throw std::runtime_error("a series vanished from the engine after compaction");
        }

        const auto& levelRes = std::get<QueryResult<double>>(levelOpt.value());
        const auto& flowRes = std::get<QueryResult<double>>(flowOpt.value());
        const auto& statusRes = std::get<QueryResult<bool>>(statusOpt.value());

        EXPECT_EQ(levelRes.timestamps.size(), 1u) << "the analog must fold to one bucket";
        if (levelRes.timestamps.size() != 1u) {
            throw std::runtime_error("analog fold shape wrong");
        }
        EXPECT_EQ(levelRes.timestamps[0], bucket0);
        EXPECT_DOUBLE_EQ(levelRes.values[0], levelSum / static_cast<double>(levelCount))
            << "level must take the TIER method (avg)";

        EXPECT_EQ(flowRes.timestamps.size(), 1u) << "the totalizer must fold to one bucket";
        if (flowRes.timestamps.size() != 1u) {
            throw std::runtime_error("totalizer fold shape wrong");
        }
        EXPECT_DOUBLE_EQ(flowRes.values[0], flowMax)
            << "flow_total must take its OVERRIDE (max); averaging a totalizer is the silent corruption "
               "per-field methods exist to prevent";

        // Two buckets, LATEST-per-bucket, IN THE WRITTEN TYPE.
        EXPECT_EQ(statusRes.timestamps.size(), 2u) << "the status word must fold to one point per bucket";
        if (statusRes.timestamps.size() != 2u) {
            throw std::runtime_error("status fold shape wrong");
        }
        EXPECT_EQ(statusRes.timestamps[0], bucket0);
        EXPECT_EQ(statusRes.timestamps[1], bucket1);
        EXPECT_FALSE(statusRes.values[0]) << "bucket 0's LATEST value is false";
        EXPECT_TRUE(statusRes.values[1]) << "bucket 1's LATEST value is true";
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}
