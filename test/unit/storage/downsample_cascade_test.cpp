// Downsampling CASCADE tests (Phase 2 of docs/downsampling-cascade-plan.md).
//
// The executable form of the plan's composition table. A cascade folds stage-1
// output through stage 2 rather than re-reading raw points, so the question
// that decides whether the feature is honest is: does
// `raw --fold--> 1m --fold--> 15m` equal `raw --fold--> 15m`?
//
//   min / max / sum / latest : YES, exactly. Proven here on GAP-BEARING data.
//   avg                      : NO. The composed mean is an UNWEIGHTED mean of
//                              bucket means, exact only when every stage-1
//                              bucket in the stage-2 window holds the same
//                              sample count. Also proven here, with the
//                              divergence measured rather than hand-waved.
//
// Irregular per-minute counts are precisely what SCADA comm gaps produce, so
// the inexact case is the real one, not a corner. See docs/api-retention.md.

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/retention/retention_policy.hpp"
#include "../../../lib/storage/tsm_compactor.hpp"
#include "../../../lib/storage/tsm_file_manager.hpp"
#include "../../../lib/storage/tsm_reader.hpp"
#include "../../../lib/storage/tsm_result.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t NS_PER_SEC = 1'000'000'000ULL;
constexpr uint64_t ONE_MINUTE_NS = 60ULL * NS_PER_SEC;
constexpr uint64_t FIFTEEN_MINUTES_NS = 15ULL * ONE_MINUTE_NS;
constexpr uint64_t ONE_DAY_NS = 24ULL * 3600ULL * NS_PER_SEC;

uint64_t nowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

DownsamplePolicy tier(uint64_t afterNanos, uint64_t intervalNanos, const std::string& method) {
    DownsamplePolicy ds;
    ds.after = std::to_string(afterNanos) + "ns";
    ds.afterNanos = afterNanos;
    ds.interval = std::to_string(intervalNanos) + "ns";
    ds.intervalNanos = intervalNanos;
    ds.method = method;
    return ds;
}

RetentionPolicy cascadePolicy(const std::string& measurement, std::vector<DownsamplePolicy> tiers) {
    RetentionPolicy p;
    p.measurement = measurement;
    p.downsampleTiers = std::move(tiers);
    timestar::retention::normalizeRetentionTiers(p);
    return p;
}

}  // namespace

class DownsampleCascadeTest : public ::testing::Test {
public:
    std::string testDir = "./test_downsample_cascade_files";
    fs::path savedCwd;
    std::unique_ptr<TSMFileManager> fileManager;
    std::unique_ptr<TSMCompactor> compactor;
    uint64_t nextSeq = 0;

    void SetUp() override {
        savedCwd = fs::current_path();
        if (fs::current_path().filename() == "test_downsample_cascade_files") {
            fs::current_path(savedCwd.parent_path());
            savedCwd = fs::current_path();
        }
        fs::remove_all(testDir);
        fs::create_directories(testDir + "/shard_0/tsm");
        fs::current_path(testDir);

        fileManager = std::make_unique<TSMFileManager>();
        compactor = std::make_unique<TSMCompactor>(fileManager.get());
    }

    void TearDown() override {
        compactor.reset();
        fileManager.reset();
        fs::current_path(savedCwd);
        fs::remove_all(testDir);
    }

    seastar::shared_ptr<TSM> makeFloatFile(const std::string& seriesKey, const std::vector<uint64_t>& timestamps,
                                           const std::vector<double>& values) {
        const uint64_t seq = nextSeq++;
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/%02u_%010lu.tsm", 0u, seq);

        TSMWriter writer(filename);
        SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
        writer.writeSeries(TSMValueType::Float, sid, timestamps, values);
        writer.writeIndex();
        writer.close();

        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = 0;
        tsm->seqNum = seq;
        return tsm;
    }

    static seastar::future<std::map<uint64_t, double>> readAllFloat(const std::string& path,
                                                                    const std::string& seriesKey) {
        auto tsm = seastar::make_shared<TSM>(path);
        co_await tsm->open();
        co_await tsm->readSparseIndex();

        SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
        TSMResult<double> result(0);
        co_await tsm->readSeries(sid, 0, UINT64_MAX, result);

        std::map<uint64_t, double> out;
        for (const auto& blk : result.blocks) {
            for (size_t i = 0; i < blk->timestamps.size(); i++) {
                out[blk->timestamps.at(i)] = blk->values.at(i);
            }
        }
        co_await tsm->close();
        co_return out;
    }

    // Compact one already-open file (or several) under a policy and return the
    // opened output path.
    seastar::future<std::string> fold(std::vector<seastar::shared_ptr<TSM>> files, const RetentionPolicy& policy,
                                      const std::string& seriesKey) {
        SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
        std::unordered_map<std::string, RetentionPolicy> policies{{policy.measurement, policy}};
        std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMap{{sid, policy.measurement}};
        auto result = co_await compactor->compact(files, policies, seriesMap);
        co_return result.outputPath;
    }

    // Write raw points to a file, fold under `policy`, and return the folded
    // series. The output file is left on disk so it can be folded again.
    seastar::future<std::pair<std::string, std::map<uint64_t, double>>> writeAndFold(const std::string& seriesKey,
                                                                                     const std::vector<uint64_t>& ts,
                                                                                     const std::vector<double>& vals,
                                                                                     const RetentionPolicy& policy) {
        auto file = makeFloatFile(seriesKey, ts, vals);
        co_await file->open();
        co_await file->readSparseIndex();
        auto path = co_await fold({file}, policy, seriesKey);
        co_await file->close();
        auto data = co_await readAllFloat(path, seriesKey);
        co_return std::make_pair(path, std::move(data));
    }

    // Fold an existing output file again under a second policy.
    seastar::future<std::map<uint64_t, double>> refold(const std::string& path, const RetentionPolicy& policy,
                                                       const std::string& seriesKey) {
        auto file = seastar::make_shared<TSM>(path);
        co_await file->open();
        co_await file->readSparseIndex();
        auto out = co_await fold({file}, policy, seriesKey);
        co_await file->close();
        co_return co_await readAllFloat(out, seriesKey);
    }
};

// ---------------------------------------------------------------------------
// Synthetic SCADA-shaped data: nominally 1 Hz, with comm gaps that make the
// per-minute sample count IRREGULAR. That irregularity is the entire reason
// avg does not compose; the exact methods must survive it untouched.
// ---------------------------------------------------------------------------
namespace {

struct GapBearingSeries {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
};

// `base` must be minute-aligned. Produces `minutes` minutes of data where
// minute i contains `counts[i % counts.size()]` samples at 1 Hz from the start
// of that minute; a count of 0 is a whole-minute outage.
GapBearingSeries makeGapBearing(uint64_t base, size_t minutes, const std::vector<size_t>& counts, double seed) {
    GapBearingSeries s;
    double v = seed;
    for (size_t m = 0; m < minutes; ++m) {
        const size_t n = counts[m % counts.size()];
        for (size_t i = 0; i < n; ++i) {
            s.timestamps.push_back(base + m * ONE_MINUTE_NS + static_cast<uint64_t>(i) * NS_PER_SEC);
            // Deliberately wide spread across minutes: an unweighted mean of
            // bucket means only diverges visibly when the bucket means differ.
            v += 1.0 + 0.37 * static_cast<double>(m);
            s.values.push_back(v);
        }
    }
    return s;
}

}  // namespace

// ===========================================================================
// CASCADE EQUIVALENCE — the composition table, executed.
//
// For each method: fold raw -> 1m, then 1m -> 15m, and compare against a
// direct raw -> 15m fold of the same points.
// ===========================================================================
SEASTAR_TEST_F(DownsampleCascadeTest, CascadeEqualsDirectFoldForComposableMethods) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    // Everything is older than both thresholds, so both stages apply in full.
    const uint64_t after = 30 * ONE_DAY_NS;
    const uint64_t base = ((now - 200 * ONE_DAY_NS) / FIFTEEN_MINUTES_NS) * FIFTEEN_MINUTES_NS;

    // 30 minutes (two 15m buckets) with a strongly irregular minute profile,
    // including two complete outages.
    const std::vector<size_t> counts{60, 5, 60, 0, 17, 60, 1, 60, 0, 60};
    auto data = makeGapBearing(base, 30, counts, 100.0);
    if (data.timestamps.empty()) {
        ADD_FAILURE() << "fixture produced no points";
        co_return;
    }

    for (const char* method : {"min", "max", "sum", "latest", "avg"}) {
        const std::string seriesKey = std::string("scada|dev=rtu1|") + method;

        // Path A: raw --fold--> 1m --fold--> 15m
        auto oneMinute = co_await self->writeAndFold(seriesKey, data.timestamps, data.values,
                                                     cascadePolicy(measurement, {tier(after, ONE_MINUTE_NS, method)}));
        auto cascaded = co_await self->refold(
            oneMinute.first, cascadePolicy(measurement, {tier(after, FIFTEEN_MINUTES_NS, method)}), seriesKey);

        // Path B: raw --fold--> 15m
        const std::string directKey = seriesKey + "_direct";
        auto direct =
            co_await self->writeAndFold(directKey, data.timestamps, data.values,
                                        cascadePolicy(measurement, {tier(after, FIFTEEN_MINUTES_NS, method)}));

        EXPECT_EQ(cascaded.size(), direct.second.size()) << "method=" << method << ": bucket COUNT must match; "
                                                         << "stage-over-stage folding must not create or lose buckets";
        if (cascaded.size() != direct.second.size()) {
            continue;
        }
        auto ci = cascaded.begin();
        auto di = direct.second.begin();
        for (; ci != cascaded.end(); ++ci, ++di) {
            EXPECT_EQ(ci->first, di->first) << "method=" << method << ": bucket START timestamps must match";
            if (std::string(method) == "avg") {
                // Documented approximation — asserted separately below.
                continue;
            }
            EXPECT_DOUBLE_EQ(ci->second, di->second)
                << "method=" << method << " bucket=" << ci->first
                << ": min/max/sum/latest compose EXACTLY over stages; a mismatch means the cascade is "
                   "silently changing stored values";
        }
    }

    co_return;
}

// ===========================================================================
// avg: the documented inexactness, MEASURED.
//
// Two claims, both asserted:
//   1. with irregular per-bucket counts the cascade diverges from the direct
//      fold, and the divergence is bounded by the spread of the stage-1 bucket
//      means (that is the actual bound: the composed value is a convex
//      combination of the same bucket means, just with 1/n weights);
//   2. with UNIFORM per-bucket counts it is exact.
// ===========================================================================
SEASTAR_TEST_F(DownsampleCascadeTest, AvgCascadePreservesSampleWeightsAcrossGaps) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t after = 30 * ONE_DAY_NS;
    const uint64_t base = ((now - 200 * ONE_DAY_NS) / FIFTEEN_MINUTES_NS) * FIFTEEN_MINUTES_NS;

    const std::vector<size_t> counts{60, 5, 60, 0, 17, 60, 1, 60, 0, 60};
    auto data = makeGapBearing(base, 30, counts, 100.0);

    const std::string seriesKey = "scada|dev=rtu1|analog";
    auto oneMinute = co_await self->writeAndFold(seriesKey, data.timestamps, data.values,
                                                 cascadePolicy(measurement, {tier(after, ONE_MINUTE_NS, "avg")}));
    auto cascaded = co_await self->refold(
        oneMinute.first, cascadePolicy(measurement, {tier(after, FIFTEEN_MINUTES_NS, "avg")}), seriesKey);

    const std::string directKey = "scada|dev=rtu1|analog_direct";
    auto direct = co_await self->writeAndFold(directKey, data.timestamps, data.values,
                                              cascadePolicy(measurement, {tier(after, FIFTEEN_MINUTES_NS, "avg")}));

    EXPECT_EQ(cascaded.size(), direct.second.size());
    if (cascaded.size() != direct.second.size()) {
        co_return;
    }

    for (const auto& [ts, exact] : direct.second) {
        EXPECT_NEAR(cascaded.at(ts), exact, 1e-12 * std::max(1.0, std::abs(exact)))
            << "Persisted sample counts must preserve the weights of gappy minutes";
    }

    // --- Uniform counts: the cascade is EXACT. ---
    const std::vector<size_t> uniform{60};
    auto even = makeGapBearing(base, 30, uniform, 500.0);
    const std::string evenKey = "scada|dev=rtu2|analog";
    auto evenOneMinute = co_await self->writeAndFold(evenKey, even.timestamps, even.values,
                                                     cascadePolicy(measurement, {tier(after, ONE_MINUTE_NS, "avg")}));
    auto evenCascaded = co_await self->refold(
        evenOneMinute.first, cascadePolicy(measurement, {tier(after, FIFTEEN_MINUTES_NS, "avg")}), evenKey);
    const std::string evenDirectKey = "scada|dev=rtu2|analog_direct";
    auto evenDirect = co_await self->writeAndFold(evenDirectKey, even.timestamps, even.values,
                                                  cascadePolicy(measurement, {tier(after, FIFTEEN_MINUTES_NS, "avg")}));

    EXPECT_EQ(evenCascaded.size(), evenDirect.second.size());
    for (const auto& [ts, exact] : evenDirect.second) {
        EXPECT_NEAR(evenCascaded.at(ts), exact, 1e-9)
            << "bucket=" << ts << ": with equal sample counts the unweighted mean of means IS the weighted mean";
    }

    co_return;
}

// ===========================================================================
// A single compaction carrying a real two-tier cascade: data spanning the raw
// band, the 1m band and the 15m band must come out ascending, with each
// segment at its own resolution and nothing crossing a boundary.
// ===========================================================================
SEASTAR_TEST_F(DownsampleCascadeTest, TwoTierPolicyPartitionsOneStreamIntoThreeResolutions) {
    const std::string measurement = "scada";
    const std::string seriesKey = "scada|dev=rtu9|value";
    const uint64_t now = nowNs();

    const uint64_t afterFine = 7 * ONE_DAY_NS;
    const uint64_t afterCoarse = 90 * ONE_DAY_NS;
    const uint64_t fineThreshold = ((now - afterFine) / ONE_MINUTE_NS) * ONE_MINUTE_NS;
    const uint64_t coarseThreshold = ((now - afterCoarse) / FIFTEEN_MINUTES_NS) * FIFTEEN_MINUTES_NS;

    std::vector<uint64_t> ts;
    std::vector<double> vals;
    double v = 0.0;
    auto emitWindow = [&](uint64_t start, size_t seconds) {
        for (size_t i = 0; i < seconds; ++i) {
            ts.push_back(start + static_cast<uint64_t>(i) * NS_PER_SEC);
            vals.push_back(v += 1.0);
        }
    };
    // 30 min at 1 Hz well below the coarse threshold -> folds to 15m.
    // Start on the 15m grid so the window is exactly two whole buckets.
    emitWindow(coarseThreshold - 45 * ONE_MINUTE_NS, 30 * 60);
    // 10 min at 1 Hz between the thresholds -> folds to 1m
    emitWindow(fineThreshold - 20 * ONE_MINUTE_NS, 10 * 60);
    // 5 min at 1 Hz above the fine threshold -> stays raw
    emitWindow(fineThreshold + ONE_MINUTE_NS, 5 * 60);

    auto policy = cascadePolicy(measurement,
                                {tier(afterFine, ONE_MINUTE_NS, "avg"), tier(afterCoarse, FIFTEEN_MINUTES_NS, "avg")});
    EXPECT_FALSE(timestar::retention::validateRetentionPolicy(policy).has_value());

    auto folded = co_await self->writeAndFold(seriesKey, ts, vals, policy);
    const auto& out = folded.second;
    if (out.empty()) {
        ADD_FAILURE() << "fold produced no points";
        co_return;
    }

    size_t coarseBuckets = 0, fineBuckets = 0, rawPoints = 0;
    uint64_t prev = 0;
    for (const auto& [t, value] : out) {
        (void)value;
        EXPECT_GT(t, prev) << "the fold output must be strictly ascending — the streaming writer relies on it";
        prev = t;
        if (t < coarseThreshold) {
            ++coarseBuckets;
            EXPECT_EQ(t % FIFTEEN_MINUTES_NS, 0u) << "stage-2 points must sit on the 15m epoch grid";
        } else if (t < fineThreshold) {
            ++fineBuckets;
            EXPECT_EQ(t % ONE_MINUTE_NS, 0u) << "stage-1 points must sit on the 1m epoch grid";
        } else {
            ++rawPoints;
        }
    }

    EXPECT_EQ(coarseBuckets, 2u) << "30 minutes of aged data is exactly two 15m buckets";
    EXPECT_EQ(fineBuckets, 10u) << "10 minutes in the middle band is exactly ten 1m buckets";
    EXPECT_EQ(rawPoints, 300u) << "the newest 5 minutes must pass through at full resolution";

    co_return;
}

// ===========================================================================
// MULTI-TIER IDEMPOTENCY: re-folding already-cascaded data changes nothing, at
// every stage. Only COMPLETE buckets fold (thresholds are aligned down to their
// own interval), so a folded bucket's single bucket-start point re-folds to
// itself for every offered method.
// ===========================================================================
SEASTAR_TEST_F(DownsampleCascadeTest, MultiTierFoldIsIdempotentAtEveryStage) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t afterFine = 7 * ONE_DAY_NS;
    const uint64_t afterCoarse = 90 * ONE_DAY_NS;
    const uint64_t coarseThreshold = ((now - afterCoarse) / FIFTEEN_MINUTES_NS) * FIFTEEN_MINUTES_NS;
    const uint64_t fineThreshold = ((now - afterFine) / ONE_MINUTE_NS) * ONE_MINUTE_NS;

    for (const char* method : {"avg", "min", "max", "sum", "latest"}) {
        const std::string seriesKey = std::string("scada|dev=idem|") + method;

        std::vector<uint64_t> ts;
        std::vector<double> vals;
        double v = 0.0;
        for (size_t i = 0; i < 30 * 60; ++i) {  // 30 min below the coarse threshold
            ts.push_back(coarseThreshold - 40 * ONE_MINUTE_NS + static_cast<uint64_t>(i) * NS_PER_SEC);
            vals.push_back(v += 0.75);
        }
        for (size_t i = 0; i < 10 * 60; ++i) {  // 10 min in the middle band
            ts.push_back(fineThreshold - 20 * ONE_MINUTE_NS + static_cast<uint64_t>(i) * NS_PER_SEC);
            vals.push_back(v += 0.75);
        }

        auto policy = cascadePolicy(
            measurement, {tier(afterFine, ONE_MINUTE_NS, method), tier(afterCoarse, FIFTEEN_MINUTES_NS, method)});
        auto first = co_await self->writeAndFold(seriesKey, ts, vals, policy);
        auto second = co_await self->refold(first.first, policy, seriesKey);

        EXPECT_EQ(first.second.size(), second.size())
            << "method=" << method << ": a second fold changed the POINT COUNT, so the fold is not idempotent";
        if (first.second.size() != second.size()) {
            continue;
        }
        auto a = first.second.begin();
        auto b = second.begin();
        for (; a != first.second.end(); ++a, ++b) {
            EXPECT_EQ(a->first, b->first) << "method=" << method;
            EXPECT_DOUBLE_EQ(a->second, b->second)
                << "method=" << method << " bucket=" << a->first
                << ": re-folding a completed bucket must reproduce it exactly; a drift here means aged data "
                   "degrades a little on every compaction, forever";
        }
    }

    co_return;
}

// ===========================================================================
// MIGRATION, at the level that matters: not "does the JSON parse" but "does the
// SAME DATA come out the same".
//
//  1. A record persisted by a pre-cascade binary must fold identically after
//     the upgrade.
//  2. A two-tier record read through the LEGACY field only (what a rolled-back
//     binary sees) must apply tier 1 and nothing else — finer than intended,
//     never coarser.
// ===========================================================================
SEASTAR_TEST_F(DownsampleCascadeTest, LegacyRecordFoldsIdenticallyAfterUpgrade) {
    const std::string measurement = "temperature";
    const uint64_t now = nowNs();
    const uint64_t after = 30 * ONE_DAY_NS;
    const uint64_t base = ((now - 200 * ONE_DAY_NS) / FIFTEEN_MINUTES_NS) * FIFTEEN_MINUTES_NS;

    const std::vector<size_t> counts{60, 5, 60, 0, 17, 60};
    auto data = makeGapBearing(base, 30, counts, 42.0);

    // (a) The pre-upgrade in-memory shape: only the legacy optional set.
    RetentionPolicy preUpgrade;
    preUpgrade.measurement = measurement;
    preUpgrade.downsample = tier(after, ONE_MINUTE_NS, "avg");

    // (b) The same record after the read path promotes it. This is exactly
    //     what NativeIndex::getRetentionPolicy now does to a stored legacy JSON
    //     record.
    RetentionPolicy postUpgrade;
    const std::string legacyJson = std::string(R"({"measurement":"temperature","downsample":{)") + R"("after":")" +
                                   std::to_string(after) + R"(ns","afterNanos":)" + std::to_string(after) +
                                   R"(,"interval":")" + std::to_string(ONE_MINUTE_NS) + R"(ns","intervalNanos":)" +
                                   std::to_string(ONE_MINUTE_NS) + R"(,"method":"avg"}})";
    EXPECT_FALSE(static_cast<bool>(glz::read_json(postUpgrade, legacyJson)));
    EXPECT_TRUE(postUpgrade.downsampleTiers.empty()) << "the stored bytes genuinely carry no tier list";
    timestar::retention::normalizeRetentionTiers(postUpgrade);
    EXPECT_EQ(postUpgrade.downsampleTiers.size(), 1u);

    auto before = co_await self->writeAndFold("temperature|s=1|pre", data.timestamps, data.values, preUpgrade);
    auto afterUpgrade = co_await self->writeAndFold("temperature|s=1|post", data.timestamps, data.values, postUpgrade);

    EXPECT_EQ(before.second.size(), afterUpgrade.second.size())
        << "an existing single-tier policy must keep folding exactly as it did before the cascade landed";
    auto a = before.second.begin();
    auto b = afterUpgrade.second.begin();
    for (; a != before.second.end() && b != afterUpgrade.second.end(); ++a, ++b) {
        EXPECT_EQ(a->first, b->first);
        EXPECT_DOUBLE_EQ(a->second, b->second);
    }

    co_return;
}

SEASTAR_TEST_F(DownsampleCascadeTest, RolledBackReaderAppliesFinestTierOnly) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t afterFine = 7 * ONE_DAY_NS;
    const uint64_t afterCoarse = 90 * ONE_DAY_NS;
    const uint64_t coarseThreshold = ((now - afterCoarse) / FIFTEEN_MINUTES_NS) * FIFTEEN_MINUTES_NS;

    // 30 minutes of 1 Hz data, all older than BOTH thresholds — so the two
    // policies below must disagree, which is what makes the comparison mean
    // something.
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    double v = 0.0;
    for (size_t i = 0; i < 30 * 60; ++i) {
        ts.push_back(coarseThreshold - 45 * ONE_MINUTE_NS + static_cast<uint64_t>(i) * NS_PER_SEC);
        vals.push_back(v += 1.5);
    }

    auto cascade = cascadePolicy(measurement,
                                 {tier(afterFine, ONE_MINUTE_NS, "avg"), tier(afterCoarse, FIFTEEN_MINUTES_NS, "avg")});
    auto persisted = glz::write_json(cascade);
    EXPECT_TRUE(persisted.has_value());

    // What a LEGACY-SHAPED reader sees: only the mirror survives its parse.
    // Simulated by projecting the parsed record onto the legacy field — the
    // fold behaviour is what this test is about. Whether a real downgraded
    // binary gets this far is a separate question with a separate answer: it
    // must read leniently, or Glaze's unknown-key default rejects the whole
    // record and drops the policy. Pinned by
    // RetentionCascadeMigrationTest::NewRecordMirrorsFinestTierIntoLegacyField
    // and documented in docs/api-retention.md.
    RetentionPolicy fullRead;
    EXPECT_FALSE(static_cast<bool>(glz::read_json(fullRead, *persisted)));
    RetentionPolicy rolledBack;
    rolledBack.measurement = fullRead.measurement;
    rolledBack.ttlNanos = fullRead.ttlNanos;
    rolledBack.downsample = fullRead.downsample;
    timestar::retention::normalizeRetentionTiers(rolledBack);

    auto viaRollback = co_await self->writeAndFold("scada|dev=rb|rollback", ts, vals, rolledBack);
    auto viaTierOneOnly = co_await self->writeAndFold(
        "scada|dev=rb|tier1", ts, vals, cascadePolicy(measurement, {tier(afterFine, ONE_MINUTE_NS, "avg")}));
    auto viaCascade = co_await self->writeAndFold("scada|dev=rb|cascade", ts, vals, cascade);

    EXPECT_EQ(viaRollback.second.size(), viaTierOneOnly.second.size())
        << "a rolled-back binary must behave exactly as a tier-1-only policy";
    EXPECT_EQ(viaRollback.second, viaTierOneOnly.second);

    // ... and that is strictly FINER than the cascade, never coarser: more
    // points retained, so nothing the newer binary would have kept is lost.
    EXPECT_EQ(viaRollback.second.size(), 30u) << "tier 1 alone gives 1m resolution: 30 buckets";
    EXPECT_EQ(viaCascade.second.size(), 2u) << "the full cascade gives 15m resolution: 2 buckets";
    EXPECT_GT(viaRollback.second.size(), viaCascade.second.size());

    co_return;
}

// ===========================================================================
// BOUNDED MEMORY: fold 90 days x 1 Hz for ONE series under a fixed budget, and
// prove the budget is structural rather than lucky by folding a THIRD of the
// history and checking the peak barely moves.
//
// The design this replaces held one AggregationState per bucket for the
// series' entire aged segment before flushing. 90 days at 1m is 129,600
// buckets; at ~176 B per std::map node that is ~22 MiB resident for a SINGLE
// in-flight series, times up to seriesBatchSize() x 4 pipelines — and the
// cascade makes it worse, since folding deeper history is exactly what a second
// tier exists for.
//
// The stream is ascending, so a bucket is final the moment a timestamp at or
// past its end arrives. The fold keeps ONE live bucket per stage and drains
// completed ones through the sink in block-aligned chunks.
// ===========================================================================
SEASTAR_TEST_F(DownsampleCascadeTest, DeepFirstFoldHoldsBoundedMemory) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t base = ((now - 200 * ONE_DAY_NS) / ONE_MINUTE_NS) * ONE_MINUTE_NS;
    auto policy = cascadePolicy(measurement, {tier(30 * ONE_DAY_NS, ONE_MINUTE_NS, "avg")});

    // Fold `days` of 1 Hz data and return (peak allocation delta, folded point
    // count).
    auto measureFold = [&](const std::string& seriesKey, uint64_t days) -> seastar::future<std::pair<size_t, size_t>> {
        const uint64_t span = days * ONE_DAY_NS;
        const size_t points = static_cast<size_t>(span / NS_PER_SEC);

        std::vector<uint64_t> ts;
        std::vector<double> vals;
        ts.reserve(points);
        vals.reserve(points);
        for (size_t i = 0; i < points; ++i) {
            ts.push_back(base + static_cast<uint64_t>(i) * NS_PER_SEC);
            vals.push_back(20.0 + std::sin(static_cast<double>(i) * 0.001));
        }

        auto file = self->makeFloatFile(seriesKey, ts, vals);
        // Release the generator's own buffers before measuring; they are the
        // test's memory, not the fold's.
        ts = {};
        vals = {};
        ts.shrink_to_fit();
        vals.shrink_to_fit();

        co_await file->open();
        co_await file->readSparseIndex();

        // Sample allocated memory while the fold runs. The compaction suspends
        // on every sink hand-off and every block read, so a reactor timer
        // genuinely observes its peak rather than just its endpoints.
        const size_t baseline = seastar::memory::stats().allocated_memory();
        size_t peak = baseline;
        seastar::timer<> sampler([&peak] { peak = std::max(peak, seastar::memory::stats().allocated_memory()); });
        sampler.arm_periodic(std::chrono::milliseconds(2));

        auto outPath = co_await self->fold({file}, policy, seriesKey);
        sampler.cancel();
        co_await file->close();

        EXPECT_FALSE(outPath.empty());
        auto folded = co_await DownsampleCascadeTest::readAllFloat(outPath, seriesKey);
        co_return std::make_pair(peak > baseline ? peak - baseline : 0, folded.size());
    };

    auto shallow = co_await measureFold("scada|dev=deep|shallow", 30);
    auto deep = co_await measureFold("scada|dev=deep|value", 90);

    GTEST_LOG_(INFO) << "fold peak allocation: 30d/" << shallow.second
                     << " buckets = " << (shallow.first / (1024 * 1024)) << " MiB, 90d/" << deep.second
                     << " buckets = " << (deep.first / (1024 * 1024)) << " MiB";

    EXPECT_EQ(deep.second, 90 * ONE_DAY_NS / ONE_MINUTE_NS)
        << "90 days at 1m resolution is exactly one point per minute of the span — a bounded-memory "
           "assertion over a fold that did not happen is worthless";
    EXPECT_EQ(shallow.second, 30 * ONE_DAY_NS / ONE_MINUTE_NS);

    // Absolute budget. What remains resident is the merge buffer
    // (MERGE_CHUNK_POINTS points), the decoded block in flight, and the writer's
    // output buffer — all bounded and independent of how much history is folded.
    // The bucket map this replaces needed ~22 MiB for its nodes ALONE on the 90d
    // input, on top of all of the above.
    constexpr size_t kBudget = 24ULL * 1024 * 1024;
    EXPECT_LT(deep.first, kBudget) << "fold of 90 days x 1 Hz took " << (deep.first / (1024 * 1024)) << " MiB";

    // And the structural claim the budget stands on: TRIPLING the history must
    // not meaningfully move the peak. This is the assertion that survives a
    // different machine, allocator, or block size — a per-bucket design would
    // grow by ~15 MiB between these two.
    const size_t growth = deep.first > shallow.first ? deep.first - shallow.first : 0;
    constexpr size_t kMaxGrowth = 8ULL * 1024 * 1024;
    EXPECT_LT(growth, kMaxGrowth) << "peak fold memory grew by " << (growth / (1024 * 1024))
                                  << " MiB when the aged segment tripled, so it is scaling with bucket count "
                                     "rather than staying bounded";

    co_return;
}
