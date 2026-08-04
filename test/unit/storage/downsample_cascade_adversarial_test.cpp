// ADVERSARIAL cascade tests (review of Phase 2, docs/downsampling-cascade-plan.md).
//
// The Phase 2 suite exercises the cascade only on SMALL series (~1800 points),
// which never reach a spill: MERGE_CHUNK_POINTS is 256Ki, so every one of those
// folds runs entirely inside the single tail flush at the end of
// processSeriesForCompaction(). The incremental machinery that the bounded-memory
// claim rests on — mid-fold drains through the sink, a live bucket carried across
// a suspension, a stage boundary crossed between two drains — is therefore only
// covered for a SINGLE tier (the 90d memory test), never for a cascade.
//
// These tests enter at the same layer but with enough points to force multiple
// spills, and pin the NaN/Inf rules at EVERY stage rather than only the finest.

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/retention/retention_policy.hpp"
#include "../../../lib/storage/tsm_compactor.hpp"
#include "../../../lib/storage/tsm_file_manager.hpp"
#include "../../../lib/storage/tsm_reader.hpp"
#include "../../../lib/storage/tsm_result.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
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

DownsamplePolicy adversarialTier(uint64_t afterNanos, uint64_t intervalNanos, const std::string& method) {
    DownsamplePolicy ds;
    ds.after = std::to_string(afterNanos) + "ns";
    ds.afterNanos = afterNanos;
    ds.interval = std::to_string(intervalNanos) + "ns";
    ds.intervalNanos = intervalNanos;
    ds.method = method;
    return ds;
}

RetentionPolicy adversarialPolicy(const std::string& measurement, std::vector<DownsamplePolicy> tiers) {
    RetentionPolicy p;
    p.measurement = measurement;
    p.downsampleTiers = std::move(tiers);
    timestar::retention::normalizeRetentionTiers(p);
    return p;
}

}  // namespace

class DownsampleCascadeAdversarialTest : public ::testing::Test {
public:
    std::string testDir = "./test_downsample_cascade_adv_files";
    fs::path savedCwd;
    std::unique_ptr<TSMFileManager> fileManager;
    std::unique_ptr<TSMCompactor> compactor;
    uint64_t nextSeq = 0;

    void SetUp() override {
        savedCwd = fs::current_path();
        if (fs::current_path().filename() == "test_downsample_cascade_adv_files") {
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

    // Read a folded series back IN STORED ORDER — a std::map would silently
    // repair an out-of-order write, which is exactly the failure the streaming
    // writer cannot tolerate.
    static seastar::future<std::vector<std::pair<uint64_t, double>>> readInStoredOrder(const std::string& path,
                                                                                       const std::string& seriesKey) {
        auto tsm = seastar::make_shared<TSM>(path);
        co_await tsm->open();
        co_await tsm->readSparseIndex();

        SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
        TSMResult<double> result(0);
        co_await tsm->readSeries(sid, 0, UINT64_MAX, result);

        std::vector<std::pair<uint64_t, double>> out;
        for (const auto& blk : result.blocks) {
            for (size_t i = 0; i < blk->timestamps.size(); i++) {
                out.emplace_back(blk->timestamps.at(i), blk->values.at(i));
            }
        }
        co_await tsm->close();
        co_return out;
    }

    seastar::future<std::vector<std::pair<uint64_t, double>>> writeAndFold(const std::string& seriesKey,
                                                                           const std::vector<uint64_t>& ts,
                                                                           const std::vector<double>& vals,
                                                                           const RetentionPolicy& policy) {
        auto file = makeFloatFile(seriesKey, ts, vals);
        co_await file->open();
        co_await file->readSparseIndex();

        SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
        std::unordered_map<std::string, RetentionPolicy> policies{{policy.measurement, policy}};
        std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMap{{sid, policy.measurement}};
        auto result = co_await compactor->compact({file}, policies, seriesMap);
        co_await file->close();
        if (result.outputPath.empty()) {
            co_return std::vector<std::pair<uint64_t, double>>{};
        }
        co_return co_await readInStoredOrder(result.outputPath, seriesKey);
    }
};

// ===========================================================================
// A cascade whose aged segment is LARGER THAN ONE SPILL WINDOW.
//
// MERGE_CHUNK_POINTS is 256Ki. Below that the whole fold happens in the single
// tail flush and none of the incremental machinery runs: no mid-fold drain, no
// live bucket carried across a co_await, no stage boundary crossed between two
// drains. This feeds ~400k points spanning BOTH stages plus a raw tail, so the
// coarse stage's buckets are handed to the sink while the fine stage is still
// open.
//
// Two things must hold and neither is checked anywhere else:
//   - the stored order is strictly ascending across the drain boundaries (the
//     streaming writer never sorts);
//   - the values equal an independently computed per-bucket fold.
// ===========================================================================
SEASTAR_TEST_F(DownsampleCascadeAdversarialTest, CascadeSpanningMultipleSpillsStaysOrderedAndExact) {
    const std::string measurement = "scada";
    const std::string seriesKey = "scada|dev=big|value";
    const uint64_t now = nowNs();

    const uint64_t afterFine = 7 * ONE_DAY_NS;
    const uint64_t afterCoarse = 30 * ONE_DAY_NS;
    const uint64_t coarseThreshold = ((now - afterCoarse) / FIFTEEN_MINUTES_NS) * FIFTEEN_MINUTES_NS;
    const uint64_t fineThreshold = ((now - afterFine) / ONE_MINUTE_NS) * ONE_MINUTE_NS;

    // 223 whole 15m buckets of 1 Hz data ending one bucket short of the coarse
    // threshold (guard band: a sub-microsecond difference between the test's
    // clock read and the compactor's cannot move a point across a stage).
    constexpr size_t kCoarseBuckets = 223;
    const uint64_t coarseEnd = coarseThreshold - FIFTEEN_MINUTES_NS;
    const uint64_t coarseStart = coarseEnd - kCoarseBuckets * FIFTEEN_MINUTES_NS;

    // 3334 whole 1m buckets between the thresholds, likewise guarded.
    constexpr size_t kFineBuckets = 3334;
    const uint64_t fineEnd = fineThreshold - ONE_MINUTE_NS;
    const uint64_t fineStart = fineEnd - kFineBuckets * ONE_MINUTE_NS;
    EXPECT_GT(fineStart, coarseThreshold) << "fixture: the fine band must sit above the coarse threshold";

    std::vector<uint64_t> ts;
    std::vector<double> vals;
    std::map<uint64_t, std::pair<double, size_t>> expectedSum;  // bucketStart -> (sum, count)

    auto emit = [&](uint64_t start, size_t seconds, uint64_t bucketWidth, double seed) {
        for (size_t i = 0; i < seconds; ++i) {
            const uint64_t t = start + static_cast<uint64_t>(i) * NS_PER_SEC;
            const double v = seed + static_cast<double>(i % 977) * 0.125;
            ts.push_back(t);
            vals.push_back(v);
            auto& acc = expectedSum[(t / bucketWidth) * bucketWidth];
            acc.first += v;
            acc.second += 1;
        }
    };

    emit(coarseStart, kCoarseBuckets * 900, FIFTEEN_MINUTES_NS, 10.0);
    emit(fineStart, kFineBuckets * 60, ONE_MINUTE_NS, 500.0);
    // Raw tail above the fine threshold.
    const uint64_t rawStart = fineThreshold + ONE_MINUTE_NS;
    for (size_t i = 0; i < 120; ++i) {
        ts.push_back(rawStart + static_cast<uint64_t>(i) * NS_PER_SEC);
        vals.push_back(-1.0 - static_cast<double>(i));
    }

    EXPECT_GT(ts.size(), 256u * 1024u) << "fixture must exceed MERGE_CHUNK_POINTS or no spill occurs";

    auto policy = adversarialPolicy(measurement, {adversarialTier(afterFine, ONE_MINUTE_NS, "avg"),
                                                  adversarialTier(afterCoarse, FIFTEEN_MINUTES_NS, "avg")});
    EXPECT_FALSE(timestar::retention::validateRetentionPolicy(policy).has_value());

    auto out = co_await self->writeAndFold(seriesKey, ts, vals, policy);
    if (out.empty()) {
        ADD_FAILURE() << "fold produced no points";
        co_return;
    }

    // 1. STORED order is strictly ascending. Not a re-sorted view of it.
    for (size_t i = 1; i < out.size(); ++i) {
        EXPECT_LT(out[i - 1].first, out[i].first) << "stored point " << i
                                                  << " is not after its predecessor; the fold handed the writer an "
                                                     "out-of-order chunk across a drain boundary and nothing sorts it";
    }

    size_t coarse = 0, fine = 0, raw = 0;
    for (const auto& [t, v] : out) {
        if (t < coarseThreshold) {
            ++coarse;
            EXPECT_EQ(t % FIFTEEN_MINUTES_NS, 0u) << "coarse point off the 15m grid at " << t;
            auto it = expectedSum.find(t);
            EXPECT_NE(it, expectedSum.end()) << "coarse bucket " << t << " has no raw points behind it";
            if (it != expectedSum.end()) {
                EXPECT_DOUBLE_EQ(v, it->second.first / static_cast<double>(it->second.second)) << "coarse bucket " << t;
            }
        } else if (t < fineThreshold) {
            ++fine;
            EXPECT_EQ(t % ONE_MINUTE_NS, 0u) << "fine point off the 1m grid at " << t;
            auto it = expectedSum.find(t);
            EXPECT_NE(it, expectedSum.end()) << "fine bucket " << t << " has no raw points behind it";
            if (it != expectedSum.end()) {
                EXPECT_DOUBLE_EQ(v, it->second.first / static_cast<double>(it->second.second)) << "fine bucket " << t;
            }
        } else {
            ++raw;
        }
    }

    EXPECT_EQ(coarse, kCoarseBuckets);
    EXPECT_EQ(fine, kFineBuckets);
    EXPECT_EQ(raw, 120u);

    co_return;
}

// ===========================================================================
// NaN and +-Inf at EVERY stage, not just the finest.
//
// Phase 1 suppressed the fabricated all-NaN bucket point at the single fold
// site; Phase 2 rewrote that site into CascadeFolder::closeLiveBucket(). The
// rule has to survive at the COARSE stage too — and a data-derived NaN
// (+Inf + -Inf) must still be emitted, since the suppression is count==0 and
// not isnan(result).
// ===========================================================================
SEASTAR_TEST_F(DownsampleCascadeAdversarialTest, NaNAndInfRulesHoldAtEveryStage) {
    const std::string measurement = "scada";
    const std::string seriesKey = "scada|dev=nan|value";
    const double NaN = std::numeric_limits<double>::quiet_NaN();
    const double Inf = std::numeric_limits<double>::infinity();

    const uint64_t now = nowNs();
    const uint64_t afterFine = 7 * ONE_DAY_NS;
    const uint64_t afterCoarse = 30 * ONE_DAY_NS;
    const uint64_t coarseThreshold = ((now - afterCoarse) / FIFTEEN_MINUTES_NS) * FIFTEEN_MINUTES_NS;
    const uint64_t fineThreshold = ((now - afterFine) / ONE_MINUTE_NS) * ONE_MINUTE_NS;

    // Coarse stage: bucket X all NaN, bucket Y mixed, bucket Z is +Inf/-Inf.
    const uint64_t cZ = coarseThreshold - 2 * FIFTEEN_MINUTES_NS;
    const uint64_t cY = cZ - FIFTEEN_MINUTES_NS;
    const uint64_t cX = cY - FIFTEEN_MINUTES_NS;
    // Fine stage: bucket P all NaN, bucket Q mixed.
    const uint64_t fQ = fineThreshold - 2 * ONE_MINUTE_NS;
    const uint64_t fP = fQ - ONE_MINUTE_NS;

    std::vector<uint64_t> ts{
        cX, cX + NS_PER_SEC, cX + 2 * NS_PER_SEC,  // all NaN
        cY, cY + NS_PER_SEC, cY + 2 * NS_PER_SEC,  // 2.0, NaN, 4.0 -> 3.0
        cZ, cZ + NS_PER_SEC,                       // +Inf, -Inf -> NaN, EMITTED
        fP, fP + NS_PER_SEC,                       // all NaN
        fQ, fQ + NS_PER_SEC,                       // 8.0, NaN -> 8.0
    };
    std::vector<double> vals{
        NaN, NaN, NaN, 2.0, NaN, 4.0, Inf, -Inf, NaN, NaN, 8.0, NaN,
    };

    auto policy = adversarialPolicy(measurement, {adversarialTier(afterFine, ONE_MINUTE_NS, "avg"),
                                                  adversarialTier(afterCoarse, FIFTEEN_MINUTES_NS, "avg")});
    auto out = co_await self->writeAndFold(seriesKey, ts, vals, policy);

    std::map<uint64_t, double> byTs(out.begin(), out.end());
    EXPECT_EQ(byTs.count(cX), 0u) << "all-NaN bucket at the COARSE stage emitted a fabricated point";
    EXPECT_EQ(byTs.count(fP), 0u) << "all-NaN bucket at the FINE stage emitted a fabricated point";
    EXPECT_EQ(byTs.count(cY), 1u);
    if (byTs.count(cY)) {
        EXPECT_DOUBLE_EQ(byTs.at(cY), 3.0);
    }
    EXPECT_EQ(byTs.count(fQ), 1u);
    if (byTs.count(fQ)) {
        EXPECT_DOUBLE_EQ(byTs.at(fQ), 8.0);
    }
    EXPECT_EQ(byTs.count(cZ), 1u) << "a data-derived NaN (+Inf + -Inf) is the correct IEEE aggregate and must "
                                     "still be emitted";
    if (byTs.count(cZ)) {
        EXPECT_TRUE(std::isnan(byTs.at(cZ)));
    }
    EXPECT_EQ(out.size(), 3u) << "expected exactly: coarse mixed, coarse Inf-Inf, fine mixed";

    co_return;
}

// ===========================================================================
// A PERSISTED policy that would fail validation must NOT fold.
//
// Persisted records never pass through the HTTP validator: they are read
// straight out of index prefix 0x0B, and can predate a rule, be hand-edited, or
// be written by a tool. The compactor used to map any unrecognised method
// string onto AVG, so a typo silently averaged a totalizer the operator asked
// to keep as `max` — destructive and invisible. Refusing to fold is the only
// non-destructive failure mode.
//
// The other half of the contract, which a blanket "skip the whole policy" fix
// would break: TTL must STILL be applied. Nothing about a bad downsample method
// makes expired points worth keeping.
// ===========================================================================
SEASTAR_TEST_F(DownsampleCascadeAdversarialTest, InvalidPersistedPolicyRefusesToFoldButStillAppliesTtl) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t ttlNanos = 100 * ONE_DAY_NS;

    // One minute's worth of aged points that a working fold would collapse to a
    // single bucket, plus expired points and a recent one.
    const uint64_t agedBase = ((now - 60 * ONE_DAY_NS) / ONE_MINUTE_NS) * ONE_MINUTE_NS;
    std::vector<uint64_t> ts{
        now - 150 * ONE_DAY_NS,
        now - 149 * ONE_DAY_NS,  // expired by TTL
        agedBase,
        agedBase + NS_PER_SEC,
        agedBase + 2 * NS_PER_SEC,
        now - 3600 * NS_PER_SEC,
    };
    std::vector<double> vals{1.0, 2.0, 10.0, 20.0, 30.0, 99.0};

    auto makePolicy = [&](const std::string& method) {
        auto p = adversarialPolicy(measurement, {adversarialTier(30 * ONE_DAY_NS, ONE_MINUTE_NS, method)});
        p.ttlNanos = ttlNanos;
        p.ttl = std::to_string(ttlNanos) + "ns";
        return p;
    };

    // Control: a VALID policy folds the aged minute to one bucket.
    auto valid = makePolicy("avg");
    EXPECT_FALSE(timestar::retention::validateRetentionPolicy(valid).has_value());
    auto foldedOut = co_await self->writeAndFold("scada|dev=ok|value", ts, vals, valid);
    EXPECT_EQ(foldedOut.size(), 2u) << "control: valid policy must fold the aged minute and drop the expired points";

    // A method string this build does not recognise.
    auto bogus = makePolicy("maximum");
    EXPECT_TRUE(timestar::retention::validateRetentionPolicy(bogus).has_value());
    auto out = co_await self->writeAndFold("scada|dev=bad|value", ts, vals, bogus);

    std::map<uint64_t, double> byTs(out.begin(), out.end());
    EXPECT_EQ(byTs.count(now - 150 * ONE_DAY_NS), 0u) << "an unusable downsample clause must not disable TTL";
    EXPECT_EQ(byTs.count(now - 149 * ONE_DAY_NS), 0u);
    EXPECT_EQ(out.size(), 4u) << "the aged points must survive at FULL resolution: a policy that would not "
                                 "validate must not fold at all, and must certainly not fall back to avg";
    EXPECT_EQ(byTs.count(agedBase + NS_PER_SEC), 1u) << "aged points were folded under an invalid policy";
    if (byTs.count(agedBase)) {
        EXPECT_DOUBLE_EQ(byTs.at(agedBase), 10.0) << "value at the bucket start is an AVG of the bucket, i.e. the "
                                                     "unrecognised method silently defaulted to avg";
    }

    co_return;
}

// ===========================================================================
// A stage band holding ZERO points, between two populated bands.
//
// The folder closes a stage when a point of a FINER stage arrives. If a whole
// stage is skipped (no point ever lands in it), that transition happens across
// two stage indices at once — and the coarse bucket must still close before the
// fine one opens.
// ===========================================================================
SEASTAR_TEST_F(DownsampleCascadeAdversarialTest, EmptyMiddleStageDoesNotDisturbNeighbours) {
    const std::string measurement = "scada";
    const std::string seriesKey = "scada|dev=hole|value";

    const uint64_t now = nowNs();
    const uint64_t afterA = 1 * ONE_DAY_NS;   // 1m
    const uint64_t afterB = 7 * ONE_DAY_NS;   // 15m
    const uint64_t afterC = 30 * ONE_DAY_NS;  // 60m
    const uint64_t tA = ((now - afterA) / ONE_MINUTE_NS) * ONE_MINUTE_NS;
    const uint64_t tC = ((now - afterC) / (60 * ONE_MINUTE_NS)) * (60 * ONE_MINUTE_NS);

    std::vector<uint64_t> ts;
    std::vector<double> vals;
    double v = 0.0;
    // Two whole 60m buckets in the coarsest band...
    for (size_t i = 0; i < 2 * 60; ++i) {
        ts.push_back(tC - 2 * 60 * ONE_MINUTE_NS + static_cast<uint64_t>(i) * ONE_MINUTE_NS);
        vals.push_back(v += 1.0);
    }
    // ... nothing at all in the 15m band ...
    // ... and two whole 1m buckets in the finest band.
    for (size_t i = 0; i < 120; ++i) {
        ts.push_back(tA - 2 * ONE_MINUTE_NS + static_cast<uint64_t>(i) * NS_PER_SEC);
        vals.push_back(v += 1.0);
    }

    auto policy = adversarialPolicy(
        measurement, {adversarialTier(afterA, ONE_MINUTE_NS, "max"), adversarialTier(afterB, FIFTEEN_MINUTES_NS, "max"),
                      adversarialTier(afterC, 60 * ONE_MINUTE_NS, "max")});
    EXPECT_FALSE(timestar::retention::validateRetentionPolicy(policy).has_value());

    auto out = co_await self->writeAndFold(seriesKey, ts, vals, policy);
    EXPECT_EQ(out.size(), 4u) << "expected two 60m buckets and two 1m buckets";
    if (out.size() != 4u) {
        co_return;
    }
    for (size_t i = 1; i < out.size(); ++i) {
        EXPECT_LT(out[i - 1].first, out[i].first) << "output not ascending across an empty stage";
    }
    EXPECT_EQ(out[0].first % (60 * ONE_MINUTE_NS), 0u);
    EXPECT_EQ(out[1].first % (60 * ONE_MINUTE_NS), 0u);
    EXPECT_EQ(out[2].first % ONE_MINUTE_NS, 0u);
    EXPECT_EQ(out[3].first % ONE_MINUTE_NS, 0u);
    // max composes: the last value of each bucket is its max here.
    EXPECT_DOUBLE_EQ(out[0].second, 60.0);
    EXPECT_DOUBLE_EQ(out[1].second, 120.0);
    EXPECT_DOUBLE_EQ(out[2].second, 180.0);
    EXPECT_DOUBLE_EQ(out[3].second, 240.0);

    co_return;
}
