// Age-driven downsample trigger (Engine::sweepDownsampleRewrites).
//
// THE PROBLEM THIS EXISTS FOR: the fold otherwise runs only when a tier merge
// happens, and a merge needs files_per_merge files to accumulate. A SCADA tag
// that stops receiving writes — a decommissioned RTU — generates no new files,
// is never re-compacted, and therefore stays at 1 Hz forever no matter how old
// its data becomes. AGE must be able to initiate the work on its own.
//
// These tests enter at the ENGINE, not at the heuristic. A unit test of the
// density arithmetic would prove nothing about whether a quiet series actually
// reaches 1m and then 15m resolution, which is the entire point of the phase.
// So they write real points through Engine::insert(), roll them over into real
// TSM files, install a policy the way PUT /retention does, drive real sweeps,
// and read the answer back through Engine::query().

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/later.hh>
#include <set>
#include <string>
#include <vector>

class DownsampleSweepTest : public ::testing::Test {
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

// Roll the memory store over and wait for the background WAL->TSM conversion to
// register its file, so the caller ends up with a known tier-0 file count.
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

// Identity of the shard's file set. A rewrite always allocates a fresh sequence
// number, so an unchanged set is proof that NO file was rewritten — which is
// what the no-re-churn gate needs to assert, and it cannot be faked by a
// rewrite that happens to produce identical bytes.
std::set<std::string> fileIdentities(Engine& engine) {
    std::set<std::string> ids;
    for (const auto& [rank, file] : engine.getTSMFileManager().getSequencedTsmFiles()) {
        ids.insert(file->getFilePath());
    }
    return ids;
}

// A cascade whose stage-1 threshold is far enough in the past that no data in
// these tests reaches it. Present so every policy below exercises the real
// multi-tier path rather than the single-tier special case.
DownsamplePolicy tier(const char* after, uint64_t afterNanos, const char* interval, uint64_t intervalNanos) {
    DownsamplePolicy t;
    t.after = after;
    t.afterNanos = afterNanos;
    t.interval = interval;
    t.intervalNanos = intervalNanos;
    t.method = "avg";
    return t;
}

void installCascade(Engine& engine, const std::string& measurement, std::vector<DownsamplePolicy> tiers) {
    RetentionPolicy policy;
    policy.measurement = measurement;
    policy.downsampleTiers = std::move(tiers);
    // Exactly what PUT /retention does: normalize, then broadcast into every
    // shard's cache.
    engine.updateRetentionPolicyCache(policy);
}

// Points actually stored for a series, read back the way a client would.
seastar::future<std::vector<uint64_t>> storedTimestamps(Engine& engine, const std::string& seriesKey) {
    auto resultOpt = co_await engine.query(seriesKey, 0, UINT64_MAX);
    if (!resultOpt.has_value()) {
        co_return std::vector<uint64_t>{};
    }
    co_return std::get<QueryResult<double>>(resultOpt.value()).timestamps;
}

}  // namespace

// ---------------------------------------------------------------------------
// THE POINT OF THE PHASE.
//
// A series that receives NO further writes must still cascade: 1 Hz -> 1m once
// the first threshold passes it, then 1m -> 15m once the second does. Nothing
// in this test ever inserts again after the initial write, and only ONE tier-0
// file exists, so a tier merge can never run (files_per_merge is 4). The sweep
// is the only thing that can move this data.
//
// "Within two sweep periods of crossing each threshold" is asserted as: the
// FIRST sweep after the threshold moves the data. Wall-clock advance is
// simulated by shortening the policy's `after` values — the fold's thresholds
// are `now - after`, so the two are interchangeable and this keeps the test
// deterministic instead of sleeping for hours.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(DownsampleSweepTest, QuietSeriesCascadesToOneMinuteThenFifteenMinutes) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const std::string measurement = "quiet_rtu";
        const uint64_t now = nowNanos();

        // 30 minutes of 1 Hz data that STOPPED six hours ago. Anchored to a 15m
        // bucket boundary so the expected bucket counts are exact.
        const uint64_t dataEnd = ((now - 6 * kHour) / (15 * kMin)) * (15 * kMin);
        const uint64_t dataStart = dataEnd - 30 * kMin;
        constexpr size_t kRawPoints = 30 * 60;  // 1 Hz for 30 minutes

        TimeStarInsert<double> insert(measurement, "value");
        insert.addTag("rtu", "decommissioned-01");
        for (size_t i = 0; i < kRawPoints; ++i) {
            insert.addValue(dataStart + i * kSec, static_cast<double>(i));
        }
        const std::string seriesKey = insert.seriesKey();
        co_await engine.insert(std::move(insert));

        if (!co_await rolloverAndAwaitTsmFile(engine)) {
            throw std::runtime_error("WAL->TSM conversion did not complete");
        }

        // ONE tier-0 file: a merge is structurally impossible from here.
        EXPECT_EQ(engine.getTSMFileManager().getFileCountInTier(0), 1u)
            << "the premise of this test is that no merge can run";

        auto raw = co_await storedTimestamps(engine, seriesKey);
        EXPECT_EQ(raw.size(), kRawPoints) << "raw data did not land intact";

        // --- Threshold 1 crosses: 1m stage now covers the whole series. ---
        //
        // after=5h with data ending 6h ago: the aligned threshold sits between
        // now-5h1m and now-5h, comfortably newer than every stored point.
        // The 15m stage's `after` of 30 days is unreachable for this data.
        installCascade(engine, measurement,
                       {tier("5h", 5 * kHour, "1m", kMin), tier("30d", 30 * kDay, "15m", 15 * kMin)});

        co_await engine.sweepDownsampleRewrites();

        auto folded1m = co_await storedTimestamps(engine, seriesKey);
        EXPECT_EQ(folded1m.size(), 30u) << "a quiet series did not reach 1m resolution: the age-driven trigger is not "
                                           "initiating work, so a decommissioned tag stays at 1 Hz forever";
        for (auto ts : folded1m) {
            EXPECT_EQ(ts % kMin, 0u) << "folded point " << ts << " is not on a 1m bucket boundary";
        }
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 1u);

        // --- Threshold 2 crosses: the 15m stage now covers the whole series. ---
        installCascade(engine, measurement,
                       {tier("5h", 5 * kHour, "1m", kMin), tier("5h30m", 5 * kHour + 30 * kMin, "15m", 15 * kMin)});

        co_await engine.sweepDownsampleRewrites();

        auto folded15m = co_await storedTimestamps(engine, seriesKey);
        EXPECT_EQ(folded15m.size(), 2u) << "the cascade stalled at 1m: the second stage's threshold passed but the "
                                           "density heuristic never re-proposed the file";
        for (auto ts : folded15m) {
            EXPECT_EQ(ts % (15 * kMin), 0u) << "folded point " << ts << " is not on a 15m bucket boundary";
        }
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 2u);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ---------------------------------------------------------------------------
// THE TIGHTEST LEGAL CASCADE STEP MUST STILL FIRE.
//
// Validation admits any cascade whose `interval[k+1]` is an exact multiple of
// `interval[k]`, so the SMALLEST legal step is exactly 2x (1m -> 2m, 15m -> 30m,
// 1h -> 2h). The hysteresis factor has to admit that step, or a policy the API
// accepts silently never advances past its first tier.
//
// The achievable ratio is strictly BELOW the interval step, because the
// estimate counts BUCKETS, not intervals:
//
//     pointCount   = m + 1        (a series folded to interval[k], m spans)
//     foldedPoints = m/r + 1      (r = interval[k+1] / interval[k])
//     ratio        = (m + 1) / (floor(m/r) + 1)  ->  r only as m -> infinity
//
// For r == 2 and an EVEN m the ratio is 2 - 1/(m/2 + 1) < 2 for every m, so a
// factor of exactly 2.0 rejects the step forever: the file is quiet, m never
// changes, and no later sweep can produce a different answer. This test pins a
// concrete instance of that (m = 30, ratio = 31/16 = 1.9375).
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(DownsampleSweepTest, TwoTimesCascadeStepIsAdmittedByTheHysteresisFactor) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const std::string measurement = "two_x_step";
        const uint64_t now = nowNanos();

        // 31 minutes of 1 Hz data that stopped six hours ago. 31 minutes is
        // chosen so the 1m fold leaves 31 points spanning 30 minutes — an EVEN
        // number of 1m spans, which is the case a factor of 2.0 cannot admit.
        const uint64_t dataEnd = ((now - 6 * kHour) / (15 * kMin)) * (15 * kMin);
        const uint64_t dataStart = dataEnd - 31 * kMin;

        TimeStarInsert<double> insert(measurement, "value");
        insert.addTag("rtu", "r1");
        for (size_t i = 0; i < 31 * 60; ++i) {
            insert.addValue(dataStart + i * kSec, static_cast<double>(i));
        }
        const std::string seriesKey = insert.seriesKey();
        co_await engine.insert(std::move(insert));
        if (!co_await rolloverAndAwaitTsmFile(engine)) {
            throw std::runtime_error("WAL->TSM conversion did not complete");
        }
        EXPECT_EQ(engine.getTSMFileManager().getFileCountInTier(0), 1u)
            << "the premise of this test is that no merge can run";

        // Stage 1 only: the 2m tier's `after` is far out of reach.
        installCascade(engine, measurement,
                       {tier("5h", 5 * kHour, "1m", kMin), tier("30d", 30 * kDay, "2m", 2 * kMin)});
        co_await engine.sweepDownsampleRewrites();

        auto folded1m = co_await storedTimestamps(engine, seriesKey);
        // Fatal: the second stage's arithmetic is only the case under review if
        // the first stage produced exactly 31 points across 30 one-minute spans.
        if (folded1m.size() != 31u || folded1m.back() - folded1m.front() != 30 * kMin) {
            throw std::runtime_error("premise broken: 1m fold produced " + std::to_string(folded1m.size()) +
                                     " points, expected 31 spanning 30m");
        }

        // Stage 2 now covers the whole (quiet, unchanging) series. r == 2, so
        // the estimated ratio is 31/16 = 1.9375: below a factor of 2.0 and
        // above every value a correctly folded series can reach (<= 1.0).
        installCascade(engine, measurement,
                       {tier("5h", 5 * kHour, "1m", kMin), tier("5h30m", 5 * kHour + 30 * kMin, "2m", 2 * kMin)});
        co_await engine.sweepDownsampleRewrites();

        auto folded2m = co_await storedTimestamps(engine, seriesKey);
        EXPECT_EQ(folded2m.size(), 16u)
            << "a 2x cascade step never fired: the hysteresis factor is set at or above the largest ratio a 2x step "
               "can produce, so this quiet series stays at 1m resolution forever even though the policy asks for 2m";
        for (auto ts : folded2m) {
            EXPECT_EQ(ts % (2 * kMin), 0u) << "folded point " << ts << " is not on a 2m bucket boundary";
        }
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 2u);

        // ...and it still does not re-churn afterwards.
        const auto after = fileIdentities(engine);
        co_await engine.sweepDownsampleRewrites();
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 2u) << "the 2m-folded file was re-proposed";
        EXPECT_EQ(fileIdentities(engine), after);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ---------------------------------------------------------------------------
// NO RE-CHURN.
//
// The sweep carries no persisted watermark — nothing records that a series has
// been folded through stage k. The ONLY thing stopping it from rewriting the
// same file every 15 minutes forever is that a correctly folded series' stored
// density has dropped to ~1 point per bucket, below the hysteresis factor.
//
// Asserted across SEVERAL sweeps, not one: a heuristic that is merely slow to
// re-trigger (say, one that prorates a partially-aged series by assuming
// uniform density) still churns, just not on the very next sweep.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(DownsampleSweepTest, AlreadyFoldedFileIsNotRewrittenAgain) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const std::string measurement = "no_rechurn";
        const uint64_t now = nowNanos();
        const uint64_t dataEnd = ((now - 6 * kHour) / (15 * kMin)) * (15 * kMin);
        const uint64_t dataStart = dataEnd - 30 * kMin;

        TimeStarInsert<double> insert(measurement, "value");
        insert.addTag("rtu", "r1");
        for (size_t i = 0; i < 30 * 60; ++i) {
            insert.addValue(dataStart + i * kSec, static_cast<double>(i));
        }
        const std::string seriesKey = insert.seriesKey();
        co_await engine.insert(std::move(insert));
        if (!co_await rolloverAndAwaitTsmFile(engine)) {
            throw std::runtime_error("WAL->TSM conversion did not complete");
        }

        installCascade(engine, measurement,
                       {tier("5h", 5 * kHour, "1m", kMin), tier("30d", 30 * kDay, "15m", 15 * kMin)});

        // First sweep folds.
        co_await engine.sweepDownsampleRewrites();
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 1u) << "the first sweep did not fold";
        const auto afterFold = fileIdentities(engine);
        const auto foldedPoints = co_await storedTimestamps(engine, seriesKey);
        EXPECT_EQ(foldedPoints.size(), 30u);

        // Every subsequent sweep must find nothing to do. The file set is the
        // proof: a rewrite always allocates a new sequence number.
        for (int sweep = 0; sweep < 5; ++sweep) {
            co_await engine.sweepDownsampleRewrites();
            EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 1u)
                << "sweep " << sweep
                << " re-proposed an already-folded file: the density heuristic does not hold, so "
                   "this file would be rewritten every sweep period forever";
            EXPECT_EQ(engine.getDownsampleSweepStats().candidateFiles, 1u)
                << "sweep " << sweep << " produced a candidate from an already-folded file";
            EXPECT_EQ(fileIdentities(engine), afterFold) << "sweep " << sweep << " rewrote the file";
        }

        // ...and the data is still exactly what the first fold produced.
        EXPECT_EQ(co_await storedTimestamps(engine, seriesKey), foldedPoints);

        // The sweep DID keep looking — this is not a false pass from an early
        // return. Six sweeps ran, each walking the shard's one file.
        EXPECT_EQ(engine.getDownsampleSweepStats().sweeps, 6u);
        EXPECT_EQ(engine.getDownsampleSweepStats().filesExamined, 6u);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ---------------------------------------------------------------------------
// A sweep with no policies installed does ZERO work.
//
// This stage runs on every shard every 15 minutes forever, and most deployments
// never install a retention policy at all. "Found no candidates" and "never
// looked" are indistinguishable from the outside without a counter, so the two
// counters that would be non-zero if it had looked are asserted directly:
// seriesEnumerations is the sweep's only index read, filesExamined its only
// per-series work.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(DownsampleSweepTest, NoPolicyMeansNoScanningAndNoIndexReads) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const uint64_t now = nowNanos();
        const uint64_t dataEnd = ((now - 6 * kHour) / (15 * kMin)) * (15 * kMin);

        // Real, aged, dense data — every ingredient a candidate needs EXCEPT a
        // policy. If the sweep scanned at all it would find this.
        TimeStarInsert<double> insert("unpoliced", "value");
        insert.addTag("rtu", "r1");
        for (size_t i = 0; i < 600; ++i) {
            insert.addValue(dataEnd - 600 * kSec + i * kSec, static_cast<double>(i));
        }
        co_await engine.insert(std::move(insert));
        if (!co_await rolloverAndAwaitTsmFile(engine)) {
            throw std::runtime_error("WAL->TSM conversion did not complete");
        }

        // 1) No policies at all.
        co_await engine.sweepDownsampleRewrites();
        EXPECT_EQ(engine.getDownsampleSweepStats().seriesEnumerations, 0u) << "the sweep read the index with no policy";
        EXPECT_EQ(engine.getDownsampleSweepStats().filesExamined, 0u) << "the sweep scanned files with no policy";
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 0u);

        // 2) A TTL-ONLY policy: actionable for the expiry stage, but it names no
        //    downsample tier, so this stage still has nothing to do and must not
        //    pay for an index scan to discover that.
        RetentionPolicy ttlOnly;
        ttlOnly.measurement = "unpoliced";
        ttlOnly.ttl = "365d";
        ttlOnly.ttlNanos = 365 * kDay;
        engine.updateRetentionPolicyCache(ttlOnly);

        co_await engine.sweepDownsampleRewrites();
        EXPECT_EQ(engine.getDownsampleSweepStats().seriesEnumerations, 0u)
            << "a TTL-only policy made the downsample stage read the index";
        EXPECT_EQ(engine.getDownsampleSweepStats().filesExamined, 0u);

        // 3) An INVALID cascade (interval[1] is not a multiple of interval[0],
        //    so no stage-k bucket lies wholly inside a stage-k+1 bucket). The
        //    compactor refuses to fold it, so proposing a rewrite would rewrite
        //    the file every sweep forever for no effect.
        RetentionPolicy invalid;
        invalid.measurement = "unpoliced";
        invalid.downsampleTiers = {tier("5h", 5 * kHour, "1m", kMin), tier("6h", 6 * kHour, "90s", 90 * kSec)};
        engine.updateRetentionPolicyCache(invalid);

        co_await engine.sweepDownsampleRewrites();
        EXPECT_EQ(engine.getDownsampleSweepStats().seriesEnumerations, 0u)
            << "an unfoldable policy made the sweep look for candidates";
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 0u)
            << "an invalid policy produced a rewrite: it cannot fold, so this would loop forever";

        // The stage really did run each time — it just did nothing.
        EXPECT_EQ(engine.getDownsampleSweepStats().sweeps, 3u);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ---------------------------------------------------------------------------
// The per-sweep rewrite cap is respected.
//
// The sweep must never convert "a shard with a lot of aged data" into an
// unbounded burst of compactions competing with tier merges and WAL->TSM
// conversion. It takes at most engine.max_downsample_rewrites_per_sweep files
// per pass and leaves the rest for the next one — and it spends those slots on
// the biggest reductions first.
//
// Nothing here changes the compaction scheduling group: a rewrite goes through
// executeCompaction exactly as a tier merge does, so it inherits the same
// shares. The cap is what bounds it, and hasCompactionCapacity() (checked
// before every rewrite) is what keeps it from queueing in front of a merge.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(DownsampleSweepTest, RewriteCapBoundsOneSweep) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const size_t cap = timestar::config().engine.max_downsample_rewrites_per_sweep;
        const size_t fileCount = cap + 1;
        // Must stay below files_per_merge, or the background tier-0 loop merges
        // these files and folds them itself — which would prove nothing about
        // the cap.
        if (cap == 0 || fileCount >= timestar::config().storage.compaction.files_per_merge) {
            throw std::runtime_error("test premise broken: cap must be > 0 and cap+1 < files_per_merge");
        }

        const std::string measurement = "capped";
        const uint64_t now = nowNanos();
        const uint64_t dataEnd = ((now - 6 * kHour) / (15 * kMin)) * (15 * kMin);

        // One dense aged series per file, each in its own rollover so each lands
        // in a distinct tier-0 file. Sizes differ so the "biggest reduction
        // first" ordering has something to order.
        std::vector<std::string> keys;
        for (size_t f = 0; f < fileCount; ++f) {
            TimeStarInsert<double> insert(measurement, "value");
            insert.addTag("rtu", "r" + std::to_string(f));
            const size_t points = 600 * (f + 1);
            for (size_t i = 0; i < points; ++i) {
                insert.addValue(dataEnd - points * kSec + i * kSec, static_cast<double>(i));
            }
            keys.push_back(insert.seriesKey());
            co_await engine.insert(std::move(insert));
            if (!co_await rolloverAndAwaitTsmFile(engine)) {
                throw std::runtime_error("WAL->TSM conversion did not complete");
            }
        }
        EXPECT_EQ(engine.getTSMFileManager().getFileCountInTier(0), fileCount);

        installCascade(engine, measurement,
                       {tier("5h", 5 * kHour, "1m", kMin), tier("30d", 30 * kDay, "15m", 15 * kMin)});

        co_await engine.sweepDownsampleRewrites();

        EXPECT_EQ(engine.getDownsampleSweepStats().candidateFiles, fileCount)
            << "every file holds aged 1 Hz data and should have qualified";
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, cap)
            << "one sweep exceeded engine.max_downsample_rewrites_per_sweep";

        // The largest series is the biggest reduction, so it must be among the
        // files the capped sweep chose.
        auto largest = co_await storedTimestamps(engine, keys.back());
        EXPECT_EQ(largest.size(), (600 * fileCount) / 60u)
            << "the capped sweep did not spend its slots on the biggest reduction first";

        // The remainder is picked up by the NEXT sweep, not dropped.
        co_await engine.sweepDownsampleRewrites();
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, cap + 1)
            << "the file left over by the cap was never revisited";
        for (size_t f = 0; f < fileCount; ++f) {
            auto ts = co_await storedTimestamps(engine, keys[f]);
            EXPECT_EQ(ts.size(), (600 * (f + 1)) / 60u) << "series " << f << " did not reach 1m resolution";
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
// A series whose data STRADDLES the threshold is left to the merge path.
//
// The estimate is only exact for a series lying wholly older than a stage
// threshold, because TSM files are immutable and so "wholly aged" is a stable
// predicate: every point folds at the same stage and the post-fold count is
// exactly the number of occupied buckets.
//
// Prorating a straddling series by assuming uniform density is precisely wrong
// for the file this sweep itself produces — a file that is half folded and half
// raw reads ~30x too dense under that assumption and gets rewritten on every
// sweep forever. This test pins the conservative choice.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(DownsampleSweepTest, PartiallyAgedSeriesFoldsWithoutChurn) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const std::string measurement = "straddler";
        const uint64_t now = nowNanos();

        // 1 Hz data running from 6 hours ago right up to a minute ago, so the
        // 5h threshold falls in the middle of it.
        TimeStarInsert<double> insert(measurement, "value");
        insert.addTag("rtu", "r1");
        const uint64_t start = now - 6 * kHour;
        for (size_t i = 0; i < 3000; ++i) {
            // Spread 3000 points evenly across the whole 6-hour window.
            insert.addValue(start + i * (6 * kHour / 3000), static_cast<double>(i));
        }
        const std::string seriesKey = insert.seriesKey();
        co_await engine.insert(std::move(insert));
        if (!co_await rolloverAndAwaitTsmFile(engine)) {
            throw std::runtime_error("WAL->TSM conversion did not complete");
        }

        const auto before = co_await storedTimestamps(engine, seriesKey);

        installCascade(engine, measurement,
                       {tier("5h", 5 * kHour, "1m", kMin), tier("30d", 30 * kDay, "15m", 15 * kMin)});

        co_await engine.sweepDownsampleRewrites();

        EXPECT_EQ(engine.getDownsampleSweepStats().filesExamined, 1u) << "the sweep should have looked";
        EXPECT_EQ(engine.getDownsampleSweepStats().candidateFiles, 1u);
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 1u);
        auto after = co_await storedTimestamps(engine, seriesKey);
        EXPECT_LT(after.size(), before.size());
        const auto identities = fileIdentities(engine);
        co_await engine.sweepDownsampleRewrites();
        EXPECT_EQ(fileIdentities(engine), identities);
        EXPECT_EQ(co_await storedTimestamps(engine, seriesKey), after);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

// ---------------------------------------------------------------------------
// Non-numeric series are never proposed.
//
// The fold is numeric-only (Boolean and String pass through unfolded in v1), so
// a rewrite driven by a Boolean series would return the file unchanged and the
// sweep would propose it again next period — an endless loop for zero effect.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(DownsampleSweepTest, NonNumericSeriesIsNotACandidate) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();

        const std::string measurement = "status_words";
        const uint64_t now = nowNanos();
        const uint64_t dataEnd = ((now - 6 * kHour) / (15 * kMin)) * (15 * kMin);

        TimeStarInsert<bool> insert(measurement, "state");
        insert.addTag("rtu", "r1");
        for (size_t i = 0; i < 1800; ++i) {
            insert.addValue(dataEnd - 1800 * kSec + i * kSec, (i % 2) == 0);
        }
        co_await engine.insert(std::move(insert));
        if (!co_await rolloverAndAwaitTsmFile(engine)) {
            throw std::runtime_error("WAL->TSM conversion did not complete");
        }

        installCascade(engine, measurement,
                       {tier("5h", 5 * kHour, "1m", kMin), tier("30d", 30 * kDay, "15m", 15 * kMin)});

        co_await engine.sweepDownsampleRewrites();

        EXPECT_EQ(engine.getDownsampleSweepStats().filesExamined, 1u);
        EXPECT_EQ(engine.getDownsampleSweepStats().candidateFiles, 0u)
            << "a Boolean series was proposed for a fold that cannot touch it";
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 0u);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

SEASTAR_TEST_F(DownsampleSweepTest, MinuteHistoryCrossesWeekTierWithoutWaitingForNewestPoint) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();
        const uint64_t now = nowNanos();
        const uint64_t start = ((now - 8 * kDay) / (15 * kMin)) * (15 * kMin);
        TimeStarInsert<double> insert("week_boundary", "value");
        for (size_t i = 0; i < 7 * 24 * 60; ++i)
            insert.addValue(start + i * kMin, 1.0);
        auto key = insert.seriesKey();
        co_await engine.insert(std::move(insert));
        if (!co_await rolloverAndAwaitTsmFile(engine))
            throw std::runtime_error("Conversion timed out");
        installCascade(engine, "week_boundary",
                       {tier("1h", kHour, "1m", kMin), tier("7d", 7 * kDay, "15m", 15 * kMin)});
        auto before = co_await storedTimestamps(engine, key);
        co_await engine.sweepDownsampleRewrites();
        auto after = co_await storedTimestamps(engine, key);
        EXPECT_LT(after.size(), before.size() - 1000);
        EXPECT_EQ(after.back(), before.back());
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 1u);
        auto files = fileIdentities(engine);
        co_await engine.sweepDownsampleRewrites();
        EXPECT_EQ(fileIdentities(engine), files);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure)
        std::rethrow_exception(failure);
}

SEASTAR_TEST_F(DownsampleSweepTest, EngineQueriesCombineIndependentlyRewrittenFiles) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();
        const uint64_t base = ((nowNanos() - 2 * kHour) / kMin) * kMin;
        std::string key;
        for (size_t part = 0; part < 2; ++part) {
            TimeStarInsert<double> insert("split_minute", "value");
            for (size_t i = 0; i < 30; ++i)
                insert.addValue(base + (part * 30 + i) * kSec, 1.0);
            key = insert.seriesKey();
            co_await engine.insert(std::move(insert));
            if (!co_await rolloverAndAwaitTsmFile(engine))
                throw std::runtime_error("Conversion timed out");
        }
        RetentionPolicy policy;
        policy.measurement = "split_minute";
        auto stage = tier("1h", kHour, "1m", kMin);
        stage.method = "sum";
        policy.downsampleTiers = {stage};
        engine.updateRetentionPolicyCache(policy);
        co_await engine.sweepDownsampleRewrites();
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 2u);
        auto result = co_await engine.query(key, 0, UINT64_MAX);
        EXPECT_TRUE(result.has_value());
        if (result)
            EXPECT_EQ(std::get<QueryResult<double>>(*result).values, std::vector<double>{60});
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure)
        std::rethrow_exception(failure);
}

SEASTAR_TEST_F(DownsampleSweepTest, LateStatusInMemoryDoesNotOverrideNewerFoldedStatus) {
    Engine engine;
    std::exception_ptr failure;
    try {
        co_await engine.init();
        const uint64_t base = ((nowNanos() - 2 * kHour) / kMin) * kMin;
        TimeStarInsert<bool> insert("late_status", "status");
        insert.addValue(base, false);
        insert.addValue(base + 59 * kSec, true);
        auto key = insert.seriesKey();
        co_await engine.insert(std::move(insert));
        if (!co_await rolloverAndAwaitTsmFile(engine))
            throw std::runtime_error("Conversion timed out");
        RetentionPolicy policy;
        policy.measurement = "late_status";
        auto stage = tier("1h", kHour, "1m", kMin);
        stage.method = "latest";
        stage.fieldMethods = std::map<std::string, std::string>{{"status", "latest"}};
        policy.downsampleTiers = {stage};
        engine.updateRetentionPolicyCache(policy);
        co_await engine.sweepDownsampleRewrites();
        EXPECT_EQ(engine.getDownsampleSweepStats().rewrites, 1u);
        TimeStarInsert<bool> late("late_status", "status");
        late.addValue(base + 30 * kSec, false);
        co_await engine.insert(std::move(late));
        auto result = co_await engine.query(key, 0, UINT64_MAX);
        EXPECT_TRUE(result.has_value());
        if (result)
            EXPECT_EQ(std::get<QueryResult<bool>>(*result).values, std::vector<bool>{true});
    } catch (...) {
        failure = std::current_exception();
    }
    co_await engine.stop();
    if (failure)
        std::rethrow_exception(failure);
}
