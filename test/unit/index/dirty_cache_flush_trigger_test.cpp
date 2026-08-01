// Regression gate for UNBOUNDED DIRTY INDEX CACHES (write-scaleout debt D-17,
// the level below the closed D-3).
//
// WHAT WAS WRONG. `NativeIndex` keeps four application caches that are mutated
// WITHOUT writing a KV entry: postings bitmaps, per-day bitmaps, HLL sketches
// and per-measurement blooms. `flushMemTable()` and `maybeFlushMemTable()` were
// the only two things that ever drained them, and `maybeFlushMemTable()` returns
// immediately unless the MEMTABLE crossed its threshold. So the trigger measured
// the one thing that was not accumulating:
//
//   * A day rollover adds real membership bits to brand-new day bitmaps while
//     creating no series and writing nothing to the memtable at all.
//   * Steady state on a fixed fleet re-touches known series, which does not grow
//     the memtable either.
//
// The result was dirty state with NO bound of any kind — not in bytes (the
// trims deliberately skip dirty entries, so the byte budgets cannot reclaim
// them), and not in time (everything outstanding is lost on a non-clean exit,
// and day-bitmap membership has no repair path on open the way postings do).
// D-3 fixed only the close()-time half: the final flush was being skipped on an
// empty memtable. Between flushes there was still nothing.
//
// WHAT THESE TESTS PIN. Two triggers, both landing in the same flush body the
// memtable threshold uses:
//
//   1. SIZE  — dirty keys / estimated dirty bytes, checked O(1) on the insert
//      path, including on the paths that write no KV entry at all.
//   2. AGE   — a periodic flush, because the size bound provably cannot fire in
//      the steady state that motivated the item: the same day bitmap re-touched
//      forever is ONE key.
//
// ...and the thing that makes the age bound affordable: an add that does not
// CHANGE a bitmap no longer marks it dirty, so an idle-but-busy shard does not
// re-serialize identical bitmaps once per interval forever.
#include "../../../lib/config/timestar_config.hpp"
#include "../../../lib/index/key_encoding.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <sstream>
#include <string>

using namespace timestar::index;
namespace ke = timestar::index::keys;

namespace {

class DirtyCacheFlushTriggerTest : public ::testing::Test {
public:
    void SetUp() override {
        std::filesystem::remove_all("shard_0/native_index");
        savedConfig_ = timestar::config();
        auto cfg = savedConfig_;
        // A DELIBERATELY HUGE write buffer. That is the whole point: these tests
        // must show the caches being drained while the memtable trigger — the
        // only one that existed — cannot possibly fire. The inverse of
        // day_bitmap_concurrent_insert_test.cpp's tiny-buffer trick.
        cfg.index.write_buffer_size = 64 * 1024 * 1024;
        timestar::setGlobalConfig(cfg);
    }
    void TearDown() override {
        timestar::setGlobalConfig(savedConfig_);
        std::filesystem::remove_all("shard_0/native_index");
    }
    timestar::TimestarConfig savedConfig_;
};

TimeStarInsert<double> makeDayInsert(const std::string& measurement, const std::string& host, uint64_t ts) {
    TimeStarInsert<double> insert(measurement, "value");
    insert.tags = {{"host", host}};
    insert.timestamps = {ts};
    insert.values = {1.0};
    return insert;
}

}  // namespace

// ONE series walked across more distinct days than the dirty-key bound allows.
// Every call after the first writes NOTHING to the memtable — the series is
// already indexed, the field type is already known — so the only thing growing
// is the day-bitmap cache. Pre-fix the dirty set simply grew to the number of
// days; post-fix it is held under the bound, which means the flush fired without
// any memtable pressure whatsoever.
//
// The reopen half is the other, more important assertion: the trigger must make
// this state DURABLE, not merely make it go away. Clearing dirty flags before
// the batch is applied is exactly the bug class D-2/D-3 were.
SEASTAR_TEST_F(DirtyCacheFlushTriggerTest, SizeTriggerBoundsDirtyCachesWithoutMemtablePressure) {
    const size_t bound = NativeIndex::maxDirtyIndexCacheKeys();
    // Comfortably past the bound so a flush must happen at least once, while
    // staying a cheap test: every iteration is a cold day-bitmap load against an
    // index with no SSTables, so there is no real I/O.
    const uint32_t days = static_cast<uint32_t>(bound * 2);
    const uint32_t firstDay = 30000;

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();

        size_t peak = 0;
        for (uint32_t d = 0; d < days; ++d) {
            auto insert = makeDayInsert("dirtybound", "h0", static_cast<uint64_t>(firstDay + d) * ke::NS_PER_DAY);
            co_await index.indexInsert(insert);
            peak = std::max(peak, index.dirtyIndexCacheKeys());
        }

        // The bound is checked once per CALL, not once per dirtied key, so a
        // single call's day loop may legally overshoot it. `recordDaySpan` caps
        // its loop at kMaxDaySpan = 366 days and the batch paths at the batch's
        // distinct-day count; 366 is the documented worst case and is what this
        // gate allows. (This workload dirties exactly one key per call, so the
        // observed peak is the bound itself -- the slack exists so the gate does
        // not fail on behaviour the register calls legal.)
        constexpr size_t kLegalOvershoot = 366;
        EXPECT_LE(peak, bound + kLegalOvershoot)
            << "dirty index-cache keys peaked at " << peak << " while walking " << days
            << " days with a 64 MB write buffer: the memtable threshold never came close, so "
            << "nothing drained the caches (write-scaleout debt D-17). The bound is " << bound << " (+"
            << kLegalOvershoot << " legal per-call overshoot).";
        // Belt and braces: the loop really did dirty more keys than the bound, so
        // the assertion above is not passing vacuously.
        EXPECT_GT(days, bound) << "test no longer exceeds the bound it is meant to exercise";

        co_await index.close();
    }

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        // Sample the FIRST day, dirtied and flushed long before close() ran, and
        // the last, which close() drained. Both must be present: a trigger that
        // clears dirty flags before its batch reaches the memtable would lose the
        // early one and look perfectly healthy on the late one.
        for (uint32_t d : {0u, days / 2, days - 1}) {
            const uint64_t ds = static_cast<uint64_t>(firstDay + d) * ke::NS_PER_DAY;
            auto r = co_await index.findSeriesWithMetadataTimeScoped("dirtybound", {}, {}, ds, ds + ke::NS_PER_DAY - 1);
            EXPECT_EQ(r.has_value() ? r->size() : 0u, 1u)
                << "day " << d << " lost its membership: the dirty-cache flush dropped state "
                << "instead of persisting it (the D-2/D-3 bug class)";
        }
        co_await index.close();
    }
}

// The AGE bound. A handful of series is nowhere near the size bound, so without a
// periodic flush this state sits in RAM until something unrelated crosses the
// memtable threshold — which, on a fixed fleet, may be never. Polls rather than
// sleeping the full interval blind, so the test costs about one interval.
SEASTAR_TEST_F(DirtyCacheFlushTriggerTest, AgeTriggerFlushesDirtyCachesWithNoFurtherWrites) {
    constexpr int kSeries = 8;
    const uint64_t ts = 31000ULL * ke::NS_PER_DAY;
    const auto interval = NativeIndex::dirtyIndexCacheFlushInterval();

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();

        for (int i = 0; i < kSeries; ++i) {
            auto insert = makeDayInsert("dirtyage", "h" + std::to_string(i), ts);
            co_await index.indexInsert(insert);
        }

        EXPECT_GT(index.dirtyIndexCacheKeys(), 0u) << "nothing was dirtied -- the test cannot observe the age trigger";
        EXPECT_LT(index.dirtyIndexCacheKeys(), NativeIndex::maxDirtyIndexCacheKeys())
            << kSeries << " series must stay well under the size bound, or this test measures the wrong trigger";

        // Deadline is a generous multiple of the interval: the assertion is "it
        // fires on its own", not "it fires within a tight window".
        const auto deadline = std::chrono::steady_clock::now() + interval * 4;
        while (index.dirtyIndexCacheKeys() > 0 && std::chrono::steady_clock::now() < deadline) {
            co_await seastar::sleep(std::chrono::milliseconds(50));
        }
        EXPECT_EQ(index.dirtyIndexCacheKeys(), 0u)
            << "dirty index-cache state survived " << (interval * 4).count()
            << " ms with no further writes: nothing bounds its AGE, so a day rollover's membership can sit only in "
            << "RAM until an unrelated memtable flush happens to run (write-scaleout debt D-17)";

        // WHY THIS STILL PINS THE TIMER'S DURABILITY despite a clean close. The
        // assertion above established that the dirty sets are EMPTY before
        // close() runs, so close()'s own flush has nothing left to serialize --
        // whatever the reopen below finds must have been written by the TIMER.
        // Had the timer cleared the dirty flags without its batch reaching the
        // memtable and WAL (the D-2/D-3 shape), close() would serialize nothing,
        // the SSTable would carry no day-bitmap keys, and the reopen would come
        // back empty.
        //
        // The sharper crash simulations use NativeIndex::abandonForTesting(): it drains
        // timer work without running the clean close-time data flush. Destroying
        // an asynchronously active object directly cannot be made safe by a C++
        // destructor because the destructor cannot suspend.
        co_await index.close();
    }

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        auto r = co_await index.findSeriesWithMetadataTimeScoped("dirtyage", {}, {}, ts, ts + ke::NS_PER_DAY - 1);
        EXPECT_EQ(r.has_value() ? r->size() : 0u, static_cast<size_t>(kSeries))
            << "the periodic flush emptied the dirty sets but the membership is not on disk: it cleared the dirty "
            << "flags without its batch reaching the memtable and WAL (the D-2/D-3 bug class), and close() then had "
            << "nothing left to persist. A counter reaching zero is not evidence of durability.";
        co_await index.close();
    }
}

// The periodic flush must be free when there is nothing to persist. This is not
// a micro-optimisation: `flushDirtyCaches()` deliberately enters on a POPULATED
// cache (so the trims run), and it unconditionally re-puts the local-ID counter
// and the postings watermark. Were the timer to use that guard instead of the
// stricter `hasDirtyCacheState()`, an idle shard would append two KV entries to
// the memtable and the WAL every interval, forever -- growing a memtable that
// nothing is writing to and eventually flushing SSTables out of pure idleness.
//
// Asserted on the index WAL's append sequence rather than on any cache counter,
// because "no write happened" is precisely what the cache counters cannot say.
SEASTAR_TEST_F(DirtyCacheFlushTriggerTest, IdleIndexWithWarmCachesWritesNothingOnTheTimer) {
    NativeIndex index(timestar::StorageLayout("."), 0);
    co_await index.open();

    const uint64_t ts = 33000ULL * ke::NS_PER_DAY;
    for (int i = 0; i < 4; ++i) {
        auto insert = makeDayInsert("dirtyidle", "h" + std::to_string(i), ts);
        co_await index.indexInsert(insert);
    }
    // Drain, leaving the caches POPULATED BUT CLEAN -- the state whose handling
    // this test is about.
    co_await index.compact();
    EXPECT_EQ(index.dirtyIndexCacheKeys(), 0u);

    const uint64_t seqAfterDrain = index.indexWalSequence();
    const auto interval = NativeIndex::dirtyIndexCacheFlushInterval();
    co_await seastar::sleep(interval * 2 + std::chrono::milliseconds(300));

    EXPECT_EQ(index.indexWalSequence(), seqAfterDrain)
        << "an idle index appended to its WAL across two dirty-cache flush intervals. The periodic flush must "
        << "consult hasDirtyCacheState(), not flushDirtyCaches()'s populated-cache guard, or every shard writes "
        << "the local-ID counter and postings watermark forever at " << interval.count() << " ms.";
    EXPECT_EQ(index.dirtyIndexCacheKeys(), 0u);

    co_await index.close();
}

// What makes the age bound affordable, and half of what makes the size bound
// meaningful: re-inserting a point whose local ID is ALREADY in the bitmap leaves
// that bitmap byte-identical to what is on disk, so it must not be marked dirty.
// It used to be, unconditionally — which pinned the entry against the trims
// forever and would now hand the periodic flush an unchanged bitmap to
// re-serialize once per interval for the life of the process.
SEASTAR_TEST_F(DirtyCacheFlushTriggerTest, RepeatedIdenticalInsertsDirtyNothing) {
    NativeIndex index(timestar::StorageLayout("."), 0);
    co_await index.open();

    const uint64_t ts = 32000ULL * ke::NS_PER_DAY;
    {
        auto insert = makeDayInsert("dirtysteady", "h0", ts);
        co_await index.indexInsert(insert);
    }
    // compact() drains the caches through flushMemTable(), giving a known-clean
    // starting point without waiting on the timer.
    co_await index.compact();
    EXPECT_EQ(index.dirtyIndexCacheKeys(), 0u) << "compact() must leave the application caches clean";

    for (int i = 0; i < 500; ++i) {
        auto insert = makeDayInsert("dirtysteady", "h0", ts);
        co_await index.indexInsert(insert);
    }

    EXPECT_EQ(index.dirtyIndexCacheKeys(), 0u)
        << "500 re-inserts of an already-recorded (series, day) dirtied the caches even though not one bit "
        << "changed. Those entries are then trim-exempt for as long as the workload continues, and every "
        << "periodic flush rewrites bitmaps identical to the ones already on disk.";

    co_await index.close();
}

// The byte estimate has to track a bitmap GROWING IN PLACE, not just the size it
// had when it was first dirtied. A day rollover creates an EMPTY day bitmap: its
// clean->dirty transition is worth a few bytes of key no matter how many million
// ids then land in it, so an estimate charged only at the transition reads ~0 on
// precisely the workload the bound exists for -- a byte trigger that can never
// fire before the key trigger is not a second bound, it is decoration.
//
// Measured as a DELTA between two probes with no flush in between, so the result
// does not depend on the HLL or bloom charges (both already paid by the first
// probe) or on any key-size arithmetic. One postings key and one day-bitmap key
// are involved throughout: all series share the tag value and the day, and differ
// only by field name.
SEASTAR_TEST_F(DirtyCacheFlushTriggerTest, ByteEstimateTracksBitmapsGrowingInPlace) {
    NativeIndex index(timestar::StorageLayout("."), 0);
    co_await index.open();

    const uint64_t ts = 34000ULL * ke::NS_PER_DAY;
    auto insertField = [&](int i) -> seastar::future<> {
        TimeStarInsert<double> insert("dirtygrow", "f" + std::to_string(i));
        insert.tags = {{"rack", "r0"}};
        insert.timestamps = {ts};
        insert.values = {1.0};
        co_await index.indexInsert(insert);
    };

    constexpr int kWarm = 200;
    constexpr int kTotal = 2000;
    for (int i = 0; i < kWarm; ++i)
        co_await insertField(i);
    const size_t bytesAfterWarm = index.dirtyIndexCacheBytes();
    const size_t keysAfterWarm = index.dirtyIndexCacheKeys();

    for (int i = kWarm; i < kTotal; ++i)
        co_await insertField(i);

    // No flush may have intervened, or the delta would be measuring a reset. Both
    // guards matter: the key count catches a flush that changed the working set,
    // and the byte comparison catches one that merely zeroed the estimate -- an
    // unguarded subtraction would then UNDERFLOW to a huge size_t and sail past
    // the assertion below, turning this gate into a no-op exactly when the
    // measurement had become meaningless.
    const size_t bytesNow = index.dirtyIndexCacheBytes();
    EXPECT_EQ(index.dirtyIndexCacheKeys(), keysAfterWarm)
        << "the dirty key set changed between probes -- a flush ran and this measurement is meaningless";
    EXPECT_GE(bytesNow, bytesAfterWarm)
        << "the byte estimate went DOWN between probes -- a flush zeroed it and this measurement is meaningless";
    const size_t grown = bytesNow >= bytesAfterWarm ? bytesNow - bytesAfterWarm : 0;

    // (kTotal - kWarm) series each add one id to the shared postings bitmap and
    // one to the day bitmap: 3600 in-place changes. At the 4-byte charge that is
    // ~14 KB; charged only at the transition it is exactly 0.
    EXPECT_GT(grown, 4096u) << "1800 further series grew two already-dirty bitmaps by 3600 ids and the byte estimate "
                            << "moved " << grown << " bytes. Charged only on the clean->dirty transition it cannot "
                            << "see in-place growth, which is every day rollover (write-scaleout debt D-17).";

    co_await index.close();
}

// ---------------------------------------------------------------------------
// STRUCTURAL pins, in the idiom day_bitmap_concurrent_insert_test.cpp already
// uses for this file. Both cover orderings whose violation is silent: the tests
// above would still pass while the durability property they exist for is gone.
TEST(DirtyCacheFlushSourceInspection, FlushOrderingAndTimerLifecycleAreIntact) {
    std::ifstream in(NATIVE_INDEX_SOURCE_PATH);
    ASSERT_TRUE(in.good()) << "cannot open " << NATIVE_INDEX_SOURCE_PATH;
    std::stringstream ss;
    ss << in.rdbuf();
    const std::string src = ss.str();

    // (1) THE DURABILITY ORDERING. flushDirtyCaches() clears every `dirty` flag
    // before it builds the batch, so between that point and the memtable apply
    // the batch is the ONLY copy of those adds. Applying AFTER the WAL append
    // means a throwing append leaves them in neither -- which is precisely how
    // D-2/D-3 lost day-bitmap membership.
    const size_t body = src.find("seastar::future<> NativeIndex::flushDirtyCaches()");
    ASSERT_NE(body, std::string::npos) << "flushDirtyCaches() not found -- this check has rotted";
    const size_t end = src.find("\n}\n", body);
    ASSERT_NE(end, std::string::npos);
    const std::string fn = src.substr(body, end - body);
    const size_t applyAt = fn.find("postingsBatch.applyTo(*memtable_)");
    const size_t appendAt = fn.find("wal_->append(postingsBatch)");
    ASSERT_NE(applyAt, std::string::npos);
    ASSERT_NE(appendAt, std::string::npos);
    EXPECT_LT(applyAt, appendAt) << "the dirty-cache batch must be applied to the memtable BEFORE the WAL append. "
                                 << "The dirty flags are already cleared by this point, so a throwing append would "
                                 << "leave those adds in neither the memtable nor the log.";

    // (2) THE TIMER LIFECYCLE. It takes flushMutex_ and appends to the WAL, so a
    // callback still in flight past close() writes through a half-torn index; one
    // that survives into ~NativeIndex writes through a dead one.
    EXPECT_NE(src.find("dirtyCacheTimer_.arm_periodic(kDirtyCacheFlushInterval)"), std::string::npos)
        << "the periodic dirty-cache flush is never armed -- the AGE bound does not exist";
    const size_t stopAt = src.find("seastar::future<> NativeIndex::stopBackgroundTasks()");
    ASSERT_NE(stopAt, std::string::npos);
    const size_t stopEnd = src.find("\n}\n", stopAt);
    ASSERT_NE(stopEnd, std::string::npos);
    const std::string stopBody = src.substr(stopAt, stopEnd - stopAt);
    EXPECT_NE(stopBody.find("dirtyCacheTimer_.cancel()"), std::string::npos)
        << "background shutdown must cancel the periodic dirty-cache flush";
    EXPECT_NE(stopBody.find("dirtyCacheGate_.close()"), std::string::npos)
        << "close() must DRAIN an in-flight dirty-cache flush, not merely stop new ones: it holds flushMutex_ and "
        << "appends to the WAL that close() is about to shut down";
    const size_t closeAt = src.find("seastar::future<> NativeIndex::close()");
    ASSERT_NE(closeAt, std::string::npos);
    const size_t closeEnd = src.find("\n}\n", closeAt);
    ASSERT_NE(closeEnd, std::string::npos);
    EXPECT_NE(src.substr(closeAt, closeEnd - closeAt).find("stopBackgroundTasks()"), std::string::npos)
        << "close() must use the common background drain";
    const size_t abandonAt = src.find("seastar::future<> NativeIndex::abandonForTesting()");
    ASSERT_NE(abandonAt, std::string::npos);
    const size_t abandonEnd = src.find("\n}\n", abandonAt);
    ASSERT_NE(abandonEnd, std::string::npos);
    EXPECT_NE(src.substr(abandonAt, abandonEnd - abandonAt).find("stopBackgroundTasks()"), std::string::npos)
        << "the non-clean test boundary must drain background work before destruction";
    const size_t dtorAt = src.find("NativeIndex::~NativeIndex()");
    ASSERT_NE(dtorAt, std::string::npos);
    const size_t dtorEnd = src.find("\n}\n", dtorAt);
    ASSERT_NE(dtorEnd, std::string::npos);
    EXPECT_NE(src.substr(dtorAt, dtorEnd - dtorAt).find("dirtyCacheTimer_.cancel()"), std::string::npos)
        << "the destructor must still cancel the timer as an emergency backstop";
}
