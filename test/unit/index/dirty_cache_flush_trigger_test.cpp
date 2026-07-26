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

        EXPECT_LE(peak, bound) << "dirty index-cache keys peaked at " << peak << " while walking " << days
                               << " days with a 64 MB write buffer: the memtable threshold never came close, so "
                               << "nothing drained the caches (write-scaleout debt D-17). The bound is " << bound
                               << ".";
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
    NativeIndex index(timestar::StorageLayout("."), 0);
    co_await index.open();

    const uint64_t ts = 31000ULL * ke::NS_PER_DAY;
    for (int i = 0; i < 8; ++i) {
        auto insert = makeDayInsert("dirtyage", "h" + std::to_string(i), ts);
        co_await index.indexInsert(insert);
    }

    EXPECT_GT(index.dirtyIndexCacheKeys(), 0u) << "nothing was dirtied -- the test cannot observe the age trigger";
    EXPECT_LT(index.dirtyIndexCacheKeys(), NativeIndex::maxDirtyIndexCacheKeys())
        << "8 series must stay well under the size bound, or this test is measuring the wrong trigger";

    // Deadline is a generous multiple of the interval: the assertion is "it fires
    // on its own", not "it fires within a tight window".
    const auto interval = NativeIndex::dirtyIndexCacheFlushInterval();
    const auto deadline = std::chrono::steady_clock::now() + interval * 10;
    while (index.dirtyIndexCacheKeys() > 0 && std::chrono::steady_clock::now() < deadline) {
        co_await seastar::sleep(std::chrono::milliseconds(50));
    }
    EXPECT_EQ(index.dirtyIndexCacheKeys(), 0u)
        << "dirty index-cache state survived " << (interval * 10).count()
        << " ms with no further writes: nothing bounds its AGE, so a day rollover's membership can sit only in "
        << "RAM until an unrelated memtable flush happens to run (write-scaleout debt D-17)";

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
    const size_t closeAt = src.find("seastar::future<> NativeIndex::close()");
    ASSERT_NE(closeAt, std::string::npos);
    const size_t closeEnd = src.find("\n}\n", closeAt);
    ASSERT_NE(closeEnd, std::string::npos);
    const std::string closeBody = src.substr(closeAt, closeEnd - closeAt);
    EXPECT_NE(closeBody.find("dirtyCacheTimer_.cancel()"), std::string::npos)
        << "close() must cancel the periodic dirty-cache flush";
    EXPECT_NE(closeBody.find("dirtyCacheGate_.close()"), std::string::npos)
        << "close() must DRAIN an in-flight dirty-cache flush, not merely stop new ones: it holds flushMutex_ and "
        << "appends to the WAL that close() is about to shut down";
    const size_t dtorAt = src.find("NativeIndex::~NativeIndex()");
    ASSERT_NE(dtorAt, std::string::npos);
    const size_t dtorEnd = src.find("\n}\n", dtorAt);
    ASSERT_NE(dtorEnd, std::string::npos);
    EXPECT_NE(src.substr(dtorAt, dtorEnd - dtorAt).find("dirtyCacheTimer_.cancel()"), std::string::npos)
        << "the destructor must cancel the timer too -- an index can be destroyed without close()";
}
