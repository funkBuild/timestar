// Regression gate for the DAY-BITMAP USE-AFTER-FREE / LOST-UPDATE defect
// (write-scaleout Phase 4d).
//
// THE CRASH THIS PINS. A `{"writes":[...]}` batch carrying thousands of DISTINCT SERIES,
// ~40 concurrent, segfaulted every node of a 3-node cluster simultaneously. Symbolized:
// `roaring_bitmap_add`, faulting on a garbage address, immediately after seastar's
// memory-pressure dump. It reproduced on binaries pre-dating all of the clustering work
// (8079fa6), and it vanished when the same BYTES were sent as few series with many
// timestamps -- i.e. it tracked the number of distinct CACHE KEYS, not the volume.
//
// THE MECHANISM. `NativeIndex::getOrLoadDayBitmapForInsert` returned a
// `roaring::Roaring*` pointing INTO `dayBitmapCache_` and let the caller write through
// it:
//
//     if ((co_await getOrLoadDayBitmapForInsert(dayCacheKey))->addChecked(localId)) ...
//
// There is no `co_await` between the two -- and that is exactly why it looked safe. But
// the pointer is computed INSIDE a coroutine that suspends (`co_await kvGet` on a cold
// key), so `co_return &entry.bitmap` and the caller's `->addChecked` happen in DIFFERENT
// reactor tasks. Between them, any other insert coroutine on the shard can:
//
//   * insert a new key into `dayBitmapCache_` -- a tsl::robin_map, OPEN ADDRESSING,
//     holding `roaring::Roaring` BY VALUE -- and REHASH it, relocating every bitmap; or
//   * cross the memtable threshold, which runs `flushDirtyDayBitmaps()` (clearing the
//     `dirty` flag that protects an entry from eviction, and calling
//     runOptimize/shrinkToFit, which reallocates the containers) and then
//     `trimDayBitmapCache()`, which ERASES the now-clean entry.
//
// Both make the caller's `->addChecked` a write through freed or moved memory. Memory
// pressure is what makes the second one routine, which is why the crash always followed
// the pressure dump. Many distinct series is what makes the first one routine, which is
// why series count and not byte count predicted it.
//
// THE SECOND, SILENT HALF. Even when nothing crashed, an add landing on an entry whose
// `dirty` flag had been cleared during the suspension was NEVER PERSISTED: the next flush
// skipped it, and the series simply disappeared from that day's discovery bitmap. That
// is a wrong query answer, not a crash, and it is what this test measures -- because it
// is DETERMINISTIC where the use-after-free is not.
//
// THE FIX. `addToDayBitmapForInsert(key, localId)` does the mutation INSIDE, after
// re-finding the entry, with no suspension in between, and re-marks the entry dirty. No
// `roaring::Roaring*` escapes a suspending coroutine any more.
//
// WHAT THIS TEST DOES. It drives concurrent inserts of many distinct series against a
// deliberately tiny write buffer, so flush + trim fire repeatedly WHILE cold day-bitmap
// loads are in flight, then re-opens the index and asserts every series is still
// discoverable in its day. Pre-fix it loses series (and, under a small heap, crashes).
#include "../../../lib/config/timestar_config.hpp"
#include "../../../lib/index/key_encoding.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <sstream>
#include <string>
#include <vector>

using namespace timestar::index;
namespace ke = timestar::index::keys;

namespace {

class DayBitmapConcurrentInsertTest : public ::testing::Test {
public:
    void SetUp() override {
        std::filesystem::remove_all("shard_0/native_index");
        savedConfig_ = timestar::config();
        auto cfg = savedConfig_;
        // A tiny write buffer makes maybeFlushMemTable() fire every few series, which is
        // what puts flushDirtyDayBitmaps() + trimDayBitmapCache() in the middle of the
        // in-flight cold loads. On a real node the same thing happens under memory
        // pressure; here it is forced rather than hoped for.
        cfg.index.write_buffer_size = 4096;
        timestar::setGlobalConfig(cfg);
    }
    void TearDown() override {
        timestar::setGlobalConfig(savedConfig_);
        std::filesystem::remove_all("shard_0/native_index");
    }
    timestar::TimestarConfig savedConfig_;
};

TimeStarInsert<double> makeInsert(const std::string& measurement, int seriesIdx, uint64_t ts) {
    TimeStarInsert<double> insert(measurement, "value");
    insert.tags = {{"host", "h" + std::to_string(seriesIdx)}};
    insert.timestamps = {ts};
    insert.values = {1.0};
    return insert;
}

}  // namespace

// MANY DISTINCT (measurement, day) KEYS, indexed CONCURRENTLY, so nearly every day-bitmap
// touch takes the COLD path -- the one that suspends in `kvGet` and therefore the one
// where the entry can be flushed (dirty flag cleared), relocated by a rehash, or evicted
// while the load is in flight.
//
// Pre-fix, an add landing after a concurrent flush had cleared the entry's dirty flag was
// never persisted: the caller wrote through the returned `Roaring*` and nothing re-marked
// the entry, so the next flush skipped it. The series then simply does not exist for
// day-scoped discovery after a restart. Post-fix the mutation happens inside, after the
// re-find, and re-marks the entry dirty.
SEASTAR_TEST_F(DayBitmapConcurrentInsertTest, ConcurrentColdDayBitmapLoadsPersistEveryAdd) {
    constexpr int kMeasurements = 400;
    const uint64_t day = 20000ULL * ke::NS_PER_DAY;

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();

        // One measurement per series => one DISTINCT day-bitmap cache key per series, so
        // the cold load (and its suspension) is the common case rather than a rarity.
        constexpr int kChunk = 40;
        for (int start = 0; start < kMeasurements; start += kChunk) {
            // The inserts must OUTLIVE the futures: indexInsert takes its argument by
            // const reference and holds it across its own suspensions, so passing a
            // temporary here dangles the moment the full expression ends.
            std::vector<TimeStarInsert<double>> inserts;
            inserts.reserve(kChunk);
            for (int i = start; i < start + kChunk && i < kMeasurements; ++i)
                inserts.push_back(makeInsert("m" + std::to_string(i), i, day));
            std::vector<seastar::future<SeriesId128>> pending;
            pending.reserve(inserts.size());
            for (const auto& ins : inserts)
                pending.push_back(index.indexInsert(ins));
            co_await seastar::when_all_succeed(pending.begin(), pending.end());
        }
        co_await index.close();
    }

    // Reopen: membership now has to come from what was actually PERSISTED, not from a
    // cache entry that happened to survive in memory.
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        size_t missing = 0;
        for (int i = 0; i < kMeasurements; ++i) {
            auto result = co_await index.findSeriesWithMetadataTimeScoped("m" + std::to_string(i), {}, {}, day,
                                                                          day + ke::NS_PER_DAY - 1);
            if (!result.has_value() || result->empty())
                ++missing;
        }
        EXPECT_EQ(missing, 0u) << missing << " of " << kMeasurements
                               << " series lost their day-bitmap membership: an add landed on a cache "
                               << "entry whose dirty flag a concurrent flush had cleared during the cold "
                               << "load, so it was never persisted (write-scaleout 4d)";
        co_await index.close();
    }
}

// The same shape through the BATCH path (Engine::insertBatch -> recordInsertDays), which
// is the one the crashing production request actually took: one metadata batch creating
// thousands of series, then per-series day recording.
SEASTAR_TEST_F(DayBitmapConcurrentInsertTest, ConcurrentBatchDayRecordingKeepsEveryMembership) {
    constexpr int kSeries = 400;
    const uint64_t baseDay = 20100ULL * ke::NS_PER_DAY;

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();

        // Create the series (and their LocalIds) via the metadata-batch path, which is
        // where recordDaySpan runs.
        std::vector<MetadataOp> ops;
        ops.reserve(kSeries);
        for (int i = 0; i < kSeries; ++i) {
            MetadataOp op;
            op.measurement = "daybatch";
            op.tags = {{"host", "h" + std::to_string(i)}};
            op.fieldName = "value";
            op.valueType = TSMValueType::Float;
            op.minTs = baseDay;
            op.maxTs = baseDay + ke::NS_PER_DAY;  // spans two days
            ops.push_back(std::move(op));
        }
        co_await index.indexMetadataBatch(ops);
        co_await index.close();
    }

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        for (int d = 0; d < 2; ++d) {
            const uint64_t dayStart = baseDay + static_cast<uint64_t>(d) * ke::NS_PER_DAY;
            auto result = co_await index.findSeriesWithMetadataTimeScoped("daybatch", {}, {}, dayStart,
                                                                          dayStart + ke::NS_PER_DAY - 1);
            EXPECT_TRUE(result.has_value());
            EXPECT_EQ(result.has_value() ? result->size() : 0u, static_cast<size_t>(kSeries))
                << "day " << d << ": recordDaySpan lost day-bitmap membership (write-scaleout 4d)";
        }
        co_await index.close();
    }
}

// ---------------------------------------------------------------------------
// The STRUCTURAL pin. The defect above is a RACE: it reproduces under load and under a
// small heap, but no unit test can schedule the reactor tasks that expose it on demand.
// What CAN be pinned deterministically is the shape that makes it possible -- a raw
// pointer into one of the bitmap caches escaping a coroutine that suspends.
//
// WHAT CHANGED IN 5.1. The caches now hold `seastar::lw_shared_ptr<BitmapEntry>` rather
// than `BitmapEntry` by value, so a rehash moves 8-byte handles and never an entry, and a
// handle keeps its entry alive even if a trim drops it from the map. That kills BOTH
// halves of the hazard structurally, and it is why the read accessors no longer carry a
// "consume the returned pointer immediately" contract -- a contract that was never
// satisfiable, because the invalidation happened before the caller resumed.
//
// So the shape this test pins is now the CACHE'S OWN TYPE plus the absence of raw
// pointers out of the accessors. Both are checked: if someone reverts the value type, a
// `co_return &entry.bitmap` becomes lethal again even though no accessor signature
// changed.
TEST(DayBitmapSourceInspection, NoBitmapPointerEscapesASuspendingCoroutine) {
    std::ifstream in(NATIVE_INDEX_SOURCE_PATH);
    ASSERT_TRUE(in.good()) << "cannot open " << NATIVE_INDEX_SOURCE_PATH;
    std::stringstream ss;
    ss << in.rdbuf();
    const std::string src = ss.str();

    std::ifstream hin(NATIVE_INDEX_HPP_SOURCE_PATH);
    ASSERT_TRUE(hin.good()) << "cannot open " << NATIVE_INDEX_HPP_SOURCE_PATH;
    std::stringstream hss;
    hss << hin.rdbuf();
    const std::string hdr = hss.str();

    // (0) THE LOAD-BEARING INVARIANT: the cache values are heap-stable handles. Everything
    // else in this test is downstream of it.
    EXPECT_NE(hdr.find("using BitmapHandle = seastar::lw_shared_ptr<BitmapEntry>"), std::string::npos)
        << "the bitmap caches must hold heap-stable, refcounted handles. Held BY VALUE in a "
        << "tsl::robin_map (open addressing) every insert RELOCATES every entry and every "
        << "trim FREES one, so no reference into the cache survives a suspension -- the "
        << "use-after-free that faults inside roaring_bitmap_add (write-scaleout 4d/5.1).";
    EXPECT_NE(hdr.find("using BitmapCache = tsl::robin_map<std::string, BitmapHandle>"), std::string::npos);
    EXPECT_EQ(hdr.find("tsl::robin_map<std::string, BitmapEntry>"), std::string::npos)
        << "a bitmap cache still holds BitmapEntry BY VALUE -- see above.";

    // (1) No raw bitmap pointer, mutable or const, may leave one of these coroutines. The
    // mutable spelling was always forbidden (the write path mutates inside); the CONST one
    // is forbidden now too, because the pin is what makes a read safe and a raw pointer
    // carries no pin. Both spacings are checked -- clang-format produces the unspaced
    // form, but a hand-written `Roaring *` would slip past a single literal.
    for (const char* spelling :
         {"seastar::future<roaring::Roaring*>", "seastar::future<roaring::Roaring *>",
          "seastar::future<const roaring::Roaring*>", "seastar::future<const roaring::Roaring *>"}) {
        EXPECT_EQ(src.find(spelling), std::string::npos)
            << "native_index.cpp hands a raw `roaring::Roaring` pointer out of a coroutine (" << spelling
            << "). The caller dereferences it in a LATER reactor task, and a raw pointer "
            << "pins nothing. Return a BitmapHandle (read side) or do the mutation inside "
            << "the accessor (write side, see addToDayBitmapForInsert).";
    }

    // ... and the replacements are actually there, so the check above cannot pass simply
    // because someone deleted the insert path.
    EXPECT_NE(src.find("NativeIndex::addToDayBitmapForInsert"), std::string::npos);
    EXPECT_NE(src.find("NativeIndex::addToPostingsBitmapForInsert"), std::string::npos);

    // (2) The trims must honour the pin. Freeing is already impossible (the holder's
    // handle keeps the entry alive), so this is about COHERENCE: evicting an entry a
    // reader still holds would leave the reader and a concurrent writer mutating two
    // different objects for the same key, and the reader's adds would be the ones lost.
    for (const char* fn : {"NativeIndex::trimBitmapCache", "NativeIndex::trimDayBitmapCache"}) {
        const size_t start = src.find(fn);
        ASSERT_NE(start, std::string::npos) << fn << " not found -- this check has rotted";
        const size_t end = src.find("\n}\n", start);
        ASSERT_NE(end, std::string::npos);
        EXPECT_NE(src.substr(start, end - start).find(".owned()"), std::string::npos)
            << fn << " evicts without checking whether the entry is PINNED. An entry a "
            << "suspended reader holds must stay in the map, or that reader and a "
            << "concurrent writer end up on divergent copies of the same key.";
    }

    // (3) Nothing may create a cache slot through `operator[]`: that inserts a NULL
    // handle, which every reader would then have to guard. `ensureEntry` materializes it.
    for (const char* spelling : {"bitmapCache_[", "dayBitmapCache_["}) {
        EXPECT_EQ(src.find(spelling), std::string::npos)
            << spelling << "...] inserts a null handle. Use ensureEntry(cache, key).";
    }
}

// ---------------------------------------------------------------------------
// A SECOND, DISTINCT defect found while building the repro above. NOT fixed here, and
// NOT introduced by 4d -- it reproduces identically on the pre-4d index code.
//
// With a 4 KB write buffer (so a memtable flush + L0 compaction fires every few series)
// and CONCURRENT inserts, day-bitmap membership that is CORRECT IN MEMORY is lost on the
// way to disk: the warm, pre-close, time-scoped count is exactly right, and the count
// after close + reopen is short by roughly one chunk's worth. It needs BOTH concurrency
// and frequent flushes -- the same workload run sequentially persists everything, and at
// a 64 KB buffer the loss disappears -- which points at the flush/compaction path, not at
// the add path this phase fixed.
//
// Enable it to work on the bug; it is a one-command repro:
//     ./test/timestar_unit_test --gtest_also_run_disabled_tests \
//         --gtest_filter='*DISABLED_ConcurrentInsertsWithTinyWriteBufferLoseDayMembership'
// Filed in docs/write-scaleout-plan.md for the index owner.
SEASTAR_TEST_F(DayBitmapConcurrentInsertTest, DISABLED_ConcurrentInsertsWithTinyWriteBufferLoseDayMembership) {
    constexpr int kSeries = 600;
    constexpr int kDays = 4;
    const uint64_t baseDay = 20500ULL * ke::NS_PER_DAY;

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        constexpr int kChunk = 50;
        for (int start = 0; start < kSeries; start += kChunk) {
            std::vector<TimeStarInsert<double>> inserts;
            inserts.reserve(kChunk);
            for (int i = start; i < start + kChunk && i < kSeries; ++i)
                inserts.push_back(
                    makeInsert("daylost", i, baseDay + static_cast<uint64_t>(i % kDays) * ke::NS_PER_DAY));
            std::vector<seastar::future<SeriesId128>> pending;
            pending.reserve(inserts.size());
            for (const auto& ins : inserts)
                pending.push_back(index.indexInsert(ins));
            co_await seastar::when_all_succeed(pending.begin(), pending.end());
        }
        // In-memory the answer is already correct here -- that is the point.
        co_await index.close();
    }
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        size_t found = 0;
        for (int d = 0; d < kDays; ++d) {
            const uint64_t ds = baseDay + static_cast<uint64_t>(d) * ke::NS_PER_DAY;
            auto r = co_await index.findSeriesWithMetadataTimeScoped("daylost", {}, {}, ds, ds + ke::NS_PER_DAY - 1);
            if (r.has_value())
                found += r->size();
        }
        EXPECT_EQ(found, static_cast<size_t>(kSeries)) << "day-bitmap membership lost between memory and disk";
        co_await index.close();
    }
}
