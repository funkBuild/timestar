// The PER-VSHARD flushed watermark (debt D-35).
//
// The snapshot producer may only truncate a group's Raft log at "the highest revision in
// TSM" when every revision below it is ALSO in TSM. D-6 bought that with a per-SHARD
// predicate -- "does ANY rolled memory store still await conversion?" -- which is correct
// and gives all ~1365 groups on a core the same answer: a shard under sustained ingest is
// never at zero pending conversions, so nothing on it ever compacts.
//
// The refinement is a 512-byte presence bitmap per MemoryStore (MemoryStore::noteVShard),
// set on the ONE lowest-level insertion path, so the question narrows to "does an
// unconverted rolled store hold data for THIS VShard?". These tests pin the two halves:
// the bitmap is populated by the real insert path, and the predicate reads it with the
// right store-list semantics (the ACTIVE store is excluded; a rolled one is not).
#include "../../../lib/core/placement_table.hpp"  // timestar::virtualShard
#include "../../../lib/core/vshard.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/storage_layout.hpp"
#include "../../../lib/storage/wal.hpp"
#include "../../../lib/storage/wal_file_manager.hpp"
#include "../../../lib/utils/series_key.hpp"

#include <gtest/gtest.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <string>
#include <vector>

namespace {

using timestar::VShardId;

// A series key plus the VShard it hashes to, and a second key that hashes SOMEWHERE
// ELSE -- the whole point of the test is that two VShards get different answers.
struct KeyAt {
    std::string key;
    uint16_t vshard = 0;
};

KeyAt keyNotIn(const std::string& measurement, const std::vector<uint16_t>& avoid) {
    for (int i = 0;; ++i) {
        KeyAt k;
        k.key = timestar::buildSeriesKey(measurement, {{"host", "h" + std::to_string(i)}}, "value");
        k.vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(k.key));
        bool clash = false;
        for (uint16_t v : avoid)
            clash = clash || v == k.vshard;
        if (!clash)
            return k;
    }
}

// Insert one point into a store's memory map WITHOUT touching the WAL: insertMemory is
// the single choke point the bitmap hangs off, and it is also what WAL replay drives
// (initFromWAL -> WALReader::readAll -> insertMemory), so exercising it here is
// exercising the recovery path too.
void insertOne(MemoryStore& store, const std::string& key, uint64_t ts, double value) {
    TimeStarInsert<double> ins = TimeStarInsert<double>::fromSeriesKey(key);
    ins.timestamps = {ts};
    ins.values = {value};
    ins.setCachedSeriesKey(key);
    store.insertMemory(std::move(ins));
}

}  // namespace

// The bitmap is set by the real insert path, for exactly the VShards written.
TEST(VShardFlushWatermarkTest, InsertRecordsTheSeriesVShardAndNothingElse) {
    MemoryStore store(1);
    const KeyAt a = keyNotIn("d35bitmap", {});
    const KeyAt b = keyNotIn("d35bitmap_other", {a.vshard});
    ASSERT_NE(a.vshard, b.vshard);

    EXPECT_FALSE(store.touchesVShard(a.vshard));
    EXPECT_FALSE(store.touchesVShard(b.vshard));
    EXPECT_EQ(store.vshardsTouched(), 0u);

    insertOne(store, a.key, 1'000, 1.0);
    EXPECT_TRUE(store.touchesVShard(a.vshard));
    EXPECT_FALSE(store.touchesVShard(b.vshard)) << "a VShard never written must never read as present";
    EXPECT_EQ(store.vshardsTouched(), 1u);

    insertOne(store, b.key, 1'001, 2.0);
    EXPECT_TRUE(store.touchesVShard(a.vshard));
    EXPECT_TRUE(store.touchesVShard(b.vshard));
    EXPECT_EQ(store.vshardsTouched(), 2u);
}

// The predicate's store-list semantics: [0] is the ACTIVE store and is NOT pending
// conversion; everything after it is a rolled store that is.
TEST(VShardFlushWatermarkTest, OnlyRolledStoresHoldingTheVShardCountAsPending) {
    const KeyAt a = keyNotIn("d35pending", {});
    const KeyAt b = keyNotIn("d35pending_other", {a.vshard});
    ASSERT_NE(a.vshard, b.vshard);

    auto active = seastar::make_shared<MemoryStore>(2);
    auto rolled = seastar::make_shared<MemoryStore>(1);
    insertOne(*active, a.key, 2'000, 1.0);  // VShard a is only in the ACTIVE store
    insertOne(*rolled, b.key, 2'001, 2.0);  // VShard b is in a store awaiting conversion

    std::vector<seastar::shared_ptr<MemoryStore>> stores{active, rolled};

    // THE D-35 DELTA, stated against BOTH real predicates rather than a respelling of
    // either: the per-SHARD one says "a conversion is pending, no group may compact",
    // while the per-VSHARD one frees VShard a, whose data that conversion has nothing to
    // do with.
    EXPECT_TRUE(WALFileManager::pendingConversion(stores)) << "the per-shard predicate is TRUE here";
    // The active store never counts: its data is the contiguous SUFFIX the boundary
    // deliberately excludes, not an out-of-order hole below it.
    EXPECT_FALSE(WALFileManager::pendingConversionForVShard(stores, a.vshard));
    // The rolled store does: VShard b's revisions may sit BELOW revisions already in TSM.
    EXPECT_TRUE(WALFileManager::pendingConversionForVShard(stores, b.vshard));

    // A VShard nobody has written at all is never pending.
    const KeyAt c = keyNotIn("d35pending_third", {a.vshard, b.vshard});
    EXPECT_FALSE(WALFileManager::pendingConversionForVShard(stores, c.vshard));
}

// With no rolled store at all, nothing is pending regardless of what the active store
// holds -- the state the producer needs to see before it may compact.
TEST(VShardFlushWatermarkTest, AnActiveStoreAloneNeverBlocksCompaction) {
    const KeyAt a = keyNotIn("d35activeonly", {});
    auto active = seastar::make_shared<MemoryStore>(1);
    insertOne(*active, a.key, 3'000, 1.0);
    std::vector<seastar::shared_ptr<MemoryStore>> stores{active};
    EXPECT_FALSE(WALFileManager::pendingConversion(stores));
    EXPECT_FALSE(WALFileManager::pendingConversionForVShard(stores, a.vshard));
}

// WAL-REPLAY PARITY. The bitmap hangs off `insertMemory` precisely so that recovery
// populates it too (`initFromWAL` -> `WALReader::readAll` -> `insertMemory`). A recovered
// store that under-reported its VShards would let the producer compact over revisions that
// are in RAM and not in TSM -- the exact loss D-6 measured -- and it would do so ONLY
// after a restart, which is the worst possible time to find out.
seastar::future<> testWalReplayRebuildsTheSameBitmap() {
    const KeyAt a = keyNotIn("d35replay", {});
    const KeyAt b = keyNotIn("d35replay_other", {a.vshard});
    EXPECT_NE(a.vshard, b.vshard);

    auto live = seastar::make_shared<MemoryStore>(777);
    co_await live->initWAL(timestar::StorageLayout("."), seastar::this_shard_id());
    const std::string walPath = live->getWAL()->filename();
    for (const auto& k : {a, b}) {
        TimeStarInsert<double> ins = TimeStarInsert<double>::fromSeriesKey(k.key);
        ins.timestamps = {4'000};
        ins.values = {7.5};
        ins.setCachedSeriesKey(k.key);
        EXPECT_FALSE(co_await live->insert(ins));
    }
    EXPECT_TRUE(live->touchesVShard(a.vshard));
    EXPECT_TRUE(live->touchesVShard(b.vshard));
    const size_t liveCount = live->vshardsTouched();
    co_await live->close();

    auto recovered = seastar::make_shared<MemoryStore>(778);
    co_await recovered->initFromWAL(walPath);
    EXPECT_TRUE(recovered->touchesVShard(a.vshard)) << "recovery must rebuild the VShard bitmap";
    EXPECT_TRUE(recovered->touchesVShard(b.vshard));
    EXPECT_EQ(recovered->vshardsTouched(), liveCount);
    co_await live->removeWAL();
}
TEST(VShardFlushWatermarkTest, WalReplayRebuildsTheSameBitmap) {
    testWalReplayRebuildsTheSameBitmap().get();
}
