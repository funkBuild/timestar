#include "../../../lib/index/native/local_id_map.hpp"

#include "../../../lib/core/series_id.hpp"

#include <gtest/gtest.h>

#include <set>

using namespace timestar::index;

class LocalIdMapTest : public ::testing::Test {};

TEST_F(LocalIdMapTest, BasicAssignment) {
    LocalIdMap map;
    SeriesId128 id1 = SeriesId128::fromSeriesKey("test_series_1");
    SeriesId128 id2 = SeriesId128::fromSeriesKey("test_series_2");

    uint32_t local1 = map.getOrAssign(id1);
    uint32_t local2 = map.getOrAssign(id2);

    EXPECT_EQ(local1, 0u);
    EXPECT_EQ(local2, 1u);
    EXPECT_EQ(map.size(), 2u);
    EXPECT_EQ(map.nextId(), 2u);
}

TEST_F(LocalIdMapTest, IdempotentAssignment) {
    LocalIdMap map;
    SeriesId128 id = SeriesId128::fromSeriesKey("test_series");

    uint32_t first = map.getOrAssign(id);
    uint32_t second = map.getOrAssign(id);

    EXPECT_EQ(first, second);
    EXPECT_EQ(map.size(), 1u);
}

TEST_F(LocalIdMapTest, MonotonicAssignment) {
    LocalIdMap map;
    for (int i = 0; i < 100; ++i) {
        SeriesId128 id = SeriesId128::fromSeriesKey("series_" + std::to_string(i));
        uint32_t localId = map.getOrAssign(id);
        EXPECT_EQ(localId, static_cast<uint32_t>(i));
    }
    EXPECT_EQ(map.size(), 100u);
}

TEST_F(LocalIdMapTest, RoundTrip) {
    LocalIdMap map;
    SeriesId128 id = SeriesId128::fromSeriesKey("round_trip_test");

    uint32_t localId = map.getOrAssign(id);
    const SeriesId128& retrieved = map.getGlobalId(localId);

    EXPECT_EQ(id, retrieved);
}

TEST_F(LocalIdMapTest, GetLocalIdMiss) {
    LocalIdMap map;
    SeriesId128 id = SeriesId128::fromSeriesKey("not_in_map");

    auto result = map.getLocalId(id);
    EXPECT_FALSE(result.has_value());
}

TEST_F(LocalIdMapTest, GetLocalIdHit) {
    LocalIdMap map;
    SeriesId128 id = SeriesId128::fromSeriesKey("in_map");
    uint32_t assigned = map.getOrAssign(id);

    auto result = map.getLocalId(id);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, assigned);
}

TEST_F(LocalIdMapTest, Restore) {
    // Build original map
    LocalIdMap original;
    std::vector<SeriesId128> ids;
    for (int i = 0; i < 50; ++i) {
        SeriesId128 id = SeriesId128::fromSeriesKey("restore_series_" + std::to_string(i));
        original.getOrAssign(id);
        ids.push_back(id);
    }

    // Restore into fresh map
    LocalIdMap restored;
    restored.restoreBegin(50, 50);
    for (uint32_t i = 0; i < 50; ++i) {
        EXPECT_TRUE(restored.restoreEntry(i, ids[i]));
    }

    EXPECT_EQ(restored.size(), 50u);
    EXPECT_EQ(restored.nextId(), 50u);

    // Verify all mappings survive
    for (uint32_t i = 0; i < 50; ++i) {
        auto localId = restored.getLocalId(ids[i]);
        ASSERT_TRUE(localId.has_value());
        EXPECT_EQ(*localId, i);
        EXPECT_EQ(restored.getGlobalId(i), ids[i]);
    }

    // New assignment should continue from 50
    SeriesId128 newId = SeriesId128::fromSeriesKey("restore_series_50");
    uint32_t newLocal = restored.getOrAssign(newId);
    EXPECT_EQ(newLocal, 50u);
}

TEST_F(LocalIdMapTest, RestoreToleratesCounterAboveSpeculativeCap) {
    // getOrAssign() issues ids up to UINT32_MAX at runtime and persists the
    // counter; restore must accept the same range or a legitimately large
    // shard runs fine until its next restart and then can never open again.
    // Only the up-front pre-allocation is capped — entries grow the map.
    const uint32_t bigCounter = LocalIdMap::kMaxSpeculativeRestoreIds + 5000;

    LocalIdMap restored;
    restored.restoreBegin(bigCounter, bigCounter);  // must not throw
    EXPECT_EQ(restored.nextId(), bigCounter);

    // Entries below AND above the speculative cap both restore.
    SeriesId128 low = SeriesId128::fromSeriesKey("big_shard_low");
    SeriesId128 high = SeriesId128::fromSeriesKey("big_shard_high");
    EXPECT_TRUE(restored.restoreEntry(7, low));
    EXPECT_TRUE(restored.restoreEntry(bigCounter - 1, high));

    EXPECT_EQ(restored.getGlobalId(7), low);
    EXPECT_EQ(restored.getGlobalId(bigCounter - 1), high);
    EXPECT_TRUE(restored.isValid(bigCounter - 1));

    // New assignments continue past the restored counter — no id reuse.
    SeriesId128 fresh = SeriesId128::fromSeriesKey("big_shard_new");
    EXPECT_EQ(restored.getOrAssign(fresh), bigCounter);
}

TEST_F(LocalIdMapTest, RestoreSkipsImplausiblyLargeEntryIds) {
    // A forward-key id implausibly far past the persisted counter is a corrupt
    // scan key: accepting it would drive a resize of up to 64 GB on the
    // startup path. It must be skipped (returns false), not grow the map and
    // not throw — one corrupt key must not brick the shard.
    LocalIdMap restored;
    restored.restoreBegin(100, 100);

    SeriesId128 good = SeriesId128::fromSeriesKey("plausible");
    SeriesId128 evil = SeriesId128::fromSeriesKey("corrupt_key");

    // Within the slack past the counter: accepted (crash-window tolerance).
    EXPECT_TRUE(restored.restoreEntry(100 + 10, good));
    EXPECT_EQ(restored.nextId(), 111u);

    // Far past the counter: rejected, state unchanged.
    EXPECT_FALSE(restored.restoreEntry(100 + LocalIdMap::kRestoreIdSlack, evil));
    EXPECT_EQ(restored.nextId(), 111u);
    EXPECT_FALSE(restored.getLocalId(evil).has_value());
}

TEST_F(LocalIdMapTest, ManySeriesRoundTrip) {
    LocalIdMap map;
    std::vector<SeriesId128> ids;

    for (int i = 0; i < 10000; ++i) {
        SeriesId128 id = SeriesId128::fromSeriesKey("bulk_series_" + std::to_string(i));
        ids.push_back(id);
        map.getOrAssign(id);
    }

    // Verify all round-trips
    for (uint32_t i = 0; i < 10000; ++i) {
        EXPECT_EQ(map.getGlobalId(i), ids[i]);
        auto localId = map.getLocalId(ids[i]);
        ASSERT_TRUE(localId.has_value());
        EXPECT_EQ(*localId, i);
    }
}

TEST_F(LocalIdMapTest, EmptyMapState) {
    LocalIdMap map;
    EXPECT_EQ(map.size(), 0u);
    EXPECT_EQ(map.nextId(), 0u);
}

TEST_F(LocalIdMapTest, GetGlobalIdOutOfBoundsReturnsZero) {
    LocalIdMap map;
    // Empty map: any localId is out of bounds
    const SeriesId128& result = map.getGlobalId(0);
    EXPECT_TRUE(result.isZero());

    const SeriesId128& result2 = map.getGlobalId(999);
    EXPECT_TRUE(result2.isZero());

    // With entries: beyond-end localId returns zero
    map.getOrAssign(SeriesId128::fromSeriesKey("s1"));
    map.getOrAssign(SeriesId128::fromSeriesKey("s2"));
    EXPECT_EQ(map.size(), 2u);

    const SeriesId128& valid = map.getGlobalId(0);
    EXPECT_FALSE(valid.isZero());

    const SeriesId128& oob = map.getGlobalId(2);
    EXPECT_TRUE(oob.isZero());

    const SeriesId128& oob2 = map.getGlobalId(UINT32_MAX);
    EXPECT_TRUE(oob2.isZero());
}
