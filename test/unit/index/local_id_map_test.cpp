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

    // Build restore data
    std::vector<std::pair<uint32_t, SeriesId128>> mappings;
    for (uint32_t i = 0; i < 50; ++i) {
        mappings.emplace_back(i, ids[i]);
    }

    // Restore into fresh map
    LocalIdMap restored;
    restored.restore(50, std::move(mappings));

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
