// Unit tests for the PlacementTable virtual shard abstraction (Phase 5).

#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <set>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// buildLocal tests
// ---------------------------------------------------------------------------

TEST(PlacementTableTest, BuildLocalDistributesEvenly) {
    for (unsigned N : {1, 2, 4, 8, 16, 32, 64}) {
        auto pt = timestar::PlacementTable::buildLocal(N);
        EXPECT_EQ(pt.coreCount(), N);

        // Count how many vshards map to each core
        std::vector<unsigned> counts(N, 0);
        for (uint16_t v = 0; v < timestar::VIRTUAL_SHARD_COUNT; ++v) {
            auto coreId = pt.mapping(v).coreId;
            EXPECT_LT(coreId, N);
            EXPECT_EQ(pt.mapping(v).serverId, 0);
            counts[coreId]++;
        }

        // Each core should own V/N ± 1 vshards
        unsigned expected = timestar::VIRTUAL_SHARD_COUNT / N;
        for (unsigned c = 0; c < N; ++c) {
            EXPECT_GE(counts[c], expected) << "core " << c << " with N=" << N;
            EXPECT_LE(counts[c], expected + 1) << "core " << c << " with N=" << N;
        }
    }
}

// ---------------------------------------------------------------------------
// coreForHash tests
// ---------------------------------------------------------------------------

TEST(PlacementTableTest, CoreForHashEqualsModulo) {
    for (unsigned N : {1, 2, 4, 7, 8, 16, 32, 64}) {
        auto pt = timestar::PlacementTable::buildLocal(N);
        for (size_t h = 0; h < 10000; ++h) {
            unsigned expected = N <= 1 ? 0 : static_cast<unsigned>(h % N);
            EXPECT_EQ(pt.coreForHash(h), expected)
                << "N=" << N << " hash=" << h;
        }
    }
}

// ---------------------------------------------------------------------------
// routeToCore equivalence test
// ---------------------------------------------------------------------------

TEST(PlacementTableTest, RouteToCoreMatchesManualHash) {
    unsigned N = 4;
    auto pt = timestar::PlacementTable::buildLocal(N);
    timestar::setGlobalPlacement(std::move(pt));

    std::vector<std::string> keys = {
        "cpu,host=server01 usage",
        "memory,host=server02 free",
        "disk,dc=us-east,host=db01 iops",
        "temperature,location=us-west value",
        "network,host=router01 bytes_in",
    };

    for (const auto& key : keys) {
        SeriesId128 id = SeriesId128::fromSeriesKey(key);
        size_t hash = SeriesId128::Hash{}(id);
        unsigned expected = static_cast<unsigned>(hash % N);
        EXPECT_EQ(timestar::routeToCore(id), expected)
            << "key=" << key;
    }
}

// ---------------------------------------------------------------------------
// vshardForHash tests
// ---------------------------------------------------------------------------

TEST(PlacementTableTest, VshardForHashMasks12Bits) {
    for (size_t h = 0; h < 100000; h += 37) {
        uint16_t vs = timestar::PlacementTable::vshardForHash(h);
        EXPECT_EQ(vs, static_cast<uint16_t>(h & 0xFFF));
        EXPECT_LT(vs, timestar::VIRTUAL_SHARD_COUNT);
    }
}

TEST(PlacementTableTest, VirtualShardConvenience) {
    SeriesId128 id = SeriesId128::fromSeriesKey("test,tag=value field");
    size_t hash = SeriesId128::Hash{}(id);
    EXPECT_EQ(timestar::virtualShard(id),
              static_cast<uint16_t>(hash & timestar::VIRTUAL_SHARD_MASK));
}

// ---------------------------------------------------------------------------
// JSON round-trip tests
// ---------------------------------------------------------------------------

TEST(PlacementTableTest, JsonRoundTrip) {
    auto original = timestar::PlacementTable::buildLocal(16);
    std::string json = original.toJson();
    EXPECT_FALSE(json.empty());

    auto restored = timestar::PlacementTable::fromJson(json);
    EXPECT_EQ(restored.coreCount(), original.coreCount());

    // Verify every vshard mapping matches
    for (uint16_t v = 0; v < timestar::VIRTUAL_SHARD_COUNT; ++v) {
        EXPECT_EQ(restored.mapping(v).serverId, original.mapping(v).serverId)
            << "vshard=" << v;
        EXPECT_EQ(restored.mapping(v).coreId, original.mapping(v).coreId)
            << "vshard=" << v;
    }
}

TEST(PlacementTableTest, JsonRoundTripSingleCore) {
    auto original = timestar::PlacementTable::buildLocal(1);
    auto restored = timestar::PlacementTable::fromJson(original.toJson());
    EXPECT_EQ(restored.coreCount(), 1u);
    for (uint16_t v = 0; v < timestar::VIRTUAL_SHARD_COUNT; ++v) {
        EXPECT_EQ(restored.mapping(v).coreId, 0);
    }
}

// ---------------------------------------------------------------------------
// Default / uninitialized placement
// ---------------------------------------------------------------------------

TEST(PlacementTableTest, DefaultPlacementRoutesToZero) {
    // A default-constructed PlacementTable (coreCount_ = 0) routes everything to core 0.
    timestar::PlacementTable pt;
    for (size_t h = 0; h < 1000; ++h) {
        EXPECT_EQ(pt.coreForHash(h), 0u);
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

TEST(PlacementTableTest, Constants) {
    EXPECT_EQ(timestar::VIRTUAL_SHARD_COUNT, 4096);
    EXPECT_EQ(timestar::VIRTUAL_SHARD_MASK, 4095);
    // Must be power of 2
    EXPECT_EQ(timestar::VIRTUAL_SHARD_COUNT & (timestar::VIRTUAL_SHARD_COUNT - 1), 0);
}
