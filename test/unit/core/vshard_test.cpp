// Golden + property tests for the derived VShard->core assignment
// (docs/clustering-vshard-workers.md, "Derived assignment rules").
//
// assignCore is THE single definition of which reactor core executes a VShard.
// The golden vectors below are a behavioural contract: changing them changes
// which core replays which VShard's journal after a core-count change and must
// be a deliberate, reviewed change -- not an accident.

#include "../../../lib/core/vshard.hpp"

#include "../../../lib/core/placement_table.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace {

using timestar::assignCore;
using timestar::VIRTUAL_SHARD_COUNT;
using timestar::VShardId;

// Determinism across platforms: the mapping is a constant expression, so these
// values are fixed at compile time regardless of target. If any fails to
// compile the formula changed.
static_assert(assignCore(VShardId{0}, 1) == 0);
static_assert(assignCore(VShardId{4095}, 1) == 0);
static_assert(assignCore(VShardId{0}, 4) == 0);
static_assert(assignCore(VShardId{1}, 4) == 1);
static_assert(assignCore(VShardId{4}, 4) == 0);
static_assert(assignCore(VShardId{4095}, 4) == 3);
static_assert(assignCore(VShardId{4095}, 8) == 7);
static_assert(assignCore(VShardId{100}, 6) == 4);

TEST(VShardTest, GoldenVectors) {
    // A frozen table of (vshard, coreCount) -> core. Update only with review.
    struct Case {
        uint16_t vshard;
        unsigned coreCount;
        unsigned core;
    };
    constexpr std::array<Case, 14> golden = {{
        {0, 1, 0},
        {4095, 1, 0},
        {0, 4, 0},
        {1, 4, 1},
        {3, 4, 3},
        {4, 4, 0},
        {4095, 4, 3},
        {0, 8, 0},
        {7, 8, 7},
        {8, 8, 0},
        {4095, 8, 7},
        {4095, 3, 0},  // 3 divides 4095
        {4095, 7, 0},  // 7 divides 4095
        {100, 6, 4},
    }};
    for (const auto& c : golden) {
        EXPECT_EQ(assignCore(VShardId{c.vshard}, c.coreCount), c.core)
            << "vshard=" << c.vshard << " coreCount=" << c.coreCount;
    }
}

TEST(VShardTest, EveryVShardMapsIntoRange) {
    for (unsigned coreCount : {1u, 2u, 3u, 4u, 6u, 7u, 8u, 16u, 64u, 512u, 4096u}) {
        for (uint16_t v = 0; v < VIRTUAL_SHARD_COUNT; ++v) {
            EXPECT_LT(assignCore(VShardId{v}, coreCount), coreCount) << "vshard=" << v << " coreCount=" << coreCount;
        }
    }
}

TEST(VShardTest, SpreadIsEvenWithinOne) {
    for (unsigned coreCount : {1u, 2u, 3u, 4u, 5u, 6u, 7u, 8u, 10u, 16u, 24u, 64u, 4096u}) {
        std::vector<unsigned> load(coreCount, 0);
        for (uint16_t v = 0; v < VIRTUAL_SHARD_COUNT; ++v)
            ++load[assignCore(VShardId{v}, coreCount)];

        const unsigned expectedFloor = VIRTUAL_SHARD_COUNT / coreCount;
        const unsigned remainder = VIRTUAL_SHARD_COUNT % coreCount;
        unsigned atCeil = 0;
        for (unsigned core = 0; core < coreCount; ++core) {
            EXPECT_GE(load[core], expectedFloor) << "coreCount=" << coreCount << " core=" << core;
            EXPECT_LE(load[core], expectedFloor + (remainder ? 1u : 0u)) << "coreCount=" << coreCount;
            if (load[core] == expectedFloor + 1)
                ++atCeil;
        }
        // Exactly `remainder` cores carry the extra VShard.
        EXPECT_EQ(atCeil, remainder) << "coreCount=" << coreCount;
    }
}

TEST(VShardTest, ZeroCoreCountIsRejected) {
    EXPECT_THROW((void)assignCore(VShardId{0}, 0), std::invalid_argument);
}

TEST(VShardTest, VShardIdValidityIsBoundedByTheFrozenRange) {
    EXPECT_TRUE(VShardId{0}.valid());
    EXPECT_TRUE(VShardId{VIRTUAL_SHARD_COUNT - 1}.valid());
    EXPECT_FALSE(VShardId{VIRTUAL_SHARD_COUNT}.valid());
    EXPECT_EQ(VShardId{42}.value(), 42);
}

// The PlacementTable's vshard->core accessor must agree with assignCore: there
// is one authority for the rule.
TEST(VShardTest, PlacementTableMappingUsesTheSameAssignment) {
    for (unsigned coreCount : {1u, 2u, 3u, 4u, 8u, 16u}) {
        const auto pt = timestar::PlacementTable::buildLocal(coreCount);
        for (uint16_t v : {uint16_t{0}, uint16_t{1}, uint16_t{100}, uint16_t{4095}}) {
            EXPECT_EQ(pt.mapping(v).coreId, assignCore(VShardId{v}, coreCount))
                << "vshard=" << v << " coreCount=" << coreCount;
        }
    }
}

}  // namespace
