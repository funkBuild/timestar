// D-11: the EXPLICIT VShard -> Raft-group id in the VShard directory (ADR 0004's
// recommended prep step for a possible later N:1 consolidation).
//
// Two things have to be true at once: identity remains the default, while a
// NON-identity mapping must actually change where a VShard's Raft work is routed --
// otherwise the accessor is decoration, every call
//     site still assumes the identity, and the migration it was added to enable would
//     still be a rebuild. The routing tests below therefore assert BOTH that a
//     non-identity map co-locates a group's VShards AND that the identity map does not
//     -- a test that only asserted the first would pass against a hard-coded identity.
#include "../../../lib/cluster/control/control_map_cache.hpp"
#include "../../../lib/cluster/data/vshard_directory.hpp"
#include "../../../lib/cluster/integration/shard_raft_plane.hpp"
#include "../../../lib/core/vshard.hpp"

#include <gtest/gtest.h>

#include <set>

using timestar::assignCore;
using timestar::VShardId;
using timestar::cluster::bucketByOwningShard;
using timestar::cluster::kControlRaftGroupId;
using timestar::cluster::shardForGroup;
using timestar::cluster::shardOwningVShard;
using timestar::control::ControlMap;
using timestar::control::ControlMapCache;
using timestar::data::VShardDirectory;

namespace {

// 64 VShards over 4 groups of 16 -- the K = 16 shape ADR 0004 sketches.
constexpr uint16_t kVShards = 64;
constexpr uint16_t kK = 16;

ControlMap placementOnly() {
    ControlMap m;
    m.epoch = 7;
    for (uint16_t vs = 0; vs < kVShards; ++vs)
        m.placement[vs] = {static_cast<timestar::raft::NodeId>((vs % 3) + 1)};
    return m;
}

ControlMap consolidated() {
    ControlMap m = placementOnly();
    for (uint16_t vs = 0; vs < kVShards; ++vs)
        m.groups[vs] = static_cast<uint16_t>(vs / kK);
    return m;
}

// The core-count-independent form of shardForGroup, so the discrimination assertions
// below do not depend on how many reactor shards the test binary happens to run with.
unsigned coreOf(uint16_t id, unsigned cores) {
    return assignCore(VShardId{id}, cores);
}

}  // namespace

// ---------------------------------------------------------------------------
// 1. Identity is the default.
// ---------------------------------------------------------------------------

TEST(VShardGroupDirectoryTest, IdentityIsTheDefault) {
    const auto map = placementOnly();
    VShardDirectory dir(1, map);
    EXPECT_TRUE(dir.identityGroupMapping());
    for (uint16_t vs = 0; vs < kVShards; ++vs)
        EXPECT_EQ(dir.groupOf(vs), vs) << vs;
    // A VShard absent from the placement map entirely still has a group: the mapping is
    // total, and an absent entry means identity, not "unassigned". Routing a Raft
    // envelope and owning the data are different questions (ownerOf answers the latter).
    EXPECT_EQ(dir.groupOf(4095), 4095);

    ControlMapCache source, restored;
    ASSERT_TRUE(source.update(map));
    const auto encoded = source.serialize();
    ASSERT_TRUE(restored.load(encoded));
    EXPECT_EQ(restored.current(), map);
    ControlMapCache truncated;
    EXPECT_FALSE(truncated.load(encoded.substr(0, encoded.size() - 8)))
        << "the mandatory zero group count may not be omitted";
}

// ---------------------------------------------------------------------------
// 2. The current v1 mapping survives a restart and malformed frames fail closed.
// ---------------------------------------------------------------------------

TEST(VShardGroupDirectoryTest, NonIdentityMappingRoundTripsThroughTheDurableCache) {
    ControlMapCache src;
    ASSERT_TRUE(src.update(consolidated()));
    ControlMapCache dst;
    ASSERT_TRUE(dst.load(src.serialize()));
    EXPECT_EQ(dst.current(), src.current());
    EXPECT_EQ(dst.current().groups.size(), kVShards);
    EXPECT_EQ(VShardDirectory(1, dst.current()).groupOf(17), 1);
}

TEST(VShardGroupDirectoryTest, MalformedV1FailsClosedAndLeavesTheCacheUntouched) {
    ControlMapCache src;
    ASSERT_TRUE(src.update(consolidated()));
    const std::string good = src.serialize();

    // The cache we are corrupting into: it already holds a valid identity map, and a
    // rejected load must leave THAT in place rather than half-adopting the new one.
    auto freshCache = [] {
        ControlMapCache c;
        c.update(placementOnly());
        return c;
    };
    const ControlMap intact = freshCache().current();

    {  // truncated group entries
        ControlMapCache c = freshCache();
        EXPECT_FALSE(c.load(good.substr(0, good.size() - 4)));
        EXPECT_EQ(c.current(), intact);
    }
    {  // impossible group count
        ControlMapCache c = freshCache();
        std::string bad = good;
        const size_t groupCountOffset = bad.size() - (8 + kVShards * 4);
        for (size_t i = 0; i < 8; ++i)
            bad[groupCountOffset + i] = static_cast<char>(0xff);
        EXPECT_FALSE(c.load(bad));
        EXPECT_EQ(c.current(), intact);
    }
    {  // trailing bytes
        ControlMapCache c = freshCache();
        EXPECT_FALSE(c.load(good + std::string(3, '\0')));
        EXPECT_EQ(c.current(), intact);
    }
    {  // the untouched original still loads, so the assertions above are not vacuous
        ControlMapCache c = freshCache();
        EXPECT_TRUE(c.load(good));
        EXPECT_EQ(c.current().groups.size(), kVShards);
    }
}

// ---------------------------------------------------------------------------
// 3. The indirection is REAL: a non-identity mapping changes routing.
// ---------------------------------------------------------------------------

TEST(VShardGroupDirectoryTest, NonIdentityMappingCoLocatesAGroupsVShards) {
    const VShardDirectory consolidatedDir(1, consolidated());
    const VShardDirectory identityDir(1, placementOnly());

    // Under consolidation every VShard of a group MUST land on the shard that owns that
    // group -- the group has ONE Raft log, one lock, one journal, and it lives on one
    // reactor. This is the property the whole prep step exists to make expressible.
    for (uint16_t g = 0; g < kVShards / kK; ++g) {
        std::set<unsigned> shards;
        for (uint16_t vs = g * kK; vs < (g + 1) * kK; ++vs) {
            EXPECT_EQ(consolidatedDir.groupOf(vs), g) << vs;
            shards.insert(shardOwningVShard(vs, &consolidatedDir));
        }
        ASSERT_EQ(shards.size(), 1u) << "group " << g << " is split across reactors";
        EXPECT_EQ(*shards.begin(), shardForGroup(g));
    }

    // DISCRIMINATION. The same 16 VShards under the IDENTITY mapping spread over the
    // cores, so the assertion above is testing the mapping and not an accident of
    // assignCore. Stated at an explicit core count so it holds however many reactor
    // shards this test binary runs with (at smp::count == 1 everything is core 0 and
    // there would be nothing to discriminate).
    for (unsigned cores : {2u, 4u, 8u}) {
        std::set<unsigned> identityCores, groupCores;
        for (uint16_t vs = 0; vs < kK; ++vs) {
            identityCores.insert(coreOf(identityDir.groupOf(vs), cores));
            groupCores.insert(coreOf(consolidatedDir.groupOf(vs), cores));
        }
        EXPECT_GT(identityCores.size(), 1u) << "cores=" << cores;
        EXPECT_EQ(groupCores.size(), 1u) << "cores=" << cores;
    }
}

TEST(VShardGroupDirectoryTest, TheProductionBucketerHonoursTheGroupMapping) {
    // bucketByOwningShard is the single funnel every write/propose fan-out uses
    // (writeSlicesToOwningShards, proposeSlicesToOwningShards and its hinted twin), so
    // testing it tests all three -- and a future fan-out that open-codes the map insert
    // instead is the regression this is aimed at.
    auto groupsOf = [](const VShardDirectory* dir) {
        timestar::data::VShardBatches split;
        for (uint16_t vs = 0; vs < kK; ++vs)
            split.emplace_back(vs, timestar::data::WriteBatch{});
        std::map<unsigned, std::vector<uint16_t>> out;
        for (const auto& [shard, slice] : bucketByOwningShard(std::move(split), dir))
            for (const auto& g : slice)
                out[shard].push_back(g.first);
        return out;
    };

    const VShardDirectory consolidatedDir(1, consolidated());
    // All 16 VShards of group 0 in ONE bucket, keyed by the shard owning group 0.
    const auto bucketed = groupsOf(&consolidatedDir);
    ASSERT_EQ(bucketed.size(), 1u);
    EXPECT_EQ(bucketed.begin()->first, shardForGroup(0));
    EXPECT_EQ(bucketed.begin()->second.size(), kK);

    // Discriminating only when there is more than one reactor to spread across; below
    // that the identity and the mapping are indistinguishable by construction and the
    // explicit-core-count assertions in the previous test carry the weight.
    if (seastar::smp::count > 1) {
        const VShardDirectory identityDir(1, placementOnly());
        EXPECT_GT(groupsOf(&identityDir).size(), 1u);
        // A null directory means the identity -- the shape the fan-out unit tests use.
        EXPECT_EQ(groupsOf(nullptr).size(), groupsOf(&identityDir).size());
    }
}

TEST(VShardGroupDirectoryTest, ControlGroupHasANonCollidingWireIdAndRoutesToShardZero) {
    // Data VShard 0 has always used Raft envelope group id 0. The control group
    // must not overload that id: both groups can be live on shard 0 at once and
    // the delivery path needs an unambiguous discriminator.
    EXPECT_EQ(VShardDirectory(1, placementOnly()).groupOf(0), 0);
    EXPECT_NE(kControlRaftGroupId, 0);
    EXPECT_EQ(kControlRaftGroupId, timestar::VIRTUAL_SHARD_COUNT);
    EXPECT_EQ(shardForGroup(kControlRaftGroupId), 0u);
}
