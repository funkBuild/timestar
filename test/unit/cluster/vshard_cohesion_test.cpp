// The single-core-snapshot precondition: vshardsCohesiveOnCores(c) must be TRUE
// exactly when the real per-series router (PlacementTable::coreForHash = hash %
// cores) agrees with assignCore(virtualShard(hash)) for EVERY hash -- i.e. when a
// VShard's data provably lives on one core. This test cross-checks the derived
// formula (c | 4096) against the actual routing functions, so a future change to
// either router can't silently break the snapshot safety gate. See memory
// snapshot-wiring-blocker.
#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/vshard.hpp"

#include <gtest/gtest.h>

using timestar::assignCore;
using timestar::PlacementTable;
using timestar::VShardId;
using timestar::vshardsCohesiveOnCores;

namespace {
// Does routeToCore agree with assignCore(vshard) for every sampled hash at this
// core count? Sample hashes as low(0..4095) + high*4096 so both the vshard bits
// (low 12) and the high bits (which hash%cores sees but assignCore does not) vary.
bool routingAgreesEverywhere(unsigned coreCount) {
    PlacementTable table = PlacementTable::buildLocal(coreCount);
    for (size_t high : {size_t{0}, size_t{1}, size_t{7}, size_t{255}, size_t{4096}, size_t{1048577}}) {
        for (size_t low = 0; low < timestar::VIRTUAL_SHARD_COUNT; ++low) {
            size_t hash = low + high * timestar::VIRTUAL_SHARD_COUNT;
            unsigned routed = table.coreForHash(hash);
            unsigned assigned = assignCore(VShardId{PlacementTable::vshardForHash(hash)}, coreCount);
            if (routed != assigned)
                return false;
        }
    }
    return true;
}
}  // namespace

TEST(VShardCohesion, PredicateMatchesRealRoutingAcrossCoreCounts) {
    for (unsigned c = 1; c <= 64; ++c)
        EXPECT_EQ(vshardsCohesiveOnCores(c), routingAgreesEverywhere(c))
            << "core count " << c << ": predicate disagrees with actual routeToCore/assignCore agreement";
}

TEST(VShardCohesion, PowersOfTwoUpTo4096AreCohesive) {
    for (unsigned c : {1u, 2u, 4u, 8u, 16u, 32u, 64u, 128u, 256u, 512u, 1024u, 2048u, 4096u})
        EXPECT_TRUE(vshardsCohesiveOnCores(c)) << "power of two " << c;
}

TEST(VShardCohesion, NonDivisorsAreNotCohesive) {
    for (unsigned c : {3u, 5u, 6u, 7u, 10u, 12u, 24u, 48u, 96u, 100u, 6000u})
        EXPECT_FALSE(vshardsCohesiveOnCores(c)) << "non-divisor of 4096: " << c;
}

TEST(VShardCohesion, ZeroCoresIsNotCohesive) {
    EXPECT_FALSE(vshardsCohesiveOnCores(0));
}
