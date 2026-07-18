#include "../../../lib/cluster/local_storage_placement.hpp"

#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <vector>

namespace {

using timestar::cluster::DesiredLocalVShardPlacement;
using timestar::cluster::StorageWorker;
using timestar::cluster::StorageWorkerId;
using timestar::cluster::StorageWorkerState;
using timestar::cluster::WorkerRuntimeMap;

static_assert(!std::is_default_constructible_v<DesiredLocalVShardPlacement>);

StorageWorker worker(uint32_t id, uint32_t weight = 1, StorageWorkerState state = StorageWorkerState::Active) {
    return StorageWorker{StorageWorkerId(id), weight, state};
}

std::vector<StorageWorker> equalWorkers(uint32_t count) {
    std::vector<StorageWorker> workers;
    workers.reserve(count);
    for (uint32_t id = 1; id <= count; ++id)
        workers.push_back(worker(id));
    return workers;
}

size_t countOwnedBy(const DesiredLocalVShardPlacement& placement, StorageWorkerId sought) {
    return static_cast<size_t>(std::count(placement.owners().begin(), placement.owners().end(), sought));
}

uint64_t ownerTableFingerprint(const DesiredLocalVShardPlacement& placement) {
    // FNV-1a over each uint32 worker ID encoded least-significant byte first.
    // The explicit encoding makes this compatibility fingerprint independent
    // of host byte order and object layout.
    uint64_t fingerprint = 14695981039346656037ULL;
    for (const auto owner : placement.owners()) {
        for (unsigned shift = 0; shift < 32; shift += 8) {
            fingerprint ^= (owner.value() >> shift) & 0xffU;
            fingerprint *= 1099511628211ULL;
        }
    }
    return fingerprint;
}

}  // namespace

TEST(LocalStoragePlacementTest, EveryVShardHasOneDesiredWorkerAndInputOrderDoesNotMatter) {
    const std::vector<StorageWorker> ordered{worker(1), worker(7), worker(42)};
    const std::vector<StorageWorker> shuffled{worker(42), worker(1), worker(7)};

    const auto first = DesiredLocalVShardPlacement::build(ordered);
    const auto second = DesiredLocalVShardPlacement::build(shuffled);

    EXPECT_EQ(first.owners(), second.owners());
    for (uint32_t vshard = 0; vshard < timestar::VIRTUAL_SHARD_COUNT; ++vshard) {
        EXPECT_TRUE(first.workerFor(static_cast<uint16_t>(vshard)).valid());
    }
    EXPECT_THROW(
        {
            const auto ignored = first.workerFor(timestar::VIRTUAL_SHARD_COUNT);
            (void)ignored;
        },
        std::out_of_range);
}

TEST(LocalStoragePlacementTest, AlgorithmVersionAndGoldenOwnersAreStable) {
    EXPECT_EQ(timestar::cluster::LOCAL_PLACEMENT_ALGORITHM_VERSION, 1u);
    const std::vector<StorageWorker> workers{worker(1), worker(7, 2), worker(42, 3)};
    const auto placement = DesiredLocalVShardPlacement::build(workers);

    // Version 1 golden vector. Any change requires a placement format/version
    // migration rather than silently remapping persisted VShards.
    const std::array<uint32_t, 16> expected{7, 1, 42, 42, 42, 7, 42, 42, 42, 42, 7, 7, 42, 42, 42, 42};
    for (uint16_t vshard = 0; vshard < expected.size(); ++vshard) {
        EXPECT_EQ(placement.workerFor(vshard).value(), expected[vshard]) << "vshard=" << vshard;
    }
    EXPECT_EQ(ownerTableFingerprint(placement), 0xc508ed4e2908d148ULL);
}

TEST(LocalStoragePlacementTest, AddingOneEqualWorkerOnlyMovesItsProportionalShareToIt) {
    for (uint32_t oldCount = 1; oldCount <= 8; ++oldCount) {
        const auto oldWorkers = equalWorkers(oldCount);
        auto newWorkers = oldWorkers;
        const StorageWorkerId added(oldCount + 1);
        newWorkers.push_back(worker(added.value()));

        const auto effective = DesiredLocalVShardPlacement::build(oldWorkers);
        const auto desired = DesiredLocalVShardPlacement::build(newWorkers);
        const auto delta = desired.deltaFromEffective(effective.owners());

        for (const auto& change : delta.changes) {
            EXPECT_EQ(change.desiredWorker, added);
            EXPECT_NE(change.effectiveWorker, added);
        }

        const double expectedMoves = static_cast<double>(timestar::VIRTUAL_SHARD_COUNT) / (oldCount + 1);
        EXPECT_NEAR(static_cast<double>(delta.changes.size()), expectedMoves, expectedMoves * 0.15)
            << "old worker count=" << oldCount;
    }
}

TEST(LocalStoragePlacementTest, DrainingWorkerRemainsRecordedAndOnlyItsVShardsNeedChange) {
    const auto activeWorkers = equalWorkers(4);
    const auto effective = DesiredLocalVShardPlacement::build(activeWorkers);
    const auto formerlyOwned = countOwnedBy(effective, StorageWorkerId(3));

    auto drainingWorkers = activeWorkers;
    drainingWorkers[2].state = StorageWorkerState::Draining;
    const auto desired = DesiredLocalVShardPlacement::build(drainingWorkers);
    const auto delta = desired.deltaFromEffective(effective.owners());

    EXPECT_EQ(delta.changes.size(), formerlyOwned);
    for (const auto& change : delta.changes) {
        EXPECT_EQ(change.effectiveWorker, StorageWorkerId(3));
        EXPECT_NE(change.desiredWorker, StorageWorkerId(3));
    }

    const auto runtime = WorkerRuntimeMap::build(drainingWorkers, 2);
    EXPECT_TRUE(runtime.coreFor(StorageWorkerId(3)).has_value());
}

TEST(LocalStoragePlacementTest, SupportedMaximumStillGivesAddedWorkerVShards) {
    const auto oldWorkers = equalWorkers(timestar::cluster::MAX_LOCAL_STORAGE_WORKERS - 1);
    auto newWorkers = oldWorkers;
    const StorageWorkerId added(timestar::cluster::MAX_LOCAL_STORAGE_WORKERS);
    newWorkers.push_back(worker(added.value()));

    const auto effective = DesiredLocalVShardPlacement::build(oldWorkers);
    const auto desired = DesiredLocalVShardPlacement::build(newWorkers);
    const auto delta = desired.deltaFromEffective(effective.owners());

    ASSERT_FALSE(delta.changes.empty());
    for (const auto& change : delta.changes)
        EXPECT_EQ(change.desiredWorker, added);
    const double expectedMoves =
        static_cast<double>(timestar::VIRTUAL_SHARD_COUNT) / timestar::cluster::MAX_LOCAL_STORAGE_WORKERS;
    EXPECT_NEAR(static_cast<double>(delta.changes.size()), expectedMoves, expectedMoves * 0.50);
}

TEST(LocalStoragePlacementTest, HardRemovalWhileWorkerStillOwnsVShardsFailsClosed) {
    const auto effective = DesiredLocalVShardPlacement::build(equalWorkers(3));
    const std::vector<StorageWorker> hardRemoved{worker(1), worker(2)};
    const auto unsafeDesired = DesiredLocalVShardPlacement::build(hardRemoved);

    EXPECT_THROW(
        {
            const auto ignored = unsafeDesired.deltaFromEffective(effective.owners());
            (void)ignored;
        },
        std::invalid_argument);
}

TEST(LocalStoragePlacementTest, MatchingEffectivePlacementProducesEmptyDelta) {
    const auto placement = DesiredLocalVShardPlacement::build(equalWorkers(3));
    EXPECT_TRUE(placement.deltaFromEffective(placement.owners()).changes.empty());
}

TEST(LocalStoragePlacementTest, IntegerTicketWeightsAreProportionalButAbsolute) {
    const auto oneToTwo = DesiredLocalVShardPlacement::build(std::vector<StorageWorker>{worker(1, 1), worker(2, 2)});
    const auto tenToTwenty =
        DesiredLocalVShardPlacement::build(std::vector<StorageWorker>{worker(1, 10), worker(2, 20)});

    const double expectedOne = static_cast<double>(timestar::VIRTUAL_SHARD_COUNT) / 3.0;
    EXPECT_NEAR(static_cast<double>(countOwnedBy(oneToTwo, StorageWorkerId(1))), expectedOne, expectedOne * 0.10);
    EXPECT_NEAR(static_cast<double>(countOwnedBy(oneToTwo, StorageWorkerId(2))), expectedOne * 2,
                expectedOne * 2 * 0.10);
    EXPECT_NE(oneToTwo.owners(), tenToTwenty.owners())
        << "weights are persisted absolute ticket counts and must never be ratio-normalized";
}

TEST(LocalStoragePlacementTest, InvalidWorkerRegistriesFailClosed) {
    EXPECT_THROW(DesiredLocalVShardPlacement::build(std::vector<StorageWorker>{}), std::invalid_argument);
    EXPECT_THROW(DesiredLocalVShardPlacement::build(std::vector<StorageWorker>{worker(0)}), std::invalid_argument);
    EXPECT_THROW(DesiredLocalVShardPlacement::build(std::vector<StorageWorker>{worker(1), worker(1)}),
                 std::invalid_argument);
    EXPECT_THROW(DesiredLocalVShardPlacement::build(std::vector<StorageWorker>{worker(1, 0)}), std::invalid_argument);
    EXPECT_THROW(DesiredLocalVShardPlacement::build(
                     std::vector<StorageWorker>{worker(1, timestar::cluster::MAX_STORAGE_WORKER_WEIGHT + 1)}),
                 std::invalid_argument);

    std::vector<StorageWorker> tooMany(timestar::cluster::MAX_LOCAL_STORAGE_WORKERS + 1);
    for (size_t index = 0; index < tooMany.size(); ++index)
        tooMany[index] = worker(static_cast<uint32_t>(index + 1));
    EXPECT_THROW(DesiredLocalVShardPlacement::build(tooMany), std::invalid_argument);

    std::vector<StorageWorker> tooManyTickets;
    for (uint32_t id = 1; id <= 9; ++id)
        tooManyTickets.push_back(worker(id, timestar::cluster::MAX_STORAGE_WORKER_WEIGHT));
    EXPECT_THROW(DesiredLocalVShardPlacement::build(tooManyTickets), std::invalid_argument);

    EXPECT_THROW(
        DesiredLocalVShardPlacement::build(std::vector<StorageWorker>{worker(1, 1, StorageWorkerState::Draining)}),
        std::invalid_argument);
    EXPECT_THROW(DesiredLocalVShardPlacement::build(std::vector<StorageWorker>{
                     worker(1, 1, static_cast<StorageWorkerState>(std::numeric_limits<uint8_t>::max()))}),
                 std::invalid_argument);
}

TEST(LocalStoragePlacementTest, ActiveWorkerBelowVShardGranularityFailsClosed) {
    const std::vector<StorageWorker> workers{worker(182, 1), worker(1001, 512), worker(1002, 512)};

    EXPECT_THROW(DesiredLocalVShardPlacement::build(workers), std::invalid_argument);
}

TEST(LocalStoragePlacementTest, InvalidEffectiveOwnerFailsClosed) {
    const auto placement = DesiredLocalVShardPlacement::build(equalWorkers(2));
    auto effective = placement.owners();
    effective[17] = StorageWorkerId();
    EXPECT_THROW(
        {
            const auto ignored = placement.deltaFromEffective(effective);
            (void)ignored;
        },
        std::invalid_argument);
}

TEST(LocalStoragePlacementTest, RuntimeMapIsOrderIndependentAndCoSchedulesEveryPersistedWorker) {
    const std::vector<StorageWorker> ordered{worker(1), worker(2), worker(3, 1, StorageWorkerState::Draining),
                                             worker(4)};
    const std::vector<StorageWorker> shuffled{ordered[3], ordered[1], ordered[0], ordered[2]};

    const auto first = WorkerRuntimeMap::build(ordered, 2);
    const auto second = WorkerRuntimeMap::build(shuffled, 2);
    EXPECT_EQ(first.assignments().size(), 4u);
    EXPECT_EQ(first.assignments().size(), second.assignments().size());
    for (uint32_t id = 1; id <= 4; ++id) {
        EXPECT_EQ(first.coreFor(StorageWorkerId(id)), second.coreFor(StorageWorkerId(id)));
        EXPECT_EQ(first.coreFor(StorageWorkerId(id)), (id - 1) % 2);
    }
    EXPECT_FALSE(first.coreFor(StorageWorkerId(99)).has_value());
}

TEST(LocalStoragePlacementTest, RuntimeMapBalancesWorkerCapacityTickets) {
    const std::vector<StorageWorker> workers{worker(1, 1024), worker(2, 1), worker(3, 1024), worker(4, 1)};
    const auto runtime = WorkerRuntimeMap::build(workers, 2);

    ASSERT_TRUE(runtime.coreFor(StorageWorkerId(1)).has_value());
    ASSERT_TRUE(runtime.coreFor(StorageWorkerId(3)).has_value());
    EXPECT_NE(runtime.coreFor(StorageWorkerId(1)), runtime.coreFor(StorageWorkerId(3)));

    std::array<uint32_t, 2> loads{};
    for (const auto& record : workers)
        loads[*runtime.coreFor(record.id)] += record.weight;
    EXPECT_EQ(loads[0], loads[1]);
}

TEST(LocalStoragePlacementTest, AddingOneWorkerAndCoreKeepsExistingRuntimeAssignments) {
    const auto fourWorkers = equalWorkers(4);
    auto fiveWorkers = fourWorkers;
    fiveWorkers.push_back(worker(5));

    const auto before = WorkerRuntimeMap::build(fourWorkers, 4);
    const auto after = WorkerRuntimeMap::build(fiveWorkers, 5);
    for (uint32_t id = 1; id <= 4; ++id)
        EXPECT_EQ(before.coreFor(StorageWorkerId(id)), after.coreFor(StorageWorkerId(id)));
    EXPECT_EQ(after.coreFor(StorageWorkerId(5)), 4u);
}

TEST(LocalStoragePlacementTest, RuntimeMapRejectsZeroCoresAndInvalidRegistry) {
    const auto workers = equalWorkers(2);
    EXPECT_THROW(WorkerRuntimeMap::build(workers, 0), std::invalid_argument);
    EXPECT_THROW(WorkerRuntimeMap::build(std::vector<StorageWorker>{worker(1), worker(1)}, 2), std::invalid_argument);
}

TEST(LocalStoragePlacementTest, DesiredPlacementConstructionDoesNotChangeLegacyRouting) {
    const auto id = SeriesId128::fromSeriesKey("legacy,host=a value");
    const auto coreCountBefore = timestar::placement().coreCount();
    const auto routeBefore = timestar::routeToCore(id);

    [[maybe_unused]] const auto desired = DesiredLocalVShardPlacement::build(equalWorkers(8));

    EXPECT_EQ(timestar::routeToCore(id), routeBefore);
    EXPECT_EQ(timestar::placement().coreCount(), coreCountBefore);
}
