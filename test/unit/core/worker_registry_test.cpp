#include "../../../lib/cluster/worker_registry.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using timestar::cluster::createWorkerRegistry;
using timestar::cluster::decodeWorkerRegistryJson;
using timestar::cluster::encodeWorkerRegistryJson;
using timestar::cluster::reconcileWorkerRegistry;
using timestar::cluster::StorageWorker;
using timestar::cluster::StorageWorkerId;
using timestar::cluster::StorageWorkerState;
using timestar::cluster::validateWorkerRegistry;
using timestar::cluster::validateWorkerRegistryTransition;
using timestar::cluster::WorkerRegistry;
using timestar::cluster::WorkerRuntimeMap;

unsigned activeCount(const WorkerRegistry& registry) {
    return static_cast<unsigned>(
        std::count_if(registry.workers.begin(), registry.workers.end(),
                      [](const StorageWorker& worker) { return worker.state == StorageWorkerState::Active; }));
}

}  // namespace

TEST(WorkerRegistryTest, FreshRegistryHasStableMonotonicIdentities) {
    const auto registry = createWorkerRegistry(3);

    EXPECT_EQ(registry.formatVersion, 1u);
    EXPECT_EQ(registry.localPlacementAlgorithmVersion, 1u);
    EXPECT_EQ(registry.generation, 1u);
    EXPECT_EQ(registry.nextWorkerId, 4u);
    ASSERT_EQ(registry.workers.size(), 3u);
    for (uint32_t index = 0; index < registry.workers.size(); ++index) {
        EXPECT_EQ(registry.workers[index].id, StorageWorkerId(index + 1));
        EXPECT_EQ(registry.workers[index].weight, 1u);
        EXPECT_EQ(registry.workers[index].state, StorageWorkerState::Active);
    }
}

TEST(WorkerRegistryTest, CanonicalJsonAndChecksumAreGoldenFormatV1) {
    const auto encoded = encodeWorkerRegistryJson(createWorkerRegistry(2));

    EXPECT_EQ(
        encoded,
        R"({"formatVersion":1,"localPlacementAlgorithmVersion":1,"generation":1,"nextWorkerId":3,"workers":[{"id":1,"weight":1,"state":0},{"id":2,"weight":1,"state":0}],"checksum":1368420888})");
}

TEST(WorkerRegistryTest, CanonicalJsonRoundTrips) {
    const auto original = reconcileWorkerRegistry(createWorkerRegistry(4), 2);
    const auto encoded = encodeWorkerRegistryJson(original);

    EXPECT_EQ(decodeWorkerRegistryJson(encoded), original);
}

TEST(WorkerRegistryTest, ShrinkDrainsHighestIdsWithoutRemovingTheirRuntimeMapping) {
    const auto original = createWorkerRegistry(5);
    const auto shrunk = reconcileWorkerRegistry(original, 3);

    EXPECT_EQ(shrunk.generation, 2u);
    EXPECT_EQ(shrunk.nextWorkerId, 6u);
    EXPECT_EQ(activeCount(shrunk), 3u);
    EXPECT_EQ(shrunk.workers[3].state, StorageWorkerState::Draining);
    EXPECT_EQ(shrunk.workers[4].state, StorageWorkerState::Draining);

    const auto runtime = WorkerRuntimeMap::build(shrunk.workers, 2);
    EXPECT_TRUE(runtime.coreFor(StorageWorkerId(4)).has_value());
    EXPECT_TRUE(runtime.coreFor(StorageWorkerId(5)).has_value());
}

TEST(WorkerRegistryTest, GrowthReactivatesLowestDrainingIdsBeforeAppending) {
    const auto original = createWorkerRegistry(5);
    const auto shrunk = reconcileWorkerRegistry(original, 2);
    const auto partiallyRegrown = reconcileWorkerRegistry(shrunk, 4);
    const auto fullyRegrown = reconcileWorkerRegistry(partiallyRegrown, 5);
    const auto expanded = reconcileWorkerRegistry(fullyRegrown, 6);

    EXPECT_EQ(partiallyRegrown.workers[2].state, StorageWorkerState::Active);
    EXPECT_EQ(partiallyRegrown.workers[3].state, StorageWorkerState::Active);
    EXPECT_EQ(partiallyRegrown.workers[4].state, StorageWorkerState::Draining);
    EXPECT_EQ(fullyRegrown.workers.size(), 5u);
    ASSERT_EQ(expanded.workers.size(), 6u);
    EXPECT_EQ(expanded.workers.back().id, StorageWorkerId(6));
    EXPECT_EQ(expanded.nextWorkerId, 7u);
    EXPECT_EQ(expanded.generation, 5u);
}

TEST(WorkerRegistryTest, IncrementalGrowthAdvancesOneGenerationAndOneIdentityAtATime) {
    auto registry = createWorkerRegistry(1);
    for (uint32_t requested = 2; requested <= 16; ++requested) {
        const auto previous = registry;
        registry = reconcileWorkerRegistry(registry, requested);
        EXPECT_EQ(registry.generation, previous.generation + 1);
        EXPECT_EQ(registry.nextWorkerId, previous.nextWorkerId + 1);
        EXPECT_EQ(registry.workers.back().id, StorageWorkerId(requested));
        EXPECT_NO_THROW(validateWorkerRegistryTransition(previous, registry));
    }
}

TEST(WorkerRegistryTest, NoOpKeepsEveryByteAndGenerationStable) {
    const auto current = reconcileWorkerRegistry(createWorkerRegistry(4), 2);
    const auto unchanged = reconcileWorkerRegistry(current, 2);

    EXPECT_EQ(unchanged, current);
    EXPECT_EQ(encodeWorkerRegistryJson(unchanged), encodeWorkerRegistryJson(current));
    EXPECT_NO_THROW(validateWorkerRegistryTransition(current, unchanged));
}

TEST(WorkerRegistryTest, InvalidCountsAndGenerationOverflowFailClosed) {
    EXPECT_THROW(
        {
            const auto ignored = createWorkerRegistry(0);
            (void)ignored;
        },
        std::invalid_argument);
    EXPECT_THROW(
        {
            const auto ignored = createWorkerRegistry(timestar::cluster::MAX_LOCAL_STORAGE_WORKERS + 1);
            (void)ignored;
        },
        std::invalid_argument);

    const auto current = createWorkerRegistry(2);
    EXPECT_THROW(
        {
            const auto ignored = reconcileWorkerRegistry(current, 0);
            (void)ignored;
        },
        std::invalid_argument);
    EXPECT_THROW(
        {
            const auto ignored = reconcileWorkerRegistry(current, timestar::cluster::MAX_LOCAL_STORAGE_WORKERS + 1);
            (void)ignored;
        },
        std::invalid_argument);

    auto exhausted = current;
    exhausted.generation = std::numeric_limits<uint64_t>::max();
    EXPECT_THROW(
        {
            const auto ignored = reconcileWorkerRegistry(exhausted, 1);
            (void)ignored;
        },
        std::overflow_error);
}

TEST(WorkerRegistryTest, RegistryValidationReusesPlacementGranularityRules) {
    WorkerRegistry registry;
    registry.generation = 1;
    registry.nextWorkerId = 1003;
    registry.workers = {
        StorageWorker{StorageWorkerId(182), 1, StorageWorkerState::Active},
        StorageWorker{StorageWorkerId(1001), 512, StorageWorkerState::Active},
        StorageWorker{StorageWorkerId(1002), 512, StorageWorkerState::Active},
    };

    EXPECT_THROW(validateWorkerRegistry(registry), std::invalid_argument);
    EXPECT_THROW(
        {
            const auto ignored = encodeWorkerRegistryJson(registry);
            (void)ignored;
        },
        std::invalid_argument);
}

TEST(WorkerRegistryTest, RegistryRejectsNonCanonicalOrderAndInvalidNextId) {
    auto registry = createWorkerRegistry(3);
    std::swap(registry.workers[0], registry.workers[1]);
    EXPECT_THROW(validateWorkerRegistry(registry), std::invalid_argument);

    registry = createWorkerRegistry(3);
    ++registry.nextWorkerId;
    EXPECT_THROW(validateWorkerRegistry(registry), std::invalid_argument);
}

TEST(WorkerRegistryTest, TransitionRejectsRollbackRemovalMutationAndIdGaps) {
    const auto previous = createWorkerRegistry(3);
    const auto newer = reconcileWorkerRegistry(previous, 4);

    EXPECT_THROW(validateWorkerRegistryTransition(newer, previous), std::invalid_argument);

    auto removed = newer;
    removed.workers.pop_back();
    removed.nextWorkerId = 4;
    ++removed.generation;
    EXPECT_THROW(validateWorkerRegistryTransition(newer, removed), std::invalid_argument);

    auto reweighted = previous;
    reweighted.workers[0].weight = 2;
    ++reweighted.generation;
    EXPECT_THROW(validateWorkerRegistryTransition(previous, reweighted), std::invalid_argument);

    auto gap = previous;
    gap.workers.push_back(StorageWorker{StorageWorkerId(5), 1, StorageWorkerState::Active});
    gap.nextWorkerId = 6;
    ++gap.generation;
    EXPECT_THROW(validateWorkerRegistryTransition(previous, gap), std::invalid_argument);
}

TEST(WorkerRegistryTest, TransitionRejectsGenerationOnlySkipAndMixedLifecycleDirection) {
    const auto previous = reconcileWorkerRegistry(createWorkerRegistry(3), 2);

    auto generationOnly = previous;
    ++generationOnly.generation;
    EXPECT_THROW(validateWorkerRegistryTransition(previous, generationOnly), std::invalid_argument);

    auto skipped = previous;
    skipped.workers[2].state = StorageWorkerState::Active;
    skipped.generation += 2;
    EXPECT_THROW(validateWorkerRegistryTransition(previous, skipped), std::invalid_argument);

    auto mixed = createWorkerRegistry(3);
    mixed.workers[2].state = StorageWorkerState::Draining;
    auto mixedCandidate = mixed;
    mixedCandidate.workers[0].state = StorageWorkerState::Draining;
    mixedCandidate.workers[2].state = StorageWorkerState::Active;
    ++mixedCandidate.generation;
    EXPECT_THROW(validateWorkerRegistryTransition(mixed, mixedCandidate), std::invalid_argument);
}

TEST(WorkerRegistryTest, DecoderRejectsCorruptionUnknownKeysDuplicatesAndAlternateSpelling) {
    const auto canonical = encodeWorkerRegistryJson(createWorkerRegistry(2));

    auto checksumCorruption = canonical;
    const auto weight = checksumCorruption.find(R"("weight":1)");
    ASSERT_NE(weight, std::string::npos);
    checksumCorruption[weight + 9] = '2';
    EXPECT_THROW(
        {
            const auto ignored = decodeWorkerRegistryJson(checksumCorruption);
            (void)ignored;
        },
        std::invalid_argument);

    auto unknownKey = canonical;
    unknownKey.insert(1, R"("unknown":7,)");
    EXPECT_THROW(
        {
            const auto ignored = decodeWorkerRegistryJson(unknownKey);
            (void)ignored;
        },
        std::invalid_argument);

    auto duplicateKey = canonical;
    duplicateKey.insert(1, R"("formatVersion":1,)");
    EXPECT_THROW(
        {
            const auto ignored = decodeWorkerRegistryJson(duplicateKey);
            (void)ignored;
        },
        std::invalid_argument);

    const auto expectDecodeFailure = [](const std::string& invalid) {
        const auto ignored = decodeWorkerRegistryJson(invalid);
        (void)ignored;
    };
    EXPECT_THROW(expectDecodeFailure(" " + canonical), std::invalid_argument);
    EXPECT_THROW(expectDecodeFailure(canonical + "\n"), std::invalid_argument);
    EXPECT_THROW(expectDecodeFailure(canonical + " trailing"), std::invalid_argument);
}

TEST(WorkerRegistryTest, DecoderBoundsInputBeforeParsing) {
    EXPECT_THROW(
        {
            const auto ignored = decodeWorkerRegistryJson("");
            (void)ignored;
        },
        std::invalid_argument);
    const std::string oversized(timestar::cluster::MAX_WORKER_REGISTRY_JSON_BYTES + 1, 'x');
    EXPECT_THROW(
        {
            const auto ignored = decodeWorkerRegistryJson(oversized);
            (void)ignored;
        },
        std::invalid_argument);
}

TEST(WorkerRegistryTest, ValidChecksummedOlderStateStillRequiresTransitionFreshnessCheck) {
    const auto oldRegistry = createWorkerRegistry(2);
    const auto newRegistry = reconcileWorkerRegistry(oldRegistry, 3);
    const auto decodedOld = decodeWorkerRegistryJson(encodeWorkerRegistryJson(oldRegistry));

    EXPECT_EQ(decodedOld, oldRegistry);
    EXPECT_THROW(validateWorkerRegistryTransition(newRegistry, decodedOld), std::invalid_argument);
}
