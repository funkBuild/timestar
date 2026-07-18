#include "../../../lib/cluster/effective_vshard_layout.hpp"

#include "../../../lib/utils/crc32.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using timestar::VIRTUAL_SHARD_COUNT;
using timestar::cluster::createInitialEffectiveVShardLayout;
using timestar::cluster::createWorkerRegistry;
using timestar::cluster::decodeEffectiveVShardLayoutBinary;
using timestar::cluster::EffectiveVShardLayout;
using timestar::cluster::EffectiveVShardTarget;
using timestar::cluster::encodeEffectiveVShardLayoutBinary;
using timestar::cluster::fingerprintWorkerRegistry;
using timestar::cluster::isEffectiveVShardLayoutConverged;
using timestar::cluster::proposeEffectiveVShardOwnershipAdvance;
using timestar::cluster::reconcileWorkerRegistry;
using timestar::cluster::StorageWorkerId;
using timestar::cluster::synchronizeEffectiveVShardLayoutTarget;
using timestar::cluster::validateEffectiveVShardLayout;
using timestar::cluster::validateEffectiveVShardLayoutTransition;
using timestar::cluster::WorkerRegistry;

std::vector<uint16_t> mismatches(const EffectiveVShardLayout& layout, const EffectiveVShardTarget& target) {
    std::vector<uint16_t> result;
    for (uint32_t vshard = 0; vshard < VIRTUAL_SHARD_COUNT; ++vshard) {
        if (layout.ownership[vshard].owner != target.desiredOwnerFor(static_cast<uint16_t>(vshard)))
            result.push_back(static_cast<uint16_t>(vshard));
    }
    return result;
}

template <typename Integer>
Integer readLittleEndian(const std::string& bytes, size_t offset) {
    Integer value = 0;
    for (size_t byte = 0; byte < sizeof(Integer); ++byte)
        value |= static_cast<Integer>(static_cast<unsigned char>(bytes[offset + byte])) << (byte * 8);
    return value;
}

template <typename Integer>
void writeLittleEndian(std::string& bytes, size_t offset, Integer value) {
    for (size_t byte = 0; byte < sizeof(Integer); ++byte) {
        bytes[offset + byte] = static_cast<char>(value & 0xffU);
        value >>= 8;
    }
}

void repairChecksum(std::string& bytes) {
    const auto checksumOffset = bytes.size() - sizeof(uint32_t);
    writeLittleEndian(bytes, checksumOffset, CRC32::compute(bytes.data(), checksumOffset));
}

}  // namespace

TEST(EffectiveVShardLayoutTest, FreshLayoutExactlyMatchesItsGenerationOneRegistry) {
    const auto target = EffectiveVShardTarget::build(createWorkerRegistry(3));
    const auto layout = createInitialEffectiveVShardLayout(target);

    EXPECT_EQ(layout.revision, 1u);
    EXPECT_EQ(layout.targetWorkerRegistryGeneration, 1u);
    EXPECT_EQ(layout.targetWorkerRegistryFingerprint, target.registryFingerprint());
    for (uint32_t vshard = 0; vshard < VIRTUAL_SHARD_COUNT; ++vshard) {
        EXPECT_EQ(layout.ownership[vshard].owner, target.desiredOwnerFor(static_cast<uint16_t>(vshard)));
        EXPECT_EQ(layout.ownership[vshard].generation, 1u);
    }
    EXPECT_NO_THROW(validateEffectiveVShardLayout(layout, target));
    EXPECT_TRUE(isEffectiveVShardLayoutConverged(layout, target));
}

TEST(EffectiveVShardLayoutTest, FreshLayoutRejectsLaterOrNoncanonicalInitialRegistries) {
    const auto later = reconcileWorkerRegistry(createWorkerRegistry(2), 3);
    EXPECT_THROW(
        {
            const auto ignored = createInitialEffectiveVShardLayout(EffectiveVShardTarget::build(later));
            (void)ignored;
        },
        std::invalid_argument);

    auto noncanonicalInitial = createWorkerRegistry(2);
    noncanonicalInitial.workers[0].weight = 2;
    const auto noncanonicalTarget = EffectiveVShardTarget::build(noncanonicalInitial);
    EXPECT_THROW(
        {
            const auto ignored = createInitialEffectiveVShardLayout(noncanonicalTarget);
            (void)ignored;
        },
        std::invalid_argument);
}

TEST(EffectiveVShardLayoutTest, RegistryFingerprintBindsExactCanonicalState) {
    const auto two = createWorkerRegistry(2);
    const auto three = createWorkerRegistry(3);
    ASSERT_EQ(two.generation, three.generation);

    const auto first = fingerprintWorkerRegistry(two);
    EXPECT_EQ(first, fingerprintWorkerRegistry(two));
    EXPECT_NE(first, fingerprintWorkerRegistry(three));
    EXPECT_EQ(first.low64, 13496512292965800027ULL);
    EXPECT_EQ(first.high64, 9737867224379180105ULL);
}

TEST(EffectiveVShardLayoutTest, WitnessAdvanceCanJumpGenerationsWithoutChangingOwnership) {
    const auto initialTarget = EffectiveVShardTarget::build(createWorkerRegistry(2));
    const auto initial = createInitialEffectiveVShardLayout(initialTarget);
    const auto generationTwo = reconcileWorkerRegistry(initialTarget.acceptedRegistry(), 3);
    const auto generationThree = reconcileWorkerRegistry(generationTwo, 4);
    const auto currentTarget = EffectiveVShardTarget::build(generationThree);

    EXPECT_NO_THROW(validateEffectiveVShardLayout(initial, currentTarget));
    EXPECT_FALSE(isEffectiveVShardLayoutConverged(initial, currentTarget));
    const auto advanced = synchronizeEffectiveVShardLayoutTarget(initial, currentTarget);

    EXPECT_EQ(advanced.revision, 2u);
    EXPECT_EQ(advanced.targetWorkerRegistryGeneration, 3u);
    EXPECT_EQ(advanced.targetWorkerRegistryFingerprint, currentTarget.registryFingerprint());
    EXPECT_EQ(advanced.ownership, initial.ownership);
    EXPECT_NO_THROW(validateEffectiveVShardLayoutTransition(initial, advanced, currentTarget));
    EXPECT_EQ(synchronizeEffectiveVShardLayoutTarget(advanced, currentTarget), advanced);
}

TEST(EffectiveVShardLayoutTest, SkippedDesiredABAIntentDoesNotBumpOwnershipGenerations) {
    const auto initialRegistry = createWorkerRegistry(2);
    const auto initialTarget = EffectiveVShardTarget::build(initialRegistry);
    const auto initial = createInitialEffectiveVShardLayout(initialTarget);
    const auto added = reconcileWorkerRegistry(initialRegistry, 3);
    ASSERT_FALSE(mismatches(initial, EffectiveVShardTarget::build(added)).empty());
    const auto drainedAgain = reconcileWorkerRegistry(added, 2);
    const auto finalTarget = EffectiveVShardTarget::build(drainedAgain);

    const auto retargeted = synchronizeEffectiveVShardLayoutTarget(initial, finalTarget);
    EXPECT_EQ(retargeted.ownership, initial.ownership);
    EXPECT_TRUE(isEffectiveVShardLayoutConverged(retargeted, finalTarget));
    for (const auto& entry : retargeted.ownership)
        EXPECT_EQ(entry.generation, 1u);
}

TEST(EffectiveVShardLayoutTest, ProposalChangesExactlyOneOwnerAndFenceGeneration) {
    const auto initialTarget = EffectiveVShardTarget::build(createWorkerRegistry(2));
    const auto initial = createInitialEffectiveVShardLayout(initialTarget);
    const auto currentTarget =
        EffectiveVShardTarget::build(reconcileWorkerRegistry(initialTarget.acceptedRegistry(), 3));
    const auto pending = mismatches(initial, currentTarget);
    ASSERT_GE(pending.size(), 2u);

    const auto first = proposeEffectiveVShardOwnershipAdvance(initial, currentTarget, pending[0]);
    EXPECT_EQ(first.revision, 2u);
    EXPECT_EQ(first.targetWorkerRegistryGeneration, currentTarget.registryGeneration());
    EXPECT_EQ(first.ownership[pending[0]].owner, currentTarget.desiredOwnerFor(pending[0]));
    EXPECT_EQ(first.ownership[pending[0]].generation, 2u);
    for (uint32_t vshard = 0; vshard < VIRTUAL_SHARD_COUNT; ++vshard) {
        if (vshard != pending[0])
            EXPECT_EQ(first.ownership[vshard], initial.ownership[vshard]);
    }

    const auto second = proposeEffectiveVShardOwnershipAdvance(first, currentTarget, pending[1]);
    EXPECT_EQ(second.revision, 3u);
    EXPECT_EQ(second.targetWorkerRegistryGeneration, first.targetWorkerRegistryGeneration);
    EXPECT_EQ(second.ownership[pending[0]], first.ownership[pending[0]]);
    EXPECT_EQ(second.ownership[pending[1]].generation, 2u);
    EXPECT_NO_THROW(validateEffectiveVShardLayoutTransition(first, second, currentTarget));
}

TEST(EffectiveVShardLayoutTest, DrainingWorkerRemainsAValidEffectiveOwnerUntilItsHandoffsComplete) {
    const auto initialTarget = EffectiveVShardTarget::build(createWorkerRegistry(3));
    const auto initial = createInitialEffectiveVShardLayout(initialTarget);
    const auto drainingTarget =
        EffectiveVShardTarget::build(reconcileWorkerRegistry(initialTarget.acceptedRegistry(), 2));
    const auto pending = mismatches(initial, drainingTarget);
    ASSERT_FALSE(pending.empty());
    ASSERT_EQ(initial.ownership[pending.front()].owner, StorageWorkerId(3));
    ASSERT_TRUE(drainingTarget.records(StorageWorkerId(3)));

    EXPECT_NO_THROW(validateEffectiveVShardLayout(initial, drainingTarget));
    const auto proposed = proposeEffectiveVShardOwnershipAdvance(initial, drainingTarget, pending.front());
    EXPECT_NE(proposed.ownership[pending.front()].owner, StorageWorkerId(3));
    EXPECT_EQ(proposed.ownership[pending.front()].generation, 2u);
}

TEST(EffectiveVShardLayoutTest, ConvergenceRequiresExactTargetAndEveryDesiredOwner) {
    const auto initialTarget = EffectiveVShardTarget::build(createWorkerRegistry(2));
    auto layout = createInitialEffectiveVShardLayout(initialTarget);
    const auto currentTarget =
        EffectiveVShardTarget::build(reconcileWorkerRegistry(initialTarget.acceptedRegistry(), 3));

    layout = synchronizeEffectiveVShardLayoutTarget(layout, currentTarget);
    EXPECT_FALSE(isEffectiveVShardLayoutConverged(layout, currentTarget));
    const auto pending = mismatches(layout, currentTarget);
    for (const auto vshard : pending) {
        layout.ownership[vshard].owner = currentTarget.desiredOwnerFor(vshard);
        ++layout.ownership[vshard].generation;
    }
    layout.revision += pending.size();

    EXPECT_NO_THROW(validateEffectiveVShardLayout(layout, currentTarget));
    EXPECT_TRUE(isEffectiveVShardLayoutConverged(layout, currentTarget));
    EXPECT_EQ(layout.targetWorkerRegistryGeneration, currentTarget.registryGeneration());
}

TEST(EffectiveVShardLayoutTest, TransitionRejectsGenerationOwnerRevisionAndTargetViolations) {
    const auto initialTarget = EffectiveVShardTarget::build(createWorkerRegistry(2));
    const auto initial = createInitialEffectiveVShardLayout(initialTarget);
    const auto currentTarget =
        EffectiveVShardTarget::build(reconcileWorkerRegistry(initialTarget.acceptedRegistry(), 3));
    const auto pending = mismatches(initial, currentTarget);
    ASSERT_GE(pending.size(), 2u);
    const auto valid = proposeEffectiveVShardOwnershipAdvance(initial, currentTarget, pending[0]);

    auto invalid = valid;
    ++invalid.revision;
    EXPECT_THROW(validateEffectiveVShardLayoutTransition(initial, invalid, currentTarget), std::invalid_argument);

    invalid = valid;
    invalid.ownership[pending[0]].generation = initial.ownership[pending[0]].generation;
    EXPECT_THROW(validateEffectiveVShardLayoutTransition(initial, invalid, currentTarget), std::invalid_argument);

    invalid = valid;
    invalid.ownership[pending[0]].owner = initial.ownership[pending[0]].owner;
    EXPECT_THROW(validateEffectiveVShardLayoutTransition(initial, invalid, currentTarget), std::invalid_argument);

    invalid = valid;
    const auto oldOwner = initial.ownership[pending[0]].owner;
    invalid.ownership[pending[0]].owner = oldOwner == StorageWorkerId(1) ? StorageWorkerId(2) : StorageWorkerId(1);
    ASSERT_NE(invalid.ownership[pending[0]].owner, oldOwner);
    ASSERT_NE(invalid.ownership[pending[0]].owner, currentTarget.desiredOwnerFor(pending[0]));
    EXPECT_THROW(validateEffectiveVShardLayoutTransition(initial, invalid, currentTarget), std::invalid_argument);

    const auto witnessOnly = synchronizeEffectiveVShardLayoutTarget(initial, currentTarget);
    invalid = witnessOnly;
    ++invalid.revision;
    invalid.ownership[pending[0]].owner = currentTarget.desiredOwnerFor(pending[0]);
    invalid.ownership[pending[0]].generation = 3;
    EXPECT_THROW(validateEffectiveVShardLayoutTransition(witnessOnly, invalid, currentTarget), std::invalid_argument);

    invalid = valid;
    invalid.ownership[pending[1]].owner = currentTarget.desiredOwnerFor(pending[1]);
    invalid.ownership[pending[1]].generation = 2;
    EXPECT_THROW(validateEffectiveVShardLayoutTransition(initial, invalid, currentTarget), std::invalid_argument);

    invalid = witnessOnly;
    ++invalid.revision;
    EXPECT_THROW(validateEffectiveVShardLayoutTransition(witnessOnly, invalid, currentTarget), std::invalid_argument);

    invalid = valid;
    invalid.targetWorkerRegistryFingerprint = initial.targetWorkerRegistryFingerprint;
    EXPECT_THROW(validateEffectiveVShardLayoutTransition(initial, invalid, currentTarget), std::invalid_argument);

    auto retargeted = witnessOnly;
    invalid = retargeted;
    ++invalid.revision;
    invalid.targetWorkerRegistryGeneration = initial.targetWorkerRegistryGeneration;
    invalid.targetWorkerRegistryFingerprint = initial.targetWorkerRegistryFingerprint;
    EXPECT_THROW(validateEffectiveVShardLayoutTransition(retargeted, invalid, currentTarget), std::invalid_argument);

    EXPECT_NO_THROW(validateEffectiveVShardLayoutTransition(valid, valid, currentTarget));
}

TEST(EffectiveVShardLayoutTest, ContextRejectsSameGenerationForkFutureTargetAndHardRemovedOwner) {
    const auto threeTarget = EffectiveVShardTarget::build(createWorkerRegistry(3));
    const auto threeLayout = createInitialEffectiveVShardLayout(threeTarget);
    const auto sameGenerationFork = EffectiveVShardTarget::build(createWorkerRegistry(2));
    EXPECT_THROW(validateEffectiveVShardLayout(threeLayout, sameGenerationFork), std::invalid_argument);

    auto future = threeLayout;
    ++future.targetWorkerRegistryGeneration;
    EXPECT_THROW(validateEffectiveVShardLayout(future, threeTarget), std::invalid_argument);

    auto unrelatedLaterRegistry = createWorkerRegistry(2);
    unrelatedLaterRegistry.generation = 2;
    const auto unrelatedLaterTarget = EffectiveVShardTarget::build(unrelatedLaterRegistry);
    EXPECT_THROW(validateEffectiveVShardLayout(threeLayout, unrelatedLaterTarget), std::invalid_argument);
}

TEST(EffectiveVShardLayoutTest, ImpossibleOwnershipHistoriesAndOverflowFailClosed) {
    const auto initialTarget = EffectiveVShardTarget::build(createWorkerRegistry(2));
    const auto initial = createInitialEffectiveVShardLayout(initialTarget);

    auto impossible = initial;
    impossible.ownership[0].generation = 2;
    EXPECT_THROW(validateEffectiveVShardLayout(impossible, initialTarget), std::invalid_argument);
    EXPECT_THROW(
        {
            const auto ignored = encodeEffectiveVShardLayoutBinary(impossible);
            (void)ignored;
        },
        std::invalid_argument);

    impossible = initial;
    impossible.revision = 2;
    impossible.ownership[0].generation = 2;
    impossible.ownership[1].generation = 2;
    EXPECT_THROW(validateEffectiveVShardLayout(impossible, initialTarget), std::invalid_argument);

    impossible = initial;
    impossible.ownership[0].owner = StorageWorkerId();
    EXPECT_THROW(validateEffectiveVShardLayout(impossible, initialTarget), std::invalid_argument);

    impossible = initial;
    impossible.ownership.pop_back();
    EXPECT_THROW(validateEffectiveVShardLayout(impossible, initialTarget), std::invalid_argument);

    const auto currentTarget =
        EffectiveVShardTarget::build(reconcileWorkerRegistry(initialTarget.acceptedRegistry(), 3));
    auto exhausted = initial;
    exhausted.revision = std::numeric_limits<uint64_t>::max();
    EXPECT_THROW(
        {
            const auto ignored = synchronizeEffectiveVShardLayoutTarget(exhausted, currentTarget);
            (void)ignored;
        },
        std::overflow_error);
}

TEST(EffectiveVShardLayoutTest, CanonicalBinaryFormatIsFixedAndRoundTrips) {
    const auto target = EffectiveVShardTarget::build(createWorkerRegistry(2));
    const auto layout = createInitialEffectiveVShardLayout(target);
    const auto encoded = encodeEffectiveVShardLayoutBinary(layout);

    ASSERT_EQ(encoded.size(), timestar::cluster::EFFECTIVE_VSHARD_LAYOUT_BINARY_BYTES);
    EXPECT_EQ(encoded.size(), 49212u);
    EXPECT_EQ(std::string_view(encoded.data(), 8), std::string_view("TSVOWN1\0", 8));
    EXPECT_EQ(readLittleEndian<uint32_t>(encoded, 8), 1u);
    EXPECT_EQ(readLittleEndian<uint32_t>(encoded, 12), 1u);
    EXPECT_EQ(readLittleEndian<uint64_t>(encoded, 16), 1u);
    EXPECT_EQ(readLittleEndian<uint64_t>(encoded, 24), 1u);
    EXPECT_EQ(readLittleEndian<uint64_t>(encoded, 32), target.registryFingerprint().low64);
    EXPECT_EQ(readLittleEndian<uint64_t>(encoded, 40), target.registryFingerprint().high64);
    EXPECT_EQ(readLittleEndian<uint32_t>(encoded, 48), 4096u);
    EXPECT_EQ(readLittleEndian<uint32_t>(encoded, 52), 12u);
    const auto checksum = readLittleEndian<uint32_t>(encoded, encoded.size() - 4);
    EXPECT_EQ(checksum, CRC32::compute(encoded.data(), encoded.size() - 4));
    EXPECT_EQ(checksum, 3104278377u);
    EXPECT_EQ(decodeEffectiveVShardLayoutBinary(encoded), layout);
}

TEST(EffectiveVShardLayoutTest, DecoderRejectsCorruptionDimensionsAndNoncanonicalLength) {
    const auto target = EffectiveVShardTarget::build(createWorkerRegistry(2));
    const auto layout = createInitialEffectiveVShardLayout(target);
    const auto canonical = encodeEffectiveVShardLayoutBinary(layout);

    auto corrupt = canonical;
    corrupt[56] ^= 1;
    const auto expectDecodeFailure = [](std::string_view bytes) {
        EXPECT_THROW(
            {
                const auto ignored = decodeEffectiveVShardLayoutBinary(bytes);
                (void)ignored;
            },
            std::invalid_argument);
    };
    expectDecodeFailure(corrupt);
    expectDecodeFailure(std::string_view(canonical).substr(0, canonical.size() - 1));
    expectDecodeFailure(canonical + "x");

    auto unsupported = canonical;
    writeLittleEndian<uint32_t>(unsupported, 8, 2);
    repairChecksum(unsupported);
    expectDecodeFailure(unsupported);

    auto wrongCount = canonical;
    writeLittleEndian<uint32_t>(wrongCount, 48, 4095);
    repairChecksum(wrongCount);
    expectDecodeFailure(wrongCount);

    auto zeroOwner = canonical;
    writeLittleEndian<uint32_t>(zeroOwner, 56, 0);
    repairChecksum(zeroOwner);
    expectDecodeFailure(zeroOwner);
}
