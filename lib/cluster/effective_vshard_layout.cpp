#include "effective_vshard_layout.hpp"

#include "../utils/crc32.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace timestar::cluster {
namespace {

constexpr std::array<char, 8> layoutMagic{'T', 'S', 'V', 'O', 'W', 'N', '1', '\0'};
constexpr size_t checksumBytes = sizeof(uint32_t);

template <typename Integer>
void appendLittleEndian(std::string& output, Integer value) {
    static_assert(std::is_unsigned_v<Integer>);
    for (size_t byte = 0; byte < sizeof(Integer); ++byte) {
        output.push_back(static_cast<char>(value & 0xffU));
        value >>= 8;
    }
}

template <typename Integer>
Integer readLittleEndian(std::string_view input, size_t& offset) {
    static_assert(std::is_unsigned_v<Integer>);
    if (offset > input.size() || input.size() - offset < sizeof(Integer))
        throw std::invalid_argument("effective VShard layout binary is truncated");

    Integer value = 0;
    for (size_t byte = 0; byte < sizeof(Integer); ++byte) {
        value |= static_cast<Integer>(static_cast<unsigned char>(input[offset + byte])) << (byte * 8);
    }
    offset += sizeof(Integer);
    return value;
}

void validateStructure(const EffectiveVShardLayout& layout) {
    if (layout.formatVersion != EFFECTIVE_VSHARD_LAYOUT_FORMAT_VERSION)
        throw std::invalid_argument("unsupported effective VShard layout format version");
    if (layout.localPlacementAlgorithmVersion != LOCAL_PLACEMENT_ALGORITHM_VERSION)
        throw std::invalid_argument("unsupported effective VShard placement algorithm version");
    if (layout.revision == 0)
        throw std::invalid_argument("effective VShard layout revision zero is reserved");
    if (layout.targetWorkerRegistryGeneration == 0)
        throw std::invalid_argument("effective VShard target registry generation zero is reserved");
    if (layout.ownership.size() != VIRTUAL_SHARD_COUNT)
        throw std::invalid_argument("effective VShard layout must contain exactly 4096 canonical entries");

    const uint64_t maximumOwnershipChanges = layout.revision - 1;
    uint64_t observedOwnershipChanges = 0;
    for (const auto& entry : layout.ownership) {
        if (!entry.owner.valid())
            throw std::invalid_argument("effective VShard layout contains invalid worker ID zero");
        if (entry.generation == 0)
            throw std::invalid_argument("effective VShard ownership generation zero is reserved");
        if (entry.generation > layout.revision)
            throw std::invalid_argument("effective VShard ownership generation exceeds layout revision");

        const uint64_t entryChanges = entry.generation - 1;
        if (entryChanges > maximumOwnershipChanges - observedOwnershipChanges) {
            throw std::invalid_argument("effective VShard ownership history exceeds the layout revision budget");
        }
        observedOwnershipChanges += entryChanges;
    }
}

[[noreturn]] void invalidTransition(const char* reason) {
    throw std::invalid_argument(std::string("invalid effective VShard layout transition: ") + reason);
}

}  // namespace

EffectiveVShardTarget::EffectiveVShardTarget(WorkerRegistry registry, WorkerRegistryFingerprint fingerprint,
                                             DesiredLocalVShardPlacement desired)
    : registry_(std::move(registry)), fingerprint_(fingerprint), desired_(std::move(desired)) {}

EffectiveVShardTarget EffectiveVShardTarget::build(WorkerRegistry acceptedRegistry) {
    auto validated = validateWorkerRegistryForPlacement(acceptedRegistry);
    return EffectiveVShardTarget(std::move(acceptedRegistry), validated.fingerprint,
                                 std::move(validated.desiredPlacement));
}

StorageWorkerId EffectiveVShardTarget::desiredOwnerFor(uint16_t vshard) const {
    return desired_.workerFor(vshard);
}

bool EffectiveVShardTarget::records(StorageWorkerId worker) const noexcept {
    const auto found =
        std::lower_bound(registry_.workers.begin(), registry_.workers.end(), worker,
                         [](const StorageWorker& candidate, StorageWorkerId sought) { return candidate.id < sought; });
    return found != registry_.workers.end() && found->id == worker;
}

EffectiveVShardLayout createInitialEffectiveVShardLayout(const EffectiveVShardTarget& target) {
    const auto& registry = target.acceptedRegistry();
    bool exactInitial = registry.generation == 1 && registry.nextWorkerId == registry.workers.size() + 1;
    for (size_t index = 0; exactInitial && index < registry.workers.size(); ++index) {
        const auto& worker = registry.workers[index];
        exactInitial =
            worker.id.value() == index + 1 && worker.weight == 1 && worker.state == StorageWorkerState::Active;
    }
    if (!exactInitial) {
        throw std::invalid_argument("initial effective VShard layout requires an exact generation-1 registry");
    }

    EffectiveVShardLayout layout;
    layout.revision = 1;
    layout.targetWorkerRegistryGeneration = target.registryGeneration();
    layout.targetWorkerRegistryFingerprint = target.registryFingerprint();
    layout.ownership.resize(VIRTUAL_SHARD_COUNT);
    for (uint32_t vshard = 0; vshard < VIRTUAL_SHARD_COUNT; ++vshard) {
        layout.ownership[vshard] = EffectiveVShardOwnership{target.desiredOwnerFor(static_cast<uint16_t>(vshard)), 1};
    }
    validateEffectiveVShardLayout(layout, target);
    return layout;
}

void validateEffectiveVShardLayout(const EffectiveVShardLayout& layout, const EffectiveVShardTarget& target) {
    validateStructure(layout);
    if (layout.targetWorkerRegistryGeneration > target.registryGeneration())
        throw std::invalid_argument("effective VShard layout targets a future worker registry generation");
    if (layout.targetWorkerRegistryGeneration == target.registryGeneration() &&
        layout.targetWorkerRegistryFingerprint != target.registryFingerprint()) {
        throw std::invalid_argument("effective VShard layout targets a divergent worker registry fingerprint");
    }
    for (const auto& entry : layout.ownership) {
        if (!target.records(entry.owner)) {
            throw std::invalid_argument("effective VShard owner was removed from the worker registry before drain");
        }
    }
}

void validateEffectiveVShardLayoutTransition(const EffectiveVShardLayout& previous,
                                             const EffectiveVShardLayout& candidate,
                                             const EffectiveVShardTarget& target) {
    validateEffectiveVShardLayout(previous, target);
    validateEffectiveVShardLayout(candidate, target);
    if (candidate == previous)
        return;

    if (previous.revision == std::numeric_limits<uint64_t>::max() || candidate.revision != previous.revision + 1)
        invalidTransition("changed state must advance layout revision exactly once");
    if (candidate.targetWorkerRegistryGeneration < previous.targetWorkerRegistryGeneration)
        invalidTransition("target worker registry generation cannot roll back");
    if (candidate.targetWorkerRegistryGeneration != target.registryGeneration() ||
        candidate.targetWorkerRegistryFingerprint != target.registryFingerprint()) {
        invalidTransition("changed state must target the exact supplied worker registry snapshot");
    }

    size_t changedOwners = 0;
    for (uint32_t vshard = 0; vshard < VIRTUAL_SHARD_COUNT; ++vshard) {
        const auto& before = previous.ownership[vshard];
        const auto& after = candidate.ownership[vshard];
        if (after.owner == before.owner) {
            if (after.generation != before.generation)
                invalidTransition("an unchanged owner cannot change its ownership generation");
            continue;
        }

        ++changedOwners;
        if (changedOwners > 1)
            invalidTransition("one layout revision cannot change more than one VShard owner");
        if (before.generation == std::numeric_limits<uint64_t>::max() || after.generation != before.generation + 1) {
            invalidTransition("a changed owner must advance its VShard generation exactly once");
        }
        if (after.owner != target.desiredOwnerFor(static_cast<uint16_t>(vshard)))
            invalidTransition("a changed owner must be the current desired active worker");
    }

    if (changedOwners == 0 && candidate.targetWorkerRegistryGeneration <= previous.targetWorkerRegistryGeneration) {
        invalidTransition("a witness-only revision must strictly advance the target registry generation");
    }
}

bool isEffectiveVShardLayoutConverged(const EffectiveVShardLayout& layout, const EffectiveVShardTarget& target) {
    validateEffectiveVShardLayout(layout, target);
    if (layout.targetWorkerRegistryGeneration != target.registryGeneration() ||
        layout.targetWorkerRegistryFingerprint != target.registryFingerprint()) {
        return false;
    }
    for (uint32_t vshard = 0; vshard < VIRTUAL_SHARD_COUNT; ++vshard) {
        if (layout.ownership[vshard].owner != target.desiredOwnerFor(static_cast<uint16_t>(vshard)))
            return false;
    }
    return true;
}

EffectiveVShardLayout synchronizeEffectiveVShardLayoutTarget(const EffectiveVShardLayout& previous,
                                                             const EffectiveVShardTarget& target) {
    validateEffectiveVShardLayout(previous, target);
    if (previous.targetWorkerRegistryGeneration == target.registryGeneration() &&
        previous.targetWorkerRegistryFingerprint == target.registryFingerprint()) {
        return previous;
    }
    if (previous.revision == std::numeric_limits<uint64_t>::max())
        throw std::overflow_error("effective VShard layout revision exhausted");

    auto result = previous;
    ++result.revision;
    result.targetWorkerRegistryGeneration = target.registryGeneration();
    result.targetWorkerRegistryFingerprint = target.registryFingerprint();
    validateEffectiveVShardLayoutTransition(previous, result, target);
    return result;
}

EffectiveVShardLayout proposeEffectiveVShardOwnershipAdvance(const EffectiveVShardLayout& previous,
                                                             const EffectiveVShardTarget& target, uint16_t vshard) {
    validateEffectiveVShardLayout(previous, target);
    if (vshard >= VIRTUAL_SHARD_COUNT)
        throw std::out_of_range("VShard ID is outside the fixed ownership range");

    const auto desired = target.desiredOwnerFor(vshard);
    const auto& before = previous.ownership[vshard];
    if (before.owner == desired)
        throw std::invalid_argument("effective VShard already has its current desired owner");
    if (previous.revision == std::numeric_limits<uint64_t>::max())
        throw std::overflow_error("effective VShard layout revision exhausted");
    if (before.generation == std::numeric_limits<uint64_t>::max())
        throw std::overflow_error("effective VShard ownership generation exhausted");

    auto result = previous;
    ++result.revision;
    result.targetWorkerRegistryGeneration = target.registryGeneration();
    result.targetWorkerRegistryFingerprint = target.registryFingerprint();
    result.ownership[vshard] = EffectiveVShardOwnership{desired, before.generation + 1};
    validateEffectiveVShardLayoutTransition(previous, result, target);
    return result;
}

std::string encodeEffectiveVShardLayoutBinary(const EffectiveVShardLayout& layout) {
    validateStructure(layout);

    std::string bytes;
    bytes.reserve(EFFECTIVE_VSHARD_LAYOUT_BINARY_BYTES);
    bytes.append(layoutMagic.data(), layoutMagic.size());
    appendLittleEndian(bytes, layout.formatVersion);
    appendLittleEndian(bytes, layout.localPlacementAlgorithmVersion);
    appendLittleEndian(bytes, layout.revision);
    appendLittleEndian(bytes, layout.targetWorkerRegistryGeneration);
    appendLittleEndian(bytes, layout.targetWorkerRegistryFingerprint.low64);
    appendLittleEndian(bytes, layout.targetWorkerRegistryFingerprint.high64);
    appendLittleEndian(bytes, static_cast<uint32_t>(VIRTUAL_SHARD_COUNT));
    appendLittleEndian(bytes, EFFECTIVE_VSHARD_LAYOUT_ENTRY_BYTES);
    for (const auto& entry : layout.ownership) {
        appendLittleEndian(bytes, entry.owner.value());
        appendLittleEndian(bytes, entry.generation);
    }
    appendLittleEndian(bytes, CRC32::compute(bytes.data(), bytes.size()));
    if (bytes.size() != EFFECTIVE_VSHARD_LAYOUT_BINARY_BYTES)
        throw std::logic_error("effective VShard layout encoder produced an unexpected size");
    return bytes;
}

EffectiveVShardLayout decodeEffectiveVShardLayoutBinary(std::string_view bytes) {
    if (bytes.size() != EFFECTIVE_VSHARD_LAYOUT_BINARY_BYTES)
        throw std::invalid_argument("effective VShard layout binary has a noncanonical size");

    size_t checksumOffset = bytes.size() - checksumBytes;
    const auto storedChecksum = readLittleEndian<uint32_t>(bytes, checksumOffset);
    if (storedChecksum != CRC32::compute(bytes.data(), bytes.size() - checksumBytes))
        throw std::invalid_argument("effective VShard layout checksum mismatch");
    if (!std::equal(layoutMagic.begin(), layoutMagic.end(), bytes.begin()))
        throw std::invalid_argument("effective VShard layout magic is invalid");

    size_t offset = layoutMagic.size();
    EffectiveVShardLayout layout;
    layout.formatVersion = readLittleEndian<uint32_t>(bytes, offset);
    layout.localPlacementAlgorithmVersion = readLittleEndian<uint32_t>(bytes, offset);
    layout.revision = readLittleEndian<uint64_t>(bytes, offset);
    layout.targetWorkerRegistryGeneration = readLittleEndian<uint64_t>(bytes, offset);
    layout.targetWorkerRegistryFingerprint.low64 = readLittleEndian<uint64_t>(bytes, offset);
    layout.targetWorkerRegistryFingerprint.high64 = readLittleEndian<uint64_t>(bytes, offset);
    const auto vshardCount = readLittleEndian<uint32_t>(bytes, offset);
    const auto entryBytes = readLittleEndian<uint32_t>(bytes, offset);
    if (vshardCount != VIRTUAL_SHARD_COUNT || entryBytes != EFFECTIVE_VSHARD_LAYOUT_ENTRY_BYTES)
        throw std::invalid_argument("effective VShard layout dimensions are unsupported");

    layout.ownership.resize(VIRTUAL_SHARD_COUNT);
    for (auto& entry : layout.ownership) {
        entry.owner = StorageWorkerId(readLittleEndian<uint32_t>(bytes, offset));
        entry.generation = readLittleEndian<uint64_t>(bytes, offset);
    }
    if (offset != bytes.size() - checksumBytes)
        throw std::invalid_argument("effective VShard layout binary has trailing fields");
    validateStructure(layout);
    if (encodeEffectiveVShardLayoutBinary(layout) != bytes)
        throw std::invalid_argument("effective VShard layout binary is not canonical");
    return layout;
}

}  // namespace timestar::cluster
