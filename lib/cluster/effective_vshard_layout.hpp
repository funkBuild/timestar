#pragma once

#include "worker_registry.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::cluster {

inline constexpr uint32_t EFFECTIVE_VSHARD_LAYOUT_FORMAT_VERSION = 1;
inline constexpr uint32_t EFFECTIVE_VSHARD_LAYOUT_ENTRY_BYTES = 12;
inline constexpr size_t EFFECTIVE_VSHARD_LAYOUT_BINARY_BYTES =
    56 + static_cast<size_t>(VIRTUAL_SHARD_COUNT) * EFFECTIVE_VSHARD_LAYOUT_ENTRY_BYTES + 4;

static_assert(VIRTUAL_SHARD_COUNT == 4096, "effective VShard layout format v1 fixes the VShard count at 4096");

struct EffectiveVShardOwnership {
    StorageWorkerId owner;
    uint64_t generation = 0;

    bool operator==(const EffectiveVShardOwnership&) const = default;
};

// Durable node-local serving authority. The target registry fields name the
// desired placement currently being reconciled; they do not claim that all
// VShards have converged to that placement.
struct EffectiveVShardLayout {
    uint32_t formatVersion = EFFECTIVE_VSHARD_LAYOUT_FORMAT_VERSION;
    uint32_t localPlacementAlgorithmVersion = LOCAL_PLACEMENT_ALGORITHM_VERSION;
    uint64_t revision = 0;
    uint64_t targetWorkerRegistryGeneration = 0;
    WorkerRegistryFingerprint targetWorkerRegistryFingerprint;
    std::vector<EffectiveVShardOwnership> ownership;

    bool operator==(const EffectiveVShardLayout&) const = default;
};

struct VShardOwnershipFence {
    uint16_t vshard = 0;
    uint64_t generation = 0;
    StorageWorkerId owner;

    bool operator==(const VShardOwnershipFence&) const = default;
};

// Immutable, cached desired-placement context bound to one exact accepted
// worker registry snapshot. Building this is intentionally the only operation
// that performs the full rendezvous calculation.
class EffectiveVShardTarget {
public:
    [[nodiscard]] static EffectiveVShardTarget build(WorkerRegistry acceptedRegistry);

    [[nodiscard]] uint64_t registryGeneration() const noexcept { return registry_.generation; }
    [[nodiscard]] WorkerRegistryFingerprint registryFingerprint() const noexcept { return fingerprint_; }
    [[nodiscard]] StorageWorkerId desiredOwnerFor(uint16_t vshard) const;
    [[nodiscard]] bool records(StorageWorkerId worker) const noexcept;
    [[nodiscard]] const WorkerRegistry& acceptedRegistry() const noexcept { return registry_; }

private:
    EffectiveVShardTarget(WorkerRegistry registry, WorkerRegistryFingerprint fingerprint,
                          DesiredLocalVShardPlacement desired);

    const WorkerRegistry registry_;
    const WorkerRegistryFingerprint fingerprint_;
    const DesiredLocalVShardPlacement desired_;
};

// Creates the first effective map only for an exact generation-1 registry.
// Every VShard begins at generation one and at its desired worker. The later
// store may call this only for a provably fresh root or explicit migration.
[[nodiscard]] EffectiveVShardLayout createInitialEffectiveVShardLayout(const EffectiveVShardTarget& target);

// Structural checks plus contextual proof that the layout is compatible with
// the exact accepted registry target. Effective owners may be Draining and may
// differ from current desired placement while a handoff is pending.
void validateEffectiveVShardLayout(const EffectiveVShardLayout& layout, const EffectiveVShardTarget& target);

// Validates an identical no-op, a target-registry witness advance, or exactly
// one ownership change to the current desired worker. Changed durable state
// advances revision exactly once; an owner change advances only that VShard's
// generation exactly once.
void validateEffectiveVShardLayoutTransition(const EffectiveVShardLayout& previous,
                                             const EffectiveVShardLayout& candidate,
                                             const EffectiveVShardTarget& target);

[[nodiscard]] bool isEffectiveVShardLayoutConverged(const EffectiveVShardLayout& layout,
                                                    const EffectiveVShardTarget& target);

// Advances only the target-registry witness. Returns the original layout when
// it already names the exact current target.
[[nodiscard]] EffectiveVShardLayout synchronizeEffectiveVShardLayoutTarget(const EffectiveVShardLayout& previous,
                                                                           const EffectiveVShardTarget& target);

// Proposes the next durable state for one handoff. It does not prove quiescence
// or execute a cutover. Task 5 must quiesce and drain every foreground and
// background operation at the prior fence before persisting/publishing this
// proposal, then open the destination.
[[nodiscard]] EffectiveVShardLayout proposeEffectiveVShardOwnershipAdvance(const EffectiveVShardLayout& previous,
                                                                           const EffectiveVShardTarget& target,
                                                                           uint16_t vshard);

// Fence tokens are a frozen data shape only in Task 3a. This pure model does
// not mint tokens or authorize runtime work. Task 3b/5 must introduce one
// root-bound accepted-state authority shared by all handles; construction must
// require opaque durable-commit and quiesced-prior-fence capabilities so a raw
// decoded/cached/speculative layout can never become an authorizer.

// Format v1 is a fixed 49,212-byte canonical binary:
//   magic[8], format:u32, placement_algorithm:u32, revision:u64,
//   target_registry_generation:u64, target_fingerprint_low:u64,
//   target_fingerprint_high:u64, vshard_count:u32, entry_bytes:u32,
//   4096 * {owner:u32, generation:u64}, crc32:u32.
// Integers and the trailing CRC are little-endian. CRC-32/ISO-HDLC covers every
// byte except the CRC itself and provides integrity, not freshness/authenticity.
[[nodiscard]] std::string encodeEffectiveVShardLayoutBinary(const EffectiveVShardLayout& layout);
[[nodiscard]] EffectiveVShardLayout decodeEffectiveVShardLayoutBinary(std::string_view bytes);

}  // namespace timestar::cluster
