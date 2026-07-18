#pragma once

#include "local_storage_placement.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::cluster {

inline constexpr uint32_t WORKER_REGISTRY_FORMAT_VERSION = 1;
inline constexpr size_t MAX_WORKER_REGISTRY_JSON_BYTES = 64 * 1024;

// A 128-bit XXH3 fingerprint of the exact canonical registry encoding. This
// binds cross-file state much more strongly than the registry's corruption CRC;
// it is an identity/fork witness, not an authenticity primitive.
struct WorkerRegistryFingerprint {
    uint64_t low64 = 0;
    uint64_t high64 = 0;

    bool operator==(const WorkerRegistryFingerprint&) const = default;
};

// Durable desired state for node-local storage workers. Worker IDs are storage
// identities, not reactor-core numbers, and are never recycled by format v1.
struct WorkerRegistry {
    uint32_t formatVersion = WORKER_REGISTRY_FORMAT_VERSION;
    uint32_t localPlacementAlgorithmVersion = LOCAL_PLACEMENT_ALGORITHM_VERSION;
    uint64_t generation = 0;
    uint32_t nextWorkerId = 0;
    std::vector<StorageWorker> workers;

    bool operator==(const WorkerRegistry&) const = default;
};

struct ValidatedWorkerRegistryPlacement {
    DesiredLocalVShardPlacement desiredPlacement;
    WorkerRegistryFingerprint fingerprint;
};

// Creates the first registry for a fresh node. At least one worker is required.
[[nodiscard]] WorkerRegistry createWorkerRegistry(unsigned activeWorkerCount);

// Reconciles the active worker count without recycling identities. Shrink marks
// the highest active IDs Draining. Growth reactivates the lowest Draining IDs
// before allocating new monotonically increasing IDs.
[[nodiscard]] WorkerRegistry reconcileWorkerRegistry(const WorkerRegistry& current,
                                                     unsigned requestedActiveWorkerCount);

// Full semantic validation, including the fixed VShard placement granularity.
// Throws std::invalid_argument on invalid state.
void validateWorkerRegistry(const WorkerRegistry& registry);

// Performs the same full validation and returns the desired placement built as
// part of that validation, allowing callers to cache it without a second full
// rendezvous calculation.
[[nodiscard]] ValidatedWorkerRegistryPlacement validateWorkerRegistryForPlacement(const WorkerRegistry& registry);

// Validates a single monotonic state transition. A checksum proves integrity,
// not freshness; callers compare a decoded candidate to their accepted state
// with this function to reject rollback/removal/recycling.
void validateWorkerRegistryTransition(const WorkerRegistry& previous, const WorkerRegistry& candidate);

// Format v1 accepts only the exact compact JSON emitted by the encoder. The
// checksum is CRC-32/ISO-HDLC over an explicitly defined binary representation,
// independent of JSON spelling and excluding the checksum field itself.
[[nodiscard]] std::string encodeWorkerRegistryJson(const WorkerRegistry& registry);
[[nodiscard]] WorkerRegistry decodeWorkerRegistryJson(std::string_view json);
[[nodiscard]] WorkerRegistryFingerprint fingerprintWorkerRegistry(const WorkerRegistry& registry);

}  // namespace timestar::cluster
