#pragma once

#include "../core/vshard.hpp"

#include <array>
#include <compare>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace timestar::cluster {

inline constexpr uint32_t LOCAL_PLACEMENT_ALGORITHM_VERSION = 1;
inline constexpr uint32_t MAX_LOCAL_STORAGE_WORKERS = 256;
inline constexpr uint32_t MAX_STORAGE_WORKER_WEIGHT = 1024;
inline constexpr uint32_t MAX_TOTAL_STORAGE_WORKER_TICKETS = 8192;

class StorageWorkerId {
public:
    constexpr StorageWorkerId() noexcept = default;
    explicit constexpr StorageWorkerId(uint32_t value) noexcept : value_(value) {}

    [[nodiscard]] constexpr uint32_t value() const noexcept { return value_; }
    [[nodiscard]] constexpr bool valid() const noexcept { return value_ != 0; }

    auto operator<=>(const StorageWorkerId&) const = default;

private:
    uint32_t value_ = 0;
};

enum class StorageWorkerState : uint8_t {
    Active,
    Draining,
};

struct StorageWorker {
    StorageWorkerId id;
    uint32_t weight = 1;
    StorageWorkerState state = StorageWorkerState::Active;
};

struct DesiredOwnershipChange {
    uint16_t vshard = 0;
    StorageWorkerId effectiveWorker;
    StorageWorkerId desiredWorker;
};

// A comparison result only. It does not authorize or execute a handoff.
struct DesiredPlacementDelta {
    std::vector<DesiredOwnershipChange> changes;
};

// Pure desired-state calculation for node-local execution placement. This is
// deliberately separate from effective ownership and legacy shard_N routing.
class DesiredLocalVShardPlacement {
public:
    static DesiredLocalVShardPlacement build(std::span<const StorageWorker> workers);
    static DesiredLocalVShardPlacement build(const std::vector<StorageWorker>& workers) {
        return build(std::span<const StorageWorker>(workers));
    }

    [[nodiscard]] StorageWorkerId workerFor(uint16_t vshard) const;
    [[nodiscard]] const std::array<StorageWorkerId, VIRTUAL_SHARD_COUNT>& owners() const noexcept { return owners_; }

    // Fails if an effective owner is absent from the worker registry. A worker
    // must remain recorded (normally Draining) until fenced handoffs have made
    // its effective ownership count zero.
    [[nodiscard]] DesiredPlacementDelta deltaFromEffective(
        const std::array<StorageWorkerId, VIRTUAL_SHARD_COUNT>& effectiveOwners) const;

private:
    DesiredLocalVShardPlacement() = default;

    std::array<StorageWorkerId, VIRTUAL_SHARD_COUNT> owners_{};
    std::vector<StorageWorkerId> recordedWorkers_;
};

struct WorkerCoreAssignment {
    StorageWorkerId worker;
    unsigned reactorCore = 0;
};

// Runtime execution mapping only. Reactor cores are not storage identities and
// are never replica failure domains. Draining workers remain mapped so their
// effective VShards stay addressable until drain completion.
class WorkerRuntimeMap {
public:
    static WorkerRuntimeMap build(std::span<const StorageWorker> workers, unsigned reactorCoreCount);
    static WorkerRuntimeMap build(const std::vector<StorageWorker>& workers, unsigned reactorCoreCount) {
        return build(std::span<const StorageWorker>(workers), reactorCoreCount);
    }

    [[nodiscard]] std::optional<unsigned> coreFor(StorageWorkerId worker) const noexcept;
    [[nodiscard]] const std::vector<WorkerCoreAssignment>& assignments() const noexcept { return assignments_; }

private:
    std::vector<WorkerCoreAssignment> assignments_;
};

}  // namespace timestar::cluster
