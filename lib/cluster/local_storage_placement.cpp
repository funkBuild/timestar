#include "local_storage_placement.hpp"

#include <algorithm>
#include <stdexcept>
#include <string>

namespace timestar::cluster {
namespace {

constexpr uint64_t vshardHashSeed = 0x4c53504c41434531ULL;  // "LSPLACE1"

struct ValidatedWorkers {
    std::vector<StorageWorker> sorted;
    uint32_t totalTickets = 0;
};

uint64_t mix64(uint64_t value) noexcept {
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebULL;
    value ^= value >> 31;
    return value;
}

uint64_t ticketScore(uint16_t vshard, StorageWorkerId worker, uint32_t ticket) noexcept {
    // Version 1 tuple encoding is fixed as:
    //   candidate = worker_id:u32 || ticket_index:u32 (ticket numbering starts at zero)
    //   salt      = mix64(seed XOR vshard:u16)
    //   score     = mix64(candidate XOR salt)
    const uint64_t candidate = (static_cast<uint64_t>(worker.value()) << 32) | ticket;
    const uint64_t salt = mix64(vshardHashSeed ^ vshard);
    return mix64(candidate ^ salt);
}

ValidatedWorkers validateWorkers(std::span<const StorageWorker> workers) {
    if (workers.size() > MAX_LOCAL_STORAGE_WORKERS) {
        throw std::invalid_argument("local storage worker count exceeds the supported limit");
    }

    ValidatedWorkers validated;
    validated.sorted.assign(workers.begin(), workers.end());
    std::sort(validated.sorted.begin(), validated.sorted.end(),
              [](const StorageWorker& left, const StorageWorker& right) { return left.id < right.id; });

    StorageWorkerId previous;
    for (const auto& worker : validated.sorted) {
        if (!worker.id.valid())
            throw std::invalid_argument("storage worker ID zero is reserved as invalid");
        if (previous.valid() && worker.id == previous)
            throw std::invalid_argument("duplicate storage worker ID " + std::to_string(worker.id.value()));
        if (worker.weight == 0 || worker.weight > MAX_STORAGE_WORKER_WEIGHT) {
            throw std::invalid_argument("storage worker weight is outside the supported absolute-ticket range");
        }
        if (worker.state != StorageWorkerState::Active && worker.state != StorageWorkerState::Draining)
            throw std::invalid_argument("storage worker has an unknown lifecycle state");
        if (validated.totalTickets > MAX_TOTAL_STORAGE_WORKER_TICKETS - worker.weight) {
            throw std::invalid_argument("aggregate storage worker tickets exceed the supported limit");
        }
        validated.totalTickets += worker.weight;
        previous = worker.id;
    }
    return validated;
}

bool containsWorker(const std::vector<StorageWorkerId>& workers, StorageWorkerId worker) {
    return std::binary_search(workers.begin(), workers.end(), worker);
}

}  // namespace

DesiredLocalVShardPlacement DesiredLocalVShardPlacement::build(std::span<const StorageWorker> workers) {
    auto validated = validateWorkers(workers);

    DesiredLocalVShardPlacement placement;
    placement.recordedWorkers_.reserve(validated.sorted.size());
    for (const auto& worker : validated.sorted)
        placement.recordedWorkers_.push_back(worker.id);

    const bool hasActiveWorker = std::ranges::any_of(
        validated.sorted, [](const StorageWorker& worker) { return worker.state == StorageWorkerState::Active; });
    if (!hasActiveWorker)
        throw std::invalid_argument("desired local placement requires at least one active storage worker");

    std::vector<uint32_t> ownedCounts(validated.sorted.size(), 0);
    for (uint32_t vshard = 0; vshard < VIRTUAL_SHARD_COUNT; ++vshard) {
        uint64_t bestScore = 0;
        StorageWorkerId bestWorker;
        size_t bestWorkerIndex = 0;
        for (size_t workerIndex = 0; workerIndex < validated.sorted.size(); ++workerIndex) {
            const auto& worker = validated.sorted[workerIndex];
            if (worker.state != StorageWorkerState::Active)
                continue;
            for (uint32_t ticket = 0; ticket < worker.weight; ++ticket) {
                const auto score = ticketScore(static_cast<uint16_t>(vshard), worker.id, ticket);
                if (!bestWorker.valid() || score > bestScore || (score == bestScore && worker.id < bestWorker)) {
                    bestScore = score;
                    bestWorker = worker.id;
                    bestWorkerIndex = workerIndex;
                }
            }
        }
        placement.owners_[vshard] = bestWorker;
        ++ownedCounts[bestWorkerIndex];
    }

    for (size_t workerIndex = 0; workerIndex < validated.sorted.size(); ++workerIndex) {
        const auto& worker = validated.sorted[workerIndex];
        if (worker.state == StorageWorkerState::Active && ownedCounts[workerIndex] == 0) {
            throw std::invalid_argument("active storage worker " + std::to_string(worker.id.value()) +
                                        " receives no VShards at the fixed placement granularity");
        }
    }

    return placement;
}

StorageWorkerId DesiredLocalVShardPlacement::workerFor(uint16_t vshard) const {
    if (vshard >= VIRTUAL_SHARD_COUNT)
        throw std::out_of_range("VShard ID is outside the fixed placement range");
    return owners_[vshard];
}

DesiredPlacementDelta DesiredLocalVShardPlacement::deltaFromEffective(
    const std::array<StorageWorkerId, VIRTUAL_SHARD_COUNT>& effectiveOwners) const {
    DesiredPlacementDelta delta;
    for (uint32_t vshard = 0; vshard < VIRTUAL_SHARD_COUNT; ++vshard) {
        const auto effective = effectiveOwners[vshard];
        if (!effective.valid())
            throw std::invalid_argument("effective local placement contains an invalid storage worker ID");
        if (!containsWorker(recordedWorkers_, effective)) {
            throw std::invalid_argument("effective storage worker " + std::to_string(effective.value()) +
                                        " was hard-removed before its VShards drained");
        }
        if (effective != owners_[vshard]) {
            delta.changes.push_back(DesiredOwnershipChange{static_cast<uint16_t>(vshard), effective, owners_[vshard]});
        }
    }
    return delta;
}

WorkerRuntimeMap WorkerRuntimeMap::build(std::span<const StorageWorker> workers, unsigned reactorCoreCount) {
    if (reactorCoreCount == 0)
        throw std::invalid_argument("worker runtime mapping requires at least one reactor core");

    auto validated = validateWorkers(workers);
    WorkerRuntimeMap runtime;
    runtime.assignments_.reserve(validated.sorted.size());

    auto byDescendingWeight = validated.sorted;
    std::sort(byDescendingWeight.begin(), byDescendingWeight.end(),
              [](const StorageWorker& left, const StorageWorker& right) {
                  if (left.weight != right.weight)
                      return left.weight > right.weight;
                  return left.id < right.id;
              });

    std::vector<uint32_t> coreTicketLoads(reactorCoreCount, 0);
    for (const auto& worker : byDescendingWeight) {
        const auto leastLoaded = std::min_element(coreTicketLoads.begin(), coreTicketLoads.end());
        const auto core = static_cast<unsigned>(std::distance(coreTicketLoads.begin(), leastLoaded));
        *leastLoaded += worker.weight;
        runtime.assignments_.push_back(WorkerCoreAssignment{worker.id, core});
    }
    std::sort(
        runtime.assignments_.begin(), runtime.assignments_.end(),
        [](const WorkerCoreAssignment& left, const WorkerCoreAssignment& right) { return left.worker < right.worker; });
    return runtime;
}

std::optional<unsigned> WorkerRuntimeMap::coreFor(StorageWorkerId worker) const noexcept {
    const auto found = std::lower_bound(
        assignments_.begin(), assignments_.end(), worker,
        [](const WorkerCoreAssignment& assignment, StorageWorkerId sought) { return assignment.worker < sought; });
    if (found == assignments_.end() || found->worker != worker)
        return std::nullopt;
    return found->reactorCore;
}

}  // namespace timestar::cluster
