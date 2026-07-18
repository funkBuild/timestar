#include "worker_registry.hpp"

#include "../utils/crc32.hpp"

#include <glaze/json.hpp>

#include <xxhash.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace {

// The JSON representation is deliberately separate from the domain model so
// enum and integer encodings cannot change implicitly with C++ types.
struct WorkerRegistryWorkerJson {
    uint32_t id = 0;
    uint32_t weight = 0;
    uint8_t state = 0;
};

struct WorkerRegistryJson {
    uint32_t formatVersion = 0;
    uint32_t localPlacementAlgorithmVersion = 0;
    uint64_t generation = 0;
    uint32_t nextWorkerId = 0;
    std::vector<WorkerRegistryWorkerJson> workers;
    uint32_t checksum = 0;
};

}  // namespace

template <>
struct glz::meta<WorkerRegistryWorkerJson> {
    using T = WorkerRegistryWorkerJson;
    static constexpr auto value = object("id", &T::id, "weight", &T::weight, "state", &T::state);
};

template <>
struct glz::meta<WorkerRegistryJson> {
    using T = WorkerRegistryJson;
    static constexpr auto value =
        object("formatVersion", &T::formatVersion, "localPlacementAlgorithmVersion", &T::localPlacementAlgorithmVersion,
               "generation", &T::generation, "nextWorkerId", &T::nextWorkerId, "workers", &T::workers, "checksum",
               &T::checksum);
};

namespace timestar::cluster {
namespace {

constexpr uint8_t activeStateEncoding = 0;
constexpr uint8_t drainingStateEncoding = 1;

template <typename Integer>
void appendLittleEndian(std::vector<uint8_t>& output, Integer value) {
    static_assert(std::is_unsigned_v<Integer>);
    for (size_t byte = 0; byte < sizeof(Integer); ++byte) {
        output.push_back(static_cast<uint8_t>(value & 0xffU));
        value >>= 8;
    }
}

uint8_t encodeState(StorageWorkerState state) {
    switch (state) {
        case StorageWorkerState::Active:
            return activeStateEncoding;
        case StorageWorkerState::Draining:
            return drainingStateEncoding;
    }
    throw std::invalid_argument("worker registry contains an unknown lifecycle state");
}

StorageWorkerState decodeState(uint8_t state) {
    switch (state) {
        case activeStateEncoding:
            return StorageWorkerState::Active;
        case drainingStateEncoding:
            return StorageWorkerState::Draining;
        default:
            throw std::invalid_argument("worker registry JSON contains an unknown lifecycle state encoding");
    }
}

uint32_t registryChecksum(const WorkerRegistry& registry) {
    // CRC-32/ISO-HDLC (reflected polynomial 0xEDB88320, initial value and
    // final XOR 0xffffffff). Format v1 hashes, in this exact order:
    //   format:u32, placement_algorithm:u32, generation:u64,
    //   next_worker_id:u32, worker_count:u32,
    //   repeated {id:u32, absolute_weight:u32, state:u8}.
    // Every integer is little-endian. The JSON checksum is excluded.
    std::vector<uint8_t> bytes;
    bytes.reserve(24 + registry.workers.size() * 9);
    appendLittleEndian(bytes, registry.formatVersion);
    appendLittleEndian(bytes, registry.localPlacementAlgorithmVersion);
    appendLittleEndian(bytes, registry.generation);
    appendLittleEndian(bytes, registry.nextWorkerId);
    appendLittleEndian(bytes, static_cast<uint32_t>(registry.workers.size()));
    for (const auto& worker : registry.workers) {
        appendLittleEndian(bytes, worker.id.value());
        appendLittleEndian(bytes, worker.weight);
        bytes.push_back(encodeState(worker.state));
    }
    return CRC32::compute(bytes.data(), bytes.size());
}

WorkerRegistryJson toJsonModel(const WorkerRegistry& registry) {
    WorkerRegistryJson json;
    json.formatVersion = registry.formatVersion;
    json.localPlacementAlgorithmVersion = registry.localPlacementAlgorithmVersion;
    json.generation = registry.generation;
    json.nextWorkerId = registry.nextWorkerId;
    json.workers.reserve(registry.workers.size());
    for (const auto& worker : registry.workers) {
        json.workers.push_back(WorkerRegistryWorkerJson{worker.id.value(), worker.weight, encodeState(worker.state)});
    }
    json.checksum = registryChecksum(registry);
    return json;
}

WorkerRegistry fromJsonModel(const WorkerRegistryJson& json) {
    WorkerRegistry registry;
    registry.formatVersion = json.formatVersion;
    registry.localPlacementAlgorithmVersion = json.localPlacementAlgorithmVersion;
    registry.generation = json.generation;
    registry.nextWorkerId = json.nextWorkerId;
    registry.workers.reserve(json.workers.size());
    for (const auto& worker : json.workers) {
        registry.workers.push_back(StorageWorker{StorageWorkerId(worker.id), worker.weight, decodeState(worker.state)});
    }
    return registry;
}

[[noreturn]] void invalidTransition(const char* reason) {
    throw std::invalid_argument(std::string("invalid worker registry transition: ") + reason);
}

std::string encodeValidatedWorkerRegistryJson(const WorkerRegistry& registry) {
    auto encoded = glz::write_json(toJsonModel(registry));
    if (!encoded)
        throw std::runtime_error("failed to encode worker registry JSON");
    return *encoded;
}

WorkerRegistryFingerprint fingerprintValidatedWorkerRegistry(const WorkerRegistry& registry) {
    const auto canonical = encodeValidatedWorkerRegistryJson(registry);
    const auto fingerprint = XXH3_128bits(canonical.data(), canonical.size());
    return {.low64 = fingerprint.low64, .high64 = fingerprint.high64};
}

}  // namespace

ValidatedWorkerRegistryPlacement validateWorkerRegistryForPlacement(const WorkerRegistry& registry) {
    if (registry.formatVersion != WORKER_REGISTRY_FORMAT_VERSION)
        throw std::invalid_argument("unsupported worker registry format version");
    if (registry.localPlacementAlgorithmVersion != LOCAL_PLACEMENT_ALGORITHM_VERSION)
        throw std::invalid_argument("unsupported local placement algorithm version");
    if (registry.generation == 0)
        throw std::invalid_argument("worker registry generation zero is reserved");
    if (registry.workers.empty())
        throw std::invalid_argument("worker registry requires at least one recorded worker");
    if (registry.workers.size() > MAX_LOCAL_STORAGE_WORKERS)
        throw std::invalid_argument("worker registry exceeds the supported worker limit");

    uint32_t previousId = 0;
    for (const auto& worker : registry.workers) {
        if (worker.id.value() <= previousId)
            throw std::invalid_argument("worker registry records must have strictly increasing IDs");
        previousId = worker.id.value();
    }
    if (previousId == std::numeric_limits<uint32_t>::max() || registry.nextWorkerId != previousId + 1)
        throw std::invalid_argument("worker registry next ID must immediately follow its highest recorded ID");

    // This intentionally centralizes every Task 1 invariant, including ticket
    // limits, lifecycle values, an active worker, and non-zero VShard ownership
    // at the fixed placement granularity.
    return {.desiredPlacement = DesiredLocalVShardPlacement::build(registry.workers),
            .fingerprint = fingerprintValidatedWorkerRegistry(registry)};
}

void validateWorkerRegistry(const WorkerRegistry& registry) {
    (void)validateWorkerRegistryForPlacement(registry);
}

WorkerRegistry createWorkerRegistry(unsigned activeWorkerCount) {
    if (activeWorkerCount == 0 || activeWorkerCount > MAX_LOCAL_STORAGE_WORKERS)
        throw std::invalid_argument("initial active worker count is outside the supported range");

    WorkerRegistry registry;
    registry.generation = 1;
    registry.nextWorkerId = activeWorkerCount + 1;
    registry.workers.reserve(activeWorkerCount);
    for (uint32_t id = 1; id <= activeWorkerCount; ++id)
        registry.workers.push_back(StorageWorker{StorageWorkerId(id), 1, StorageWorkerState::Active});
    validateWorkerRegistry(registry);
    return registry;
}

void validateWorkerRegistryTransition(const WorkerRegistry& previous, const WorkerRegistry& candidate) {
    validateWorkerRegistry(previous);
    validateWorkerRegistry(candidate);

    if (candidate == previous)
        return;
    if (previous.generation == std::numeric_limits<uint64_t>::max() ||
        candidate.generation != previous.generation + 1) {
        invalidTransition("changed state must advance generation exactly once");
    }
    if (candidate.workers.size() < previous.workers.size())
        invalidTransition("recorded workers cannot be removed");

    bool stateChanged = false;
    bool activated = false;
    bool drained = false;
    for (size_t index = 0; index < previous.workers.size(); ++index) {
        const auto& before = previous.workers[index];
        const auto& after = candidate.workers[index];
        if (after.id != before.id)
            invalidTransition("existing worker IDs cannot change or move");
        if (after.weight != before.weight)
            invalidTransition("existing worker weights are immutable");
        if (after.state != before.state) {
            stateChanged = true;
            activated |= before.state == StorageWorkerState::Draining && after.state == StorageWorkerState::Active;
            drained |= before.state == StorageWorkerState::Active && after.state == StorageWorkerState::Draining;
        }
    }
    if (activated && drained)
        invalidTransition("one generation cannot mix activation and draining");

    const size_t appendedCount = candidate.workers.size() - previous.workers.size();
    if (appendedCount > std::numeric_limits<uint32_t>::max() - previous.nextWorkerId)
        invalidTransition("next worker ID would overflow");
    const uint32_t expectedNextWorkerId = previous.nextWorkerId + static_cast<uint32_t>(appendedCount);
    if (candidate.nextWorkerId != expectedNextWorkerId)
        invalidTransition("next worker ID must advance by exactly the appended record count");

    for (size_t offset = 0; offset < appendedCount; ++offset) {
        const auto& appended = candidate.workers[previous.workers.size() + offset];
        const auto expectedId = previous.nextWorkerId + static_cast<uint32_t>(offset);
        if (appended.id.value() != expectedId)
            invalidTransition("appended worker IDs must be contiguous and monotonic");
        if (appended.weight != 1 || appended.state != StorageWorkerState::Active)
            invalidTransition("new workers must start Active with absolute weight one");
    }
    if (drained && appendedCount != 0)
        invalidTransition("one generation cannot both drain and append workers");
    if (!stateChanged && appendedCount == 0)
        invalidTransition("generation changed without a registry state change");
}

WorkerRegistry reconcileWorkerRegistry(const WorkerRegistry& current, unsigned requestedActiveWorkerCount) {
    validateWorkerRegistry(current);
    if (requestedActiveWorkerCount == 0 || requestedActiveWorkerCount > MAX_LOCAL_STORAGE_WORKERS)
        throw std::invalid_argument("requested active worker count is outside the supported range");

    const auto isActive = [](const StorageWorker& worker) { return worker.state == StorageWorkerState::Active; };
    const auto currentActiveCount = static_cast<unsigned>(std::ranges::count_if(current.workers, isActive));
    if (currentActiveCount == requestedActiveWorkerCount)
        return current;
    if (current.generation == std::numeric_limits<uint64_t>::max())
        throw std::overflow_error("worker registry generation exhausted");

    WorkerRegistry result = current;
    if (requestedActiveWorkerCount < currentActiveCount) {
        auto toDrain = currentActiveCount - requestedActiveWorkerCount;
        for (auto worker = result.workers.rbegin(); worker != result.workers.rend() && toDrain != 0; ++worker) {
            if (worker->state == StorageWorkerState::Active) {
                worker->state = StorageWorkerState::Draining;
                --toDrain;
            }
        }
    } else {
        auto toActivate = requestedActiveWorkerCount - currentActiveCount;
        for (auto& worker : result.workers) {
            if (toActivate != 0 && worker.state == StorageWorkerState::Draining) {
                worker.state = StorageWorkerState::Active;
                --toActivate;
            }
        }
        while (toActivate != 0) {
            if (result.workers.size() == MAX_LOCAL_STORAGE_WORKERS)
                throw std::invalid_argument("worker registry has no remaining identity slots");
            if (result.nextWorkerId == std::numeric_limits<uint32_t>::max())
                throw std::overflow_error("worker registry ID space exhausted");
            result.workers.push_back(
                StorageWorker{StorageWorkerId(result.nextWorkerId), 1, StorageWorkerState::Active});
            ++result.nextWorkerId;
            --toActivate;
        }
    }
    ++result.generation;
    validateWorkerRegistryTransition(current, result);
    return result;
}

std::string encodeWorkerRegistryJson(const WorkerRegistry& registry) {
    validateWorkerRegistry(registry);
    return encodeValidatedWorkerRegistryJson(registry);
}

WorkerRegistryFingerprint fingerprintWorkerRegistry(const WorkerRegistry& registry) {
    return validateWorkerRegistryForPlacement(registry).fingerprint;
}

WorkerRegistry decodeWorkerRegistryJson(std::string_view json) {
    if (json.empty() || json.size() > MAX_WORKER_REGISTRY_JSON_BYTES)
        throw std::invalid_argument("worker registry JSON size is outside the supported range");

    WorkerRegistryJson decoded;
    const auto parseError = glz::read_json(decoded, json);
    if (parseError)
        throw std::invalid_argument("failed to parse worker registry JSON");

    auto registry = fromJsonModel(decoded);
    validateWorkerRegistry(registry);
    if (decoded.checksum != registryChecksum(registry))
        throw std::invalid_argument("worker registry checksum mismatch");

    // Requiring the byte-for-byte encoder output closes duplicate-key and
    // alternate-spelling ambiguities even if a JSON parser accepts them. It
    // also makes task 2b crash-recovery comparisons deterministic.
    if (encodeWorkerRegistryJson(registry) != json)
        throw std::invalid_argument("worker registry JSON is not in canonical format");
    return registry;
}

}  // namespace timestar::cluster
