#pragma once

#include "../storage/storage_layout.hpp"
#include "worker_registry.hpp"

#include <functional>
#include <optional>

namespace timestar {

class ShardStoreLock;

namespace cluster {

// Post-success durability boundaries used by process-crash tests. Observers
// must not throw; an exception terminates the process. Production callers
// normally leave the observer empty.
enum class WorkerRegistryCommitStage {
    TemporaryCreated,
    TemporaryWritten,
    TemporarySynced,
    TemporaryClosed,
    Renamed,
    DirectorySynced,
    RecoveryTemporarySynced,
    RecoveryTemporaryRemoved,
    RecoveryRenamed,
    RecoveryFinalSynced,
    RecoveryDirectorySynced,
};

using WorkerRegistryCommitObserver = std::function<void(WorkerRegistryCommitStage)>;

// Descriptor-relative durable storage for workers.json. Every operation
// requires the existing exclusive data-root lock; this class never acquires a
// competing lock and is deliberately disconnected from live Engine routing.
class WorkerRegistryStore {
public:
    explicit WorkerRegistryStore(StorageLayout layout, WorkerRegistryCommitObserver observer = {});

    // Returns the accepted registry, or nullopt for a fresh root. A complete,
    // valid forward temporary generation is rolled forward; partial/stale
    // scratch state is removed durably. Invalid authoritative state fails
    // closed and is preserved for inspection.
    [[nodiscard]] std::optional<WorkerRegistry> loadAndRecover(const ShardStoreLock& lock) const;

    // Installs an exact initial registry or one valid generation transition.
    // An identical candidate is a no-op and does not replace the inode.
    void install(const WorkerRegistry& candidate, const ShardStoreLock& lock) const;

private:
    [[nodiscard]] int lockedDirectoryFd(const ShardStoreLock& lock) const;
    void observe(WorkerRegistryCommitStage stage) const noexcept;

    const StorageLayout layout_;
    const WorkerRegistryCommitObserver observer_;
};

}  // namespace cluster
}  // namespace timestar
