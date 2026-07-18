#pragma once

#include "storage_layout.hpp"

#include <filesystem>
#include <string>

namespace timestar {

namespace cluster {
class WorkerRegistryStore;
}

enum class ShardStoreStartupStatus {
    FreshStore,
    MatchingShardCount,
    UnsafeShardCountChange,
    InterruptedLegacyRebalance,
    UncommittedInitialization,
    IncompleteStore,
    InvalidMetadata,
};

struct ShardStoreInspection {
    ShardStoreStartupStatus status = ShardStoreStartupStatus::InvalidMetadata;
    unsigned previousShardCount = 0;
    unsigned requestedShardCount = 0;
    std::string detail;

    [[nodiscard]] bool canStart() const noexcept {
        return status == ShardStoreStartupStatus::FreshStore || status == ShardStoreStartupStatus::MatchingShardCount;
    }

    [[nodiscard]] bool isFresh() const noexcept { return status == ShardStoreStartupStatus::FreshStore; }
};

// Exclusive advisory ownership of one legacy data root. The directory file
// descriptor remains open for the object's lifetime, so the lock covers the
// inspection-to-shutdown interval without creating a lock file in the store.
class ShardStoreLock {
public:
    ShardStoreLock(const ShardStoreLock&) = delete;
    ShardStoreLock& operator=(const ShardStoreLock&) = delete;
    ShardStoreLock(ShardStoreLock&& other) noexcept;
    ShardStoreLock& operator=(ShardStoreLock&& other) noexcept;
    ~ShardStoreLock();

private:
    friend class cluster::WorkerRegistryStore;
    friend class ShardStoreStartup;
    ShardStoreLock(int directoryFd, std::filesystem::path dataDir);

    int directoryFd_ = -1;
    std::filesystem::path dataDir_;
};

// Read-only startup inspection for the legacy shard_N storage layout.
//
// This class deliberately has no dependency on ShardRebalancer: normal server
// startup may inspect and record a safe shard count, but it must never invoke
// the legacy mutating migration automatically.
class ShardStoreStartup {
public:
    explicit ShardStoreStartup(StorageLayout layout);

    // Creates the root when absent, acquires a non-blocking exclusive lock on
    // its directory inode, and rejects a symlinked root.
    [[nodiscard]] ShardStoreLock acquireExclusiveLock() const;

    [[nodiscard]] ShardStoreInspection inspect(unsigned requestedShardCount, const ShardStoreLock& lock) const;
    [[nodiscard]] std::string failureMessage(const ShardStoreInspection& inspection) const;

    // Called after complete Engine initialization. It commits a genuinely
    // fresh layout after validating every shard, and is a no-op for an already
    // committed matching layout.
    void commitAfterInitialization(const ShardStoreInspection& inspection, const ShardStoreLock& lock) const;

private:
    const StorageLayout layout_;
};

// Stateful startup boundary used by the server. The session owns the root
// lock for its whole lifetime and makes the required order observable and
// testable: inspect, authorize the first storage mutation, complete Engine
// initialization, then commit the fresh-store marker.
class ShardStoreStartupSession {
public:
    ShardStoreStartupSession(StorageLayout layout, unsigned requestedShardCount);

    [[nodiscard]] const ShardStoreInspection& inspection() const noexcept { return inspection_; }
    [[nodiscard]] bool canStart() const noexcept { return inspection_.canStart(); }
    [[nodiscard]] std::string failureMessage() const;

    void authorizeFirstStorageMutation();
    void commitEngineInitialization();

private:
    enum class State {
        Inspected,
        MutationAuthorized,
        InitializationCommitted,
    };

    ShardStoreStartup startup_;
    ShardStoreLock lock_;
    ShardStoreInspection inspection_;
    State state_ = State::Inspected;
};

}  // namespace timestar
