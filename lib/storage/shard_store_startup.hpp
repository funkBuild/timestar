#pragma once

#include "storage_layout.hpp"

#include <string>

namespace timestar {

// Outcome of the read-only startup inspection of a legacy shard_N data root.
//
// Only FreshStore and MatchingShardCount permit startup. Every other state is a
// fail-closed refusal: the historical automatic core-count rebalancer could
// replace a populated NativeIndex with an empty one, making already-ingested
// series undiscoverable, so an ambiguous or changed store is never migrated by
// normal server startup.
enum class ShardStoreStartupStatus {
    FreshStore,                  // No existing shard data; record the count on commit.
    MatchingShardCount,          // Existing store already at the requested core count.
    UnsafeShardCountChange,      // Existing store at a different core count — refused.
    InterruptedLegacyRebalance,  // A prior rebalance did not finish — refused.
    InvalidMetadata,             // Store present but its shard metadata is unusable — refused.
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

// Read-only startup safety gate for the legacy shard_N storage layout.
//
// This class deliberately has no dependency on ShardRebalancer: normal server
// startup may inspect the store and, on success, record a safe shard count, but
// it must never invoke the legacy mutating migration (execute/recoverIfNeeded)
// automatically. Exclusion between processes is provided by the data-directory
// lock the server already holds, so this gate performs no locking itself.
class ShardStoreStartup {
public:
    explicit ShardStoreStartup(StorageLayout layout);

    // Pure inspection: reads shard_count.meta, rebalance.state, and the shard_N
    // directory set. Creates and mutates nothing.
    [[nodiscard]] ShardStoreInspection inspect(unsigned requestedShardCount) const;

    // Human-readable operator guidance for a refused inspection.
    [[nodiscard]] std::string failureMessage(const ShardStoreInspection& inspection) const;

    // Records the requested count in shard_count.meta once Engine initialization
    // has produced the canonical shard directories. Idempotent for a matching
    // store. Must only be called for an inspection that canStart().
    void commitAfterInitialization(const ShardStoreInspection& inspection) const;

    [[nodiscard]] const StorageLayout& layout() const noexcept { return layout_; }

private:
    StorageLayout layout_;
};

// Stateful startup boundary used by the server. Runs the inspection at
// construction and makes the required order observable: inspect (construct),
// complete Engine initialization, then commit the shard-count marker.
class ShardStoreStartupSession {
public:
    ShardStoreStartupSession(StorageLayout layout, unsigned requestedShardCount);

    [[nodiscard]] const ShardStoreInspection& inspection() const noexcept { return inspection_; }
    [[nodiscard]] bool canStart() const noexcept { return inspection_.canStart(); }
    [[nodiscard]] std::string failureMessage() const;

    void commitEngineInitialization();

private:
    ShardStoreStartup startup_;
    ShardStoreInspection inspection_;
    bool committed_ = false;
};

}  // namespace timestar
