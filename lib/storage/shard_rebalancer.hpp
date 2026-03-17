#pragma once

#include <cstdint>
#include <map>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

namespace timestar {

// Persisted state for crash-safe rebalancing.
enum class RebalancePhase {
    None,            // No rebalance in progress
    InProgress,      // New shard dirs being written (old dirs intact)
    RenamesStarted,  // Old dirs being renamed to _old (point of no return)
    Complete         // New dirs in place, awaiting cleanup of _old dirs
};

struct RebalanceState {
    RebalancePhase phase = RebalancePhase::None;
    unsigned oldShardCount = 0;
    unsigned newShardCount = 0;
};

// Per-TSM-file analysis: which new shards does it map to?
struct TSMFileAction {
    std::string sourcePath;
    // If all series hash to a single shard, this is a move (rename).
    // Otherwise it's a split across multiple target shards.
    bool canMove = false;
    unsigned moveTargetShard = 0;
    // For splits: series IDs grouped by target shard (populated during execute)
};

class ShardRebalancer {
public:
    explicit ShardRebalancer(const std::string& dataDir);

    // Check if rebalancing is needed. Reads shard_count.meta and/or scans
    // for existing shard directories.
    bool isRebalanceNeeded(unsigned newShardCount);

    // The shard count from the previous run (0 if unknown/fresh install).
    unsigned previousShardCount() const { return _oldShardCount; }

    // Run the full rebalance. Must be called from shard 0 inside seastar::async
    // or a seastar::thread context (blocking filesystem + Seastar DMA I/O).
    seastar::future<> execute(unsigned newShardCount);

    // Recover from a partially-completed rebalance (crash recovery).
    // Called on startup if rebalance.state file exists.
    seastar::future<> recoverIfNeeded(unsigned newShardCount);

    // Write the current shard count to shard_count.meta.
    // Called after successful engine startup or rebalance.
    static void writeShardCountMeta(const std::string& dataDir, unsigned shardCount);

    // Read the persisted shard count. Returns 0 if file doesn't exist.
    static unsigned readShardCountMeta(const std::string& dataDir);

private:
    std::string _dataDir;
    unsigned _oldShardCount = 0;

    // Paths
    std::string shardDir(unsigned shard) const;
    std::string shardDirNew(unsigned shard) const;
    std::string shardDirOld(unsigned shard) const;
    std::string metaFilePath() const;
    std::string stateFilePath() const;

    // State file management
    void writeState(const RebalanceState& state);
    RebalanceState readState();
    void removeState();

    // Detect old shard count by scanning directories
    unsigned detectShardCountFromDirs() const;

    // Create staging directories for new shard layout
    void createStagingDirs(unsigned newShardCount);

    // Phase A: Process WAL files from old shards
    seastar::future<> processWALFiles(unsigned oldShardCount, unsigned newShardCount);

    // Phase B+C: Analyze and process TSM files (move or split)
    seastar::future<> processTSMFiles(unsigned oldShardCount, unsigned newShardCount);

    // Phase D: Copy NativeIndex directories from old shards to new shards
    void moveNativeIndex();

    // Phase E: Atomic directory cutover
    void performCutover(unsigned oldShardCount, unsigned newShardCount);

    // Phase F: Cleanup old directories
    void cleanup(unsigned oldShardCount);

    // Complete a partially-done cutover (crash recovery)
    void completeCutover(unsigned oldShardCount, unsigned newShardCount);
};

}  // namespace timestar
