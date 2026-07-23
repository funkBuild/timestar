#pragma once

#include "manifest.hpp"
#include "sstable.hpp"

#include <cstdint>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

namespace timestar::index {

// Configuration for the compaction engine.
struct CompactionConfig {
    uint32_t level0Threshold = 4;  // Trigger L0 compaction when this many L0 files exist
    // Simple tiered policy for L1+: when a level accumulates this many files,
    // merge the whole level into one file at the next level. L2 compactions
    // also fold in the existing L3 file(s) so L3 stays a single file and the
    // compaction becomes a full compaction (enabling tombstone GC) whenever
    // no L0/L1 files exist at that moment.
    uint32_t levelThreshold = 8;
    int blockSize = 16384;
    int bloomBitsPerKey = 15;
    uint32_t rateLimitMBps = 0;  // Max compaction write throughput (MB/s). 0 = unlimited.
    uint64_t tombstoneGracePeriodMs =
        10ULL * 24 * 3600 * 1000;  // Drop tombstones older than this (default 10 days). 0 = keep forever.
};

// Compaction engine: merges multiple SSTables into fewer, larger SSTables.
// Runs as a Seastar coroutine in the background.
class CompactionEngine {
public:
    CompactionEngine(std::string dataDir, Manifest& manifest, CompactionConfig config = {});

    // Check if compaction is needed and perform it if so.
    // Called after each MemTable flush.
    seastar::future<> maybeCompact();

    // Force compaction of all data (e.g., for manual compaction).
    seastar::future<> compactAll();

private:
    struct CompactionJob {
        int inputLevel = 0;
        std::vector<SSTableMetadata> inputFiles;
    };

    // Pick files for compaction. Returns nullopt if no compaction needed.
    std::optional<CompactionJob> pickCompaction();

    // Execute a compaction job: merge input files, write output, update manifest.
    seastar::future<> doCompaction(CompactionJob job);

    // Build the SSTable filename from a file number.
    std::string sstFilename(uint64_t fileNumber);

    std::string dataDir_;
    Manifest& manifest_;
    CompactionConfig config_;
};

}  // namespace timestar::index
