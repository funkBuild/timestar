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
        int inputLevel;
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
