#pragma once

#include "aligned_buffer.hpp"
#include "memory_store.hpp"
#include "timestar_config.hpp"
#include "tsm.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <vector>

// Forward declaration
class TSMCompactor;

class TSMFileManager {
private:
    static constexpr size_t MAX_TIERS = 5;
    static size_t filesPerCompaction() { return timestar::config().storage.compaction.tier0_min_files; }

    int shardId;
    // No atomic needed: TSMFileManager is a per-shard object in Seastar's shard-per-core model,
    // only accessed from a single thread.
    uint64_t nextSequenceId = 0;
    // Track files by tier for compaction
    std::vector<seastar::shared_ptr<TSM>> tiers[MAX_TIERS];

    // Compactor (unique ownership, destructor defined in .cpp where TSMCompactor is complete)
    std::unique_ptr<TSMCompactor> compactor;

    // Counter for completed compactions (read by Engine to update Prometheus metrics)
    uint64_t completedCompactions_ = 0;
    std::optional<seastar::future<>> compactionTask;

    // I/O scheduling group for compaction (lower priority than queries).
    // Set by Engine via setCompactionGroup() after scheduling groups are created.
    seastar::scheduling_group _compactionGroup;
    bool _compactionGroupSet = false;

    seastar::future<> openTsmFile(std::string path);
    std::string basePath();
    seastar::future<> checkAndTriggerCompaction();

    std::map<uint64_t, seastar::shared_ptr<TSM>> sequencedTsmFiles;

public:
    // Read-only access to the sequenced TSM file map.
    const std::map<uint64_t, seastar::shared_ptr<TSM>>& getSequencedTsmFiles() const { return sequencedTsmFiles; }

    // Direct insertion into the sequenced file map (for testing).
    void setSequencedTsmFile(uint64_t seq, seastar::shared_ptr<TSM> tsm) { sequencedTsmFiles[seq] = std::move(tsm); }

    TSMFileManager();
    ~TSMFileManager();  // Defined in .cpp where TSMCompactor is complete

    seastar::future<> init();
    seastar::future<> stop();
    seastar::future<> writeMemstore(seastar::shared_ptr<MemoryStore> memStore, uint64_t tier = 0);
    std::optional<TSMValueType> getSeriesType(const std::string& seriesKey);
    std::optional<TSMValueType> getSeriesType(const SeriesId128& seriesId);

    // Compaction support
    std::vector<seastar::shared_ptr<TSM>> getFilesInTier(uint64_t tier) const;
    size_t getFileCountInTier(uint64_t tier) const;
    bool shouldCompactTier(uint64_t tier) const;
    seastar::future<> addTSMFile(seastar::shared_ptr<TSM> file);
    seastar::future<> removeTSMFiles(const std::vector<seastar::shared_ptr<TSM>>& files);

    // Allocate a globally unique sequence ID for new TSM files
    uint64_t allocateSequenceId() { return nextSequenceId++; }

    // Get the compactor (for tombstone rewrites)
    TSMCompactor* getCompactor() { return compactor.get(); }

    // Number of compactions completed since startup (for Prometheus metrics)
    uint64_t getCompletedCompactions() const { return completedCompactions_; }

    // Set the I/O scheduling group for background compaction.
    void setCompactionGroup(seastar::scheduling_group sg) {
        _compactionGroup = sg;
        _compactionGroupSet = true;
    }

    // Start background compaction
    seastar::future<> startCompactionLoop();
    seastar::future<> stopCompactionLoop();

    // Per-shard query I/O semaphore.  Gates concurrent DMA reads during
    // query execution to prevent reactor stalls from unbounded parallel I/O.
    // Series-level concurrency stays high (64) for CPU-bound work; only
    // actual disk reads are throttled.  Default 16 matches typical NVMe
    // queue depth while keeping the pipeline full.
    seastar::semaphore queryIoSem{16};
};
