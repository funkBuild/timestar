#pragma once

#include "query_parser.hpp"  // AggregationMethod enum
#include "retention_policy.hpp"
#include "series_compaction_data.hpp"  // Phase 3: Parallel series processing
#include "series_id.hpp"
#include "timestar_config.hpp"
#include "tsm.hpp"
#include "tsm_result.hpp"
#include "tsm_writer.hpp"

#include <atomic>
#include <memory>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <string>
#include <vector>

// Forward declarations
class CompactionStrategy;
class TSMFileManager;

// Compaction statistics for monitoring
struct CompactionStats {
    uint64_t filesCompacted = 0;
    uint64_t bytesRead = 0;
    uint64_t bytesWritten = 0;
    uint64_t pointsRead = 0;
    uint64_t pointsWritten = 0;
    uint64_t duplicatesRemoved = 0;
    std::chrono::milliseconds duration{0};
};

// Result from compact(): output path and stats, avoiding shared mutable state.
struct CompactionResult {
    std::string outputPath;
    CompactionStats stats;
};

// Represents a plan for compacting a set of TSM files
struct CompactionPlan {
    std::vector<seastar::shared_ptr<TSM>> sourceFiles;
    uint64_t targetTier;
    uint64_t targetSeqNum;
    std::string targetPath;
    uint64_t estimatedSize;

    bool isValid() const { return !sourceFiles.empty() && !targetPath.empty(); }
};

class TSMCompactor {
private:
    // Compaction limits and thresholds — read from TOML config at runtime.
    static size_t maxConcurrentCompactions() { return timestar::config().storage.compaction.max_concurrent; }
    static size_t maxMemoryPerCompaction() { return timestar::config().storage.compaction.max_memory; }
    static size_t batchSize() { return timestar::config().storage.compaction.batch_size; }
    static size_t tier0MinFiles() { return timestar::config().storage.compaction.tier0_min_files; }
    static size_t tier1MinFiles() { return timestar::config().storage.compaction.tier1_min_files; }
    static size_t tier2MinFiles() { return timestar::config().storage.compaction.tier2_min_files; }

    TSMFileManager* fileManager;
    std::unique_ptr<CompactionStrategy> strategy;
    // lastCompactStats removed — compact() now returns stats via CompactionResult
    seastar::semaphore compactionSemaphore{2};  // Re-initialized in constructor from config
    bool compactionEnabled{true};

    // Per-series retention context built in compact(), passed to processSeriesForCompaction
    struct SeriesRetentionContext {
        uint64_t ttlCutoff = 0;            // Points with ts < ttlCutoff are expired
        uint64_t downsampleThreshold = 0;  // Points with ts < threshold get downsampled
        uint64_t downsampleInterval = 0;   // Bucket size in nanoseconds
        timestar::AggregationMethod downsampleMethod = timestar::AggregationMethod::AVG;
    };
    using SeriesRetentionMap = std::unordered_map<SeriesId128, SeriesRetentionContext, SeriesId128::Hash>;

    // Pending retention context set by Engine before compaction
    std::unordered_map<std::string, RetentionPolicy> _pendingRetentionPolicies;
    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> _pendingSeriesMeasurementMap;

    // Track active compactions
    struct ActiveCompaction {
        CompactionPlan plan;
        std::chrono::steady_clock::time_point startTime;
        CompactionStats stats;
    };
    std::vector<ActiveCompaction> activeCompactions;

    // Generate output filename for compacted file
    std::string generateCompactedFilename(uint64_t tier, uint64_t seqNum);

    // Phase 3: Process series for compaction without writing (for parallel processing)
    template <typename T>
    seastar::future<SeriesCompactionData<T>> processSeriesForCompaction(
        const SeriesId128& seriesId, const std::vector<seastar::shared_ptr<TSM>>& sources,
        const SeriesRetentionMap& seriesRetention);

    // Phase 3: Write pre-processed series data to TSMWriter
    template <typename T>
    void writeSeriesCompactionData(TSMWriter& writer, SeriesCompactionData<T>&& data, CompactionStats& stats);

    // Get all unique series IDs from files
    std::vector<SeriesId128> getAllSeriesIds(const std::vector<seastar::shared_ptr<TSM>>& files);

public:
    explicit TSMCompactor(TSMFileManager* manager);
    ~TSMCompactor() = default;

    // Set retention policies and series->measurement map for the next compaction.
    // Called by Engine before triggering compaction so that TTL/downsampling
    // can be applied during processSeriesForCompaction().
    void setRetentionContext(
        const std::unordered_map<std::string, RetentionPolicy>& policies,
        const std::unordered_map<SeriesId128, std::string, SeriesId128::Hash>& seriesMeasurementMap) {
        _pendingRetentionPolicies = policies;
        _pendingSeriesMeasurementMap = seriesMeasurementMap;
    }

    // Set custom compaction strategy
    void setStrategy(std::unique_ptr<CompactionStrategy> newStrategy) { strategy = std::move(newStrategy); }

    // Main compaction method - merges files and returns result with path + stats.
    // retentionPolicies: per-measurement policies for TTL/downsampling (empty = no retention).
    // seriesMetadataMap: SeriesId128 -> measurement name, pre-built by caller for efficiency.
    // targetTier/targetSeq: pre-allocated from CompactionPlan.  When called
    // without a plan (e.g. from tests), pass 0 for both and compact() will
    // compute the tier from input files and allocate a fresh sequence ID.
    seastar::future<CompactionResult> compact(
        const std::vector<seastar::shared_ptr<TSM>>& files, uint64_t targetTier, uint64_t targetSeq,
        const std::unordered_map<std::string, RetentionPolicy>& retentionPolicies = {},
        const std::unordered_map<SeriesId128, std::string, SeriesId128::Hash>& seriesMeasurementMap = {});

    // Convenience overload for callers without a pre-allocated plan (auto-allocates tier/seq).
    seastar::future<CompactionResult> compact(
        const std::vector<seastar::shared_ptr<TSM>>& files,
        const std::unordered_map<std::string, RetentionPolicy>& retentionPolicies,
        const std::unordered_map<SeriesId128, std::string, SeriesId128::Hash>& seriesMeasurementMap) {
        return compact(files, 0, 0, retentionPolicies, seriesMeasurementMap);
    }

    // Convenience overload: compact files with no retention and auto-allocated tier/seq.
    seastar::future<CompactionResult> compact(const std::vector<seastar::shared_ptr<TSM>>& files) {
        return compact(files, 0, 0);
    }

    // Create a compaction plan for given tier
    CompactionPlan planCompaction(uint64_t tier);

    // Execute a compaction plan
    seastar::future<CompactionStats> executeCompaction(const CompactionPlan& plan);

    // Check if compaction is needed for a tier
    bool shouldCompact(uint64_t tier) const;

    // Run continuous background compaction
    seastar::future<> runCompactionLoop();

    // Stop all compactions
    void stopCompaction() { compactionEnabled = false; }

    // Get current compaction statistics
    std::vector<CompactionStats> getActiveCompactionStats() const;

    // Force a full compaction of all tiers
    seastar::future<> forceFullCompaction();

    // Check if a file is currently part of an active compaction.
    // Single-threaded per shard — no synchronisation needed.
    bool isFileInActiveCompaction(const seastar::shared_ptr<TSM>& file) const;

    // True when at least one compaction semaphore unit is available.
    // Single-threaded: safe to call and then co_await executeCompaction()
    // without a race, because no other task runs between the check and the
    // first suspension point inside executeCompaction (get_units).
    bool hasCompactionCapacity() const { return compactionSemaphore.available_units() > 0; }

    // Rewrite a single tombstoned file at the same tier to reclaim space.
    // Caller must verify hasCompactionCapacity() first (non-blocking design).
    seastar::future<CompactionStats> executeTombstoneRewrite(seastar::shared_ptr<TSM> file);
};

// Abstract base class for compaction strategies
class CompactionStrategy {
public:
    virtual ~CompactionStrategy() = default;

    // Determine if compaction should run for a tier
    virtual bool shouldCompact(uint64_t tier, size_t fileCount, size_t totalSize) const = 0;

    // Select which files to compact
    virtual std::vector<seastar::shared_ptr<TSM>> selectFiles(
        const std::vector<seastar::shared_ptr<TSM>>& availableFiles, uint64_t tier) const = 0;

    // Get target tier for compacted file
    virtual uint64_t getTargetTier(uint64_t sourceTier, size_t fileCount) const = 0;
};

// Leveled compaction strategy - similar to Cassandra/RocksDB
class LeveledCompactionStrategy : public CompactionStrategy {
private:
    static constexpr size_t NUM_TIERS = 4;
    static constexpr size_t MIN_FILES_PER_TIER[NUM_TIERS] = {4, 4, 4, 8};
    static constexpr size_t MAX_FILES_PER_TIER[NUM_TIERS] = {8, 8, 8, 16};
    static constexpr size_t MAX_BYTES_PER_TIER[NUM_TIERS] = {
        100 * 1024 * 1024,         // 100MB for tier 0
        1024 * 1024 * 1024,        // 1GB for tier 1
        10L * 1024 * 1024 * 1024,  // 10GB for tier 2
        UINT64_MAX                 // No limit for tier 3
    };

public:
    bool shouldCompact(uint64_t tier, size_t fileCount, size_t totalSize) const override {
        if (tier >= NUM_TIERS)
            return false;

        return fileCount >= MIN_FILES_PER_TIER[tier] || totalSize >= MAX_BYTES_PER_TIER[tier];
    }

    std::vector<seastar::shared_ptr<TSM>> selectFiles(const std::vector<seastar::shared_ptr<TSM>>& availableFiles,
                                                      uint64_t tier) const override {
        if (tier >= NUM_TIERS)
            return {};

        std::vector<seastar::shared_ptr<TSM>> selected;

        // Filter files by tier
        for (const auto& file : availableFiles) {
            if (file->tierNum == tier) {
                selected.push_back(file);
            }
        }

        // Sort by sequence number (oldest first)
        std::sort(selected.begin(), selected.end(), [](const auto& a, const auto& b) { return a->seqNum < b->seqNum; });

        // Take up to MAX_FILES_PER_TIER files
        if (selected.size() > MAX_FILES_PER_TIER[tier]) {
            selected.resize(MAX_FILES_PER_TIER[tier]);
        }

        return selected;
    }

    uint64_t getTargetTier(uint64_t sourceTier, size_t fileCount) const override {
        if (sourceTier >= NUM_TIERS)
            return sourceTier;
        // Promote to next tier if we're compacting enough files
        if (fileCount >= MIN_FILES_PER_TIER[sourceTier] && sourceTier < 3) {
            return sourceTier + 1;
        }
        return sourceTier;
    }
};

// Time-based compaction strategy - compact old files
class TimeBasedCompactionStrategy : public CompactionStrategy {
private:
    std::chrono::hours maxAge;

public:
    explicit TimeBasedCompactionStrategy(std::chrono::hours age = std::chrono::hours(24)) : maxAge(age) {}

    bool shouldCompact(uint64_t tier, size_t fileCount, size_t totalSize) const override;

    std::vector<seastar::shared_ptr<TSM>> selectFiles(const std::vector<seastar::shared_ptr<TSM>>& availableFiles,
                                                      uint64_t tier) const override;

    uint64_t getTargetTier(uint64_t sourceTier, size_t fileCount) const override {
        return std::min(sourceTier + 1, 3UL);
    }
};
