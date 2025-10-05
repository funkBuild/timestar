#ifndef __TSM_COMPACTOR_H_INCLUDED__
#define __TSM_COMPACTOR_H_INCLUDED__

#include <vector>
#include <memory>
#include <string>
#include <queue>
#include <atomic>
#include <optional>

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/semaphore.hh>

#include "tsm.hpp"
#include "tsm_writer.hpp"
#include "tsm_result.hpp"
#include "series_id.hpp"
#include "series_compaction_data.hpp"  // Phase 3: Parallel series processing

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
    std::chrono::milliseconds duration;
};

// Represents a plan for compacting a set of TSM files
struct CompactionPlan {
    std::vector<seastar::shared_ptr<TSM>> sourceFiles;
    uint64_t targetTier;
    uint64_t targetSeqNum;
    std::string targetPath;
    uint64_t estimatedSize;
    
    bool isValid() const {
        return !sourceFiles.empty() && !targetPath.empty();
    }
};

// Phase 1.3: Include streaming block iterator
#include "tsm_block_iterator.hpp"

// Iterator for merging multiple TSM files in sorted order
template<typename T>
class TSMMergeIterator {
private:
    struct SeriesIterator {
        seastar::shared_ptr<TSM> file;
        SeriesId128 seriesId;
        std::shared_ptr<TSMBlockIterator<T>> blockIterator;  // Phase 1.3: Use streaming iterator
        TSMBlock<T>* currentBlock = nullptr;  // Phase 1.3: Pointer to current block
        size_t pointIndex = 0;
        bool exhausted = false;

        SeriesIterator(seastar::shared_ptr<TSM> f, const SeriesId128& id)
            : file(f), seriesId(id) {}

        uint64_t currentTimestamp() const {
            if (exhausted || !currentBlock) {
                return UINT64_MAX;
            }
            if (pointIndex >= currentBlock->timestamps->size()) {
                return UINT64_MAX;
            }
            return currentBlock->timestamps->at(pointIndex);
        }

        T currentValue() const {
            return currentBlock->values->at(pointIndex);
        }

        // Phase 1.3: Async advance that loads blocks on-demand
        seastar::future<> advance() {
            pointIndex++;
            if (currentBlock && pointIndex >= currentBlock->timestamps->size()) {
                // Current block exhausted, load next block
                if (blockIterator->hasNext()) {
                    currentBlock = co_await blockIterator->nextBlock();
                    pointIndex = 0;
                } else {
                    exhausted = true;
                    currentBlock = nullptr;
                }
            }
        }
    };
    
    std::vector<SeriesIterator> iterators;
    SeriesId128 seriesId;

    // Priority queue to efficiently find minimum timestamp
    // When timestamps are equal, prefer higher iterator index (newer file)
    struct QueueItem {
        uint64_t timestamp;
        size_t iteratorIndex;
        uint64_t fileRank;  // Used to prefer newer files when timestamps are equal
        
        // Comparison for min-heap: smaller timestamp first,
        // but for equal timestamps, prefer HIGHER rank (newer file) first
        bool operator>(const QueueItem& other) const {
            if (timestamp != other.timestamp) {
                return timestamp > other.timestamp;
            }
            // For same timestamp, we want LOWER rank to be "greater" so it's processed later
            return fileRank < other.fileRank;
        }
    };
    std::priority_queue<QueueItem, std::vector<QueueItem>, std::greater<QueueItem>> minHeap;
    
public:
    TSMMergeIterator(const SeriesId128& series,
                     const std::vector<seastar::shared_ptr<TSM>>& files)
        : seriesId(series) {
        iterators.reserve(files.size());
        for (auto& file : files) {
            iterators.emplace_back(file, series);
        }
    }
    
    // Phase 1.3: Streaming initialization - only read first block
    seastar::future<> init() {
        for (size_t i = 0; i < iterators.size(); i++) {
            auto& iter = iterators[i];

            // Create block iterator (lightweight - no I/O yet)
            iter.blockIterator = std::make_shared<TSMBlockIterator<T>>(
                iter.file, seriesId, 0, UINT64_MAX);

            // Initialize block iterator (reads index metadata only)
            co_await iter.blockIterator->init();

            // Load first block if available
            if (iter.blockIterator->hasNext()) {
                iter.currentBlock = co_await iter.blockIterator->nextBlock();
                if (iter.currentBlock && !iter.currentBlock->timestamps->empty()) {
                    minHeap.push({iter.currentTimestamp(), i, iter.file->rankAsInteger()});
                } else {
                    iter.exhausted = true;
                }
            } else {
                iter.exhausted = true;
            }
        }
    }
    
    bool hasNext() const {
        return !minHeap.empty();
    }

    // Phase 2.2: Getter for series ID (needed by pipeline merge)
    const SeriesId128& getSeriesId() const {
        return seriesId;
    }
    
    // Phase 1.3: Async next() for streaming block loading
    seastar::future<std::pair<uint64_t, T>> next() {
        auto item = minHeap.top();
        minHeap.pop();

        auto& iter = iterators[item.iteratorIndex];
        T value = iter.currentValue();
        co_await iter.advance();  // Phase 1.3: Async advance

        // Re-add to heap if not exhausted
        if (!iter.exhausted) {
            minHeap.push({iter.currentTimestamp(), item.iteratorIndex, iter.file->rankAsInteger()});
        }

        co_return std::make_pair(item.timestamp, value);
    }

    // Phase 1.3: Async nextBatch for efficient processing
    seastar::future<std::vector<std::pair<uint64_t, T>>> nextBatch(size_t maxPoints = 1000) {
        std::vector<std::pair<uint64_t, T>> batch;
        batch.reserve(maxPoints);

        while (hasNext() && batch.size() < maxPoints) {
            auto [timestamp, value] = co_await next();  // Phase 1.3: Async

            // Check if there are more iterators at the same timestamp
            // and skip them (keeping the value from the highest rank/newest file)
            while (hasNext() && !minHeap.empty() && minHeap.top().timestamp == timestamp) {
                // Pop and advance iterators with same timestamp
                // The value we already have is from the newest file (highest rank)
                auto dupItem = minHeap.top();
                minHeap.pop();

                auto& dupIter = iterators[dupItem.iteratorIndex];
                co_await dupIter.advance();  // Phase 1.3: Async

                // Re-add to heap if not exhausted
                if (!dupIter.exhausted) {
                    minHeap.push({dupIter.currentTimestamp(), dupItem.iteratorIndex,
                                 dupIter.file->rankAsInteger()});
                }
            }

            batch.push_back({timestamp, value});
        }

        co_return batch;
    }
};

class TSMCompactor {
private:
    // Limits for compaction
    static constexpr size_t MAX_CONCURRENT_COMPACTIONS = 2;
    static constexpr size_t MAX_MEMORY_PER_COMPACTION = 256 * 1024 * 1024; // 256MB
    static constexpr size_t BATCH_SIZE = 10000; // Points per batch
    
    // Compaction thresholds per tier
    static constexpr size_t TIER0_MIN_FILES = 4;
    static constexpr size_t TIER1_MIN_FILES = 4;
    static constexpr size_t TIER2_MIN_FILES = 4;
    
    TSMFileManager* fileManager;
    std::unique_ptr<CompactionStrategy> strategy;
    seastar::semaphore compactionSemaphore{MAX_CONCURRENT_COMPACTIONS};
    std::atomic<bool> compactionEnabled{true};
    
    // Track active compactions
    struct ActiveCompaction {
        CompactionPlan plan;
        std::chrono::steady_clock::time_point startTime;
        CompactionStats stats;
    };
    std::vector<ActiveCompaction> activeCompactions;
    
    // Generate output filename for compacted file
    std::string generateCompactedFilename(uint64_t tier, uint64_t seqNum);
    
    // Merge series data from multiple files
    template<typename T>
    seastar::future<> mergeSeries(const SeriesId128& seriesId,
                                  const std::vector<seastar::shared_ptr<TSM>>& sources,
                                  TSMWriter& writer,
                                  CompactionStats& stats);

    // Phase 2.2: Merge series using pre-initialized iterator (for pipeline)
    // Phase 5.2: Templated on iterator type to support specialized merges
    template<typename T, typename MergeIterator>
    seastar::future<> mergeSeriesWithIterator(MergeIterator& merger,
                                               const std::vector<seastar::shared_ptr<TSM>>& sources,
                                               TSMWriter& writer,
                                               CompactionStats& stats);

    // Phase A: Bulk-load merge (no streaming, minimal async overhead)
    template<typename T>
    seastar::future<> mergeSeriesBulk(const SeriesId128& seriesId,
                                       const std::vector<seastar::shared_ptr<TSM>>& sources,
                                       TSMWriter& writer,
                                       CompactionStats& stats);

    // Phase 3: Process series for compaction without writing (for parallel processing)
    template<typename T>
    seastar::future<SeriesCompactionData<T>> processSeriesForCompaction(
        const SeriesId128& seriesId,
        const std::vector<seastar::shared_ptr<TSM>>& sources);

    // Phase 3: Write pre-processed series data to TSMWriter
    template<typename T>
    void writeSeriesCompactionData(TSMWriter& writer, SeriesCompactionData<T>&& data, CompactionStats& stats);

    // Get all unique series IDs from files
    std::vector<SeriesId128> getAllSeriesIds(const std::vector<seastar::shared_ptr<TSM>>& files);
    
public:
    explicit TSMCompactor(TSMFileManager* manager);
    ~TSMCompactor() = default;
    
    // Set custom compaction strategy
    void setStrategy(std::unique_ptr<CompactionStrategy> newStrategy) {
        strategy = std::move(newStrategy);
    }
    
    // Main compaction method - merges files and returns path to new file
    seastar::future<std::string> compact(const std::vector<seastar::shared_ptr<TSM>>& files);
    
    // Create a compaction plan for given tier
    CompactionPlan planCompaction(uint64_t tier);
    
    // Execute a compaction plan
    seastar::future<CompactionStats> executeCompaction(const CompactionPlan& plan);
    
    // Check if compaction is needed for a tier
    bool shouldCompact(uint64_t tier) const;
    
    // Run continuous background compaction
    seastar::future<> runCompactionLoop();
    
    // Stop all compactions
    void stopCompaction() {
        compactionEnabled = false;
    }
    
    // Get current compaction statistics
    std::vector<CompactionStats> getActiveCompactionStats() const;
    
    // Force a full compaction of all tiers
    seastar::future<> forceFullCompaction();
};

// Abstract base class for compaction strategies
class CompactionStrategy {
public:
    virtual ~CompactionStrategy() = default;
    
    // Determine if compaction should run for a tier
    virtual bool shouldCompact(uint64_t tier, 
                              size_t fileCount, 
                              size_t totalSize) const = 0;
    
    // Select which files to compact
    virtual std::vector<seastar::shared_ptr<TSM>> selectFiles(
        const std::vector<seastar::shared_ptr<TSM>>& availableFiles,
        uint64_t tier) const = 0;
    
    // Get target tier for compacted file
    virtual uint64_t getTargetTier(uint64_t sourceTier, size_t fileCount) const = 0;
};

// Leveled compaction strategy - similar to Cassandra/RocksDB
class LeveledCompactionStrategy : public CompactionStrategy {
private:
    static constexpr size_t MIN_FILES_PER_TIER[4] = {4, 4, 4, 8};
    static constexpr size_t MAX_FILES_PER_TIER[4] = {8, 8, 8, 16};
    static constexpr size_t MAX_BYTES_PER_TIER[4] = {
        100 * 1024 * 1024,   // 100MB for tier 0
        1024 * 1024 * 1024,  // 1GB for tier 1
        10L * 1024 * 1024 * 1024,  // 10GB for tier 2
        UINT64_MAX           // No limit for tier 3
    };
    
public:
    bool shouldCompact(uint64_t tier, size_t fileCount, size_t totalSize) const override {
        if (tier >= 4) return false;
        
        return fileCount >= MIN_FILES_PER_TIER[tier] ||
               totalSize >= MAX_BYTES_PER_TIER[tier];
    }
    
    std::vector<seastar::shared_ptr<TSM>> selectFiles(
        const std::vector<seastar::shared_ptr<TSM>>& availableFiles,
        uint64_t tier) const override {
        
        std::vector<seastar::shared_ptr<TSM>> selected;
        
        // Filter files by tier
        for (const auto& file : availableFiles) {
            if (file->tierNum == tier) {
                selected.push_back(file);
            }
        }
        
        // Sort by sequence number (oldest first)
        std::sort(selected.begin(), selected.end(),
                 [](const auto& a, const auto& b) {
                     return a->seqNum < b->seqNum;
                 });
        
        // Take up to MAX_FILES_PER_TIER files
        if (selected.size() > MAX_FILES_PER_TIER[tier]) {
            selected.resize(MAX_FILES_PER_TIER[tier]);
        }
        
        return selected;
    }
    
    uint64_t getTargetTier(uint64_t sourceTier, size_t fileCount) const override {
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
    explicit TimeBasedCompactionStrategy(std::chrono::hours age = std::chrono::hours(24))
        : maxAge(age) {}
    
    bool shouldCompact(uint64_t tier, size_t fileCount, size_t totalSize) const override;
    
    std::vector<seastar::shared_ptr<TSM>> selectFiles(
        const std::vector<seastar::shared_ptr<TSM>>& availableFiles,
        uint64_t tier) const override;
    
    uint64_t getTargetTier(uint64_t sourceTier, size_t fileCount) const override {
        return std::min(sourceTier + 1, 3UL);
    }
};

#endif // __TSM_COMPACTOR_H_INCLUDED__