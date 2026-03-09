#ifndef BULK_BLOCK_LOADER_H_INCLUDED
#define BULK_BLOCK_LOADER_H_INCLUDED

#include "tsm.hpp"
#include "series_id.hpp"
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/when_all.hh>
#include <vector>
#include <memory>
#include <cassert>
#include <algorithm>
#include <queue>

// Phase A: Bulk Block Loading - Remove async streaming overhead
//
// KEY OPTIMIZATION: Load ALL blocks for a series in ONE batch I/O operation
// instead of streaming them one-at-a-time with async overhead.
//
// Before (Streaming):
//   init() → nextBlock() → advance() → nextBlock() → advance()...
//   = N async operations for N blocks
//
// After (Bulk):
//   loadAllBlocks() → return vector<TSMBlock>
//   = 1 async operation for all blocks

template<typename T>
struct SeriesBlocks {
    SeriesId128 seriesId;
    std::vector<std::unique_ptr<TSMBlock<T>>> blocks;  // All blocks loaded in memory
    size_t totalPoints = 0;
    uint64_t fileRank = 0;  // For deduplication (higher rank = newer file)

    SeriesBlocks() = default;
    SeriesBlocks(const SeriesId128& id) : seriesId(id) {}

    // Get point at specific position across all blocks
    struct PointLocation {
        size_t blockIdx;
        size_t pointIdx;
        bool valid;
    };

    PointLocation getPointLocation(size_t globalIndex) const {
        size_t accumulated = 0;
        for (size_t blockIdx = 0; blockIdx < blocks.size(); blockIdx++) {
            size_t blockSize = blocks[blockIdx]->timestamps->size();
            if (globalIndex < accumulated + blockSize) {
                return {blockIdx, globalIndex - accumulated, true};
            }
            accumulated += blockSize;
        }
        return {0, 0, false};
    }
};

// Phase A: Bulk loader for reading all blocks of a series from multiple files
template<typename T>
class BulkBlockLoader {
public:
    // Load all blocks for a series from a single TSM file
    static seastar::future<SeriesBlocks<T>> loadFromFile(
        seastar::shared_ptr<TSM> file,
        const SeriesId128& seriesId,
        uint64_t startTime = 0,
        uint64_t endTime = UINT64_MAX) {

        SeriesBlocks<T> result(seriesId);
        result.fileRank = file->rankAsInteger();

        // Ensure the full index entry is loaded (lazy-loads from sparse index
        // via DMA read if not already cached). getSeriesBlocks() only checks
        // the fullIndexCache, so we must call getFullIndexEntry() first to
        // populate it.
        auto* indexEntry = co_await file->getFullIndexEntry(seriesId);
        if (!indexEntry) {
            co_return result;  // Series not in this file
        }

        auto indexBlocks = file->getSeriesBlocks(seriesId);
        if (indexBlocks.empty()) {
            co_return result;  // Series not in this file
        }

        // Filter blocks by time range
        std::vector<TSMIndexBlock> relevantBlocks;
        for (const auto& block : indexBlocks) {
            if (block.maxTime >= startTime && block.minTime <= endTime) {
                relevantBlocks.push_back(block);
            }
        }

        if (relevantBlocks.empty()) {
            co_return result;
        }

        // Reserve space for blocks
        result.blocks.reserve(relevantBlocks.size());

        // Phase A: TRULY batch load all blocks in parallel using when_all
        std::vector<seastar::future<std::unique_ptr<TSMBlock<T>>>> blockFutures;
        blockFutures.reserve(relevantBlocks.size());

        // Start all reads in parallel
        for (const auto& indexBlock : relevantBlocks) {
            blockFutures.push_back(file->readSingleBlock<T>(indexBlock, startTime, endTime));
        }

        // Wait for all reads to complete at once (single await point!)
        auto blockResults = co_await seastar::when_all(blockFutures.begin(), blockFutures.end());

        // Process results (when_all returns vector of futures)
        for (auto& future : blockResults) {
            auto block = std::move(future.get());
            if (block && !block->timestamps->empty()) {
                result.totalPoints += block->timestamps->size();
                result.blocks.push_back(std::move(block));
            }
        }

        co_return result;
    }

    // Load all blocks for a series from multiple TSM files
    static seastar::future<std::vector<SeriesBlocks<T>>> loadFromFiles(
        const std::vector<seastar::shared_ptr<TSM>>& files,
        const SeriesId128& seriesId,
        uint64_t startTime = 0,
        uint64_t endTime = UINT64_MAX) {

        std::vector<SeriesBlocks<T>> allBlocks;
        allBlocks.reserve(files.size());

        // Load from each file
        for (const auto& file : files) {
            auto blocks = co_await loadFromFile(file, seriesId, startTime, endTime);
            if (!blocks.blocks.empty()) {
                allBlocks.push_back(std::move(blocks));
            }
        }

        co_return allBlocks;
    }
};

// Phase A: In-memory merge context for bulk-loaded blocks
// This replaces the streaming iterator approach with direct array access
template<typename T>
struct BulkMergeContext {
    SeriesBlocks<T>* source;  // Pointer to source blocks
    size_t currentBlockIdx = 0;
    size_t currentPointIdx = 0;
    bool exhausted = false;

    explicit BulkMergeContext(SeriesBlocks<T>* src) : source(src) {
        if (!src || src->blocks.empty()) {
            exhausted = true;
        }
    }

    // Get current timestamp (no async!)
    uint64_t currentTimestamp() const {
        if (exhausted || currentBlockIdx >= source->blocks.size()) {
            return UINT64_MAX;
        }
        const auto& block = source->blocks[currentBlockIdx];
        if (currentPointIdx >= block->timestamps->size()) {
            return UINT64_MAX;
        }
        return block->timestamps->at(currentPointIdx);
    }

    // Get current value (no async!)
    T currentValue() const {
        const auto& block = source->blocks[currentBlockIdx];
        return block->values->at(currentPointIdx);
    }

    // Advance to next point (no async!)
    void advance() {
        if (exhausted || !source) {
            exhausted = true;
            return;
        }

        currentPointIdx++;

        // Move to next block if current exhausted
        while (currentBlockIdx < source->blocks.size()) {
            if (currentPointIdx < source->blocks[currentBlockIdx]->timestamps->size()) {
                return;  // Still have points in current block
            }

            // Move to next block
            currentBlockIdx++;
            currentPointIdx = 0;
        }

        // All blocks exhausted
        exhausted = true;
    }

    bool hasMore() const {
        return !exhausted;
    }

    uint64_t getFileRank() const {
        return source->fileRank;
    }
};

// Phase B: Specialized 2-way merge (fastest: just compare two values)
template<typename T>
class BulkMerger2Way {
private:
    BulkMergeContext<T> ctx0;
    BulkMergeContext<T> ctx1;

public:
    explicit BulkMerger2Way(std::vector<SeriesBlocks<T>>& sources)
        : ctx0(&sources[0]), ctx1(&sources[1]) {
        if (sources.size() != 2) {
            throw std::invalid_argument("BulkMerger2Way requires exactly 2 sources, got " +
                                        std::to_string(sources.size()));
        }
    }

    [[gnu::always_inline]]
    inline bool hasNext() const {
        return ctx0.hasMore() || ctx1.hasMore();
    }

    // Get next point (NO ASYNC! NO HEAP! Just compare two values)
    [[gnu::always_inline]]
    inline std::pair<uint64_t, T> next() {
        uint64_t ts0 = ctx0.currentTimestamp();
        uint64_t ts1 = ctx1.currentTimestamp();

        if (ts0 < ts1) {
            T value = ctx0.currentValue();
            ctx0.advance();
            return {ts0, value};
        } else if (ts1 < ts0) {
            T value = ctx1.currentValue();
            ctx1.advance();
            return {ts1, value};
        } else {
            // Duplicate: prefer newer file (higher rank)
            T value = (ctx0.getFileRank() > ctx1.getFileRank())
                      ? ctx0.currentValue()
                      : ctx1.currentValue();
            ctx0.advance();
            ctx1.advance();
            return {ts0, value};
        }
    }

    // Get next batch (NO ASYNC!)
    std::vector<std::pair<uint64_t, T>> nextBatch(size_t maxPoints = 1000) {
        std::vector<std::pair<uint64_t, T>> batch;
        batch.reserve(maxPoints);

        while (hasNext() && batch.size() < maxPoints) {
            batch.push_back(next());
        }

        return batch;
    }
};

// Phase B: Specialized 3-way merge (fast: min of three values)
template<typename T>
class BulkMerger3Way {
private:
    BulkMergeContext<T> ctx0;
    BulkMergeContext<T> ctx1;
    BulkMergeContext<T> ctx2;

public:
    explicit BulkMerger3Way(std::vector<SeriesBlocks<T>>& sources)
        : ctx0(&sources[0]), ctx1(&sources[1]), ctx2(&sources[2]) {
        if (sources.size() != 3) {
            throw std::invalid_argument("BulkMerger3Way requires exactly 3 sources, got " +
                                        std::to_string(sources.size()));
        }
    }

    [[gnu::always_inline]]
    inline bool hasNext() const {
        return ctx0.hasMore() || ctx1.hasMore() || ctx2.hasMore();
    }

    // Get next point (NO ASYNC! NO HEAP! Just find min of three)
    [[gnu::always_inline]]
    inline std::pair<uint64_t, T> next() {
        uint64_t ts0 = ctx0.currentTimestamp();
        uint64_t ts1 = ctx1.currentTimestamp();
        uint64_t ts2 = ctx2.currentTimestamp();

        // Find minimum timestamp
        uint64_t minTs = std::min({ts0, ts1, ts2});

        // Collect value from source with min timestamp (prefer highest rank on tie).
        // At least one source must match minTs since it's their minimum.
        T value{};
        uint64_t maxRank = 0;
        bool found = false;

        if (ts0 == minTs) {
            value = ctx0.currentValue();
            maxRank = ctx0.getFileRank();
            found = true;
            ctx0.advance();
        }
        if (ts1 == minTs) {
            if (!found || ctx1.getFileRank() > maxRank) {
                value = ctx1.currentValue();
                maxRank = ctx1.getFileRank();
                found = true;
            }
            ctx1.advance();
        }
        if (ts2 == minTs) {
            if (!found || ctx2.getFileRank() > maxRank) {
                value = ctx2.currentValue();
            }
            ctx2.advance();
        }

        return {minTs, value};
    }

    // Get next batch (NO ASYNC!)
    std::vector<std::pair<uint64_t, T>> nextBatch(size_t maxPoints = 1000) {
        std::vector<std::pair<uint64_t, T>> batch;
        batch.reserve(maxPoints);

        while (hasNext() && batch.size() < maxPoints) {
            batch.push_back(next());
        }

        return batch;
    }
};

// Phase A: Bulk N-way merge for in-memory blocks (N≥4 files)
// Merges multiple SeriesBlocks into batches without async overhead
template<typename T>
class BulkMerger {
private:
    std::vector<BulkMergeContext<T>> contexts;

    // Priority queue for N≥4 files
    struct QueueItem {
        uint64_t timestamp;
        size_t contextIndex;
        uint64_t fileRank;

        bool operator>(const QueueItem& other) const {
            if (timestamp != other.timestamp) {
                return timestamp > other.timestamp;
            }
            return fileRank < other.fileRank;  // Prefer newer file
        }
    };
    std::priority_queue<QueueItem, std::vector<QueueItem>, std::greater<QueueItem>> minHeap;

public:
    explicit BulkMerger(std::vector<SeriesBlocks<T>>& sources) {
        contexts.reserve(sources.size());
        for (auto& src : sources) {
            contexts.emplace_back(&src);
        }

        // Initialize heap
        for (size_t i = 0; i < contexts.size(); i++) {
            if (contexts[i].hasMore()) {
                minHeap.push({contexts[i].currentTimestamp(), i, contexts[i].getFileRank()});
            }
        }
    }

    bool hasNext() const {
        return !minHeap.empty();
    }

    // Get next point (NO ASYNC!)
    std::pair<uint64_t, T> next() {
        auto item = minHeap.top();
        minHeap.pop();

        auto& ctx = contexts[item.contextIndex];
        uint64_t timestamp = ctx.currentTimestamp();
        T value = ctx.currentValue();

        ctx.advance();

        if (ctx.hasMore()) {
            minHeap.push({ctx.currentTimestamp(), item.contextIndex, ctx.getFileRank()});
        }

        return {timestamp, value};
    }

    // Get next batch (NO ASYNC!)
    std::vector<std::pair<uint64_t, T>> nextBatch(size_t maxPoints = 1000) {
        std::vector<std::pair<uint64_t, T>> batch;
        batch.reserve(maxPoints);

        while (hasNext() && batch.size() < maxPoints) {
            auto [timestamp, value] = next();

            // Skip duplicates from other sources (keep newest file's value)
            while (hasNext() && !minHeap.empty() && minHeap.top().timestamp == timestamp) {
                auto dupItem = minHeap.top();
                minHeap.pop();

                auto& dupCtx = contexts[dupItem.contextIndex];
                dupCtx.advance();

                if (dupCtx.hasMore()) {
                    minHeap.push({dupCtx.currentTimestamp(), dupItem.contextIndex, dupCtx.getFileRank()});
                }
            }

            batch.push_back({timestamp, value});
        }

        return batch;
    }
};

// Phase 1: Block Ordering Optimization - Metadata for overlap detection
// Represents timestamp range and location of a block for fast overlap checking
template<typename T>
struct BlockMetadata {
    uint64_t minTime;
    uint64_t maxTime;
    size_t fileIndex;        // Which SeriesBlocks (file) this came from
    size_t blockIndex;       // Which block within that file
    TSMBlock<T>* blockPtr;   // Pointer to actual block data
    uint64_t fileRank;       // For deduplication (higher = newer)

    // Phase 2: Zero-copy support
    seastar::shared_ptr<TSM> sourceFile;  // Source TSM file for zero-copy reads
    TSMIndexBlock indexBlock;             // Original index block metadata

    // Check if this block overlaps with another
    bool overlapsWith(const BlockMetadata& other) const {
        // Two blocks overlap if their ranges intersect
        // No overlap if: this ends before other starts OR this starts after other ends
        return !(maxTime < other.minTime || minTime > other.maxTime);
    }

    // Comparison for sorting by timestamp
    bool operator<(const BlockMetadata& other) const {
        // Sort by minTime first (temporal order)
        if (minTime != other.minTime) return minTime < other.minTime;
        // If same start, sort by maxTime (shorter ranges first)
        if (maxTime != other.maxTime) return maxTime < other.maxTime;
        // If same range, prefer newer files (higher rank)
        return fileRank > other.fileRank;
    }
};

// Phase 1: Merge segment - represents contiguous blocks that need merging
struct MergeSegment {
    size_t startIdx;      // Index in blockMeta vector (inclusive)
    size_t endIdx;        // Index in blockMeta vector (inclusive)
    bool needsMerge;      // true if blocks in this range overlap

    size_t blockCount() const { return endIdx - startIdx + 1; }
};

#endif // BULK_BLOCK_LOADER_H_INCLUDED
