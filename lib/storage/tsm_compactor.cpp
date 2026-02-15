#include "tsm_compactor.hpp"
#include "tsm_file_manager.hpp"
#include "tsm_writer.hpp"
#include "logger.hpp"
#include "compaction_pipeline.hpp"  // Phase 2.1: Include prefetch manager
#include "tsm_merge_specialized.hpp"  // Phase 5.1: Specialized N-way merges
#include "bulk_block_loader.hpp"  // Phase A: Bulk loading optimization
#include <filesystem>
#include <set>
#include <unordered_map>
#include <chrono>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/reactor.hh>

namespace fs = std::filesystem;

TSMCompactor::TSMCompactor(TSMFileManager* manager) 
    : fileManager(manager),
      strategy(std::make_unique<LeveledCompactionStrategy>()) {
}

std::string TSMCompactor::generateCompactedFilename(uint64_t tier, uint64_t seqNum) {
    int shardId = seastar::this_shard_id();
    char buffer[256];
    snprintf(buffer, sizeof(buffer), "shard_%d/tsm/%lu_%lu.tsm", shardId, tier, seqNum);
    return std::string(buffer);
}

std::vector<SeriesId128> TSMCompactor::getAllSeriesIds(
    const std::vector<seastar::shared_ptr<TSM>>& files) {

    std::set<SeriesId128> uniqueIds;

    // Collect all unique series IDs from all files
    for (const auto& file : files) {
        auto ids = file->getSeriesIds();
        for (const auto& id : ids) {
            uniqueIds.insert(id);
        }
    }

    // Convert set to vector for iteration
    return std::vector<SeriesId128>(uniqueIds.begin(), uniqueIds.end());
}

template<typename T>
seastar::future<> TSMCompactor::mergeSeries(
    const SeriesId128& seriesId,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats) {

    // Create merge iterator for this series
    TSMMergeIterator<T> merger(seriesId, sources);
    co_await merger.init();

    // Collect all tombstones for this series from all source files
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    for (const auto& file : sources) {
        if (file->hasTombstones()) {
            auto tombstones = file->getTombstones();
            if (tombstones) {
                uint64_t seriesIdHash = file->getSeriesIdHash(seriesId);
                auto ranges = tombstones->getTombstoneRanges(seriesIdHash);
                tombstoneRanges.insert(tombstoneRanges.end(), ranges.begin(), ranges.end());
            }
        }
    }
    
    // Sort and merge overlapping tombstone ranges
    if (!tombstoneRanges.empty()) {
        std::sort(tombstoneRanges.begin(), tombstoneRanges.end());
        
        // Merge overlapping ranges
        std::vector<std::pair<uint64_t, uint64_t>> mergedRanges;
        mergedRanges.push_back(tombstoneRanges[0]);
        
        for (size_t i = 1; i < tombstoneRanges.size(); ++i) {
            auto& last = mergedRanges.back();
            const auto& current = tombstoneRanges[i];
            
            if (current.first <= last.second + 1) {
                // Overlapping or adjacent, merge
                last.second = std::max(last.second, current.second);
            } else {
                // Non-overlapping, add as new range
                mergedRanges.push_back(current);
            }
        }
        
        tombstoneRanges = std::move(mergedRanges);
    }
    
    std::vector<uint64_t> timestamps;
    std::vector<T> values;
    timestamps.reserve(BATCH_SIZE);
    values.reserve(BATCH_SIZE);
    
    uint64_t lastTimestamp = 0;
    size_t tombstonesFiltered = 0;
    
    // Phase 1.3: Process in batches with streaming
    while (merger.hasNext()) {
        auto batch = co_await merger.nextBatch(BATCH_SIZE);  // Phase 1.3: Async nextBatch
        
        for (const auto& [ts, val] : batch) {
            stats.pointsRead++;
            
            // Check if this point is tombstoned
            bool isTombstoned = false;
            for (const auto& [startTime, endTime] : tombstoneRanges) {
                if (ts >= startTime && ts <= endTime) {
                    isTombstoned = true;
                    tombstonesFiltered++;
                    break;
                }
                // Early exit if we've passed this tombstone range
                if (startTime > ts) {
                    break;
                }
            }
            
            if (isTombstoned) {
                continue; // Skip tombstoned data
            }
            
            // Deduplicate - skip if same timestamp
            if (ts != lastTimestamp) {
                timestamps.push_back(ts);
                values.push_back(val);
                lastTimestamp = ts;
                stats.pointsWritten++;
            } else {
                stats.duplicatesRemoved++;
            }
            
            // Write when batch is full
            if (timestamps.size() >= BATCH_SIZE) {
                writer.writeSeries(TSM::getValueType<T>(), seriesId, timestamps, values);
                timestamps.clear();
                values.clear();
            }
        }
    }

    // Write remaining data
    if (!timestamps.empty()) {
        writer.writeSeries(TSM::getValueType<T>(), seriesId, timestamps, values);
    }

    // Log tombstone filtering stats if any points were filtered
    if (tombstonesFiltered > 0) {
        tsdb::compactor_log.info("Compaction: Filtered {} tombstoned points for series {}",
                                 tombstonesFiltered, seriesId.toHex());
    }

    co_return;
}

// Phase 2.2: Merge series using pre-initialized iterator (for pipeline)
// Phase 5.2: Templated on iterator type to support specialized merges
template<typename T, typename MergeIterator>
seastar::future<> TSMCompactor::mergeSeriesWithIterator(
    MergeIterator& merger,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats) {

    // Iterator already initialized (by prefetch pipeline or specialized merge)
    const SeriesId128& seriesId = merger.getSeriesId();

    // Collect all tombstones for this series from all source files
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    for (const auto& file : sources) {
        if (file->hasTombstones()) {
            auto tombstones = file->getTombstones();
            if (tombstones) {
                uint64_t seriesIdHash = file->getSeriesIdHash(seriesId);
                auto ranges = tombstones->getTombstoneRanges(seriesIdHash);
                tombstoneRanges.insert(tombstoneRanges.end(), ranges.begin(), ranges.end());
            }
        }
    }

    // Sort and merge overlapping tombstone ranges
    if (!tombstoneRanges.empty()) {
        std::sort(tombstoneRanges.begin(), tombstoneRanges.end());

        // Merge overlapping ranges
        std::vector<std::pair<uint64_t, uint64_t>> mergedRanges;
        mergedRanges.push_back(tombstoneRanges[0]);

        for (size_t i = 1; i < tombstoneRanges.size(); ++i) {
            auto& last = mergedRanges.back();
            const auto& current = tombstoneRanges[i];

            if (current.first <= last.second + 1) {
                // Overlapping or adjacent, merge
                last.second = std::max(last.second, current.second);
            } else {
                // Non-overlapping, add as new range
                mergedRanges.push_back(current);
            }
        }

        tombstoneRanges = std::move(mergedRanges);
    }

    std::vector<uint64_t> timestamps;
    std::vector<T> values;
    timestamps.reserve(BATCH_SIZE);
    values.reserve(BATCH_SIZE);

    uint64_t lastTimestamp = 0;
    size_t tombstonesFiltered = 0;

    // Process in batches with streaming
    while (merger.hasNext()) {
        auto batch = co_await merger.nextBatch(BATCH_SIZE);

        for (const auto& [ts, val] : batch) {
            stats.pointsRead++;

            // Phase 4.1: Check if this point is tombstoned using binary search O(log m)
            bool isTombstoned = false;
            if (!tombstoneRanges.empty()) {
                // Binary search: find first range with startTime > ts
                auto it = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(), ts,
                    [](uint64_t timestamp, const std::pair<uint64_t, uint64_t>& range) {
                        return timestamp < range.first;  // Compare ts with startTime
                    });

                // Check the previous range (if exists) to see if ts falls within it
                if (it != tombstoneRanges.begin()) {
                    --it;
                    const auto& [startTime, endTime] = *it;
                    if (ts >= startTime && ts <= endTime) {
                        isTombstoned = true;
                        tombstonesFiltered++;
                    }
                }
            }

            if (isTombstoned) {
                continue; // Skip tombstoned data
            }

            // Deduplicate - skip if same timestamp
            if (ts != lastTimestamp) {
                timestamps.push_back(ts);
                values.push_back(val);
                lastTimestamp = ts;
                stats.pointsWritten++;
            } else {
                stats.duplicatesRemoved++;
            }

            // Write when batch is full
            if (timestamps.size() >= BATCH_SIZE) {
                writer.writeSeriesDirect(TSM::getValueType<T>(), seriesId,
                                        std::move(timestamps),
                                        std::move(values));
                timestamps = std::vector<uint64_t>();
                values = std::vector<T>();
                timestamps.reserve(BATCH_SIZE);
                values.reserve(BATCH_SIZE);
            }
        }
    }

    // Write remaining data
    if (!timestamps.empty()) {
        writer.writeSeriesDirect(TSM::getValueType<T>(), seriesId,
                                std::move(timestamps),
                                std::move(values));
    }

    // Log tombstone filtering stats if any points were filtered
    if (tombstonesFiltered > 0) {
        tsdb::compactor_log.info("Compaction: Filtered {} tombstoned points for series {}",
                                 tombstonesFiltered, seriesId.toHex());
    }

    co_return;
}

// Phase A: Bulk-load merge - loads all blocks at once, minimizes async overhead
template<typename T>
seastar::future<> TSMCompactor::mergeSeriesBulk(
    const SeriesId128& seriesId,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats) {

    // Phase A: Load ALL blocks from ALL files in one batch operation
    auto allBlocks = co_await BulkBlockLoader<T>::loadFromFiles(sources, seriesId);

    if (allBlocks.empty()) {
        co_return;  // Series not in any file
    }

    // Collect tombstones (same as before)
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    for (const auto& file : sources) {
        if (file->hasTombstones()) {
            auto tombstones = file->getTombstones();
            if (tombstones) {
                uint64_t seriesIdHash = file->getSeriesIdHash(seriesId);
                auto ranges = tombstones->getTombstoneRanges(seriesIdHash);
                tombstoneRanges.insert(tombstoneRanges.end(), ranges.begin(), ranges.end());
            }
        }
    }

    // Sort and merge overlapping tombstone ranges
    if (!tombstoneRanges.empty()) {
        std::sort(tombstoneRanges.begin(), tombstoneRanges.end());

        std::vector<std::pair<uint64_t, uint64_t>> mergedRanges;
        mergedRanges.push_back(tombstoneRanges[0]);

        for (size_t i = 1; i < tombstoneRanges.size(); ++i) {
            auto& last = mergedRanges.back();
            const auto& current = tombstoneRanges[i];

            if (current.first <= last.second + 1) {
                last.second = std::max(last.second, current.second);
            } else {
                mergedRanges.push_back(current);
            }
        }

        tombstoneRanges = std::move(mergedRanges);
    }

    // Phase 1/2: Build metadata index for block ordering optimization
    std::vector<BlockMetadata<T>> blockMeta;

    // Phase 2: Get index blocks from source files for zero-copy
    std::vector<std::vector<TSMIndexBlock>> sourceIndexBlocks;
    for (const auto& file : sources) {
        sourceIndexBlocks.push_back(file->getSeriesBlocks(seriesId));
    }

    for (size_t fileIdx = 0; fileIdx < allBlocks.size(); fileIdx++) {
        const auto& fileBlocks = allBlocks[fileIdx];
        const auto& indexBlocks = sourceIndexBlocks[fileIdx];

        // Phase 2: Build metadata from index blocks directly (no decompression needed!)
        for (size_t blockIdx = 0; blockIdx < indexBlocks.size(); blockIdx++) {
            const auto& indexBlock = indexBlocks[blockIdx];

            BlockMetadata<T> meta;
            // Use metadata from index - no need to decompress!
            meta.minTime = indexBlock.minTime;
            meta.maxTime = indexBlock.maxTime;
            meta.fileIndex = fileIdx;
            meta.blockIndex = blockIdx;
            meta.blockPtr = (blockIdx < fileBlocks.blocks.size()) ? fileBlocks.blocks[blockIdx].get() : nullptr;
            meta.fileRank = fileBlocks.fileRank;

            // Phase 2: Store source file and index block for zero-copy
            meta.sourceFile = sources[fileIdx];
            meta.indexBlock = indexBlock;

            blockMeta.push_back(meta);
        }
    }

    // Phase 1: Sort blocks by timestamp
    std::sort(blockMeta.begin(), blockMeta.end());

    // Phase 1: Analyze overlap patterns to detect fast path opportunities
    std::vector<MergeSegment> segments;
    if (!blockMeta.empty()) {
        size_t segmentStart = 0;
        bool currentNeedsMerge = false;

        for (size_t i = 1; i < blockMeta.size(); i++) {
            bool overlaps = blockMeta[i-1].overlapsWith(blockMeta[i]);

            if (i == 1) {
                // First comparison - establish initial pattern
                currentNeedsMerge = overlaps;
            } else if (overlaps != currentNeedsMerge) {
                // Pattern changed - close current segment
                segments.push_back({segmentStart, i-1, currentNeedsMerge});
                segmentStart = i;
                currentNeedsMerge = overlaps;
            }
        }

        // Close final segment
        segments.push_back({segmentStart, blockMeta.size()-1, currentNeedsMerge});
    }

    std::vector<uint64_t> currentTimestamps;
    std::vector<T> currentValues;
    currentTimestamps.reserve(BATCH_SIZE);
    currentValues.reserve(BATCH_SIZE);

    uint64_t lastTimestamp = 0;
    size_t tombstonesFiltered = 0;

    // Phase B: Helper lambda to process batches (same logic for all merger types)
    auto processMerge = [&](auto& merger) {
        while (merger.hasNext()) {
            auto batch = merger.nextBatch(BATCH_SIZE);  // NO co_await!

            for (const auto& [ts, val] : batch) {
                stats.pointsRead++;

                // Phase 4.1: Binary search tombstones
                bool isTombstoned = false;
                if (!tombstoneRanges.empty()) {
                    auto it = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(), ts,
                        [](uint64_t timestamp, const std::pair<uint64_t, uint64_t>& range) {
                            return timestamp < range.first;
                        });

                    if (it != tombstoneRanges.begin()) {
                        --it;
                        const auto& [startTime, endTime] = *it;
                        if (ts >= startTime && ts <= endTime) {
                            isTombstoned = true;
                            tombstonesFiltered++;
                        }
                    }
                }

                if (isTombstoned) {
                    continue;
                }

                // Deduplicate
                if (ts != lastTimestamp) {
                    currentTimestamps.push_back(ts);
                    currentValues.push_back(val);
                    lastTimestamp = ts;
                    stats.pointsWritten++;
                } else {
                    stats.duplicatesRemoved++;
                }

                // Write when batch full
                if (currentTimestamps.size() >= BATCH_SIZE) {
                    writer.writeSeriesDirect(TSM::getValueType<T>(), seriesId,
                                            std::move(currentTimestamps),
                                            std::move(currentValues));
                    currentTimestamps = std::vector<uint64_t>();
                    currentValues = std::vector<T>();
                    currentTimestamps.reserve(BATCH_SIZE);
                    currentValues.reserve(BATCH_SIZE);
                }
            }
        }
    };

    // Phase 1: Helper lambda for direct block copy (fast path - no overlap)
    auto copyBlockDirect = [&](const TSMBlock<T>* block) {
        // ULTRA FAST PATH: No tombstones - copy entire block at once
        if (tombstoneRanges.empty()) {
            const auto& timestamps = *block->timestamps;
            const auto& values = *block->values;
            size_t blockSize = timestamps.size();

            stats.pointsRead += blockSize;
            stats.pointsWritten += blockSize;

            // Copy points in batches
            for (size_t i = 0; i < blockSize; i++) {
                currentTimestamps.push_back(timestamps[i]);
                currentValues.push_back(values[i]);

                // Write when batch full
                if (currentTimestamps.size() >= BATCH_SIZE) {
                    writer.writeSeriesDirect(TSM::getValueType<T>(), seriesId,
                                            std::move(currentTimestamps),
                                            std::move(currentValues));
                    currentTimestamps = std::vector<uint64_t>();
                    currentValues = std::vector<T>();
                    currentTimestamps.reserve(BATCH_SIZE);
                    currentValues.reserve(BATCH_SIZE);
                }
            }

            if (!timestamps.empty()) {
                lastTimestamp = timestamps.back();
            }
            return;
        }

        // SLOW PATH: Have tombstones - check each point
        for (size_t i = 0; i < block->timestamps->size(); i++) {
            uint64_t ts = block->timestamps->at(i);
            const T& val = block->values->at(i);

            stats.pointsRead++;

            // Check tombstones
            bool isTombstoned = false;
            auto it = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(), ts,
                [](uint64_t timestamp, const std::pair<uint64_t, uint64_t>& range) {
                    return timestamp < range.first;
                });

            if (it != tombstoneRanges.begin()) {
                --it;
                const auto& [startTime, endTime] = *it;
                if (ts >= startTime && ts <= endTime) {
                    isTombstoned = true;
                    tombstonesFiltered++;
                }
            }

            if (isTombstoned) {
                continue;
            }

            // Deduplicate
            if (ts != lastTimestamp) {
                currentTimestamps.push_back(ts);
                currentValues.push_back(val);
                lastTimestamp = ts;
                stats.pointsWritten++;
            } else {
                stats.duplicatesRemoved++;
            }

            // Write when batch full
            if (currentTimestamps.size() >= BATCH_SIZE) {
                writer.writeSeriesDirect(TSM::getValueType<T>(), seriesId,
                                        std::move(currentTimestamps),
                                        std::move(currentValues));
                currentTimestamps = std::vector<uint64_t>();
                currentValues = std::vector<T>();
                currentTimestamps.reserve(BATCH_SIZE);
                currentValues.reserve(BATCH_SIZE);
            }
        }
    };

    // Phase 1: Log optimization statistics
    if (!segments.empty()) {
        size_t fastPathBlocks = 0;
        size_t slowPathBlocks = 0;
        size_t fastPathSegments = 0;
        size_t slowPathSegments = 0;

        for (const auto& seg : segments) {
            if (seg.needsMerge) {
                slowPathSegments++;
                slowPathBlocks += seg.blockCount();
            } else {
                fastPathSegments++;
                fastPathBlocks += seg.blockCount();
            }
        }

        tsdb::compactor_log.info(
            "Series {}: {} blocks, {} segments ({} fast path [{} blocks], {} slow path [{} blocks])",
            seriesId.toHex(),
            blockMeta.size(),
            segments.size(),
            fastPathSegments,
            fastPathBlocks,
            slowPathSegments,
            slowPathBlocks
        );
    }

    // Phase 1/2: Check if we can use fast path for ALL blocks
    bool allBlocksNonOverlapping = segments.empty() ||
        std::all_of(segments.begin(), segments.end(),
                    [](const MergeSegment& seg) { return !seg.needsMerge; });

    if (allBlocksNonOverlapping && !blockMeta.empty()) {
        // Phase 2: ZERO-COPY PATH if no tombstones
        if (tombstoneRanges.empty()) {
            // ULTIMATE FAST PATH: Copy compressed blocks directly (no decompress/recompress)

            // Batch read all compressed blocks in parallel (avoid sequential async overhead)
            std::vector<seastar::future<seastar::temporary_buffer<uint8_t>>> readFutures;
            readFutures.reserve(blockMeta.size());

            for (const auto& meta : blockMeta) {
                readFutures.push_back(meta.sourceFile->readCompressedBlock(meta.indexBlock));
            }

            // Wait for all reads to complete at once
            auto compressedBlocks = co_await seastar::when_all(readFutures.begin(), readFutures.end());

            // Write all compressed blocks to destination
            for (size_t i = 0; i < blockMeta.size(); i++) {
                const auto& meta = blockMeta[i];
                auto compressedData = std::move(compressedBlocks[i].get());

                writer.writeCompressedBlock(
                    TSM::getValueType<T>(),
                    seriesId,
                    std::move(compressedData),
                    meta.minTime,
                    meta.maxTime
                );

                // Zero-copy path - we don't decompress so we don't know exact point counts
                // Stats tracking happens at byte level in calling code
            }

            co_return;
        }

        // Phase 1: FAST PATH with tombstones - decompress and filter
        for (const auto& meta : blockMeta) {
            copyBlockDirect(meta.blockPtr);
        }
    } else {
        // SLOW PATH: At least some overlap - use existing full merge logic
        // Fall back to Phase B specialized mergers
        size_t fileCount = allBlocks.size();
        if (fileCount == 2) {
            BulkMerger2Way<T> merger(allBlocks);
            processMerge(merger);
        } else if (fileCount == 3) {
            BulkMerger3Way<T> merger(allBlocks);
            processMerge(merger);
        } else {
            BulkMerger<T> merger(allBlocks);
            processMerge(merger);
        }
    }

    // Write remaining data
    if (!currentTimestamps.empty()) {
        writer.writeSeriesDirect(TSM::getValueType<T>(), seriesId,
                                std::move(currentTimestamps),
                                std::move(currentValues));
    }

    // Log tombstone filtering
    if (tombstonesFiltered > 0) {
        tsdb::compactor_log.info("Compaction: Filtered {} tombstoned points for series {}",
                                 tombstonesFiltered, seriesId.toHex());
    }

    co_return;
}

// Phase 3: Process series for compaction without writing (enables parallel processing)
template<typename T>
seastar::future<SeriesCompactionData<T>> TSMCompactor::processSeriesForCompaction(
    const SeriesId128& seriesId,
    const std::vector<seastar::shared_ptr<TSM>>& sources) {

    SeriesCompactionData<T> result(seriesId, TSM::getValueType<T>());

    // Load ALL blocks from ALL files
    auto allBlocks = co_await BulkBlockLoader<T>::loadFromFiles(sources, seriesId);

    if (allBlocks.empty()) {
        co_return result;  // Empty result
    }

    // Collect tombstones
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    for (const auto& file : sources) {
        if (file->hasTombstones()) {
            auto tombstones = file->getTombstones();
            if (tombstones) {
                uint64_t seriesIdHash = file->getSeriesIdHash(seriesId);
                auto ranges = tombstones->getTombstoneRanges(seriesIdHash);
                tombstoneRanges.insert(tombstoneRanges.end(), ranges.begin(), ranges.end());
            }
        }
    }

    // Sort and merge overlapping tombstone ranges
    if (!tombstoneRanges.empty()) {
        std::sort(tombstoneRanges.begin(), tombstoneRanges.end());

        std::vector<std::pair<uint64_t, uint64_t>> mergedRanges;
        mergedRanges.push_back(tombstoneRanges[0]);

        for (size_t i = 1; i < tombstoneRanges.size(); ++i) {
            auto& last = mergedRanges.back();
            const auto& current = tombstoneRanges[i];

            if (current.first <= last.second + 1) {
                last.second = std::max(last.second, current.second);
            } else {
                mergedRanges.push_back(current);
            }
        }

        tombstoneRanges = std::move(mergedRanges);
    }

    // Build metadata index
    std::vector<BlockMetadata<T>> blockMeta;
    std::vector<std::vector<TSMIndexBlock>> sourceIndexBlocks;
    for (const auto& file : sources) {
        sourceIndexBlocks.push_back(file->getSeriesBlocks(seriesId));
    }

    for (size_t fileIdx = 0; fileIdx < allBlocks.size(); fileIdx++) {
        const auto& fileBlocks = allBlocks[fileIdx];
        const auto& indexBlocks = sourceIndexBlocks[fileIdx];

        for (size_t blockIdx = 0; blockIdx < indexBlocks.size(); blockIdx++) {
            const auto& indexBlock = indexBlocks[blockIdx];

            BlockMetadata<T> meta;
            meta.minTime = indexBlock.minTime;
            meta.maxTime = indexBlock.maxTime;
            meta.fileIndex = fileIdx;
            meta.blockIndex = blockIdx;
            meta.blockPtr = (blockIdx < fileBlocks.blocks.size()) ? fileBlocks.blocks[blockIdx].get() : nullptr;
            meta.fileRank = fileBlocks.fileRank;
            meta.sourceFile = sources[fileIdx];
            meta.indexBlock = indexBlock;

            blockMeta.push_back(meta);
        }
    }

    // Sort blocks by timestamp
    std::sort(blockMeta.begin(), blockMeta.end());

    // Analyze overlap patterns
    std::vector<MergeSegment> segments;
    if (!blockMeta.empty()) {
        size_t segmentStart = 0;
        bool currentNeedsMerge = false;

        for (size_t i = 1; i < blockMeta.size(); i++) {
            bool overlaps = blockMeta[i-1].overlapsWith(blockMeta[i]);

            if (i == 1) {
                currentNeedsMerge = overlaps;
            } else if (overlaps != currentNeedsMerge) {
                segments.push_back({segmentStart, i-1, currentNeedsMerge});
                segmentStart = i;
                currentNeedsMerge = overlaps;
            }
        }

        segments.push_back({segmentStart, blockMeta.size()-1, currentNeedsMerge});
    }

    // Check if we can use zero-copy fast path
    bool allBlocksNonOverlapping = segments.empty() ||
        std::all_of(segments.begin(), segments.end(),
                    [](const MergeSegment& seg) { return !seg.needsMerge; });

    if (allBlocksNonOverlapping && !blockMeta.empty() && tombstoneRanges.empty()) {
        // ZERO-COPY PATH: Load compressed blocks in parallel
        result.isZeroCopy = true;
        result.compressedBlocks.reserve(blockMeta.size());

        std::vector<seastar::future<seastar::temporary_buffer<uint8_t>>> readFutures;
        readFutures.reserve(blockMeta.size());

        for (const auto& meta : blockMeta) {
            readFutures.push_back(meta.sourceFile->readCompressedBlock(meta.indexBlock));
        }

        auto compressedData = co_await seastar::when_all(readFutures.begin(), readFutures.end());

        for (size_t i = 0; i < blockMeta.size(); i++) {
            typename SeriesCompactionData<T>::CompressedBlock block;
            block.data = std::move(compressedData[i].get());
            block.minTime = blockMeta[i].minTime;
            block.maxTime = blockMeta[i].maxTime;
            result.compressedBlocks.push_back(std::move(block));
        }

        co_return result;
    }

    // SLOW PATH: Decompress and merge (either overlap or tombstones)
    result.isZeroCopy = false;
    result.timestamps.reserve(BATCH_SIZE * 10);  // Pre-allocate
    result.values.reserve(BATCH_SIZE * 10);

    uint64_t lastTimestamp = 0;
    size_t tombstonesFiltered = 0;

    // Helper to process blocks
    auto processBlock = [&](const TSMBlock<T>* block) {
        for (size_t i = 0; i < block->timestamps->size(); i++) {
            uint64_t ts = block->timestamps->at(i);
            const T& val = block->values->at(i);

            result.pointsRead++;

            // Check tombstones
            bool isTombstoned = false;
            if (!tombstoneRanges.empty()) {
                auto it = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(), ts,
                    [](uint64_t timestamp, const std::pair<uint64_t, uint64_t>& range) {
                        return timestamp < range.first;
                    });

                if (it != tombstoneRanges.begin()) {
                    --it;
                    const auto& [startTime, endTime] = *it;
                    if (ts >= startTime && ts <= endTime) {
                        isTombstoned = true;
                        tombstonesFiltered++;
                    }
                }
            }

            if (isTombstoned) {
                continue;
            }

            // Deduplicate
            if (ts != lastTimestamp) {
                result.timestamps.push_back(ts);
                result.values.push_back(val);
                lastTimestamp = ts;
                result.pointsWritten++;
            } else {
                result.duplicatesRemoved++;
            }
        }
    };

    // Process based on overlap
    if (allBlocksNonOverlapping) {
        // Fast path with tombstones: direct block copy
        for (const auto& meta : blockMeta) {
            processBlock(meta.blockPtr);
        }
    } else {
        // Slow path: full merge
        size_t fileCount = allBlocks.size();
        if (fileCount == 2) {
            BulkMerger2Way<T> merger(allBlocks);
            while (merger.hasNext()) {
                auto [ts, val] = merger.next();
                result.pointsRead++;

                // Check tombstones and deduplicate
                bool isTombstoned = false;
                if (!tombstoneRanges.empty()) {
                    auto it = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(), ts,
                        [](uint64_t timestamp, const std::pair<uint64_t, uint64_t>& range) {
                            return timestamp < range.first;
                        });

                    if (it != tombstoneRanges.begin()) {
                        --it;
                        const auto& [startTime, endTime] = *it;
                        if (ts >= startTime && ts <= endTime) {
                            isTombstoned = true;
                        }
                    }
                }

                if (!isTombstoned && ts != lastTimestamp) {
                    result.timestamps.push_back(ts);
                    result.values.push_back(val);
                    lastTimestamp = ts;
                    result.pointsWritten++;
                } else if (ts == lastTimestamp) {
                    result.duplicatesRemoved++;
                }
            }
        } else if (fileCount == 3) {
            BulkMerger3Way<T> merger(allBlocks);
            while (merger.hasNext()) {
                auto [ts, val] = merger.next();
                result.pointsRead++;

                bool isTombstoned = false;
                if (!tombstoneRanges.empty()) {
                    auto it = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(), ts,
                        [](uint64_t timestamp, const std::pair<uint64_t, uint64_t>& range) {
                            return timestamp < range.first;
                        });

                    if (it != tombstoneRanges.begin()) {
                        --it;
                        const auto& [startTime, endTime] = *it;
                        if (ts >= startTime && ts <= endTime) {
                            isTombstoned = true;
                        }
                    }
                }

                if (!isTombstoned && ts != lastTimestamp) {
                    result.timestamps.push_back(ts);
                    result.values.push_back(val);
                    lastTimestamp = ts;
                    result.pointsWritten++;
                } else if (ts == lastTimestamp) {
                    result.duplicatesRemoved++;
                }
            }
        } else {
            BulkMerger<T> merger(allBlocks);
            while (merger.hasNext()) {
                auto [ts, val] = merger.next();
                result.pointsRead++;

                bool isTombstoned = false;
                if (!tombstoneRanges.empty()) {
                    auto it = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(), ts,
                        [](uint64_t timestamp, const std::pair<uint64_t, uint64_t>& range) {
                            return timestamp < range.first;
                        });

                    if (it != tombstoneRanges.begin()) {
                        --it;
                        const auto& [startTime, endTime] = *it;
                        if (ts >= startTime && ts <= endTime) {
                            isTombstoned = true;
                        }
                    }
                }

                if (!isTombstoned && ts != lastTimestamp) {
                    result.timestamps.push_back(ts);
                    result.values.push_back(val);
                    lastTimestamp = ts;
                    result.pointsWritten++;
                } else if (ts == lastTimestamp) {
                    result.duplicatesRemoved++;
                }
            }
        }
    }

    co_return result;
}

// Phase 3: Write pre-processed series data to TSMWriter
template<typename T>
void TSMCompactor::writeSeriesCompactionData(
    TSMWriter& writer,
    SeriesCompactionData<T>&& data,
    CompactionStats& stats) {

    if (data.isZeroCopy) {
        // Zero-copy path: write compressed blocks directly
        for (auto& block : data.compressedBlocks) {
            writer.writeCompressedBlock(
                data.seriesType,
                data.seriesId,
                std::move(block.data),
                block.minTime,
                block.maxTime
            );
        }
    } else {
        // Slow path: write decompressed data
        if (!data.timestamps.empty()) {
            writer.writeSeries(
                data.seriesType,
                data.seriesId,
                data.timestamps,
                data.values
            );
        }
    }

    // Update stats
    stats.pointsRead += data.pointsRead;
    stats.pointsWritten += data.pointsWritten;
    stats.duplicatesRemoved += data.duplicatesRemoved;
}

seastar::future<std::string> TSMCompactor::compact(
    const std::vector<seastar::shared_ptr<TSM>>& files) {
    
    if (files.empty()) {
        co_return std::string();
    }
    
    // Determine output tier and filename
    uint64_t maxTier = 0;
    uint64_t maxSeq = 0;
    for (const auto& file : files) {
        maxTier = std::max(maxTier, file->tierNum);
        maxSeq = std::max(maxSeq, file->seqNum);
    }
    
    uint64_t targetTier = strategy->getTargetTier(maxTier, files.size());
    uint64_t targetSeq = maxSeq + 1;
    std::string outputPath = generateCompactedFilename(targetTier, targetSeq);
    
    // Create temporary file for writing
    std::string tempPath = outputPath + ".tmp";
    TSMWriter writer(tempPath);
    
    CompactionStats stats;
    stats.filesCompacted = files.size();
    auto startTime = std::chrono::steady_clock::now();
    
    // Get all unique series across files
    auto allSeries = getAllSeriesIds(files);

    // Phase 2.2: Group series by type for pipelined processing
    std::vector<SeriesId128> floatSeries;
    std::vector<SeriesId128> boolSeries;
    std::vector<SeriesId128> stringSeries;

    for (const auto& seriesId : allSeries) {
        auto seriesType = files[0]->getSeriesType(seriesId);
        if (!seriesType.has_value()) {
            continue;
        }

        if (seriesType.value() == TSMValueType::Float) {
            floatSeries.push_back(seriesId);
        } else if (seriesType.value() == TSMValueType::Boolean) {
            boolSeries.push_back(seriesId);
        } else if (seriesType.value() == TSMValueType::String) {
            stringSeries.push_back(seriesId);
        }
    }

    // Phase 3: Parallel series processing with batching
    // Batch size tuned for performance vs memory tradeoff
    // Testing shows: batch_size=10 → 651ms, batch_size=20 → 541ms, batch_size=50 → 2533ms (regression)
    const size_t SERIES_BATCH_SIZE = 20;

    // Semaphore to serialize writes (TSMWriter not thread-safe)
    seastar::semaphore writeSemaphore{1};

    // Helper lambda for parallel batch processing
    auto processBatch = [&](const auto& seriesVec, auto* typePtr) -> seastar::future<> {
        using ValueType = std::remove_pointer_t<decltype(typePtr)>;

        for (size_t i = 0; i < seriesVec.size(); i += SERIES_BATCH_SIZE) {
            size_t batchEnd = std::min(i + SERIES_BATCH_SIZE, seriesVec.size());

            // Start all series in batch in parallel
            std::vector<seastar::future<>> processFutures;
            processFutures.reserve(batchEnd - i);

            for (size_t j = i; j < batchEnd; j++) {
                const auto& seriesId = seriesVec[j];

                // Process series and write with serialization
                auto future = processSeriesForCompaction<ValueType>(seriesId, files)
                    .then([this, &writer, &writeSemaphore, &stats](SeriesCompactionData<ValueType> data) {
                        // Serialize writes with semaphore
                        return seastar::with_semaphore(writeSemaphore, 1,
                            [this, &writer, &stats, data = std::move(data)]() mutable {
                                writeSeriesCompactionData(writer, std::move(data), stats);
                            });
                    });

                processFutures.push_back(std::move(future));
            }

            // Wait for batch to complete
            co_await seastar::when_all_succeed(processFutures.begin(), processFutures.end());
        }
    };

    // Process all series types in parallel batches
    if (!floatSeries.empty()) {
        co_await processBatch(floatSeries, (double*)nullptr);
    }

    if (!boolSeries.empty()) {
        co_await processBatch(boolSeries, (bool*)nullptr);
    }

    if (!stringSeries.empty()) {
        co_await processBatch(stringSeries, (std::string*)nullptr);
    }
    
    // Finalize the file
    writer.writeIndex();
    writer.close();
    
    // Calculate statistics
    auto endTime = std::chrono::steady_clock::now();
    stats.filesCompacted = files.size();
    stats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime);

    // Store stats so executeCompaction can retrieve them
    lastCompactStats = stats;

    // Atomic rename from temp to final
    fs::rename(tempPath, outputPath);

    co_return outputPath;
}

CompactionPlan TSMCompactor::planCompaction(uint64_t tier) {
    CompactionPlan plan;
    plan.targetTier = tier;
    
    // Get files from file manager using new tier tracking
    std::vector<seastar::shared_ptr<TSM>> tierFiles = fileManager->getFilesInTier(tier);
    
    // Use strategy to select files
    plan.sourceFiles = strategy->selectFiles(tierFiles, tier);
    
    if (!plan.sourceFiles.empty()) {
        // Calculate target tier and sequence
        uint64_t maxSeq = 0;
        for (const auto& file : plan.sourceFiles) {
            maxSeq = std::max(maxSeq, file->seqNum);
        }
        
        plan.targetTier = strategy->getTargetTier(tier, plan.sourceFiles.size());
        plan.targetSeqNum = maxSeq + 1;
        plan.targetPath = generateCompactedFilename(plan.targetTier, plan.targetSeqNum);
        
        // Estimate output size (rough estimate - 70% of input due to compression)
        plan.estimatedSize = 0;
        for (const auto& file : plan.sourceFiles) {
            plan.estimatedSize += file->getFileSize();
        }
        plan.estimatedSize = plan.estimatedSize * 0.7;
    }
    
    return plan;
}

seastar::future<CompactionStats> TSMCompactor::executeCompaction(const CompactionPlan& plan) {
    if (!plan.isValid()) {
        co_return CompactionStats{};
    }
    
    // Acquire semaphore to limit concurrent compactions
    auto units = co_await seastar::get_units(compactionSemaphore, 1);
    
    // Track this compaction
    ActiveCompaction active;
    active.plan = plan;
    active.startTime = std::chrono::steady_clock::now();
    activeCompactions.push_back(active);
    
    // Perform compaction
    std::string newFile = co_await compact(plan.sourceFiles);
    
    if (!newFile.empty()) {
        // Open the new file
        auto newTSM = seastar::make_shared<TSM>(newFile);
        co_await newTSM->open();
        
        // Add to file manager
        co_await fileManager->addTSMFile(newTSM);
        
        // Remove old files from manager and mark for deletion
        co_await fileManager->removeTSMFiles(plan.sourceFiles);
        
        // Delete tombstone files for the compacted TSM files
        for (const auto& file : plan.sourceFiles) {
            if (file->hasTombstones()) {
                co_await file->deleteTombstoneFile();
            }
        }
    }
    
    // Propagate stats from the compact() call and add timing
    active.stats = lastCompactStats;
    auto endTime = std::chrono::steady_clock::now();
    active.stats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - active.startTime);

    // Remove from active list
    activeCompactions.erase(
        std::remove_if(activeCompactions.begin(), activeCompactions.end(),
                      [&](const ActiveCompaction& a) {
                          return a.plan.targetPath == plan.targetPath;
                      }),
        activeCompactions.end());
    
    co_return active.stats;
}

bool TSMCompactor::shouldCompact(uint64_t tier) const {
    // Use the file manager's method to check
    return fileManager->shouldCompactTier(tier);
}

seastar::future<> TSMCompactor::runCompactionLoop() {
    while (compactionEnabled) {
        bool didCompaction = false;
        
        // Check each tier for compaction
        for (uint64_t tier = 0; tier < 4; tier++) {
            if (shouldCompact(tier)) {
                auto plan = planCompaction(tier);
                if (plan.isValid()) {
                    auto stats = co_await executeCompaction(plan);
                    didCompaction = true;
                    
                    tsdb::compactor_log.info("Compacted {} files in tier {}, removed {} duplicates in {}ms",
                                            stats.filesCompacted, tier, stats.duplicatesRemoved,
                                            stats.duration.count());
                }
            }
        }
        
        // Sleep before next check
        if (!didCompaction) {
            co_await seastar::sleep(std::chrono::seconds(30));
        } else {
            // Short sleep after compaction to allow system to stabilize
            co_await seastar::sleep(std::chrono::seconds(5));
        }
    }
}

seastar::future<> TSMCompactor::forceFullCompaction() {
    tsdb::compactor_log.info("Starting forced full compaction...");
    
    // Compact each tier from bottom up
    for (uint64_t tier = 0; tier < 3; tier++) {
        // Get all files in this tier
        std::vector<seastar::shared_ptr<TSM>> tierFiles;
        for (const auto& [seq, file] : fileManager->sequencedTsmFiles) {
            if (file->tierNum == tier) {
                tierFiles.push_back(file);
            }
        }
        
        if (tierFiles.size() > 1) {
            tsdb::compactor_log.info("Compacting {} files in tier {}", tierFiles.size(), tier);
            
            CompactionPlan plan;
            plan.sourceFiles = tierFiles;
            plan.targetTier = tier + 1;
            plan.targetSeqNum = 0; // Will be set properly
            plan.targetPath = generateCompactedFilename(plan.targetTier, plan.targetSeqNum);
            
            auto stats = co_await executeCompaction(plan);
            
            std::cout << "Compacted tier " << tier << ": " 
                     << stats.filesCompacted << " files, "
                     << stats.pointsWritten << " points written, "
                     << stats.duplicatesRemoved << " duplicates removed"
                     << std::endl;
        }
    }
    
    std::cout << "Full compaction complete" << std::endl;
}

std::vector<CompactionStats> TSMCompactor::getActiveCompactionStats() const {
    std::vector<CompactionStats> stats;
    for (const auto& active : activeCompactions) {
        stats.push_back(active.stats);
    }
    return stats;
}

// Explicit template instantiations
template seastar::future<> TSMCompactor::mergeSeries<double>(
    const SeriesId128& seriesId,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

template seastar::future<> TSMCompactor::mergeSeries<bool>(
    const SeriesId128& seriesId,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

// Phase 2.2/5.2: Template instantiations for all merge iterator types
// Generic heap-based merges
template seastar::future<> TSMCompactor::mergeSeriesWithIterator<double, TSMMergeIterator<double>>(
    TSMMergeIterator<double>& merger,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

template seastar::future<> TSMCompactor::mergeSeriesWithIterator<bool, TSMMergeIterator<bool>>(
    TSMMergeIterator<bool>& merger,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

template seastar::future<> TSMCompactor::mergeSeriesWithIterator<std::string, TSMMergeIterator<std::string>>(
    TSMMergeIterator<std::string>& merger,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

// Phase 5.1: Specialized 2-way merges
template seastar::future<> TSMCompactor::mergeSeriesWithIterator<double, TwoWayMergeIterator<double>>(
    TwoWayMergeIterator<double>& merger,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

// Phase 5.1: Specialized 4-way merges
template seastar::future<> TSMCompactor::mergeSeriesWithIterator<double, FourWayMergeIterator<double>>(
    FourWayMergeIterator<double>& merger,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

// Phase A: Bulk merge template instantiations
template seastar::future<> TSMCompactor::mergeSeriesBulk<double>(
    const SeriesId128& seriesId,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

template seastar::future<> TSMCompactor::mergeSeriesBulk<bool>(
    const SeriesId128& seriesId,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

template seastar::future<> TSMCompactor::mergeSeriesBulk<std::string>(
    const SeriesId128& seriesId,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

// Phase 3: Parallel processing template instantiations
template seastar::future<SeriesCompactionData<double>> TSMCompactor::processSeriesForCompaction<double>(
    const SeriesId128& seriesId,
    const std::vector<seastar::shared_ptr<TSM>>& sources);

template seastar::future<SeriesCompactionData<bool>> TSMCompactor::processSeriesForCompaction<bool>(
    const SeriesId128& seriesId,
    const std::vector<seastar::shared_ptr<TSM>>& sources);

template seastar::future<SeriesCompactionData<std::string>> TSMCompactor::processSeriesForCompaction<std::string>(
    const SeriesId128& seriesId,
    const std::vector<seastar::shared_ptr<TSM>>& sources);

template void TSMCompactor::writeSeriesCompactionData<double>(
    TSMWriter& writer,
    SeriesCompactionData<double>&& data,
    CompactionStats& stats);

template void TSMCompactor::writeSeriesCompactionData<bool>(
    TSMWriter& writer,
    SeriesCompactionData<bool>&& data,
    CompactionStats& stats);

template void TSMCompactor::writeSeriesCompactionData<std::string>(
    TSMWriter& writer,
    SeriesCompactionData<std::string>&& data,
    CompactionStats& stats);

// TimeBasedCompactionStrategy implementation
bool TimeBasedCompactionStrategy::shouldCompact(uint64_t tier, 
                                               size_t fileCount, 
                                               size_t totalSize) const {
    // For time-based, compact if we have old files
    // This would need file creation time tracking
    return fileCount >= 2; // Simple check for now
}

std::vector<seastar::shared_ptr<TSM>> TimeBasedCompactionStrategy::selectFiles(
    const std::vector<seastar::shared_ptr<TSM>>& availableFiles,
    uint64_t tier) const {
    
    // Select oldest files
    std::vector<seastar::shared_ptr<TSM>> selected = availableFiles;
    
    // Sort by sequence number (assuming older = lower seq)
    std::sort(selected.begin(), selected.end(),
             [](const auto& a, const auto& b) {
                 return a->seqNum < b->seqNum;
             });
    
    // Take first half of files
    if (selected.size() > 2) {
        selected.resize(selected.size() / 2);
    }
    
    return selected;
}