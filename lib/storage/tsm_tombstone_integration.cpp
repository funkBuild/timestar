#include "logger.hpp"
#include "logging_config.hpp"
#include "tsm.hpp"

#include <algorithm>
#include <filesystem>
#include <functional>
#include <limits>

namespace fs = std::filesystem;

// Get path for tombstone file based on TSM file path
std::string TSM::getTombstonePath() const {
    // Replace .tsm extension with .tombstone
    size_t dotPos = filePath.rfind('.');
    if (dotPos != std::string::npos) {
        return filePath.substr(0, dotPos) + ".tombstone";
    }
    return filePath + ".tombstone";
}

// Load tombstones when opening TSM file
seastar::future<> TSM::loadTombstones() {
    try {
        std::string tombstonePath = getTombstonePath();
        LOG_INSERT_PATH(timestar::tsm_log, trace, "Loading tombstones from: {} for TSM: {}", tombstonePath, filePath);

        tombstones = std::make_unique<timestar::TSMTombstone>(tombstonePath);

        // Check if tombstone file exists
        bool exists = co_await tombstones->exists();
        LOG_INSERT_PATH(timestar::tsm_log, trace, "Tombstone file exists: {}", exists);

        if (exists) {
            co_await tombstones->load();
            LOG_INSERT_PATH(timestar::tsm_log, debug, "Successfully loaded tombstones from: {}", tombstonePath);
        } else {
            LOG_INSERT_PATH(timestar::tsm_log, trace, "No tombstone file found for: {}", filePath);
        }
    } catch (const std::exception& e) {
        // Log warning but don't fail - TSM can work without tombstones
        timestar::tsm_log.warn("Failed to load tombstones for {}: {}", filePath, e.what());
        tombstones.reset();  // Clear tombstone manager on error
    }
    co_return;
}

// Check if series exists in the given time range
bool TSM::hasSeriesInTimeRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) const {
    // Check bloom filter first
    if (!seriesBloomFilter.contains(seriesId.getRawData())) {
        return false;
    }

    // Check if series exists in cache (promote to front on hit)
    auto it = fullIndexCache.find(seriesId);
    if (it == fullIndexCache.end()) {
        // Not in cache - could exist but not loaded yet
        // For tombstone operations, be conservative and assume it might exist
        return seriesBloomFilter.contains(seriesId.getRawData());
    }

    // Promote to front of LRU list on access
    lruList.splice(lruList.begin(), lruList, it->second);

    // Check if any index blocks overlap with the time range
    const auto& indexEntry = it->second->second;
    for (const auto& block : indexEntry.indexBlocks) {
        if (block.minTime <= endTime && block.maxTime >= startTime) {
            return true;
        }
    }

    return false;
}

// Check if this TSM file could potentially contain data in the given time range
// This is more permissive than hasSeriesInTimeRange - used for delete operations
bool TSM::couldContainTimeRange(uint64_t startTime, uint64_t endTime) const {
    // For now, be very permissive - assume any TSM file could contain the data
    // In a more sophisticated implementation, we could check file-level time bounds
    // But for delete operations, it's better to be safe and create tombstones
    // even if no current data exists (to handle future writes)

    // Only reject if the time range is clearly invalid
    if (startTime > endTime) {
        return false;
    }

    return true;  // Accept all valid time ranges for delete operations
}

// Delete range with verification
seastar::future<bool> TSM::deleteRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) {
    // IMPORTANT: Only add tombstones if the series actually exists in this TSM file
    // This prevents unnecessary tombstone creation for non-existent series

    // Load the full index entry (uses bloom filter + lazy loading)
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry) {
        // Series doesn't exist in this TSM file - no tombstone needed
        LOG_INSERT_PATH(timestar::tsm_log, trace, "Series '{}' not found in TSM {} - skipping tombstone",
                        seriesId.toHex(), filePath);
        co_return false;
    }

    // Check if any index blocks overlap with the deletion time range
    bool hasOverlap = false;
    for (const auto& block : indexEntry->indexBlocks) {
        if (block.minTime <= endTime && block.maxTime >= startTime) {
            hasOverlap = true;
            break;
        }
    }

    if (!hasOverlap) {
        // No data in the requested time range - no tombstone needed
        LOG_INSERT_PATH(timestar::tsm_log, trace,
                        "Series '{}' has no data in range [{}, {}] in TSM {} - skipping tombstone", seriesId.toHex(),
                        startTime, endTime, filePath);
        co_return false;
    }

    // Series exists and has data in the time range - add tombstone
    LOG_INSERT_PATH(timestar::tsm_log, debug, "Adding tombstone for series '{}' in TSM {}", seriesId.toHex(), filePath);

    // Initialize tombstones if not already done
    if (!tombstones) {
        tombstones = std::make_unique<timestar::TSMTombstone>(getTombstonePath());
    }

    // Add tombstone using the full SeriesId128 (no hash truncation)
    bool added = co_await tombstones->addTombstone(seriesId, startTime, endTime, nullptr);

    if (added) {
        // Persist tombstone immediately for durability
        co_await tombstones->flush();
        LOG_INSERT_PATH(timestar::tsm_log, debug, "Tombstone persisted for series '{}' in TSM {}", seriesId.toHex(),
                        filePath);
    }

    co_return added;
}

// Query with tombstone filtering
template <class T>
seastar::future<TSMResult<T>> TSM::queryWithTombstones(const SeriesId128& seriesId, uint64_t startTime,
                                                       uint64_t endTime) {
    // First, perform the regular query using optimized batched reads
    TSMResult<T> result(rankAsInteger());
    co_await readSeriesBatched<T>(seriesId, startTime, endTime, result);

    // hasTombstones() is O(1): checks both null and entry count == 0.
    // Skips the per-series map lookup and result.empty() check when no
    // tombstones exist for this TSM file (the common case).
    if (!hasTombstones()) {
        co_return result;
    }

    // Apply tombstone filtering if tombstones exist and there's data
    if (result.empty()) {
        co_return result;
    }

    // Get merged tombstone ranges for this series
    auto ranges = tombstones->getTombstoneRanges(seriesId);
    if (ranges.empty()) {
        // No tombstones for this series — return data as-is, no copy needed
        LOG_INSERT_PATH(timestar::tsm_log, trace, "No tombstones for series {} in TSM {}, returning unfiltered data",
                        seriesId.toHex(), filePath);
        co_return result;
    }

    LOG_INSERT_PATH(timestar::tsm_log, trace, "TSM {} has {} tombstone ranges for series {}, filtering in single pass",
                    filePath, ranges.size(), seriesId.toHex());

    // Single-pass filter: iterate blocks directly and copy only non-tombstoned points
    // into one output block, avoiding the intermediate getAllData() copy.
    size_t totalPoints = 0;
    for (const auto& block : result.blocks) {
        totalPoints += block->size();
    }

    auto filteredBlock = std::make_unique<TSMBlock<T>>(totalPoints);
    auto& outTimestamps = filteredBlock->timestamps;
    auto& outValues = filteredBlock->values;

    size_t tombstonedCount = 0;
    // Two-pointer sweep: since both timestamps and tombstone ranges are sorted,
    // we advance a range index linearly instead of binary searching per point.
    // This is O(N + T) instead of O(N * log T).
    size_t ri = 0;
    const size_t numRanges = ranges.size();
    for (auto& block : result.blocks) {
        const auto& ts = block->timestamps;
        auto& vals = block->values;
        // Rewind range pointer if this block starts before where we left off.
        // Blocks are sorted by startTime but may overlap, so the two-pointer
        // invariant (monotonically advancing ri) can break across block boundaries.
        if (!ts.empty() && ri > 0) {
            while (ri > 0 && ranges[ri - 1].second >= ts[0]) {
                --ri;
            }
        }
        for (size_t i = 0; i < ts.size(); ++i) {
            uint64_t t = ts[i];
            // Advance range pointer past ranges that end before this timestamp
            while (ri < numRanges && ranges[ri].second < t) {
                ++ri;
            }
            bool isTombstoned = (ri < numRanges && t >= ranges[ri].first && t <= ranges[ri].second);
            if (!isTombstoned) {
                outTimestamps.push_back(t);
                if constexpr (std::is_same_v<T, bool>) {
                    outValues.push_back(vals[i]);
                } else {
                    outValues.push_back(std::move(vals[i]));
                }
            } else {
                ++tombstonedCount;
            }
        }
    }

    LOG_INSERT_PATH(timestar::tsm_log, trace, "Tombstone filtering: {} points -> {} points ({} removed)", totalPoints,
                    outTimestamps.size(), tombstonedCount);

    // Replace blocks with filtered result
    result.blocks.clear();
    if (!outTimestamps.empty()) {
        result.appendBlock(std::move(filteredBlock));
    }

    co_return result;
}

// Estimate fraction of file data covered by tombstones (metadata-only, no data reads).
// Uses time-range overlap between tombstone ranges and index block [minTime, maxTime]
// to estimate dead bytes, weighted by compressed block size.
//
// Threading: runs on a single shard's reactor.  The only suspension point is
// prefetchFullIndexEntries() (DMA I/O).  After that, getSeriesBlocks() and
// getTombstoneRanges() are pure synchronous cache/map lookups — no other task
// on this shard can mutate the TSM object while the CPU-bound loop executes.
seastar::future<double> TSM::estimateTombstoneCoverage() {
    if (!hasTombstones()) {
        co_return 0.0;
    }

    uint64_t fileSize = getFileSize();
    if (fileSize == 0) {
        co_return 0.0;
    }

    // Get all tombstoned series
    auto tombstonedSeriesSet = tombstones->getTombstonedSeries();
    if (tombstonedSeriesSet.empty()) {
        co_return 0.0;
    }

    std::vector<SeriesId128> tombstonedSeries(tombstonedSeriesSet.begin(), tombstonedSeriesSet.end());

    // Batch-prefetch full index entries into the LRU cache (single DMA I/O
    // suspension point).  After this returns, getSeriesBlocks() is guaranteed
    // to find the entries in cache — provided the number of tombstoned series
    // is ≤ maxCacheEntries(), which is the common case.
    co_await prefetchFullIndexEntries(tombstonedSeries);

    // Pure CPU work below — no suspension points, no iterator invalidation risk.
    double estimatedDeadBytes = 0.0;

    for (const auto& seriesId : tombstonedSeries) {
        auto blocks = getSeriesBlocks(seriesId);
        if (blocks.empty()) {
            continue;
        }

        auto ranges = tombstones->getTombstoneRanges(seriesId);
        if (ranges.empty()) {
            continue;
        }

        for (const auto& block : blocks) {
            uint64_t blockDuration = block.maxTime - block.minTime;

            if (blockDuration == 0) {
                // Single-point block: 100% dead if timestamp falls in any range
                auto rangeIt = std::upper_bound(ranges.begin(), ranges.end(),
                                                std::make_pair(block.minTime, std::numeric_limits<uint64_t>::max()));
                if (rangeIt != ranges.begin()) {
                    --rangeIt;
                    if (block.minTime >= rangeIt->first && block.minTime <= rangeIt->second) {
                        estimatedDeadBytes += block.size;
                    }
                }
            } else {
                // Compute total overlap duration between block range and tombstone ranges
                uint64_t overlapDuration = 0;
                for (const auto& [rStart, rEnd] : ranges) {
                    if (rStart > block.maxTime || rEnd < block.minTime) {
                        continue;  // No overlap
                    }
                    uint64_t overlapStart = std::max(rStart, block.minTime);
                    uint64_t overlapEnd = std::min(rEnd, block.maxTime);
                    overlapDuration += (overlapEnd - overlapStart);
                }
                double overlapFraction = static_cast<double>(overlapDuration) / static_cast<double>(blockDuration);
                // Clamp to [0, 1] in case of overlapping tombstone ranges
                overlapFraction = std::min(overlapFraction, 1.0);
                estimatedDeadBytes += overlapFraction * block.size;
            }
        }
    }

    co_return estimatedDeadBytes / static_cast<double>(fileSize);
}

// Delete tombstone file after successful compaction
seastar::future<> TSM::deleteTombstoneFile() {
    if (tombstones) {
        co_await tombstones->remove();
        tombstones.reset();
    }
    co_return;
}

// Explicit template instantiations for supported types
template seastar::future<TSMResult<double>> TSM::queryWithTombstones<double>(const SeriesId128&, uint64_t, uint64_t);
template seastar::future<TSMResult<bool>> TSM::queryWithTombstones<bool>(const SeriesId128&, uint64_t, uint64_t);
template seastar::future<TSMResult<std::string>> TSM::queryWithTombstones<std::string>(const SeriesId128&, uint64_t,
                                                                                       uint64_t);
template seastar::future<TSMResult<int64_t>> TSM::queryWithTombstones<int64_t>(const SeriesId128&, uint64_t, uint64_t);