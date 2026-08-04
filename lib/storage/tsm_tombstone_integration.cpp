#include "logger.hpp"
#include "logging_config.hpp"
#include "placement_table.hpp"
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
    auto mutationUnits = co_await seastar::get_units(tombstoneMutationSemaphore_, 1);
    std::string tombstonePath = getTombstonePath();
    LOG_INSERT_PATH(timestar::tsm_log, trace, "Loading tombstones from: {} for TSM: {}", tombstonePath, filePath);

    tombstones = std::make_unique<timestar::TSMTombstone>(tombstonePath);

    // A present sidecar is part of the immutable generation's logical data.
    // Never open the raw TSM after discarding an unreadable or corrupt delete
    // set: doing so resurrects points that were durably removed.
    bool exists = co_await tombstones->exists();
    LOG_INSERT_PATH(timestar::tsm_log, trace, "Tombstone file exists: {}", exists);

    if (exists) {
        try {
            co_await tombstones->load();
            LOG_INSERT_PATH(timestar::tsm_log, debug, "Successfully loaded tombstones from: {}", tombstonePath);
        } catch (const std::exception& e) {
            tombstones.reset();
            throw std::runtime_error("Failed to load tombstone sidecar " + tombstonePath + " for " + filePath + ": " +
                                     e.what());
        }
    } else {
        LOG_INSERT_PATH(timestar::tsm_log, trace, "No tombstone file found for: {}", filePath);
    }
    co_return;
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
        // Delete ranges and tombstone filters are inclusive at both ends. An
        // exclusive comparison here skipped an exact-point delete whenever the
        // point was the minimum (and, for a one-point block, the maximum) time.
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

    // Keep the in-memory range mutation and its atomic sidecar publication in
    // one serial critical section. A second delete must not rewrite the same
    // vectors or temporary/final paths while this flush is suspended in I/O.
    auto mutationUnits = co_await seastar::get_units(tombstoneMutationSemaphore_, 1);

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

seastar::future<size_t> TSM::deleteVShard(uint16_t vshard) {
    if (vshard >= timestar::VIRTUAL_SHARD_COUNT)
        throw std::invalid_argument("TSM::deleteVShard: invalid VShard");

    std::vector<SeriesId128> targetIds;
    forEachSeriesId([&](const SeriesId128& id) {
        if (timestar::virtualShard(id) == vshard)
            targetIds.push_back(id);
    });
    if (targetIds.empty())
        co_return 0;

    // One lock and one atomic sidecar rewrite for the complete generation
    // fence. Per-series deleteRange would rewrite/fsync an ever-growing file
    // once per identity and becomes quadratic on a production-sized VShard.
    auto mutationUnits = co_await seastar::get_units(tombstoneMutationSemaphore_, 1);
    if (!tombstones)
        tombstones = std::make_unique<timestar::TSMTombstone>(getTombstonePath());
    if (tombstones->addFullRangeTombstones(targetIds))
        co_await tombstones->flush();
    co_return targetIds.size();
}

// Query with tombstone filtering
template <class T>
seastar::future<TSMResult<T>> TSM::queryWithTombstones(const SeriesId128& seriesId, uint64_t startTime,
                                                       uint64_t endTime) {
    // First, perform the regular query using optimized batched reads.
    // Rank = duplicate-resolution priority (last-write-wins across files).
    TSMResult<T> result(dataRank());
    try {
        co_await readSeriesBatched<T>(seriesId, startTime, endTime, result);
    } catch (...) {
        // Name the file in the error: this is the message the query handler's
        // dropped-series log and QUERY_INCOMPLETE reason ultimately print, and
        // without the path a corrupt block cannot be traced to its TSM file.
        rethrowWithFilePath();
    }

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
            // Guard against corrupted blocks where maxTime < minTime
            if (block.maxTime < block.minTime)
                continue;
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

    co_return std::min(estimatedDeadBytes / static_cast<double>(fileSize), 1.0);
}

// Delete the sidecar after successful compaction. Do not reset the in-memory
// tombstone manager: queries can retain a shared_ptr<TSM> across compaction and
// continue reading the unlinked TSM through its open descriptor.
seastar::future<bool> TSM::deleteTombstoneFile() {
    auto mutationUnits = co_await seastar::get_units(tombstoneMutationSemaphore_, 1);
    if (tombstones) {
        co_return co_await tombstones->unlinkFile();
    }
    co_return false;
}

// Explicit template instantiations for supported types
template seastar::future<TSMResult<double>> TSM::queryWithTombstones<double>(const SeriesId128&, uint64_t, uint64_t);
template seastar::future<TSMResult<bool>> TSM::queryWithTombstones<bool>(const SeriesId128&, uint64_t, uint64_t);
template seastar::future<TSMResult<std::string>> TSM::queryWithTombstones<std::string>(const SeriesId128&, uint64_t,
                                                                                       uint64_t);
template seastar::future<TSMResult<int64_t>> TSM::queryWithTombstones<int64_t>(const SeriesId128&, uint64_t, uint64_t);
