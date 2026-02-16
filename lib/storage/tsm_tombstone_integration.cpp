#include "tsm.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include <filesystem>
#include <functional>

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
        LOG_INSERT_PATH(tsdb::tsm_log, trace, "Loading tombstones from: {} for TSM: {}",
                        tombstonePath, filePath);
        
        tombstones = std::make_unique<tsdb::TSMTombstone>(tombstonePath);
        
        // Check if tombstone file exists
        bool exists = co_await tombstones->exists();
        LOG_INSERT_PATH(tsdb::tsm_log, trace, "Tombstone file exists: {}", exists);
        
        if (exists) {
            co_await tombstones->load();
            LOG_INSERT_PATH(tsdb::tsm_log, debug, "Successfully loaded tombstones from: {}", tombstonePath);
        } else {
            LOG_INSERT_PATH(tsdb::tsm_log, trace, "No tombstone file found for: {}", filePath);
        }
    } catch (const std::exception& e) {
        // Log warning but don't fail - TSM can work without tombstones
        tsdb::tsm_log.warn("Failed to load tombstones for {}: {}", filePath, e.what());
        tombstones.reset();  // Clear tombstone manager on error
    }
    co_return;
}

// Check if series exists in the given time range
bool TSM::hasSeriesInTimeRange(
    const SeriesId128& seriesId,
    uint64_t startTime,
    uint64_t endTime) const {

    // Check bloom filter first
    if (!seriesBloomFilter.contains(seriesId.toBytes())) {
        return false;
    }

    // Check if series exists in cache
    auto it = fullIndexCache.find(seriesId);
    if (it == fullIndexCache.end()) {
        // Not in cache - could exist but not loaded yet
        // For tombstone operations, be conservative and assume it might exist
        return seriesBloomFilter.contains(seriesId.toBytes());
    }

    // Check if any index blocks overlap with the time range
    const auto& indexEntry = it->second;
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
seastar::future<bool> TSM::deleteRange(
    const SeriesId128& seriesId,
    uint64_t startTime,
    uint64_t endTime) {

    // IMPORTANT: Only add tombstones if the series actually exists in this TSM file
    // This prevents unnecessary tombstone creation for non-existent series

    // Load the full index entry (uses bloom filter + lazy loading)
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry) {
        // Series doesn't exist in this TSM file - no tombstone needed
        LOG_INSERT_PATH(tsdb::tsm_log, trace, "Series '{}' not found in TSM {} - skipping tombstone",
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
        LOG_INSERT_PATH(tsdb::tsm_log, trace, "Series '{}' has no data in range [{}, {}] in TSM {} - skipping tombstone",
                        seriesId.toHex(), startTime, endTime, filePath);
        co_return false;
    }
    
    // Series exists and has data in the time range - add tombstone
    LOG_INSERT_PATH(tsdb::tsm_log, debug, "Adding tombstone for series '{}' in TSM {}",
                    seriesId.toHex(), filePath);
    
    // Initialize tombstones if not already done
    if (!tombstones) {
        tombstones = std::make_unique<tsdb::TSMTombstone>(getTombstonePath());
    }
    
    // Add tombstone using the full SeriesId128 (no hash truncation)
    bool added = co_await tombstones->addTombstone(seriesId, startTime, endTime, nullptr);
    
    if (added) {
        // Persist tombstone immediately for durability
        co_await tombstones->flush();
        LOG_INSERT_PATH(tsdb::tsm_log, debug, "Tombstone persisted for series '{}' in TSM {}",
                        seriesId.toHex(), filePath);
    }
    
    co_return added;
}

// Query with tombstone filtering
template <class T>
seastar::future<TSMResult<T>> TSM::queryWithTombstones(
    const SeriesId128& seriesId,
    uint64_t startTime,
    uint64_t endTime) {
    
    // First, perform the regular query using optimized batched reads
    TSMResult<T> result(rankAsInteger());
    co_await readSeriesBatched<T>(seriesId, startTime, endTime, result);
    
    if (!tombstones) {
        LOG_INSERT_PATH(tsdb::tsm_log, trace, "No tombstones loaded for TSM file {}, returning unfiltered data", filePath);
    }
    
    // Apply tombstone filtering if tombstones exist
    if (tombstones && !result.empty()) {
        LOG_INSERT_PATH(tsdb::tsm_log, trace, "TSM {} has tombstones, filtering series: {}",
                        filePath, seriesId.toHex());

        // Get all data from blocks
        auto [allTimestamps, allValues] = result.getAllData();

        LOG_INSERT_PATH(tsdb::tsm_log, trace, "Data before filtering: {} points", allTimestamps.size());

        if (!allTimestamps.empty()) {
            // Filter out tombstoned data using full SeriesId128 (no hash truncation)
            auto [filteredTimestamps, filteredValues] =
                tombstones->filterTombstoned(seriesId, allTimestamps, allValues);
            
            LOG_INSERT_PATH(tsdb::tsm_log, trace, "Data after filtering: {} points", filteredTimestamps.size());
            
            // Always clear original blocks when tombstone filtering is applied
            result.blocks.clear();

            // Rebuild result with filtered data if any remains
            if (!filteredTimestamps.empty()) {
                auto block = std::make_unique<TSMBlock<T>>(filteredTimestamps.size());
                block->timestamps = std::make_unique<std::vector<uint64_t>>(std::move(filteredTimestamps));
                block->values = std::make_unique<std::vector<T>>(std::move(filteredValues));
                result.appendBlock(block);
            }
        }
    }
    
    co_return result;
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
template seastar::future<TSMResult<double>> TSM::queryWithTombstones<double>(
    const SeriesId128&, uint64_t, uint64_t);
template seastar::future<TSMResult<bool>> TSM::queryWithTombstones<bool>(
    const SeriesId128&, uint64_t, uint64_t);
template seastar::future<TSMResult<std::string>> TSM::queryWithTombstones<std::string>(
    const SeriesId128&, uint64_t, uint64_t);