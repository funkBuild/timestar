#include "tsm.hpp"
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
        std::cerr << "[TOMBSTONE_LOAD] Loading tombstones from: " << tombstonePath 
                  << " for TSM: " << filePath << std::endl;
        
        tombstones = std::make_unique<tsdb::TSMTombstone>(tombstonePath);
        
        // Check if tombstone file exists
        bool exists = co_await tombstones->exists();
        std::cerr << "[TOMBSTONE_LOAD] Tombstone file exists: " << exists << std::endl;
        
        if (exists) {
            co_await tombstones->load();
            std::cerr << "[TOMBSTONE_LOAD] Successfully loaded tombstones from: " << tombstonePath << std::endl;
        } else {
            std::cerr << "[TOMBSTONE_LOAD] No tombstone file found for: " << filePath << std::endl;
        }
    } catch (const std::exception& e) {
        // Log warning but don't fail - TSM can work without tombstones
        std::cerr << "[TOMBSTONE_LOAD] Warning: Failed to load tombstones for " << filePath 
                  << ": " << e.what() << std::endl;
        tombstones.reset();  // Clear tombstone manager on error
    }
    co_return;
}

// Get series ID hash for tombstone compatibility
uint64_t TSM::getSeriesIdHash(const SeriesId128& seriesId) const {
    // Use hash function to convert SeriesId128 to numeric ID for tombstones
    std::hash<SeriesId128> hasher;
    return hasher(seriesId);
}

// Check if series exists in the given time range
bool TSM::hasSeriesInTimeRange(
    const SeriesId128& seriesId,
    uint64_t startTime,
    uint64_t endTime) const {
    
    // Check if series exists in index
    auto it = index.find(seriesId);
    if (it == index.end()) {
        return false;
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
    
    // Check if series exists in the index of this TSM file
    auto it = index.find(seriesId);
    if (it == index.end()) {
        // Series doesn't exist in this TSM file - no tombstone needed
        std::cerr << "[TSM_DELETE] Series '" << seriesId.toHex() 
                  << "' not found in TSM " << filePath << " - skipping tombstone" << std::endl;
        co_return false;
    }
    
    // Check if any index blocks overlap with the deletion time range
    bool hasOverlap = false;
    const auto& indexEntry = it->second;
    for (const auto& block : indexEntry.indexBlocks) {
        if (block.minTime <= endTime && block.maxTime >= startTime) {
            hasOverlap = true;
            break;
        }
    }
    
    if (!hasOverlap) {
        // No data in the requested time range - no tombstone needed
        std::cerr << "[TSM_DELETE] Series '" << seriesId.toHex() 
                  << "' has no data in range [" << startTime << ", " << endTime 
                  << "] in TSM " << filePath << " - skipping tombstone" << std::endl;
        co_return false;
    }
    
    // Series exists and has data in the time range - add tombstone
    std::cerr << "[TSM_DELETE] Adding tombstone for series '" << seriesId.toHex() 
              << "' in TSM " << filePath << std::endl;
    
    // Initialize tombstones if not already done
    if (!tombstones) {
        tombstones = std::make_unique<tsdb::TSMTombstone>(getTombstonePath());
    }
    
    // Add tombstone (passing nullptr for now - verification already done above)
    uint64_t seriesIdHash = getSeriesIdHash(seriesId);
    bool added = co_await tombstones->addTombstone(seriesIdHash, startTime, endTime, nullptr);
    
    if (added) {
        // Persist tombstone immediately for durability
        co_await tombstones->flush();
        std::cerr << "[TSM_DELETE] Tombstone persisted for series '" << seriesId.toHex() 
                  << "' in TSM " << filePath << std::endl;
    }
    
    co_return added;
}

// Query with tombstone filtering
template <class T>
seastar::future<TSMResult<T>> TSM::queryWithTombstones(
    const SeriesId128& seriesId,
    uint64_t startTime,
    uint64_t endTime) {
    
    // First, perform the regular query
    TSMResult<T> result(rankAsInteger());
    co_await readSeries<T>(seriesId, startTime, endTime, result);
    
    if (!tombstones) {
        std::cerr << "[TOMBSTONE_DEBUG] No tombstones loaded for TSM file " << filePath 
                  << ", returning unfiltered data" << std::endl;
    }
    
    // Apply tombstone filtering if tombstones exist
    if (tombstones && !result.empty()) {
        uint64_t seriesIdHash = getSeriesIdHash(seriesId);
        
        std::cerr << "[TOMBSTONE_DEBUG] TSM " << filePath << " has tombstones, filtering series: " << seriesId.toHex() 
                  << " (Hash: " << seriesIdHash << ")" << std::endl;
        
        // Get all data from blocks
        auto [allTimestamps, allValues] = result.getAllData();
        
        std::cerr << "[TOMBSTONE_DEBUG] Data before filtering: " << allTimestamps.size() << " points" << std::endl;
        
        if (!allTimestamps.empty()) {
            // Filter out tombstoned data
            auto [filteredTimestamps, filteredValues] = 
                tombstones->filterTombstoned(seriesIdHash, allTimestamps, allValues);
            
            std::cerr << "[TOMBSTONE_DEBUG] Data after filtering: " << filteredTimestamps.size() << " points" << std::endl;
            
            // Rebuild result with filtered data
            if (!filteredTimestamps.empty()) {
                result.blocks.clear();
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