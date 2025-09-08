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

// Convert series key to numeric ID for tombstones
uint64_t TSM::getSeriesId(const std::string& seriesKey) const {
    // Use hash function to convert string key to numeric ID
    // This should match the ID generation used elsewhere in the system
    std::hash<std::string> hasher;
    return hasher(seriesKey);
}

// Check if series exists in the given time range
bool TSM::hasSeriesInTimeRange(
    const std::string& seriesKey,
    uint64_t startTime,
    uint64_t endTime) const {
    
    // Check if series exists in index
    auto it = index.find(seriesKey);
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
    const std::string& seriesKey,
    uint64_t startTime,
    uint64_t endTime) {
    
    // For delete operations, we should create tombstones even if no current data exists
    // This ensures future writes in this time range are properly handled
    // Only skip if this is clearly the wrong TSM file (e.g., time range completely outside file bounds)
    
    // Check if this TSM file could potentially contain data in this time range
    // This is more permissive than hasSeriesInTimeRange - it only checks file-level time bounds
    if (!couldContainTimeRange(startTime, endTime)) {
        // Time range is completely outside this TSM file's time bounds
        co_return false;
    }
    
    // Initialize tombstones if not already done
    if (!tombstones) {
        tombstones = std::make_unique<tsdb::TSMTombstone>(getTombstonePath());
    }
    
    // Add tombstone (passing nullptr for now - verification already done above)
    uint64_t seriesId = getSeriesId(seriesKey);
    bool added = co_await tombstones->addTombstone(seriesId, startTime, endTime, nullptr);
    
    if (added) {
        // Persist tombstone immediately for durability
        co_await tombstones->flush();
    }
    
    co_return added;
}

// Query with tombstone filtering
template <class T>
seastar::future<TSMResult<T>> TSM::queryWithTombstones(
    const std::string& seriesKey,
    uint64_t startTime,
    uint64_t endTime) {
    
    // First, perform the regular query
    TSMResult<T> result(rankAsInteger());
    co_await readSeries<T>(seriesKey, startTime, endTime, result);
    
    if (!tombstones) {
        std::cerr << "[TOMBSTONE_DEBUG] No tombstones loaded for TSM file " << filePath 
                  << ", returning unfiltered data" << std::endl;
    }
    
    // Apply tombstone filtering if tombstones exist
    if (tombstones && !result.empty()) {
        uint64_t seriesId = getSeriesId(seriesKey);
        
        std::cerr << "[TOMBSTONE_DEBUG] TSM " << filePath << " has tombstones, filtering series: " << seriesKey 
                  << " (ID: " << seriesId << ")" << std::endl;
        
        // Get all data from blocks
        auto [allTimestamps, allValues] = result.getAllData();
        
        std::cerr << "[TOMBSTONE_DEBUG] Data before filtering: " << allTimestamps.size() << " points" << std::endl;
        
        if (!allTimestamps.empty()) {
            // Filter out tombstoned data
            auto [filteredTimestamps, filteredValues] = 
                tombstones->filterTombstoned(seriesId, allTimestamps, allValues);
            
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
    const std::string&, uint64_t, uint64_t);
template seastar::future<TSMResult<bool>> TSM::queryWithTombstones<bool>(
    const std::string&, uint64_t, uint64_t);
template seastar::future<TSMResult<std::string>> TSM::queryWithTombstones<std::string>(
    const std::string&, uint64_t, uint64_t);