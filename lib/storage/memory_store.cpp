#include "memory_store.hpp"
#include "util.hpp"
#include "logger.hpp"
#include "logging_config.hpp"

#include <stdexcept>

template <class T>
void InMemorySeries<T>::insert(TSDBInsert<T> &insertRequest){
  timestamps.insert(timestamps.end(), insertRequest.timestamps.begin(), insertRequest.timestamps.end());
  values.insert(values.end(), insertRequest.values.begin(), insertRequest.values.end());
}

template <class T>
void InMemorySeries<T>::sort(){
  auto p = sort_permutation(timestamps,
    [](uint64_t const& a, uint64_t const& b){ return a < b; });

  timestamps = apply_permutation(timestamps, p);
  values = apply_permutation(values, p);
}

seastar::future<> MemoryStore::close(){
  closed = true;

  if(wal){
    tsdb::memory_log.debug("Closing WAL for memory store {}", sequenceNumber);
    co_await wal->close();
  }
}

seastar::future<> MemoryStore::initWAL(){
  wal = std::make_unique<WAL>(sequenceNumber);
  co_await wal->init(this);
};

seastar::future<> MemoryStore::removeWAL(){
  if(wal){
    co_await wal->remove();
    wal.reset(nullptr);
  }
};

seastar::future<> MemoryStore::initFromWAL(std::string filename){
  WALReader reader(filename);
  co_await reader.readAll(this);
  // Don't initialize a new WAL here - this memory store is being recovered
  // from an existing WAL that will be converted to TSM and removed
}

seastar::future<bool> MemoryStore::isFull(){
  if(!wal) {
    co_return false;
  }
  
  // Use getCurrentSize() which includes buffered data not yet flushed to disk
  size_t walSize = wal->getCurrentSize();
  co_return walSize >= WAL_SIZE_THRESHOLD;
}

template <class T>
void MemoryStore::insertMemory(TSDBInsert<T> &insertRequest){
  // In-memory insert
  auto it = series.find(insertRequest.seriesKey());

  if(it == series.end()){
    InMemorySeries<T> newSeries;
    it = series.insert({insertRequest.seriesKey(), std::move(newSeries)}).first;
  }

  std::get<InMemorySeries<T>>(it->second).insert(insertRequest);
  
  // Clear tombstone ranges that overlap with inserted data
  const std::string& seriesKey = insertRequest.seriesKey();
  auto deletedIt = deletedRanges.find(seriesKey);
  if (deletedIt != deletedRanges.end() && !deletedIt->second.empty()) {
    // Find min and max timestamps from the insert
    uint64_t minTs = *std::min_element(insertRequest.timestamps.begin(), insertRequest.timestamps.end());
    uint64_t maxTs = *std::max_element(insertRequest.timestamps.begin(), insertRequest.timestamps.end());
    
    // Remove or modify tombstone ranges that overlap with [minTs, maxTs]
    auto& ranges = deletedIt->second;
    std::vector<std::pair<uint64_t, uint64_t>> newRanges;
    
    for (const auto& [rangeStart, rangeEnd] : ranges) {
      // If no overlap, keep the range
      if (rangeEnd < minTs || rangeStart > maxTs) {
        newRanges.push_back({rangeStart, rangeEnd});
      } else {
        // There is overlap - split the range if needed
        if (rangeStart < minTs) {
          // Keep the part before the inserted data
          newRanges.push_back({rangeStart, minTs - 1});
        }
        if (rangeEnd > maxTs) {
          // Keep the part after the inserted data
          newRanges.push_back({maxTs + 1, rangeEnd});
        }
        // The overlapping part is removed (not added to newRanges)
      }
    }
    
    ranges = std::move(newRanges);
    
    // Clean up empty entries
    if (ranges.empty()) {
      deletedRanges.erase(deletedIt);
    }
  }
}

template <class T>
bool MemoryStore::wouldExceedThreshold(TSDBInsert<T> &insertRequest){
  if(!wal) {
    return false;
  }
  
  size_t currentSize = wal->getCurrentSize();
  size_t estimatedSize = wal->estimateInsertSize(insertRequest);
  
  return (currentSize + estimatedSize) >= WAL_SIZE_THRESHOLD;
}

template <class T>
seastar::future<bool> MemoryStore::insert(TSDBInsert<T> &insertRequest){
  if(closed)
    throw std::runtime_error("MemoryStore is closed");

  // Check if this insert would exceed threshold
  bool needsRollover = wouldExceedThreshold(insertRequest);
  if(needsRollover) {
    // Don't insert - signal that rollover is needed
    co_return true;
  }

  // WAL Insert
  if(wal) {
    co_await wal->insert(insertRequest);
    LOG_INSERT_PATH(tsdb::memory_log, trace, "WAL insert completed for series: {}", insertRequest.seriesKey());
  }

  insertMemory(insertRequest);
  
  co_return false; // No rollover needed
}

std::optional<TSMValueType> MemoryStore::getSeriesType(std::string &seriesKey){
  auto it = series.find(seriesKey);

  if(it == series.end())
    return {};
  
  return (TSMValueType)it->second.index();
}


template void InMemorySeries<double>::insert(TSDBInsert<double> &insertRequest);
template void InMemorySeries<double>::sort();
template void InMemorySeries<bool>::insert(TSDBInsert<bool> &insertRequest);
template void InMemorySeries<bool>::sort();
template void InMemorySeries<std::string>::insert(TSDBInsert<std::string> &insertRequest);
template void InMemorySeries<std::string>::sort();

template seastar::future<bool> MemoryStore::insert<double>(TSDBInsert<double> &insertRequest);
template seastar::future<bool> MemoryStore::insert<bool>(TSDBInsert<bool> &insertRequest);
template seastar::future<bool> MemoryStore::insert<std::string>(TSDBInsert<std::string> &insertRequest);
template void MemoryStore::insertMemory<double>(TSDBInsert<double> &insertRequest);
template void MemoryStore::insertMemory<bool>(TSDBInsert<bool> &insertRequest);
template void MemoryStore::insertMemory<std::string>(TSDBInsert<std::string> &insertRequest);
template bool MemoryStore::wouldExceedThreshold<double>(TSDBInsert<double> &insertRequest);
template bool MemoryStore::wouldExceedThreshold<bool>(TSDBInsert<bool> &insertRequest);
template bool MemoryStore::wouldExceedThreshold<std::string>(TSDBInsert<std::string> &insertRequest);

void MemoryStore::deleteRange(const std::string& seriesKey, uint64_t startTime, uint64_t endTime) {
    std::cerr << "[MEMORY_STORE_DELETE] deleteRange called for series: " << seriesKey 
              << ", startTime=" << startTime << ", endTime=" << endTime << std::endl;
    
    tsdb::memory_log.debug("Deleting range for series {} from {} to {}", 
                          seriesKey, startTime, endTime);
    
    // Add the deleted range to our tracking
    deletedRanges[seriesKey].push_back({startTime, endTime});
    
    std::cerr << "[MEMORY_STORE_DELETE] Added deleted range to tracking. Total ranges for series " 
              << seriesKey << ": " << deletedRanges[seriesKey].size() << std::endl;
    
    // TODO: We could optimize by merging overlapping ranges, but for now
    // we'll just track all deletions separately
}