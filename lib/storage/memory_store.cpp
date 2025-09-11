#include "memory_store.hpp"
#include "util.hpp"
#include "logger.hpp"
#include "logging_config.hpp"

#include <stdexcept>
#include <chrono>

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
  co_await wal->init(this, false); // false = not recovery, create fresh WAL
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
  SeriesId128 seriesId = insertRequest.seriesId128();
  auto it = series.find(seriesId);

  if(it == series.end()){
    InMemorySeries<T> newSeries;
    it = series.insert({seriesId, std::move(newSeries)}).first;
  }

  std::get<InMemorySeries<T>>(it->second).insert(insertRequest);
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
bool MemoryStore::wouldBatchExceedThreshold(std::vector<TSDBInsert<T>> &insertRequests){
  if(!wal || insertRequests.empty()) {
    return false;
  }
  
  size_t currentSize = wal->getCurrentSize();
  size_t totalEstimatedSize = 0;
  
  for(auto& insertRequest : insertRequests) {
    totalEstimatedSize += wal->estimateInsertSize(insertRequest);
  }
  
  return (currentSize + totalEstimatedSize) >= WAL_SIZE_THRESHOLD;
}

template <class T>
seastar::future<bool> MemoryStore::insert(TSDBInsert<T> &insertRequest){
  if(closed)
    throw std::runtime_error("MemoryStore is closed");

  // Check if this insert would exceed the 16MB WAL threshold
  bool needsRollover = wouldExceedThreshold(insertRequest);
  if(needsRollover) {
    // Don't insert - signal that rollover is needed
    tsdb::memory_log.debug("Insert would exceed 16MB WAL limit, signaling rollover needed");
    co_return true;
  }

  // Try to insert to WAL
  if(wal) {
    try {
      co_await wal->insert(insertRequest);
      LOG_INSERT_PATH(tsdb::memory_log, trace, "WAL insert completed for series: {}", insertRequest.seriesKey());
    } catch (const std::runtime_error& e) {
      // Check if it's a rollover signal from WAL
      if (std::string(e.what()) == "WAL rollover needed") {
        tsdb::memory_log.debug("WAL signaled rollover needed during insert");
        co_return true;  // Signal rollover needed
      }
      // Otherwise, it's a real error
      throw;
    }
  }

  // Only insert to memory if WAL write succeeded
  insertMemory(insertRequest);
  
  co_return false; // No rollover needed
}

template <class T>
seastar::future<bool> MemoryStore::insertBatch(std::vector<TSDBInsert<T>> &insertRequests){
  if(closed)
    throw std::runtime_error("MemoryStore is closed");
    
  if(insertRequests.empty()) {
    co_return false; // No work to do
  }

  auto start_memory_batch = std::chrono::high_resolution_clock::now();
  LOG_INSERT_PATH(tsdb::memory_log, info, "[PERF] [MEMORY] Batch insert started for {} requests", insertRequests.size());

  // Check if this batch would exceed the WAL threshold
  bool needsRollover = wouldBatchExceedThreshold(insertRequests);
  if(needsRollover) {
    // Don't insert - signal that rollover is needed
    LOG_INSERT_PATH(tsdb::memory_log, debug, "Batch insert would exceed WAL limit, signaling rollover needed");
    co_return true;
  }

  // Try to insert batch to WAL using the existing batch functionality
  auto start_wal_insert = std::chrono::high_resolution_clock::now();
  if(wal) {
    try {
      co_await wal->insertBatch(insertRequests);
      LOG_INSERT_PATH(tsdb::memory_log, trace, "WAL batch insert completed for {} requests", insertRequests.size());
    } catch (const std::runtime_error& e) {
      // Check if it's a rollover signal from WAL
      if (std::string(e.what()) == "WAL rollover needed") {
        LOG_INSERT_PATH(tsdb::memory_log, debug, "WAL signaled rollover needed during batch insert");
        co_return true;  // Signal rollover needed
      }
      // Otherwise, it's a real error
      throw;
    }
  }
  auto end_wal_insert = std::chrono::high_resolution_clock::now();
  
  // Only insert to memory if WAL write succeeded - process all requests in batch
  auto start_memory_insert = std::chrono::high_resolution_clock::now();
  for(auto& insertRequest : insertRequests) {
    insertMemory(insertRequest);
  }
  auto end_memory_insert = std::chrono::high_resolution_clock::now();
  
  auto end_memory_batch = std::chrono::high_resolution_clock::now();
  auto wal_insert_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_wal_insert - start_wal_insert);
  auto memory_insert_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_memory_insert - start_memory_insert);
  auto total_memory_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_memory_batch - start_memory_batch);
  
  LOG_INSERT_PATH(tsdb::memory_log, info, "[PERF] [MEMORY] WAL batch insert: {}μs", wal_insert_duration.count());
  LOG_INSERT_PATH(tsdb::memory_log, info, "[PERF] [MEMORY] In-memory batch insert: {}μs", memory_insert_duration.count());
  LOG_INSERT_PATH(tsdb::memory_log, info, "[PERF] [MEMORY] Total batch insert: {}μs", total_memory_duration.count());
  
  co_return false; // No rollover needed
}

std::optional<TSMValueType> MemoryStore::getSeriesType(const SeriesId128 &seriesId){
  auto it = series.find(seriesId);

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
template seastar::future<bool> MemoryStore::insertBatch<double>(std::vector<TSDBInsert<double>> &insertRequests);
template seastar::future<bool> MemoryStore::insertBatch<bool>(std::vector<TSDBInsert<bool>> &insertRequests);
template seastar::future<bool> MemoryStore::insertBatch<std::string>(std::vector<TSDBInsert<std::string>> &insertRequests);
template void MemoryStore::insertMemory<double>(TSDBInsert<double> &insertRequest);
template void MemoryStore::insertMemory<bool>(TSDBInsert<bool> &insertRequest);
template void MemoryStore::insertMemory<std::string>(TSDBInsert<std::string> &insertRequest);
template bool MemoryStore::wouldExceedThreshold<double>(TSDBInsert<double> &insertRequest);
template bool MemoryStore::wouldExceedThreshold<bool>(TSDBInsert<bool> &insertRequest);
template bool MemoryStore::wouldExceedThreshold<std::string>(TSDBInsert<std::string> &insertRequest);
template bool MemoryStore::wouldBatchExceedThreshold<double>(std::vector<TSDBInsert<double>> &insertRequests);
template bool MemoryStore::wouldBatchExceedThreshold<bool>(std::vector<TSDBInsert<bool>> &insertRequests);
template bool MemoryStore::wouldBatchExceedThreshold<std::string>(std::vector<TSDBInsert<std::string>> &insertRequests);

void MemoryStore::deleteRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) {
    tsdb::memory_log.debug("Deleting range for series {} from {} to {}", 
                          seriesId.toHex(), startTime, endTime);
    
    // Find the series in memory
    auto it = series.find(seriesId);
    if (it == series.end()) {
        return; // No data to delete
    }
    
    // Actually remove data from memory based on variant type
    std::visit([&](auto& inMemorySeries) {
        using T = typename std::decay_t<decltype(inMemorySeries.values)>::value_type;
        
        auto& timestamps = inMemorySeries.timestamps;
        auto& values = inMemorySeries.values;
        
        std::vector<uint64_t> newTimestamps;
        std::vector<T> newValues;
        
        int deletedCount = 0;
        for (size_t i = 0; i < timestamps.size(); ++i) {
            uint64_t ts = timestamps[i];
            
            // Keep data that's outside the deletion range
            if (ts < startTime || ts > endTime) {
                newTimestamps.push_back(timestamps[i]);
                newValues.push_back(values[i]);
            } else {
                deletedCount++;
            }
        }
        
        timestamps = std::move(newTimestamps);
        values = std::move(newValues);
        
    }, it->second);
    
    // If all data was removed, remove the series entirely
    auto& variantSeries = it->second;
    bool isEmpty = std::visit([](const auto& inMemorySeries) {
        return inMemorySeries.timestamps.empty();
    }, variantSeries);
    
    if (isEmpty) {
        series.erase(it);
    }
}