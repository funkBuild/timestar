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
  co_await initWAL();
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